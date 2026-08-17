use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "metadata-read-stats")]
use std::marker::PhantomData;
#[cfg(feature = "metadata-read-stats")]
use std::rc::Rc;
#[cfg(feature = "metadata-read-stats")]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use nokv_meta_store::{
    Check, Commit, Key, Keyspace, LimitKind, Mutation, ReadBatch, ReadOp, ReadResult, ReadSnapshot,
    Scan, ScanItem, ScanPage, StoreError, StoreLimits, TxnStore, WriteTxn,
};
use nokv_types::{
    CommandDigest, CommitVersion, LogicalShardId, ObjectNamespaceId, OwnerEpoch,
    PlacementGeneration, ReadVersion, RequestId, RootActivationState, RootId,
};
use sha2::{Digest, Sha256};

use super::codec::{
    change_event_key, classify_schema_marker_for_open, encode_schema_marker,
    validate_schema_marker, SCHEMA_ID, SYSTEM_SCHEMA_KEY,
};
use super::keyspace::{
    keyspaces, MetadataFamily, CHANGE_EVENT, COMMAND_DEDUPE, HISTORY, RECOVERY_OUTBOX, ROOT_FENCE,
    SYSTEM,
};
#[cfg(feature = "metadata-read-stats")]
use super::read_stats::{self, MetadataReadStats, MetadataReadStatsSessionError};
use super::records::{CommandDedupeRecord, CurrentValue, HistoryValue, RootFence};
use super::recovery::{
    assemble_recovery_storage, decode_recovery_outbox_key, recovery_chunk_key,
    recovery_chunk_key_for_layout, recovery_genesis_digest, recovery_outbox_key,
    recovery_outbox_scan_start, recovery_segment_record_budget, recovery_storage_chunk_count,
    recovery_storage_logical_length, split_recovery_storage, DecodedRecoveryOutboxKey,
    RecoveryKeyLayout, RecoveryMutationV1, RecoveryOutboxRecord, RecoveryOutboxSegment,
    RecoveryResultV1, RecoveryState, MAX_RECOVERY_BYTES, MAX_RECOVERY_SEGMENT_RECORDS,
    RECOVERY_CHAIN_DIGEST_BYTES,
};

const SYSTEM_SHARD_IDENTITY_KEY: &[u8] = b"shard_identity";
const SYSTEM_OWNER_FENCE_KEY: &[u8] = b"owner_fence";
const SYSTEM_COMMIT_CLOCK_KEY: &[u8] = b"commit_clock";
const SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY: &[u8] = b"lease_clock_high_water";
const SYSTEM_APPLIED_RECOVERY_LSN_KEY: &[u8] = b"applied_recovery_lsn";
const SYSTEM_RECOVERY_CHAIN_DIGEST_KEY: &[u8] = b"recovery_chain_digest";
const SYSTEM_VALUE_FORMAT_VERSION: u8 = 1;
const INITIAL_COMMIT_VERSION: u64 = 1;

const MAX_COMMAND_ITEMS: usize = 256;
const MAX_DELIMITED_SCAN_ITEMS: usize = MAX_COMMAND_ITEMS * 2;
const MAX_HISTORICAL_SCAN_PAGE_ROWS: usize = MAX_COMMAND_ITEMS;
const MAX_HISTORICAL_SCAN_ATTEMPTS: usize = 4;
const HISTORICAL_SCAN_RETRY_DELAYS: [Duration; MAX_HISTORICAL_SCAN_ATTEMPTS - 1] = [
    Duration::from_millis(1),
    Duration::from_millis(2),
    Duration::from_millis(4),
];
const MAX_RECOVERY_SCAN_ATTEMPTS: usize = 4;
const RECOVERY_SCAN_RETRY_DELAYS: [Duration; MAX_RECOVERY_SCAN_ATTEMPTS - 1] = [
    Duration::from_millis(1),
    Duration::from_millis(2),
    Duration::from_millis(4),
];
const MAX_COMMAND_KEY_BYTES: usize = 8 * 1024;
const HISTORY_KEY_OVERHEAD_BYTES: usize =
    1 + std::mem::size_of::<u32>() + std::mem::size_of::<u64>();
const MAX_DERIVED_KEY_BYTES: usize = MAX_COMMAND_KEY_BYTES + HISTORY_KEY_OVERHEAD_BYTES;
const MAX_STORED_VALUE_BYTES: usize = u16::MAX as usize;
// Domain payloads are wrapped in CurrentValue, HistoryValue, or
// CommandDedupeRecord before insertion. Keep explicit headroom for every
// durable envelope without binding the schema to one adapter.
const MAX_RECORD_PAYLOAD_BYTES: usize = 60 * 1024;
const MAX_COMMAND_VALUE_BYTES: usize = MAX_RECORD_PAYLOAD_BYTES;
const MAX_DETERMINISTIC_RESULT_BYTES: usize = MAX_RECORD_PAYLOAD_BYTES;
const MAX_EVENT_BYTES: usize = MAX_RECORD_PAYLOAD_BYTES;

/// Transitional transaction-store limits for the workspace schema.
///
/// The decimal 16,000,000-byte transaction envelope preserves three
/// high-amplification lifecycles characterized against the pre-provider
/// Holt-backed engine: a 60-field, 61,203-byte projection on a short path, and
/// a 4,096-byte path with 64 dependencies and a 57,243-byte projection, plus a
/// successful disjoint 60-field replacement whose fully derived transaction is
/// 9,859,091 bytes at the maximum tested event-union boundary.
/// Together with the configured maximum mutation overhead it remains below
/// Holt's 16 MiB WAL-record ceiling. This is a transitional compatibility
/// profile, not proof that every domain-valid command fits; providers with a
/// smaller hard transaction limit are not qualified for it.
pub const fn store_limits() -> StoreLimits {
    StoreLimits {
        max_reads: 8,
        max_checks: 1_024,
        max_mutations: 1_024,
        max_key_bytes: MAX_DERIVED_KEY_BYTES,
        max_value_bytes: MAX_STORED_VALUE_BYTES,
        max_read_bytes: 8 * 1024 * 1024,
        max_transaction_bytes: 16_000_000,
        max_result_rows: 1_024,
        max_result_bytes: 8 * 1024 * 1024,
    }
}

#[derive(Clone, Copy)]
pub(super) enum MetadataPointReadSource {
    System,
    RootFence,
    WorkspaceCurrent,
    PathCurrent,
    Other,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootFenceAction {
    Install,
    /// One-way upgrade of a pre-namespace root fence. It preserves placement,
    /// activation state, and owner epoch while adding the immutable namespace.
    BindObjectNamespace {
        expected: RootActivationState,
    },
    RequireActive,
    Transition {
        expected: RootActivationState,
        next: RootActivationState,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandPredicate {
    Value {
        family: MetadataFamily,
        key: Vec<u8>,
        expected: Option<Vec<u8>>,
    },
    PrefixEmpty {
        family: MetadataFamily,
        prefix: Vec<u8>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandMutation {
    Put {
        family: MetadataFamily,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        family: MetadataFamily,
        key: Vec<u8>,
    },
}

impl CommandMutation {
    fn family(&self) -> MetadataFamily {
        match self {
            Self::Put { family, .. } | Self::Delete { family, .. } => *family,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HistoryProjection {
    pub family: MetadataFamily,
    pub key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventProjection {
    pub payload: Vec<u8>,
}

/// One bounded, root-scoped durable metadata transaction.
///
/// The caller-visible digest is verified against a canonical encoding before
/// any record is read or mutated. Domain record values are payload bytes; the
/// executor wraps them with their commit version. `read_version` must equal the
/// shard's current commit clock. This exact fence makes the assigned commit
/// version deterministically `read_version + 1`, which domain commands use for
/// zero-reference and retention records without guessing across concurrent
/// writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommand {
    pub schema_id: String,
    pub root_id: RootId,
    pub logical_shard_id: LogicalShardId,
    pub object_namespace_id: Option<ObjectNamespaceId>,
    pub placement_generation: PlacementGeneration,
    pub owner_epoch: OwnerEpoch,
    pub request_id: RequestId,
    pub command_digest: CommandDigest,
    pub read_version: ReadVersion,
    pub root_fence_action: RootFenceAction,
    pub predicates: Vec<CommandPredicate>,
    pub mutations: Vec<CommandMutation>,
    pub history_projection: Vec<HistoryProjection>,
    pub event_projection: Vec<EventProjection>,
    pub deterministic_result: Vec<u8>,
}

impl MetadataCommand {
    pub fn seal(mut self) -> Self {
        self.command_digest = self.canonical_digest();
        self
    }

    pub fn canonical_digest(&self) -> CommandDigest {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.metadata.command.v1\0");
        hash_bytes(&mut hasher, self.schema_id.as_bytes());
        hasher.update(self.root_id.as_bytes());
        hasher.update(self.logical_shard_id.as_bytes());
        if let Some(object_namespace_id) = self.object_namespace_id {
            hasher.update(object_namespace_id.as_bytes());
        }
        hasher.update(self.placement_generation.get().to_be_bytes());
        hasher.update(self.owner_epoch.get().to_be_bytes());
        hasher.update(self.request_id.as_bytes());
        hasher.update(self.read_version.get().to_be_bytes());
        match self.root_fence_action {
            RootFenceAction::Install => hasher.update([1]),
            RootFenceAction::BindObjectNamespace { expected } => {
                hasher.update([4, expected.into()]);
            }
            RootFenceAction::RequireActive => hasher.update([2]),
            RootFenceAction::Transition { expected, next } => {
                hasher.update([3, expected.into(), next.into()]);
            }
        }
        hash_u64(&mut hasher, self.predicates.len());
        for predicate in &self.predicates {
            match predicate {
                CommandPredicate::Value {
                    family,
                    key,
                    expected,
                } => {
                    hasher.update([1, family.format_tag()]);
                    hash_bytes(&mut hasher, key);
                    match expected {
                        Some(value) => {
                            hasher.update([1]);
                            hash_bytes(&mut hasher, value);
                        }
                        None => hasher.update([0]),
                    }
                }
                CommandPredicate::PrefixEmpty { family, prefix } => {
                    hasher.update([2, family.format_tag()]);
                    hash_bytes(&mut hasher, prefix);
                }
            }
        }
        hash_u64(&mut hasher, self.mutations.len());
        for mutation in &self.mutations {
            match mutation {
                CommandMutation::Put { family, key, value } => {
                    hasher.update([1, family.format_tag()]);
                    hash_bytes(&mut hasher, key);
                    hash_bytes(&mut hasher, value);
                }
                CommandMutation::Delete { family, key } => {
                    hasher.update([2, family.format_tag()]);
                    hash_bytes(&mut hasher, key);
                }
            }
        }
        hash_u64(&mut hasher, self.history_projection.len());
        for projection in &self.history_projection {
            hasher.update([projection.family.format_tag()]);
            hash_bytes(&mut hasher, &projection.key);
        }
        hash_u64(&mut hasher, self.event_projection.len());
        for projection in &self.event_projection {
            hash_bytes(&mut hasher, &projection.payload);
        }
        hash_bytes(&mut hasher, &self.deterministic_result);
        CommandDigest::from_bytes(hasher.finalize().into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCommandResult {
    pub commit_version: CommitVersion,
    pub deterministic_result: Vec<u8>,
    /// Exact RecoveryOutbox position committed in the same transaction.
    pub recovery_lsn: u64,
    /// Hash-chain digest at `recovery_lsn`; exact retries return the same value.
    pub recovery_chain_digest: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
    pub replayed: bool,
}

/// Read-only structural consistency result for the unique RecoveryOutbox.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryFsckReport {
    pub state: RecoveryState,
    pub outbox_records: u64,
    pub metadata_command_records: u64,
    pub dedupe_records: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandLimit {
    Checks,
    Mutations,
    KeyBytes,
    ValueBytes,
    TransactionBytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandFit {
    Fits,
    Exceeds {
        kind: CommandLimit,
        actual: usize,
        maximum: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataScanItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum DelimitedMetadataScanItem {
    Record(MetadataScanItem),
    CommonPrefix(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaError {
    Store {
        operation: &'static str,
        source: StoreError,
    },
    Internal {
        operation: &'static str,
        message: String,
    },
    SchemaGate {
        reason: String,
    },
    CorruptRecord {
        record: &'static str,
        reason: String,
    },
    InvalidCommand {
        reason: String,
    },
    CommandDigestMismatch,
    RequestIdReused,
    OwnerEpochMismatch {
        expected: u64,
        actual: u64,
    },
    OwnerEpochNotMonotonic {
        current: u64,
        next: u64,
    },
    PlacementMismatch,
    RootFenceAlreadyInstalled,
    RootFenceMissing,
    RootFenceStateMismatch {
        expected: RootActivationState,
        actual: RootActivationState,
    },
    InvalidRootFenceTransition {
        from: RootActivationState,
        to: RootActivationState,
    },
    ReadVersionInFuture {
        requested: u64,
        current: u64,
    },
    ReadStabilityExhausted {
        attempts: usize,
    },
    WriteReadVersionMismatch {
        requested: u64,
        current: u64,
    },
    LeaseDeadlineReached {
        lease_clock_ms: u64,
        requested_deadline_ms: u64,
    },
    PredicateFailed,
    WriteConflict,
    VersionOverflow,
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store { operation, source } => {
                write!(formatter, "metadata store {operation} failed: {source}")
            }
            Self::Internal { operation, message } => {
                write!(formatter, "metadata {operation} failed: {message}")
            }
            Self::SchemaGate { reason } => write!(formatter, "metadata schema rejected: {reason}"),
            Self::CorruptRecord { record, reason } => {
                write!(formatter, "corrupt {record} record: {reason}")
            }
            Self::InvalidCommand { reason } => {
                write!(formatter, "invalid metadata command: {reason}")
            }
            Self::CommandDigestMismatch => formatter.write_str("metadata command digest mismatch"),
            Self::RequestIdReused => {
                formatter.write_str("request id was already used by a different command")
            }
            Self::OwnerEpochMismatch { expected, actual } => {
                write!(
                    formatter,
                    "owner epoch mismatch: expected {expected}, actual {actual}"
                )
            }
            Self::OwnerEpochNotMonotonic { current, next } => {
                write!(
                    formatter,
                    "owner epoch must advance: current {current}, next {next}"
                )
            }
            Self::PlacementMismatch => formatter.write_str("root placement fence mismatch"),
            Self::RootFenceAlreadyInstalled => formatter.write_str("root fence already installed"),
            Self::RootFenceMissing => formatter.write_str("root fence is missing"),
            Self::RootFenceStateMismatch { expected, actual } => write!(
                formatter,
                "root fence state mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::InvalidRootFenceTransition { from, to } => {
                write!(
                    formatter,
                    "invalid root fence transition {from:?} -> {to:?}"
                )
            }
            Self::ReadVersionInFuture { requested, current } => write!(
                formatter,
                "read version {requested} is newer than current version {current}"
            ),
            Self::ReadStabilityExhausted { attempts } => write!(
                formatter,
                "metadata read could not capture a stable shard commit clock after {attempts} attempts"
            ),
            Self::WriteReadVersionMismatch { requested, current } => write!(
                formatter,
                "metadata write read-version mismatch: requested {requested}, current {current}"
            ),
            Self::LeaseDeadlineReached {
                lease_clock_ms,
                requested_deadline_ms,
            } => write!(
                formatter,
                "lease deadline {requested_deadline_ms} is not newer than lease clock {lease_clock_ms}"
            ),
            Self::PredicateFailed => formatter.write_str("metadata command predicate failed"),
            Self::WriteConflict => formatter.write_str("metadata command lost an atomic race"),
            Self::VersionOverflow => formatter.write_str("metadata commit version overflow"),
        }
    }
}

impl std::error::Error for MetaError {}

#[derive(Clone)]
pub struct MetaShard {
    store: Arc<dyn TxnStore>,
    logical_shard_id: LogicalShardId,
    command_gate: Arc<RwLock<()>>,
    #[cfg(feature = "metadata-read-stats")]
    read_stats_identity: Arc<ReadStatsIdentity>,
}

#[cfg(feature = "metadata-read-stats")]
#[derive(Default)]
struct ReadStatsIdentity {
    metadata_active: AtomicBool,
}

/// Short-lived point reader bound to one validated root and read version.
///
/// The owning `MetaShard` keeps the command gate for the reader's complete
/// lifetime. This type stays inside the workspace package so callers cannot
/// retain the gate across scans, object I/O, or RPC work.
pub(super) struct FencedPointReader<'a> {
    store: &'a MetaShard,
    context: ReadFenceContext,
    version: ReadVersion,
}

#[derive(Clone, Copy)]
struct ReadFenceContext {
    root_id: RootId,
    placement_generation: PlacementGeneration,
    owner_epoch: OwnerEpoch,
}

/// Thread-bound logical metadata read counters.
///
/// This diagnostic API is available only with the `metadata-read-stats`
/// feature. Reads through clones of `store` are included when they execute on
/// the thread that owns the session.
#[cfg(feature = "metadata-read-stats")]
#[must_use = "finish the session to obtain counters, or drop it to cancel collection"]
pub struct MetadataReadStatsSession<'a> {
    store: &'a MetaShard,
    store_key: usize,
    active: bool,
    not_send: PhantomData<Rc<()>>,
}

#[cfg(feature = "metadata-read-stats")]
impl MetadataReadStatsSession<'_> {
    pub fn finish(mut self) -> Result<MetadataReadStats, MetadataReadStatsSessionError> {
        let result = read_stats::finish_session(self.store_key);
        self.release();
        result
    }

    fn release(&mut self) {
        self.active = false;
        self.store
            .read_stats_identity
            .metadata_active
            .store(false, Ordering::Release);
    }
}

#[cfg(feature = "metadata-read-stats")]
impl Drop for MetadataReadStatsSession<'_> {
    fn drop(&mut self) {
        if self.active {
            read_stats::cancel_session(self.store_key);
            self.release();
        }
    }
}

impl FencedPointReader<'_> {
    pub(super) fn get(
        &self,
        family: MetadataFamily,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, MetaError> {
        validate_root_scoped_bytes(self.context.root_id, key, "point-read key")?;
        self.store
            .read_at_fence(family, key, self.version, self.context)
    }
}

impl MetaShard {
    /// Initialize the workspace schema in one empty physical store.
    pub fn initialize(
        store: Arc<dyn TxnStore>,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, MetaError> {
        let shard = Self::bind(store, logical_shard_id)?;
        let mut txn = WriteTxn {
            checks: keyspaces()
                .iter()
                .map(|definition| Check::EmptyPrefix {
                    keyspace: definition.id,
                    prefix: Vec::new(),
                })
                .collect(),
            mutations: Vec::new(),
        };
        for (key, value) in system_bootstrap_rows(logical_shard_id) {
            txn.checks.push(Check::Absent {
                key: Key::new(SYSTEM.id, key),
            });
            txn.mutations.push(Mutation::Put {
                key: Key::new(SYSTEM.id, key),
                value,
            });
        }
        match shard.commit("initialize system records", txn)? {
            Commit::Applied => shard.open_domain(),
            Commit::Conflict => Err(MetaError::SchemaGate {
                reason: "fresh metadata store is not empty".to_owned(),
            }),
        }
    }

    /// Open and validate an existing workspace schema.
    pub fn open(
        store: Arc<dyn TxnStore>,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, MetaError> {
        Self::bind(store, logical_shard_id)?.open_domain()
    }

    fn bind(store: Arc<dyn TxnStore>, logical_shard_id: LogicalShardId) -> Result<Self, MetaError> {
        validate_store_profile(store.profile().limits)?;
        store
            .ready()
            .map_err(|source| store_error("readiness check", source))?;
        Ok(Self {
            store,
            logical_shard_id,
            command_gate: Arc::new(RwLock::new(())),
            #[cfg(feature = "metadata-read-stats")]
            read_stats_identity: Arc::new(ReadStatsIdentity::default()),
        })
    }

    fn open_domain(self) -> Result<Self, MetaError> {
        let schema = self.required_value(SYSTEM.id, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        classify_schema_marker_for_open(&schema).map_err(|error| MetaError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = self.required_value(
            SYSTEM.id,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard)? != self.logical_shard_id {
            return Err(MetaError::SchemaGate {
                reason: "logical shard identity does not match requested store".to_owned(),
            });
        }
        decode_system_u64(
            &self.required_value(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?,
            "System(owner_fence)",
        )?;
        decode_system_u64(
            &self.required_value(
                SYSTEM.id,
                SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                "System(applied_recovery_lsn)",
            )?,
            "System(applied_recovery_lsn)",
        )?;
        decode_system_digest(
            &self.required_value(
                SYSTEM.id,
                SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
                "System(recovery_chain_digest)",
            )?,
            "System(recovery_chain_digest)",
        )?;
        let clock = decode_system_u64(
            &self.required_value(SYSTEM.id, SYSTEM_COMMIT_CLOCK_KEY, "System(commit_clock)")?,
            "System(commit_clock)",
        )?;
        CommitVersion::new(clock).map_err(|error| corrupt("System(commit_clock)", error))?;
        decode_system_u64(
            &self.required_value(
                SYSTEM.id,
                SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                "System(lease_clock_high_water)",
            )?,
            "System(lease_clock_high_water)",
        )?;
        self.verify_recovery_chain_unlocked()?;
        Ok(self)
    }

    pub fn current_read_version(&self) -> Result<ReadVersion, MetaError> {
        let value =
            self.required_value(SYSTEM.id, SYSTEM_COMMIT_CLOCK_KEY, "System(commit_clock)")?;
        let value = decode_system_u64(&value, "System(commit_clock)")?;
        ReadVersion::new(value).map_err(|error| corrupt("System(commit_clock)", error))
    }

    /// Return the logical shard identity sealed into this store.
    pub fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    /// Start an explicit metadata read-statistics diagnostic session.
    ///
    /// Only one session may be active on a thread. The returned guard is not
    /// sendable and must be finished on that thread. Session setup and finish
    /// can be placed outside a benchmark's timed interval.
    #[cfg(feature = "metadata-read-stats")]
    pub fn begin_read_stats_session(
        &self,
    ) -> Result<MetadataReadStatsSession<'_>, MetadataReadStatsSessionError> {
        if self
            .read_stats_identity
            .metadata_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(MetadataReadStatsSessionError::StoreSessionAlreadyActive);
        }
        let store_key = self.read_stats_store_key();
        if let Err(error) = read_stats::begin_session(store_key) {
            self.read_stats_identity
                .metadata_active
                .store(false, Ordering::Release);
            return Err(error);
        }
        Ok(MetadataReadStatsSession {
            store: self,
            store_key,
            active: true,
            not_send: PhantomData,
        })
    }

    /// Return the persisted physical-owner epoch. `None` is the fresh epoch-zero
    /// sentinel before the first owner is admitted.
    pub fn current_owner_epoch(&self) -> Result<Option<OwnerEpoch>, MetaError> {
        let value =
            self.required_value(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let value = decode_system_u64(&value, "System(owner_fence)")?;
        if value == 0 {
            Ok(None)
        } else {
            OwnerEpoch::new(value)
                .map(Some)
                .map_err(|error| corrupt("System(owner_fence)", error))
        }
    }

    /// Inspect one shard-local root fence during owner bootstrap.
    ///
    /// Ordinary reads and writes must still use the fenced context APIs.
    pub fn root_fence(&self, root_id: RootId) -> Result<Option<RootFence>, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        self.read_value(
            ROOT_FENCE.id,
            root_id.as_bytes(),
            MetadataPointReadSource::RootFence,
            "read RootFence",
        )?
        .map(|value| RootFence::decode(&value).map_err(|error| corrupt("RootFence", error)))
        .transpose()
    }

    /// Return the persisted monotonic lease clock used by snapshot expiry.
    pub fn lease_clock_high_water(&self) -> Result<u64, MetaError> {
        let value = self.required_value(
            SYSTEM.id,
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            "System(lease_clock_high_water)",
        )?;
        decode_system_u64(&value, "System(lease_clock_high_water)")
    }

    /// Return the durable recovery tail atomically serialized with writes.
    pub fn recovery_state(&self) -> Result<RecoveryState, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        self.recovery_state_unlocked()
    }

    /// Read strictly ordered recovery rows after `start_after_lsn`.
    pub fn recovery_outbox_after(
        &self,
        start_after_lsn: u64,
        limit: usize,
    ) -> Result<Vec<RecoveryOutboxRecord>, MetaError> {
        self.recovery_outbox_after_with_byte_budget(start_after_lsn, limit, MAX_RECOVERY_BYTES)
    }

    #[cfg(test)]
    pub(crate) fn replace_recovery_header_for_test(
        &self,
        recovery_lsn: u64,
        replacement: Option<Vec<u8>>,
    ) -> Result<(), MetaError> {
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| internal("lock command gate", error))?;
        let key = Key::new(RECOVERY_OUTBOX.id, recovery_outbox_key(recovery_lsn));
        let mutation = match replacement {
            Some(value) => Mutation::Put { key, value },
            None => Mutation::Delete { key },
        };
        match self.commit(
            "inject RecoveryOutbox test fault",
            WriteTxn {
                checks: Vec::new(),
                mutations: vec![mutation],
            },
        )? {
            Commit::Applied => Ok(()),
            Commit::Conflict => Err(MetaError::WriteConflict),
        }
    }

    fn recovery_outbox_after_with_byte_budget(
        &self,
        start_after_lsn: u64,
        limit: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<RecoveryOutboxRecord>, MetaError> {
        const MAX_RECOVERY_PAGE_ROWS: usize = 1024;
        if limit == 0 || limit > MAX_RECOVERY_PAGE_ROWS {
            return Err(invalid(format!(
                "recovery outbox limit must be in 1..={MAX_RECOVERY_PAGE_ROWS}"
            )));
        }
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        self.recovery_outbox_after_unlocked(start_after_lsn, limit, max_encoded_bytes)
    }

    fn recovery_outbox_after_unlocked(
        &self,
        start_after_lsn: u64,
        limit: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<RecoveryOutboxRecord>, MetaError> {
        let mut rows = Vec::with_capacity(limit);
        let mut encoded_bytes = 0_usize;
        for layout in RecoveryKeyLayout::ORDERED {
            let mut after = Some(recovery_outbox_scan_start(layout, start_after_lsn));
            loop {
                let page = self.scan_page(
                    RECOVERY_OUTBOX.id,
                    &[layout.header_tag()],
                    after.as_deref(),
                    (limit - rows.len()).min(MAX_HISTORICAL_SCAN_PAGE_ROWS),
                    None,
                    0,
                    "scan RecoveryOutbox",
                )?;
                let more = page.more;
                for item in page.items {
                    let ScanItem::Row { key, value } = item else {
                        return Err(corrupt(
                            "RecoveryOutbox",
                            "non-delimited scan returned a common prefix",
                        ));
                    };
                    after = Some(key.clone());
                    let decoded = decode_recovery_outbox_key(&key)
                        .map_err(|error| corrupt("RecoveryOutbox key", error))?;
                    if decoded.layout != layout {
                        return Err(corrupt(
                            "RecoveryOutbox key",
                            "header tag and decoded layout disagree",
                        ));
                    }
                    let row_encoded_bytes = recovery_storage_logical_length(&value)
                        .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
                    if !rows.is_empty()
                        && encoded_bytes.saturating_add(row_encoded_bytes) > max_encoded_bytes
                    {
                        return Ok(rows);
                    }
                    let row = self.read_recovery_record(decoded, &value)?;
                    if row.recovery_lsn != decoded.recovery_lsn {
                        return Err(MetaError::CorruptRecord {
                            record: "RecoveryOutbox",
                            reason: "row LSN does not match ordered key".to_owned(),
                        });
                    }
                    encoded_bytes = encoded_bytes.saturating_add(row_encoded_bytes);
                    rows.push(row);
                    if rows.len() == limit {
                        return Ok(rows);
                    }
                }
                if !more {
                    break;
                }
            }
        }
        Ok(rows)
    }

    /// Verify every durable recovery row and the `System` tail.
    pub fn verify_recovery_chain(&self) -> Result<RecoveryState, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        self.verify_recovery_chain_unlocked()
    }

    /// Seal one bounded canonical segment strictly after an exact local tail.
    pub fn recovery_segment_after(
        &self,
        boundary: RecoveryState,
        limit: usize,
    ) -> Result<Option<RecoveryOutboxSegment>, MetaError> {
        if limit == 0 || limit > MAX_RECOVERY_SEGMENT_RECORDS {
            return Err(invalid(format!(
                "recovery segment limit must be in 1..={MAX_RECOVERY_SEGMENT_RECORDS}"
            )));
        }
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        let current = self.recovery_state_unlocked()?;
        if boundary.applied_recovery_lsn > current.applied_recovery_lsn {
            return Err(corrupt(
                "RecoveryOutbox segment boundary",
                format!(
                    "boundary LSN {} is newer than local tail {}",
                    boundary.applied_recovery_lsn, current.applied_recovery_lsn
                ),
            ));
        }
        let expected_boundary_digest = if boundary.applied_recovery_lsn == 0 {
            recovery_genesis_digest(self.logical_shard_id)
        } else {
            self.recovery_record_at_unlocked(boundary.applied_recovery_lsn)?
                .chain_digest
        };
        if boundary.chain_digest != expected_boundary_digest {
            return Err(corrupt(
                "RecoveryOutbox segment boundary",
                "boundary digest does not match its local LSN",
            ));
        }
        if boundary == current {
            return Ok(None);
        }
        let rows = self.recovery_outbox_after_unlocked(
            boundary.applied_recovery_lsn,
            limit,
            recovery_segment_record_budget(limit),
        )?;
        if rows.is_empty() {
            return Err(corrupt(
                "RecoveryOutbox segment",
                "local tail is newer than boundary but no following row exists",
            ));
        }
        let segment = RecoveryOutboxSegment::seal(self.logical_shard_id, rows)
            .map_err(|error| corrupt("RecoveryOutbox segment", error))?;
        segment
            .verify_follows(boundary)
            .map_err(|error| corrupt("RecoveryOutbox segment", error))?;
        Ok(Some(segment))
    }

    /// Replay one exact outbox row through the ordinary authoritative write
    /// entrypoint. Existing identical rows are idempotent; a gap fails before
    /// any mutation is attempted.
    pub fn replay_recovery_record(
        &self,
        record: &RecoveryOutboxRecord,
    ) -> Result<RecoveryState, MetaError> {
        record
            .verify()
            .map_err(|error| corrupt("RecoveryOutbox replay", error))?;
        if let RecoveryMutationV1::MetadataCommand { command, .. } = &record.mutation {
            if command.logical_shard_id != self.logical_shard_id {
                return Err(corrupt(
                    "RecoveryOutbox replay",
                    "metadata command targets a different logical shard",
                ));
            }
        }
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| internal("lock command gate", error))?;
        self.replay_recovery_record_unlocked(record)
    }

    /// Replay one fully validated segment after checking its entire overlap
    /// with the target before applying the first missing row.
    pub fn replay_recovery_segment(
        &self,
        segment: &RecoveryOutboxSegment,
    ) -> Result<RecoveryState, MetaError> {
        segment
            .verify()
            .map_err(|error| corrupt("RecoveryOutbox replay", error))?;
        if segment.logical_shard_id != self.logical_shard_id {
            return Err(corrupt(
                "RecoveryOutbox replay",
                "segment targets a different logical shard",
            ));
        }
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| internal("lock command gate", error))?;
        let before = self.recovery_state_unlocked()?;
        if before.applied_recovery_lsn < segment.first_lsn {
            segment
                .verify_follows(before)
                .map_err(|error| corrupt("RecoveryOutbox replay", error))?;
        } else {
            for record in segment
                .records
                .iter()
                .take_while(|record| record.recovery_lsn <= before.applied_recovery_lsn)
            {
                let local = self.recovery_record_at_unlocked(record.recovery_lsn)?;
                if &local != record {
                    return Err(corrupt(
                        "RecoveryOutbox replay",
                        format!("target diverges at overlapping LSN {}", record.recovery_lsn),
                    ));
                }
            }
            if before.applied_recovery_lsn < segment.last_lsn {
                let overlap = self.recovery_record_at_unlocked(before.applied_recovery_lsn)?;
                if overlap.chain_digest != before.chain_digest {
                    return Err(corrupt(
                        "RecoveryOutbox replay",
                        "target tail digest disagrees with its overlapping row",
                    ));
                }
            }
        }
        for record in &segment.records {
            if record.recovery_lsn > before.applied_recovery_lsn {
                self.replay_recovery_record_unlocked(record)?;
            }
        }
        self.recovery_state_unlocked()
    }

    /// Verify the full recovery head/chain and the bidirectional binding
    /// between every metadata-command row and its exact dedupe result. This is
    /// diagnostic only and never repairs, skips, or guesses a missing row.
    pub fn fsck_recovery(&self) -> Result<RecoveryFsckReport, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        self.with_stable_recovery_scan(|state| self.fsck_recovery_at_state_unlocked(state))
    }

    fn fsck_recovery_at_state_unlocked(
        &self,
        state: RecoveryState,
    ) -> Result<RecoveryFsckReport, MetaError> {
        self.verify_recovery_chain_at_state_unlocked(state)?;
        let mut outbox_records = 0_u64;
        let mut metadata_command_records = 0_u64;
        let mut after_lsn = 0_u64;
        while after_lsn < state.applied_recovery_lsn {
            let rows = self.recovery_outbox_after_unlocked(
                after_lsn,
                MAX_HISTORICAL_SCAN_PAGE_ROWS,
                MAX_RECOVERY_BYTES,
            )?;
            if rows.is_empty() {
                return Err(corrupt(
                    "RecoveryOutbox fsck",
                    format!("chain stopped after LSN {after_lsn}"),
                ));
            }
            for row in &rows {
                outbox_records = outbox_records
                    .checked_add(1)
                    .ok_or(MetaError::VersionOverflow)?;
                if let RecoveryMutationV1::MetadataCommand { command, .. } = &row.mutation {
                    let key = command_dedupe_key(command.root_id, command.request_id);
                    let value = self
                        .read_value(
                            COMMAND_DEDUPE.id,
                            &key,
                            MetadataPointReadSource::Other,
                            "fsck CommandDedupe",
                        )?
                        .ok_or_else(|| {
                            corrupt(
                                "CommandDedupe recovery binding",
                                format!("missing dedupe result for LSN {}", row.recovery_lsn),
                            )
                        })?;
                    let dedupe = CommandDedupeRecord::decode(&value)
                        .map_err(|error| corrupt("CommandDedupe", error))?;
                    let bound = self.validate_dedupe_recovery_binding(&key, &dedupe)?;
                    if &bound != row {
                        return Err(corrupt(
                            "CommandDedupe recovery binding",
                            format!("dedupe points away from LSN {}", row.recovery_lsn),
                        ));
                    }
                    metadata_command_records = metadata_command_records
                        .checked_add(1)
                        .ok_or(MetaError::VersionOverflow)?;
                }
            }
            after_lsn = rows
                .last()
                .expect("non-empty recovery fsck page")
                .recovery_lsn;
        }

        let mut dedupe_records = 0_u64;
        let mut after = None;
        loop {
            let page = self.scan_page(
                COMMAND_DEDUPE.id,
                &[],
                after.as_deref(),
                MAX_HISTORICAL_SCAN_PAGE_ROWS,
                None,
                0,
                "fsck CommandDedupe",
            )?;
            let more = page.more;
            for item in page.items {
                let ScanItem::Row { key, value } = item else {
                    return Err(corrupt(
                        "CommandDedupe",
                        "non-delimited scan returned a common prefix",
                    ));
                };
                after = Some(key.clone());
                let dedupe = CommandDedupeRecord::decode(&value)
                    .map_err(|error| corrupt("CommandDedupe", error))?;
                self.validate_dedupe_recovery_binding(&key, &dedupe)?;
                dedupe_records = dedupe_records
                    .checked_add(1)
                    .ok_or(MetaError::VersionOverflow)?;
            }
            if !more {
                break;
            }
        }
        if outbox_records != state.applied_recovery_lsn
            || metadata_command_records != dedupe_records
        {
            return Err(corrupt(
                "CommandDedupe recovery binding",
                format!(
                    "outbox rows {outbox_records}, metadata commands {metadata_command_records}, dedupe rows {dedupe_records}"
                ),
            ));
        }
        Ok(RecoveryFsckReport {
            state,
            outbox_records,
            metadata_command_records,
            dedupe_records,
        })
    }

    fn replay_recovery_record_unlocked(
        &self,
        record: &RecoveryOutboxRecord,
    ) -> Result<RecoveryState, MetaError> {
        let before = self.recovery_state_unlocked()?;
        if before.applied_recovery_lsn >= record.recovery_lsn {
            let local = self.recovery_record_at_unlocked(record.recovery_lsn)?;
            if &local != record {
                return Err(corrupt(
                    "RecoveryOutbox replay",
                    format!("target diverges at existing LSN {}", record.recovery_lsn),
                ));
            }
            return Ok(before);
        }
        let expected_lsn = before
            .applied_recovery_lsn
            .checked_add(1)
            .ok_or(MetaError::VersionOverflow)?;
        if record.recovery_lsn != expected_lsn {
            return Err(corrupt(
                "RecoveryOutbox replay",
                format!(
                    "expected next LSN {expected_lsn}, found {}",
                    record.recovery_lsn
                ),
            ));
        }
        if record.previous_chain_digest != before.chain_digest {
            return Err(corrupt(
                "RecoveryOutbox replay",
                format!("LSN {expected_lsn} does not follow the target chain digest"),
            ));
        }

        match (&record.mutation, &record.result) {
            (
                RecoveryMutationV1::MetadataCommand {
                    command,
                    lease_deadline_ms,
                },
                RecoveryResultV1::MetadataCommand {
                    commit_version,
                    deterministic_result,
                },
            ) => {
                let result =
                    self.execute_with_lease_deadline_unlocked(command, *lease_deadline_ms)?;
                if &result.commit_version != commit_version
                    || &result.deterministic_result != deterministic_result
                    || result.recovery_lsn != record.recovery_lsn
                    || result.recovery_chain_digest != record.chain_digest
                {
                    return Err(corrupt(
                        "RecoveryOutbox replay",
                        format!("metadata result changed at LSN {}", record.recovery_lsn),
                    ));
                }
            }
            (
                RecoveryMutationV1::ObserveLeaseClock {
                    root_id,
                    placement_generation,
                    owner_epoch,
                    observed_ms,
                },
                RecoveryResultV1::LeaseClock {
                    effective_high_water_ms,
                },
            ) => {
                let effective = self.observe_lease_clock_unlocked(
                    *root_id,
                    *placement_generation,
                    *owner_epoch,
                    *observed_ms,
                )?;
                if &effective != effective_high_water_ms {
                    return Err(corrupt(
                        "RecoveryOutbox replay",
                        format!("lease-clock result changed at LSN {}", record.recovery_lsn),
                    ));
                }
            }
            (
                RecoveryMutationV1::AdvanceOwnerEpoch { expected, next },
                RecoveryResultV1::OwnerEpoch {
                    applied_owner_epoch,
                },
            ) => {
                self.advance_owner_epoch_unlocked(*expected, *next)?;
                if next != applied_owner_epoch {
                    return Err(corrupt(
                        "RecoveryOutbox replay",
                        format!("owner result changed at LSN {}", record.recovery_lsn),
                    ));
                }
            }
            _ => {
                return Err(corrupt(
                    "RecoveryOutbox replay",
                    "mutation and deterministic result variants differ",
                ))
            }
        }
        let after = self.recovery_state_unlocked()?;
        let expected = RecoveryState {
            applied_recovery_lsn: record.recovery_lsn,
            chain_digest: record.chain_digest,
        };
        if after != expected {
            return Err(corrupt(
                "RecoveryOutbox replay",
                format!("authoritative write produced a different tail at LSN {expected_lsn}"),
            ));
        }
        Ok(after)
    }

    /// Persist a monotonic lease-clock observation for the current owner.
    ///
    /// Wall-clock regression never moves the durable value backwards. The
    /// returned value is the effective high-water after this observation.
    pub fn observe_lease_clock(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        observed_ms: u64,
    ) -> Result<u64, MetaError> {
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| internal("lock command gate", error))?;
        self.observe_lease_clock_unlocked(root_id, placement_generation, owner_epoch, observed_ms)
    }

    fn observe_lease_clock_unlocked(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        observed_ms: u64,
    ) -> Result<u64, MetaError> {
        let schema = self.required_value(SYSTEM.id, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema).map_err(|error| MetaError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = self.required_value(
            SYSTEM.id,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard)? != self.logical_shard_id {
            return Err(MetaError::PlacementMismatch);
        }
        let owner =
            self.required_value(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let actual_owner = decode_system_u64(&owner, "System(owner_fence)")?;
        if actual_owner != owner_epoch.get() {
            return Err(MetaError::OwnerEpochMismatch {
                expected: owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let root_fence = self
            .read_value(
                ROOT_FENCE.id,
                root_id.as_bytes(),
                MetadataPointReadSource::RootFence,
                "read RootFence",
            )?
            .ok_or(MetaError::RootFenceMissing)?;
        let fence = RootFence::decode(&root_fence).map_err(|error| corrupt("RootFence", error))?;
        if fence.logical_shard_id != self.logical_shard_id
            || fence.placement_generation != placement_generation
        {
            return Err(MetaError::PlacementMismatch);
        }
        if fence.activation_state != RootActivationState::Active {
            return Err(MetaError::RootFenceStateMismatch {
                expected: RootActivationState::Active,
                actual: fence.activation_state,
            });
        }
        let clock = self.required_value(
            SYSTEM.id,
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            "System(lease_clock_high_water)",
        )?;
        let current = decode_system_u64(&clock, "System(lease_clock_high_water)")?;
        if observed_ms <= current {
            return Ok(current);
        }
        let recovery = self.plan_recovery(
            RecoveryMutationV1::ObserveLeaseClock {
                root_id,
                placement_generation,
                owner_epoch,
                observed_ms,
            },
            RecoveryResultV1::LeaseClock {
                effective_high_water_ms: observed_ms,
            },
        )?;
        let mut txn = WriteTxn {
            checks: vec![
                value_check(SYSTEM.id, SYSTEM_SCHEMA_KEY, &schema),
                value_check(SYSTEM.id, SYSTEM_SHARD_IDENTITY_KEY, &shard),
                value_check(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, &owner),
                value_check(ROOT_FENCE.id, root_id.as_bytes(), &root_fence),
                value_check(SYSTEM.id, SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY, &clock),
            ],
            mutations: vec![Mutation::Put {
                key: Key::new(SYSTEM.id, SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY),
                value: encode_system_u64(observed_ms).to_vec(),
            }],
        };
        enqueue_recovery(&mut txn, &recovery);
        match self.commit("advance lease clock", txn)? {
            Commit::Applied => Ok(observed_ms),
            Commit::Conflict => Err(MetaError::WriteConflict),
        }
    }

    /// Advance the durable physical-owner epoch against one exact predecessor.
    ///
    /// `None` names the bootstrap epoch zero. An exact retry of an already
    /// applied advancement succeeds; any other stale or non-monotonic request
    /// fails closed.
    pub fn advance_owner_epoch(
        &self,
        expected: Option<OwnerEpoch>,
        next: OwnerEpoch,
    ) -> Result<(), MetaError> {
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| internal("lock command gate", error))?;
        self.advance_owner_epoch_unlocked(expected, next)
    }

    fn advance_owner_epoch_unlocked(
        &self,
        expected: Option<OwnerEpoch>,
        next: OwnerEpoch,
    ) -> Result<(), MetaError> {
        let expected_raw = expected.map(OwnerEpoch::get).unwrap_or(0);
        let schema = self.required_value(SYSTEM.id, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema).map_err(|error| MetaError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = self.required_value(
            SYSTEM.id,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard)? != self.logical_shard_id {
            return Err(MetaError::PlacementMismatch);
        }
        let owner =
            self.required_value(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let current = decode_system_u64(&owner, "System(owner_fence)")?;
        if current == next.get() {
            return Ok(());
        }
        if current != expected_raw {
            return Err(MetaError::OwnerEpochMismatch {
                expected: expected_raw,
                actual: current,
            });
        }
        if next.get() <= current {
            return Err(MetaError::OwnerEpochNotMonotonic {
                current,
                next: next.get(),
            });
        }
        let recovery = self.plan_recovery(
            RecoveryMutationV1::AdvanceOwnerEpoch { expected, next },
            RecoveryResultV1::OwnerEpoch {
                applied_owner_epoch: next,
            },
        )?;
        let mut txn = WriteTxn {
            checks: vec![
                value_check(SYSTEM.id, SYSTEM_SCHEMA_KEY, &schema),
                value_check(SYSTEM.id, SYSTEM_SHARD_IDENTITY_KEY, &shard),
                value_check(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, &owner),
            ],
            mutations: vec![Mutation::Put {
                key: Key::new(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY),
                value: encode_system_u64(next.get()).to_vec(),
            }],
        };
        enqueue_recovery(&mut txn, &recovery);
        match self.commit("advance owner epoch", txn)? {
            Commit::Applied => Ok(()),
            Commit::Conflict => Err(MetaError::WriteConflict),
        }
    }

    pub fn execute(&self, command: &MetadataCommand) -> Result<MetadataCommandResult, MetaError> {
        self.execute_with_lease_deadline(command, None)
    }

    pub(crate) fn execute_before_lease_deadline(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: u64,
    ) -> Result<MetadataCommandResult, MetaError> {
        self.execute_with_lease_deadline(command, Some(lease_deadline_ms))
    }

    /// Check whether a fresh execution of `command` fits the serving write
    /// envelope without reading or mutating the transaction store.
    ///
    /// This check covers the fully derived transaction, including history,
    /// dedupe, and recovery-outbox rows. It does not check current predicates,
    /// fences, the commit clock, or an existing dedupe result.
    pub(crate) fn command_fit(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: Option<u64>,
    ) -> Result<CommandFit, MetaError> {
        let txn = self.command_txn_for_fit(command, lease_deadline_ms)?;
        match txn.validate(&store_limits()) {
            Ok(()) => Ok(CommandFit::Fits),
            Err(StoreError::LimitExceeded {
                kind,
                actual,
                maximum,
            }) => Ok(CommandFit::Exceeds {
                kind: command_limit(kind)?,
                actual,
                maximum,
            }),
            Err(error) => Err(internal("derive command fit", error)),
        }
    }

    fn command_txn_for_fit(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: Option<u64>,
    ) -> Result<WriteTxn, MetaError> {
        self.validate_command(command)?;
        if command.command_digest != command.canonical_digest() {
            return Err(MetaError::CommandDigestMismatch);
        }
        let next_version = command
            .read_version
            .get()
            .checked_add(1)
            .and_then(|version| CommitVersion::new(version).ok())
            .ok_or(MetaError::VersionOverflow)?;
        let predicate_plan = synthetic_predicate_plan(command)?;
        self.validate_history_projection(command, &predicate_plan)?;
        let root_plan = synthetic_root_fence_plan(command)?;
        let recovery = build_recovery_plan(
            encode_system_u64(0).to_vec(),
            encode_system_digest(recovery_genesis_digest(command.logical_shard_id)),
            RecoveryMutationV1::MetadataCommand {
                command: Box::new(command.clone()),
                lease_deadline_ms,
            },
            RecoveryResultV1::MetadataCommand {
                commit_version: next_version,
                deterministic_result: command.deterministic_result.clone(),
            },
        )?;
        let state = CommandTxnState {
            next_version,
            schema: encode_schema_marker(),
            shard: encode_shard_identity(command.logical_shard_id),
            owner: encode_system_u64(command.owner_epoch.get()).to_vec(),
            clock: encode_system_u64(command.read_version.get()).to_vec(),
            lease_clock: lease_deadline_ms.map(|_| encode_system_u64(0).to_vec()),
            root_plan,
            predicate_plan,
            recovery,
        };
        build_command_txn(command, &state)
    }

    fn execute_with_lease_deadline(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: Option<u64>,
    ) -> Result<MetadataCommandResult, MetaError> {
        // Validation and canonical hashing depend only on caller-owned command
        // bytes. Keep that potentially non-trivial work outside the shard-wide
        // sequencing window; the guarded section below is reserved for reads
        // and writes that must observe one commit-clock / recovery-chain state.
        self.validate_command(command)?;
        if command.command_digest != command.canonical_digest() {
            return Err(MetaError::CommandDigestMismatch);
        }
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| internal("lock command gate", error))?;
        self.execute_with_lease_deadline_unlocked(command, lease_deadline_ms)
    }

    fn execute_with_lease_deadline_unlocked(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: Option<u64>,
    ) -> Result<MetadataCommandResult, MetaError> {
        let dedupe_key = command_dedupe_key(command.root_id, command.request_id);
        if let Some(result) = self.replayed_result(&dedupe_key, command.command_digest)? {
            return Ok(result);
        }

        let schema = self.required_value(SYSTEM.id, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema).map_err(|error| MetaError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = self.required_value(
            SYSTEM.id,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard)? != command.logical_shard_id {
            return Err(MetaError::PlacementMismatch);
        }
        let owner =
            self.required_value(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let actual_owner = decode_system_u64(&owner, "System(owner_fence)")?;
        if actual_owner != command.owner_epoch.get() {
            return Err(MetaError::OwnerEpochMismatch {
                expected: command.owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let clock =
            self.required_value(SYSTEM.id, SYSTEM_COMMIT_CLOCK_KEY, "System(commit_clock)")?;
        let current_version = decode_system_u64(&clock, "System(commit_clock)")?;
        if command.read_version.get() != current_version {
            return Err(MetaError::WriteReadVersionMismatch {
                requested: command.read_version.get(),
                current: current_version,
            });
        }
        let next_version_raw = current_version
            .checked_add(1)
            .ok_or(MetaError::VersionOverflow)?;
        let next_version =
            CommitVersion::new(next_version_raw).map_err(|_| MetaError::VersionOverflow)?;

        let root_plan = self.plan_root_fence(command)?;
        let lease_clock = lease_deadline_ms
            .map(|requested_deadline_ms| {
                let clock = self.required_value(
                    SYSTEM.id,
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    "System(lease_clock_high_water)",
                )?;
                let lease_clock_ms = decode_system_u64(&clock, "System(lease_clock_high_water)")?;
                if requested_deadline_ms <= lease_clock_ms {
                    return Err(MetaError::LeaseDeadlineReached {
                        lease_clock_ms,
                        requested_deadline_ms,
                    });
                }
                Ok(clock)
            })
            .transpose()?;
        let predicate_plan = self.plan_predicates(command)?;
        self.validate_history_projection(command, &predicate_plan)?;

        let recovery = self.plan_recovery(
            RecoveryMutationV1::MetadataCommand {
                command: Box::new(command.clone()),
                lease_deadline_ms,
            },
            RecoveryResultV1::MetadataCommand {
                commit_version: next_version,
                deterministic_result: command.deterministic_result.clone(),
            },
        )?;
        let state = CommandTxnState {
            next_version,
            schema,
            shard,
            owner,
            clock,
            lease_clock,
            root_plan,
            predicate_plan,
            recovery,
        };
        let txn = build_command_txn(command, &state)?;

        match self.commit("execute metadata command", txn)? {
            Commit::Applied => Ok(MetadataCommandResult {
                commit_version: next_version,
                deterministic_result: command.deterministic_result.clone(),
                recovery_lsn: state.recovery.row.recovery_lsn,
                recovery_chain_digest: state.recovery.row.chain_digest,
                replayed: false,
            }),
            Commit::Conflict => {
                if let Some(result) = self.replayed_result(&dedupe_key, command.command_digest)? {
                    Ok(result)
                } else {
                    Err(MetaError::WriteConflict)
                }
            }
        }
    }

    pub fn read_at(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        key: &[u8],
        version: ReadVersion,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        validate_root_scoped_bytes(root_id, key, "read key")?;
        self.read_at_fence(
            family,
            key,
            version,
            ReadFenceContext {
                root_id,
                placement_generation,
                owner_epoch,
            },
        )
    }

    /// Run dependent point reads at one captured logical version.
    ///
    /// `requested_version = None` captures the current version inside the
    /// guarded window. A supplied historical version is rejected if it is
    /// newer than the same captured current version. Every callback point read
    /// revalidates owner, root, and commit-clock records in the same store
    /// snapshot as its data row. The callback must remain limited to metadata
    /// point reads and local decoding.
    pub(super) fn with_fenced_point_reads<R, E>(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        requested_version: Option<ReadVersion>,
        read: impl FnOnce(ReadVersion, &FencedPointReader<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<MetaError>,
    {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| E::from(internal("lock read gate", error)))?;
        let current_version = self
            .validate_read_context(root_id, placement_generation, owner_epoch)
            .map_err(E::from)?;
        let version = requested_version.unwrap_or(current_version);
        if version > current_version {
            return Err(E::from(MetaError::ReadVersionInFuture {
                requested: version.get(),
                current: current_version.get(),
            }));
        }
        let reader = FencedPointReader {
            store: self,
            context: ReadFenceContext {
                root_id,
                placement_generation,
                owner_epoch,
            },
            version,
        };
        read(version, &reader)
    }

    /// Read one exact request replay record through the same ownership fences
    /// as ordinary metadata reads.
    ///
    /// Domain services use the stored deterministic result to short-circuit a
    /// response-loss retry before re-reading mutable preconditions. The domain
    /// result must retain and verify its own stable input digest.
    pub fn lookup_request(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        request_id: RequestId,
    ) -> Result<Option<CommandDedupeRecord>, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        let key = command_dedupe_key(root_id, request_id);
        let (_, value) = self.read_value_at_fence(
            COMMAND_DEDUPE.id,
            &key,
            MetadataPointReadSource::Other,
            "read CommandDedupe",
            ReadFenceContext {
                root_id,
                placement_generation,
                owner_epoch,
            },
        )?;
        value
            .map(|value| {
                CommandDedupeRecord::decode(&value).map_err(|error| corrupt("CommandDedupe", error))
            })
            .transpose()
    }

    /// Return one exact replay result together with its authoritative recovery
    /// receipt. Unlike the raw dedupe inspection API, this verifies that the
    /// dedupe payload, request identity, LSN, and RecoveryOutbox result are one
    /// closed binding before returning it.
    pub fn lookup_request_result(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        request_id: RequestId,
    ) -> Result<Option<MetadataCommandResult>, MetaError> {
        let Some(dedupe) =
            self.lookup_request(root_id, placement_generation, owner_epoch, request_id)?
        else {
            return Ok(None);
        };
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        let key = command_dedupe_key(root_id, request_id);
        let recovery = self.validate_dedupe_recovery_binding(&key, &dedupe)?;
        Ok(Some(MetadataCommandResult {
            commit_version: dedupe.commit_version,
            deterministic_result: dedupe.deterministic_result,
            recovery_lsn: dedupe.recovery_lsn,
            recovery_chain_digest: recovery.chain_digest,
            replayed: true,
        }))
    }

    /// Stable ordered prefix scan at one fenced read version.
    ///
    /// The current-version path assembles one logical prefix scan from bounded physical pages.
    /// A historical scan also reconstructs keys replaced or deleted after `version` from History.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_prefix_at(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        prefix: &[u8],
        version: ReadVersion,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<MetadataScanItem>, MetaError> {
        self.scan_prefix_at_impl(
            root_id,
            placement_generation,
            owner_epoch,
            family,
            prefix,
            None,
            version,
            start_after,
            limit,
            MAX_COMMAND_ITEMS,
        )
        .map(|items| {
            items
                .into_iter()
                .map(|item| match item {
                    DelimitedMetadataScanItem::Record(record) => record,
                    DelimitedMetadataScanItem::CommonPrefix(_) => {
                        unreachable!("a scan without a delimiter cannot emit a common prefix")
                    }
                })
                .collect()
        })
    }

    /// Stable ordered prefix scan that folds deeper keys at `delimiter` and
    /// returns concrete records and implicit common prefixes at that level.
    ///
    /// Every returned record or common prefix counts against `limit`. The
    /// prefix byte string includes the delimiter and is a valid exclusive
    /// cursor for the next raw page.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn scan_delimited_prefix_at(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        prefix: &[u8],
        delimiter: u8,
        version: ReadVersion,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<DelimitedMetadataScanItem>, MetaError> {
        self.scan_prefix_at_impl(
            root_id,
            placement_generation,
            owner_epoch,
            family,
            prefix,
            Some(delimiter),
            version,
            start_after,
            limit,
            MAX_DELIMITED_SCAN_ITEMS,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_prefix_at_impl(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        prefix: &[u8],
        delimiter: Option<u8>,
        version: ReadVersion,
        start_after: Option<&[u8]>,
        limit: usize,
        max_items: usize,
    ) -> Result<Vec<DelimitedMetadataScanItem>, MetaError> {
        let effective_limit = if limit == 0 {
            max_items
        } else {
            limit.min(max_items)
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_call();
        let mut current_version =
            self.validate_read_fence(root_id, placement_generation, owner_epoch, prefix, version)?;
        let read_context = ReadFenceContext {
            root_id,
            placement_generation,
            owner_epoch,
        };

        // Current-state rows are already in caller order. Read each physical
        // page and the clock in one store snapshot. If the clock advances,
        // discard every collected page and reconstruct the requested version
        // from History.
        if version == current_version {
            if let Some(visible) = self.collect_current_scan_at_clock(
                family,
                prefix,
                start_after,
                effective_limit,
                delimiter,
                version,
                current_version,
                read_context,
            )? {
                return Ok(visible);
            }
            current_version = self.validate_read_fence(
                root_id,
                placement_generation,
                owner_epoch,
                prefix,
                version,
            )?;
        }

        let visible = self.reconstruct_historical_scan(
            root_id,
            placement_generation,
            owner_epoch,
            family,
            prefix,
            version,
            current_version,
            read_context,
        )?;

        let mut page = Vec::with_capacity(effective_limit);
        let mut last_common_prefix = None;
        for (key, value) in visible {
            let item = match delimiter.and_then(|delimiter| {
                key.get(prefix.len()..)?
                    .iter()
                    .position(|byte| *byte == delimiter)
                    .map(|offset| key[..prefix.len() + offset + 1].to_vec())
            }) {
                Some(common_prefix) => {
                    if last_common_prefix.as_ref() == Some(&common_prefix) {
                        continue;
                    }
                    last_common_prefix = Some(common_prefix.clone());
                    DelimitedMetadataScanItem::CommonPrefix(common_prefix)
                }
                None => DelimitedMetadataScanItem::Record(MetadataScanItem { key, value }),
            };
            let item_key = match &item {
                DelimitedMetadataScanItem::Record(record) => record.key.as_slice(),
                DelimitedMetadataScanItem::CommonPrefix(prefix) => prefix.as_slice(),
            };
            if start_after.is_some_and(|marker| item_key <= marker) {
                continue;
            }
            page.push(item);
            if page.len() == effective_limit {
                break;
            }
        }
        Ok(page)
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_current_scan_at_clock(
        &self,
        family: MetadataFamily,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        limit: usize,
        delimiter: Option<u8>,
        version: ReadVersion,
        expected_clock: ReadVersion,
        read_context: ReadFenceContext,
    ) -> Result<Option<Vec<DelimitedMetadataScanItem>>, MetaError> {
        let mut after = start_after.map(ToOwned::to_owned);
        let mut visible = Vec::with_capacity(limit);
        loop {
            let remaining = limit - visible.len();
            if remaining == 0 {
                return Ok(Some(visible));
            }
            let Some(page) = self.scan_page_at_clock(
                family.keyspace(),
                prefix,
                after.as_deref(),
                remaining,
                delimiter,
                expected_clock,
                read_context,
                "scan current metadata",
            )?
            else {
                return Ok(None);
            };
            if page.items.len() > remaining {
                return Err(corrupt(
                    "transaction-store scan",
                    "scan page exceeds the requested row limit",
                ));
            }
            let more = page.more;
            if let Some(last) = page.items.last() {
                after = Some(last.key().to_vec());
            } else if more {
                return Err(corrupt(
                    "transaction-store scan",
                    "incomplete scan page omitted its continuation cursor",
                ));
            }
            for item in page.items {
                let item = match item {
                    ScanItem::Row { key, value } => {
                        let current = CurrentValue::decode(&value)
                            .map_err(|error| corrupt(family.name(), error))?;
                        if current.modified_version.get() > version.get() {
                            return Err(corrupt(
                                family.name(),
                                "current row is newer than the stable scan clock",
                            ));
                        }
                        DelimitedMetadataScanItem::Record(MetadataScanItem {
                            key,
                            value: current.payload,
                        })
                    }
                    ScanItem::CommonPrefix(prefix) => {
                        DelimitedMetadataScanItem::CommonPrefix(prefix)
                    }
                };
                visible.push(item);
            }
            if !more {
                return Ok(Some(visible));
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn reconstruct_historical_scan(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        family: MetadataFamily,
        prefix: &[u8],
        version: ReadVersion,
        mut expected_clock: ReadVersion,
        read_context: ReadFenceContext,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, MetaError> {
        let mut attempt = 1;
        loop {
            let Some(current_rows) = self.collect_scan_at_clock(
                family.keyspace(),
                prefix,
                None,
                MAX_HISTORICAL_SCAN_PAGE_ROWS,
                expected_clock,
                read_context,
                "scan current metadata",
            )?
            else {
                expected_clock = self.prepare_historical_scan_retry(
                    root_id,
                    placement_generation,
                    owner_epoch,
                    prefix,
                    version,
                    attempt,
                )?;
                attempt += 1;
                continue;
            };
            // History keys encode the complete user-key length before the
            // root-scoped key. The current durable format therefore cannot
            // seek one variable-length root prefix without a schema migration.
            let history_prefix = [family.format_tag()];
            let Some(history_rows) = self.collect_scan_at_clock(
                HISTORY.id,
                &history_prefix,
                None,
                MAX_HISTORICAL_SCAN_PAGE_ROWS,
                expected_clock,
                read_context,
                "scan metadata history",
            )?
            else {
                expected_clock = self.prepare_historical_scan_retry(
                    root_id,
                    placement_generation,
                    owner_epoch,
                    prefix,
                    version,
                    attempt,
                )?;
                attempt += 1;
                continue;
            };

            let mut visible = BTreeMap::new();
            for item in current_rows {
                let ScanItem::Row { key, value } = item else {
                    return Err(corrupt(
                        family.name(),
                        "non-delimited scan returned a common prefix",
                    ));
                };
                let current =
                    CurrentValue::decode(&value).map_err(|error| corrupt(family.name(), error))?;
                if current.modified_version.get() <= version.get() {
                    visible.insert(key, current.payload);
                }
            }
            for item in history_rows {
                let ScanItem::Row { key, value } = item else {
                    return Err(corrupt(
                        "History",
                        "non-delimited scan returned a common prefix",
                    ));
                };
                let user_key = history_user_key(&key)?;
                if !user_key.starts_with(prefix) || visible.contains_key(user_key) {
                    continue;
                }
                let history =
                    HistoryValue::decode(&value).map_err(|error| corrupt("History", error))?;
                if history.previous_modified_version.get() <= version.get()
                    && version.get() < history.transition_version.get()
                {
                    if let Some(previous) = history.previous_payload {
                        visible.insert(user_key.to_vec(), previous);
                    }
                }
            }
            return Ok(visible);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_historical_scan_retry(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        prefix: &[u8],
        version: ReadVersion,
        attempt: usize,
    ) -> Result<ReadVersion, MetaError> {
        if attempt == MAX_HISTORICAL_SCAN_ATTEMPTS {
            self.validate_read_fence(root_id, placement_generation, owner_epoch, prefix, version)?;
            return Err(MetaError::ReadStabilityExhausted {
                attempts: MAX_HISTORICAL_SCAN_ATTEMPTS,
            });
        }
        thread::sleep(HISTORICAL_SCAN_RETRY_DELAYS[attempt - 1]);
        self.validate_read_fence(root_id, placement_generation, owner_epoch, prefix, version)
    }

    /// Read one immutable change event through the ordinary ownership fences.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn read_change_event_at(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        key: &[u8],
        version: ReadVersion,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        validate_root_scoped_bytes(root_id, key, "change-event key")?;
        let (current_version, record) = self.read_value_at_fence(
            CHANGE_EVENT.id,
            key,
            MetadataPointReadSource::Other,
            "read ChangeEvent",
            ReadFenceContext {
                root_id,
                placement_generation,
                owner_epoch,
            },
        )?;
        if version > current_version {
            return Err(MetaError::ReadVersionInFuture {
                requested: version.get(),
                current: current_version.get(),
            });
        }
        let Some(record) = record else {
            return Ok(None);
        };
        let current =
            CurrentValue::decode(&record).map_err(|error| corrupt("ChangeEvent", error))?;
        Ok((current.modified_version.get() <= version.get()).then_some(current.payload))
    }

    /// Stable ordered scan of immutable typed change events.
    ///
    /// Change events are executor-owned and therefore are deliberately absent
    /// from [`MetadataFamily`]. This read-only entry point retains that
    /// boundary while exposing the same exact root/placement/owner/read-version
    /// fence as ordinary metadata scans.
    #[allow(clippy::too_many_arguments)]
    pub fn scan_change_events_at(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        prefix: &[u8],
        version: ReadVersion,
        start_after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<MetadataScanItem>, MetaError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| internal("lock read gate", error))?;
        let mut expected_clock =
            self.validate_read_fence(root_id, placement_generation, owner_epoch, prefix, version)?;
        let effective_limit = if limit == 0 {
            MAX_COMMAND_ITEMS
        } else {
            limit.min(MAX_COMMAND_ITEMS)
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_call();
        let read_context = ReadFenceContext {
            root_id,
            placement_generation,
            owner_epoch,
        };
        'restart: loop {
            let mut events = Vec::with_capacity(effective_limit);
            let mut after = start_after.map(ToOwned::to_owned);
            loop {
                let Some(page) = self.scan_page_at_clock(
                    CHANGE_EVENT.id,
                    prefix,
                    after.as_deref(),
                    effective_limit,
                    None,
                    expected_clock,
                    read_context,
                    "scan ChangeEvent",
                )?
                else {
                    expected_clock = self.validate_read_fence(
                        root_id,
                        placement_generation,
                        owner_epoch,
                        prefix,
                        version,
                    )?;
                    continue 'restart;
                };
                let more = page.more;
                if let Some(last) = page.items.last() {
                    after = Some(last.key().to_vec());
                }
                for item in page.items {
                    let ScanItem::Row { key, value } = item else {
                        return Err(corrupt(
                            "ChangeEvent",
                            "non-delimited scan returned a common prefix",
                        ));
                    };
                    let current = CurrentValue::decode(&value)
                        .map_err(|error| corrupt("ChangeEvent", error))?;
                    if current.modified_version.get() <= version.get() {
                        events.push(MetadataScanItem {
                            key,
                            value: current.payload,
                        });
                    }
                    if events.len() == effective_limit {
                        return Ok(events);
                    }
                }
                if !more {
                    return Ok(events);
                }
            }
        }
    }

    fn validate_read_fence(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        key_or_prefix: &[u8],
        version: ReadVersion,
    ) -> Result<ReadVersion, MetaError> {
        validate_root_scoped_bytes(root_id, key_or_prefix, "read key or prefix")?;
        let current_version =
            self.validate_read_context(root_id, placement_generation, owner_epoch)?;
        if version > current_version {
            return Err(MetaError::ReadVersionInFuture {
                requested: version.get(),
                current: current_version.get(),
            });
        }
        Ok(current_version)
    }

    fn validate_read_context(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
    ) -> Result<ReadVersion, MetaError> {
        self.read_batch_at_fence(
            ReadFenceContext {
                root_id,
                placement_generation,
                owner_epoch,
            },
            Vec::new(),
            "validate read context",
        )
        .map(|(version, _)| version)
    }

    fn read_batch_at_fence(
        &self,
        context: ReadFenceContext,
        data_ops: Vec<ReadOp>,
        operation: &'static str,
    ) -> Result<(ReadVersion, Vec<ReadResult>), MetaError> {
        let mut ops = Vec::with_capacity(3 + data_ops.len());
        ops.push(ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY)));
        ops.push(ReadOp::Get(Key::new(
            ROOT_FENCE.id,
            context.root_id.as_bytes(),
        )));
        ops.push(ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_COMMIT_CLOCK_KEY)));
        ops.extend(data_ops);
        let snapshot = self.read_batch(ReadBatch { ops }, operation)?;
        let mut results = snapshot.results.into_iter();
        let owner = required_get_result(results.next(), "System(owner_fence)")?;
        let fence = required_get_result(results.next(), "RootFence")?;
        let clock = required_get_result(results.next(), "System(commit_clock)")?;
        #[cfg(feature = "metadata-read-stats")]
        {
            self.record_point(MetadataPointReadSource::System, Some(owner.len()));
            self.record_point(MetadataPointReadSource::RootFence, Some(fence.len()));
            self.record_point(MetadataPointReadSource::System, Some(clock.len()));
        }
        let actual_owner = decode_system_u64(&owner, "System(owner_fence)")?;
        if actual_owner != context.owner_epoch.get() {
            return Err(MetaError::OwnerEpochMismatch {
                expected: context.owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let fence = RootFence::decode(&fence).map_err(|error| corrupt("RootFence", error))?;
        if fence.logical_shard_id != self.logical_shard_id
            || fence.placement_generation != context.placement_generation
        {
            return Err(MetaError::PlacementMismatch);
        }
        if fence.activation_state != RootActivationState::Active {
            return Err(MetaError::RootFenceStateMismatch {
                expected: RootActivationState::Active,
                actual: fence.activation_state,
            });
        }
        let clock = decode_system_u64(&clock, "System(commit_clock)")?;
        let clock =
            ReadVersion::new(clock).map_err(|error| corrupt("System(commit_clock)", error))?;
        Ok((clock, results.collect()))
    }

    fn read_at_fence(
        &self,
        family: MetadataFamily,
        key: &[u8],
        version: ReadVersion,
        context: ReadFenceContext,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        let (mut expected_clock, record) = self.read_value_at_fence(
            family.keyspace(),
            key,
            point_source(family),
            "read current metadata",
            context,
        )?;
        if version > expected_clock {
            return Err(MetaError::ReadVersionInFuture {
                requested: version.get(),
                current: expected_clock.get(),
            });
        }
        if let Some(record) = record {
            let current =
                CurrentValue::decode(&record).map_err(|error| corrupt(family.name(), error))?;
            if current.modified_version.get() > expected_clock.get() {
                return Err(corrupt(
                    family.name(),
                    format!(
                        "record version {} is newer than the captured commit clock {}",
                        current.modified_version.get(),
                        expected_clock.get()
                    ),
                ));
            }
            if current.modified_version.get() <= version.get() {
                return Ok(Some(current.payload));
            }
        } else if version == expected_clock {
            return Ok(None);
        }
        let prefix = history_prefix(family, key);
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_call();
        loop {
            let Some(rows) = self.collect_scan_at_clock(
                HISTORY.id,
                &prefix,
                None,
                MAX_HISTORICAL_SCAN_PAGE_ROWS,
                expected_clock,
                context,
                "read metadata history",
            )?
            else {
                expected_clock = self.validate_read_context(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                )?;
                if version > expected_clock {
                    return Err(MetaError::ReadVersionInFuture {
                        requested: version.get(),
                        current: expected_clock.get(),
                    });
                }
                continue;
            };
            let mut previous_payload = None;
            for item in rows {
                let ScanItem::Row { value, .. } = item else {
                    return Err(corrupt(
                        "History",
                        "non-delimited scan returned a common prefix",
                    ));
                };
                let history =
                    HistoryValue::decode(&value).map_err(|error| corrupt("History", error))?;
                if history.previous_modified_version.get() <= version.get()
                    && version.get() < history.transition_version.get()
                {
                    previous_payload = Some(history.previous_payload);
                    break;
                }
            }
            return Ok(previous_payload.flatten());
        }
    }

    fn validate_command(&self, command: &MetadataCommand) -> Result<(), MetaError> {
        if command.schema_id != SCHEMA_ID {
            return Err(MetaError::SchemaGate {
                reason: format!("expected schema {SCHEMA_ID}, found {}", command.schema_id),
            });
        }
        if command.logical_shard_id != self.logical_shard_id {
            return Err(MetaError::PlacementMismatch);
        }
        if command.object_namespace_id.is_none() {
            return Err(invalid(
                "metadata command requires an object namespace identity",
            ));
        }
        for (name, count) in [
            ("predicates", command.predicates.len()),
            ("mutations", command.mutations.len()),
            ("history projections", command.history_projection.len()),
            ("event projections", command.event_projection.len()),
        ] {
            if count > MAX_COMMAND_ITEMS {
                return Err(invalid(format!(
                    "{name} count {count} exceeds {MAX_COMMAND_ITEMS}"
                )));
            }
        }
        if command.deterministic_result.len() > MAX_DETERMINISTIC_RESULT_BYTES {
            return Err(invalid("deterministic result exceeds size bound"));
        }
        if !matches!(command.root_fence_action, RootFenceAction::RequireActive)
            && (!command.predicates.is_empty()
                || !command.mutations.is_empty()
                || !command.history_projection.is_empty()
                || !command.event_projection.is_empty())
        {
            return Err(invalid(
                "root-fence installation/transition cannot carry domain mutations",
            ));
        }
        for predicate in &command.predicates {
            match predicate {
                CommandPredicate::Value { key, expected, .. } => {
                    validate_root_scoped_bytes(command.root_id, key, "predicate key")?;
                    if let Some(value) = expected {
                        validate_value_bytes(value, "predicate value")?;
                    }
                }
                CommandPredicate::PrefixEmpty { prefix, .. } => {
                    validate_root_scoped_bytes(command.root_id, prefix, "predicate prefix")?;
                }
            }
        }
        let mut mutation_keys = BTreeSet::new();
        for mutation in &command.mutations {
            validate_root_scoped_bytes(command.root_id, mutation.key(), "mutation key")?;
            if !mutation_keys.insert((mutation.family(), mutation.key().to_vec())) {
                return Err(invalid("duplicate mutation key"));
            }
            if let CommandMutation::Put { value, .. } = mutation {
                validate_value_bytes(value, "mutation value")?;
            }
        }
        for projection in &command.history_projection {
            validate_root_scoped_bytes(command.root_id, &projection.key, "history key")?;
        }
        if command
            .event_projection
            .iter()
            .any(|event| event.payload.len() > MAX_EVENT_BYTES)
        {
            return Err(invalid("event projection exceeds size bound"));
        }
        Ok(())
    }

    fn plan_root_fence(&self, command: &MetadataCommand) -> Result<RootFencePlan, MetaError> {
        let key = command.root_id.as_bytes();
        let current = self.read_value(
            ROOT_FENCE.id,
            key,
            MetadataPointReadSource::RootFence,
            "read RootFence",
        )?;
        match command.root_fence_action {
            RootFenceAction::Install => {
                if current.is_some() {
                    return Err(MetaError::RootFenceAlreadyInstalled);
                }
                Ok(RootFencePlan::Install {
                    value: RootFence {
                        logical_shard_id: command.logical_shard_id,
                        object_namespace_id: command.object_namespace_id,
                        placement_generation: command.placement_generation,
                        activation_state: RootActivationState::Installing,
                    }
                    .encode()
                    .expect("typed RootFence always fits its fixed format"),
                })
            }
            RootFenceAction::BindObjectNamespace { expected } => {
                let current = current.ok_or(MetaError::RootFenceMissing)?;
                let fence =
                    RootFence::decode(&current).map_err(|error| corrupt("RootFence", error))?;
                validate_root_placement_identity(command, fence)?;
                if fence.activation_state != expected {
                    return Err(MetaError::RootFenceStateMismatch {
                        expected,
                        actual: fence.activation_state,
                    });
                }
                let object_namespace_id = command
                    .object_namespace_id
                    .ok_or_else(|| invalid("object namespace binding requires a namespace id"))?;
                match fence.object_namespace_id {
                    None => Ok(RootFencePlan::Replace {
                        expected: current,
                        value: RootFence {
                            object_namespace_id: Some(object_namespace_id),
                            ..fence
                        }
                        .encode()
                        .expect("typed RootFence always fits its fixed format"),
                    }),
                    Some(actual) if actual == object_namespace_id => {
                        Ok(RootFencePlan::Assert { expected: current })
                    }
                    Some(_) => Err(MetaError::PlacementMismatch),
                }
            }
            RootFenceAction::RequireActive => {
                let current = current.ok_or(MetaError::RootFenceMissing)?;
                let fence =
                    RootFence::decode(&current).map_err(|error| corrupt("RootFence", error))?;
                validate_root_placement(command, fence)?;
                if fence.activation_state != RootActivationState::Active {
                    return Err(MetaError::RootFenceStateMismatch {
                        expected: RootActivationState::Active,
                        actual: fence.activation_state,
                    });
                }
                Ok(RootFencePlan::Assert { expected: current })
            }
            RootFenceAction::Transition { expected, next } => {
                if !valid_root_transition(expected, next) {
                    return Err(MetaError::InvalidRootFenceTransition {
                        from: expected,
                        to: next,
                    });
                }
                let current = current.ok_or(MetaError::RootFenceMissing)?;
                let fence =
                    RootFence::decode(&current).map_err(|error| corrupt("RootFence", error))?;
                validate_root_placement(command, fence)?;
                if fence.activation_state != expected {
                    return Err(MetaError::RootFenceStateMismatch {
                        expected,
                        actual: fence.activation_state,
                    });
                }
                Ok(RootFencePlan::Replace {
                    expected: current,
                    value: RootFence {
                        activation_state: next,
                        ..fence
                    }
                    .encode()
                    .expect("typed RootFence always fits its fixed format"),
                })
            }
        }
    }

    fn plan_predicates(&self, command: &MetadataCommand) -> Result<PredicatePlan, MetaError> {
        let mut plan = PredicatePlan::default();
        for predicate in &command.predicates {
            match predicate {
                CommandPredicate::Value {
                    family,
                    key,
                    expected,
                } => {
                    let map_key = (*family, key.clone());
                    if plan.exact.contains_key(&map_key) {
                        return Err(invalid("duplicate exact predicate"));
                    }
                    let raw = self.read_family_value(*family, key, "plan exact predicate")?;
                    let current = match &raw {
                        Some(value) => {
                            let current = CurrentValue::decode(value)
                                .map_err(|error| corrupt(family.name(), error))?;
                            Some(current)
                        }
                        None => None,
                    };
                    if current.as_ref().map(|value| &value.payload) != expected.as_ref() {
                        return Err(MetaError::PredicateFailed);
                    }
                    plan.exact.insert(
                        map_key,
                        PlannedExactPredicate {
                            family: *family,
                            key: key.clone(),
                            current,
                            raw,
                        },
                    );
                }
                CommandPredicate::PrefixEmpty { family, prefix } => {
                    if !self
                        .scan_page(
                            family.keyspace(),
                            prefix,
                            None,
                            1,
                            None,
                            0,
                            "plan prefix-empty predicate",
                        )?
                        .items
                        .is_empty()
                    {
                        return Err(MetaError::PredicateFailed);
                    }
                    plan.prefix_empty.push((*family, prefix.clone()));
                }
            }
        }
        validate_predicate_plan(command, &plan)?;
        Ok(plan)
    }

    fn validate_history_projection(
        &self,
        command: &MetadataCommand,
        plan: &PredicatePlan,
    ) -> Result<(), MetaError> {
        let requested = command
            .history_projection
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if requested.len() != command.history_projection.len() {
            return Err(invalid("duplicate history projection"));
        }
        let expected = command
            .mutations
            .iter()
            .filter_map(|mutation| {
                let planned = plan
                    .exact
                    .get(&(mutation.family(), mutation.key().to_vec()))?;
                planned.current.as_ref().map(|_| HistoryProjection {
                    family: mutation.family(),
                    key: mutation.key().to_vec(),
                })
            })
            .collect::<BTreeSet<_>>();
        if requested != expected {
            return Err(invalid(
                "history projection must exactly cover overwritten/deleted values",
            ));
        }
        Ok(())
    }

    fn replayed_result(
        &self,
        key: &[u8],
        digest: CommandDigest,
    ) -> Result<Option<MetadataCommandResult>, MetaError> {
        let Some(value) = self.read_value(
            COMMAND_DEDUPE.id,
            key,
            MetadataPointReadSource::Other,
            "read CommandDedupe",
        )?
        else {
            return Ok(None);
        };
        let record =
            CommandDedupeRecord::decode(&value).map_err(|error| corrupt("CommandDedupe", error))?;
        if record.command_digest != digest {
            return Err(MetaError::RequestIdReused);
        }
        let recovery = self.validate_dedupe_recovery_binding(key, &record)?;
        Ok(Some(MetadataCommandResult {
            commit_version: record.commit_version,
            deterministic_result: record.deterministic_result,
            recovery_lsn: record.recovery_lsn,
            recovery_chain_digest: recovery.chain_digest,
            replayed: true,
        }))
    }

    fn validate_dedupe_recovery_binding(
        &self,
        dedupe_key: &[u8],
        dedupe: &CommandDedupeRecord,
    ) -> Result<RecoveryOutboxRecord, MetaError> {
        let row = self.recovery_record_at_unlocked(dedupe.recovery_lsn)?;
        let (
            RecoveryMutationV1::MetadataCommand { command, .. },
            RecoveryResultV1::MetadataCommand {
                commit_version,
                deterministic_result,
            },
        ) = (&row.mutation, &row.result)
        else {
            return Err(corrupt(
                "CommandDedupe recovery binding",
                format!(
                    "dedupe LSN {} does not name a metadata command",
                    dedupe.recovery_lsn
                ),
            ));
        };
        if command_dedupe_key(command.root_id, command.request_id) != dedupe_key
            || command.command_digest != dedupe.command_digest
            || commit_version != &dedupe.commit_version
            || deterministic_result != &dedupe.deterministic_result
        {
            return Err(corrupt(
                "CommandDedupe recovery binding",
                format!(
                    "dedupe result does not match RecoveryOutbox LSN {}",
                    dedupe.recovery_lsn
                ),
            ));
        }
        Ok(row)
    }

    fn recovery_state_unlocked(&self) -> Result<RecoveryState, MetaError> {
        let (lsn_value, digest_value) = self.recovery_tail_values()?;
        let lsn = decode_system_u64(&lsn_value, "System(applied_recovery_lsn)")?;
        let chain_digest = decode_system_digest(&digest_value, "System(recovery_chain_digest)")?;
        Ok(RecoveryState {
            applied_recovery_lsn: lsn,
            chain_digest,
        })
    }

    fn recovery_tail_values(&self) -> Result<(Vec<u8>, Vec<u8>), MetaError> {
        let snapshot = self.read_batch(
            ReadBatch {
                ops: vec![
                    ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_APPLIED_RECOVERY_LSN_KEY)),
                    ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_RECOVERY_CHAIN_DIGEST_KEY)),
                ],
            },
            "read recovery tail",
        )?;
        let mut results = snapshot.results.into_iter();
        let Some(ReadResult::Get(lsn)) = results.next() else {
            return Err(corrupt("System(recovery tail)", "LSN result is missing"));
        };
        let Some(ReadResult::Get(digest)) = results.next() else {
            return Err(corrupt(
                "System(recovery tail)",
                "chain digest result is missing",
            ));
        };
        let lsn = lsn.ok_or_else(|| MetaError::CorruptRecord {
            record: "System(applied_recovery_lsn)",
            reason: "record is missing".to_owned(),
        })?;
        let digest = digest.ok_or_else(|| MetaError::CorruptRecord {
            record: "System(recovery_chain_digest)",
            reason: "record is missing".to_owned(),
        })?;
        Ok((lsn, digest))
    }

    fn plan_recovery(
        &self,
        mutation: RecoveryMutationV1,
        result: RecoveryResultV1,
    ) -> Result<RecoveryPlan, MetaError> {
        let (lsn_value, digest_value) = self.recovery_tail_values()?;
        build_recovery_plan(lsn_value, digest_value, mutation, result)
    }

    fn verify_recovery_chain_unlocked(&self) -> Result<RecoveryState, MetaError> {
        self.with_stable_recovery_scan(|state| self.verify_recovery_chain_at_state_unlocked(state))
    }

    fn with_stable_recovery_scan<T>(
        &self,
        mut scan: impl FnMut(RecoveryState) -> Result<T, MetaError>,
    ) -> Result<T, MetaError> {
        for attempt in 0..MAX_RECOVERY_SCAN_ATTEMPTS {
            let before = self.recovery_state_unlocked()?;
            let result = scan(before);
            let after = self.recovery_state_unlocked()?;
            if before == after {
                return result;
            }
            if let Some(delay) = RECOVERY_SCAN_RETRY_DELAYS.get(attempt) {
                thread::sleep(*delay);
            }
        }
        Err(MetaError::ReadStabilityExhausted {
            attempts: MAX_RECOVERY_SCAN_ATTEMPTS,
        })
    }

    fn verify_recovery_chain_at_state_unlocked(
        &self,
        state: RecoveryState,
    ) -> Result<RecoveryState, MetaError> {
        let mut expected_lsn = 1_u64;
        let mut previous_chain_digest = recovery_genesis_digest(self.logical_shard_id);
        let mut expected_chunk_keys = BTreeSet::new();
        let mut after = None;
        loop {
            let page = self.scan_page(
                RECOVERY_OUTBOX.id,
                &[],
                after.as_deref(),
                MAX_HISTORICAL_SCAN_PAGE_ROWS,
                None,
                0,
                "verify RecoveryOutbox",
            )?;
            let more = page.more;
            for item in page.items {
                let ScanItem::Row { key, value } = item else {
                    return Err(corrupt(
                        "RecoveryOutbox",
                        "non-delimited scan returned a common prefix",
                    ));
                };
                after = Some(key.clone());
                match key.first().copied() {
                    Some(tag) if RecoveryKeyLayout::from_header_tag(tag).is_some() => {
                        let decoded = decode_recovery_outbox_key(&key)
                            .map_err(|error| corrupt("RecoveryOutbox key", error))?;
                        let chunk_count = recovery_storage_chunk_count(&value)
                            .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
                        for index in 0..chunk_count {
                            expected_chunk_keys.insert(recovery_chunk_key_for_layout(
                                decoded.layout,
                                decoded.recovery_lsn,
                                index,
                            ));
                        }
                        let row = self.read_recovery_record(decoded, &value)?;
                        if decoded.recovery_lsn != expected_lsn || row.recovery_lsn != expected_lsn
                        {
                            return Err(MetaError::CorruptRecord {
                                record: "RecoveryOutbox",
                                reason: format!(
                                    "expected contiguous LSN {expected_lsn}, found key {} row {}",
                                    decoded.recovery_lsn, row.recovery_lsn
                                ),
                            });
                        }
                        if row.previous_chain_digest != previous_chain_digest {
                            return Err(MetaError::CorruptRecord {
                                record: "RecoveryOutbox",
                                reason: format!(
                                    "LSN {expected_lsn} does not link to its predecessor"
                                ),
                            });
                        }
                        previous_chain_digest = row.chain_digest;
                        expected_lsn = expected_lsn
                            .checked_add(1)
                            .ok_or(MetaError::VersionOverflow)?;
                    }
                    Some(tag) if RecoveryKeyLayout::from_chunk_tag(tag).is_some() => {
                        if !expected_chunk_keys.remove(&key) {
                            return Err(MetaError::CorruptRecord {
                                record: "RecoveryOutbox chunk",
                                reason: "orphaned or malformed chunk key".to_owned(),
                            });
                        }
                    }
                    _ => {
                        return Err(MetaError::CorruptRecord {
                            record: "RecoveryOutbox key",
                            reason: "unknown storage-key tag".to_owned(),
                        });
                    }
                }
            }
            if !more {
                break;
            }
        }
        if !expected_chunk_keys.is_empty() {
            return Err(MetaError::CorruptRecord {
                record: "RecoveryOutbox chunk",
                reason: "one or more declared chunks are missing".to_owned(),
            });
        }
        let observed_lsn = expected_lsn - 1;
        if state.applied_recovery_lsn != observed_lsn || state.chain_digest != previous_chain_digest
        {
            return Err(MetaError::CorruptRecord {
                record: "System(recovery tail)",
                reason: format!(
                    "tail does not match outbox: System LSN {}, scanned LSN {observed_lsn}",
                    state.applied_recovery_lsn
                ),
            });
        }
        Ok(state)
    }

    fn read_recovery_record(
        &self,
        decoded: DecodedRecoveryOutboxKey,
        header: &[u8],
    ) -> Result<RecoveryOutboxRecord, MetaError> {
        let chunk_count = recovery_storage_chunk_count(header)
            .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
        let mut chunks = Vec::with_capacity(chunk_count as usize);
        for index in 0..chunk_count {
            let value = self
                .read_value(
                    RECOVERY_OUTBOX.id,
                    &recovery_chunk_key_for_layout(decoded.layout, decoded.recovery_lsn, index),
                    MetadataPointReadSource::Other,
                    "read RecoveryOutbox chunk",
                )?
                .ok_or_else(|| MetaError::CorruptRecord {
                    record: "RecoveryOutbox chunk",
                    reason: format!("missing LSN {} chunk {index}", decoded.recovery_lsn),
                })?;
            chunks.push(value);
        }
        let logical = assemble_recovery_storage(header, chunks)
            .map_err(|error| corrupt("RecoveryOutbox storage", error))?;
        RecoveryOutboxRecord::decode(&logical).map_err(|error| corrupt("RecoveryOutbox", error))
    }

    fn recovery_record_at_unlocked(
        &self,
        recovery_lsn: u64,
    ) -> Result<RecoveryOutboxRecord, MetaError> {
        if recovery_lsn == 0 {
            return Err(corrupt("RecoveryOutbox", "LSN zero has no outbox row"));
        }
        let mut found = None;
        for layout in RecoveryKeyLayout::ORDERED {
            let key = recovery_outbox_scan_start(layout, recovery_lsn);
            let Some(header) = self.read_value(
                RECOVERY_OUTBOX.id,
                &key,
                MetadataPointReadSource::Other,
                "read RecoveryOutbox header",
            )?
            else {
                continue;
            };
            if found.is_some() {
                return Err(corrupt(
                    "RecoveryOutbox",
                    format!("duplicate storage layouts contain LSN {recovery_lsn}"),
                ));
            }
            let row = self.read_recovery_record(
                DecodedRecoveryOutboxKey {
                    recovery_lsn,
                    layout,
                },
                &header,
            )?;
            if row.recovery_lsn != recovery_lsn {
                return Err(corrupt(
                    "RecoveryOutbox",
                    format!(
                        "key LSN {recovery_lsn} does not match row LSN {}",
                        row.recovery_lsn
                    ),
                ));
            }
            found = Some(row);
        }
        found.ok_or_else(|| {
            corrupt(
                "RecoveryOutbox",
                format!("missing recovery row at LSN {recovery_lsn}"),
            )
        })
    }

    #[cfg(feature = "metadata-read-stats")]
    #[inline]
    fn read_stats_store_key(&self) -> usize {
        Arc::as_ptr(&self.read_stats_identity) as usize
    }

    #[cfg(feature = "metadata-read-stats")]
    #[inline]
    fn record_point(&self, source: MetadataPointReadSource, value_bytes: Option<usize>) {
        read_stats::record_point(self.read_stats_store_key(), source, value_bytes);
    }

    #[cfg(feature = "metadata-read-stats")]
    #[inline]
    fn record_scan_call(&self) {
        read_stats::record_scan_call(self.read_stats_store_key());
    }

    #[cfg(feature = "metadata-read-stats")]
    #[inline]
    fn record_scan_result(&self, page: &ScanPage, requested_limit: usize) {
        let mut key_bytes = 0_u64;
        let mut value_bytes = 0_u64;
        for item in &page.items {
            key_bytes = key_bytes.saturating_add(byte_len(item.key()));
            if let ScanItem::Row { value, .. } = item {
                value_bytes = value_bytes.saturating_add(byte_len(value));
            }
        }
        read_stats::record_scan_result(
            self.read_stats_store_key(),
            key_bytes,
            value_bytes,
            page.more || page.items.len() == requested_limit,
        );
    }

    fn read_family_value(
        &self,
        family: MetadataFamily,
        key: &[u8],
        operation: &'static str,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        self.read_value(family.keyspace(), key, point_source(family), operation)
    }

    fn read_value(
        &self,
        keyspace: Keyspace,
        key: &[u8],
        source: MetadataPointReadSource,
        operation: &'static str,
    ) -> Result<Option<Vec<u8>>, MetaError> {
        let snapshot = self.read_batch(
            ReadBatch {
                ops: vec![ReadOp::Get(Key::new(keyspace, key))],
            },
            operation,
        )?;
        let Some(ReadResult::Get(value)) = snapshot.results.into_iter().next() else {
            return Err(corrupt("transaction-store read", "point result is missing"));
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_point(source, value.as_ref().map(Vec::len));
        #[cfg(not(feature = "metadata-read-stats"))]
        let _ = source;
        Ok(value)
    }

    fn read_value_at_fence(
        &self,
        keyspace: Keyspace,
        key: &[u8],
        source: MetadataPointReadSource,
        operation: &'static str,
        context: ReadFenceContext,
    ) -> Result<(ReadVersion, Option<Vec<u8>>), MetaError> {
        let (clock, mut results) = self.read_batch_at_fence(
            context,
            vec![ReadOp::Get(Key::new(keyspace, key))],
            operation,
        )?;
        let Some(ReadResult::Get(value)) = results.pop() else {
            return Err(corrupt("transaction-store read", "point result is missing"));
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_point(source, value.as_ref().map(Vec::len));
        #[cfg(not(feature = "metadata-read-stats"))]
        let _ = source;
        Ok((clock, value))
    }

    fn required_value(
        &self,
        keyspace: Keyspace,
        key: &[u8],
        record: &'static str,
    ) -> Result<Vec<u8>, MetaError> {
        self.read_value(
            keyspace,
            key,
            MetadataPointReadSource::System,
            "read required record",
        )?
        .ok_or_else(|| MetaError::CorruptRecord {
            record,
            reason: "record is missing".to_owned(),
        })
    }

    fn read_batch(
        &self,
        batch: ReadBatch,
        operation: &'static str,
    ) -> Result<ReadSnapshot, MetaError> {
        batch
            .validate(&store_limits())
            .map_err(|source| store_error(operation, source))?;
        self.store
            .read(batch)
            .map_err(|source| store_error(operation, source))
    }

    fn commit(&self, operation: &'static str, txn: WriteTxn) -> Result<Commit, MetaError> {
        txn.validate(&store_limits())
            .map_err(|source| store_error(operation, source))?;
        self.store
            .commit(txn)
            .map_err(|source| store_error(operation, source))
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_page(
        &self,
        keyspace: Keyspace,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        delimiter: Option<u8>,
        reserved_gets: usize,
        operation: &'static str,
    ) -> Result<ScanPage, MetaError> {
        let scan = scan_request(keyspace, prefix, after, limit, delimiter, reserved_gets)?;
        let snapshot = self.read_batch(
            ReadBatch {
                ops: vec![ReadOp::Scan(scan)],
            },
            operation,
        )?;
        let Some(ReadResult::Scan(page)) = snapshot.results.into_iter().next() else {
            return Err(corrupt("transaction-store read", "scan result is missing"));
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_result(&page, limit);
        Ok(page)
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_page_at_clock(
        &self,
        keyspace: Keyspace,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
        delimiter: Option<u8>,
        expected_clock: ReadVersion,
        context: ReadFenceContext,
        operation: &'static str,
    ) -> Result<Option<ScanPage>, MetaError> {
        let scan = scan_request(keyspace, prefix, after, limit, delimiter, 3)?;
        let (clock, mut results) =
            self.read_batch_at_fence(context, vec![ReadOp::Scan(scan)], operation)?;
        let Some(ReadResult::Scan(page)) = results.pop() else {
            return Err(corrupt("transaction-store read", "scan result is missing"));
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_result(&page, limit);
        Ok((clock == expected_clock).then_some(page))
    }

    #[allow(clippy::too_many_arguments)]
    fn collect_scan_at_clock(
        &self,
        keyspace: Keyspace,
        prefix: &[u8],
        start_after: Option<&[u8]>,
        page_rows: usize,
        expected_clock: ReadVersion,
        context: ReadFenceContext,
        operation: &'static str,
    ) -> Result<Option<Vec<ScanItem>>, MetaError> {
        let mut after = start_after.map(ToOwned::to_owned);
        let mut rows = Vec::new();
        loop {
            let Some(page) = self.scan_page_at_clock(
                keyspace,
                prefix,
                after.as_deref(),
                page_rows,
                None,
                expected_clock,
                context,
                operation,
            )?
            else {
                return Ok(None);
            };
            let more = page.more;
            if let Some(last) = page.items.last() {
                after = Some(last.key().to_vec());
            }
            rows.extend(page.items);
            if !more {
                return Ok(Some(rows));
            }
        }
    }
}

#[derive(Default)]
struct PredicatePlan {
    exact: BTreeMap<(MetadataFamily, Vec<u8>), PlannedExactPredicate>,
    prefix_empty: Vec<(MetadataFamily, Vec<u8>)>,
}

struct RecoveryPlan {
    lsn_value: Vec<u8>,
    digest_value: Vec<u8>,
    header: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    row: RecoveryOutboxRecord,
}

struct CommandTxnState {
    next_version: CommitVersion,
    schema: Vec<u8>,
    shard: Vec<u8>,
    owner: Vec<u8>,
    clock: Vec<u8>,
    lease_clock: Option<Vec<u8>>,
    root_plan: RootFencePlan,
    predicate_plan: PredicatePlan,
    recovery: RecoveryPlan,
}

struct PlannedExactPredicate {
    family: MetadataFamily,
    key: Vec<u8>,
    current: Option<CurrentValue>,
    raw: Option<Vec<u8>>,
}

enum RootFencePlan {
    Install { value: Vec<u8> },
    Assert { expected: Vec<u8> },
    Replace { expected: Vec<u8>, value: Vec<u8> },
}

fn build_recovery_plan(
    lsn_value: Vec<u8>,
    digest_value: Vec<u8>,
    mutation: RecoveryMutationV1,
    result: RecoveryResultV1,
) -> Result<RecoveryPlan, MetaError> {
    let applied_lsn = decode_system_u64(&lsn_value, "System(applied_recovery_lsn)")?;
    let recovery_lsn = applied_lsn
        .checked_add(1)
        .ok_or(MetaError::VersionOverflow)?;
    let previous_chain_digest =
        decode_system_digest(&digest_value, "System(recovery_chain_digest)")?;
    let row = RecoveryOutboxRecord::new(recovery_lsn, previous_chain_digest, mutation, result)
        .map_err(|error| corrupt("RecoveryOutbox", error))?;
    let logical = row
        .encode()
        .map_err(|error| corrupt("RecoveryOutbox", error))?;
    let (header, chunks) = split_recovery_storage(&logical)
        .map_err(|error| corrupt("RecoveryOutbox storage", error))?;
    Ok(RecoveryPlan {
        lsn_value,
        digest_value,
        header,
        chunks,
        row,
    })
}

fn synthetic_root_fence_plan(command: &MetadataCommand) -> Result<RootFencePlan, MetaError> {
    let encoded = |state, object_namespace_id| {
        RootFence {
            logical_shard_id: command.logical_shard_id,
            object_namespace_id,
            placement_generation: command.placement_generation,
            activation_state: state,
        }
        .encode()
        .map_err(|error| internal("derive command root fence", error))
    };
    match command.root_fence_action {
        RootFenceAction::Install => Ok(RootFencePlan::Install {
            value: encoded(RootActivationState::Installing, command.object_namespace_id)?,
        }),
        RootFenceAction::BindObjectNamespace { expected } => {
            let object_namespace_id = command
                .object_namespace_id
                .ok_or_else(|| invalid("object namespace binding requires a namespace id"))?;
            Ok(RootFencePlan::Replace {
                expected: encoded(expected, None)?,
                value: encoded(expected, Some(object_namespace_id))?,
            })
        }
        RootFenceAction::RequireActive => Ok(RootFencePlan::Assert {
            expected: encoded(RootActivationState::Active, command.object_namespace_id)?,
        }),
        RootFenceAction::Transition { expected, next } => {
            if !valid_root_transition(expected, next) {
                return Err(MetaError::InvalidRootFenceTransition {
                    from: expected,
                    to: next,
                });
            }
            Ok(RootFencePlan::Replace {
                expected: encoded(expected, command.object_namespace_id)?,
                value: encoded(next, command.object_namespace_id)?,
            })
        }
    }
}

fn synthetic_predicate_plan(command: &MetadataCommand) -> Result<PredicatePlan, MetaError> {
    let version =
        CommitVersion::new(command.read_version.get()).map_err(|_| MetaError::VersionOverflow)?;
    let mut plan = PredicatePlan::default();
    for predicate in &command.predicates {
        match predicate {
            CommandPredicate::Value {
                family,
                key,
                expected,
            } => {
                let map_key = (*family, key.clone());
                if plan.exact.contains_key(&map_key) {
                    return Err(invalid("duplicate exact predicate"));
                }
                let current = expected.as_ref().map(|payload| CurrentValue {
                    created_version: version,
                    modified_version: version,
                    payload: payload.clone(),
                });
                let raw = current
                    .as_ref()
                    .map(CurrentValue::encode)
                    .transpose()
                    .map_err(|error| internal("derive command predicate", error))?;
                plan.exact.insert(
                    map_key,
                    PlannedExactPredicate {
                        family: *family,
                        key: key.clone(),
                        current,
                        raw,
                    },
                );
            }
            CommandPredicate::PrefixEmpty { family, prefix } => {
                plan.prefix_empty.push((*family, prefix.clone()));
            }
        }
    }
    validate_predicate_plan(command, &plan)?;
    Ok(plan)
}

fn validate_predicate_plan(
    command: &MetadataCommand,
    plan: &PredicatePlan,
) -> Result<(), MetaError> {
    for mutation in &command.mutations {
        let Some(predicate) = plan
            .exact
            .get(&(mutation.family(), mutation.key().to_vec()))
        else {
            return Err(invalid(
                "every mutation requires one exact value/absence predicate",
            ));
        };
        if mutation.family() == MetadataFamily::WorkspaceIncarnationClaim
            && (matches!(mutation, CommandMutation::Delete { .. }) || predicate.current.is_some())
        {
            return Err(invalid(
                "workspace incarnation claims are append-only and permanent",
            ));
        }
        if matches!(mutation, CommandMutation::Delete { .. }) && predicate.current.is_none() {
            return Err(invalid("delete mutation requires an existing value"));
        }
    }
    Ok(())
}

fn build_command_txn(
    command: &MetadataCommand,
    state: &CommandTxnState,
) -> Result<WriteTxn, MetaError> {
    let dedupe_key = command_dedupe_key(command.root_id, command.request_id);
    let dedupe_record = CommandDedupeRecord {
        command_digest: command.command_digest,
        commit_version: state.next_version,
        recovery_lsn: state.recovery.row.recovery_lsn,
        deterministic_result: command.deterministic_result.clone(),
    }
    .encode()
    .map_err(|error| internal("derive command dedupe", error))?;

    let mut txn = WriteTxn {
        checks: vec![
            value_check(SYSTEM.id, SYSTEM_SCHEMA_KEY, &state.schema),
            value_check(SYSTEM.id, SYSTEM_SHARD_IDENTITY_KEY, &state.shard),
            value_check(SYSTEM.id, SYSTEM_OWNER_FENCE_KEY, &state.owner),
            value_check(SYSTEM.id, SYSTEM_COMMIT_CLOCK_KEY, &state.clock),
        ],
        mutations: vec![Mutation::Put {
            key: Key::new(SYSTEM.id, SYSTEM_COMMIT_CLOCK_KEY),
            value: encode_system_u64(state.next_version.get()).to_vec(),
        }],
    };
    if let Some(lease_clock) = &state.lease_clock {
        txn.checks.push(value_check(
            SYSTEM.id,
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            lease_clock,
        ));
    }
    enqueue_root_fence(&mut txn, command, &state.root_plan);
    enqueue_predicate_guards(&mut txn, &state.predicate_plan);

    for planned in state.predicate_plan.exact.values() {
        let Some(previous) = &planned.current else {
            continue;
        };
        if !command
            .history_projection
            .iter()
            .any(|projection| projection.family == planned.family && projection.key == planned.key)
        {
            continue;
        }
        let key = history_key(planned.family, &planned.key, state.next_version);
        let value = HistoryValue {
            transition_version: state.next_version,
            previous_created_version: previous.created_version,
            previous_modified_version: previous.modified_version,
            previous_payload: Some(previous.payload.clone()),
        }
        .encode()
        .map_err(|error| internal("derive command history", error))?;
        txn.checks.push(Check::Absent {
            key: Key::new(HISTORY.id, key.clone()),
        });
        txn.mutations.push(Mutation::Put {
            key: Key::new(HISTORY.id, key),
            value,
        });
    }

    for mutation in &command.mutations {
        match mutation {
            CommandMutation::Put { family, key, value } => {
                let planned = state
                    .predicate_plan
                    .exact
                    .get(&(*family, key.clone()))
                    .expect("every mutation has one exact predicate");
                let created_version = planned
                    .current
                    .as_ref()
                    .map(|current| current.created_version)
                    .unwrap_or(state.next_version);
                let encoded = CurrentValue {
                    created_version,
                    modified_version: state.next_version,
                    payload: value.clone(),
                }
                .encode()
                .map_err(|error| internal("derive command value", error))?;
                txn.mutations.push(Mutation::Put {
                    key: Key::new(family.keyspace(), key.clone()),
                    value: encoded,
                });
            }
            CommandMutation::Delete { family, key } => {
                txn.mutations.push(Mutation::Delete {
                    key: Key::new(family.keyspace(), key.clone()),
                });
            }
        }
    }
    for (sequence, projection) in command.event_projection.iter().enumerate() {
        let sequence = u32::try_from(sequence)
            .expect("validated event count fits the event-key sequence width");
        let key = change_event_key(command.root_id, state.next_version, sequence);
        let value = CurrentValue {
            created_version: state.next_version,
            modified_version: state.next_version,
            payload: projection.payload.clone(),
        }
        .encode()
        .map_err(|error| internal("derive command event", error))?;
        txn.checks.push(Check::Absent {
            key: Key::new(CHANGE_EVENT.id, key.clone()),
        });
        txn.mutations.push(Mutation::Put {
            key: Key::new(CHANGE_EVENT.id, key),
            value,
        });
    }
    txn.checks.push(Check::Absent {
        key: Key::new(COMMAND_DEDUPE.id, dedupe_key.clone()),
    });
    txn.mutations.push(Mutation::Put {
        key: Key::new(COMMAND_DEDUPE.id, dedupe_key),
        value: dedupe_record,
    });
    enqueue_recovery(&mut txn, &state.recovery);
    Ok(txn)
}

fn enqueue_root_fence(txn: &mut WriteTxn, command: &MetadataCommand, plan: &RootFencePlan) {
    let key = Key::new(ROOT_FENCE.id, command.root_id.as_bytes());
    match plan {
        RootFencePlan::Install { value } => {
            txn.checks.push(Check::Absent { key: key.clone() });
            txn.mutations.push(Mutation::Put {
                key,
                value: value.clone(),
            });
        }
        RootFencePlan::Assert { expected } => {
            txn.checks.push(Check::Value {
                key,
                expected: expected.clone(),
            });
        }
        RootFencePlan::Replace { expected, value } => {
            txn.checks.push(Check::Value {
                key: key.clone(),
                expected: expected.clone(),
            });
            txn.mutations.push(Mutation::Put {
                key,
                value: value.clone(),
            });
        }
    }
}

fn enqueue_recovery(txn: &mut WriteTxn, plan: &RecoveryPlan) {
    txn.checks.push(value_check(
        SYSTEM.id,
        SYSTEM_APPLIED_RECOVERY_LSN_KEY,
        &plan.lsn_value,
    ));
    txn.checks.push(value_check(
        SYSTEM.id,
        SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
        &plan.digest_value,
    ));
    txn.mutations.push(Mutation::Put {
        key: Key::new(SYSTEM.id, SYSTEM_APPLIED_RECOVERY_LSN_KEY),
        value: encode_system_u64(plan.row.recovery_lsn).to_vec(),
    });
    txn.mutations.push(Mutation::Put {
        key: Key::new(SYSTEM.id, SYSTEM_RECOVERY_CHAIN_DIGEST_KEY),
        value: encode_system_digest(plan.row.chain_digest),
    });
    let header_key = recovery_outbox_key(plan.row.recovery_lsn);
    txn.checks.push(Check::Absent {
        key: Key::new(RECOVERY_OUTBOX.id, header_key),
    });
    txn.mutations.push(Mutation::Put {
        key: Key::new(RECOVERY_OUTBOX.id, header_key),
        value: plan.header.clone(),
    });
    for (index, chunk) in plan.chunks.iter().enumerate() {
        let key = recovery_chunk_key(plan.row.recovery_lsn, index as u32);
        txn.checks.push(Check::Absent {
            key: Key::new(RECOVERY_OUTBOX.id, key),
        });
        txn.mutations.push(Mutation::Put {
            key: Key::new(RECOVERY_OUTBOX.id, key),
            value: chunk.clone(),
        });
    }
}

fn enqueue_predicate_guards(txn: &mut WriteTxn, plan: &PredicatePlan) {
    for predicate in plan.exact.values() {
        let key = Key::new(predicate.family.keyspace(), predicate.key.clone());
        txn.checks.push(match &predicate.raw {
            Some(expected) => Check::Value {
                key,
                expected: expected.clone(),
            },
            None => Check::Absent { key },
        });
    }
    for (family, prefix) in &plan.prefix_empty {
        txn.checks.push(Check::EmptyPrefix {
            keyspace: family.keyspace(),
            prefix: prefix.clone(),
        });
    }
}

fn validate_root_placement(command: &MetadataCommand, fence: RootFence) -> Result<(), MetaError> {
    if validate_root_placement_identity(command, fence).is_ok()
        && fence.object_namespace_id == command.object_namespace_id
    {
        Ok(())
    } else {
        Err(MetaError::PlacementMismatch)
    }
}

fn validate_root_placement_identity(
    command: &MetadataCommand,
    fence: RootFence,
) -> Result<(), MetaError> {
    if fence.logical_shard_id == command.logical_shard_id
        && fence.placement_generation == command.placement_generation
    {
        Ok(())
    } else {
        Err(MetaError::PlacementMismatch)
    }
}

fn valid_root_transition(from: RootActivationState, to: RootActivationState) -> bool {
    matches!(
        (from, to),
        (RootActivationState::Installing, RootActivationState::Active)
            | (RootActivationState::Active, RootActivationState::Draining)
            | (RootActivationState::Active, RootActivationState::Fenced)
            | (RootActivationState::Draining, RootActivationState::Fenced)
    )
}

fn point_source(family: MetadataFamily) -> MetadataPointReadSource {
    match family {
        MetadataFamily::WorkspaceCurrent => MetadataPointReadSource::WorkspaceCurrent,
        MetadataFamily::PathCurrent => MetadataPointReadSource::PathCurrent,
        _ => MetadataPointReadSource::Other,
    }
}

#[cfg(feature = "metadata-read-stats")]
fn byte_len(value: &[u8]) -> u64 {
    u64::try_from(value.len()).unwrap_or(u64::MAX)
}

fn encode_shard_identity(shard: LogicalShardId) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + LogicalShardId::BYTE_WIDTH);
    value.push(SYSTEM_VALUE_FORMAT_VERSION);
    value.extend_from_slice(shard.as_bytes());
    value
}

fn decode_shard_identity(value: &[u8]) -> Result<LogicalShardId, MetaError> {
    if value.len() != 1 + LogicalShardId::BYTE_WIDTH
        || value.first() != Some(&SYSTEM_VALUE_FORMAT_VERSION)
    {
        return Err(MetaError::CorruptRecord {
            record: "System(shard_identity)",
            reason: "invalid version or width".to_owned(),
        });
    }
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&value[1..]);
    Ok(LogicalShardId::from_bytes(bytes))
}

fn encode_system_u64(value: u64) -> [u8; 9] {
    let mut encoded = [0; 9];
    encoded[0] = SYSTEM_VALUE_FORMAT_VERSION;
    encoded[1..].copy_from_slice(&value.to_be_bytes());
    encoded
}

fn decode_system_u64(value: &[u8], record: &'static str) -> Result<u64, MetaError> {
    if value.len() != 9 || value.first() != Some(&SYSTEM_VALUE_FORMAT_VERSION) {
        return Err(MetaError::CorruptRecord {
            record,
            reason: "invalid version or width".to_owned(),
        });
    }
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&value[1..]);
    Ok(u64::from_be_bytes(bytes))
}

fn encode_system_digest(value: [u8; RECOVERY_CHAIN_DIGEST_BYTES]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + RECOVERY_CHAIN_DIGEST_BYTES);
    encoded.push(SYSTEM_VALUE_FORMAT_VERSION);
    encoded.extend_from_slice(&value);
    encoded
}

fn decode_system_digest(
    value: &[u8],
    record: &'static str,
) -> Result<[u8; RECOVERY_CHAIN_DIGEST_BYTES], MetaError> {
    if value.len() != 1 + RECOVERY_CHAIN_DIGEST_BYTES
        || value.first() != Some(&SYSTEM_VALUE_FORMAT_VERSION)
    {
        return Err(MetaError::CorruptRecord {
            record,
            reason: "invalid version or width".to_owned(),
        });
    }
    let mut digest = [0; RECOVERY_CHAIN_DIGEST_BYTES];
    digest.copy_from_slice(&value[1..]);
    Ok(digest)
}

fn command_dedupe_key(root: RootId, request: RequestId) -> Vec<u8> {
    [root.as_bytes().as_slice(), request.as_bytes().as_slice()].concat()
}

fn history_prefix(family: MetadataFamily, key: &[u8]) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(1 + 4 + key.len());
    prefix.push(family.format_tag());
    prefix.extend_from_slice(
        &u32::try_from(key.len())
            .expect("validated metadata key length fits u32")
            .to_be_bytes(),
    );
    prefix.extend_from_slice(key);
    prefix
}

fn history_key(family: MetadataFamily, key: &[u8], transition_version: CommitVersion) -> Vec<u8> {
    let mut encoded = history_prefix(family, key);
    encoded.extend_from_slice(&(!transition_version.get()).to_be_bytes());
    encoded
}

fn history_user_key(encoded: &[u8]) -> Result<&[u8], MetaError> {
    const HEADER_BYTES: usize = 1 + 4;
    const VERSION_BYTES: usize = 8;
    if encoded.len() < HEADER_BYTES + VERSION_BYTES {
        return Err(MetaError::CorruptRecord {
            record: "History key",
            reason: "key is truncated".to_owned(),
        });
    }
    let mut length = [0; 4];
    length.copy_from_slice(&encoded[1..HEADER_BYTES]);
    let user_key_bytes = u32::from_be_bytes(length) as usize;
    let expected = HEADER_BYTES
        .checked_add(user_key_bytes)
        .and_then(|length| length.checked_add(VERSION_BYTES))
        .ok_or_else(|| MetaError::CorruptRecord {
            record: "History key",
            reason: "key length overflow".to_owned(),
        })?;
    if encoded.len() != expected {
        return Err(MetaError::CorruptRecord {
            record: "History key",
            reason: format!("expected {expected} bytes, found {}", encoded.len()),
        });
    }
    Ok(&encoded[HEADER_BYTES..HEADER_BYTES + user_key_bytes])
}

fn validate_root_scoped_bytes(
    root: RootId,
    value: &[u8],
    kind: &'static str,
) -> Result<(), MetaError> {
    if value.len() > MAX_COMMAND_KEY_BYTES {
        return Err(invalid(format!("{kind} exceeds size bound")));
    }
    if !value.starts_with(root.as_bytes()) {
        return Err(invalid(format!("{kind} is outside command root")));
    }
    Ok(())
}

fn validate_value_bytes(value: &[u8], kind: &'static str) -> Result<(), MetaError> {
    if value.len() > MAX_COMMAND_VALUE_BYTES {
        Err(invalid(format!("{kind} exceeds size bound")))
    } else {
        Ok(())
    }
}

fn system_bootstrap_rows(shard: LogicalShardId) -> Vec<(&'static [u8], Vec<u8>)> {
    vec![
        (SYSTEM_SCHEMA_KEY, encode_schema_marker()),
        (SYSTEM_SHARD_IDENTITY_KEY, encode_shard_identity(shard)),
        (SYSTEM_OWNER_FENCE_KEY, encode_system_u64(0).to_vec()),
        (
            SYSTEM_COMMIT_CLOCK_KEY,
            encode_system_u64(INITIAL_COMMIT_VERSION).to_vec(),
        ),
        (
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            encode_system_u64(0).to_vec(),
        ),
        (
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            encode_system_u64(0).to_vec(),
        ),
        (
            SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
            encode_system_digest(recovery_genesis_digest(shard)),
        ),
    ]
}

fn validate_store_profile(actual: StoreLimits) -> Result<(), MetaError> {
    let required = store_limits();
    for (name, available, needed) in [
        ("reads", actual.max_reads, required.max_reads),
        ("checks", actual.max_checks, required.max_checks),
        ("mutations", actual.max_mutations, required.max_mutations),
        ("key bytes", actual.max_key_bytes, required.max_key_bytes),
        (
            "value bytes",
            actual.max_value_bytes,
            required.max_value_bytes,
        ),
        ("read bytes", actual.max_read_bytes, required.max_read_bytes),
        (
            "transaction bytes",
            actual.max_transaction_bytes,
            required.max_transaction_bytes,
        ),
        (
            "result rows",
            actual.max_result_rows,
            required.max_result_rows,
        ),
        (
            "result bytes",
            actual.max_result_bytes,
            required.max_result_bytes,
        ),
    ] {
        if available < needed {
            return Err(MetaError::SchemaGate {
                reason: format!(
                    "metadata store supports {available} {name}, serving schema requires {needed}"
                ),
            });
        }
    }
    Ok(())
}

fn command_limit(kind: LimitKind) -> Result<CommandLimit, MetaError> {
    match kind {
        LimitKind::Checks => Ok(CommandLimit::Checks),
        LimitKind::Mutations => Ok(CommandLimit::Mutations),
        LimitKind::KeyBytes => Ok(CommandLimit::KeyBytes),
        LimitKind::ValueBytes => Ok(CommandLimit::ValueBytes),
        LimitKind::TransactionBytes => Ok(CommandLimit::TransactionBytes),
        LimitKind::Reads
        | LimitKind::ReadBytes
        | LimitKind::ResultRows
        | LimitKind::ResultBytes => Err(internal(
            "derive command fit",
            format!("write transaction reported {kind}"),
        )),
    }
}

fn value_check(keyspace: Keyspace, key: &[u8], expected: &[u8]) -> Check {
    Check::Value {
        key: Key::new(keyspace, key),
        expected: expected.to_vec(),
    }
}

fn required_get_result(
    result: Option<ReadResult>,
    record: &'static str,
) -> Result<Vec<u8>, MetaError> {
    let Some(ReadResult::Get(value)) = result else {
        return Err(corrupt("transaction-store read", "point result is missing"));
    };
    value.ok_or_else(|| MetaError::CorruptRecord {
        record,
        reason: "record is missing".to_owned(),
    })
}

fn scan_request(
    keyspace: Keyspace,
    prefix: &[u8],
    after: Option<&[u8]>,
    limit: usize,
    delimiter: Option<u8>,
    reserved_gets: usize,
) -> Result<Scan, MetaError> {
    let limits = store_limits();
    let reserved_bytes = reserved_gets
        .checked_mul(limits.max_value_bytes)
        .ok_or_else(|| internal("build scan", "point-read reserve overflow"))?;
    let max_bytes = limits
        .max_result_bytes
        .checked_sub(reserved_bytes)
        .ok_or_else(|| internal("build scan", "point reads exhaust result budget"))?;
    let minimum_row = limits
        .max_key_bytes
        .checked_add(limits.max_value_bytes)
        .ok_or_else(|| internal("build scan", "maximum row size overflow"))?;
    if max_bytes < minimum_row {
        return Err(internal(
            "build scan",
            "remaining result budget cannot hold one maximum row",
        ));
    }
    Ok(Scan {
        keyspace,
        prefix: prefix.to_vec(),
        after: after.map(ToOwned::to_owned),
        limit,
        max_bytes,
        delimiter,
    })
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hash_u64(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u64).to_be_bytes());
}

fn invalid(reason: impl Into<String>) -> MetaError {
    MetaError::InvalidCommand {
        reason: reason.into(),
    }
}

fn corrupt(record: &'static str, error: impl std::fmt::Display) -> MetaError {
    MetaError::CorruptRecord {
        record,
        reason: error.to_string(),
    }
}

fn store_error(operation: &'static str, source: StoreError) -> MetaError {
    MetaError::Store { operation, source }
}

fn internal(operation: &'static str, error: impl std::fmt::Display) -> MetaError {
    MetaError::Internal {
        operation,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use nokv_meta_store::{AckBoundary, Authority, LimitKind, StoreProfile, UnknownCommit};
    use tempfile::tempdir;

    use super::super::query_records::{ChangeEventKind, ChangeEventRecord, TypedProjection};
    use super::*;

    struct UnavailableCommitStore {
        inner: Arc<dyn TxnStore>,
        fail_next_commit: AtomicBool,
    }

    impl TxnStore for UnavailableCommitStore {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            self.inner.read(batch)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            if self.fail_next_commit.swap(false, Ordering::AcqRel) {
                return Err(StoreError::Unavailable(
                    "injected definitely-not-applied commit".to_owned(),
                ));
            }
            self.inner.commit(txn)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    struct AppliedThenLostAckStore {
        inner: Arc<dyn TxnStore>,
        lose_next_ack: AtomicBool,
    }

    impl TxnStore for AppliedThenLostAckStore {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            self.inner.read(batch)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            let lose_ack = self.lose_next_ack.swap(false, Ordering::AcqRel);
            let outcome = self.inner.commit(txn)?;
            if lose_ack && outcome == Commit::Applied {
                return Err(StoreError::OutcomeUnknown {
                    state: UnknownCommit::Settled,
                    reason: "injected acknowledgement loss after apply".to_owned(),
                });
            }
            Ok(outcome)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    struct AdvanceOwnerBeforeDataRead {
        inner: Arc<dyn TxnStore>,
        controller: MetaShard,
        armed: AtomicBool,
    }

    impl TxnStore for AdvanceOwnerBeforeDataRead {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            let reads_operation = batch.ops.iter().any(|op| match op {
                ReadOp::Get(key) => key.keyspace == MetadataFamily::Operation.keyspace(),
                ReadOp::Scan(scan) => scan.keyspace == MetadataFamily::Operation.keyspace(),
            });
            if reads_operation && self.armed.swap(false, Ordering::AcqRel) {
                self.controller
                    .advance_owner_epoch(Some(epoch(1)), epoch(2))
                    .expect("injected owner advancement must succeed");
            }
            self.inner.read(batch)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            self.inner.commit(txn)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    struct ShortScanPageStore {
        inner: Arc<dyn TxnStore>,
        max_scan_items: usize,
        operation_scan_reads: AtomicUsize,
        advance_clock_on_second_scan: AtomicBool,
        controller: Option<MetaShard>,
    }

    impl TxnStore for ShortScanPageStore {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            let scans_operation = batch.ops.iter().any(|op| {
                matches!(
                    op,
                    ReadOp::Scan(scan)
                        if scan.keyspace == MetadataFamily::Operation.keyspace()
                )
            });
            if scans_operation {
                let scan_index = self.operation_scan_reads.fetch_add(1, Ordering::AcqRel);
                if scan_index == 1
                    && self
                        .advance_clock_on_second_scan
                        .swap(false, Ordering::AcqRel)
                {
                    let controller = self
                        .controller
                        .as_ref()
                        .expect("clock advancement requires a controller");
                    controller
                        .execute(&create_command(
                            controller,
                            request(250),
                            scoped_key(root(2), b"clock-short-page/1-late"),
                            b"late",
                        ))
                        .expect("injected clock advancement must succeed");
                }
            }

            let mut snapshot = self.inner.read(batch.clone())?;
            for (op, result) in batch.ops.iter().zip(&mut snapshot.results) {
                let (ReadOp::Scan(scan), ReadResult::Scan(page)) = (op, result) else {
                    continue;
                };
                if scan.keyspace != MetadataFamily::Operation.keyspace()
                    || page.items.len() <= self.max_scan_items
                {
                    continue;
                }
                page.items.truncate(self.max_scan_items);
                page.more = true;
            }
            Ok(snapshot)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            self.inner.commit(txn)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    struct HistoricalScanClockChurnStore {
        inner: Arc<dyn TxnStore>,
        controller: MetaShard,
        remaining_advances: AtomicUsize,
        history_scan_reads: AtomicUsize,
    }

    impl TxnStore for HistoricalScanClockChurnStore {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            let scans_history = batch
                .ops
                .iter()
                .any(|op| matches!(op, ReadOp::Scan(scan) if scan.keyspace == HISTORY.id));
            if scans_history {
                let scan_index = self.history_scan_reads.fetch_add(1, Ordering::AcqRel);
                if self
                    .remaining_advances
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    let request_fill = u8::try_from(200 + scan_index)
                        .expect("bounded historical scan attempts fit one request byte");
                    let key = scoped_key(
                        root(2),
                        format!("historical-clock-churn/{scan_index}").as_bytes(),
                    );
                    self.controller
                        .execute(&create_command(
                            &self.controller,
                            request(request_fill),
                            key,
                            b"advance-clock",
                        ))
                        .expect("injected clock advancement must succeed");
                }
            }
            self.inner.read(batch)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            self.inner.commit(txn)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    struct RecoveryScanChurnStore {
        inner: Arc<dyn TxnStore>,
        controller: MetaShard,
        skipped_recovery_scans: AtomicUsize,
        remaining_advances: AtomicUsize,
        recovery_scan_reads: AtomicUsize,
    }

    impl TxnStore for RecoveryScanChurnStore {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            let scans_recovery = batch
                .ops
                .iter()
                .any(|op| matches!(op, ReadOp::Scan(scan) if scan.keyspace == RECOVERY_OUTBOX.id));
            if scans_recovery {
                let scan_index = self.recovery_scan_reads.fetch_add(1, Ordering::AcqRel);
                let skip = self
                    .skipped_recovery_scans
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok();
                if !skip
                    && self
                        .remaining_advances
                        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                            remaining.checked_sub(1)
                        })
                        .is_ok()
                {
                    let request_fill = u8::try_from(200 + scan_index)
                        .expect("bounded recovery scan attempts fit one request byte");
                    let key = scoped_key(
                        root(2),
                        format!("recovery-scan-churn/{scan_index}").as_bytes(),
                    );
                    self.controller
                        .execute(&create_command(
                            &self.controller,
                            request(request_fill),
                            key,
                            b"advance-recovery-tail",
                        ))
                        .expect("injected recovery-tail advancement must succeed");
                }
            }
            self.inner.read(batch)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            self.inner.commit(txn)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    struct ProfileOnlyStore {
        limits: StoreLimits,
    }

    impl TxnStore for ProfileOnlyStore {
        fn profile(&self) -> StoreProfile {
            StoreProfile {
                limits: self.limits,
                ack: AckBoundary::LocalSync,
                authority: Authority::Local,
            }
        }

        fn read(&self, _batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            panic!("profile rejection must happen before store I/O")
        }

        fn commit(&self, _txn: WriteTxn) -> Result<Commit, StoreError> {
            panic!("profile rejection must happen before store I/O")
        }

        fn ready(&self) -> Result<(), StoreError> {
            panic!("profile rejection must happen before readiness I/O")
        }
    }

    fn shard(fill: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([fill; 16])
    }

    fn root(fill: u8) -> RootId {
        RootId::from_bytes([fill; 16])
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; 16])
    }

    fn generation(value: u64) -> PlacementGeneration {
        PlacementGeneration::new(value).unwrap()
    }

    fn epoch(value: u64) -> OwnerEpoch {
        OwnerEpoch::new(value).unwrap()
    }

    fn scoped_key(root: RootId, suffix: &[u8]) -> Vec<u8> {
        [root.as_bytes().as_slice(), suffix].concat()
    }

    fn base_command(
        store: &MetaShard,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(2),
            logical_shard_id: shard(1),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([10; 16])),
            placement_generation: generation(7),
            owner_epoch: epoch(1),
            request_id,
            command_digest: CommandDigest::from_bytes([0; 32]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
    }

    fn ready_store() -> MetaShard {
        let store = crate::workspace::test_support::memory(shard(1)).unwrap();
        make_store_ready(store)
    }

    fn ready_file_store(path: &std::path::Path) -> MetaShard {
        let store = crate::workspace::test_support::initialize_file(path, shard(1)).unwrap();
        make_store_ready(store)
    }

    fn make_store_ready(store: MetaShard) -> MetaShard {
        store.advance_owner_epoch(None, epoch(1)).unwrap();
        let install = base_command(&store, request(1), RootFenceAction::Install).seal();
        let installed = store.execute(&install).unwrap();
        assert_eq!(installed.commit_version.get(), 2);
        let activate = base_command(
            &store,
            request(2),
            RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
        )
        .seal();
        let activated = store.execute(&activate).unwrap();
        assert_eq!(activated.commit_version.get(), 3);
        store
    }

    fn recovery_scan_churn_reader() -> (MetaShard, Arc<RecoveryScanChurnStore>) {
        let controller = ready_store();
        let wrapper = Arc::new(RecoveryScanChurnStore {
            inner: Arc::clone(&controller.store),
            controller,
            skipped_recovery_scans: AtomicUsize::new(0),
            remaining_advances: AtomicUsize::new(0),
            recovery_scan_reads: AtomicUsize::new(0),
        });
        let reader = MetaShard::open(wrapper.clone(), shard(1)).unwrap();
        (reader, wrapper)
    }

    fn create_command(
        store: &MetaShard,
        request_id: RequestId,
        key: Vec<u8>,
        value: &[u8],
    ) -> MetadataCommand {
        let mut command = base_command(store, request_id, RootFenceAction::RequireActive);
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: None,
        });
        command.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key,
            value: value.to_vec(),
        });
        command.deterministic_result = b"created".to_vec();
        command.event_projection.push(EventProjection {
            payload: ChangeEventRecord {
                workbench_id: nokv_types::WorkbenchId::new("engine-event").unwrap(),
                workspace_incarnation_id: nokv_types::WorkspaceIncarnationId::from_bytes([3; 16]),
                kind: ChangeEventKind::WorkspaceCreated,
                artifact_revision_id: None,
                commit_id: None,
                operation_id: None,
                path: None,
                before: TypedProjection::empty(),
                after: TypedProjection::empty(),
            }
            .encode()
            .unwrap(),
        });
        command.seal()
    }

    fn read_operation(store: &MetaShard, key: &[u8], version: CommitVersion) -> Option<Vec<u8>> {
        store
            .read_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                key,
                ReadVersion::new(version.get()).unwrap(),
            )
            .unwrap()
    }

    fn raw_put(store: &MetaShard, keyspace: Keyspace, key: &[u8], value: &[u8]) {
        assert_eq!(
            store
                .commit(
                    "inject test row",
                    WriteTxn {
                        checks: Vec::new(),
                        mutations: vec![Mutation::Put {
                            key: Key::new(keyspace, key),
                            value: value.to_vec(),
                        }],
                    },
                )
                .unwrap(),
            Commit::Applied
        );
    }

    fn raw_delete(store: &MetaShard, keyspace: Keyspace, key: &[u8]) {
        assert_eq!(
            store
                .commit(
                    "delete test row",
                    WriteTxn {
                        checks: Vec::new(),
                        mutations: vec![Mutation::Delete {
                            key: Key::new(keyspace, key),
                        }],
                    },
                )
                .unwrap(),
            Commit::Applied
        );
    }

    type TxnShapeRow = (u8, u16, usize, usize);

    fn txn_shape(txn: &WriteTxn) -> (Vec<TxnShapeRow>, Vec<TxnShapeRow>) {
        let checks = txn
            .checks
            .iter()
            .map(|check| match check {
                Check::Value { key, expected } => {
                    (0, key.keyspace.get(), key.bytes.len(), expected.len())
                }
                Check::Absent { key } => (1, key.keyspace.get(), key.bytes.len(), 0),
                Check::EmptyPrefix { keyspace, prefix } => (2, keyspace.get(), prefix.len(), 0),
            })
            .collect();
        let mutations = txn
            .mutations
            .iter()
            .map(|mutation| match mutation {
                Mutation::Put { key, value } => {
                    (0, key.keyspace.get(), key.bytes.len(), value.len())
                }
                Mutation::Delete { key } => (1, key.keyspace.get(), key.bytes.len(), 0),
            })
            .collect();
        (checks, mutations)
    }

    fn capture_next_commit(
        store: &mut MetaShard,
    ) -> Arc<crate::workspace::test_support::CommitCaptureStore> {
        let (wrapped, capture) =
            crate::workspace::test_support::capture_txn_store(Arc::clone(&store.store));
        store.store = wrapped;
        capture
    }

    fn put_history_row(
        store: &MetaShard,
        key: &[u8],
        transition_version: u64,
        previous_modified_version: u64,
        previous_payload: &[u8],
    ) {
        let transition_version = CommitVersion::new(transition_version).unwrap();
        let value = HistoryValue {
            transition_version,
            previous_created_version: CommitVersion::new(2).unwrap(),
            previous_modified_version: CommitVersion::new(previous_modified_version).unwrap(),
            previous_payload: Some(previous_payload.to_vec()),
        }
        .encode()
        .unwrap();
        raw_put(
            store,
            HISTORY.id,
            &history_key(MetadataFamily::Operation, key, transition_version),
            &value,
        );
    }

    #[test]
    fn fresh_store_freezes_schema_shard_and_bootstrap_version() {
        let store = crate::workspace::test_support::memory(shard(1)).unwrap();
        assert_eq!(keyspaces().len(), 29);
        assert_eq!(store.current_read_version().unwrap().get(), 1);
        assert_eq!(
            store.advance_owner_epoch(Some(epoch(1)), epoch(2)),
            Err(MetaError::OwnerEpochMismatch {
                expected: 1,
                actual: 0,
            })
        );
        store.advance_owner_epoch(None, epoch(1)).unwrap();
        store.advance_owner_epoch(None, epoch(1)).unwrap();
    }

    #[test]
    fn new_root_fence_install_requires_an_object_namespace_before_mutation() {
        let store = crate::workspace::test_support::memory(shard(1)).unwrap();
        store.advance_owner_epoch(None, epoch(1)).unwrap();
        let before = store.recovery_state().unwrap();
        let mut install = base_command(&store, request(1), RootFenceAction::Install);
        install.object_namespace_id = None;
        let install = install.seal();

        let error = store
            .execute(&install)
            .expect_err("a production install must not recreate a legacy unbound fence");

        assert!(
            matches!(&error, MetaError::InvalidCommand { reason }
                if reason.contains("object namespace")),
            "unexpected error: {error:?}"
        );
        assert_eq!(store.root_fence(root(2)).unwrap(), None);
        assert_eq!(store.recovery_state().unwrap(), before);
    }

    #[test]
    fn initialization_rejects_a_store_below_the_serving_profile_before_io() {
        let mut limits = store_limits();
        limits.max_key_bytes -= 1;
        let error = match MetaShard::initialize(Arc::new(ProfileOnlyStore { limits }), shard(1)) {
            Ok(_) => panic!("a smaller physical profile must not bind"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MetaError::SchemaGate { reason }
                if reason.contains("key bytes") && reason.contains("8205")
        ));
    }

    #[test]
    fn maximum_domain_key_leaves_exact_history_key_headroom() {
        let store = ready_store();
        let mut key = root(2).as_bytes().to_vec();
        key.resize(MAX_COMMAND_KEY_BYTES, b'k');
        let history = history_key(
            MetadataFamily::Operation,
            &key,
            CommitVersion::new(5).unwrap(),
        );
        assert_eq!(history.len(), MAX_DERIVED_KEY_BYTES);
        assert_eq!(store_limits().max_key_bytes, MAX_DERIVED_KEY_BYTES);

        store
            .execute(&create_command(&store, request(80), key.clone(), b"first"))
            .unwrap();
        let mut replace = base_command(&store, request(81), RootFenceAction::RequireActive);
        replace.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"first".to_vec()),
        });
        replace.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key: key.clone(),
            value: b"second".to_vec(),
        });
        replace.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key,
        });
        store.execute(&replace.seal()).unwrap();
    }

    #[test]
    fn command_fit_uses_no_store_io() {
        let mut store = ready_store();
        let mut command = create_command(
            &store,
            request(251),
            scoped_key(root(2), b"fit-pure"),
            b"value",
        );
        command.deterministic_result = vec![0x5a; MAX_DETERMINISTIC_RESULT_BYTES];
        let command = command.seal();
        store.store = Arc::new(ProfileOnlyStore {
            limits: store_limits(),
        });

        assert_eq!(store.command_fit(&command, None), Ok(CommandFit::Fits));
        let txn = store.command_txn_for_fit(&command, None).unwrap();
        let recovery_rows = txn
            .mutations
            .iter()
            .filter(|mutation| match mutation {
                Mutation::Put { key, .. } | Mutation::Delete { key } => {
                    key.keyspace == RECOVERY_OUTBOX.id
                }
            })
            .count();
        assert!(
            recovery_rows > 2,
            "maximum deterministic result must span multiple recovery chunks"
        );
    }

    #[test]
    fn command_fit_matches_create_event_and_multichunk_transaction_shape() {
        let mut store = ready_store();
        let mut command = create_command(
            &store,
            request(252),
            scoped_key(root(2), b"fit-create"),
            b"value",
        );
        command.deterministic_result = vec![0xa5; MAX_DETERMINISTIC_RESULT_BYTES];
        let command = command.seal();
        let predicted = store.command_txn_for_fit(&command, None).unwrap();
        assert_eq!(store.command_fit(&command, None), Ok(CommandFit::Fits));

        let capture = capture_next_commit(&mut store);
        store.execute(&command).unwrap();
        capture.with_last_commit(|actual| {
            assert_eq!(txn_shape(&predicted), txn_shape(actual));
            assert_eq!(actual.validate(&store_limits()), Ok(()));
        });
        assert_eq!(predicted.validate(&store_limits()), Ok(()));
    }

    #[test]
    fn command_fit_matches_replace_history_transaction_shape() {
        let mut store = ready_store();
        let key = scoped_key(root(2), b"fit-replace");
        store
            .execute(&create_command(&store, request(253), key.clone(), b"first"))
            .unwrap();
        let mut command = base_command(&store, request(254), RootFenceAction::RequireActive);
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"first".to_vec()),
        });
        command.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key: key.clone(),
            value: b"second".to_vec(),
        });
        command.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key,
        });
        let command = command.seal();
        let predicted = store.command_txn_for_fit(&command, None).unwrap();
        assert_eq!(store.command_fit(&command, None), Ok(CommandFit::Fits));

        let capture = capture_next_commit(&mut store);
        store.execute(&command).unwrap();
        capture.with_last_commit(|actual| {
            assert_eq!(txn_shape(&predicted), txn_shape(actual));
            assert!(actual.mutations.iter().any(|mutation| matches!(
                mutation,
                Mutation::Put { key, .. } if key.keyspace == HISTORY.id
            )));
            assert_eq!(actual.validate(&store_limits()), Ok(()));
        });
        assert_eq!(predicted.validate(&store_limits()), Ok(()));
    }

    #[test]
    fn derived_transaction_limit_failure_has_no_state_change() {
        let store = ready_store();
        let version_before = store.current_read_version().unwrap();
        let recovery_before = store.recovery_state().unwrap();
        let mut command = base_command(&store, request(82), RootFenceAction::RequireActive);
        for index in 0..160 {
            let key = scoped_key(root(2), format!("serving-limit/{index:02}").as_bytes());
            command.predicates.push(CommandPredicate::Value {
                family: MetadataFamily::Operation,
                key: key.clone(),
                expected: None,
            });
            command.mutations.push(CommandMutation::Put {
                family: MetadataFamily::Operation,
                key,
                value: vec![index as u8; MAX_COMMAND_VALUE_BYTES],
            });
        }
        let command = command.seal();
        assert!(matches!(
            store.command_fit(&command, None),
            Ok(CommandFit::Exceeds {
                kind: CommandLimit::TransactionBytes,
                maximum: 16_000_000,
                ..
            })
        ));
        let error = store
            .execute(&command)
            .expect_err("fully derived transaction must exceed the serving byte envelope");
        assert!(matches!(
            error,
            MetaError::Store {
                operation: "execute metadata command",
                source: StoreError::LimitExceeded {
                    kind: LimitKind::TransactionBytes,
                    maximum: 16_000_000,
                    ..
                },
            }
        ));
        assert_eq!(store.current_read_version().unwrap(), version_before);
        assert_eq!(store.recovery_state().unwrap(), recovery_before);
    }

    #[test]
    fn applied_then_lost_ack_remains_typed_and_reconciles_by_request() {
        let mut store = ready_store();
        let wrapper = Arc::new(AppliedThenLostAckStore {
            inner: Arc::clone(&store.store),
            lose_next_ack: AtomicBool::new(false),
        });
        store.store = wrapper.clone();
        let command = create_command(
            &store,
            request(83),
            scoped_key(root(2), b"unknown-outcome"),
            b"value",
        );
        let before = store.recovery_state().unwrap();
        wrapper.lose_next_ack.store(true, Ordering::Release);
        assert!(matches!(
            store.execute(&command),
            Err(MetaError::Store {
                operation: "execute metadata command",
                source: StoreError::OutcomeUnknown {
                    state: UnknownCommit::Settled,
                    ..
                },
            })
        ));
        let after_unknown = store.recovery_state().unwrap();
        assert_eq!(
            after_unknown.applied_recovery_lsn,
            before.applied_recovery_lsn + 1
        );

        let reconciled = store.execute(&command).unwrap();
        assert!(reconciled.replayed);
        assert_eq!(reconciled.recovery_lsn, after_unknown.applied_recovery_lsn);
        assert_eq!(reconciled.recovery_chain_digest, after_unknown.chain_digest);
    }

    #[test]
    fn point_data_and_owner_fence_share_one_store_snapshot() {
        let store = ready_store();
        let key = scoped_key(root(2), b"owner-race");
        store
            .execute(&create_command(&store, request(84), key.clone(), b"value"))
            .unwrap();
        let version = store.current_read_version().unwrap();
        let inner = Arc::clone(&store.store);
        let controller = MetaShard::open(Arc::clone(&inner), shard(1)).unwrap();
        let wrapper = Arc::new(AdvanceOwnerBeforeDataRead {
            inner,
            controller: controller.clone(),
            armed: AtomicBool::new(false),
        });
        let stale = MetaShard::open(wrapper.clone(), shard(1)).unwrap();
        wrapper.armed.store(true, Ordering::Release);

        assert_eq!(
            stale.read_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &key,
                version,
            ),
            Err(MetaError::OwnerEpochMismatch {
                expected: 1,
                actual: 2,
            })
        );
        assert_eq!(controller.current_read_version().unwrap(), version);
    }

    #[test]
    fn file_reopen_rejects_a_different_logical_shard() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        {
            let store =
                crate::workspace::test_support::initialize_file(&database, shard(1)).unwrap();
            store.advance_owner_epoch(None, epoch(1)).unwrap();
        }
        assert!(matches!(
            crate::workspace::test_support::open_file(&database, shard(2)),
            Err(MetaError::SchemaGate { .. })
        ));
        let reopened = crate::workspace::test_support::open_file(&database, shard(1)).unwrap();
        assert_eq!(reopened.current_read_version().unwrap().get(), 1);
    }

    #[test]
    fn root_install_activate_and_stale_fences_fail_closed() {
        let store = ready_store();
        let duplicate_install = base_command(&store, request(3), RootFenceAction::Install).seal();
        assert_eq!(
            store.execute(&duplicate_install),
            Err(MetaError::RootFenceAlreadyInstalled)
        );

        let mut stale_placement = create_command(
            &store,
            request(4),
            scoped_key(root(2), b"operation"),
            b"value",
        );
        stale_placement.placement_generation = generation(8);
        stale_placement = stale_placement.seal();
        assert_eq!(
            store.execute(&stale_placement),
            Err(MetaError::PlacementMismatch)
        );

        store.advance_owner_epoch(Some(epoch(1)), epoch(2)).unwrap();
        let stale_owner =
            create_command(&store, request(5), scoped_key(root(2), b"other"), b"value");
        assert_eq!(
            store.execute(&stale_owner),
            Err(MetaError::OwnerEpochMismatch {
                expected: 1,
                actual: 2,
            })
        );
    }

    #[test]
    fn exact_replay_returns_the_original_result_and_mismatch_fails() {
        let store = ready_store();
        let key = scoped_key(root(2), b"publish");
        let command = create_command(&store, request(3), key.clone(), b"v1");
        let first = store.execute(&command).unwrap();
        assert!(!first.replayed);
        assert_eq!(first.deterministic_result, b"created");

        let replay = store.execute(&command).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);
        assert_eq!(replay.deterministic_result, first.deterministic_result);
        let durable = store
            .lookup_request(root(2), generation(7), epoch(1), request(3))
            .unwrap()
            .unwrap();
        assert_eq!(durable.command_digest, command.command_digest);
        assert_eq!(durable.commit_version, first.commit_version);
        assert_eq!(durable.deterministic_result, b"created");
        assert!(store
            .lookup_request(root(2), generation(7), epoch(1), request(99))
            .unwrap()
            .is_none());

        let mut mismatch = command.clone();
        mismatch.deterministic_result = b"different".to_vec();
        mismatch = mismatch.seal();
        assert_eq!(store.execute(&mismatch), Err(MetaError::RequestIdReused));
        assert_eq!(
            read_operation(&store, &key, first.commit_version),
            Some(b"v1".to_vec())
        );
    }

    #[test]
    fn fenced_point_reader_rejects_a_foreign_root_key() {
        let store = ready_store();
        let result =
            store.with_fenced_point_reads(root(2), generation(7), epoch(1), None, |_, reader| {
                reader.get(MetadataFamily::Operation, &scoped_key(root(3), b"foreign"))
            });
        assert!(matches!(result, Err(MetaError::InvalidCommand { .. })));
    }

    #[test]
    fn current_fenced_point_miss_does_not_consult_history() {
        let store = ready_store();
        let key = scoped_key(root(2), b"missing-current");
        let history_key = history_key(
            MetadataFamily::Operation,
            &key,
            CommitVersion::new(2).unwrap(),
        );
        raw_put(&store, HISTORY.id, &history_key, b"corrupt-history");

        let value = store
            .with_fenced_point_reads(root(2), generation(7), epoch(1), None, |_, reader| {
                reader.get(MetadataFamily::Operation, &key)
            })
            .unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn current_prefix_scan_rejects_a_row_from_a_future_version() {
        let store = ready_store();
        let key = scoped_key(root(2), b"future-current");
        let current = store.current_read_version().unwrap();
        let future = CommitVersion::new(current.get() + 1).unwrap();
        let encoded = CurrentValue {
            created_version: future,
            modified_version: future,
            payload: b"impossible".to_vec(),
        }
        .encode()
        .unwrap();
        raw_put(&store, MetadataFamily::Operation.keyspace(), &key, &encoded);

        assert!(matches!(
            store.scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                root(2).as_bytes(),
                current,
                None,
                1,
            ),
            Err(MetaError::CorruptRecord {
                record: "operation",
                ..
            })
        ));
    }

    #[test]
    fn current_prefix_scan_resumes_short_store_pages() {
        let mut store = ready_store();
        let prefix = scoped_key(root(2), b"short-page/");
        let mut expected = Vec::new();
        for index in 0..5_u8 {
            let key = [prefix.as_slice(), format!("{index}").as_bytes()].concat();
            let value = format!("value-{index}").into_bytes();
            store
                .execute(&create_command(
                    &store,
                    request(20 + index),
                    key.clone(),
                    &value,
                ))
                .unwrap();
            expected.push(MetadataScanItem { key, value });
        }
        let version = store.current_read_version().unwrap();
        let wrapper = Arc::new(ShortScanPageStore {
            inner: Arc::clone(&store.store),
            max_scan_items: 2,
            operation_scan_reads: AtomicUsize::new(0),
            advance_clock_on_second_scan: AtomicBool::new(false),
            controller: None,
        });
        store.store = wrapper.clone();

        let actual = store
            .scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                version,
                None,
                expected.len(),
            )
            .unwrap();

        assert_eq!(actual, expected);
        assert_eq!(wrapper.operation_scan_reads.load(Ordering::Acquire), 3);
    }

    #[test]
    fn current_delimited_scan_resumes_from_common_prefix_cursor() {
        let mut store = ready_store();
        let prefix = scoped_key(root(2), b"short-delimited/");
        let common_a = [prefix.as_slice(), b"a/"].concat();
        let common_b = [prefix.as_slice(), b"b/"].concat();
        let direct_c = [prefix.as_slice(), b"c"].concat();
        for (index, (suffix, value)) in [
            (b"a/one".as_slice(), b"a-one".as_slice()),
            (b"a/two".as_slice(), b"a-two".as_slice()),
            (b"b/one".as_slice(), b"b-one".as_slice()),
            (b"c".as_slice(), b"c".as_slice()),
        ]
        .into_iter()
        .enumerate()
        {
            store
                .execute(&create_command(
                    &store,
                    request(30 + index as u8),
                    [prefix.as_slice(), suffix].concat(),
                    value,
                ))
                .unwrap();
        }
        let version = store.current_read_version().unwrap();
        let wrapper = Arc::new(ShortScanPageStore {
            inner: Arc::clone(&store.store),
            max_scan_items: 1,
            operation_scan_reads: AtomicUsize::new(0),
            advance_clock_on_second_scan: AtomicBool::new(false),
            controller: None,
        });
        store.store = wrapper.clone();

        let actual = store
            .scan_delimited_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                b'/',
                version,
                None,
                3,
            )
            .unwrap();

        assert_eq!(
            actual,
            vec![
                DelimitedMetadataScanItem::CommonPrefix(common_a),
                DelimitedMetadataScanItem::CommonPrefix(common_b),
                DelimitedMetadataScanItem::Record(MetadataScanItem {
                    key: direct_c,
                    value: b"c".to_vec(),
                }),
            ]
        );
        assert_eq!(wrapper.operation_scan_reads.load(Ordering::Acquire), 3);
    }

    #[test]
    fn current_prefix_scan_discards_short_pages_after_clock_change() {
        let mut store = ready_store();
        let prefix = scoped_key(root(2), b"clock-short-page/");
        let mut expected = Vec::new();
        for index in 0..4_u8 {
            let key = [prefix.as_slice(), format!("{index}").as_bytes()].concat();
            let value = format!("value-{index}").into_bytes();
            store
                .execute(&create_command(
                    &store,
                    request(40 + index),
                    key.clone(),
                    &value,
                ))
                .unwrap();
            expected.push(MetadataScanItem { key, value });
        }
        let version = store.current_read_version().unwrap();
        let inner = Arc::clone(&store.store);
        let controller = MetaShard::open(Arc::clone(&inner), shard(1)).unwrap();
        let wrapper = Arc::new(ShortScanPageStore {
            inner,
            max_scan_items: 2,
            operation_scan_reads: AtomicUsize::new(0),
            advance_clock_on_second_scan: AtomicBool::new(true),
            controller: Some(controller),
        });
        store.store = wrapper.clone();

        let actual = store
            .scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                version,
                None,
                expected.len(),
            )
            .unwrap();

        assert_eq!(actual, expected);
        assert!(!actual.iter().any(|item| item.key.ends_with(b"late")));
        assert_eq!(wrapper.operation_scan_reads.load(Ordering::Acquire), 5);
    }

    #[test]
    fn replacement_and_delete_retain_exact_historical_values() {
        let store = ready_store();
        let key = scoped_key(root(2), b"history");
        let created = store
            .execute(&create_command(&store, request(3), key.clone(), b"v1"))
            .unwrap();

        let mut replace = base_command(&store, request(4), RootFenceAction::RequireActive);
        replace.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"v1".to_vec()),
        });
        replace.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key: key.clone(),
            value: b"v2".to_vec(),
        });
        replace.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key: key.clone(),
        });
        replace.deterministic_result = b"replaced".to_vec();
        let replaced = store.execute(&replace.seal()).unwrap();

        assert_eq!(
            read_operation(&store, &key, created.commit_version),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            read_operation(&store, &key, replaced.commit_version),
            Some(b"v2".to_vec())
        );

        let mut delete = base_command(&store, request(5), RootFenceAction::RequireActive);
        delete.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"v2".to_vec()),
        });
        delete.mutations.push(CommandMutation::Delete {
            family: MetadataFamily::Operation,
            key: key.clone(),
        });
        delete.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key: key.clone(),
        });
        let deleted = store.execute(&delete.seal()).unwrap();
        assert_eq!(
            read_operation(&store, &key, replaced.commit_version),
            Some(b"v2".to_vec())
        );
        assert_eq!(read_operation(&store, &key, deleted.commit_version), None);

        let scan = |version: CommitVersion| {
            store
                .scan_prefix_at(
                    root(2),
                    generation(7),
                    epoch(1),
                    MetadataFamily::Operation,
                    &scoped_key(root(2), b"his"),
                    ReadVersion::new(version.get()).unwrap(),
                    None,
                    10,
                )
                .unwrap()
        };
        assert_eq!(
            scan(created.commit_version),
            vec![MetadataScanItem {
                key: key.clone(),
                value: b"v1".to_vec(),
            }]
        );
        assert_eq!(
            scan(replaced.commit_version),
            vec![MetadataScanItem {
                key,
                value: b"v2".to_vec(),
            }]
        );
        assert!(scan(deleted.commit_version).is_empty());
    }

    #[test]
    fn historical_point_read_resumes_after_a_full_internal_page() {
        let store = ready_store();
        let key = scoped_key(root(2), b"historical-point-pages");
        let target_version = CommitVersion::new(2).unwrap();
        let newest_transition = 3 + MAX_HISTORICAL_SCAN_PAGE_ROWS as u64;

        for transition in 3..=newest_transition {
            let payload = if transition == 3 {
                b"target".as_slice()
            } else {
                b"newer".as_slice()
            };
            put_history_row(&store, &key, transition, transition - 1, payload);
        }
        let current = CurrentValue {
            created_version: target_version,
            modified_version: CommitVersion::new(newest_transition).unwrap(),
            payload: b"current".to_vec(),
        }
        .encode()
        .unwrap();
        raw_put(&store, MetadataFamily::Operation.keyspace(), &key, &current);
        raw_put(
            &store,
            SYSTEM.id,
            SYSTEM_COMMIT_CLOCK_KEY,
            &encode_system_u64(newest_transition),
        );

        #[cfg(feature = "metadata-read-stats")]
        let metadata_stats_session = store.begin_read_stats_session().unwrap();
        assert_eq!(
            read_operation(&store, &key, target_version),
            Some(b"target".to_vec())
        );
        #[cfg(feature = "metadata-read-stats")]
        {
            let metadata = metadata_stats_session.finish().unwrap();
            assert_eq!(metadata.scan_calls, 1);
            assert_eq!(metadata.scan_raw_limit_stops, 1);
            assert_eq!(metadata.point_reads_system, 6);
            assert_eq!(metadata.point_reads_root_fence, 3);
        }
    }

    #[test]
    fn current_prefix_scan_seeks_after_cursor_and_stops_at_limit() {
        let store = ready_store();
        let prefix = scoped_key(root(2), b"bounded/");
        let before_cursor = scoped_key(root(2), b"bounded/00-corrupt");
        let first_key = scoped_key(root(2), b"bounded/01");
        let second_key = scoped_key(root(2), b"bounded/02");
        let after_limit = scoped_key(root(2), b"bounded/03-corrupt");

        store
            .execute(&create_command(
                &store,
                request(3),
                first_key.clone(),
                b"first",
            ))
            .unwrap();
        store
            .execute(&create_command(
                &store,
                request(4),
                second_key.clone(),
                b"second",
            ))
            .unwrap();

        // Deliberately malformed envelopes provide an observable test seam:
        // decoding either row means the cursor or page bound was not pushed
        // into the current-version storage iteration.
        raw_put(
            &store,
            MetadataFamily::Operation.keyspace(),
            &before_cursor,
            b"corrupt-before-cursor",
        );
        raw_put(
            &store,
            MetadataFamily::Operation.keyspace(),
            &after_limit,
            b"corrupt-after-limit",
        );

        let version = store.current_read_version().unwrap();
        #[cfg(feature = "metadata-read-stats")]
        let metadata_stats_session = store.begin_read_stats_session().unwrap();
        let page = store
            .scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                version,
                Some(&before_cursor),
                2,
            )
            .unwrap();
        assert_eq!(
            page,
            vec![
                MetadataScanItem {
                    key: first_key,
                    value: b"first".to_vec(),
                },
                MetadataScanItem {
                    key: second_key,
                    value: b"second".to_vec(),
                },
            ]
        );
        #[cfg(feature = "metadata-read-stats")]
        {
            let metadata = metadata_stats_session.finish().unwrap();
            assert_eq!(metadata.scan_calls, 1);
            assert_eq!(metadata.scan_raw_limit_stops, 1);
            assert!(metadata.scan_key_bytes > 0);
            assert!(metadata.scan_value_bytes > 0);
            assert_eq!(metadata.point_reads_system, 4);
            assert_eq!(metadata.point_reads_root_fence, 2);
        }
    }

    #[cfg(feature = "metadata-read-stats")]
    #[test]
    fn read_stats_session_is_store_exclusive_and_drop_releases_it() {
        let store = ready_store();
        let clone = store.clone();
        let other_store = ready_store();
        let session = store.begin_read_stats_session().unwrap();

        clone.current_read_version().unwrap();
        other_store.current_read_version().unwrap();
        let contender = store.clone();
        let error = std::thread::spawn(move || match contender.begin_read_stats_session() {
            Ok(_) => panic!("a second session for one store must be rejected"),
            Err(error) => error,
        })
        .join()
        .unwrap();
        assert_eq!(
            error,
            MetadataReadStatsSessionError::StoreSessionAlreadyActive
        );

        let stats = session.finish().unwrap();
        assert_eq!(stats.point_reads_system, 1);
        assert_eq!(stats.point_reads_total(), 1);

        let cancelled = clone.begin_read_stats_session().unwrap();
        drop(cancelled);
        let replacement = store.begin_read_stats_session().unwrap();
        replacement.finish().unwrap();
    }

    #[test]
    fn reopened_delimited_scan_skips_nested_values_and_counts_common_prefixes() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("delimited-scan");
        let store = ready_file_store(&database);
        let prefix = scoped_key(root(2), b"delimited/");
        // The path codec shifts each UTF-8 byte by one. Bytes 0 and 1 are
        // therefore reserved for the component delimiter and exact marker.
        let common_a = [prefix.as_slice(), b"b\0"].concat();
        let direct_a = [prefix.as_slice(), b"b\x01"].concat();
        let direct_a_control = [prefix.as_slice(), b"b\x02\x01"].concat();
        let nested_a = [prefix.as_slice(), b"b\0effq\x01"].concat();
        let direct_b = [prefix.as_slice(), b"c\x01"].concat();

        store
            .execute(&create_command(
                &store,
                request(3),
                direct_a.clone(),
                b"direct-a",
            ))
            .unwrap();
        store
            .execute(&create_command(
                &store,
                request(4),
                direct_a_control.clone(),
                b"direct-a-control",
            ))
            .unwrap();
        store
            .execute(&create_command(
                &store,
                request(5),
                direct_b.clone(),
                b"direct-b",
            ))
            .unwrap();
        // A delimiter rollup must skip this nested subtree without decoding
        // its value. A recursive scan would fail closed on this envelope.
        raw_put(
            &store,
            MetadataFamily::Operation.keyspace(),
            &nested_a,
            b"corrupt-nested-value",
        );
        drop(store);
        let store = crate::workspace::test_support::open_file(&database, shard(1)).unwrap();
        #[cfg(feature = "metadata-read-stats")]
        let metadata_stats_session = store.begin_read_stats_session().unwrap();

        let page = store
            .scan_delimited_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                0,
                store.current_read_version().unwrap(),
                None,
                4,
            )
            .unwrap();
        assert_eq!(
            page,
            vec![
                DelimitedMetadataScanItem::CommonPrefix(common_a),
                DelimitedMetadataScanItem::Record(MetadataScanItem {
                    key: direct_a.clone(),
                    value: b"direct-a".to_vec(),
                }),
                DelimitedMetadataScanItem::Record(MetadataScanItem {
                    key: direct_a_control.clone(),
                    value: b"direct-a-control".to_vec(),
                }),
                DelimitedMetadataScanItem::Record(MetadataScanItem {
                    key: direct_b.clone(),
                    value: b"direct-b".to_vec(),
                }),
            ]
        );

        let continuation = store
            .scan_delimited_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                0,
                store.current_read_version().unwrap(),
                Some(&direct_a),
                1,
            )
            .unwrap();
        assert_eq!(
            continuation,
            vec![DelimitedMetadataScanItem::Record(MetadataScanItem {
                key: direct_a_control,
                value: b"direct-a-control".to_vec(),
            })]
        );
        #[cfg(feature = "metadata-read-stats")]
        {
            let metadata = metadata_stats_session.finish().unwrap();
            assert_eq!(metadata.scan_calls, 2);
            assert!(metadata.scan_key_bytes > 0);
            assert!(metadata.scan_value_bytes > 0);
        }
    }

    #[test]
    fn historical_delimited_scan_filters_after_mvcc_reconstruction() {
        let store = ready_store();
        let prefix = scoped_key(root(2), b"historical-delimited/");
        let direct = [prefix.as_slice(), b"a\xff"].concat();
        let nested = [prefix.as_slice(), b"b\0deep\xff"].concat();
        let created = store
            .execute(&create_command(
                &store,
                request(3),
                direct.clone(),
                b"historical-direct",
            ))
            .unwrap();

        let mut remove = base_command(&store, request(4), RootFenceAction::RequireActive);
        remove.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: direct.clone(),
            expected: Some(b"historical-direct".to_vec()),
        });
        remove.mutations.push(CommandMutation::Delete {
            family: MetadataFamily::Operation,
            key: direct.clone(),
        });
        remove.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key: direct.clone(),
        });
        store.execute(&remove.seal()).unwrap();
        store
            .execute(&create_command(
                &store,
                request(5),
                nested,
                b"new-nested-value",
            ))
            .unwrap();

        let historical = store
            .scan_delimited_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                0,
                ReadVersion::new(created.commit_version.get()).unwrap(),
                None,
                10,
            )
            .unwrap();
        assert_eq!(
            historical,
            vec![DelimitedMetadataScanItem::Record(MetadataScanItem {
                key: direct,
                value: b"historical-direct".to_vec(),
            })]
        );
    }

    #[test]
    fn historical_prefix_read_resumes_each_physical_scan_page() {
        let store = ready_store();
        let prefix = scoped_key(root(2), b"historical-prefix-pages/");
        let target_key = [prefix.as_slice(), b"zzz"].concat();
        let start_after = [prefix.as_slice(), b"yyy"].concat();

        for index in 0..MAX_HISTORICAL_SCAN_PAGE_ROWS {
            let suffix = format!("{index:03}");
            let key = [prefix.as_slice(), suffix.as_bytes()].concat();
            put_history_row(&store, &key, 3, 2, b"earlier");
        }
        put_history_row(&store, &target_key, 3, 2, b"target");

        #[cfg(feature = "metadata-read-stats")]
        let metadata_stats_session = store.begin_read_stats_session().unwrap();
        let page = store
            .scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                ReadVersion::new(2).unwrap(),
                Some(&start_after),
                1,
            )
            .unwrap();
        assert_eq!(
            page,
            vec![MetadataScanItem {
                key: target_key,
                value: b"target".to_vec(),
            }]
        );
        #[cfg(feature = "metadata-read-stats")]
        {
            let metadata = metadata_stats_session.finish().unwrap();
            assert_eq!(metadata.scan_calls, 1);
            assert_eq!(metadata.scan_raw_limit_stops, 1);
            assert_eq!(metadata.point_reads_system, 8);
            assert_eq!(metadata.point_reads_root_fence, 4);
        }
    }

    #[test]
    fn historical_scan_returns_retryable_error_after_bounded_clock_churn() {
        let mut store = ready_store();
        let key = scoped_key(root(2), b"historical-clock-target");
        let created = store
            .execute(&create_command(
                &store,
                request(180),
                key.clone(),
                b"earlier",
            ))
            .unwrap();
        let mut replace = base_command(&store, request(181), RootFenceAction::RequireActive);
        replace.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"earlier".to_vec()),
        });
        replace.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key: key.clone(),
            value: b"current".to_vec(),
        });
        replace.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key: key.clone(),
        });
        store.execute(&replace.seal()).unwrap();

        let inner = Arc::clone(&store.store);
        let controller = MetaShard::open(Arc::clone(&inner), shard(1)).unwrap();
        let wrapper = Arc::new(HistoricalScanClockChurnStore {
            inner,
            controller,
            remaining_advances: AtomicUsize::new(MAX_HISTORICAL_SCAN_ATTEMPTS),
            history_scan_reads: AtomicUsize::new(0),
        });
        store.store = wrapper.clone();

        assert_eq!(
            store.scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &key,
                ReadVersion::new(created.commit_version.get()).unwrap(),
                None,
                1,
            ),
            Err(MetaError::ReadStabilityExhausted {
                attempts: MAX_HISTORICAL_SCAN_ATTEMPTS,
            })
        );
        assert_eq!(
            wrapper.history_scan_reads.load(Ordering::Acquire),
            MAX_HISTORICAL_SCAN_ATTEMPTS
        );
    }

    #[test]
    fn change_event_scan_uses_an_exclusive_cursor_and_page_limit() {
        let store = ready_store();
        let first = store
            .execute(&create_command(
                &store,
                request(3),
                scoped_key(root(2), b"event/first"),
                b"first",
            ))
            .unwrap();
        let second = store
            .execute(&create_command(
                &store,
                request(4),
                scoped_key(root(2), b"event/second"),
                b"second",
            ))
            .unwrap();
        let first_key = change_event_key(root(2), first.commit_version, 0);
        let second_key = change_event_key(root(2), second.commit_version, 0);

        let page = store
            .scan_change_events_at(
                root(2),
                generation(7),
                epoch(1),
                root(2).as_bytes(),
                store.current_read_version().unwrap(),
                Some(&first_key),
                1,
            )
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].key, second_key);
    }

    #[test]
    fn historical_prefix_scan_fails_closed_on_corrupt_history_in_second_internal_page() {
        let store = ready_store();
        let prefix = scoped_key(root(2), b"historical-bounded/");
        for index in 0..MAX_HISTORICAL_SCAN_PAGE_ROWS {
            let suffix = format!("{index:03}");
            let key = [prefix.as_slice(), suffix.as_bytes()].concat();
            put_history_row(&store, &key, 3, 2, b"visible-at-version-two");
        }

        let corrupt_key = [prefix.as_slice(), b"zzz"].concat();
        let corrupt_history_key = history_key(
            MetadataFamily::Operation,
            &corrupt_key,
            CommitVersion::new(3).unwrap(),
        );
        raw_put(
            &store,
            HISTORY.id,
            &corrupt_history_key,
            b"corrupt-history-tail",
        );

        assert!(matches!(
            store.scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                ReadVersion::new(2).unwrap(),
                None,
                1,
            ),
            Err(MetaError::CorruptRecord {
                record: "History",
                ..
            })
        ));
    }

    #[test]
    fn failed_predicate_writes_nothing_and_does_not_advance_clock() {
        let store = ready_store();
        let key = scoped_key(root(2), b"guarded");
        store
            .execute(&create_command(&store, request(3), key.clone(), b"v1"))
            .unwrap();
        let before = store.current_read_version().unwrap();
        let recovery_before = store.recovery_state().unwrap();

        let mut command = base_command(&store, request(4), RootFenceAction::RequireActive);
        command.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"wrong".to_vec()),
        });
        command.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key: key.clone(),
            value: b"v2".to_vec(),
        });
        command.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key,
        });
        assert_eq!(
            store.execute(&command.seal()),
            Err(MetaError::PredicateFailed)
        );
        assert_eq!(store.current_read_version().unwrap(), before);
        assert_eq!(store.recovery_state().unwrap(), recovery_before);
    }

    #[test]
    fn lease_clock_is_monotonic_and_owner_fenced() {
        let store = ready_store();
        assert_eq!(store.lease_clock_high_water().unwrap(), 0);
        assert_eq!(
            store
                .observe_lease_clock(root(2), generation(7), epoch(1), 100)
                .unwrap(),
            100
        );
        assert_eq!(
            store
                .observe_lease_clock(root(2), generation(7), epoch(1), 75)
                .unwrap(),
            100
        );
        assert_eq!(store.lease_clock_high_water().unwrap(), 100);
        assert_eq!(
            store.observe_lease_clock(root(2), generation(8), epoch(1), 101),
            Err(MetaError::PlacementMismatch)
        );
        assert_eq!(
            store.observe_lease_clock(root(2), generation(7), epoch(2), 101),
            Err(MetaError::OwnerEpochMismatch {
                expected: 2,
                actual: 1,
            })
        );
        assert_eq!(store.lease_clock_high_water().unwrap(), 100);

        let expired = create_command(
            &store,
            request(10),
            scoped_key(root(2), b"expired"),
            b"value",
        );
        assert_eq!(
            store.execute_before_lease_deadline(&expired, 100),
            Err(MetaError::LeaseDeadlineReached {
                lease_clock_ms: 100,
                requested_deadline_ms: 100,
            })
        );

        let replayable = create_command(
            &store,
            request(11),
            scoped_key(root(2), b"replayable"),
            b"value",
        );
        let applied = store
            .execute_before_lease_deadline(&replayable, 101)
            .unwrap();
        assert!(!applied.replayed);
        store
            .observe_lease_clock(root(2), generation(7), epoch(1), 200)
            .unwrap();
        let replay = store
            .execute_before_lease_deadline(&replayable, 101)
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, applied.commit_version);
    }

    #[test]
    fn write_requires_the_exact_current_read_version() {
        let store = ready_store();
        let stale = create_command(&store, request(3), scoped_key(root(2), b"stale"), b"stale");
        let intervening = store
            .execute(&create_command(
                &store,
                request(4),
                scoped_key(root(2), b"intervening"),
                b"value",
            ))
            .unwrap();

        assert_eq!(
            store.execute(&stale),
            Err(MetaError::WriteReadVersionMismatch {
                requested: intervening.commit_version.get() - 1,
                current: intervening.commit_version.get(),
            })
        );
        assert_eq!(
            store.current_read_version().unwrap().get(),
            intervening.commit_version.get()
        );
    }

    #[test]
    fn concurrent_exact_retries_converge_to_one_commit() {
        let store = ready_store();
        let command = create_command(
            &store,
            request(3),
            scoped_key(root(2), b"concurrent"),
            b"value",
        );
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for _ in 0..2 {
            let store = store.clone();
            let command = command.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                store.execute(&command)
            }));
        }
        barrier.wait();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().unwrap().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(results[0].commit_version, results[1].commit_version);
        assert_eq!(results.iter().filter(|result| result.replayed).count(), 1);
    }

    #[test]
    fn command_digest_and_history_envelope_are_mandatory() {
        let store = ready_store();
        let key = scoped_key(root(2), b"digest");
        let mut command = create_command(&store, request(3), key.clone(), b"v1");
        command.deterministic_result = b"tampered".to_vec();
        assert_eq!(
            store.execute(&command),
            Err(MetaError::CommandDigestMismatch)
        );

        store
            .execute(&create_command(&store, request(4), key.clone(), b"v1"))
            .unwrap();
        let mut replace = base_command(&store, request(5), RootFenceAction::RequireActive);
        replace.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(b"v1".to_vec()),
        });
        replace.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key,
            value: b"v2".to_vec(),
        });
        assert!(matches!(
            store.execute(&replace.seal()),
            Err(MetaError::InvalidCommand { .. })
        ));
    }

    #[test]
    fn workspace_incarnation_claims_are_append_only_and_permanent() {
        let store = ready_store();
        let key = scoped_key(root(2), b"incarnation-claim");

        let mut create = base_command(&store, request(30), RootFenceAction::RequireActive);
        create.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::WorkspaceIncarnationClaim,
            key: key.clone(),
            expected: None,
        });
        create.mutations.push(CommandMutation::Put {
            family: MetadataFamily::WorkspaceIncarnationClaim,
            key: key.clone(),
            value: b"owner-a".to_vec(),
        });
        store.execute(&create.seal()).unwrap();

        let mut replace = base_command(&store, request(31), RootFenceAction::RequireActive);
        replace.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::WorkspaceIncarnationClaim,
            key: key.clone(),
            expected: Some(b"owner-a".to_vec()),
        });
        replace.mutations.push(CommandMutation::Put {
            family: MetadataFamily::WorkspaceIncarnationClaim,
            key: key.clone(),
            value: b"owner-b".to_vec(),
        });
        assert!(matches!(
            store.execute(&replace.seal()),
            Err(MetaError::InvalidCommand { .. })
        ));

        let mut delete = base_command(&store, request(32), RootFenceAction::RequireActive);
        delete.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::WorkspaceIncarnationClaim,
            key: key.clone(),
            expected: Some(b"owner-a".to_vec()),
        });
        delete.mutations.push(CommandMutation::Delete {
            family: MetadataFamily::WorkspaceIncarnationClaim,
            key,
        });
        assert!(matches!(
            store.execute(&delete.seal()),
            Err(MetaError::InvalidCommand { .. })
        ));
    }

    #[test]
    fn command_size_bounds_fit_serving_envelopes_and_reject_before_atomic() {
        let store = ready_store();
        let key = scoped_key(root(2), b"serving-value-boundary");
        let boundary = vec![0x5a; MAX_RECORD_PAYLOAD_BYTES];

        let mut create = create_command(&store, request(40), key.clone(), &boundary);
        create.event_projection = vec![EventProjection {
            payload: boundary.clone(),
        }];
        create.deterministic_result = boundary.clone();
        let created = store.execute(&create.seal()).unwrap();
        assert_eq!(
            read_operation(&store, &key, created.commit_version),
            Some(boundary.clone())
        );

        let replacement = vec![0x6b; MAX_RECORD_PAYLOAD_BYTES];
        let mut replace = base_command(&store, request(41), RootFenceAction::RequireActive);
        replace.predicates.push(CommandPredicate::Value {
            family: MetadataFamily::Operation,
            key: key.clone(),
            expected: Some(boundary),
        });
        replace.mutations.push(CommandMutation::Put {
            family: MetadataFamily::Operation,
            key: key.clone(),
            value: replacement.clone(),
        });
        replace.history_projection.push(HistoryProjection {
            family: MetadataFamily::Operation,
            key: key.clone(),
        });
        replace.event_projection.push(EventProjection {
            payload: replacement.clone(),
        });
        replace.deterministic_result = replacement.clone();
        let replaced = store.execute(&replace.seal()).unwrap();
        assert_eq!(
            read_operation(&store, &key, replaced.commit_version),
            Some(replacement)
        );

        let recovery_before = store.recovery_state().unwrap();
        let version_before = store.current_read_version().unwrap();
        let oversized = vec![0; MAX_RECORD_PAYLOAD_BYTES + 1];

        let oversized_mutation = create_command(
            &store,
            request(42),
            scoped_key(root(2), b"oversized-mutation"),
            &oversized,
        );
        assert!(matches!(
            store.execute(&oversized_mutation),
            Err(MetaError::InvalidCommand { .. })
        ));

        let mut oversized_event = base_command(&store, request(43), RootFenceAction::RequireActive);
        oversized_event.event_projection.push(EventProjection {
            payload: oversized.clone(),
        });
        assert!(matches!(
            store.execute(&oversized_event.seal()),
            Err(MetaError::InvalidCommand { .. })
        ));

        let mut oversized_result =
            base_command(&store, request(44), RootFenceAction::RequireActive);
        oversized_result.deterministic_result = oversized;
        assert!(matches!(
            store.execute(&oversized_result.seal()),
            Err(MetaError::InvalidCommand { .. })
        ));
        assert_eq!(store.current_read_version().unwrap(), version_before);
        assert_eq!(store.recovery_state().unwrap(), recovery_before);
    }

    #[test]
    fn every_real_write_entrypoint_advances_one_hash_chained_lsn() {
        let store = ready_store();
        assert_eq!(store.recovery_state().unwrap().applied_recovery_lsn, 3);

        store
            .observe_lease_clock(root(2), generation(7), epoch(1), 100)
            .unwrap();
        let command = create_command(
            &store,
            request(50),
            scoped_key(root(2), b"recovery"),
            b"value",
        );
        let applied = store.execute_before_lease_deadline(&command, 101).unwrap();
        assert_eq!(applied.recovery_lsn, 5);
        assert_eq!(
            applied.recovery_chain_digest,
            store.recovery_outbox_after(4, 1).unwrap()[0].chain_digest
        );
        store.advance_owner_epoch(Some(epoch(1)), epoch(2)).unwrap();

        let state = store.verify_recovery_chain().unwrap();
        assert_eq!(state.applied_recovery_lsn, 6);
        let rows = store.recovery_outbox_after(0, 10).unwrap();
        assert_eq!(
            rows.iter().map(|row| row.recovery_lsn).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );
        for pair in rows.windows(2) {
            assert_eq!(pair[1].previous_chain_digest, pair[0].chain_digest);
        }
        assert_eq!(state.chain_digest, rows.last().unwrap().chain_digest);

        let dedupe = store
            .lookup_request(root(2), generation(7), epoch(2), request(50))
            .unwrap()
            .unwrap();
        assert_eq!(dedupe.recovery_lsn, 5);

        store
            .observe_lease_clock(root(2), generation(7), epoch(2), 75)
            .unwrap();
        store.advance_owner_epoch(Some(epoch(1)), epoch(2)).unwrap();
        let replay = store.execute_before_lease_deadline(&command, 101).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.recovery_lsn, applied.recovery_lsn);
        assert_eq!(replay.recovery_chain_digest, applied.recovery_chain_digest);
        assert_eq!(store.recovery_state().unwrap(), state);
    }

    #[test]
    fn recovery_outbox_page_has_an_encoded_byte_budget_but_always_returns_one_row() {
        let store = ready_store();
        let all = store.recovery_outbox_after(0, 10).unwrap();
        assert_eq!(all.len(), 3);
        let first_bytes = all[0].encode().unwrap().len();
        let second_bytes = all[1].encode().unwrap().len();

        assert_eq!(
            store
                .recovery_outbox_after_with_byte_budget(0, 10, 1)
                .unwrap(),
            all[..1]
        );
        assert_eq!(
            store
                .recovery_outbox_after_with_byte_budget(0, 10, first_bytes + second_bytes - 1,)
                .unwrap(),
            all[..1]
        );
        assert_eq!(
            store
                .recovery_outbox_after_with_byte_budget(0, 10, first_bytes + second_bytes)
                .unwrap(),
            all[..2]
        );
    }

    #[test]
    fn sequential_recovery_outbox_writes_avoid_pathological_holt_fragmentation() {
        const COMMANDS: u8 = 160;
        const VALUE_BYTES: usize = 3 * 1024;

        let temporary = tempdir().unwrap();
        let (store, physical) = crate::workspace::test_support::initialize_file_with_holt_store(
            &temporary.path().join("metadata"),
            shard(1),
        )
        .unwrap();
        let store = make_store_ready(store);
        let value = vec![0x5a; VALUE_BYTES];
        for index in 0..COMMANDS {
            let command = create_command(
                &store,
                request(index + 10),
                scoped_key(root(2), &[b'w', index]),
                &value,
            );
            store.execute(&command).unwrap();
        }

        physical.checkpoint_for_test().unwrap();
        let stats = physical
            .keyspace_stats_for_test(RECOVERY_OUTBOX.id)
            .unwrap();
        // The fixed-width decimal layout uses two blobs. Keep 4x headroom for
        // Holt shape changes while rejecting the binary-LSN layout's cliff.
        assert!(
            stats.blob_count <= 8,
            "{} sequential outbox records fragmented across {} Holt blobs (space={}, max_fill={}, underfilled={})",
            COMMANDS + 3,
            stats.blob_count,
            stats.total_space_used,
            stats.max_blob_fill_per_mille,
            stats.underfilled_child_blobs
        );
    }

    #[test]
    fn recovery_outbox_survives_file_reopen_with_exact_tail() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        let expected;
        {
            let store =
                crate::workspace::test_support::initialize_file(&database, shard(1)).unwrap();
            store.advance_owner_epoch(None, epoch(1)).unwrap();
            store
                .execute(&base_command(&store, request(1), RootFenceAction::Install).seal())
                .unwrap();
            store
                .execute(
                    &base_command(
                        &store,
                        request(2),
                        RootFenceAction::Transition {
                            expected: RootActivationState::Installing,
                            next: RootActivationState::Active,
                        },
                    )
                    .seal(),
                )
                .unwrap();
            store
                .observe_lease_clock(root(2), generation(7), epoch(1), 55)
                .unwrap();
            expected = store.verify_recovery_chain().unwrap();
            assert_eq!(expected.applied_recovery_lsn, 4);
        }

        let reopened = crate::workspace::test_support::open_file(&database, shard(1)).unwrap();
        assert_eq!(reopened.verify_recovery_chain().unwrap(), expected);
        assert_eq!(reopened.recovery_outbox_after(0, 10).unwrap().len(), 4);
        assert_eq!(reopened.lease_clock_high_water().unwrap(), 55);
    }

    #[test]
    fn format9_store_is_rejected_without_rewriting_its_marker_or_recovery_tail() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        let expected_recovery;
        let format9_marker;
        {
            let store = ready_file_store(&database);
            expected_recovery = store.verify_recovery_chain().unwrap();
            format9_marker = {
                let mut marker = encode_schema_marker();
                let version_start = marker.len() - std::mem::size_of::<u32>();
                marker[version_start..].copy_from_slice(&9_u32.to_be_bytes());
                marker
            };
            raw_put(&store, SYSTEM.id, SYSTEM_SCHEMA_KEY, &format9_marker);
        }

        assert!(matches!(
            crate::workspace::test_support::open_file(&database, shard(1)),
            Err(MetaError::SchemaGate { .. })
        ));

        let raw = crate::workspace::test_support::open_file_txn_store(&database).unwrap();
        let snapshot = raw
            .read(ReadBatch {
                ops: vec![
                    ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_SCHEMA_KEY)),
                    ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_APPLIED_RECOVERY_LSN_KEY)),
                    ReadOp::Get(Key::new(SYSTEM.id, SYSTEM_RECOVERY_CHAIN_DIGEST_KEY)),
                ],
            })
            .unwrap();
        let ReadResult::Get(marker) = &snapshot.results[0] else {
            panic!("schema marker read must return a point value");
        };
        assert_eq!(marker.as_deref(), Some(format9_marker.as_slice()));
        let ReadResult::Get(lsn) = &snapshot.results[1] else {
            panic!("recovery LSN read must return a point value");
        };
        assert_eq!(
            lsn.as_deref(),
            Some(encode_system_u64(expected_recovery.applied_recovery_lsn).as_slice())
        );
        let ReadResult::Get(digest) = &snapshot.results[2] else {
            panic!("recovery digest read must return a point value");
        };
        assert_eq!(
            digest.as_deref(),
            Some(encode_system_digest(expected_recovery.chain_digest).as_slice())
        );
    }

    #[test]
    fn reopen_rejects_unknown_recovery_storage_key_tag() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        {
            let store = ready_file_store(&database);
            raw_put(
                &store,
                RECOVERY_OUTBOX.id,
                &[2, 0, 0, 0, 0],
                b"unknown recovery row",
            );
        }
        assert!(matches!(
            crate::workspace::test_support::open_file(&database, shard(1)),
            Err(MetaError::CorruptRecord {
                record: "RecoveryOutbox key",
                ..
            })
        ));
    }

    #[test]
    fn reopen_rejects_a_missing_recovery_chunk() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        {
            let store = ready_file_store(&database);
            raw_delete(&store, RECOVERY_OUTBOX.id, &recovery_chunk_key(1, 0));
        }
        assert!(matches!(
            crate::workspace::test_support::open_file(&database, shard(1)),
            Err(MetaError::CorruptRecord {
                record: "RecoveryOutbox chunk",
                ..
            })
        ));
    }

    #[test]
    fn recovery_mutations_replay_through_the_authoritative_write_paths() {
        let source = ready_store();
        source
            .observe_lease_clock(root(2), generation(7), epoch(1), 100)
            .unwrap();
        let command = create_command(
            &source,
            request(60),
            scoped_key(root(2), b"material"),
            b"payload",
        );
        source.execute_before_lease_deadline(&command, 101).unwrap();
        source
            .advance_owner_epoch(Some(epoch(1)), epoch(2))
            .unwrap();
        let source_rows = source.recovery_outbox_after(0, 16).unwrap();

        let encoded = RecoveryOutboxSegment::seal(shard(1), source_rows.clone())
            .unwrap()
            .encode()
            .unwrap();
        let segment = RecoveryOutboxSegment::decode(&encoded).unwrap();
        let target = crate::workspace::test_support::memory(shard(1)).unwrap();
        let replayed = target.replay_recovery_segment(&segment).unwrap();
        assert_eq!(replayed, source.verify_recovery_chain().unwrap());

        // Response-loss replay of the exact same segment is idempotent and
        // returns the same authoritative tail without adding another row.
        assert_eq!(target.replay_recovery_segment(&segment).unwrap(), replayed);

        assert_eq!(target.recovery_outbox_after(0, 16).unwrap(), source_rows);
        assert_eq!(
            target.verify_recovery_chain().unwrap(),
            source.verify_recovery_chain().unwrap()
        );
        assert_eq!(target.current_owner_epoch().unwrap(), Some(epoch(2)));
        let version = target.current_read_version().unwrap();
        assert_eq!(
            target
                .read_at(
                    root(2),
                    generation(7),
                    epoch(2),
                    MetadataFamily::Operation,
                    &scoped_key(root(2), b"material"),
                    version,
                )
                .unwrap(),
            Some(b"payload".to_vec())
        );
    }

    #[test]
    fn replaying_an_older_exact_record_returns_the_current_recovery_tail() {
        let source = ready_store();
        let rows = source.recovery_outbox_after(0, 16).unwrap();
        assert!(rows.len() >= 2);
        let target = crate::workspace::test_support::memory(shard(1)).unwrap();
        target.replay_recovery_record(&rows[0]).unwrap();
        target.replay_recovery_record(&rows[1]).unwrap();
        let current = target.recovery_state().unwrap();

        assert_eq!(target.replay_recovery_record(&rows[0]).unwrap(), current);
        assert_eq!(target.recovery_state().unwrap(), current);
    }

    #[test]
    fn recovery_segment_replay_resumes_after_a_definite_middle_failure() {
        let source = ready_store();
        let source_rows = source.recovery_outbox_after(0, 16).unwrap();
        let segment = RecoveryOutboxSegment::seal(shard(1), source_rows.clone()).unwrap();

        let mut target = crate::workspace::test_support::memory(shard(1)).unwrap();
        let wrapper = Arc::new(UnavailableCommitStore {
            inner: Arc::clone(&target.store),
            fail_next_commit: AtomicBool::new(false),
        });
        target.store = wrapper.clone();
        target.replay_recovery_record(&source_rows[0]).unwrap();
        let prefix = target.recovery_state().unwrap();

        wrapper.fail_next_commit.store(true, Ordering::Release);
        assert!(matches!(
            target.replay_recovery_segment(&segment),
            Err(MetaError::Store {
                source: StoreError::Unavailable(_),
                ..
            })
        ));
        assert_eq!(target.recovery_state().unwrap(), prefix);

        let recovered = target.replay_recovery_segment(&segment).unwrap();
        assert_eq!(recovered, source.recovery_state().unwrap());
        assert_eq!(target.recovery_outbox_after(0, 16).unwrap(), source_rows);
    }

    #[test]
    fn recovery_segment_reconciles_an_applied_middle_record_after_lost_ack() {
        let source = ready_store();
        let source_rows = source.recovery_outbox_after(0, 16).unwrap();
        let segment = RecoveryOutboxSegment::seal(shard(1), source_rows.clone()).unwrap();

        let mut target = crate::workspace::test_support::memory(shard(1)).unwrap();
        let wrapper = Arc::new(AppliedThenLostAckStore {
            inner: Arc::clone(&target.store),
            lose_next_ack: AtomicBool::new(false),
        });
        target.store = wrapper.clone();
        target.replay_recovery_record(&source_rows[0]).unwrap();
        let prefix = target.recovery_state().unwrap();

        wrapper.lose_next_ack.store(true, Ordering::Release);
        assert!(matches!(
            target.replay_recovery_segment(&segment),
            Err(MetaError::Store {
                source: StoreError::OutcomeUnknown {
                    state: UnknownCommit::Settled,
                    ..
                },
                ..
            })
        ));
        let after_unknown = target.recovery_state().unwrap();
        assert_eq!(
            after_unknown.applied_recovery_lsn,
            prefix.applied_recovery_lsn + 1
        );
        assert_eq!(
            target.recovery_outbox_after(0, 16).unwrap(),
            source_rows[..2].to_vec()
        );
        target.fsck_recovery().unwrap();

        let recovered = target.replay_recovery_segment(&segment).unwrap();
        assert_eq!(recovered, source.recovery_state().unwrap());
        assert_eq!(target.recovery_outbox_after(0, 16).unwrap(), source_rows);
        let report = target.fsck_recovery().unwrap();
        assert_eq!(report.outbox_records, source_rows.len() as u64);
        assert_eq!(report.metadata_command_records, report.dedupe_records);
    }

    #[test]
    fn recovery_segment_replay_rejects_foreign_shard_and_header_tamper() {
        let source = ready_store();
        let rows = source.recovery_outbox_after(0, 16).unwrap();
        let segment = RecoveryOutboxSegment::seal(shard(1), rows.clone()).unwrap();

        let target = crate::workspace::test_support::memory(shard(1)).unwrap();
        let before = target.recovery_state().unwrap();
        let foreign = RecoveryOutboxSegment::seal(shard(9), vec![rows[0].clone()]).unwrap();
        assert!(matches!(
            target.replay_recovery_segment(&foreign),
            Err(MetaError::CorruptRecord { .. })
        ));
        assert_eq!(target.recovery_state().unwrap(), before);

        let mut tampered = segment;
        tampered.first_lsn = tampered.first_lsn.checked_add(1).unwrap();
        assert!(matches!(
            target.replay_recovery_segment(&tampered),
            Err(MetaError::CorruptRecord { .. })
        ));
        assert_eq!(target.recovery_state().unwrap(), before);
    }

    #[test]
    fn recovery_segment_export_is_available_at_the_command_return_boundary() {
        let store = ready_store();
        let before = store.recovery_state().unwrap();
        let command = create_command(
            &store,
            request(61),
            scoped_key(root(2), b"receipt-before-owner-ack"),
            b"payload",
        );

        let result = store.execute(&command).unwrap();
        let segment = store
            .recovery_segment_after(before, 16)
            .unwrap()
            .expect("the applied command must be exportable before its caller can acknowledge it");

        assert_eq!(segment.first_lsn, result.recovery_lsn);
        assert_eq!(segment.last_lsn, result.recovery_lsn);
        assert_eq!(segment.last_chain_digest, result.recovery_chain_digest);
        assert_eq!(segment.records.len(), 1);
    }

    #[test]
    fn recovery_replay_rejects_a_gap_before_mutating_the_target() {
        let source = ready_store();
        let source_state = source.recovery_state().unwrap();
        let command = create_command(
            &source,
            request(62),
            scoped_key(root(2), b"gap"),
            b"payload",
        );
        source.execute(&command).unwrap();
        let row = source
            .recovery_outbox_after(source_state.applied_recovery_lsn, 1)
            .unwrap()[0]
            .clone();

        let target = crate::workspace::test_support::memory(shard(1)).unwrap();
        let before = target.recovery_state().unwrap();
        assert!(matches!(
            target.replay_recovery_record(&row),
            Err(MetaError::CorruptRecord {
                record: "RecoveryOutbox replay",
                ..
            })
        ));
        assert_eq!(target.recovery_state().unwrap(), before);
    }

    #[test]
    fn recovery_fsck_binds_every_command_dedupe_record_to_its_exact_lsn() {
        let store = ready_store();
        let command = create_command(
            &store,
            request(63),
            scoped_key(root(2), b"fsck"),
            b"payload",
        );
        store.execute(&command).unwrap();
        let clean = store.fsck_recovery().unwrap();
        assert_eq!(clean.state, store.verify_recovery_chain().unwrap());
        assert_eq!(clean.metadata_command_records, clean.dedupe_records);

        let key = command_dedupe_key(root(2), request(63));
        let raw = store
            .required_value(COMMAND_DEDUPE.id, &key, "CommandDedupe")
            .unwrap();
        let mut dedupe = CommandDedupeRecord::decode(&raw).unwrap();
        dedupe.recovery_lsn = 1;
        let corrupted = dedupe.encode().unwrap();
        raw_put(&store, COMMAND_DEDUPE.id, &key, &corrupted);

        for _ in 0..2 {
            assert!(matches!(
                store.fsck_recovery(),
                Err(MetaError::CorruptRecord {
                    record: "CommandDedupe recovery binding",
                    ..
                })
            ));
            assert_eq!(
                store
                    .required_value(COMMAND_DEDUPE.id, &key, "CommandDedupe")
                    .unwrap(),
                corrupted,
                "fsck must diagnose without repairing or guessing"
            );
        }
    }

    #[test]
    fn recovery_chain_verification_retries_a_cross_instance_tail_change() {
        let (reader, wrapper) = recovery_scan_churn_reader();
        wrapper.remaining_advances.store(1, Ordering::Release);

        let verified = reader.verify_recovery_chain().unwrap();
        assert_eq!(verified, wrapper.controller.recovery_state().unwrap());
    }

    #[test]
    fn recovery_fsck_retries_a_tail_change_during_the_binding_scan() {
        let (reader, wrapper) = recovery_scan_churn_reader();
        wrapper.skipped_recovery_scans.store(1, Ordering::Release);
        wrapper.remaining_advances.store(1, Ordering::Release);

        let report = reader.fsck_recovery().unwrap();
        assert_eq!(report.state, wrapper.controller.recovery_state().unwrap());
        assert_eq!(report.metadata_command_records, report.dedupe_records);
    }

    #[test]
    fn recovery_fsck_does_not_report_corruption_while_the_tail_keeps_changing() {
        let (reader, wrapper) = recovery_scan_churn_reader();
        wrapper
            .remaining_advances
            .store(MAX_RECOVERY_SCAN_ATTEMPTS, Ordering::Release);

        assert!(
            matches!(
                reader.fsck_recovery(),
                Err(MetaError::ReadStabilityExhausted {
                    attempts: MAX_RECOVERY_SCAN_ATTEMPTS
                })
            ),
            "continuous cross-instance tail movement must remain a typed retryable condition"
        );
    }
}
