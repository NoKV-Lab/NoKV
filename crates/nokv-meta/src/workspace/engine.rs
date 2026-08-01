use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, RwLock};

use holt::{Durability, Record, RecordVersion, Tree, TreeConfig, DB};
use nokv_types::{
    CommandDigest, CommitVersion, LogicalShardId, OwnerEpoch, PlacementGeneration, ReadVersion,
    RequestId, RootActivationState, RootId,
};
use sha2::{Digest, Sha256};

use super::codec::{
    encode_schema_marker, validate_schema_marker, ARTIFACT_MANIFEST_TREE, ARTIFACT_REVISION_TREE,
    CHANGE_EVENT_TREE, COMMAND_DEDUPE_TREE, COMMIT_CONSUMER_TREE, COMMIT_MEMBER_TREE, COMMIT_TREE,
    GC_BARRIER_TREE, GC_CANDIDATE_TREE, HISTORY_HOLD_TREE, HISTORY_TREE, OPERATION_TREE,
    PATH_CURRENT_TREE, RECOVERY_OUTBOX_TREE, RESTORE_MEMBER_TREE, REVISION_REF_TREE,
    ROOT_FENCE_TREE, SCHEMA_ID, SCHEMA_TREES, SECONDARY_INDEX_TREE, SNAPSHOT_ALIAS_TREE,
    SNAPSHOT_REF_TREE, STAGED_OBJECT_TREE, SYSTEM_SCHEMA_KEY, SYSTEM_TREE, TAG_TREE,
    WORKBENCH_COMMIT_HEAD_TREE, WORKSPACE_CURRENT_TREE,
};
use super::records::{CommandDedupeRecord, CurrentValue, HistoryValue, RootFence};
use super::recovery::{
    assemble_recovery_storage, decode_recovery_outbox_key, recovery_chunk_key,
    recovery_genesis_digest, recovery_outbox_key, recovery_storage_chunk_count,
    recovery_storage_logical_length, split_recovery_storage, RecoveryMutationV1,
    RecoveryOutboxRecord, RecoveryResultV1, RecoveryState, MAX_RECOVERY_BYTES,
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
const MAX_COMMAND_KEY_BYTES: usize = 8 * 1024;
// Holt limits one stored value to u16::MAX bytes. Domain payloads are wrapped
// in CurrentValue, HistoryValue, or CommandDedupeRecord before insertion, so
// keep explicit headroom for every durable envelope.
const MAX_HOLT_RECORD_PAYLOAD_BYTES: usize = 60 * 1024;
const MAX_COMMAND_VALUE_BYTES: usize = MAX_HOLT_RECORD_PAYLOAD_BYTES;
const MAX_DETERMINISTIC_RESULT_BYTES: usize = MAX_HOLT_RECORD_PAYLOAD_BYTES;
const MAX_EVENT_BYTES: usize = MAX_HOLT_RECORD_PAYLOAD_BYTES;

/// Durable metadata families that domain commands may mutate.
///
/// System, root-fence, dedupe, and history records are deliberately absent:
/// the executor owns those families and derives their mutations itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum MetadataFamily {
    WorkspaceCurrent = 0x02,
    PathCurrent = 0x03,
    ArtifactRevision = 0x04,
    ArtifactManifest = 0x05,
    RevisionRef = 0x06,
    Commit = 0x07,
    CommitMember = 0x08,
    WorkbenchCommitHead = 0x09,
    Tag = 0x0a,
    SnapshotRef = 0x0b,
    SnapshotAlias = 0x0c,
    HistoryHold = 0x0d,
    CommitConsumer = 0x0e,
    SecondaryIndex = 0x0f,
    Operation = 0x11,
    RestoreMember = 0x12,
    StagedObject = 0x13,
    GcCandidate = 0x15,
    GcBarrier = 0x16,
}

impl MetadataFamily {
    pub const fn tree_name(self) -> &'static str {
        match self {
            Self::WorkspaceCurrent => WORKSPACE_CURRENT_TREE,
            Self::PathCurrent => PATH_CURRENT_TREE,
            Self::ArtifactRevision => ARTIFACT_REVISION_TREE,
            Self::ArtifactManifest => ARTIFACT_MANIFEST_TREE,
            Self::RevisionRef => REVISION_REF_TREE,
            Self::Commit => COMMIT_TREE,
            Self::CommitMember => COMMIT_MEMBER_TREE,
            Self::WorkbenchCommitHead => WORKBENCH_COMMIT_HEAD_TREE,
            Self::Tag => TAG_TREE,
            Self::SnapshotRef => SNAPSHOT_REF_TREE,
            Self::SnapshotAlias => SNAPSHOT_ALIAS_TREE,
            Self::HistoryHold => HISTORY_HOLD_TREE,
            Self::CommitConsumer => COMMIT_CONSUMER_TREE,
            Self::SecondaryIndex => SECONDARY_INDEX_TREE,
            Self::Operation => OPERATION_TREE,
            Self::RestoreMember => RESTORE_MEMBER_TREE,
            Self::StagedObject => STAGED_OBJECT_TREE,
            Self::GcCandidate => GC_CANDIDATE_TREE,
            Self::GcBarrier => GC_BARRIER_TREE,
        }
    }

    pub const fn history_tag(self) -> u8 {
        self as u8
    }

    const ALL: [Self; 19] = [
        Self::WorkspaceCurrent,
        Self::PathCurrent,
        Self::ArtifactRevision,
        Self::ArtifactManifest,
        Self::RevisionRef,
        Self::Commit,
        Self::CommitMember,
        Self::WorkbenchCommitHead,
        Self::Tag,
        Self::SnapshotRef,
        Self::SnapshotAlias,
        Self::HistoryHold,
        Self::CommitConsumer,
        Self::SecondaryIndex,
        Self::Operation,
        Self::RestoreMember,
        Self::StagedObject,
        Self::GcCandidate,
        Self::GcBarrier,
    ];
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootFenceAction {
    Install,
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
        hasher.update(self.placement_generation.get().to_be_bytes());
        hasher.update(self.owner_epoch.get().to_be_bytes());
        hasher.update(self.request_id.as_bytes());
        hasher.update(self.read_version.get().to_be_bytes());
        match self.root_fence_action {
            RootFenceAction::Install => hasher.update([1]),
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
                    hasher.update([1, *family as u8]);
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
                    hasher.update([2, *family as u8]);
                    hash_bytes(&mut hasher, prefix);
                }
            }
        }
        hash_u64(&mut hasher, self.mutations.len());
        for mutation in &self.mutations {
            match mutation {
                CommandMutation::Put { family, key, value } => {
                    hasher.update([1, *family as u8]);
                    hash_bytes(&mut hasher, key);
                    hash_bytes(&mut hasher, value);
                }
                CommandMutation::Delete { family, key } => {
                    hasher.update([2, *family as u8]);
                    hash_bytes(&mut hasher, key);
                }
            }
        }
        hash_u64(&mut hasher, self.history_projection.len());
        for projection in &self.history_projection {
            hasher.update([projection.family as u8]);
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
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataScanItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentMetadataError {
    Backend {
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

impl std::fmt::Display for AgentMetadataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend { operation, message } => {
                write!(formatter, "Holt {operation} failed: {message}")
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

impl std::error::Error for AgentMetadataError {}

#[derive(Clone)]
pub struct AgentMetadataStore {
    db: DB,
    logical_shard_id: LogicalShardId,
    command_gate: Arc<RwLock<()>>,
    system: Tree,
    root_fence: Tree,
    command_dedupe: Tree,
    change_event: Tree,
    history: Tree,
    recovery_outbox: Tree,
    families: BTreeMap<MetadataFamily, Tree>,
}

impl AgentMetadataStore {
    pub fn open_memory(logical_shard_id: LogicalShardId) -> Result<Self, AgentMetadataError> {
        Self::initialize_fresh(TreeConfig::memory(), logical_shard_id)
    }

    pub fn create_file(
        path: impl AsRef<Path>,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, AgentMetadataError> {
        let path = path.as_ref();
        require_fresh_location(path)?;
        let mut config = TreeConfig::new(path);
        config.durability = Durability::Wal { sync: true };
        config.checkpoint.auto_vacuum = false;
        Self::initialize_fresh(config, logical_shard_id)
    }

    pub fn reopen_file(
        path: impl AsRef<Path>,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, AgentMetadataError> {
        let path = path.as_ref();
        require_existing_location(path)?;
        let mut config = TreeConfig::new(path);
        config.durability = Durability::Wal { sync: true };
        config.checkpoint.auto_vacuum = false;
        let db = DB::open(config).map_err(|error| backend("open database", error))?;
        Self::open_marked(db, logical_shard_id)
    }

    fn initialize_fresh(
        config: TreeConfig,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, AgentMetadataError> {
        let db = DB::open(config).map_err(|error| backend("create database", error))?;
        let existing = db
            .list_trees()
            .map_err(|error| backend("inspect fresh database", error))?;
        if !existing.is_empty() {
            return Err(AgentMetadataError::SchemaGate {
                reason: format!("fresh store already contains trees {existing:?}"),
            });
        }
        for tree in SCHEMA_TREES {
            db.create_tree(tree)
                .map_err(|error| backend("create schema tree", error))?;
        }
        let initialized = db
            .atomic(|batch| {
                batch.put_if_absent(SYSTEM_TREE, SYSTEM_SCHEMA_KEY, &encode_schema_marker());
                batch.put_if_absent(
                    SYSTEM_TREE,
                    SYSTEM_SHARD_IDENTITY_KEY,
                    &encode_shard_identity(logical_shard_id),
                );
                batch.put_if_absent(SYSTEM_TREE, SYSTEM_OWNER_FENCE_KEY, &encode_system_u64(0));
                batch.put_if_absent(
                    SYSTEM_TREE,
                    SYSTEM_COMMIT_CLOCK_KEY,
                    &encode_system_u64(INITIAL_COMMIT_VERSION),
                );
                batch.put_if_absent(
                    SYSTEM_TREE,
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    &encode_system_u64(0),
                );
                batch.put_if_absent(
                    SYSTEM_TREE,
                    SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                    &encode_system_u64(0),
                );
                batch.put_if_absent(
                    SYSTEM_TREE,
                    SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
                    &encode_system_digest(recovery_genesis_digest(logical_shard_id)),
                );
            })
            .map_err(|error| backend("initialize system records", error))?;
        if !initialized {
            return Err(AgentMetadataError::SchemaGate {
                reason: "fresh system records collided".to_owned(),
            });
        }
        Self::open_marked(db, logical_shard_id)
    }

    fn open_marked(db: DB, logical_shard_id: LogicalShardId) -> Result<Self, AgentMetadataError> {
        validate_tree_registry(&db)?;
        let system = db
            .open_tree(SYSTEM_TREE)
            .map_err(|error| backend("open System", error))?;
        let schema = required_record(&system, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = required_record(&system, SYSTEM_SHARD_IDENTITY_KEY, "System(shard_identity)")?;
        if decode_shard_identity(&shard.value)? != logical_shard_id {
            return Err(AgentMetadataError::SchemaGate {
                reason: "logical shard identity does not match requested store".to_owned(),
            });
        }
        decode_system_u64(
            &required_record(&system, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?.value,
            "System(owner_fence)",
        )?;
        decode_system_u64(
            &required_record(
                &system,
                SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                "System(applied_recovery_lsn)",
            )?
            .value,
            "System(applied_recovery_lsn)",
        )?;
        decode_system_digest(
            &required_record(
                &system,
                SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
                "System(recovery_chain_digest)",
            )?
            .value,
            "System(recovery_chain_digest)",
        )?;
        let clock = decode_system_u64(
            &required_record(&system, SYSTEM_COMMIT_CLOCK_KEY, "System(commit_clock)")?.value,
            "System(commit_clock)",
        )?;
        CommitVersion::new(clock).map_err(|error| corrupt("System(commit_clock)", error))?;
        decode_system_u64(
            &required_record(
                &system,
                SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                "System(lease_clock_high_water)",
            )?
            .value,
            "System(lease_clock_high_water)",
        )?;

        let root_fence = db
            .open_tree(ROOT_FENCE_TREE)
            .map_err(|error| backend("open RootFence", error))?;
        let command_dedupe = db
            .open_tree(COMMAND_DEDUPE_TREE)
            .map_err(|error| backend("open CommandDedupe", error))?;
        let change_event = db
            .open_tree(CHANGE_EVENT_TREE)
            .map_err(|error| backend("open ChangeEvent", error))?;
        let history = db
            .open_tree(HISTORY_TREE)
            .map_err(|error| backend("open History", error))?;
        let recovery_outbox = db
            .open_tree(RECOVERY_OUTBOX_TREE)
            .map_err(|error| backend("open RecoveryOutbox", error))?;
        let mut families = BTreeMap::new();
        for family in MetadataFamily::ALL {
            families.insert(
                family,
                db.open_tree(family.tree_name())
                    .map_err(|error| backend("open metadata family", error))?,
            );
        }
        let store = Self {
            db,
            logical_shard_id,
            command_gate: Arc::new(RwLock::new(())),
            system,
            root_fence,
            command_dedupe,
            change_event,
            history,
            recovery_outbox,
            families,
        };
        store.verify_recovery_chain_unlocked()?;
        Ok(store)
    }

    pub fn current_read_version(&self) -> Result<ReadVersion, AgentMetadataError> {
        let record = required_record(
            &self.system,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )?;
        let value = decode_system_u64(&record.value, "System(commit_clock)")?;
        ReadVersion::new(value).map_err(|error| corrupt("System(commit_clock)", error))
    }

    /// Return the logical shard identity sealed into this store.
    pub fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    /// Return the persisted physical-owner epoch. `None` is the fresh epoch-zero
    /// sentinel before the first owner is admitted.
    pub fn current_owner_epoch(&self) -> Result<Option<OwnerEpoch>, AgentMetadataError> {
        let record = required_record(&self.system, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let value = decode_system_u64(&record.value, "System(owner_fence)")?;
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
    pub fn root_fence(&self, root_id: RootId) -> Result<Option<RootFence>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.root_fence
            .get(root_id.as_bytes())
            .map_err(|error| backend("read RootFence", error))?
            .map(|value| RootFence::decode(&value).map_err(|error| corrupt("RootFence", error)))
            .transpose()
    }

    /// Return the persisted monotonic lease clock used by snapshot expiry.
    pub fn lease_clock_high_water(&self) -> Result<u64, AgentMetadataError> {
        let record = required_record(
            &self.system,
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            "System(lease_clock_high_water)",
        )?;
        decode_system_u64(&record.value, "System(lease_clock_high_water)")
    }

    /// Return the durable recovery tail atomically serialized with writes.
    pub fn recovery_state(&self) -> Result<RecoveryState, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.recovery_state_unlocked()
    }

    /// Read strictly ordered recovery rows after `start_after_lsn`.
    pub fn recovery_outbox_after(
        &self,
        start_after_lsn: u64,
        limit: usize,
    ) -> Result<Vec<RecoveryOutboxRecord>, AgentMetadataError> {
        self.recovery_outbox_after_with_byte_budget(start_after_lsn, limit, MAX_RECOVERY_BYTES)
    }

    fn recovery_outbox_after_with_byte_budget(
        &self,
        start_after_lsn: u64,
        limit: usize,
        max_encoded_bytes: usize,
    ) -> Result<Vec<RecoveryOutboxRecord>, AgentMetadataError> {
        const MAX_RECOVERY_PAGE_ROWS: usize = 1024;
        if limit == 0 || limit > MAX_RECOVERY_PAGE_ROWS {
            return Err(invalid(format!(
                "recovery outbox limit must be in 1..={MAX_RECOVERY_PAGE_ROWS}"
            )));
        }
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        let start = recovery_outbox_key(start_after_lsn);
        let mut rows = Vec::with_capacity(limit);
        let mut encoded_bytes = 0_usize;
        for entry in self
            .recovery_outbox
            .range()
            .prefix(&[0])
            .start_after(&start)
        {
            let holt::RangeEntry::Key { key, value, .. } =
                entry.map_err(|error| backend("scan RecoveryOutbox", error))?
            else {
                continue;
            };
            let key_lsn = decode_recovery_outbox_key(&key)
                .map_err(|error| corrupt("RecoveryOutbox key", error))?;
            let row_encoded_bytes = recovery_storage_logical_length(&value)
                .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
            if !rows.is_empty()
                && encoded_bytes.saturating_add(row_encoded_bytes) > max_encoded_bytes
            {
                break;
            }
            let row = self.read_recovery_record(key_lsn, &value)?;
            if row.recovery_lsn != key_lsn {
                return Err(AgentMetadataError::CorruptRecord {
                    record: "RecoveryOutbox",
                    reason: "row LSN does not match ordered key".to_owned(),
                });
            }
            encoded_bytes = encoded_bytes.saturating_add(row_encoded_bytes);
            rows.push(row);
            if rows.len() == limit {
                break;
            }
        }
        Ok(rows)
    }

    /// Verify every durable recovery row and the `System` tail.
    pub fn verify_recovery_chain(&self) -> Result<RecoveryState, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.verify_recovery_chain_unlocked()
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
    ) -> Result<u64, AgentMetadataError> {
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| backend("lock command gate", error))?;
        let schema = required_record(&self.system, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = required_record(
            &self.system,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard.value)? != self.logical_shard_id {
            return Err(AgentMetadataError::PlacementMismatch);
        }
        let owner = required_record(&self.system, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let root_fence = self
            .root_fence
            .get_record(root_id.as_bytes())
            .map_err(|error| backend("read RootFence", error))?
            .ok_or(AgentMetadataError::RootFenceMissing)?;
        let fence =
            RootFence::decode(&root_fence.value).map_err(|error| corrupt("RootFence", error))?;
        if fence.logical_shard_id != self.logical_shard_id
            || fence.placement_generation != placement_generation
        {
            return Err(AgentMetadataError::PlacementMismatch);
        }
        if fence.activation_state != RootActivationState::Active {
            return Err(AgentMetadataError::RootFenceStateMismatch {
                expected: RootActivationState::Active,
                actual: fence.activation_state,
            });
        }
        let clock = required_record(
            &self.system,
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            "System(lease_clock_high_water)",
        )?;
        let current = decode_system_u64(&clock.value, "System(lease_clock_high_water)")?;
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
        let committed = self
            .db
            .atomic(|batch| {
                batch.assert_version(SYSTEM_TREE, SYSTEM_SCHEMA_KEY, schema.version);
                batch.assert_version(SYSTEM_TREE, SYSTEM_SHARD_IDENTITY_KEY, shard.version);
                batch.assert_version(SYSTEM_TREE, SYSTEM_OWNER_FENCE_KEY, owner.version);
                batch.assert_version(ROOT_FENCE_TREE, root_id.as_bytes(), root_fence.version);
                batch.compare_and_put(
                    SYSTEM_TREE,
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    clock.version,
                    &encode_system_u64(observed_ms),
                );
                enqueue_recovery(batch, &recovery);
            })
            .map_err(|error| backend("advance lease clock", error))?;
        if committed {
            Ok(observed_ms)
        } else {
            Err(AgentMetadataError::WriteConflict)
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
    ) -> Result<(), AgentMetadataError> {
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| backend("lock command gate", error))?;
        let expected_raw = expected.map(OwnerEpoch::get).unwrap_or(0);
        let schema = required_record(&self.system, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = required_record(
            &self.system,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard.value)? != self.logical_shard_id {
            return Err(AgentMetadataError::PlacementMismatch);
        }
        let owner = required_record(&self.system, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let current = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if current == next.get() {
            return Ok(());
        }
        if current != expected_raw {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: expected_raw,
                actual: current,
            });
        }
        if next.get() <= current {
            return Err(AgentMetadataError::OwnerEpochNotMonotonic {
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
        let committed = self
            .db
            .atomic(|batch| {
                batch.assert_version(SYSTEM_TREE, SYSTEM_SCHEMA_KEY, schema.version);
                batch.assert_version(SYSTEM_TREE, SYSTEM_SHARD_IDENTITY_KEY, shard.version);
                batch.compare_and_put(
                    SYSTEM_TREE,
                    SYSTEM_OWNER_FENCE_KEY,
                    owner.version,
                    &encode_system_u64(next.get()),
                );
                enqueue_recovery(batch, &recovery);
            })
            .map_err(|error| backend("advance owner epoch", error))?;
        if committed {
            Ok(())
        } else {
            Err(AgentMetadataError::WriteConflict)
        }
    }

    pub fn execute(
        &self,
        command: &MetadataCommand,
    ) -> Result<MetadataCommandResult, AgentMetadataError> {
        self.execute_with_lease_deadline(command, None)
    }

    pub(crate) fn execute_before_lease_deadline(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: u64,
    ) -> Result<MetadataCommandResult, AgentMetadataError> {
        self.execute_with_lease_deadline(command, Some(lease_deadline_ms))
    }

    fn execute_with_lease_deadline(
        &self,
        command: &MetadataCommand,
        lease_deadline_ms: Option<u64>,
    ) -> Result<MetadataCommandResult, AgentMetadataError> {
        // Validation and canonical hashing depend only on caller-owned command
        // bytes. Keep that potentially non-trivial work outside the shard-wide
        // sequencing window; the guarded section below is reserved for reads
        // and writes that must observe one commit-clock / recovery-chain state.
        self.validate_command(command)?;
        if command.command_digest != command.canonical_digest() {
            return Err(AgentMetadataError::CommandDigestMismatch);
        }
        let dedupe_key = command_dedupe_key(command.root_id, command.request_id);
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| backend("lock command gate", error))?;
        if let Some(result) = self.replayed_result(&dedupe_key, command.command_digest)? {
            return Ok(result);
        }

        let schema = required_record(&self.system, SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let shard = required_record(
            &self.system,
            SYSTEM_SHARD_IDENTITY_KEY,
            "System(shard_identity)",
        )?;
        if decode_shard_identity(&shard.value)? != command.logical_shard_id {
            return Err(AgentMetadataError::PlacementMismatch);
        }
        let owner = required_record(&self.system, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != command.owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: command.owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let clock = required_record(
            &self.system,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )?;
        let current_version = decode_system_u64(&clock.value, "System(commit_clock)")?;
        if command.read_version.get() != current_version {
            return Err(AgentMetadataError::WriteReadVersionMismatch {
                requested: command.read_version.get(),
                current: current_version,
            });
        }
        let next_version_raw = current_version
            .checked_add(1)
            .ok_or(AgentMetadataError::VersionOverflow)?;
        let next_version = CommitVersion::new(next_version_raw)
            .map_err(|_| AgentMetadataError::VersionOverflow)?;

        let root_plan = self.plan_root_fence(command)?;
        let lease_clock = lease_deadline_ms
            .map(|requested_deadline_ms| {
                let clock = required_record(
                    &self.system,
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    "System(lease_clock_high_water)",
                )?;
                let lease_clock_ms =
                    decode_system_u64(&clock.value, "System(lease_clock_high_water)")?;
                if requested_deadline_ms <= lease_clock_ms {
                    return Err(AgentMetadataError::LeaseDeadlineReached {
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

        let dedupe_record = CommandDedupeRecord {
            command_digest: command.command_digest,
            commit_version: next_version,
            recovery_lsn: recovery.row.recovery_lsn,
            deterministic_result: command.deterministic_result.clone(),
        }
        .encode()
        .map_err(|error| corrupt("CommandDedupe", error))?;

        let committed = self
            .db
            .atomic(|batch| {
                batch.assert_version(SYSTEM_TREE, SYSTEM_SCHEMA_KEY, schema.version);
                batch.assert_version(SYSTEM_TREE, SYSTEM_SHARD_IDENTITY_KEY, shard.version);
                batch.assert_version(SYSTEM_TREE, SYSTEM_OWNER_FENCE_KEY, owner.version);
                if let Some(lease_clock) = &lease_clock {
                    batch.assert_version(
                        SYSTEM_TREE,
                        SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                        lease_clock.version,
                    );
                }
                batch.compare_and_put(
                    SYSTEM_TREE,
                    SYSTEM_COMMIT_CLOCK_KEY,
                    clock.version,
                    &encode_system_u64(next_version_raw),
                );
                enqueue_root_fence(batch, command, &root_plan);
                enqueue_predicate_guards(batch, &predicate_plan, next_version);

                for planned in predicate_plan.exact.values() {
                    let Some(previous) = &planned.current else {
                        continue;
                    };
                    if !command.history_projection.iter().any(|projection| {
                        projection.family == planned.family && projection.key == planned.key
                    }) {
                        continue;
                    }
                    let key = history_key(planned.family, &planned.key, next_version);
                    let value = HistoryValue {
                        transition_version: next_version,
                        previous_created_version: previous.created_version,
                        previous_modified_version: previous.modified_version,
                        previous_payload: Some(previous.payload.clone()),
                    }
                    .encode()
                    .expect("validated command history value fits the format envelope");
                    batch.put(HISTORY_TREE, &key, &value);
                }

                for mutation in &command.mutations {
                    match mutation {
                        CommandMutation::Put { family, key, value } => {
                            let planned = predicate_plan
                                .exact
                                .get(&(*family, key.clone()))
                                .expect("every mutation has one exact predicate");
                            let created_version = planned
                                .current
                                .as_ref()
                                .map(|current| current.created_version)
                                .unwrap_or(next_version);
                            let encoded = CurrentValue {
                                created_version,
                                modified_version: next_version,
                                payload: value.clone(),
                            }
                            .encode()
                            .expect("validated command value fits the format envelope");
                            batch.put(family.tree_name(), key, &encoded);
                        }
                        CommandMutation::Delete { family, key } => {
                            batch.delete(family.tree_name(), key);
                        }
                    }
                }
                for (sequence, projection) in command.event_projection.iter().enumerate() {
                    let key = change_event_key(command.root_id, next_version, sequence);
                    let value = CurrentValue {
                        created_version: next_version,
                        modified_version: next_version,
                        payload: projection.payload.clone(),
                    }
                    .encode()
                    .expect("validated event fits the format envelope");
                    batch.put_if_absent(CHANGE_EVENT_TREE, &key, &value);
                }
                batch.put_if_absent(COMMAND_DEDUPE_TREE, &dedupe_key, &dedupe_record);
                enqueue_recovery(batch, &recovery);
            })
            .map_err(|error| backend("execute metadata command", error))?;
        if committed {
            Ok(MetadataCommandResult {
                commit_version: next_version,
                deterministic_result: command.deterministic_result.clone(),
                replayed: false,
            })
        } else if let Some(result) = self.replayed_result(&dedupe_key, command.command_digest)? {
            Ok(result)
        } else {
            Err(AgentMetadataError::WriteConflict)
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
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.validate_read_fence(root_id, placement_generation, owner_epoch, key, version)?;
        self.read_at_unfenced(family, key, version)
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
    ) -> Result<Option<CommandDedupeRecord>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        let key = command_dedupe_key(root_id, request_id);
        let current_version = self.current_read_version()?;
        self.validate_read_fence(
            root_id,
            placement_generation,
            owner_epoch,
            &key,
            current_version,
        )?;
        self.command_dedupe
            .get(&key)
            .map_err(|error| backend("read CommandDedupe", error))?
            .map(|value| {
                CommandDedupeRecord::decode(&value).map_err(|error| corrupt("CommandDedupe", error))
            })
            .transpose()
    }

    /// Stable ordered prefix scan at one fenced read version.
    ///
    /// The current-version path is one Holt prefix scan. A historical scan also
    /// reconstructs keys replaced or deleted after `version` from History.
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
    ) -> Result<Vec<MetadataScanItem>, AgentMetadataError> {
        let effective_limit = if limit == 0 {
            MAX_COMMAND_ITEMS
        } else {
            limit.min(MAX_COMMAND_ITEMS)
        };
        // Fence and capture one immutable cross-tree view while writes are
        // excluded, then release the NoKV read gate before walking or decoding
        // the page. Holt's captured views keep current and History consistent
        // without making a long or historical scan block the write sequencer.
        let (current_version, current_view, history_view) = {
            let _read_guard = self
                .command_gate
                .read()
                .map_err(|error| backend("lock read gate", error))?;
            let current_version = self.validate_read_fence(
                root_id,
                placement_generation,
                owner_epoch,
                prefix,
                version,
            )?;
            let history_family_prefix = [family.history_tag()];
            let scopes = if version == current_version {
                vec![(family.tree_name(), prefix)]
            } else {
                vec![
                    (family.tree_name(), prefix),
                    (HISTORY_TREE, history_family_prefix.as_slice()),
                ]
            };
            let (current_view, history_view) = self
                .db
                .view(&scopes, |view| {
                    let current = view
                        .tree(family.tree_name())
                        .expect("requested metadata family is present in the DB view")
                        .clone();
                    let history = view.tree(HISTORY_TREE).cloned();
                    Ok((current, history))
                })
                .map_err(|error| backend("capture metadata scan view", error))?;
            (current_version, current_view, history_view)
        };

        // Current-state rows are already in the exact order required by the
        // caller. Push the exclusive cursor into Holt and stop advancing the
        // storage iterator as soon as the bounded page is full. Historical
        // reads cannot use this shortcut: a key absent from current state may
        // still need to be reconstructed from History before ordering and
        // pagination are applied.
        if version == current_version {
            let mut range = current_view.range();
            if let Some(marker) = start_after {
                range = range.start_after(marker);
            }

            let mut visible = Vec::with_capacity(effective_limit);
            for entry in range {
                let holt::RangeEntry::Key { key, value, .. } =
                    entry.map_err(|error| backend("scan current metadata", error))?
                else {
                    continue;
                };
                let current = CurrentValue::decode(&value)
                    .map_err(|error| corrupt(family.tree_name(), error))?;
                if current.modified_version.get() > version.get() {
                    continue;
                }
                visible.push(MetadataScanItem {
                    key,
                    value: current.payload,
                });
                if visible.len() == effective_limit {
                    break;
                }
            }
            return Ok(visible);
        }

        let mut visible = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        for entry in current_view.range() {
            let holt::RangeEntry::Key { key, value, .. } =
                entry.map_err(|error| backend("scan current metadata", error))?
            else {
                continue;
            };
            let current =
                CurrentValue::decode(&value).map_err(|error| corrupt(family.tree_name(), error))?;
            if current.modified_version.get() <= version.get() {
                visible.insert(key, current.payload);
            }
        }

        let history_view = history_view
            .expect("historical metadata scan captures the matching History family view");
        for entry in history_view.range() {
            let holt::RangeEntry::Key { key, value, .. } =
                entry.map_err(|error| backend("scan metadata history", error))?
            else {
                continue;
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

        Ok(visible
            .into_iter()
            .filter(|(key, _)| start_after.is_none_or(|marker| key.as_slice() > marker))
            .take(effective_limit)
            .map(|(key, value)| MetadataScanItem { key, value })
            .collect())
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
    ) -> Result<Vec<MetadataScanItem>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.validate_read_fence(root_id, placement_generation, owner_epoch, prefix, version)?;
        let effective_limit = if limit == 0 {
            MAX_COMMAND_ITEMS
        } else {
            limit.min(MAX_COMMAND_ITEMS)
        };
        let mut events = Vec::with_capacity(effective_limit);
        for entry in self.change_event.range().prefix(prefix) {
            let holt::RangeEntry::Key { key, value, .. } =
                entry.map_err(|error| backend("scan ChangeEvent", error))?
            else {
                continue;
            };
            if start_after.is_some_and(|marker| key.as_slice() <= marker) {
                continue;
            }
            let current =
                CurrentValue::decode(&value).map_err(|error| corrupt("ChangeEvent", error))?;
            if current.modified_version.get() > version.get() {
                continue;
            }
            events.push(MetadataScanItem {
                key,
                value: current.payload,
            });
            if events.len() == effective_limit {
                break;
            }
        }
        Ok(events)
    }

    fn validate_read_fence(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        key_or_prefix: &[u8],
        version: ReadVersion,
    ) -> Result<ReadVersion, AgentMetadataError> {
        validate_root_scoped_bytes(root_id, key_or_prefix, "read key or prefix")?;
        let owner = required_record(&self.system, SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let fence = self
            .root_fence
            .get(root_id.as_bytes())
            .map_err(|error| backend("read RootFence", error))?
            .ok_or(AgentMetadataError::RootFenceMissing)?;
        let fence = RootFence::decode(&fence).map_err(|error| corrupt("RootFence", error))?;
        if fence.logical_shard_id != self.logical_shard_id
            || fence.placement_generation != placement_generation
        {
            return Err(AgentMetadataError::PlacementMismatch);
        }
        if fence.activation_state != RootActivationState::Active {
            return Err(AgentMetadataError::RootFenceStateMismatch {
                expected: RootActivationState::Active,
                actual: fence.activation_state,
            });
        }
        let current_version = self.current_read_version()?;
        if version > current_version {
            return Err(AgentMetadataError::ReadVersionInFuture {
                requested: version.get(),
                current: current_version.get(),
            });
        }
        Ok(current_version)
    }

    fn read_at_unfenced(
        &self,
        family: MetadataFamily,
        key: &[u8],
        version: ReadVersion,
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        let tree = self.family(family);
        if let Some(record) = tree
            .get(key)
            .map_err(|error| backend("read current metadata", error))?
        {
            let current = CurrentValue::decode(&record)
                .map_err(|error| corrupt(family.tree_name(), error))?;
            if current.modified_version.get() <= version.get() {
                return Ok(Some(current.payload));
            }
        }
        let prefix = history_prefix(family, key);
        for entry in self.history.range().prefix(&prefix) {
            let holt::RangeEntry::Key { value, .. } =
                entry.map_err(|error| backend("read metadata history", error))?
            else {
                continue;
            };
            let history =
                HistoryValue::decode(&value).map_err(|error| corrupt("History", error))?;
            if history.previous_modified_version.get() <= version.get()
                && version.get() < history.transition_version.get()
            {
                return Ok(history.previous_payload);
            }
        }
        Ok(None)
    }

    fn validate_command(&self, command: &MetadataCommand) -> Result<(), AgentMetadataError> {
        if command.schema_id != SCHEMA_ID {
            return Err(AgentMetadataError::SchemaGate {
                reason: format!("expected schema {SCHEMA_ID}, found {}", command.schema_id),
            });
        }
        if command.logical_shard_id != self.logical_shard_id {
            return Err(AgentMetadataError::PlacementMismatch);
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

    fn plan_root_fence(
        &self,
        command: &MetadataCommand,
    ) -> Result<RootFencePlan, AgentMetadataError> {
        let key = command.root_id.as_bytes();
        let current = self
            .root_fence
            .get_record(key)
            .map_err(|error| backend("read RootFence", error))?;
        match command.root_fence_action {
            RootFenceAction::Install => {
                if current.is_some() {
                    return Err(AgentMetadataError::RootFenceAlreadyInstalled);
                }
                Ok(RootFencePlan::Install {
                    value: RootFence {
                        logical_shard_id: command.logical_shard_id,
                        placement_generation: command.placement_generation,
                        activation_state: RootActivationState::Installing,
                    }
                    .encode()
                    .expect("typed RootFence always fits its fixed format"),
                })
            }
            RootFenceAction::RequireActive => {
                let current = current.ok_or(AgentMetadataError::RootFenceMissing)?;
                let fence = RootFence::decode(&current.value)
                    .map_err(|error| corrupt("RootFence", error))?;
                validate_root_placement(command, fence)?;
                if fence.activation_state != RootActivationState::Active {
                    return Err(AgentMetadataError::RootFenceStateMismatch {
                        expected: RootActivationState::Active,
                        actual: fence.activation_state,
                    });
                }
                Ok(RootFencePlan::Assert {
                    version: current.version,
                })
            }
            RootFenceAction::Transition { expected, next } => {
                if !valid_root_transition(expected, next) {
                    return Err(AgentMetadataError::InvalidRootFenceTransition {
                        from: expected,
                        to: next,
                    });
                }
                let current = current.ok_or(AgentMetadataError::RootFenceMissing)?;
                let fence = RootFence::decode(&current.value)
                    .map_err(|error| corrupt("RootFence", error))?;
                validate_root_placement(command, fence)?;
                if fence.activation_state != expected {
                    return Err(AgentMetadataError::RootFenceStateMismatch {
                        expected,
                        actual: fence.activation_state,
                    });
                }
                Ok(RootFencePlan::Replace {
                    version: current.version,
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

    fn plan_predicates(
        &self,
        command: &MetadataCommand,
    ) -> Result<PredicatePlan, AgentMetadataError> {
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
                    let record = self
                        .family(*family)
                        .get_record(key)
                        .map_err(|error| backend("plan exact predicate", error))?;
                    let (current, version) = match record {
                        Some(record) => {
                            let current = CurrentValue::decode(&record.value)
                                .map_err(|error| corrupt(family.tree_name(), error))?;
                            (Some(current), Some(record.version))
                        }
                        None => (None, None),
                    };
                    if current.as_ref().map(|value| &value.payload) != expected.as_ref() {
                        return Err(AgentMetadataError::PredicateFailed);
                    }
                    plan.exact.insert(
                        map_key,
                        PlannedExactPredicate {
                            family: *family,
                            key: key.clone(),
                            current,
                            version,
                        },
                    );
                }
                CommandPredicate::PrefixEmpty { family, prefix } => {
                    if self
                        .family(*family)
                        .range()
                        .prefix(prefix)
                        .into_iter()
                        .next()
                        .transpose()
                        .map_err(|error| backend("plan prefix-empty predicate", error))?
                        .is_some()
                    {
                        return Err(AgentMetadataError::PredicateFailed);
                    }
                    plan.prefix_empty.push((*family, prefix.clone()));
                }
            }
        }
        for mutation in &command.mutations {
            if !plan
                .exact
                .contains_key(&(mutation.family(), mutation.key().to_vec()))
            {
                return Err(invalid(
                    "every mutation requires one exact value/absence predicate",
                ));
            }
            if matches!(mutation, CommandMutation::Delete { .. })
                && plan
                    .exact
                    .get(&(mutation.family(), mutation.key().to_vec()))
                    .is_some_and(|predicate| predicate.current.is_none())
            {
                return Err(invalid("delete mutation requires an existing value"));
            }
        }
        Ok(plan)
    }

    fn validate_history_projection(
        &self,
        command: &MetadataCommand,
        plan: &PredicatePlan,
    ) -> Result<(), AgentMetadataError> {
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
    ) -> Result<Option<MetadataCommandResult>, AgentMetadataError> {
        let Some(value) = self
            .command_dedupe
            .get(key)
            .map_err(|error| backend("read CommandDedupe", error))?
        else {
            return Ok(None);
        };
        let record =
            CommandDedupeRecord::decode(&value).map_err(|error| corrupt("CommandDedupe", error))?;
        if record.command_digest != digest {
            return Err(AgentMetadataError::RequestIdReused);
        }
        Ok(Some(MetadataCommandResult {
            commit_version: record.commit_version,
            deterministic_result: record.deterministic_result,
            replayed: true,
        }))
    }

    fn recovery_state_unlocked(&self) -> Result<RecoveryState, AgentMetadataError> {
        let lsn = decode_system_u64(
            &required_record(
                &self.system,
                SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                "System(applied_recovery_lsn)",
            )?
            .value,
            "System(applied_recovery_lsn)",
        )?;
        let chain_digest = decode_system_digest(
            &required_record(
                &self.system,
                SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
                "System(recovery_chain_digest)",
            )?
            .value,
            "System(recovery_chain_digest)",
        )?;
        Ok(RecoveryState {
            applied_recovery_lsn: lsn,
            chain_digest,
        })
    }

    fn plan_recovery(
        &self,
        mutation: RecoveryMutationV1,
        result: RecoveryResultV1,
    ) -> Result<RecoveryPlan, AgentMetadataError> {
        let lsn_record = required_record(
            &self.system,
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            "System(applied_recovery_lsn)",
        )?;
        let applied_lsn = decode_system_u64(&lsn_record.value, "System(applied_recovery_lsn)")?;
        let recovery_lsn = applied_lsn
            .checked_add(1)
            .ok_or(AgentMetadataError::VersionOverflow)?;
        let digest_record = required_record(
            &self.system,
            SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
            "System(recovery_chain_digest)",
        )?;
        let previous_chain_digest =
            decode_system_digest(&digest_record.value, "System(recovery_chain_digest)")?;
        let row = RecoveryOutboxRecord::new(recovery_lsn, previous_chain_digest, mutation, result)
            .map_err(|error| corrupt("RecoveryOutbox", error))?;
        let logical = row
            .encode()
            .map_err(|error| corrupt("RecoveryOutbox", error))?;
        let (header, chunks) = split_recovery_storage(&logical)
            .map_err(|error| corrupt("RecoveryOutbox storage", error))?;
        Ok(RecoveryPlan {
            lsn_record,
            digest_record,
            header,
            chunks,
            row,
        })
    }

    fn verify_recovery_chain_unlocked(&self) -> Result<RecoveryState, AgentMetadataError> {
        let state = self.recovery_state_unlocked()?;
        let mut expected_lsn = 1_u64;
        let mut previous_chain_digest = recovery_genesis_digest(self.logical_shard_id);
        let mut expected_chunk_keys = BTreeSet::new();
        for entry in self.recovery_outbox.range() {
            let holt::RangeEntry::Key { key, value, .. } =
                entry.map_err(|error| backend("verify RecoveryOutbox", error))?
            else {
                continue;
            };
            match key.first().copied() {
                Some(0) => {
                    let key_lsn = decode_recovery_outbox_key(&key)
                        .map_err(|error| corrupt("RecoveryOutbox key", error))?;
                    let chunk_count = recovery_storage_chunk_count(&value)
                        .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
                    for index in 0..chunk_count {
                        expected_chunk_keys.insert(recovery_chunk_key(key_lsn, index).to_vec());
                    }
                    let row = self.read_recovery_record(key_lsn, &value)?;
                    if key_lsn != expected_lsn || row.recovery_lsn != expected_lsn {
                        return Err(AgentMetadataError::CorruptRecord {
                            record: "RecoveryOutbox",
                            reason: format!(
                                "expected contiguous LSN {expected_lsn}, found key {key_lsn} row {}",
                                row.recovery_lsn
                            ),
                        });
                    }
                    if row.previous_chain_digest != previous_chain_digest {
                        return Err(AgentMetadataError::CorruptRecord {
                            record: "RecoveryOutbox",
                            reason: format!("LSN {expected_lsn} does not link to its predecessor"),
                        });
                    }
                    previous_chain_digest = row.chain_digest;
                    expected_lsn = expected_lsn
                        .checked_add(1)
                        .ok_or(AgentMetadataError::VersionOverflow)?;
                }
                Some(1) => {
                    if !expected_chunk_keys.remove(&key) {
                        return Err(AgentMetadataError::CorruptRecord {
                            record: "RecoveryOutbox chunk",
                            reason: "orphaned or malformed chunk key".to_owned(),
                        });
                    }
                }
                _ => {
                    return Err(AgentMetadataError::CorruptRecord {
                        record: "RecoveryOutbox key",
                        reason: "unknown storage-key tag".to_owned(),
                    });
                }
            }
        }
        if !expected_chunk_keys.is_empty() {
            return Err(AgentMetadataError::CorruptRecord {
                record: "RecoveryOutbox chunk",
                reason: "one or more declared chunks are missing".to_owned(),
            });
        }
        let observed_lsn = expected_lsn - 1;
        if state.applied_recovery_lsn != observed_lsn || state.chain_digest != previous_chain_digest
        {
            return Err(AgentMetadataError::CorruptRecord {
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
        recovery_lsn: u64,
        header: &[u8],
    ) -> Result<RecoveryOutboxRecord, AgentMetadataError> {
        let chunk_count = recovery_storage_chunk_count(header)
            .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
        let mut chunks = Vec::with_capacity(chunk_count as usize);
        for index in 0..chunk_count {
            let value = self
                .recovery_outbox
                .get(&recovery_chunk_key(recovery_lsn, index))
                .map_err(|error| backend("read RecoveryOutbox chunk", error))?
                .ok_or_else(|| AgentMetadataError::CorruptRecord {
                    record: "RecoveryOutbox chunk",
                    reason: format!("missing LSN {recovery_lsn} chunk {index}"),
                })?;
            chunks.push(value);
        }
        let logical = assemble_recovery_storage(header, chunks)
            .map_err(|error| corrupt("RecoveryOutbox storage", error))?;
        RecoveryOutboxRecord::decode(&logical).map_err(|error| corrupt("RecoveryOutbox", error))
    }

    fn family(&self, family: MetadataFamily) -> &Tree {
        self.families
            .get(&family)
            .expect("every MetadataFamily is opened at startup")
    }
}

#[derive(Default)]
struct PredicatePlan {
    exact: BTreeMap<(MetadataFamily, Vec<u8>), PlannedExactPredicate>,
    prefix_empty: Vec<(MetadataFamily, Vec<u8>)>,
}

struct RecoveryPlan {
    lsn_record: Record,
    digest_record: Record,
    header: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    row: RecoveryOutboxRecord,
}

struct PlannedExactPredicate {
    family: MetadataFamily,
    key: Vec<u8>,
    current: Option<CurrentValue>,
    version: Option<RecordVersion>,
}

enum RootFencePlan {
    Install {
        value: Vec<u8>,
    },
    Assert {
        version: RecordVersion,
    },
    Replace {
        version: RecordVersion,
        value: Vec<u8>,
    },
}

fn enqueue_root_fence(
    batch: &mut holt::DBAtomicBatch,
    command: &MetadataCommand,
    plan: &RootFencePlan,
) {
    match plan {
        RootFencePlan::Install { value } => {
            batch.put_if_absent(ROOT_FENCE_TREE, command.root_id.as_bytes(), value);
        }
        RootFencePlan::Assert { version } => {
            batch.assert_version(ROOT_FENCE_TREE, command.root_id.as_bytes(), *version);
        }
        RootFencePlan::Replace { version, value } => {
            batch.compare_and_put(ROOT_FENCE_TREE, command.root_id.as_bytes(), *version, value);
        }
    }
}

fn enqueue_recovery(batch: &mut holt::DBAtomicBatch, plan: &RecoveryPlan) {
    batch.compare_and_put(
        SYSTEM_TREE,
        SYSTEM_APPLIED_RECOVERY_LSN_KEY,
        plan.lsn_record.version,
        &encode_system_u64(plan.row.recovery_lsn),
    );
    batch.compare_and_put(
        SYSTEM_TREE,
        SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
        plan.digest_record.version,
        &encode_system_digest(plan.row.chain_digest),
    );
    batch.put_if_absent(
        RECOVERY_OUTBOX_TREE,
        &recovery_outbox_key(plan.row.recovery_lsn),
        &plan.header,
    );
    for (index, chunk) in plan.chunks.iter().enumerate() {
        batch.put_if_absent(
            RECOVERY_OUTBOX_TREE,
            &recovery_chunk_key(plan.row.recovery_lsn, index as u32),
            chunk,
        );
    }
}

fn enqueue_predicate_guards(
    batch: &mut holt::DBAtomicBatch,
    plan: &PredicatePlan,
    commit_version: CommitVersion,
) {
    for predicate in plan.exact.values() {
        match predicate.version {
            Some(version) => {
                batch.assert_version(predicate.family.tree_name(), &predicate.key, version);
            }
            None => {
                let sentinel = CurrentValue {
                    created_version: commit_version,
                    modified_version: commit_version,
                    payload: Vec::new(),
                }
                .encode()
                .expect("empty sentinel fits current-value envelope");
                batch.put_if_absent(predicate.family.tree_name(), &predicate.key, &sentinel);
                batch.delete(predicate.family.tree_name(), &predicate.key);
            }
        }
    }
    for (family, prefix) in &plan.prefix_empty {
        batch.assert_prefix_empty(family.tree_name(), prefix);
    }
}

fn validate_root_placement(
    command: &MetadataCommand,
    fence: RootFence,
) -> Result<(), AgentMetadataError> {
    if fence.logical_shard_id == command.logical_shard_id
        && fence.placement_generation == command.placement_generation
    {
        Ok(())
    } else {
        Err(AgentMetadataError::PlacementMismatch)
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

fn validate_tree_registry(db: &DB) -> Result<(), AgentMetadataError> {
    let mut actual = db
        .list_trees()
        .map_err(|error| backend("inspect schema trees", error))?;
    actual.sort();
    let mut expected = SCHEMA_TREES
        .iter()
        .map(|tree| (*tree).to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    if actual == expected {
        Ok(())
    } else {
        Err(AgentMetadataError::SchemaGate {
            reason: format!("expected trees {expected:?}, found {actual:?}"),
        })
    }
}

fn required_record(
    tree: &Tree,
    key: &[u8],
    record: &'static str,
) -> Result<Record, AgentMetadataError> {
    tree.get_record(key)
        .map_err(|error| backend("read required record", error))?
        .ok_or_else(|| AgentMetadataError::CorruptRecord {
            record,
            reason: "record is missing".to_owned(),
        })
}

fn encode_shard_identity(shard: LogicalShardId) -> Vec<u8> {
    let mut value = Vec::with_capacity(1 + LogicalShardId::BYTE_WIDTH);
    value.push(SYSTEM_VALUE_FORMAT_VERSION);
    value.extend_from_slice(shard.as_bytes());
    value
}

fn decode_shard_identity(value: &[u8]) -> Result<LogicalShardId, AgentMetadataError> {
    if value.len() != 1 + LogicalShardId::BYTE_WIDTH
        || value.first() != Some(&SYSTEM_VALUE_FORMAT_VERSION)
    {
        return Err(AgentMetadataError::CorruptRecord {
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

fn decode_system_u64(value: &[u8], record: &'static str) -> Result<u64, AgentMetadataError> {
    if value.len() != 9 || value.first() != Some(&SYSTEM_VALUE_FORMAT_VERSION) {
        return Err(AgentMetadataError::CorruptRecord {
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
) -> Result<[u8; RECOVERY_CHAIN_DIGEST_BYTES], AgentMetadataError> {
    if value.len() != 1 + RECOVERY_CHAIN_DIGEST_BYTES
        || value.first() != Some(&SYSTEM_VALUE_FORMAT_VERSION)
    {
        return Err(AgentMetadataError::CorruptRecord {
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
    prefix.push(family.history_tag());
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

fn history_user_key(encoded: &[u8]) -> Result<&[u8], AgentMetadataError> {
    const HEADER_BYTES: usize = 1 + 4;
    const VERSION_BYTES: usize = 8;
    if encoded.len() < HEADER_BYTES + VERSION_BYTES {
        return Err(AgentMetadataError::CorruptRecord {
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
        .ok_or_else(|| AgentMetadataError::CorruptRecord {
            record: "History key",
            reason: "key length overflow".to_owned(),
        })?;
    if encoded.len() != expected {
        return Err(AgentMetadataError::CorruptRecord {
            record: "History key",
            reason: format!("expected {expected} bytes, found {}", encoded.len()),
        });
    }
    Ok(&encoded[HEADER_BYTES..HEADER_BYTES + user_key_bytes])
}

fn change_event_key(root: RootId, version: CommitVersion, sequence: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 8 + 4);
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(&version.get().to_be_bytes());
    key.extend_from_slice(
        &u32::try_from(sequence)
            .expect("validated event count fits u32")
            .to_be_bytes(),
    );
    key
}

fn validate_root_scoped_bytes(
    root: RootId,
    value: &[u8],
    kind: &'static str,
) -> Result<(), AgentMetadataError> {
    if value.len() > MAX_COMMAND_KEY_BYTES {
        return Err(invalid(format!("{kind} exceeds size bound")));
    }
    if !value.starts_with(root.as_bytes()) {
        return Err(invalid(format!("{kind} is outside command root")));
    }
    Ok(())
}

fn validate_value_bytes(value: &[u8], kind: &'static str) -> Result<(), AgentMetadataError> {
    if value.len() > MAX_COMMAND_VALUE_BYTES {
        Err(invalid(format!("{kind} exceeds size bound")))
    } else {
        Ok(())
    }
}

fn require_fresh_location(path: &Path) -> Result<(), AgentMetadataError> {
    match std::fs::metadata(path) {
        Ok(metadata) if !metadata.is_dir() => Err(AgentMetadataError::SchemaGate {
            reason: format!("{} is not a directory", path.display()),
        }),
        Ok(_) => {
            let mut entries =
                std::fs::read_dir(path).map_err(|error| AgentMetadataError::Backend {
                    operation: "inspect store directory",
                    message: error.to_string(),
                })?;
            if entries
                .next()
                .transpose()
                .map_err(|error| AgentMetadataError::Backend {
                    operation: "inspect store directory",
                    message: error.to_string(),
                })?
                .is_some()
            {
                Err(AgentMetadataError::SchemaGate {
                    reason: format!("{} is not empty", path.display()),
                })
            } else {
                Ok(())
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AgentMetadataError::Backend {
            operation: "inspect store path",
            message: error.to_string(),
        }),
    }
}

fn require_existing_location(path: &Path) -> Result<(), AgentMetadataError> {
    let metadata = std::fs::metadata(path).map_err(|error| AgentMetadataError::SchemaGate {
        reason: format!("cannot open {}: {error}", path.display()),
    })?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(AgentMetadataError::SchemaGate {
            reason: format!("{} is not a directory", path.display()),
        })
    }
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hash_u64(hasher: &mut Sha256, value: usize) {
    hasher.update((value as u64).to_be_bytes());
}

fn invalid(reason: impl Into<String>) -> AgentMetadataError {
    AgentMetadataError::InvalidCommand {
        reason: reason.into(),
    }
}

fn corrupt(record: &'static str, error: impl std::fmt::Display) -> AgentMetadataError {
    AgentMetadataError::CorruptRecord {
        record,
        reason: error.to_string(),
    }
}

fn backend(operation: &'static str, error: impl std::fmt::Display) -> AgentMetadataError {
    AgentMetadataError::Backend {
        operation,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::tempdir;

    use super::super::query_records::{ChangeEventKind, ChangeEventRecord, TypedProjection};
    use super::*;

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
        store: &AgentMetadataStore,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(2),
            logical_shard_id: shard(1),
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

    fn ready_store() -> AgentMetadataStore {
        let store = AgentMetadataStore::open_memory(shard(1)).unwrap();
        make_store_ready(store)
    }

    fn ready_file_store(path: &std::path::Path) -> AgentMetadataStore {
        let store = AgentMetadataStore::create_file(path, shard(1)).unwrap();
        make_store_ready(store)
    }

    fn make_store_ready(store: AgentMetadataStore) -> AgentMetadataStore {
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

    fn create_command(
        store: &AgentMetadataStore,
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

    fn read_operation(
        store: &AgentMetadataStore,
        key: &[u8],
        version: CommitVersion,
    ) -> Option<Vec<u8>> {
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

    #[test]
    fn fresh_store_freezes_schema_shard_and_bootstrap_version() {
        let store = AgentMetadataStore::open_memory(shard(1)).unwrap();
        let mut actual = store.db.list_trees().unwrap();
        actual.sort();
        let mut expected = SCHEMA_TREES
            .iter()
            .map(|tree| (*tree).to_owned())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(actual, expected);
        assert_eq!(store.current_read_version().unwrap().get(), 1);
        assert_eq!(
            store.advance_owner_epoch(Some(epoch(1)), epoch(2)),
            Err(AgentMetadataError::OwnerEpochMismatch {
                expected: 1,
                actual: 0,
            })
        );
        store.advance_owner_epoch(None, epoch(1)).unwrap();
        store.advance_owner_epoch(None, epoch(1)).unwrap();
    }

    #[test]
    fn file_reopen_rejects_a_different_logical_shard() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        {
            let store = AgentMetadataStore::create_file(&database, shard(1)).unwrap();
            store.advance_owner_epoch(None, epoch(1)).unwrap();
        }
        assert!(matches!(
            AgentMetadataStore::reopen_file(&database, shard(2)),
            Err(AgentMetadataError::SchemaGate { .. })
        ));
        let reopened = AgentMetadataStore::reopen_file(&database, shard(1)).unwrap();
        assert_eq!(reopened.current_read_version().unwrap().get(), 1);
    }

    #[test]
    fn root_install_activate_and_stale_fences_fail_closed() {
        let store = ready_store();
        let duplicate_install = base_command(&store, request(3), RootFenceAction::Install).seal();
        assert_eq!(
            store.execute(&duplicate_install),
            Err(AgentMetadataError::RootFenceAlreadyInstalled)
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
            Err(AgentMetadataError::PlacementMismatch)
        );

        store.advance_owner_epoch(Some(epoch(1)), epoch(2)).unwrap();
        let stale_owner =
            create_command(&store, request(5), scoped_key(root(2), b"other"), b"value");
        assert_eq!(
            store.execute(&stale_owner),
            Err(AgentMetadataError::OwnerEpochMismatch {
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
        assert_eq!(
            store.execute(&mismatch),
            Err(AgentMetadataError::RequestIdReused)
        );
        assert_eq!(
            read_operation(&store, &key, first.commit_version),
            Some(b"v1".to_vec())
        );
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
        store
            .family(MetadataFamily::Operation)
            .put(&before_cursor, b"corrupt-before-cursor")
            .unwrap();
        store
            .family(MetadataFamily::Operation)
            .put(&after_limit, b"corrupt-after-limit")
            .unwrap();

        let page = store
            .scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                store.current_read_version().unwrap(),
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
    }

    #[test]
    fn historical_prefix_scan_remains_fail_closed_past_page_limit() {
        let store = ready_store();
        let prefix = scoped_key(root(2), b"historical-bounded/");
        let first_key = scoped_key(root(2), b"historical-bounded/01");
        let corrupt_tail = scoped_key(root(2), b"historical-bounded/02-corrupt");
        let first = store
            .execute(&create_command(&store, request(3), first_key, b"first"))
            .unwrap();
        store
            .execute(&create_command(
                &store,
                request(4),
                scoped_key(root(2), b"outside-prefix"),
                b"advance-clock",
            ))
            .unwrap();
        store
            .family(MetadataFamily::Operation)
            .put(&corrupt_tail, b"corrupt-historical-tail")
            .unwrap();

        assert!(matches!(
            store.scan_prefix_at(
                root(2),
                generation(7),
                epoch(1),
                MetadataFamily::Operation,
                &prefix,
                ReadVersion::new(first.commit_version.get()).unwrap(),
                None,
                1,
            ),
            Err(AgentMetadataError::CorruptRecord {
                record: OPERATION_TREE,
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
            Err(AgentMetadataError::PredicateFailed)
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
            Err(AgentMetadataError::PlacementMismatch)
        );
        assert_eq!(
            store.observe_lease_clock(root(2), generation(7), epoch(2), 101),
            Err(AgentMetadataError::OwnerEpochMismatch {
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
            Err(AgentMetadataError::LeaseDeadlineReached {
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
            Err(AgentMetadataError::WriteReadVersionMismatch {
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
            Err(AgentMetadataError::CommandDigestMismatch)
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
            Err(AgentMetadataError::InvalidCommand { .. })
        ));
    }

    #[test]
    fn command_size_bounds_fit_holt_envelopes_and_reject_before_atomic() {
        let store = ready_store();
        let key = scoped_key(root(2), b"holt-value-boundary");
        let boundary = vec![0x5a; MAX_HOLT_RECORD_PAYLOAD_BYTES];

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

        let replacement = vec![0x6b; MAX_HOLT_RECORD_PAYLOAD_BYTES];
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
        let oversized = vec![0; MAX_HOLT_RECORD_PAYLOAD_BYTES + 1];

        let oversized_mutation = create_command(
            &store,
            request(42),
            scoped_key(root(2), b"oversized-mutation"),
            &oversized,
        );
        assert!(matches!(
            store.execute(&oversized_mutation),
            Err(AgentMetadataError::InvalidCommand { .. })
        ));

        let mut oversized_event = base_command(&store, request(43), RootFenceAction::RequireActive);
        oversized_event.event_projection.push(EventProjection {
            payload: oversized.clone(),
        });
        assert!(matches!(
            store.execute(&oversized_event.seal()),
            Err(AgentMetadataError::InvalidCommand { .. })
        ));

        let mut oversized_result =
            base_command(&store, request(44), RootFenceAction::RequireActive);
        oversized_result.deterministic_result = oversized;
        assert!(matches!(
            store.execute(&oversized_result.seal()),
            Err(AgentMetadataError::InvalidCommand { .. })
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
        store.execute_before_lease_deadline(&command, 101).unwrap();
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
    fn recovery_outbox_survives_file_reopen_with_exact_tail() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        let expected;
        {
            let store = AgentMetadataStore::create_file(&database, shard(1)).unwrap();
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

        let reopened = AgentMetadataStore::reopen_file(&database, shard(1)).unwrap();
        assert_eq!(reopened.verify_recovery_chain().unwrap(), expected);
        assert_eq!(reopened.recovery_outbox_after(0, 10).unwrap().len(), 4);
        assert_eq!(reopened.lease_clock_high_water().unwrap(), 55);
    }

    #[test]
    fn reopen_rejects_unknown_recovery_storage_key_tag() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("metadata");
        {
            let store = ready_file_store(&database);
            store
                .recovery_outbox
                .put(&[2, 0, 0, 0, 0], b"unknown recovery row")
                .unwrap();
        }
        assert!(matches!(
            AgentMetadataStore::reopen_file(&database, shard(1)),
            Err(AgentMetadataError::CorruptRecord {
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
            assert!(store
                .recovery_outbox
                .delete(&recovery_chunk_key(1, 0))
                .unwrap());
        }
        assert!(matches!(
            AgentMetadataStore::reopen_file(&database, shard(1)),
            Err(AgentMetadataError::CorruptRecord {
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

        let target = AgentMetadataStore::open_memory(shard(1)).unwrap();
        for row in &source_rows {
            match (&row.mutation, &row.result) {
                (
                    RecoveryMutationV1::AdvanceOwnerEpoch { expected, next },
                    RecoveryResultV1::OwnerEpoch {
                        applied_owner_epoch,
                    },
                ) => {
                    target.advance_owner_epoch(*expected, *next).unwrap();
                    assert_eq!(next, applied_owner_epoch);
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
                ) => assert_eq!(
                    target
                        .observe_lease_clock(
                            *root_id,
                            *placement_generation,
                            *owner_epoch,
                            *observed_ms,
                        )
                        .unwrap(),
                    *effective_high_water_ms
                ),
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
                    let replayed = match lease_deadline_ms {
                        Some(deadline) => target
                            .execute_before_lease_deadline(command, *deadline)
                            .unwrap(),
                        None => target.execute(command).unwrap(),
                    };
                    assert_eq!(&replayed.commit_version, commit_version);
                    assert_eq!(&replayed.deterministic_result, deterministic_result);
                    assert!(!replayed.replayed);
                }
                _ => panic!("strict recovery codec forbids mismatched mutation/result variants"),
            }
        }

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
}
