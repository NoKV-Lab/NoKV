use std::collections::{BTreeMap, BTreeSet};
#[cfg(feature = "metadata-read-stats")]
use std::marker::PhantomData;
use std::path::Path;
#[cfg(feature = "metadata-read-stats")]
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard};

#[cfg(test)]
use nokv_types::TargetActivationToken;
use nokv_types::{
    CommandDigest, CommitVersion, LogicalShardId, MetadataMigrationTargetBinding,
    MetadataRecoveryFrontier, OperationId, OwnerEpoch, PlacementGeneration, ReadVersion, RequestId,
    RootActivationState, RootId, RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
    SourceQuiesceReceipt,
};
use sha2::{Digest, Sha256};

#[cfg(test)]
use crate::built_in_holt::HoltRuntimeGuard;
use crate::built_in_holt::NoopHoltRuntimeGuard;

use super::authority::{
    decode_authority_marker, decode_store_identity, encode_authority_marker, encode_store_identity,
    validate_metadata_store_identity, workspace_metadata_contract_digest,
    AcknowledgedMetadataFrontier, MetadataAuthorityEvidence, MetadataAuthorityMarker,
    MetadataAuthorityState, MetadataStoreIdentity,
};
#[cfg(test)]
use super::codec::SCHEMA_TREES;
use super::codec::{
    change_event_key, encode_schema_marker, validate_schema_marker, ARTIFACT_MANIFEST_TREE,
    ARTIFACT_REVISION_TREE, COMMIT_CONSUMER_TREE, COMMIT_MEMBER_TREE, COMMIT_TREE, GC_BARRIER_TREE,
    GC_CANDIDATE_TREE, HISTORY_HOLD_TREE, OPERATION_TREE, PATH_CURRENT_TREE, RESTORE_MEMBER_TREE,
    REVISION_REF_TREE, SCHEMA_ID, SECONDARY_INDEX_TREE, SNAPSHOT_ALIAS_TREE, SNAPSHOT_REF_TREE,
    STAGED_OBJECT_TREE, SYSTEM_SCHEMA_KEY, TAG_TREE, WORKBENCH_COMMIT_HEAD_TREE,
    WORKSPACE_CURRENT_TREE, WORKSPACE_INCARNATION_CLAIM_TREE,
};
use super::commit_receipt::{
    digest_authority_marker, digest_source_receipt, digest_target_binding, purpose_evidence_digest,
    MetadataAuthorityCommitActionV1, MetadataCommandCommitClassV1,
    MetadataCommitLiveResolutionOriginV1, MetadataCommitPurposeV1,
    MetadataCommitReceiptDirtySourceV1, MetadataCommitReceiptErrorV1,
    MetadataCommitReceiptMutationBackendResultV1, MetadataCommitReceiptPersistBackendResultV1,
    MetadataCommitReceiptPersistCommandV1, MetadataCommitReceiptPersistErrorV1,
    MetadataCommitReceiptPersistOutcomeV1, MetadataCommitReceiptPoisonCommandV1,
    MetadataCommitReceiptPoisonOutcomeV1, MetadataCommitReceiptPoisonReasonV1,
    MetadataCommitReceiptQualificationV1, MetadataCommitReceiptResolveCommandV1,
    MetadataCommitReceiptResolveOutcomeV1, MetadataCommitReceiptStateV1,
    MetadataCommitReceiptStoreV1, MetadataCommitResolutionV1, MetadataFrontierPointV1,
    MetadataRuntimeCommitBundleV1, PlannedMetadataCommitV1,
};
#[cfg(test)]
use super::commit_receipt::{
    digest_target_token, MetadataCommitReceiptMutationNotDispatchedV1,
    MetadataCommitReceiptPersistNotDispatchedV1, MetadataCommitResolutionBasisV1,
};
use super::commit_recovery_fence::{
    mint_pending_recovery_open_v1, MetadataCommitRecoveryFenceFactoryV1,
    MetadataOldDispatchExclusionInstallationV1, MetadataPendingRecoveryOpenCommandV1,
    MetadataPendingRecoveryOpenOutcomeV1,
};
#[cfg(test)]
use super::provider::HoltProvider;
use super::provider::{
    all_ordered_spaces, AtomicCommitOutcome, AtomicOp, AtomicPlan, HoltProviderFactory,
    MetadataProvider, MetadataReadView, MetadataTransaction, OrderedSpaceId, ProviderCapabilities,
    ProviderError, ProviderErrorKind, ProviderRecord, ProviderScan, ProviderScanItem, ReadScope,
    ReadWitness,
};
#[cfg(feature = "metadata-read-stats")]
use super::read_stats::{self, MetadataReadStats, MetadataReadStatsSessionError};
use super::records::{CommandDedupeRecord, CurrentValue, HistoryValue, RootFence};
use super::recovery::{
    assemble_recovery_storage, decode_recovery_outbox_key, recovery_chunk_key,
    recovery_genesis_digest, recovery_outbox_key, recovery_storage_chunk_count,
    recovery_storage_logical_length, split_recovery_storage, RecoveryMutationV1,
    RecoveryOutboxRecord, RecoveryResultV1, RecoveryState, MAX_RECOVERY_BYTES,
    MAX_STORAGE_CHUNK_DATA_BYTES, RECOVERY_CHAIN_DIGEST_BYTES,
};
use crate::provider::v1::{
    CreateRecoveryIntentV1, MetadataProviderFactoryV1, ProviderContractOfferV1,
    ProviderCreateRequestV1, ProviderDiagnosticsSnapshotV1, ProviderDiagnosticsV1,
    ProviderOperationV1, ProviderReopenRequestV1, ProviderSchemaV1,
};

/// Nominal authority for engine-owned receipt and recovery command minting.
///
/// The type is visible to sibling protocol modules only as a constructor
/// parameter. Its field and production constructor remain private to this
/// module, so provider bindings and forwarding runtimes cannot manufacture
/// commit or recovery authority.
pub(super) struct MetadataCommitEngineMintAuthorityV1 {
    _private: (),
}

impl MetadataCommitEngineMintAuthorityV1 {
    fn new() -> Self {
        Self { _private: () }
    }

    #[cfg(test)]
    pub(super) fn for_test() -> Self {
        Self::new()
    }
}

pub(super) const SYSTEM_STORE_IDENTITY_KEY: &[u8] = b"store_identity";
pub(super) const SYSTEM_METADATA_AUTHORITY_KEY: &[u8] = b"metadata_authority";
pub(super) const SYSTEM_OWNER_FENCE_KEY: &[u8] = b"owner_fence";
pub(super) const SYSTEM_COMMIT_CLOCK_KEY: &[u8] = b"commit_clock";
pub(super) const SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY: &[u8] = b"lease_clock_high_water";
pub(super) const SYSTEM_APPLIED_RECOVERY_LSN_KEY: &[u8] = b"applied_recovery_lsn";
pub(super) const SYSTEM_RECOVERY_CHAIN_DIGEST_KEY: &[u8] = b"recovery_chain_digest";
const SYSTEM_VALUE_FORMAT_VERSION: u8 = 1;
const INITIAL_COMMIT_VERSION: u64 = 1;

pub(super) const MAX_COMMAND_ITEMS: usize = 256;
const MAX_DELIMITED_SCAN_ITEMS: usize = MAX_COMMAND_ITEMS * 2;
const MAX_COMMAND_KEY_BYTES: usize = 8 * 1024;
// The default provider limits one stored value to u16::MAX bytes. Domain
// payloads are wrapped in durable envelopes, so retain explicit headroom.
const MAX_METADATA_RECORD_PAYLOAD_BYTES: usize = 60 * 1024;
const MAX_COMMAND_VALUE_BYTES: usize = MAX_METADATA_RECORD_PAYLOAD_BYTES;
const MAX_DETERMINISTIC_RESULT_BYTES: usize = MAX_METADATA_RECORD_PAYLOAD_BYTES;
const MAX_EVENT_BYTES: usize = MAX_METADATA_RECORD_PAYLOAD_BYTES;

#[derive(Clone, Copy)]
pub(crate) struct CanonicalProviderRequirementValues {
    pub max_key_bytes: usize,
    pub max_value_bytes: usize,
    pub max_atomic_operations: usize,
    pub max_logical_plan_bytes: usize,
}

pub(crate) const fn canonical_provider_requirement_values() -> CanonicalProviderRequirementValues {
    const HISTORY_KEY_ENVELOPE_BYTES: usize = 1 + 4 + 8;
    const COMMAND_DEDUPE_VALUE_ENVELOPE_BYTES: usize = 1 + 32 + 8 + 8 + 4;
    const FIXED_COMMAND_OPERATIONS: usize = 11;
    const MAX_RECOVERY_CHUNKS: usize = MAX_RECOVERY_BYTES.div_ceil(MAX_STORAGE_CHUNK_DATA_BYTES);
    const MAX_ATOMIC_OPERATIONS: usize =
        FIXED_COMMAND_OPERATIONS + (MAX_COMMAND_ITEMS * 4) + MAX_RECOVERY_CHUNKS;
    const MAX_KEY_BYTES: usize = MAX_COMMAND_KEY_BYTES + HISTORY_KEY_ENVELOPE_BYTES;
    const MAX_VALUE_BYTES: usize =
        MAX_METADATA_RECORD_PAYLOAD_BYTES + COMMAND_DEDUPE_VALUE_ENVELOPE_BYTES;
    CanonicalProviderRequirementValues {
        max_key_bytes: MAX_KEY_BYTES,
        max_value_bytes: MAX_VALUE_BYTES,
        max_atomic_operations: MAX_ATOMIC_OPERATIONS,
        // This is a complete, deliberately conservative ceiling: every legal
        // operation fits one largest engine-emitted key plus one largest
        // engine-emitted value. It avoids assuming provider-private framing or
        // correlations between command fields and recovery chunks.
        max_logical_plan_bytes: MAX_ATOMIC_OPERATIONS * (MAX_KEY_BYTES + MAX_VALUE_BYTES),
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
    WorkspaceIncarnationClaim = 0x17,
}

impl MetadataFamily {
    pub(super) const fn tree_name(self) -> &'static str {
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
            Self::WorkspaceIncarnationClaim => WORKSPACE_INCARNATION_CLAIM_TREE,
        }
    }

    pub(super) const fn history_tag(self) -> u8 {
        self as u8
    }

    pub(super) const ALL: [Self; 20] = [
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
        Self::WorkspaceIncarnationClaim,
    ];
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RootFenceAction {
    Install {
        layout_profile: RootLayoutProfile,
        layout_generation: RootLayoutGeneration,
        partition_id: RootPartitionId,
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
            RootFenceAction::Install {
                layout_profile,
                layout_generation,
                partition_id,
            } => {
                hasher.update([1, layout_profile.into()]);
                hasher.update(layout_generation.get().to_be_bytes());
                hasher.update(partition_id.as_bytes());
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
pub(super) enum DelimitedMetadataScanItem {
    Record(MetadataScanItem),
    CommonPrefix(Vec<u8>),
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
    CommitReceiptRecoveryRequired,
    CommitOutcomeUnknown,
    TransactionTooLarge {
        affected_bytes: usize,
        max_bytes: usize,
    },
    ProviderUnavailable {
        operation: &'static str,
        message: String,
    },
    ProviderAuthorityMismatch {
        operation: &'static str,
        message: String,
    },
    MetadataStoreIdentityMismatch,
    MetadataAuthorityBindingMismatch,
    MetadataAuthorityStateMismatch {
        expected: MetadataAuthorityState,
        actual: MetadataAuthorityState,
    },
    InvalidMetadataAuthorityTransition {
        from: MetadataAuthorityState,
        to: MetadataAuthorityState,
    },
    MetadataMigrationAdmission {
        reason: String,
    },
    VersionOverflow,
}

impl std::fmt::Display for AgentMetadataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Backend { operation, message } => {
                write!(formatter, "metadata provider {operation} failed: {message}")
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
            Self::CommitReceiptRecoveryRequired => {
                formatter.write_str("metadata commit receipt recovery is required")
            }
            Self::CommitOutcomeUnknown => {
                formatter.write_str("metadata command commit outcome is unknown")
            }
            Self::TransactionTooLarge {
                affected_bytes,
                max_bytes,
            } => write!(
                formatter,
                "metadata transaction affects {affected_bytes} bytes; provider limit is {max_bytes}"
            ),
            Self::ProviderUnavailable { operation, message } => {
                write!(formatter, "metadata provider unavailable during {operation}: {message}")
            }
            Self::ProviderAuthorityMismatch { operation, message } => write!(
                formatter,
                "metadata provider authority mismatch during {operation}: {message}"
            ),
            Self::MetadataStoreIdentityMismatch => {
                formatter.write_str("metadata store identity does not match the admitted identity")
            }
            Self::MetadataAuthorityBindingMismatch => formatter
                .write_str("metadata authority marker does not match the immutable store identity"),
            Self::MetadataAuthorityStateMismatch { expected, actual } => write!(
                formatter,
                "metadata authority state mismatch: expected {expected:?}, actual {actual:?}"
            ),
            Self::InvalidMetadataAuthorityTransition { from, to } => write!(
                formatter,
                "invalid metadata authority transition {from:?} -> {to:?}"
            ),
            Self::MetadataMigrationAdmission { reason } => {
                write!(formatter, "metadata migration admission failed: {reason}")
            }
            Self::VersionOverflow => formatter.write_str("metadata commit version overflow"),
        }
    }
}

impl std::error::Error for AgentMetadataError {}

/// Canonical workspace metadata facade.
///
/// An opened facade never exposes the provider or its raw transaction handle:
///
/// ```compile_fail
/// use nokv_meta::provider::v1::MetadataProvider;
/// use nokv_meta::workspace::AgentMetadataStore;
///
/// fn begin_raw_transaction(store: &AgentMetadataStore) {
///     let _raw_transaction = store.provider.begin_write();
/// }
/// ```
///
/// Standalone constructors derive their store identity internally. An
/// arbitrary authority identity cannot be combined with implicit standalone
/// acknowledgement or runtime guards:
///
/// ```compile_fail
/// use nokv_meta::workspace::{AgentMetadataStore, MetadataStoreIdentity};
///
/// let identity: MetadataStoreIdentity = todo!();
/// let _store = AgentMetadataStore::open_memory_with_identity(identity);
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::{AgentMetadataStore, MetadataStoreIdentity};
///
/// let identity: MetadataStoreIdentity = todo!();
/// let _store = AgentMetadataStore::create_file_with_identity("metadata", identity);
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::{AgentMetadataStore, MetadataStoreIdentity};
///
/// let identity: MetadataStoreIdentity = todo!();
/// let _store = AgentMetadataStore::reopen_file_with_identity("metadata", identity);
/// ```
///
/// Provider A and receipt B cannot be supplied as independent allocations:
///
/// ```compile_fail
/// use std::sync::Arc;
/// use nokv_meta::provider::v1::{CreateRecoveryIntentV1, MetadataProviderFactoryV1};
/// use nokv_meta::workspace::{
///     AgentMetadataStore, MetadataCommitReceiptStoreV1, MetadataStoreCreateModeV1,
///     MetadataStoreIdentity,
/// };
///
/// fn mix_runtime_parts<A, B>(
///     provider_a: Arc<A>,
///     receipt_b: Arc<B>,
///     identity: MetadataStoreIdentity,
/// ) where
///     A: MetadataProviderFactoryV1 + 'static,
///     B: MetadataCommitReceiptStoreV1 + 'static,
/// {
///     let _ = AgentMetadataStore::create_with_runtime_commit_bundle_v1(
///         provider_a,
///         receipt_b,
///         identity,
///         CreateRecoveryIntentV1::Fresh,
///         MetadataStoreCreateModeV1::Active,
///     );
/// }
/// ```
///
/// Migration-target construction remains an internal, not-qualified surface:
///
/// ```compile_fail
/// use nokv_meta::workspace::{
///     AgentMetadataStore, MetadataMigrationTargetBinding, MetadataStoreIdentity,
/// };
///
/// let identity: MetadataStoreIdentity = todo!();
/// let binding: MetadataMigrationTargetBinding = todo!();
/// let _store = AgentMetadataStore::open_migration_target_memory(identity, binding);
/// ```
///
/// ```compile_fail
/// use nokv_meta::workspace::{
///     AgentMetadataStore, MetadataMigrationTargetBinding, MetadataStoreIdentity,
/// };
///
/// let identity: MetadataStoreIdentity = todo!();
/// let binding: MetadataMigrationTargetBinding = todo!();
/// let _store = AgentMetadataStore::create_migration_target_file(
///     "metadata",
///     identity,
///     binding,
/// );
/// ```
#[derive(Clone)]
pub struct AgentMetadataStore {
    provider: Arc<dyn MetadataProvider>,
    fail_stop: Arc<MetadataStoreFailStop>,
    identity: MetadataStoreIdentity,
    command_gate: Arc<RwLock<()>>,
    runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
    receipt_qualification: MetadataCommitReceiptQualificationV1,
    #[cfg(feature = "metadata-read-stats")]
    read_stats_identity: Arc<MetadataReadStatsIdentity>,
}

pub(super) struct MetadataDiagnosticReadView<'a> {
    delegate: Box<dyn MetadataReadView>,
    _command_guard: RwLockReadGuard<'a, ()>,
}

impl MetadataDiagnosticReadView<'_> {
    pub(super) fn as_ref(&self) -> &dyn MetadataReadView {
        self.delegate.as_ref()
    }
}

impl std::ops::Deref for MetadataDiagnosticReadView<'_> {
    type Target = dyn MetadataReadView;

    fn deref(&self) -> &Self::Target {
        self.delegate.as_ref()
    }
}

#[derive(Default)]
struct MetadataStoreFailStop {
    tripped: AtomicBool,
}

impl MetadataStoreFailStop {
    fn ensure_serving(&self, operation: ProviderOperationV1) -> Result<(), ProviderError> {
        if self.tripped.load(Ordering::Acquire) {
            Err(ProviderError::authority_mismatch(operation))
        } else {
            Ok(())
        }
    }

    fn trip(&self) {
        self.tripped.store(true, Ordering::Release);
    }
}

struct PendingCommitFailStopGuard<'a> {
    fail_stop: &'a MetadataStoreFailStop,
    armed: bool,
}

impl<'a> PendingCommitFailStopGuard<'a> {
    fn arm(fail_stop: &'a MetadataStoreFailStop) -> Self {
        Self {
            fail_stop,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingCommitFailStopGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.fail_stop.trip();
        }
    }
}

struct FailStopMetadataProvider {
    delegate: Arc<dyn MetadataProvider>,
    fail_stop: Arc<MetadataStoreFailStop>,
    logical_shard_id: LogicalShardId,
    capabilities: ProviderCapabilities,
    diagnostics: Option<FailStopProviderDiagnostics>,
}

impl FailStopMetadataProvider {
    fn new(delegate: Arc<dyn MetadataProvider>, fail_stop: Arc<MetadataStoreFailStop>) -> Self {
        let logical_shard_id = delegate.logical_shard_id();
        let capabilities = delegate.capabilities();
        let diagnostics = delegate
            .diagnostics()
            .is_some()
            .then(|| FailStopProviderDiagnostics {
                delegate: Arc::clone(&delegate),
                fail_stop: Arc::clone(&fail_stop),
            });
        Self {
            delegate,
            fail_stop,
            logical_shard_id,
            capabilities,
            diagnostics,
        }
    }
}

impl MetadataProvider for FailStopMetadataProvider {
    fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    fn capabilities(&self) -> ProviderCapabilities {
        self.capabilities
    }

    fn validate_runtime(&self) -> Result<(), ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ValidateRuntime)?;
        self.delegate.validate_runtime()?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ValidateRuntime)
    }

    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ReadRecord)?;
        let result = self.delegate.get(space, key)?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ReadRecord)?;
        Ok(result)
    }

    fn begin_read(
        &self,
        scopes: &[ReadScope],
    ) -> Result<Box<dyn MetadataReadView + 'static>, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::BeginRead)?;
        let delegate = self.delegate.begin_read(scopes)?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::BeginRead)?;
        Ok(Box::new(FailStopMetadataReadView {
            delegate,
            fail_stop: Arc::clone(&self.fail_stop),
        }))
    }

    fn begin_write(&self) -> Result<Box<dyn MetadataTransaction + 'static>, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::BeginWrite)?;
        let delegate = self.delegate.begin_write()?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::BeginWrite)?;
        Ok(Box::new(FailStopMetadataTransaction {
            delegate,
            fail_stop: Arc::clone(&self.fail_stop),
        }))
    }

    fn diagnostics(&self) -> Option<&dyn ProviderDiagnosticsV1> {
        self.diagnostics
            .as_ref()
            .map(|diagnostics| diagnostics as &dyn ProviderDiagnosticsV1)
    }
}

struct FailStopMetadataReadView {
    delegate: Box<dyn MetadataReadView>,
    fail_stop: Arc<MetadataStoreFailStop>,
}

impl MetadataReadView for FailStopMetadataReadView {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ReadRecord)?;
        let result = self.delegate.get(space, key)?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ReadRecord)?;
        Ok(result)
    }

    fn scan(
        &self,
        request: &ProviderScan,
    ) -> Result<super::provider::ProviderScanPage, ProviderError> {
        self.fail_stop.ensure_serving(ProviderOperationV1::Scan)?;
        let result = self.delegate.scan(request)?;
        self.fail_stop.ensure_serving(ProviderOperationV1::Scan)?;
        Ok(result)
    }
}

struct FailStopMetadataTransaction {
    delegate: Box<dyn MetadataTransaction>,
    fail_stop: Arc<MetadataStoreFailStop>,
}

impl MetadataReadView for FailStopMetadataTransaction {
    fn get(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
    ) -> Result<Option<ProviderRecord>, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ReadRecord)?;
        let result = self.delegate.get(space, key)?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ReadRecord)?;
        Ok(result)
    }

    fn scan(
        &self,
        request: &ProviderScan,
    ) -> Result<super::provider::ProviderScanPage, ProviderError> {
        self.fail_stop.ensure_serving(ProviderOperationV1::Scan)?;
        let result = self.delegate.scan(request)?;
        self.fail_stop.ensure_serving(ProviderOperationV1::Scan)?;
        Ok(result)
    }
}

impl MetadataTransaction for FailStopMetadataTransaction {
    fn prefix_is_empty(&self, space: OrderedSpaceId, prefix: &[u8]) -> Result<bool, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ValidatePlan)?;
        let result = self.delegate.prefix_is_empty(space, prefix)?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::ValidatePlan)?;
        Ok(result)
    }

    fn commit(self: Box<Self>, plan: AtomicPlan) -> Result<AtomicCommitOutcome, ProviderError> {
        self.fail_stop.ensure_serving(ProviderOperationV1::Commit)?;
        let result = self.delegate.commit(plan);
        if self.fail_stop.tripped.load(Ordering::Acquire) {
            match result {
                Err(error) if error.kind() == ProviderErrorKind::UnknownCommitUnsettled => {
                    Err(error)
                }
                _ => Err(ProviderError::unknown_commit_settled()),
            }
        } else {
            result
        }
    }
}

struct FailStopProviderDiagnostics {
    delegate: Arc<dyn MetadataProvider>,
    fail_stop: Arc<MetadataStoreFailStop>,
}

impl ProviderDiagnosticsV1 for FailStopProviderDiagnostics {
    fn snapshot(&self) -> Result<ProviderDiagnosticsSnapshotV1, ProviderError> {
        self.fail_stop
            .ensure_serving(ProviderOperationV1::Diagnostics)?;
        let diagnostics = self
            .delegate
            .diagnostics()
            .ok_or_else(|| ProviderError::unavailable(ProviderOperationV1::Diagnostics))?;
        let result = diagnostics.snapshot()?;
        self.fail_stop
            .ensure_serving(ProviderOperationV1::Diagnostics)?;
        Ok(result)
    }
}

struct UntrackedStandaloneRuntimeBundle<F> {
    factory: F,
    frozen_digest: [u8; 32],
}

impl<F> UntrackedStandaloneRuntimeBundle<F> {
    fn new(factory: F, location_class: u8) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.metadata.untracked-standalone-runtime.v1\0");
        hasher.update([location_class]);
        Self {
            factory,
            frozen_digest: hasher.finalize().into(),
        }
    }
}

impl<F> MetadataProviderFactoryV1 for UntrackedStandaloneRuntimeBundle<F>
where
    F: MetadataProviderFactoryV1,
{
    fn contract_offer(
        &self,
        schema: &ProviderSchemaV1,
    ) -> Result<ProviderContractOfferV1, ProviderError> {
        self.factory.contract_offer(schema)
    }

    fn create(
        &self,
        request: &ProviderCreateRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        self.factory.create(request)
    }

    fn reopen(
        &self,
        request: &ProviderReopenRequestV1,
    ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
        self.factory.reopen(request)
    }
}

impl<F> MetadataCommitRecoveryFenceFactoryV1 for UntrackedStandaloneRuntimeBundle<F>
where
    F: MetadataCommitRecoveryFenceFactoryV1,
{
    fn old_dispatch_exclusion_installation_v1(&self) -> MetadataOldDispatchExclusionInstallationV1 {
        self.factory.old_dispatch_exclusion_installation_v1()
    }

    fn reopen_pending_with_old_dispatch_excluded_v1(
        &self,
        command: MetadataPendingRecoveryOpenCommandV1,
    ) -> MetadataPendingRecoveryOpenOutcomeV1 {
        self.factory
            .reopen_pending_with_old_dispatch_excluded_v1(command)
    }
}

impl<F> MetadataCommitReceiptStoreV1 for UntrackedStandaloneRuntimeBundle<F>
where
    F: MetadataCommitRecoveryFenceFactoryV1 + Send + Sync,
{
    fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
        MetadataCommitReceiptQualificationV1::UntrackedStandalone
    }

    fn frozen_runtime_bundle_digest_v1(&self) -> [u8; 32] {
        self.frozen_digest
    }

    fn load_commit_receipt_v1(
        &self,
        _store_identity: MetadataStoreIdentity,
    ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
        Ok(MetadataCommitReceiptStateV1::UntrackedStandalone)
    }

    fn persist_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptPersistCommandV1,
    ) -> MetadataCommitReceiptPersistOutcomeV1 {
        command
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::Persisted)
    }

    fn resolve_pending_commit_v1(
        &self,
        command: MetadataCommitReceiptResolveCommandV1,
    ) -> MetadataCommitReceiptResolveOutcomeV1 {
        command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed)
    }

    fn poison_commit_receipt_v1(
        &self,
        command: MetadataCommitReceiptPoisonCommandV1,
    ) -> MetadataCommitReceiptPoisonOutcomeV1 {
        command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed)
    }
}

/// Qualified provider-neutral state installed during public SPI v1 creation.
///
/// Migration-target creation is intentionally absent while that path remains
/// not qualified.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataStoreCreateModeV1 {
    Active,
}

#[cfg(feature = "metadata-read-stats")]
#[derive(Default)]
struct MetadataReadStatsIdentity {
    active: AtomicBool,
}

/// Short-lived point reader bound to one validated root and read version.
///
/// The owning store keeps the shard read gate for the reader's complete
/// lifetime. This type stays inside the workspace package so callers cannot
/// retain the gate across scans, object I/O, or RPC work.
pub(super) struct FencedPointReader<'a> {
    store: &'a AgentMetadataStore,
    root_id: RootId,
    version: ReadVersion,
    current_version: ReadVersion,
}

/// Thread-bound logical read counters plus optional provider-wide telemetry.
///
/// This diagnostic API is available only with the `metadata-read-stats`
/// feature. Reads through clones of `store` are included when they execute on
/// the thread that owns the session. Provider physical counters remain
/// store-wide when supported, so callers must exclude concurrent store activity
/// before attributing them to the measured workload.
#[cfg(feature = "metadata-read-stats")]
#[must_use = "finish the session to obtain counters, or drop it to cancel collection"]
pub struct MetadataReadStatsSession<'a> {
    store: &'a AgentMetadataStore,
    store_key: usize,
    storage_before: MetadataReadStats,
    active: bool,
    not_send: PhantomData<Rc<()>>,
}

#[cfg(feature = "metadata-read-stats")]
impl MetadataReadStatsSession<'_> {
    pub fn finish(mut self) -> Result<MetadataReadStats, MetadataReadStatsSessionError> {
        let logical = read_stats::finish_session(self.store_key)?;
        let storage_after = self
            .store
            .provider_diagnostics_snapshot()
            .map_err(|error| MetadataReadStatsSessionError::Provider(error.to_string()))?;
        let result = storage_after
            .delta_since(&self.storage_before)
            .map(|mut combined| {
                read_stats::merge_logical_counters(&mut combined, logical);
                combined
            })
            .map_err(MetadataReadStatsSessionError::from);
        self.release();
        result
    }

    fn release(&mut self) {
        self.active = false;
        self.store
            .read_stats_identity
            .active
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
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        validate_root_scoped_bytes(self.root_id, key, "point-read key")?;
        if self.version == self.current_version {
            self.store
                .read_current_at_unfenced(family, key, self.current_version)
        } else {
            self.store.read_at_unfenced(family, key, self.version)
        }
    }
}

impl AgentMetadataStore {
    /// Create or reconcile one provider installation through public SPI v1.
    ///
    /// The facade constructs the canonical workspace schema and durable
    /// System genesis. Provider-specific configuration is already resolved by
    /// `factory` and never enters the create request.
    pub fn create_with_runtime_commit_bundle_v1<B>(
        runtime_bundle: Arc<B>,
        identity: MetadataStoreIdentity,
        recovery_intent: CreateRecoveryIntentV1,
        mode: MetadataStoreCreateModeV1,
    ) -> Result<Self, AgentMetadataError>
    where
        B: MetadataRuntimeCommitBundleV1 + 'static,
    {
        let authority_marker = match mode {
            MetadataStoreCreateModeV1::Active => {
                MetadataAuthorityMarker::for_identity(identity, MetadataAuthorityState::Active)
            }
        };
        let runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1> = runtime_bundle;
        Self::create_with_bundle_and_marker(
            runtime_bundle,
            identity,
            recovery_intent,
            authority_marker,
            false,
        )
    }

    /// Reopen an existing provider installation through public SPI v1.
    pub fn reopen_with_runtime_commit_bundle_v1<B>(
        runtime_bundle: Arc<B>,
        expected_identity: MetadataStoreIdentity,
    ) -> Result<Self, AgentMetadataError>
    where
        B: MetadataRuntimeCommitBundleV1 + 'static,
    {
        let runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1> = runtime_bundle;
        Self::reopen_with_runtime_bundle(runtime_bundle, expected_identity, false)
    }

    /// Open a standalone in-memory Holt store.
    ///
    /// This constructor derives a deterministic identity for tests and
    /// single-process use. Distributed bootstrap must use the public provider
    /// facade with one durable runtime commit bundle.
    pub fn open_memory(logical_shard_id: LogicalShardId) -> Result<Self, AgentMetadataError> {
        let identity = MetadataStoreIdentity::standalone_holt_memory(logical_shard_id);
        let runtime_bundle = Arc::new(UntrackedStandaloneRuntimeBundle::new(
            HoltProviderFactory::memory(),
            1,
        ));
        Self::open_memory_with_marker(
            runtime_bundle,
            identity,
            MetadataAuthorityMarker::for_identity(identity, MetadataAuthorityState::Active),
        )
    }

    fn open_memory_with_marker<B>(
        runtime_bundle: Arc<B>,
        identity: MetadataStoreIdentity,
        authority_marker: MetadataAuthorityMarker,
    ) -> Result<Self, AgentMetadataError>
    where
        B: MetadataRuntimeCommitBundleV1 + 'static,
    {
        let runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1> = runtime_bundle;
        Self::create_with_bundle_and_marker(
            runtime_bundle,
            identity,
            CreateRecoveryIntentV1::Fresh,
            authority_marker,
            true,
        )
    }

    /// Create a standalone file-backed Holt store.
    ///
    /// The identity is derived from the logical shard and absolute path.
    /// Distributed bootstrap must use
    /// [`Self::create_with_runtime_commit_bundle_v1`].
    pub fn create_file(
        path: impl AsRef<Path>,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, AgentMetadataError> {
        let path = path.as_ref();
        let identity_path = std::path::absolute(path)
            .map_err(|error| backend("resolve standalone metadata path", error))?;
        let identity =
            MetadataStoreIdentity::standalone_holt_file(logical_shard_id, &identity_path);
        let runtime_bundle = Arc::new(UntrackedStandaloneRuntimeBundle::new(
            HoltProviderFactory::file(path, Arc::new(NoopHoltRuntimeGuard)),
            2,
        ));
        Self::create_with_bundle_and_marker(
            runtime_bundle,
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataAuthorityMarker::for_identity(identity, MetadataAuthorityState::Active),
            true,
        )
    }

    /// Reopen a deterministic standalone file-backed Holt store.
    ///
    /// This method never adopts an arbitrary durable identity. A store created
    /// with an explicit authority must use
    /// [`Self::reopen_with_runtime_commit_bundle_v1`].
    pub fn reopen_file(
        path: impl AsRef<Path>,
        logical_shard_id: LogicalShardId,
    ) -> Result<Self, AgentMetadataError> {
        let path = path.as_ref();
        let identity_path = std::path::absolute(path)
            .map_err(|error| backend("resolve standalone metadata path", error))?;
        let expected_identity =
            MetadataStoreIdentity::standalone_holt_file(logical_shard_id, &identity_path);
        let runtime_bundle = Arc::new(UntrackedStandaloneRuntimeBundle::new(
            HoltProviderFactory::file(path, Arc::new(NoopHoltRuntimeGuard)),
            2,
        ));
        Self::reopen_with_runtime_bundle(runtime_bundle, expected_identity, true)
    }

    fn create_with_bundle_and_marker(
        runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
        identity: MetadataStoreIdentity,
        recovery_intent: CreateRecoveryIntentV1,
        authority_marker: MetadataAuthorityMarker,
        allow_untracked_standalone: bool,
    ) -> Result<Self, AgentMetadataError> {
        validate_store_identity(identity)?;
        if !authority_marker.matches_identity(identity) {
            return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
        }
        validate_authority_marker_for_identity(identity, authority_marker)?;
        let receipt_state = validate_runtime_commit_bundle(
            runtime_bundle.as_ref(),
            identity,
            allow_untracked_standalone,
        )?;
        if let Some((source, planned)) = dirty_receipt_source_and_plan(&receipt_state) {
            if recovery_intent != CreateRecoveryIntentV1::ReconcilePrepared {
                return Err(AgentMetadataError::CommitReceiptRecoveryRequired);
            }
            return Self::recover_dirty_receipt_on_open(
                runtime_bundle,
                identity,
                source,
                planned,
                Some(authority_marker),
            );
        }
        validate_create_receipt_preflight(&receipt_state, recovery_intent)?;
        let schema = canonical_provider_schema_v1();
        let offer = runtime_bundle
            .contract_offer(&schema)
            .map_err(provider_error)?;
        validate_provider_capabilities(offer.capabilities)?;
        let request = ProviderCreateRequestV1::mint(schema, identity, recovery_intent);
        let provider = runtime_bundle.create(&request).map_err(provider_error)?;
        request.ensure_execution_claimed().map_err(provider_error)?;
        validate_provider_offer(provider.as_ref(), offer.capabilities)?;
        if recovery_intent == CreateRecoveryIntentV1::ReconcilePrepared
            && provider
                .get(
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_SCHEMA_KEY,
                )
                .map_err(provider_error)?
                .is_some()
        {
            let store = Self::open_marked(provider, identity, runtime_bundle)?;
            let durable_marker = decode_authority_marker(
                &store
                    .required_system_record(
                        SYSTEM_METADATA_AUTHORITY_KEY,
                        "System(metadata_authority)",
                    )?
                    .value,
            )
            .map_err(|error| corrupt("MetadataAuthorityState", error))?;
            if durable_marker != authority_marker {
                return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
            }
            Ok(store)
        } else {
            Self::initialize_fresh(provider, identity, authority_marker, runtime_bundle)
        }
    }

    fn reopen_with_runtime_bundle(
        runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
        expected_identity: MetadataStoreIdentity,
        allow_untracked_standalone: bool,
    ) -> Result<Self, AgentMetadataError> {
        validate_store_identity(expected_identity)?;
        let receipt_state = validate_runtime_commit_bundle(
            runtime_bundle.as_ref(),
            expected_identity,
            allow_untracked_standalone,
        )?;
        if let Some((source, planned)) = dirty_receipt_source_and_plan(&receipt_state) {
            return Self::recover_dirty_receipt_on_open(
                runtime_bundle,
                expected_identity,
                source,
                planned,
                None,
            );
        }
        validate_reopen_receipt_preflight(&receipt_state)?;
        let schema = canonical_provider_schema_v1();
        let offer = runtime_bundle
            .contract_offer(&schema)
            .map_err(provider_error)?;
        validate_provider_capabilities(offer.capabilities)?;
        let request = ProviderReopenRequestV1::mint(schema, expected_identity);
        let provider = runtime_bundle.reopen(&request).map_err(provider_error)?;
        request.ensure_execution_claimed().map_err(provider_error)?;
        validate_provider_offer(provider.as_ref(), offer.capabilities)?;
        Self::open_marked(provider, expected_identity, runtime_bundle)
    }

    fn recover_dirty_receipt_on_open(
        runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
        expected_identity: MetadataStoreIdentity,
        source: MetadataCommitReceiptDirtySourceV1,
        planned: PlannedMetadataCommitV1,
        expected_authority_marker: Option<MetadataAuthorityMarker>,
    ) -> Result<Self, AgentMetadataError> {
        let installation = runtime_bundle.old_dispatch_exclusion_installation_v1();
        if !installation.is_supported() {
            return Err(AgentMetadataError::CommitReceiptRecoveryRequired);
        }

        let mint_authority = MetadataCommitEngineMintAuthorityV1::new();
        let (command, witness) = mint_pending_recovery_open_v1(
            &mint_authority,
            &planned,
            source,
            canonical_provider_schema_v1(),
            installation.clone(),
        )
        .map_err(|_| AgentMetadataError::CommitReceiptRecoveryRequired)?;
        let opened = runtime_bundle
            .reopen_pending_with_old_dispatch_excluded_v1(command)
            .into_result_for(witness)
            .map_err(|_| AgentMetadataError::CommitReceiptRecoveryRequired)?;
        if opened.planned() != &planned || opened.installation() != &installation {
            return Err(AgentMetadataError::CommitReceiptRecoveryRequired);
        }

        let (provider, recovery_allocation) = opened.into_recovery_parts_v1();
        if validate_provider_contract(provider.as_ref(), expected_identity.logical_shard_id)
            .is_err()
        {
            return Err(AgentMetadataError::CommitReceiptRecoveryRequired);
        }
        let store = Self::new_opened(provider, expected_identity, runtime_bundle);
        if store.validate_opened_marked().is_err() || store.validate_provider_runtime().is_err() {
            return Err(AgentMetadataError::CommitReceiptRecoveryRequired);
        }
        if let Some(expected_marker) = expected_authority_marker {
            let durable_marker = store
                .required_system_record(SYSTEM_METADATA_AUTHORITY_KEY, "System(metadata_authority)")
                .and_then(|record| {
                    decode_authority_marker(&record.value)
                        .map_err(|error| corrupt("MetadataAuthorityState", error))
                });
            if durable_marker.as_ref() != Ok(&expected_marker) {
                return Err(AgentMetadataError::CommitReceiptRecoveryRequired);
            }
        }

        let observation = store
            .begin_commit_resolution_view()
            .and_then(|view| store.observe_planned_from(view.as_ref(), &planned));
        let resolution = match observation {
            Ok(PlannedCommitObservation::Applied {
                purpose_evidence_digest,
            }) => MetadataCommitResolutionV1::applied(
                &mint_authority,
                source,
                planned.exact_next(),
                purpose_evidence_digest,
            ),
            Ok(PlannedCommitObservation::NotApplied {
                purpose_evidence_digest,
            }) if source == MetadataCommitReceiptDirtySourceV1::PoisonedSettled => {
                MetadataCommitResolutionV1::not_applied_settled(
                    &mint_authority,
                    planned.prior(),
                    purpose_evidence_digest,
                )
            }
            Ok(PlannedCommitObservation::NotApplied { .. })
            | Ok(PlannedCommitObservation::Foreign)
            | Err(_) => return Err(AgentMetadataError::CommitReceiptRecoveryRequired),
        };
        let (resolve_command, resolve_witness) =
            MetadataCommitReceiptResolveCommandV1::mint_recovery(
                &mint_authority,
                &planned,
                recovery_allocation,
                resolution,
            )
            .map_err(|_| AgentMetadataError::CommitReceiptRecoveryRequired)?;
        let _terminal_result = store
            .runtime_bundle
            .resolve_pending_commit_v1(resolve_command)
            .into_result_for(resolve_witness);

        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
    }

    fn initialize_fresh(
        provider: Arc<dyn MetadataProvider>,
        identity: MetadataStoreIdentity,
        authority_marker: MetadataAuthorityMarker,
        runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
    ) -> Result<Self, AgentMetadataError> {
        validate_store_identity(identity)?;
        if !authority_marker.matches_identity(identity) {
            return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
        }
        validate_authority_marker_for_identity(identity, authority_marker)?;
        validate_provider_contract(provider.as_ref(), identity.logical_shard_id)?;
        let store = Self::new_opened(provider, identity, runtime_bundle);
        let planning_view = store.begin_commit_resolution_view()?;
        store.require_canonical_absent_from(planning_view.as_ref())?;
        let mut plan = AtomicPlan::default();
        for space in all_ordered_spaces() {
            plan.operations.push(AtomicOp::AssertPrefixEmpty {
                space,
                prefix: Vec::new(),
            });
        }
        for (key, value) in [
            (SYSTEM_SCHEMA_KEY, encode_schema_marker()),
            (SYSTEM_STORE_IDENTITY_KEY, encode_store_identity(identity)),
            (
                SYSTEM_METADATA_AUTHORITY_KEY,
                encode_authority_marker(authority_marker),
            ),
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
                encode_system_digest(recovery_genesis_digest(
                    identity.logical_shard_id,
                    identity.contract_digest,
                )),
            ),
        ] {
            plan.operations.push(AtomicOp::PutIfAbsent {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                key: key.to_vec(),
                value,
            });
        }
        let authority_marker_encoded = encode_authority_marker(authority_marker);
        let genesis = AcknowledgedMetadataFrontier {
            write_sequence: authority_marker.write_sequence,
            commit_version: CommitVersion::new(INITIAL_COMMIT_VERSION)
                .expect("the initial commit version is non-zero"),
            recovery_lsn: 0,
            chain_digest: recovery_genesis_digest(
                identity.logical_shard_id,
                identity.contract_digest,
            ),
        };
        let planned = store.plan_exact_commit(
            MetadataCommitPurposeV1::Genesis {
                authority_marker_digest: digest_authority_marker(&authority_marker_encoded),
            },
            MetadataFrontierPointV1::Absent,
            genesis,
        )?;
        let initialized = store.commit_planned_exact(plan, &planned)?;
        if initialized != AtomicCommitOutcome::Committed {
            return Err(AgentMetadataError::SchemaGate {
                reason: "fresh system records collided".to_owned(),
            });
        }
        store.validate_opened_marked()?;
        Ok(store)
    }

    fn open_marked(
        provider: Arc<dyn MetadataProvider>,
        expected_identity: MetadataStoreIdentity,
        runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
    ) -> Result<Self, AgentMetadataError> {
        validate_provider_contract(provider.as_ref(), expected_identity.logical_shard_id)?;
        let store = Self::new_opened(provider, expected_identity, runtime_bundle);
        store.validate_opened_marked()?;
        store.reconcile_receipt_on_open()?;
        store.validate_provider_runtime()?;
        Ok(store)
    }

    fn new_opened(
        provider: Arc<dyn MetadataProvider>,
        identity: MetadataStoreIdentity,
        runtime_bundle: Arc<dyn MetadataRuntimeCommitBundleV1>,
    ) -> Self {
        let receipt_qualification = runtime_bundle.commit_receipt_qualification_v1();
        let fail_stop = Arc::new(MetadataStoreFailStop::default());
        let provider = Arc::new(FailStopMetadataProvider::new(
            provider,
            Arc::clone(&fail_stop),
        ));
        Self {
            provider,
            fail_stop,
            identity,
            command_gate: Arc::new(RwLock::new(())),
            runtime_bundle,
            receipt_qualification,
            #[cfg(feature = "metadata-read-stats")]
            read_stats_identity: Arc::new(MetadataReadStatsIdentity::default()),
        }
    }

    fn validate_opened_marked(&self) -> Result<(), AgentMetadataError> {
        let schema = self.required_system_record(SYSTEM_SCHEMA_KEY, "System(schema)")?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let durable_identity = decode_store_identity(
            &self
                .required_system_record(SYSTEM_STORE_IDENTITY_KEY, "System(store_identity)")?
                .value,
        )
        .map_err(|error| corrupt("MetadataStoreIdentity", error))?;
        if durable_identity != self.identity {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        }
        let authority = decode_authority_marker(
            &self
                .required_system_record(
                    SYSTEM_METADATA_AUTHORITY_KEY,
                    "System(metadata_authority)",
                )?
                .value,
        )
        .map_err(|error| corrupt("MetadataAuthorityState", error))?;
        if !authority.matches_identity(durable_identity) {
            return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
        }
        decode_system_u64(
            &self
                .required_system_record(SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?
                .value,
            "System(owner_fence)",
        )?;
        decode_system_u64(
            &self
                .required_system_record(
                    SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                    "System(applied_recovery_lsn)",
                )?
                .value,
            "System(applied_recovery_lsn)",
        )?;
        decode_system_digest(
            &self
                .required_system_record(
                    SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
                    "System(recovery_chain_digest)",
                )?
                .value,
            "System(recovery_chain_digest)",
        )?;
        let clock = decode_system_u64(
            &self
                .required_system_record(SYSTEM_COMMIT_CLOCK_KEY, "System(commit_clock)")?
                .value,
            "System(commit_clock)",
        )?;
        CommitVersion::new(clock).map_err(|error| corrupt("System(commit_clock)", error))?;
        decode_system_u64(
            &self
                .required_system_record(
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    "System(lease_clock_high_water)",
                )?
                .value,
            "System(lease_clock_high_water)",
        )?;
        self.verify_recovery_chain_unlocked()?;
        Ok(())
    }

    pub fn current_read_version(&self) -> Result<ReadVersion, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.current_read_version_unlocked()
    }

    fn current_read_version_unlocked(&self) -> Result<ReadVersion, AgentMetadataError> {
        let record =
            self.required_system_record(SYSTEM_COMMIT_CLOCK_KEY, "System(commit_clock)")?;
        let value = decode_system_u64(&record.value, "System(commit_clock)")?;
        ReadVersion::new(value).map_err(|error| corrupt("System(commit_clock)", error))
    }

    /// Return the commit and recovery frontiers from one provider-consistent
    /// System-space read view.
    ///
    /// This is the diagnostic boundary used to prove that a lifecycle sweep
    /// did not race a durable metadata write. It must not be decomposed into
    /// independent point reads because those reads could observe different
    /// logical commits.
    pub fn metadata_frontier(&self) -> Result<(ReadVersion, RecoveryState), AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        let read_view = self
            .provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                prefix: Vec::new(),
            }])
            .map_err(provider_error)?;
        let clock = required_record(
            read_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )?;
        let commit_version = decode_system_u64(&clock.value, "System(commit_clock)")?;
        let read_version = ReadVersion::new(commit_version)
            .map_err(|error| corrupt("System(commit_clock)", error))?;
        Ok((read_version, recovery_state_from(read_view.as_ref())?))
    }

    fn acknowledgement_frontier_from(
        &self,
        reader: &dyn MetadataReadView,
    ) -> Result<AcknowledgedMetadataFrontier, AgentMetadataError> {
        let authority = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_METADATA_AUTHORITY_KEY,
            "System(metadata_authority)",
        )?;
        let marker = decode_authority_marker(&authority.value)
            .map_err(|error| corrupt("MetadataAuthorityState", error))?;
        if !marker.matches_identity(self.identity) {
            return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
        }
        let clock = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )?;
        let recovery = recovery_state_from(reader)?;
        Ok(AcknowledgedMetadataFrontier {
            write_sequence: marker.write_sequence,
            commit_version: CommitVersion::new(decode_system_u64(
                &clock.value,
                "System(commit_clock)",
            )?)
            .map_err(|error| corrupt("System(commit_clock)", error))?,
            recovery_lsn: recovery.applied_recovery_lsn,
            chain_digest: recovery.chain_digest,
        })
    }

    fn begin_commit_resolution_view(
        &self,
    ) -> Result<Box<dyn MetadataReadView>, AgentMetadataError> {
        let scopes = all_ordered_spaces()
            .into_iter()
            .map(|space| ReadScope {
                space,
                prefix: Vec::new(),
            })
            .collect::<Vec<_>>();
        self.provider.begin_read(&scopes).map_err(provider_error)
    }

    fn frontier_point_from(
        &self,
        reader: &dyn MetadataReadView,
    ) -> Result<MetadataFrontierPointV1, AgentMetadataError> {
        let schema = reader
            .get(
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                SYSTEM_SCHEMA_KEY,
            )
            .map_err(provider_error)?;
        if schema.is_none() {
            self.require_canonical_absent_from(reader)?;
            return Ok(MetadataFrontierPointV1::Absent);
        }
        validate_schema_marker(&schema.expect("schema presence was checked").value).map_err(
            |error| AgentMetadataError::SchemaGate {
                reason: error.to_string(),
            },
        )?;
        let identity = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_STORE_IDENTITY_KEY,
            "System(store_identity)",
        )?;
        let durable_identity = decode_store_identity(&identity.value)
            .map_err(|error| corrupt("MetadataStoreIdentity", error))?;
        if durable_identity != self.identity {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        }
        Ok(MetadataFrontierPointV1::Exact(
            self.acknowledgement_frontier_from(reader)?,
        ))
    }

    fn require_canonical_absent_from(
        &self,
        reader: &dyn MetadataReadView,
    ) -> Result<(), AgentMetadataError> {
        for space in all_ordered_spaces() {
            let page = reader
                .scan(&ProviderScan {
                    space,
                    prefix: Vec::new(),
                    start_after: None,
                    delimiter: None,
                    limit: 1,
                })
                .map_err(provider_error)?;
            if !page.items.is_empty() {
                return Err(AgentMetadataError::SchemaGate {
                    reason: "pre-genesis metadata provider is not canonically empty".to_owned(),
                });
            }
        }
        Ok(())
    }

    fn plan_exact_commit(
        &self,
        purpose: MetadataCommitPurposeV1,
        prior: MetadataFrontierPointV1,
        exact_next: AcknowledgedMetadataFrontier,
    ) -> Result<PlannedMetadataCommitV1, AgentMetadataError> {
        PlannedMetadataCommitV1::plan_exact(
            self.identity,
            self.runtime_bundle.frozen_runtime_bundle_digest_v1(),
            purpose,
            prior,
            exact_next,
        )
        .map_err(receipt_error)
    }

    fn ensure_receipt_clean_for(
        &self,
        prior: MetadataFrontierPointV1,
    ) -> Result<(), AgentMetadataError> {
        if self.receipt_qualification == MetadataCommitReceiptQualificationV1::UntrackedStandalone {
            return Ok(());
        }
        let state = match self.runtime_bundle.load_commit_receipt_v1(self.identity) {
            Ok(state) => state,
            Err(error) => {
                self.fail_stop.trip();
                return Err(receipt_error(error));
            }
        };
        match state {
            MetadataCommitReceiptStateV1::Clean {
                store_identity,
                frozen_bundle_digest,
                frontier,
            } if store_identity == self.identity
                && frozen_bundle_digest
                    == self.runtime_bundle.frozen_runtime_bundle_digest_v1()
                && frontier == prior =>
            {
                Ok(())
            }
            MetadataCommitReceiptStateV1::Pending(_)
            | MetadataCommitReceiptStateV1::PoisonedSettled(_)
            | MetadataCommitReceiptStateV1::PoisonedUnsettled(_) => {
                self.fail_stop.trip();
                Err(AgentMetadataError::CommitOutcomeUnknown)
            }
            _ => {
                self.fail_stop.trip();
                Err(AgentMetadataError::ProviderAuthorityMismatch {
                    operation: "validate metadata commit receipt",
                    message:
                        "durable receipt does not exactly match the provider planning frontier"
                            .to_owned(),
                })
            }
        }
    }

    fn poison_receipt_for_resolution(
        &self,
        origin: MetadataCommitLiveResolutionOriginV1,
        reason: MetadataCommitReceiptPoisonReasonV1,
    ) -> Result<MetadataCommitLiveResolutionOriginV1, AgentMetadataError> {
        let authority = MetadataCommitEngineMintAuthorityV1::new();
        let (command, witness) =
            MetadataCommitReceiptPoisonCommandV1::mint(&authority, origin, reason)
                .map_err(receipt_error)?;
        let result = self
            .runtime_bundle
            .poison_commit_receipt_v1(command)
            .into_live_resolution_origin_for(witness);
        if result.is_err() {
            self.fail_stop.trip();
            return Err(AgentMetadataError::CommitOutcomeUnknown);
        }
        result.map_err(receipt_error)
    }

    fn resolve_receipt_after_provider_effect(
        &self,
        origin: MetadataCommitLiveResolutionOriginV1,
        resolution: MetadataCommitResolutionV1,
    ) -> Result<(), AgentMetadataError> {
        let authority = MetadataCommitEngineMintAuthorityV1::new();
        let (command, witness) =
            MetadataCommitReceiptResolveCommandV1::mint_live(&authority, origin, resolution)
                .map_err(receipt_error)?;
        if self
            .runtime_bundle
            .resolve_pending_commit_v1(command)
            .into_result_for(witness)
            .is_err()
        {
            self.fail_stop.trip();
            return Err(AgentMetadataError::CommitOutcomeUnknown);
        }
        Ok(())
    }

    fn poison_uncertain_commit_and_fail_stop(
        &self,
        origin: MetadataCommitLiveResolutionOriginV1,
        reason: MetadataCommitReceiptPoisonReasonV1,
    ) -> Result<(), AgentMetadataError> {
        let authority = MetadataCommitEngineMintAuthorityV1::new();
        let (command, witness) =
            MetadataCommitReceiptPoisonCommandV1::mint(&authority, origin, reason)
                .map_err(receipt_error)?;
        let poisoned = self
            .runtime_bundle
            .poison_commit_receipt_v1(command)
            .into_live_resolution_origin_for(witness)
            .is_ok();
        self.fail_stop.trip();
        if poisoned {
            Ok(())
        } else {
            Err(AgentMetadataError::CommitOutcomeUnknown)
        }
    }

    fn close_uncertain_live_origin(
        &self,
        origin: MetadataCommitLiveResolutionOriginV1,
        poison_reason: Option<MetadataCommitReceiptPoisonReasonV1>,
    ) -> Result<(), AgentMetadataError> {
        if origin.source() == MetadataCommitReceiptDirtySourceV1::Pending {
            if let Some(reason) = poison_reason {
                return self.poison_uncertain_commit_and_fail_stop(origin, reason);
            }
        }
        drop(origin);
        self.fail_stop.trip();
        Ok(())
    }

    fn commit_planned_exact(
        &self,
        plan: AtomicPlan,
        planned: &PlannedMetadataCommitV1,
    ) -> Result<AtomicCommitOutcome, AgentMetadataError> {
        planned
            .validate_binding(
                self.identity,
                self.runtime_bundle.frozen_runtime_bundle_digest_v1(),
            )
            .map_err(receipt_error)?;
        self.ensure_receipt_clean_for(planned.prior())?;
        let mut pending_fail_stop = PendingCommitFailStopGuard::arm(self.fail_stop.as_ref());
        let mint_authority = MetadataCommitEngineMintAuthorityV1::new();
        let (persist_command, persist_witness) =
            MetadataCommitReceiptPersistCommandV1::mint(&mint_authority, planned);
        let live_origin = match self
            .runtime_bundle
            .persist_pending_commit_v1(persist_command)
            .into_live_resolution_origin_for(persist_witness)
        {
            Ok(origin) => origin,
            Err(error) => {
                return match error {
                    MetadataCommitReceiptPersistErrorV1::UnavailableBeforeEffect => {
                        pending_fail_stop.disarm();
                        Err(AgentMetadataError::ProviderUnavailable {
                            operation: "persist metadata commit receipt",
                            message: error.to_string(),
                        })
                    }
                    MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect => {
                        self.fail_stop.trip();
                        Err(AgentMetadataError::ProviderAuthorityMismatch {
                            operation: "persist metadata commit receipt",
                            message: error.to_string(),
                        })
                    }
                    MetadataCommitReceiptPersistErrorV1::RecoveryRequired => {
                        self.fail_stop.trip();
                        Err(AgentMetadataError::CommitReceiptRecoveryRequired)
                    }
                };
            }
        };

        let transaction = match self.provider.begin_write() {
            Ok(transaction) => transaction,
            Err(error) => {
                let resolution = self.resolve_planned_from_provider(
                    planned,
                    live_origin,
                    true,
                    true,
                    Some(MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome),
                )?;
                if let Some(outcome) = resolution {
                    pending_fail_stop.disarm();
                    return match outcome {
                        AtomicCommitOutcome::Committed => Ok(AtomicCommitOutcome::Committed),
                        AtomicCommitOutcome::Conflict => Err(provider_error(error)),
                    };
                }
                return Err(AgentMetadataError::CommitOutcomeUnknown);
            }
        };
        match transaction.commit(plan) {
            Ok(AtomicCommitOutcome::Committed) => {
                let resolution = self.resolve_planned_from_provider(
                    planned,
                    live_origin,
                    false,
                    true,
                    Some(MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome),
                )?;
                if resolution == Some(AtomicCommitOutcome::Committed) {
                    pending_fail_stop.disarm();
                    Ok(AtomicCommitOutcome::Committed)
                } else {
                    Err(AgentMetadataError::CommitOutcomeUnknown)
                }
            }
            Ok(AtomicCommitOutcome::Conflict) => {
                let resolution = self.resolve_planned_from_provider(
                    planned,
                    live_origin,
                    true,
                    true,
                    Some(MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome),
                )?;
                if let Some(outcome) = resolution {
                    pending_fail_stop.disarm();
                    Ok(outcome)
                } else {
                    Err(AgentMetadataError::CommitOutcomeUnknown)
                }
            }
            Err(error) if error.kind() == ProviderErrorKind::UnknownCommitSettled => {
                let live_origin = self.poison_receipt_for_resolution(
                    live_origin,
                    MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
                )?;
                let resolution = self.resolve_planned_from_provider(
                    planned,
                    live_origin,
                    true,
                    true,
                    Some(MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome),
                );
                self.fail_stop.trip();
                resolution?.ok_or(AgentMetadataError::CommitOutcomeUnknown)
            }
            Err(error) if error.kind() == ProviderErrorKind::UnknownCommitUnsettled => {
                let live_origin = self.poison_receipt_for_resolution(
                    live_origin,
                    MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
                )?;
                let resolution = self.resolve_planned_from_provider(
                    planned,
                    live_origin,
                    false,
                    false,
                    Some(MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome),
                );
                self.fail_stop.trip();
                resolution?.ok_or(AgentMetadataError::CommitOutcomeUnknown)
            }
            Err(error) => {
                let resolution = self.resolve_planned_from_provider(
                    planned,
                    live_origin,
                    true,
                    true,
                    Some(MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome),
                )?;
                match resolution {
                    Some(AtomicCommitOutcome::Committed) => {
                        pending_fail_stop.disarm();
                        Ok(AtomicCommitOutcome::Committed)
                    }
                    Some(AtomicCommitOutcome::Conflict) => {
                        pending_fail_stop.disarm();
                        Err(provider_error(error))
                    }
                    None => Err(AgentMetadataError::CommitOutcomeUnknown),
                }
            }
        }
    }

    fn resolve_planned_from_provider(
        &self,
        planned: &PlannedMetadataCommitV1,
        origin: MetadataCommitLiveResolutionOriginV1,
        prior_is_terminal: bool,
        retry_prior_for_causal_settlement: bool,
        poison_reason_on_uncertain: Option<MetadataCommitReceiptPoisonReasonV1>,
    ) -> Result<Option<AtomicCommitOutcome>, AgentMetadataError> {
        let first = match self
            .begin_commit_resolution_view()
            .and_then(|view| self.observe_planned_from(view.as_ref(), planned))
        {
            Ok(observation) => observation,
            Err(_) => {
                self.close_uncertain_live_origin(origin, poison_reason_on_uncertain)?;
                return Ok(None);
            }
        };
        let observation = if retry_prior_for_causal_settlement
            && matches!(first, PlannedCommitObservation::NotApplied { .. })
        {
            let second = match self
                .begin_commit_resolution_view()
                .and_then(|view| self.observe_planned_from(view.as_ref(), planned))
            {
                Ok(observation) => observation,
                Err(_) => {
                    self.close_uncertain_live_origin(origin, poison_reason_on_uncertain)?;
                    return Ok(None);
                }
            };
            second
        } else {
            first
        };
        match observation {
            PlannedCommitObservation::Applied {
                purpose_evidence_digest,
            } => {
                let authority = MetadataCommitEngineMintAuthorityV1::new();
                let source = origin.source();
                self.resolve_receipt_after_provider_effect(
                    origin,
                    MetadataCommitResolutionV1::applied(
                        &authority,
                        source,
                        planned.exact_next(),
                        purpose_evidence_digest,
                    ),
                )?;
                Ok(Some(AtomicCommitOutcome::Committed))
            }
            PlannedCommitObservation::NotApplied {
                purpose_evidence_digest,
            } if prior_is_terminal => {
                let origin = match origin.source() {
                    MetadataCommitReceiptDirtySourceV1::Pending => self
                        .poison_receipt_for_resolution(
                            origin,
                            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
                        )?,
                    MetadataCommitReceiptDirtySourceV1::PoisonedSettled => origin,
                    MetadataCommitReceiptDirtySourceV1::PoisonedUnsettled => {
                        drop(origin);
                        self.fail_stop.trip();
                        return Ok(None);
                    }
                };
                let authority = MetadataCommitEngineMintAuthorityV1::new();
                self.resolve_receipt_after_provider_effect(
                    origin,
                    MetadataCommitResolutionV1::not_applied_settled(
                        &authority,
                        planned.prior(),
                        purpose_evidence_digest,
                    ),
                )?;
                Ok(Some(AtomicCommitOutcome::Conflict))
            }
            PlannedCommitObservation::NotApplied { .. } => {
                drop(origin);
                self.fail_stop.trip();
                Ok(None)
            }
            PlannedCommitObservation::Foreign => {
                self.close_uncertain_live_origin(origin, poison_reason_on_uncertain)?;
                Ok(None)
            }
        }
    }

    fn reconcile_receipt_on_open(&self) -> Result<(), AgentMetadataError> {
        let state = self
            .runtime_bundle
            .load_commit_receipt_v1(self.identity)
            .map_err(receipt_error)?;
        match state {
            MetadataCommitReceiptStateV1::UntrackedStandalone
                if self.receipt_qualification
                    == MetadataCommitReceiptQualificationV1::UntrackedStandalone =>
            {
                Ok(())
            }
            MetadataCommitReceiptStateV1::Clean {
                store_identity,
                frozen_bundle_digest,
                frontier,
            } => {
                if store_identity != self.identity
                    || frozen_bundle_digest != self.runtime_bundle.frozen_runtime_bundle_digest_v1()
                {
                    return Err(AgentMetadataError::ProviderAuthorityMismatch {
                        operation: "validate metadata commit receipt binding",
                        message: "receipt is bound to another store or runtime bundle".to_owned(),
                    });
                }
                let view = self.begin_commit_resolution_view()?;
                if self.frontier_point_from(view.as_ref())? != frontier {
                    return Err(AgentMetadataError::ProviderAuthorityMismatch {
                        operation: "validate metadata commit receipt frontier",
                        message: "provider frontier differs from the exact clean receipt"
                            .to_owned(),
                    });
                }
                Ok(())
            }
            MetadataCommitReceiptStateV1::Pending(_)
            | MetadataCommitReceiptStateV1::PoisonedSettled(_)
            | MetadataCommitReceiptStateV1::PoisonedUnsettled(_) => {
                Err(AgentMetadataError::CommitReceiptRecoveryRequired)
            }
            MetadataCommitReceiptStateV1::UntrackedStandalone => {
                Err(AgentMetadataError::ProviderAuthorityMismatch {
                    operation: "validate metadata commit receipt qualification",
                    message: "distributed runtime cannot use an untracked receipt".to_owned(),
                })
            }
        }
    }

    fn observe_planned_from(
        &self,
        reader: &dyn MetadataReadView,
        planned: &PlannedMetadataCommitV1,
    ) -> Result<PlannedCommitObservation, AgentMetadataError> {
        let current = self.frontier_point_from(reader)?;
        if current == MetadataFrontierPointV1::Exact(planned.exact_next()) {
            let evidence = self.verify_applied_purpose_from(reader, planned)?;
            return Ok(PlannedCommitObservation::Applied {
                purpose_evidence_digest: purpose_evidence_digest(planned, true, &evidence),
            });
        }
        if current == planned.prior() {
            let evidence = self.verify_not_applied_purpose_from(reader, planned)?;
            return Ok(PlannedCommitObservation::NotApplied {
                purpose_evidence_digest: purpose_evidence_digest(planned, false, &evidence),
            });
        }
        Ok(PlannedCommitObservation::Foreign)
    }

    fn verify_applied_purpose_from(
        &self,
        reader: &dyn MetadataReadView,
        planned: &PlannedMetadataCommitV1,
    ) -> Result<Vec<u8>, AgentMetadataError> {
        match planned.purpose() {
            MetadataCommitPurposeV1::Genesis {
                authority_marker_digest,
            } => {
                self.verify_canonical_genesis_from(
                    reader,
                    planned.exact_next(),
                    *authority_marker_digest,
                )?;
                Ok(b"canonical-genesis-v1".to_vec())
            }
            MetadataCommitPurposeV1::AdvanceOwnerEpoch { expected, next } => {
                let row = self.required_recovery_row_from(reader, planned.exact_next())?;
                if row.mutation
                    != (RecoveryMutationV1::AdvanceOwnerEpoch {
                        expected: *expected,
                        next: *next,
                    })
                    || row.result
                        != (RecoveryResultV1::OwnerEpoch {
                            applied_owner_epoch: *next,
                        })
                {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "owner-epoch recovery row does not match the planned commit",
                    ));
                }
                let owner = required_record(
                    reader,
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_OWNER_FENCE_KEY,
                    "System(owner_fence)",
                )?;
                if decode_system_u64(&owner.value, "System(owner_fence)")? != next.get() {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "owner fence does not match the planned next epoch",
                    ));
                }
                row.encode()
                    .map_err(|error| corrupt("RecoveryOutbox", error))
            }
            MetadataCommitPurposeV1::ObserveLeaseClock {
                root_id,
                placement_generation,
                owner_epoch,
                observed_ms,
            } => {
                let row = self.required_recovery_row_from(reader, planned.exact_next())?;
                if row.mutation
                    != (RecoveryMutationV1::ObserveLeaseClock {
                        root_id: *root_id,
                        placement_generation: *placement_generation,
                        owner_epoch: *owner_epoch,
                        observed_ms: *observed_ms,
                    })
                    || row.result
                        != (RecoveryResultV1::LeaseClock {
                            effective_high_water_ms: *observed_ms,
                        })
                {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "lease-clock recovery row does not match the planned commit",
                    ));
                }
                let clock = required_record(
                    reader,
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    "System(lease_clock_high_water)",
                )?;
                if decode_system_u64(&clock.value, "System(lease_clock_high_water)")?
                    != *observed_ms
                {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "lease clock does not match the planned observation",
                    ));
                }
                row.encode()
                    .map_err(|error| corrupt("RecoveryOutbox", error))
            }
            MetadataCommitPurposeV1::MetadataCommand {
                class,
                root_id,
                request_id,
                command_digest,
                lease_deadline_ms,
            } => {
                let dedupe_key = command_dedupe_key(*root_id, *request_id);
                let dedupe = required_record(
                    reader,
                    crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
                    &dedupe_key,
                    "CommandDedupe",
                )?;
                let dedupe_record = CommandDedupeRecord::decode(&dedupe.value)
                    .map_err(|error| corrupt("CommandDedupe", error))?;
                if dedupe_record.command_digest != *command_digest
                    || dedupe_record.commit_version != planned.exact_next().commit_version
                    || dedupe_record.recovery_lsn != planned.exact_next().recovery_lsn
                {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "command dedupe does not match the planned commit",
                    ));
                }
                let row = self.required_recovery_row_from(reader, planned.exact_next())?;
                let RecoveryMutationV1::MetadataCommand {
                    command,
                    lease_deadline_ms: durable_deadline,
                } = &row.mutation
                else {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "recovery row is not a metadata command",
                    ));
                };
                let durable_class =
                    if matches!(command.root_fence_action, RootFenceAction::RequireActive) {
                        MetadataCommandCommitClassV1::Domain
                    } else {
                        MetadataCommandCommitClassV1::RootFence
                    };
                let RecoveryResultV1::MetadataCommand {
                    commit_version,
                    deterministic_result,
                } = &row.result
                else {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "recovery result is not a metadata command",
                    ));
                };
                if durable_class != *class
                    || command.root_id != *root_id
                    || command.request_id != *request_id
                    || command.command_digest != *command_digest
                    || durable_deadline != lease_deadline_ms
                    || *commit_version != planned.exact_next().commit_version
                    || *deterministic_result != dedupe_record.deterministic_result
                {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "metadata command recovery evidence does not match the plan",
                    ));
                }
                let mut evidence = dedupe.value;
                evidence.extend_from_slice(
                    &row.encode()
                        .map_err(|error| corrupt("RecoveryOutbox", error))?,
                );
                Ok(evidence)
            }
            MetadataCommitPurposeV1::Authority {
                next_marker_digest, ..
            } => {
                let authority = required_record(
                    reader,
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_METADATA_AUTHORITY_KEY,
                    "System(metadata_authority)",
                )?;
                if digest_authority_marker(&authority.value) != *next_marker_digest {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "authority marker does not match the planned next state",
                    ));
                }
                Ok(authority.value)
            }
        }
    }

    fn verify_not_applied_purpose_from(
        &self,
        reader: &dyn MetadataReadView,
        planned: &PlannedMetadataCommitV1,
    ) -> Result<Vec<u8>, AgentMetadataError> {
        match planned.purpose() {
            MetadataCommitPurposeV1::Genesis { .. } => {
                self.require_canonical_absent_from(reader)?;
                Ok(b"canonical-absence-v1".to_vec())
            }
            MetadataCommitPurposeV1::AdvanceOwnerEpoch { expected, .. } => {
                self.require_next_recovery_absent_from(reader, planned.exact_next())?;
                let owner = required_record(
                    reader,
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_OWNER_FENCE_KEY,
                    "System(owner_fence)",
                )?;
                let expected = expected.map(OwnerEpoch::get).unwrap_or(0);
                if decode_system_u64(&owner.value, "System(owner_fence)")? != expected {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "owner fence does not match the planned predecessor",
                    ));
                }
                Ok(owner.value)
            }
            MetadataCommitPurposeV1::ObserveLeaseClock { observed_ms, .. } => {
                self.require_next_recovery_absent_from(reader, planned.exact_next())?;
                let clock = required_record(
                    reader,
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
                    "System(lease_clock_high_water)",
                )?;
                let current = decode_system_u64(&clock.value, "System(lease_clock_high_water)")?;
                if current >= *observed_ms {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "lease clock no longer matches the planned predecessor",
                    ));
                }
                Ok(clock.value)
            }
            MetadataCommitPurposeV1::MetadataCommand {
                root_id,
                request_id,
                ..
            } => {
                self.require_next_recovery_absent_from(reader, planned.exact_next())?;
                if reader
                    .get(
                        crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
                        &command_dedupe_key(*root_id, *request_id),
                    )
                    .map_err(provider_error)?
                    .is_some()
                {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "command dedupe exists at the planned predecessor frontier",
                    ));
                }
                Ok(b"dedupe-and-recovery-absent-v1".to_vec())
            }
            MetadataCommitPurposeV1::Authority {
                prior_marker_digest,
                ..
            } => {
                let authority = required_record(
                    reader,
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
                    SYSTEM_METADATA_AUTHORITY_KEY,
                    "System(metadata_authority)",
                )?;
                if digest_authority_marker(&authority.value) != *prior_marker_digest {
                    return Err(corrupt(
                        "metadata commit purpose evidence",
                        "authority marker does not match the planned predecessor",
                    ));
                }
                Ok(authority.value)
            }
        }
    }

    fn required_recovery_row_from(
        &self,
        reader: &dyn MetadataReadView,
        next: AcknowledgedMetadataFrontier,
    ) -> Result<RecoveryOutboxRecord, AgentMetadataError> {
        let header = required_record(
            reader,
            crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
            &recovery_outbox_key(next.recovery_lsn),
            "RecoveryOutbox",
        )?;
        let row = self.read_recovery_record_from(reader, next.recovery_lsn, &header.value)?;
        if row.recovery_lsn != next.recovery_lsn || row.chain_digest != next.chain_digest {
            return Err(corrupt(
                "metadata commit purpose evidence",
                "recovery row does not match the planned next frontier",
            ));
        }
        Ok(row)
    }

    fn require_next_recovery_absent_from(
        &self,
        reader: &dyn MetadataReadView,
        next: AcknowledgedMetadataFrontier,
    ) -> Result<(), AgentMetadataError> {
        if reader
            .get(
                crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                &recovery_outbox_key(next.recovery_lsn),
            )
            .map_err(provider_error)?
            .is_some()
        {
            return Err(corrupt(
                "metadata commit purpose evidence",
                "planned next recovery row exists at the predecessor frontier",
            ));
        }
        Ok(())
    }

    fn verify_canonical_genesis_from(
        &self,
        reader: &dyn MetadataReadView,
        genesis: AcknowledgedMetadataFrontier,
        authority_marker_digest: [u8; 32],
    ) -> Result<(), AgentMetadataError> {
        let page = reader
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                prefix: Vec::new(),
                start_after: None,
                delimiter: None,
                limit: 0,
            })
            .map_err(provider_error)?;
        let rows = page
            .items
            .into_iter()
            .map(|item| match item {
                ProviderScanItem::Key { key, value } => Ok((key, value)),
                ProviderScanItem::CommonPrefix(_) => Err(corrupt(
                    "canonical metadata genesis",
                    "undelimited System scan returned a common prefix",
                )),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if rows.len() != 8 {
            return Err(corrupt(
                "canonical metadata genesis",
                "System does not contain exactly the eight canonical genesis rows",
            ));
        }
        let required = |key: &[u8]| {
            rows.get(key)
                .ok_or_else(|| corrupt("canonical metadata genesis", "required row is missing"))
        };
        validate_schema_marker(required(SYSTEM_SCHEMA_KEY)?).map_err(|error| {
            AgentMetadataError::SchemaGate {
                reason: error.to_string(),
            }
        })?;
        if decode_store_identity(required(SYSTEM_STORE_IDENTITY_KEY)?)
            .map_err(|error| corrupt("MetadataStoreIdentity", error))?
            != self.identity
            || digest_authority_marker(required(SYSTEM_METADATA_AUTHORITY_KEY)?)
                != authority_marker_digest
            || decode_system_u64(required(SYSTEM_OWNER_FENCE_KEY)?, "System(owner_fence)")? != 0
            || decode_system_u64(required(SYSTEM_COMMIT_CLOCK_KEY)?, "System(commit_clock)")?
                != INITIAL_COMMIT_VERSION
            || decode_system_u64(
                required(SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY)?,
                "System(lease_clock_high_water)",
            )? != 0
            || decode_system_u64(
                required(SYSTEM_APPLIED_RECOVERY_LSN_KEY)?,
                "System(applied_recovery_lsn)",
            )? != 0
            || decode_system_digest(
                required(SYSTEM_RECOVERY_CHAIN_DIGEST_KEY)?,
                "System(recovery_chain_digest)",
            )? != recovery_genesis_digest(
                self.identity.logical_shard_id,
                self.identity.contract_digest,
            )
            || genesis.write_sequence != 0
            || genesis.commit_version.get() != INITIAL_COMMIT_VERSION
            || genesis.recovery_lsn != 0
        {
            return Err(corrupt(
                "canonical metadata genesis",
                "one or more canonical genesis values differ",
            ));
        }
        for space in all_ordered_spaces()
            .into_iter()
            .filter(|space| *space != crate::workspace::provider_catalog::SYSTEM_SPACE)
        {
            let page = reader
                .scan(&ProviderScan {
                    space,
                    prefix: Vec::new(),
                    start_after: None,
                    delimiter: None,
                    limit: 1,
                })
                .map_err(provider_error)?;
            if !page.items.is_empty() {
                return Err(corrupt(
                    "canonical metadata genesis",
                    "a user, root-fence, dedupe, history, event, or recovery row exists",
                ));
            }
        }
        Ok(())
    }

    /// Return the logical shard identity sealed into this store.
    pub fn logical_shard_id(&self) -> LogicalShardId {
        self.identity.logical_shard_id
    }

    /// Return the exact immutable identity admitted when this store was opened.
    pub fn metadata_store_identity(&self) -> MetadataStoreIdentity {
        self.identity
    }

    /// Open one provider-native, cross-space consistent view for metadata
    /// diagnostics. Callers must honor the provider's declared view lifetime
    /// and must not replace pagination with independently opened views.
    pub(super) fn begin_diagnostic_read(
        &self,
        scopes: &[ReadScope],
    ) -> Result<MetadataDiagnosticReadView<'_>, ProviderError> {
        let command_guard = self.command_gate.read().map_err(|_| {
            ProviderError::backend(
                ProviderOperationV1::BeginRead,
                "metadata command gate is poisoned",
            )
        })?;
        let delegate = self.provider.begin_read(scopes)?;
        Ok(MetadataDiagnosticReadView {
            delegate,
            _command_guard: command_guard,
        })
    }

    #[cfg(test)]
    pub(super) fn delete_diagnostic_row_for_test(
        &self,
        space: OrderedSpaceId,
        key: Vec<u8>,
    ) -> Result<(), AgentMetadataError> {
        let transaction = self.provider.begin_write().map_err(provider_error)?;
        let outcome = transaction
            .commit(AtomicPlan {
                operations: vec![AtomicOp::Delete { space, key }],
            })
            .map_err(provider_error)?;
        if outcome == AtomicCommitOutcome::Committed {
            Ok(())
        } else {
            Err(AgentMetadataError::WriteConflict)
        }
    }

    pub(super) fn provider_capabilities(&self) -> super::provider::ProviderCapabilities {
        self.provider.capabilities()
    }

    /// Revalidate the process-local resources backing this metadata provider.
    ///
    /// Owner bootstrap and lease renewal use this storage-neutral cut point in
    /// addition to their external lifecycle receipt. It performs no logical
    /// metadata mutation and does not change provider ownership.
    pub fn validate_provider_runtime(&self) -> Result<(), AgentMetadataError> {
        self.provider.validate_runtime().map_err(provider_error)
    }

    /// Return the durable authority state. Reads remain available in every
    /// state, while ordinary writes require `Active` in their atomic plan.
    pub fn metadata_authority_state(&self) -> Result<MetadataAuthorityState, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.metadata_authority_state_unlocked()
    }

    fn metadata_authority_state_unlocked(
        &self,
    ) -> Result<MetadataAuthorityState, AgentMetadataError> {
        let marker = self.required_authority_marker()?;
        Ok(marker.state)
    }

    /// Atomically stop admitting source writes and return the exact durable
    /// recovery receipt installed with that barrier.
    pub fn quiesce_metadata_authority(
        &self,
        migration_id: OperationId,
        owner_epoch: OwnerEpoch,
    ) -> Result<SourceQuiesceReceipt, AgentMetadataError> {
        if migration_id.as_bytes().iter().all(|byte| *byte == 0) {
            return Err(migration_admission("migration id must not be all-zero"));
        }
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| backend("lock command gate", error))?;
        let planning_view = self.begin_commit_resolution_view()?;
        let schema = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let identity = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_STORE_IDENTITY_KEY,
            "System(store_identity)",
        )?;
        let durable_identity = decode_store_identity(&identity.value)
            .map_err(|error| corrupt("MetadataStoreIdentity", error))?;
        if durable_identity != self.identity {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        }
        let authority = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_METADATA_AUTHORITY_KEY,
            "System(metadata_authority)",
        )?;
        let marker = decode_authority_marker(&authority.value)
            .map_err(|error| corrupt("MetadataAuthorityState", error))?;
        if !marker.matches_identity(durable_identity) {
            return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
        }
        if marker.state == MetadataAuthorityState::Quiescing {
            let MetadataAuthorityEvidence::SourceQuiesceReceipt(receipt) = marker.evidence else {
                return Err(migration_admission(
                    "quiescing source is missing its durable receipt",
                ));
            };
            validate_source_receipt_request(self.identity, migration_id, owner_epoch, receipt)?;
            self.ensure_receipt_clean_for(self.frontier_point_from(planning_view.as_ref())?)?;
            return Ok(receipt);
        }
        if marker.state != MetadataAuthorityState::Active {
            return Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::Active,
                actual: marker.state,
            });
        }
        let owner = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_OWNER_FENCE_KEY,
            "System(owner_fence)",
        )?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let recovery_lsn = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            "System(applied_recovery_lsn)",
        )?;
        let chain_digest = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
            "System(recovery_chain_digest)",
        )?;
        let commit_clock = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )?;
        let receipt = SourceQuiesceReceipt {
            logical_shard_id: self.identity.logical_shard_id,
            migration_id,
            source_authority_id: self.identity.authority_id,
            source_authority_generation: self.identity.authority_generation,
            owner_epoch,
            frontier: MetadataRecoveryFrontier {
                recovery_lsn: decode_system_u64(
                    &recovery_lsn.value,
                    "System(applied_recovery_lsn)",
                )?,
                chain_digest: decode_system_digest(
                    &chain_digest.value,
                    "System(recovery_chain_digest)",
                )?,
                commit_version: CommitVersion::new(decode_system_u64(
                    &commit_clock.value,
                    "System(commit_clock)",
                )?)
                .map_err(|error| corrupt("System(commit_clock)", error))?,
                state_digest: logical_state_digest(planning_view.as_ref())?,
            },
            contract_digest: self.identity.contract_digest,
        };
        let next_marker = MetadataAuthorityMarker {
            state: MetadataAuthorityState::Quiescing,
            evidence: MetadataAuthorityEvidence::SourceQuiesceReceipt(receipt),
            ..marker
                .advance_write_sequence()
                .ok_or(AgentMetadataError::VersionOverflow)?
        };
        let prior = self.frontier_point_from(planning_view.as_ref())?;
        let MetadataFrontierPointV1::Exact(prior_frontier) = prior else {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        };
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: prior_frontier
                .write_sequence
                .checked_add(1)
                .ok_or(AgentMetadataError::VersionOverflow)?,
            ..prior_frontier
        };
        let planned = self.plan_exact_commit(
            MetadataCommitPurposeV1::Authority {
                action: MetadataAuthorityCommitActionV1::Quiesce {
                    migration_id,
                    owner_epoch,
                },
                prior_marker_digest: digest_authority_marker(&authority.value),
                next_marker_digest: digest_authority_marker(&encode_authority_marker(next_marker)),
            },
            prior,
            exact_next,
        )?;
        let plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_SCHEMA_KEY.to_vec(),
                    witness: schema.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_STORE_IDENTITY_KEY.to_vec(),
                    witness: identity.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_OWNER_FENCE_KEY.to_vec(),
                    witness: owner.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_APPLIED_RECOVERY_LSN_KEY.to_vec(),
                    witness: recovery_lsn.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_RECOVERY_CHAIN_DIGEST_KEY.to_vec(),
                    witness: chain_digest.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_COMMIT_CLOCK_KEY.to_vec(),
                    witness: commit_clock.witness,
                },
                AtomicOp::CompareAndPut {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_METADATA_AUTHORITY_KEY.to_vec(),
                    witness: authority.witness,
                    value: encode_authority_marker(next_marker),
                },
            ],
        };
        match self.commit_planned_exact(plan, &planned)? {
            AtomicCommitOutcome::Committed => Ok(receipt),
            AtomicCommitOutcome::Conflict => Err(AgentMetadataError::WriteConflict),
        }
    }

    /// Permanently fence the exact quiesced source after cutover.
    pub fn fence_quiesced_metadata_authority(
        &self,
        receipt: &SourceQuiesceReceipt,
    ) -> Result<(), AgentMetadataError> {
        self.transition_with_exact_evidence(
            MetadataAuthorityState::Quiescing,
            MetadataAuthorityState::Fenced,
            MetadataAuthorityEvidence::SourceQuiesceReceipt(*receipt),
        )
    }

    /// Activate a target only with the deterministic control token and only
    /// when its exact local logical frontier matches that token.
    #[cfg(test)]
    pub(crate) fn activate_migration_target(
        &self,
        token: &TargetActivationToken,
    ) -> Result<(), AgentMetadataError> {
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| backend("lock command gate", error))?;
        let planning_view = self.begin_commit_resolution_view()?;
        let schema = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let (identity, authority, marker) = self.required_authority_from(planning_view.as_ref())?;
        let expected_evidence = MetadataAuthorityEvidence::TargetActivationToken(*token);
        if marker.state == MetadataAuthorityState::Active {
            if marker.evidence == expected_evidence {
                self.ensure_receipt_clean_for(self.frontier_point_from(planning_view.as_ref())?)?;
                return Ok(());
            }
            return Err(migration_admission(
                "active target carries a different activation token",
            ));
        }
        if marker.state != MetadataAuthorityState::MigrationTarget {
            return Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::MigrationTarget,
                actual: marker.state,
            });
        }
        let MetadataAuthorityEvidence::MigrationTargetBinding(binding) = marker.evidence else {
            return Err(migration_admission(
                "migration target is missing its immutable migration binding",
            ));
        };
        validate_target_token_identity(self.identity, binding, token)?;
        let recovery_lsn = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            "System(applied_recovery_lsn)",
        )?;
        let chain_digest = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
            "System(recovery_chain_digest)",
        )?;
        let commit_clock = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )?;
        let local_frontier = MetadataRecoveryFrontier {
            recovery_lsn: decode_system_u64(&recovery_lsn.value, "System(applied_recovery_lsn)")?,
            chain_digest: decode_system_digest(
                &chain_digest.value,
                "System(recovery_chain_digest)",
            )?,
            commit_version: CommitVersion::new(decode_system_u64(
                &commit_clock.value,
                "System(commit_clock)",
            )?)
            .map_err(|error| corrupt("System(commit_clock)", error))?,
            state_digest: logical_state_digest(planning_view.as_ref())?,
        };
        if local_frontier != token.frontier {
            return Err(migration_admission(
                "migration target logical frontier does not match the activation token",
            ));
        }
        let next_marker = MetadataAuthorityMarker {
            state: MetadataAuthorityState::Active,
            evidence: expected_evidence,
            ..marker
                .advance_write_sequence()
                .ok_or(AgentMetadataError::VersionOverflow)?
        };
        let prior = self.frontier_point_from(planning_view.as_ref())?;
        let MetadataFrontierPointV1::Exact(prior_frontier) = prior else {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        };
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: prior_frontier
                .write_sequence
                .checked_add(1)
                .ok_or(AgentMetadataError::VersionOverflow)?,
            ..prior_frontier
        };
        let planned = self.plan_exact_commit(
            MetadataCommitPurposeV1::Authority {
                action: MetadataAuthorityCommitActionV1::ActivateTarget {
                    migration_id: token.migration_id,
                    activation_token_digest: digest_target_token(token),
                },
                prior_marker_digest: digest_authority_marker(&authority.value),
                next_marker_digest: digest_authority_marker(&encode_authority_marker(next_marker)),
            },
            prior,
            exact_next,
        )?;
        let plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_SCHEMA_KEY.to_vec(),
                    witness: schema.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_STORE_IDENTITY_KEY.to_vec(),
                    witness: identity.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_APPLIED_RECOVERY_LSN_KEY.to_vec(),
                    witness: recovery_lsn.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_RECOVERY_CHAIN_DIGEST_KEY.to_vec(),
                    witness: chain_digest.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_COMMIT_CLOCK_KEY.to_vec(),
                    witness: commit_clock.witness,
                },
                AtomicOp::CompareAndPut {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_METADATA_AUTHORITY_KEY.to_vec(),
                    witness: authority.witness,
                    value: encode_authority_marker(next_marker),
                },
            ],
        };
        match self.commit_planned_exact(plan, &planned)? {
            AtomicCommitOutcome::Committed => Ok(()),
            AtomicCommitOutcome::Conflict => Err(AgentMetadataError::WriteConflict),
        }
    }

    /// Permanently abandon a migration target. This does not activate it.
    #[cfg(test)]
    pub(crate) fn fence_migration_target(&self) -> Result<(), AgentMetadataError> {
        let marker = self.required_authority_marker()?;
        let MetadataAuthorityEvidence::MigrationTargetBinding(binding) = marker.evidence else {
            return Err(migration_admission(
                "migration target fence requires its exact durable binding",
            ));
        };
        self.transition_with_exact_evidence(
            MetadataAuthorityState::MigrationTarget,
            MetadataAuthorityState::Fenced,
            MetadataAuthorityEvidence::MigrationTargetBinding(binding),
        )
    }

    fn transition_with_exact_evidence(
        &self,
        expected: MetadataAuthorityState,
        next: MetadataAuthorityState,
        evidence: MetadataAuthorityEvidence,
    ) -> Result<(), AgentMetadataError> {
        if !expected.permits_transition_to(next) {
            return Err(AgentMetadataError::InvalidMetadataAuthorityTransition {
                from: expected,
                to: next,
            });
        }
        let _command_guard = self
            .command_gate
            .write()
            .map_err(|error| backend("lock command gate", error))?;
        let planning_view = self.begin_commit_resolution_view()?;
        let schema = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let (identity, authority, marker) = self.required_authority_from(planning_view.as_ref())?;
        if marker.state == next {
            if marker.evidence == evidence {
                self.ensure_receipt_clean_for(self.frontier_point_from(planning_view.as_ref())?)?;
                return Ok(());
            }
            return Err(migration_admission(
                "authority transition replay carries different durable evidence",
            ));
        }
        if marker.state != expected {
            return Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected,
                actual: marker.state,
            });
        }
        if marker.evidence != evidence {
            return Err(migration_admission(
                "authority transition evidence does not match the durable marker",
            ));
        }
        let next_marker = MetadataAuthorityMarker {
            state: next,
            ..marker
                .advance_write_sequence()
                .ok_or(AgentMetadataError::VersionOverflow)?
        };
        let action = match evidence {
            MetadataAuthorityEvidence::SourceQuiesceReceipt(receipt) => {
                MetadataAuthorityCommitActionV1::FenceQuiescedSource {
                    migration_id: receipt.migration_id,
                    source_receipt_digest: digest_source_receipt(&receipt),
                }
            }
            MetadataAuthorityEvidence::MigrationTargetBinding(binding) => {
                MetadataAuthorityCommitActionV1::FenceTarget {
                    migration_id: binding.migration_id,
                    target_binding_digest: digest_target_binding(&binding),
                }
            }
            _ => {
                return Err(migration_admission(
                    "authority fence requires exact source or target migration evidence",
                ));
            }
        };
        let prior = self.frontier_point_from(planning_view.as_ref())?;
        let MetadataFrontierPointV1::Exact(prior_frontier) = prior else {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        };
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: prior_frontier
                .write_sequence
                .checked_add(1)
                .ok_or(AgentMetadataError::VersionOverflow)?,
            ..prior_frontier
        };
        let planned = self.plan_exact_commit(
            MetadataCommitPurposeV1::Authority {
                action,
                prior_marker_digest: digest_authority_marker(&authority.value),
                next_marker_digest: digest_authority_marker(&encode_authority_marker(next_marker)),
            },
            prior,
            exact_next,
        )?;
        let plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_SCHEMA_KEY.to_vec(),
                    witness: schema.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_STORE_IDENTITY_KEY.to_vec(),
                    witness: identity.witness,
                },
                AtomicOp::CompareAndPut {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_METADATA_AUTHORITY_KEY.to_vec(),
                    witness: authority.witness,
                    value: encode_authority_marker(next_marker),
                },
            ],
        };
        match self.commit_planned_exact(plan, &planned)? {
            AtomicCommitOutcome::Committed => Ok(()),
            AtomicCommitOutcome::Conflict => Err(AgentMetadataError::WriteConflict),
        }
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
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(MetadataReadStatsSessionError::StoreSessionAlreadyActive);
        }
        let store_key = self.read_stats_store_key();
        let storage_before = self
            .provider_diagnostics_snapshot()
            .map_err(|error| MetadataReadStatsSessionError::Provider(error.to_string()))?;
        if let Err(error) = read_stats::begin_session(store_key) {
            self.read_stats_identity
                .active
                .store(false, Ordering::Release);
            return Err(error);
        }
        Ok(MetadataReadStatsSession {
            store: self,
            store_key,
            storage_before,
            active: true,
            not_send: PhantomData,
        })
    }

    #[cfg(feature = "metadata-read-stats")]
    fn provider_diagnostics_snapshot(&self) -> Result<MetadataReadStats, ProviderError> {
        let Some(diagnostics) = self.provider.diagnostics() else {
            return Ok(MetadataReadStats::default());
        };
        Ok(diagnostics_snapshot_to_read_stats(diagnostics.snapshot()?))
    }

    /// Return the persisted physical-owner epoch. `None` is the fresh epoch-zero
    /// sentinel before the first owner is admitted.
    pub fn current_owner_epoch(&self) -> Result<Option<OwnerEpoch>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.current_owner_epoch_unlocked()
    }

    fn current_owner_epoch_unlocked(&self) -> Result<Option<OwnerEpoch>, AgentMetadataError> {
        let record = self.required_system_record(SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
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
        self.read_tree_value(
            crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
            root_id.as_bytes(),
            MetadataPointReadSource::RootFence,
            "read RootFence",
        )?
        .map(|value| RootFence::decode(&value).map_err(|error| corrupt("RootFence", error)))
        .transpose()
    }

    /// Return the persisted monotonic lease clock used by snapshot expiry.
    pub fn lease_clock_high_water(&self) -> Result<u64, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.lease_clock_high_water_unlocked()
    }

    fn lease_clock_high_water_unlocked(&self) -> Result<u64, AgentMetadataError> {
        let record = self.required_system_record(
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
        let read_view = self
            .provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                prefix: Vec::new(),
            }])
            .map_err(provider_error)?;
        let scan = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                prefix: vec![0],
                start_after: Some(start.to_vec()),
                delimiter: None,
                limit,
            })
            .map_err(provider_error)?;
        let mut rows = Vec::with_capacity(limit);
        let mut encoded_bytes = 0_usize;
        for entry in scan.items {
            let ProviderScanItem::Key { key, value } = entry else {
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
            let row = self.read_recovery_record_from(read_view.as_ref(), key_lsn, &value)?;
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
        let planning_view = self.begin_commit_resolution_view()?;
        let schema = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let (identity, authority, authority_marker) =
            self.required_active_authority_from(planning_view.as_ref())?;
        let owner = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_OWNER_FENCE_KEY,
            "System(owner_fence)",
        )?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let root_fence = planning_view
            .get(
                crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
                root_id.as_bytes(),
            )
            .map_err(provider_error)?
            .ok_or(AgentMetadataError::RootFenceMissing)?;
        let fence =
            RootFence::decode(&root_fence.value).map_err(|error| corrupt("RootFence", error))?;
        if fence.logical_shard_id != self.identity.logical_shard_id
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
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY,
            "System(lease_clock_high_water)",
        )?;
        let current = decode_system_u64(&clock.value, "System(lease_clock_high_water)")?;
        if observed_ms <= current {
            self.ensure_receipt_clean_for(self.frontier_point_from(planning_view.as_ref())?)?;
            return Ok(current);
        }
        let recovery = self.plan_recovery(
            planning_view.as_ref(),
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
        let prior = self.frontier_point_from(planning_view.as_ref())?;
        let MetadataFrontierPointV1::Exact(prior_frontier) = prior else {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        };
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: prior_frontier
                .write_sequence
                .checked_add(1)
                .ok_or(AgentMetadataError::VersionOverflow)?,
            recovery_lsn: recovery.row.recovery_lsn,
            chain_digest: recovery.row.chain_digest,
            ..prior_frontier
        };
        let planned = self.plan_exact_commit(
            MetadataCommitPurposeV1::ObserveLeaseClock {
                root_id,
                placement_generation,
                owner_epoch,
                observed_ms,
            },
            prior,
            exact_next,
        )?;
        let mut plan = AtomicPlan::default();
        for (key, record) in [
            (SYSTEM_SCHEMA_KEY, &schema),
            (SYSTEM_STORE_IDENTITY_KEY, &identity),
            (SYSTEM_OWNER_FENCE_KEY, &owner),
        ] {
            plan.operations.push(AtomicOp::AssertUnchanged {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                key: key.to_vec(),
                witness: record.witness.clone(),
            });
        }
        enqueue_active_authority_advance(&mut plan, &authority, authority_marker)?;
        plan.operations.push(AtomicOp::AssertUnchanged {
            space: crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
            key: root_id.as_bytes().to_vec(),
            witness: root_fence.witness,
        });
        plan.operations.push(AtomicOp::CompareAndPut {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            key: SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY.to_vec(),
            witness: clock.witness,
            value: encode_system_u64(observed_ms).to_vec(),
        });
        enqueue_recovery(&mut plan, &recovery);
        match self.commit_planned_exact(plan, &planned)? {
            AtomicCommitOutcome::Committed => Ok(observed_ms),
            AtomicCommitOutcome::Conflict => Err(AgentMetadataError::WriteConflict),
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
        let planning_view = self.begin_commit_resolution_view()?;
        let schema = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let (identity, authority, authority_marker) =
            self.required_active_authority_from(planning_view.as_ref())?;
        let owner = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_OWNER_FENCE_KEY,
            "System(owner_fence)",
        )?;
        let current = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if current == next.get() {
            self.ensure_receipt_clean_for(self.frontier_point_from(planning_view.as_ref())?)?;
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
            planning_view.as_ref(),
            RecoveryMutationV1::AdvanceOwnerEpoch { expected, next },
            RecoveryResultV1::OwnerEpoch {
                applied_owner_epoch: next,
            },
        )?;
        let prior = self.frontier_point_from(planning_view.as_ref())?;
        let MetadataFrontierPointV1::Exact(prior_frontier) = prior else {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        };
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: prior_frontier
                .write_sequence
                .checked_add(1)
                .ok_or(AgentMetadataError::VersionOverflow)?,
            recovery_lsn: recovery.row.recovery_lsn,
            chain_digest: recovery.row.chain_digest,
            ..prior_frontier
        };
        let planned = self.plan_exact_commit(
            MetadataCommitPurposeV1::AdvanceOwnerEpoch { expected, next },
            prior,
            exact_next,
        )?;
        let mut plan = AtomicPlan::default();
        plan.operations.push(AtomicOp::AssertUnchanged {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            key: SYSTEM_SCHEMA_KEY.to_vec(),
            witness: schema.witness,
        });
        plan.operations.push(AtomicOp::AssertUnchanged {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            key: SYSTEM_STORE_IDENTITY_KEY.to_vec(),
            witness: identity.witness,
        });
        enqueue_active_authority_advance(&mut plan, &authority, authority_marker)?;
        plan.operations.push(AtomicOp::CompareAndPut {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            key: SYSTEM_OWNER_FENCE_KEY.to_vec(),
            witness: owner.witness,
            value: encode_system_u64(next.get()).to_vec(),
        });
        enqueue_recovery(&mut plan, &recovery);
        match self.commit_planned_exact(plan, &planned)? {
            AtomicCommitOutcome::Committed => Ok(()),
            AtomicCommitOutcome::Conflict => Err(AgentMetadataError::WriteConflict),
        }
    }

    /// Replay one already-verified recovery row through the same authoritative
    /// write paths that originally produced it. This is the sole recovery
    /// dispatcher used by diagnostics and recovery conformance tests.
    pub(super) fn replay_recovery_record(
        &self,
        row: &RecoveryOutboxRecord,
    ) -> Result<(), AgentMetadataError> {
        match (&row.mutation, &row.result) {
            (
                RecoveryMutationV1::AdvanceOwnerEpoch { expected, next },
                RecoveryResultV1::OwnerEpoch {
                    applied_owner_epoch,
                },
            ) => {
                self.advance_owner_epoch(*expected, *next)?;
                if next != applied_owner_epoch {
                    return Err(corrupt(
                        "RecoveryOutbox replay result",
                        "owner epoch result differs from mutation",
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
                let actual = self.observe_lease_clock(
                    *root_id,
                    *placement_generation,
                    *owner_epoch,
                    *observed_ms,
                )?;
                if actual != *effective_high_water_ms {
                    return Err(corrupt(
                        "RecoveryOutbox replay result",
                        "lease-clock result differs from authoritative replay",
                    ));
                }
            }
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
                let actual = match lease_deadline_ms {
                    Some(deadline) => self.execute_before_lease_deadline(command, *deadline)?,
                    None => self.execute(command)?,
                };
                if actual.commit_version != *commit_version
                    || actual.deterministic_result != *deterministic_result
                    || actual.replayed
                {
                    return Err(corrupt(
                        "RecoveryOutbox replay result",
                        "metadata command result differs from authoritative replay",
                    ));
                }
            }
            _ => {
                return Err(corrupt(
                    "RecoveryOutbox replay result",
                    "mutation and result variants are not paired",
                ));
            }
        }
        let frontier = self.recovery_state()?;
        if frontier.applied_recovery_lsn != row.recovery_lsn
            || frontier.chain_digest != row.chain_digest
        {
            return Err(corrupt(
                "RecoveryOutbox replay frontier",
                "authoritative write path did not consume the exact recovery LSN/digest",
            ));
        }
        Ok(())
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
        let planning_view = self.begin_commit_resolution_view()?;
        let (identity, authority, authority_marker) =
            self.required_authority_from(planning_view.as_ref())?;
        if let Some(result) =
            self.replayed_result_from(planning_view.as_ref(), &dedupe_key, command.command_digest)?
        {
            self.ensure_receipt_clean_for(self.frontier_point_from(planning_view.as_ref())?)?;
            return Ok(result);
        }
        if authority_marker.state != MetadataAuthorityState::Active {
            return Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::Active,
                actual: authority_marker.state,
            });
        }

        let schema = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )?;
        validate_schema_marker(&schema.value).map_err(|error| AgentMetadataError::SchemaGate {
            reason: error.to_string(),
        })?;
        let owner = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_OWNER_FENCE_KEY,
            "System(owner_fence)",
        )?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != command.owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: command.owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let clock = required_record(
            planning_view.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
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

        let root_plan = self.plan_root_fence(planning_view.as_ref(), command)?;
        let lease_clock = lease_deadline_ms
            .map(|requested_deadline_ms| {
                let clock = required_record(
                    planning_view.as_ref(),
                    crate::workspace::provider_catalog::SYSTEM_SPACE,
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
        let predicate_plan = self.plan_predicates(planning_view.as_ref(), command)?;
        self.validate_history_projection(command, &predicate_plan)?;

        let recovery = self.plan_recovery(
            planning_view.as_ref(),
            RecoveryMutationV1::MetadataCommand {
                command: Box::new(command.clone()),
                lease_deadline_ms,
            },
            RecoveryResultV1::MetadataCommand {
                commit_version: next_version,
                deterministic_result: command.deterministic_result.clone(),
            },
        )?;
        let prior = self.frontier_point_from(planning_view.as_ref())?;
        let MetadataFrontierPointV1::Exact(prior_frontier) = prior else {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        };
        let exact_next = AcknowledgedMetadataFrontier {
            write_sequence: prior_frontier
                .write_sequence
                .checked_add(1)
                .ok_or(AgentMetadataError::VersionOverflow)?,
            commit_version: next_version,
            recovery_lsn: recovery.row.recovery_lsn,
            chain_digest: recovery.row.chain_digest,
        };
        let class = if matches!(command.root_fence_action, RootFenceAction::RequireActive) {
            MetadataCommandCommitClassV1::Domain
        } else {
            MetadataCommandCommitClassV1::RootFence
        };
        let planned = self.plan_exact_commit(
            MetadataCommitPurposeV1::MetadataCommand {
                class,
                root_id: command.root_id,
                request_id: command.request_id,
                command_digest: command.command_digest,
                lease_deadline_ms,
            },
            prior,
            exact_next,
        )?;

        let dedupe_record = CommandDedupeRecord {
            command_digest: command.command_digest,
            commit_version: next_version,
            recovery_lsn: recovery.row.recovery_lsn,
            deterministic_result: command.deterministic_result.clone(),
        }
        .encode()
        .map_err(|error| corrupt("CommandDedupe", error))?;

        let mut atomic = AtomicPlan::default();
        for (key, record) in [
            (SYSTEM_SCHEMA_KEY, &schema),
            (SYSTEM_STORE_IDENTITY_KEY, &identity),
            (SYSTEM_OWNER_FENCE_KEY, &owner),
        ] {
            atomic.operations.push(AtomicOp::AssertUnchanged {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                key: key.to_vec(),
                witness: record.witness.clone(),
            });
        }
        enqueue_active_authority_advance(&mut atomic, &authority, authority_marker)?;
        if let Some(lease_clock) = &lease_clock {
            atomic.operations.push(AtomicOp::AssertUnchanged {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                key: SYSTEM_LEASE_CLOCK_HIGH_WATER_KEY.to_vec(),
                witness: lease_clock.witness.clone(),
            });
        }
        atomic.operations.push(AtomicOp::CompareAndPut {
            space: crate::workspace::provider_catalog::SYSTEM_SPACE,
            key: SYSTEM_COMMIT_CLOCK_KEY.to_vec(),
            witness: clock.witness,
            value: encode_system_u64(next_version_raw).to_vec(),
        });
        enqueue_root_fence(&mut atomic, command, &root_plan);
        enqueue_predicate_guards(&mut atomic, &predicate_plan);

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
            atomic.operations.push(AtomicOp::Put {
                space: crate::workspace::provider_catalog::HISTORY_SPACE,
                key,
                value,
            });
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
                    atomic.operations.push(AtomicOp::Put {
                        space: crate::workspace::provider_catalog::domain_space(*family),
                        key: key.clone(),
                        value: encoded,
                    });
                }
                CommandMutation::Delete { family, key } => {
                    atomic.operations.push(AtomicOp::Delete {
                        space: crate::workspace::provider_catalog::domain_space(*family),
                        key: key.clone(),
                    });
                }
            }
        }
        for (sequence, projection) in command.event_projection.iter().enumerate() {
            let sequence = u32::try_from(sequence)
                .expect("validated event count fits the event-key sequence width");
            let key = change_event_key(command.root_id, next_version, sequence);
            let value = CurrentValue {
                created_version: next_version,
                modified_version: next_version,
                payload: projection.payload.clone(),
            }
            .encode()
            .expect("validated event fits the format envelope");
            atomic.operations.push(AtomicOp::PutIfAbsent {
                space: crate::workspace::provider_catalog::CHANGE_EVENT_SPACE,
                key,
                value,
            });
        }
        atomic.operations.push(AtomicOp::PutIfAbsent {
            space: crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
            key: dedupe_key.clone(),
            value: dedupe_record,
        });
        enqueue_recovery(&mut atomic, &recovery);

        let outcome = self.commit_planned_exact(atomic, &planned)?;
        if outcome == AtomicCommitOutcome::Committed {
            Ok(MetadataCommandResult {
                commit_version: next_version,
                deterministic_result: command.deterministic_result.clone(),
                replayed: false,
            })
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

    /// Run dependent point reads under one ownership/fence validation.
    ///
    /// `requested_version = None` captures the current version inside the
    /// guarded window. A supplied historical version is rejected if it is
    /// newer than the same captured current version. The callback must remain
    /// limited to metadata point reads and local decoding.
    pub(super) fn with_fenced_point_reads<R, E>(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        requested_version: Option<ReadVersion>,
        read: impl FnOnce(ReadVersion, &FencedPointReader<'_>) -> Result<R, E>,
    ) -> Result<R, E>
    where
        E: From<AgentMetadataError>,
    {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| E::from(backend("lock read gate", error)))?;
        let current_version = self
            .validate_read_context(root_id, placement_generation, owner_epoch)
            .map_err(E::from)?;
        let version = requested_version.unwrap_or(current_version);
        if version > current_version {
            return Err(E::from(AgentMetadataError::ReadVersionInFuture {
                requested: version.get(),
                current: current_version.get(),
            }));
        }
        let reader = FencedPointReader {
            store: self,
            root_id,
            version,
            current_version,
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
    ) -> Result<Option<CommandDedupeRecord>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        let key = command_dedupe_key(root_id, request_id);
        let current_version = self.current_read_version_unlocked()?;
        self.validate_read_fence(
            root_id,
            placement_generation,
            owner_epoch,
            &key,
            current_version,
        )?;
        self.read_tree_value(
            crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
            &key,
            MetadataPointReadSource::Other,
            "read CommandDedupe",
        )?
        .map(|value| {
            CommandDedupeRecord::decode(&value).map_err(|error| corrupt("CommandDedupe", error))
        })
        .transpose()
    }

    /// Stable ordered prefix scan at one fenced read version.
    ///
    /// The current-version path is one provider prefix scan. A historical scan also
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
    ) -> Result<Vec<DelimitedMetadataScanItem>, AgentMetadataError> {
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
    ) -> Result<Vec<DelimitedMetadataScanItem>, AgentMetadataError> {
        let effective_limit = if limit == 0 {
            max_items
        } else {
            limit.min(max_items)
        };
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_call();
        // Fence and capture one immutable provider view while writes are
        // excluded, then release the NoKV read gate before decoding the page.
        let (current_version, read_view) = {
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
            let scopes = if version == current_version {
                vec![ReadScope {
                    space: crate::workspace::provider_catalog::domain_space(family),
                    prefix: prefix.to_vec(),
                }]
            } else {
                vec![
                    ReadScope {
                        space: crate::workspace::provider_catalog::domain_space(family),
                        prefix: prefix.to_vec(),
                    },
                    ReadScope {
                        space: crate::workspace::provider_catalog::HISTORY_SPACE,
                        prefix: vec![family.history_tag()],
                    },
                ]
            };
            let read_view = self.provider.begin_read(&scopes).map_err(provider_error)?;
            (current_version, read_view)
        };

        // Current-state rows are already in the exact order required by the
        // caller. Push the exclusive cursor into the provider and stop advancing the
        // storage iterator as soon as the bounded page is full. Historical
        // reads cannot use this shortcut: a key absent from current state may
        // still need to be reconstructed from History before ordering and
        // pagination are applied.
        if version == current_version {
            let scan = read_view
                .scan(&ProviderScan {
                    space: crate::workspace::provider_catalog::domain_space(family),
                    prefix: prefix.to_vec(),
                    start_after: start_after.map(<[u8]>::to_vec),
                    delimiter,
                    limit: effective_limit,
                })
                .map_err(provider_error)?;
            let mut visible = Vec::with_capacity(effective_limit);
            #[cfg(feature = "metadata-read-stats")]
            let mut key_bytes = 0_u64;
            #[cfg(feature = "metadata-read-stats")]
            let mut value_bytes = 0_u64;
            #[cfg(feature = "metadata-read-stats")]
            let stopped_at_limit = scan.items.len() == effective_limit;
            for entry in scan.items {
                let item = match entry {
                    ProviderScanItem::Key { key, value } => {
                        #[cfg(feature = "metadata-read-stats")]
                        {
                            key_bytes = key_bytes.saturating_add(byte_len(&key));
                            value_bytes = value_bytes.saturating_add(byte_len(&value));
                        }
                        let current = CurrentValue::decode(&value)
                            .map_err(|error| corrupt(family.tree_name(), error))?;
                        if current.modified_version.get() > version.get() {
                            return Err(AgentMetadataError::CorruptRecord {
                                record: family.tree_name(),
                                reason: format!(
                                    "current row version {} exceeds captured version {}",
                                    current.modified_version.get(),
                                    version.get()
                                ),
                            });
                        }
                        DelimitedMetadataScanItem::Record(MetadataScanItem {
                            key,
                            value: current.payload,
                        })
                    }
                    ProviderScanItem::CommonPrefix(prefix) => {
                        #[cfg(feature = "metadata-read-stats")]
                        {
                            key_bytes = key_bytes.saturating_add(byte_len(&prefix));
                        }
                        DelimitedMetadataScanItem::CommonPrefix(prefix)
                    }
                };
                visible.push(item);
            }
            #[cfg(feature = "metadata-read-stats")]
            {
                self.record_scan_cursor(
                    scan.stats.visited,
                    scan.stats.returned,
                    scan.stats.common_prefixes,
                    scan.stats.restarts,
                    key_bytes,
                    value_bytes,
                    stopped_at_limit,
                );
            }
            return Ok(visible);
        }

        let mut visible = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        #[cfg(feature = "metadata-read-stats")]
        let mut current_key_bytes = 0_u64;
        #[cfg(feature = "metadata-read-stats")]
        let mut current_value_bytes = 0_u64;
        let current_scan = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::domain_space(family),
                prefix: prefix.to_vec(),
                start_after: None,
                delimiter: None,
                limit: 0,
            })
            .map_err(provider_error)?;
        for entry in &current_scan.items {
            let ProviderScanItem::Key { key, value } = entry else {
                continue;
            };
            #[cfg(feature = "metadata-read-stats")]
            {
                current_key_bytes = current_key_bytes.saturating_add(byte_len(key));
                current_value_bytes = current_value_bytes.saturating_add(byte_len(value));
            }
            let current =
                CurrentValue::decode(value).map_err(|error| corrupt(family.tree_name(), error))?;
            if current.modified_version.get() <= version.get() {
                visible.insert(key.clone(), current.payload);
            }
        }
        #[cfg(feature = "metadata-read-stats")]
        {
            self.record_scan_cursor(
                current_scan.stats.visited,
                current_scan.stats.returned,
                current_scan.stats.common_prefixes,
                current_scan.stats.restarts,
                current_key_bytes,
                current_value_bytes,
                false,
            );
        }

        #[cfg(feature = "metadata-read-stats")]
        let mut history_key_bytes = 0_u64;
        #[cfg(feature = "metadata-read-stats")]
        let mut history_value_bytes = 0_u64;
        let history_scan = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::HISTORY_SPACE,
                prefix: vec![family.history_tag()],
                start_after: None,
                delimiter: None,
                limit: 0,
            })
            .map_err(provider_error)?;
        for entry in &history_scan.items {
            let ProviderScanItem::Key { key, value } = entry else {
                continue;
            };
            #[cfg(feature = "metadata-read-stats")]
            {
                history_key_bytes = history_key_bytes.saturating_add(byte_len(key));
                history_value_bytes = history_value_bytes.saturating_add(byte_len(value));
            }
            let user_key = history_user_key(key)?;
            if !user_key.starts_with(prefix) || visible.contains_key(user_key) {
                continue;
            }
            let history = HistoryValue::decode(value).map_err(|error| corrupt("History", error))?;
            if history.previous_modified_version.get() <= version.get()
                && version.get() < history.transition_version.get()
            {
                if let Some(previous) = history.previous_payload {
                    visible.insert(user_key.to_vec(), previous);
                }
            }
        }
        #[cfg(feature = "metadata-read-stats")]
        {
            self.record_scan_cursor(
                history_scan.stats.visited,
                history_scan.stats.returned,
                history_scan.stats.common_prefixes,
                history_scan.stats.restarts,
                history_key_bytes,
                history_value_bytes,
                false,
            );
        }

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

    /// Read one immutable change event through the ordinary ownership fences.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn read_change_event_at(
        &self,
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        key: &[u8],
        version: ReadVersion,
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        let _read_guard = self
            .command_gate
            .read()
            .map_err(|error| backend("lock read gate", error))?;
        self.validate_read_fence(root_id, placement_generation, owner_epoch, key, version)?;
        let Some(record) = self.read_tree_value(
            crate::workspace::provider_catalog::CHANGE_EVENT_SPACE,
            key,
            MetadataPointReadSource::Other,
            "read ChangeEvent",
        )?
        else {
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
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_call();
        let read_view = self
            .provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::CHANGE_EVENT_SPACE,
                prefix: prefix.to_vec(),
            }])
            .map_err(provider_error)?;
        let scan = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::CHANGE_EVENT_SPACE,
                prefix: prefix.to_vec(),
                start_after: start_after.map(<[u8]>::to_vec),
                delimiter: None,
                limit: effective_limit,
            })
            .map_err(provider_error)?;
        let mut events = Vec::with_capacity(effective_limit);
        #[cfg(feature = "metadata-read-stats")]
        let mut key_bytes = 0_u64;
        #[cfg(feature = "metadata-read-stats")]
        let mut value_bytes = 0_u64;
        #[cfg(feature = "metadata-read-stats")]
        let mut stopped_at_limit = false;
        for entry in scan.items {
            let ProviderScanItem::Key { key, value } = entry else {
                continue;
            };
            #[cfg(feature = "metadata-read-stats")]
            {
                key_bytes = key_bytes.saturating_add(byte_len(&key));
                value_bytes = value_bytes.saturating_add(byte_len(&value));
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
                #[cfg(feature = "metadata-read-stats")]
                {
                    stopped_at_limit = true;
                }
                break;
            }
        }
        #[cfg(feature = "metadata-read-stats")]
        {
            self.record_scan_cursor(
                scan.stats.visited,
                scan.stats.returned,
                scan.stats.common_prefixes,
                scan.stats.restarts,
                key_bytes,
                value_bytes,
                stopped_at_limit,
            );
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
        let current_version =
            self.validate_read_context(root_id, placement_generation, owner_epoch)?;
        if version > current_version {
            return Err(AgentMetadataError::ReadVersionInFuture {
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
    ) -> Result<ReadVersion, AgentMetadataError> {
        let owner = self.required_system_record(SYSTEM_OWNER_FENCE_KEY, "System(owner_fence)")?;
        let actual_owner = decode_system_u64(&owner.value, "System(owner_fence)")?;
        if actual_owner != owner_epoch.get() {
            return Err(AgentMetadataError::OwnerEpochMismatch {
                expected: owner_epoch.get(),
                actual: actual_owner,
            });
        }
        let fence = self
            .read_tree_value(
                crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
                root_id.as_bytes(),
                MetadataPointReadSource::RootFence,
                "read RootFence",
            )?
            .ok_or(AgentMetadataError::RootFenceMissing)?;
        let fence = RootFence::decode(&fence).map_err(|error| corrupt("RootFence", error))?;
        if fence.logical_shard_id != self.identity.logical_shard_id
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
        let current_version = self.current_read_version_unlocked()?;
        Ok(current_version)
    }

    fn read_at_unfenced(
        &self,
        family: MetadataFamily,
        key: &[u8],
        version: ReadVersion,
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        if let Some(record) = self.read_family_value(family, key, "read current metadata")? {
            let current = CurrentValue::decode(&record)
                .map_err(|error| corrupt(family.tree_name(), error))?;
            if current.modified_version.get() <= version.get() {
                return Ok(Some(current.payload));
            }
        }
        let prefix = history_prefix(family, key);
        #[cfg(feature = "metadata-read-stats")]
        self.record_scan_call();
        #[cfg(feature = "metadata-read-stats")]
        let mut key_bytes = 0_u64;
        #[cfg(feature = "metadata-read-stats")]
        let mut value_bytes = 0_u64;
        let mut previous_payload = None;
        let read_view = self
            .provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::HISTORY_SPACE,
                prefix: prefix.clone(),
            }])
            .map_err(provider_error)?;
        let scan = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::HISTORY_SPACE,
                prefix,
                start_after: None,
                delimiter: None,
                limit: 0,
            })
            .map_err(provider_error)?;
        for entry in scan.items {
            let ProviderScanItem::Key {
                key: _history_key,
                value,
            } = entry
            else {
                continue;
            };
            #[cfg(feature = "metadata-read-stats")]
            {
                key_bytes = key_bytes.saturating_add(byte_len(&_history_key));
                value_bytes = value_bytes.saturating_add(byte_len(&value));
            }
            let history =
                HistoryValue::decode(&value).map_err(|error| corrupt("History", error))?;
            if history.previous_modified_version.get() <= version.get()
                && version.get() < history.transition_version.get()
            {
                previous_payload = Some(history.previous_payload);
                break;
            }
        }
        #[cfg(feature = "metadata-read-stats")]
        {
            self.record_scan_cursor(
                scan.stats.visited,
                scan.stats.returned,
                scan.stats.common_prefixes,
                scan.stats.restarts,
                key_bytes,
                value_bytes,
                false,
            );
        }
        Ok(previous_payload.flatten())
    }

    fn read_current_at_unfenced(
        &self,
        family: MetadataFamily,
        key: &[u8],
        current_version: ReadVersion,
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        let Some(record) = self.read_family_value(family, key, "read current metadata")? else {
            return Ok(None);
        };
        let current =
            CurrentValue::decode(&record).map_err(|error| corrupt(family.tree_name(), error))?;
        if current.modified_version.get() > current_version.get() {
            return Err(corrupt(
                family.tree_name(),
                format!(
                    "record version {} is newer than the captured commit clock {}",
                    current.modified_version.get(),
                    current_version.get()
                ),
            ));
        }
        Ok(Some(current.payload))
    }

    fn validate_command(&self, command: &MetadataCommand) -> Result<(), AgentMetadataError> {
        if command.schema_id != SCHEMA_ID {
            return Err(AgentMetadataError::SchemaGate {
                reason: format!("expected schema {SCHEMA_ID}, found {}", command.schema_id),
            });
        }
        if command.logical_shard_id != self.identity.logical_shard_id {
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
        reader: &dyn MetadataReadView,
        command: &MetadataCommand,
    ) -> Result<RootFencePlan, AgentMetadataError> {
        let key = command.root_id.as_bytes();
        let current = reader
            .get(crate::workspace::provider_catalog::ROOT_FENCE_SPACE, key)
            .map_err(provider_error)?;
        match command.root_fence_action {
            RootFenceAction::Install {
                layout_profile,
                layout_generation,
                partition_id,
            } => {
                if current.is_some() {
                    return Err(AgentMetadataError::RootFenceAlreadyInstalled);
                }
                Ok(RootFencePlan::Install {
                    value: RootFence {
                        logical_shard_id: command.logical_shard_id,
                        placement_generation: command.placement_generation,
                        layout_profile,
                        layout_generation,
                        partition_id,
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
                    witness: current.witness,
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
                    witness: current.witness,
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
        reader: &dyn MetadataReadView,
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
                    let record = reader
                        .get(
                            crate::workspace::provider_catalog::domain_space(*family),
                            key,
                        )
                        .map_err(provider_error)?;
                    let (current, witness) = match record {
                        Some(record) => {
                            let current = CurrentValue::decode(&record.value)
                                .map_err(|error| corrupt(family.tree_name(), error))?;
                            (Some(current), Some(record.witness))
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
                            witness,
                        },
                    );
                }
                CommandPredicate::PrefixEmpty { family, prefix } => {
                    let page = reader
                        .scan(&ProviderScan {
                            space: crate::workspace::provider_catalog::domain_space(*family),
                            prefix: prefix.clone(),
                            start_after: None,
                            delimiter: None,
                            limit: 1,
                        })
                        .map_err(provider_error)?;
                    if !page.items.is_empty() {
                        return Err(AgentMetadataError::PredicateFailed);
                    }
                    plan.prefix_empty.push((*family, prefix.clone()));
                }
            }
        }
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
                && (matches!(mutation, CommandMutation::Delete { .. })
                    || predicate.current.is_some())
            {
                return Err(invalid(
                    "workspace incarnation claims are append-only and permanent",
                ));
            }
            if matches!(mutation, CommandMutation::Delete { .. }) && predicate.current.is_none() {
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

    fn replayed_result_from(
        &self,
        reader: &dyn MetadataReadView,
        key: &[u8],
        digest: CommandDigest,
    ) -> Result<Option<MetadataCommandResult>, AgentMetadataError> {
        let value = reader
            .get(
                crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
                key,
            )
            .map_err(provider_error)?
            .map(|record| record.value);
        decode_replayed_result(value, digest)
    }

    fn recovery_state_unlocked(&self) -> Result<RecoveryState, AgentMetadataError> {
        let lsn = decode_system_u64(
            &self
                .required_system_record(
                    SYSTEM_APPLIED_RECOVERY_LSN_KEY,
                    "System(applied_recovery_lsn)",
                )?
                .value,
            "System(applied_recovery_lsn)",
        )?;
        let chain_digest = decode_system_digest(
            &self
                .required_system_record(
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
}

fn decode_replayed_result(
    value: Option<Vec<u8>>,
    digest: CommandDigest,
) -> Result<Option<MetadataCommandResult>, AgentMetadataError> {
    let Some(value) = value else {
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

impl AgentMetadataStore {
    fn plan_recovery(
        &self,
        reader: &dyn MetadataReadView,
        mutation: RecoveryMutationV1,
        result: RecoveryResultV1,
    ) -> Result<RecoveryPlan, AgentMetadataError> {
        let lsn_record = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            "System(applied_recovery_lsn)",
        )?;
        let applied_lsn = decode_system_u64(&lsn_record.value, "System(applied_recovery_lsn)")?;
        let recovery_lsn = applied_lsn
            .checked_add(1)
            .ok_or(AgentMetadataError::VersionOverflow)?;
        let digest_record = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
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
        let read_view = self
            .provider
            .begin_read(&[
                ReadScope {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    prefix: Vec::new(),
                },
                ReadScope {
                    space: crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                    prefix: Vec::new(),
                },
            ])
            .map_err(provider_error)?;
        let state = recovery_state_from(read_view.as_ref())?;
        let scan = read_view
            .scan(&ProviderScan {
                space: crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                prefix: Vec::new(),
                start_after: None,
                delimiter: None,
                limit: 0,
            })
            .map_err(provider_error)?;
        let mut expected_lsn = 1_u64;
        let mut previous_chain_digest = recovery_genesis_digest(
            self.identity.logical_shard_id,
            self.identity.contract_digest,
        );
        let mut expected_chunk_keys = BTreeSet::new();
        for entry in scan.items {
            let ProviderScanItem::Key { key, value } = entry else {
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
                    let row =
                        self.read_recovery_record_from(read_view.as_ref(), key_lsn, &value)?;
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

    pub(super) fn read_recovery_record_from(
        &self,
        reader: &dyn MetadataReadView,
        recovery_lsn: u64,
        header: &[u8],
    ) -> Result<RecoveryOutboxRecord, AgentMetadataError> {
        let chunk_count = recovery_storage_chunk_count(header)
            .map_err(|error| corrupt("RecoveryOutbox storage header", error))?;
        let mut chunks = Vec::with_capacity(chunk_count as usize);
        for index in 0..chunk_count {
            let value = reader
                .get(
                    crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                    &recovery_chunk_key(recovery_lsn, index),
                )
                .map_err(provider_error)?
                .map(|record| record.value)
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
    #[allow(clippy::too_many_arguments)]
    fn record_scan_cursor(
        &self,
        visited_units: u64,
        returned_keys: u64,
        common_prefixes: u64,
        restarts: u64,
        key_bytes: u64,
        value_bytes: u64,
        stopped_at_limit: bool,
    ) {
        read_stats::record_scan_cursor(
            self.read_stats_store_key(),
            visited_units,
            returned_keys,
            common_prefixes,
            restarts,
            key_bytes,
            value_bytes,
            stopped_at_limit,
        );
    }

    fn read_family_value(
        &self,
        family: MetadataFamily,
        key: &[u8],
        operation: &'static str,
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        self.read_tree_value(
            crate::workspace::provider_catalog::domain_space(family),
            key,
            point_source(family),
            operation,
        )
    }

    fn read_tree_value(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
        source: MetadataPointReadSource,
        _operation: &'static str,
    ) -> Result<Option<Vec<u8>>, AgentMetadataError> {
        let value = self
            .provider
            .get(space, key)
            .map_err(provider_error)?
            .map(|record| record.value);
        #[cfg(feature = "metadata-read-stats")]
        self.record_point(source, value.as_ref().map(Vec::len));
        #[cfg(not(feature = "metadata-read-stats"))]
        let _ = source;
        Ok(value)
    }

    fn read_tree_record(
        &self,
        space: OrderedSpaceId,
        key: &[u8],
        source: MetadataPointReadSource,
        _operation: &'static str,
    ) -> Result<Option<ProviderRecord>, AgentMetadataError> {
        let record = self.provider.get(space, key).map_err(provider_error)?;
        #[cfg(feature = "metadata-read-stats")]
        self.record_point(source, record.as_ref().map(|record| record.value.len()));
        #[cfg(not(feature = "metadata-read-stats"))]
        let _ = source;
        Ok(record)
    }

    fn required_system_record(
        &self,
        key: &[u8],
        record: &'static str,
    ) -> Result<ProviderRecord, AgentMetadataError> {
        self.read_tree_record(
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            key,
            MetadataPointReadSource::System,
            "read required record",
        )?
        .ok_or_else(|| AgentMetadataError::CorruptRecord {
            record,
            reason: "record is missing".to_owned(),
        })
    }

    fn required_authority_marker(&self) -> Result<MetadataAuthorityMarker, AgentMetadataError> {
        let read_view = self
            .provider
            .begin_read(&[ReadScope {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                prefix: Vec::new(),
            }])
            .map_err(provider_error)?;
        let (_, _, marker) = self.required_authority_from(read_view.as_ref())?;
        Ok(marker)
    }

    fn required_active_authority_from(
        &self,
        reader: &dyn MetadataReadView,
    ) -> Result<(ProviderRecord, ProviderRecord, MetadataAuthorityMarker), AgentMetadataError> {
        let (identity, authority, marker) = self.required_authority_from(reader)?;
        if marker.state != MetadataAuthorityState::Active {
            return Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::Active,
                actual: marker.state,
            });
        }
        Ok((identity, authority, marker))
    }

    fn required_authority_from(
        &self,
        reader: &dyn MetadataReadView,
    ) -> Result<(ProviderRecord, ProviderRecord, MetadataAuthorityMarker), AgentMetadataError> {
        let identity = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_STORE_IDENTITY_KEY,
            "System(store_identity)",
        )?;
        let durable_identity = decode_store_identity(&identity.value)
            .map_err(|error| corrupt("MetadataStoreIdentity", error))?;
        if durable_identity != self.identity {
            return Err(AgentMetadataError::MetadataStoreIdentityMismatch);
        }
        let authority = required_record(
            reader,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_METADATA_AUTHORITY_KEY,
            "System(metadata_authority)",
        )?;
        let marker = decode_authority_marker(&authority.value)
            .map_err(|error| corrupt("MetadataAuthorityState", error))?;
        if !marker.matches_identity(durable_identity) {
            return Err(AgentMetadataError::MetadataAuthorityBindingMismatch);
        }
        validate_authority_marker_for_identity(durable_identity, marker)?;
        Ok((identity, authority, marker))
    }
}

#[derive(Default)]
struct PredicatePlan {
    exact: BTreeMap<(MetadataFamily, Vec<u8>), PlannedExactPredicate>,
    prefix_empty: Vec<(MetadataFamily, Vec<u8>)>,
}

struct RecoveryPlan {
    lsn_record: ProviderRecord,
    digest_record: ProviderRecord,
    header: Vec<u8>,
    chunks: Vec<Vec<u8>>,
    row: RecoveryOutboxRecord,
}

enum PlannedCommitObservation {
    Applied { purpose_evidence_digest: [u8; 32] },
    NotApplied { purpose_evidence_digest: [u8; 32] },
    Foreign,
}

struct PlannedExactPredicate {
    family: MetadataFamily,
    key: Vec<u8>,
    current: Option<CurrentValue>,
    witness: Option<ReadWitness>,
}

enum RootFencePlan {
    Install {
        value: Vec<u8>,
    },
    Assert {
        witness: ReadWitness,
    },
    Replace {
        witness: ReadWitness,
        value: Vec<u8>,
    },
}

fn enqueue_root_fence(
    atomic: &mut AtomicPlan,
    command: &MetadataCommand,
    root_plan: &RootFencePlan,
) {
    let operation = match root_plan {
        RootFencePlan::Install { value } => AtomicOp::PutIfAbsent {
            space: crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
            key: command.root_id.as_bytes().to_vec(),
            value: value.clone(),
        },
        RootFencePlan::Assert { witness } => AtomicOp::AssertUnchanged {
            space: crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
            key: command.root_id.as_bytes().to_vec(),
            witness: witness.clone(),
        },
        RootFencePlan::Replace { witness, value } => AtomicOp::CompareAndPut {
            space: crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
            key: command.root_id.as_bytes().to_vec(),
            witness: witness.clone(),
            value: value.clone(),
        },
    };
    atomic.operations.push(operation);
}

fn enqueue_recovery(atomic: &mut AtomicPlan, recovery: &RecoveryPlan) {
    atomic.operations.push(AtomicOp::CompareAndPut {
        space: crate::workspace::provider_catalog::SYSTEM_SPACE,
        key: SYSTEM_APPLIED_RECOVERY_LSN_KEY.to_vec(),
        witness: recovery.lsn_record.witness.clone(),
        value: encode_system_u64(recovery.row.recovery_lsn).to_vec(),
    });
    atomic.operations.push(AtomicOp::CompareAndPut {
        space: crate::workspace::provider_catalog::SYSTEM_SPACE,
        key: SYSTEM_RECOVERY_CHAIN_DIGEST_KEY.to_vec(),
        witness: recovery.digest_record.witness.clone(),
        value: encode_system_digest(recovery.row.chain_digest),
    });
    atomic.operations.push(AtomicOp::PutIfAbsent {
        space: crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
        key: recovery_outbox_key(recovery.row.recovery_lsn).to_vec(),
        value: recovery.header.clone(),
    });
    for (index, chunk) in recovery.chunks.iter().enumerate() {
        atomic.operations.push(AtomicOp::PutIfAbsent {
            space: crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
            key: recovery_chunk_key(recovery.row.recovery_lsn, index as u32).to_vec(),
            value: chunk.clone(),
        });
    }
}

fn enqueue_active_authority_advance(
    atomic: &mut AtomicPlan,
    authority: &ProviderRecord,
    marker: MetadataAuthorityMarker,
) -> Result<(), AgentMetadataError> {
    let next = marker
        .advance_active_write()
        .ok_or(AgentMetadataError::VersionOverflow)?;
    atomic.operations.push(AtomicOp::CompareAndPut {
        space: crate::workspace::provider_catalog::SYSTEM_SPACE,
        key: SYSTEM_METADATA_AUTHORITY_KEY.to_vec(),
        witness: authority.witness.clone(),
        value: encode_authority_marker(next),
    });
    Ok(())
}

fn enqueue_predicate_guards(atomic: &mut AtomicPlan, plan: &PredicatePlan) {
    for predicate in plan.exact.values() {
        match &predicate.witness {
            Some(witness) => {
                atomic.operations.push(AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::domain_space(predicate.family),
                    key: predicate.key.clone(),
                    witness: witness.clone(),
                });
            }
            None => {
                atomic.operations.push(AtomicOp::AssertAbsent {
                    space: crate::workspace::provider_catalog::domain_space(predicate.family),
                    key: predicate.key.clone(),
                });
            }
        }
    }
    for (family, prefix) in &plan.prefix_empty {
        atomic.operations.push(AtomicOp::AssertPrefixEmpty {
            space: crate::workspace::provider_catalog::domain_space(*family),
            prefix: prefix.clone(),
        });
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

fn required_record(
    reader: &dyn MetadataReadView,
    space: OrderedSpaceId,
    key: &[u8],
    record: &'static str,
) -> Result<ProviderRecord, AgentMetadataError> {
    reader
        .get(space, key)
        .map_err(provider_error)?
        .ok_or_else(|| AgentMetadataError::CorruptRecord {
            record,
            reason: "record is missing".to_owned(),
        })
}

fn validate_store_identity(identity: MetadataStoreIdentity) -> Result<(), AgentMetadataError> {
    validate_metadata_store_identity(identity).map_err(|error| AgentMetadataError::SchemaGate {
        reason: error.to_string(),
    })
}

fn validate_source_receipt_request(
    identity: MetadataStoreIdentity,
    migration_id: OperationId,
    owner_epoch: OwnerEpoch,
    receipt: SourceQuiesceReceipt,
) -> Result<(), AgentMetadataError> {
    if receipt.logical_shard_id != identity.logical_shard_id
        || receipt.migration_id != migration_id
        || receipt.source_authority_id != identity.authority_id
        || receipt.source_authority_generation != identity.authority_generation
        || receipt.owner_epoch != owner_epoch
        || receipt.contract_digest != identity.contract_digest
    {
        return Err(migration_admission(
            "quiesce replay does not match the durable source receipt",
        ));
    }
    Ok(())
}

fn validate_migration_target_binding(
    identity: MetadataStoreIdentity,
    binding: MetadataMigrationTargetBinding,
) -> Result<(), AgentMetadataError> {
    if binding.logical_shard_id != identity.logical_shard_id
        || binding.target_authority_id != identity.authority_id
        || binding.target_authority_generation != identity.authority_generation
        || binding.contract_digest != identity.contract_digest
        || binding
            .migration_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        || binding
            .source_authority_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
        || binding.source_authority_id == binding.target_authority_id
    {
        return Err(migration_admission(
            "migration target binding does not match the target store identity",
        ));
    }
    Ok(())
}

fn validate_authority_marker_for_identity(
    identity: MetadataStoreIdentity,
    marker: MetadataAuthorityMarker,
) -> Result<(), AgentMetadataError> {
    let matches = match marker.evidence {
        MetadataAuthorityEvidence::None => true,
        MetadataAuthorityEvidence::MigrationTargetBinding(binding) => {
            validate_migration_target_binding(identity, binding).is_ok()
        }
        MetadataAuthorityEvidence::SourceQuiesceReceipt(receipt) => {
            receipt.logical_shard_id == identity.logical_shard_id
                && receipt.source_authority_id == identity.authority_id
                && receipt.source_authority_generation == identity.authority_generation
                && receipt.contract_digest == identity.contract_digest
        }
        MetadataAuthorityEvidence::TargetActivationToken(token) => {
            token.logical_shard_id == identity.logical_shard_id
                && token.target_authority_id == identity.authority_id
                && token.target_authority_generation == identity.authority_generation
                && token.contract_digest == identity.contract_digest
        }
    };
    if matches {
        Ok(())
    } else {
        Err(AgentMetadataError::MetadataAuthorityBindingMismatch)
    }
}

#[cfg(test)]
fn validate_target_token_identity(
    identity: MetadataStoreIdentity,
    binding: MetadataMigrationTargetBinding,
    token: &TargetActivationToken,
) -> Result<(), AgentMetadataError> {
    if token.logical_shard_id != identity.logical_shard_id
        || token.migration_id != binding.migration_id
        || token.source_authority_id != binding.source_authority_id
        || token.source_authority_generation != binding.source_authority_generation
        || token.target_authority_id != identity.authority_id
        || token.target_authority_generation != identity.authority_generation
        || token.contract_digest != identity.contract_digest
        || token.migration_id.as_bytes().iter().all(|byte| *byte == 0)
        || token.source_receipt_digest.iter().all(|byte| *byte == 0)
        || token.frontier.chain_digest.iter().all(|byte| *byte == 0)
        || token.frontier.state_digest.iter().all(|byte| *byte == 0)
    {
        return Err(migration_admission(
            "target activation token does not match the target store identity",
        ));
    }
    Ok(())
}

/// Hash only provider-neutral logical metadata. `System` is excluded because
/// it carries provider-installation identity, local authority evidence, and
/// physical-owner state; its logical recovery LSN, chain, and commit version
/// are bound separately in `MetadataRecoveryFrontier`. Root fences, dedupe,
/// events, history, recovery rows, and every domain space are included.
fn logical_state_digest(reader: &dyn MetadataReadView) -> Result<[u8; 32], AgentMetadataError> {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.metadata.logical-state.v1\0");
    for space in logical_state_spaces() {
        hash_logical_state_frame(&mut hasher, 1, &logical_state_space_tag(space));
        let page = reader
            .scan(&ProviderScan {
                space,
                prefix: Vec::new(),
                start_after: None,
                delimiter: None,
                limit: 0,
            })
            .map_err(provider_error)?;
        for item in page.items {
            let ProviderScanItem::Key { key, value } = item else {
                return Err(AgentMetadataError::CorruptRecord {
                    record: "logical metadata state",
                    reason: "undelimited provider scan returned a common prefix".to_owned(),
                });
            };
            hash_logical_state_frame(&mut hasher, 2, &key);
            hash_logical_state_frame(&mut hasher, 3, &value);
        }
        hash_logical_state_frame(&mut hasher, 4, &[]);
    }
    Ok(hasher.finalize().into())
}

pub(super) fn logical_state_spaces() -> Vec<OrderedSpaceId> {
    crate::workspace::provider_catalog::logical_state_spaces()
}

pub(super) fn logical_state_space_tag(space: OrderedSpaceId) -> [u8; 2] {
    assert_ne!(
        space,
        crate::workspace::provider_catalog::SYSTEM_SPACE,
        "System is not logical migration state"
    );
    space.to_be_bytes()
}

pub(super) fn hash_logical_state_frame(hasher: &mut Sha256, tag: u8, bytes: &[u8]) {
    hasher.update([tag]);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn migration_admission(reason: impl Into<String>) -> AgentMetadataError {
    AgentMetadataError::MetadataMigrationAdmission {
        reason: reason.into(),
    }
}

fn validate_runtime_commit_bundle(
    runtime_bundle: &dyn MetadataRuntimeCommitBundleV1,
    identity: MetadataStoreIdentity,
    allow_untracked_standalone: bool,
) -> Result<MetadataCommitReceiptStateV1, AgentMetadataError> {
    let qualification = runtime_bundle.commit_receipt_qualification_v1();
    let frozen_bundle_digest = runtime_bundle.frozen_runtime_bundle_digest_v1();
    if frozen_bundle_digest.iter().all(|byte| *byte == 0) {
        return Err(AgentMetadataError::ProviderAuthorityMismatch {
            operation: "validate metadata runtime bundle",
            message: "frozen runtime bundle digest must not be all-zero".to_owned(),
        });
    }
    if qualification == MetadataCommitReceiptQualificationV1::UntrackedStandalone
        && !allow_untracked_standalone
    {
        return Err(AgentMetadataError::ProviderAuthorityMismatch {
            operation: "validate metadata commit receipt qualification",
            message: "distributed runtime requires a durable exact commit receipt".to_owned(),
        });
    }
    let state = runtime_bundle
        .load_commit_receipt_v1(identity)
        .map_err(receipt_error)?;
    match &state {
        MetadataCommitReceiptStateV1::Clean {
            store_identity,
            frozen_bundle_digest: durable_digest,
            ..
        } if *store_identity == identity && *durable_digest == frozen_bundle_digest => {}
        MetadataCommitReceiptStateV1::Pending(planned)
        | MetadataCommitReceiptStateV1::PoisonedSettled(planned)
        | MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => planned
            .validate_binding(identity, frozen_bundle_digest)
            .map_err(receipt_error)?,
        MetadataCommitReceiptStateV1::UntrackedStandalone
            if qualification == MetadataCommitReceiptQualificationV1::UntrackedStandalone
                && allow_untracked_standalone => {}
        _ => {
            return Err(AgentMetadataError::ProviderAuthorityMismatch {
                operation: "validate metadata commit receipt binding",
                message: "receipt is bound to another store or runtime bundle".to_owned(),
            });
        }
    }
    Ok(state)
}

fn dirty_receipt_source_and_plan(
    state: &MetadataCommitReceiptStateV1,
) -> Option<(MetadataCommitReceiptDirtySourceV1, PlannedMetadataCommitV1)> {
    match state {
        MetadataCommitReceiptStateV1::Pending(planned) => {
            Some((MetadataCommitReceiptDirtySourceV1::Pending, planned.clone()))
        }
        MetadataCommitReceiptStateV1::PoisonedSettled(planned) => Some((
            MetadataCommitReceiptDirtySourceV1::PoisonedSettled,
            planned.clone(),
        )),
        MetadataCommitReceiptStateV1::PoisonedUnsettled(planned) => Some((
            MetadataCommitReceiptDirtySourceV1::PoisonedUnsettled,
            planned.clone(),
        )),
        MetadataCommitReceiptStateV1::Clean { .. }
        | MetadataCommitReceiptStateV1::UntrackedStandalone => None,
    }
}

fn validate_create_receipt_preflight(
    state: &MetadataCommitReceiptStateV1,
    recovery_intent: CreateRecoveryIntentV1,
) -> Result<(), AgentMetadataError> {
    match (recovery_intent, state) {
        (_, MetadataCommitReceiptStateV1::UntrackedStandalone)
        | (
            CreateRecoveryIntentV1::Fresh,
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Absent,
                ..
            },
        )
        | (CreateRecoveryIntentV1::ReconcilePrepared, MetadataCommitReceiptStateV1::Clean { .. }) => {
            Ok(())
        }
        _ => Err(AgentMetadataError::ProviderAuthorityMismatch {
            operation: "validate metadata create receipt state",
            message: "create intent does not match the durable commit receipt state".to_owned(),
        }),
    }
}

fn validate_reopen_receipt_preflight(
    state: &MetadataCommitReceiptStateV1,
) -> Result<(), AgentMetadataError> {
    match state {
        MetadataCommitReceiptStateV1::UntrackedStandalone
        | MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Exact(_),
            ..
        } => Ok(()),
        MetadataCommitReceiptStateV1::Clean {
            frontier: MetadataFrontierPointV1::Absent,
            ..
        } => Err(AgentMetadataError::ProviderAuthorityMismatch {
            operation: "validate metadata reopen receipt state",
            message: "reopen requires an exact durable provider frontier".to_owned(),
        }),
        MetadataCommitReceiptStateV1::Pending(_)
        | MetadataCommitReceiptStateV1::PoisonedSettled(_)
        | MetadataCommitReceiptStateV1::PoisonedUnsettled(_) => {
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        }
    }
}

fn validate_provider_contract(
    provider: &dyn MetadataProvider,
    logical_shard_id: LogicalShardId,
) -> Result<(), AgentMetadataError> {
    provider.validate_runtime().map_err(provider_error)?;
    if provider.logical_shard_id() != logical_shard_id {
        return Err(AgentMetadataError::SchemaGate {
            reason: "logical shard identity does not match requested provider".to_owned(),
        });
    }
    validate_provider_capabilities(provider.capabilities())
}

pub fn canonical_provider_schema_v1() -> ProviderSchemaV1 {
    ProviderSchemaV1::new(workspace_metadata_contract_digest(), all_ordered_spaces())
        .expect("the frozen workspace provider catalog is nonempty and unique")
}

fn validate_provider_offer(
    provider: &dyn MetadataProvider,
    offered: ProviderCapabilities,
) -> Result<(), AgentMetadataError> {
    if provider.capabilities() != offered {
        return Err(AgentMetadataError::SchemaGate {
            reason: "opened metadata provider differs from its pre-open contract offer".to_owned(),
        });
    }
    Ok(())
}

fn validate_provider_capabilities(
    capabilities: ProviderCapabilities,
) -> Result<(), AgentMetadataError> {
    let schema = canonical_provider_schema_v1();
    let offer = crate::provider::v1::ProviderContractOfferV1 { capabilities };
    let report = crate::provider::admission::admit_provider_offer_v1(&schema, &offer);
    if !report.is_qualified() {
        return Err(AgentMetadataError::SchemaGate {
            reason: "metadata provider does not prove the complete NoKV command surface".to_owned(),
        });
    }
    Ok(())
}

fn recovery_state_from(reader: &dyn MetadataReadView) -> Result<RecoveryState, AgentMetadataError> {
    let lsn = required_record(
        reader,
        crate::workspace::provider_catalog::SYSTEM_SPACE,
        SYSTEM_APPLIED_RECOVERY_LSN_KEY,
        "System(applied_recovery_lsn)",
    )?;
    let digest = required_record(
        reader,
        crate::workspace::provider_catalog::SYSTEM_SPACE,
        SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
        "System(recovery_chain_digest)",
    )?;
    Ok(RecoveryState {
        applied_recovery_lsn: decode_system_u64(&lsn.value, "System(applied_recovery_lsn)")?,
        chain_digest: decode_system_digest(&digest.value, "System(recovery_chain_digest)")?,
    })
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

#[cfg(feature = "metadata-read-stats")]
fn diagnostics_snapshot_to_read_stats(
    snapshot: ProviderDiagnosticsSnapshotV1,
) -> MetadataReadStats {
    MetadataReadStats {
        provider_cache_hits: snapshot.cache_hits,
        provider_cache_misses: snapshot.cache_misses,
        provider_full_read_operations: snapshot.full_read_operations,
        provider_full_read_bytes: snapshot.full_read_bytes,
        provider_point_full_read_operations: snapshot.point_full_read_operations,
        provider_scan_full_read_operations: snapshot.scan_full_read_operations,
        provider_internal_full_read_operations: snapshot.internal_full_read_operations,
        provider_partial_read_cache_hits: snapshot.partial_read_cache_hits,
        provider_partial_read_cache_misses: snapshot.partial_read_cache_misses,
        ..MetadataReadStats::default()
    }
}

fn encode_system_u64(value: u64) -> [u8; 9] {
    let mut encoded = [0; 9];
    encoded[0] = SYSTEM_VALUE_FORMAT_VERSION;
    encoded[1..].copy_from_slice(&value.to_be_bytes());
    encoded
}

pub(super) fn decode_system_u64(
    value: &[u8],
    record: &'static str,
) -> Result<u64, AgentMetadataError> {
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

pub(super) fn decode_system_digest(
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

fn provider_error(error: ProviderError) -> AgentMetadataError {
    let operation = error.operation().as_str();
    match error.kind() {
        ProviderErrorKind::Backend => AgentMetadataError::Backend {
            operation,
            message: "metadata provider operation failed".to_owned(),
        },
        ProviderErrorKind::InvalidPlan => AgentMetadataError::Backend {
            operation: "validate provider operation",
            message: "metadata provider rejected the operation plan".to_owned(),
        },
        ProviderErrorKind::SchemaGate => AgentMetadataError::SchemaGate {
            reason: "metadata provider rejected the canonical workspace schema".to_owned(),
        },
        ProviderErrorKind::OpenExecutionRejected => AgentMetadataError::SchemaGate {
            reason: "metadata provider rejected the engine open execution".to_owned(),
        },
        ProviderErrorKind::UnknownCommitSettled | ProviderErrorKind::UnknownCommitUnsettled => {
            AgentMetadataError::CommitOutcomeUnknown
        }
        ProviderErrorKind::TransactionTooLarge => {
            let limit = error
                .limit()
                .expect("transaction-too-large errors carry a stable limit");
            AgentMetadataError::TransactionTooLarge {
                affected_bytes: limit.affected_bytes,
                max_bytes: limit.max_bytes,
            }
        }
        ProviderErrorKind::Unavailable => AgentMetadataError::ProviderUnavailable {
            operation,
            message: "metadata provider is unavailable".to_owned(),
        },
        ProviderErrorKind::AuthorityMismatch => AgentMetadataError::ProviderAuthorityMismatch {
            operation,
            message: "metadata provider authority changed".to_owned(),
        },
    }
}

fn receipt_error(error: MetadataCommitReceiptErrorV1) -> AgentMetadataError {
    match error {
        MetadataCommitReceiptErrorV1::Poisoned => AgentMetadataError::CommitOutcomeUnknown,
        MetadataCommitReceiptErrorV1::Unavailable
        | MetadataCommitReceiptErrorV1::InvalidBinding => {
            AgentMetadataError::ProviderAuthorityMismatch {
                operation: "metadata commit receipt",
                message: error.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::super::query_records::{ChangeEventKind, ChangeEventRecord, TypedProjection};
    use super::*;
    use crate::built_in_holt::{
        acquire_existing_file_store_reservation_v1, HoltExistingStoreReservation,
        HoltRuntimeGuardError, HoltStoreObjectIdentity,
    };
    use crate::provider::v1::{ProviderScanPage, ProviderScanStats};

    #[derive(Clone, Copy)]
    enum BarrierCommitResult {
        Committed,
        BackendError,
        UnknownUnsettled,
    }

    struct BarrierCommitTransaction {
        barrier: Arc<Barrier>,
        commit_calls: Arc<std::sync::atomic::AtomicUsize>,
        read_calls: Arc<std::sync::atomic::AtomicUsize>,
        result: BarrierCommitResult,
    }

    impl MetadataReadView for BarrierCommitTransaction {
        fn get(
            &self,
            _space: OrderedSpaceId,
            _key: &[u8],
        ) -> Result<Option<ProviderRecord>, ProviderError> {
            self.read_calls.fetch_add(1, Ordering::AcqRel);
            Ok(None)
        }

        fn scan(&self, _request: &ProviderScan) -> Result<ProviderScanPage, ProviderError> {
            Ok(ProviderScanPage {
                items: Vec::new(),
                stats: ProviderScanStats {
                    visited: 0,
                    returned: 0,
                    common_prefixes: 0,
                    restarts: 0,
                },
            })
        }
    }

    impl MetadataTransaction for BarrierCommitTransaction {
        fn prefix_is_empty(
            &self,
            _space: OrderedSpaceId,
            _prefix: &[u8],
        ) -> Result<bool, ProviderError> {
            self.read_calls.fetch_add(1, Ordering::AcqRel);
            Ok(true)
        }

        fn commit(
            self: Box<Self>,
            _plan: AtomicPlan,
        ) -> Result<AtomicCommitOutcome, ProviderError> {
            self.commit_calls.fetch_add(1, Ordering::AcqRel);
            self.barrier.wait();
            self.barrier.wait();
            match self.result {
                BarrierCommitResult::Committed => Ok(AtomicCommitOutcome::Committed),
                BarrierCommitResult::BackendError => Err(ProviderError::backend(
                    ProviderOperationV1::Commit,
                    "injected backend response",
                )),
                BarrierCommitResult::UnknownUnsettled => {
                    Err(ProviderError::unknown_commit_unsettled())
                }
            }
        }
    }

    struct RecordingRuntimeBundle {
        factory: Mutex<Option<HoltProviderFactory>>,
        identity: MetadataStoreIdentity,
        state: Arc<Mutex<MetadataCommitReceiptStateV1>>,
        persist_count: Mutex<usize>,
        resolve_count: Mutex<usize>,
        contract_offer_calls: AtomicUsize,
        create_calls: AtomicUsize,
        reopen_calls: AtomicUsize,
        recovery_open_calls: AtomicUsize,
        reject_persist: AtomicBool,
        reject_resolve: AtomicBool,
        swap_persist_outcome: AtomicBool,
        runtime_poisoned: AtomicBool,
    }

    impl RecordingRuntimeBundle {
        const FROZEN_DIGEST: [u8; 32] = [0x91; 32];

        fn memory(identity: MetadataStoreIdentity) -> Arc<Self> {
            let bundle = Self::empty_with_state(
                identity,
                Arc::new(Mutex::new(Self::clean(
                    identity,
                    MetadataFrontierPointV1::Absent,
                ))),
            );
            *bundle.factory.lock().unwrap() = Some(HoltProviderFactory::memory());
            bundle
        }

        fn empty_with_state(
            identity: MetadataStoreIdentity,
            state: Arc<Mutex<MetadataCommitReceiptStateV1>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                factory: Mutex::new(None),
                identity,
                state,
                persist_count: Mutex::new(0),
                resolve_count: Mutex::new(0),
                contract_offer_calls: AtomicUsize::new(0),
                create_calls: AtomicUsize::new(0),
                reopen_calls: AtomicUsize::new(0),
                recovery_open_calls: AtomicUsize::new(0),
                reject_persist: AtomicBool::new(false),
                reject_resolve: AtomicBool::new(false),
                swap_persist_outcome: AtomicBool::new(false),
                runtime_poisoned: AtomicBool::new(false),
            })
        }

        fn file(path: &Path, identity: MetadataStoreIdentity) -> Arc<Self> {
            Self::file_with_state(
                path,
                identity,
                Arc::new(Mutex::new(Self::clean(
                    identity,
                    MetadataFrontierPointV1::Absent,
                ))),
            )
        }

        fn file_with_state(
            path: &Path,
            identity: MetadataStoreIdentity,
            state: Arc<Mutex<MetadataCommitReceiptStateV1>>,
        ) -> Arc<Self> {
            let bundle = Self::empty_with_state(identity, state);
            let runtime_guard: Arc<dyn HoltRuntimeGuard> = bundle.clone();
            *bundle.factory.lock().unwrap() = Some(HoltProviderFactory::file(path, runtime_guard));
            bundle
        }

        fn reserved_existing_with_state(
            reservation: HoltExistingStoreReservation,
            identity: MetadataStoreIdentity,
            state: Arc<Mutex<MetadataCommitReceiptStateV1>>,
        ) -> Arc<Self> {
            let bundle = Self::empty_with_state(identity, state);
            let runtime_guard: Arc<dyn HoltRuntimeGuard> = bundle.clone();
            *bundle.factory.lock().unwrap() = Some(HoltProviderFactory::reserved_existing(
                reservation,
                runtime_guard,
            ));
            bundle
        }

        fn clean(
            identity: MetadataStoreIdentity,
            frontier: MetadataFrontierPointV1,
        ) -> MetadataCommitReceiptStateV1 {
            MetadataCommitReceiptStateV1::Clean {
                store_identity: identity,
                frozen_bundle_digest: Self::FROZEN_DIGEST,
                frontier,
            }
        }

        fn persist_count(&self) -> usize {
            *self.persist_count.lock().unwrap()
        }

        fn resolve_count(&self) -> usize {
            *self.resolve_count.lock().unwrap()
        }

        fn reject_persist(&self) {
            self.reject_persist.store(true, Ordering::Release);
        }

        fn swap_next_persist_outcome(&self) {
            self.swap_persist_outcome.store(true, Ordering::Release);
        }

        fn reject_next_resolve(&self) {
            self.reject_resolve.store(true, Ordering::Release);
        }

        fn provider_call_counts(&self) -> (usize, usize, usize, usize) {
            (
                self.contract_offer_calls.load(Ordering::Acquire),
                self.create_calls.load(Ordering::Acquire),
                self.reopen_calls.load(Ordering::Acquire),
                self.recovery_open_calls.load(Ordering::Acquire),
            )
        }
    }

    impl MetadataProviderFactoryV1 for RecordingRuntimeBundle {
        fn contract_offer(
            &self,
            schema: &ProviderSchemaV1,
        ) -> Result<ProviderContractOfferV1, ProviderError> {
            self.contract_offer_calls.fetch_add(1, Ordering::AcqRel);
            self.factory
                .lock()
                .unwrap()
                .as_ref()
                .expect("test runtime factory is installed")
                .contract_offer(schema)
        }

        fn create(
            &self,
            request: &ProviderCreateRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.create_calls.fetch_add(1, Ordering::AcqRel);
            self.factory
                .lock()
                .unwrap()
                .as_ref()
                .expect("test runtime factory is installed")
                .create(request)
        }

        fn reopen(
            &self,
            request: &ProviderReopenRequestV1,
        ) -> Result<Arc<dyn MetadataProvider>, ProviderError> {
            self.reopen_calls.fetch_add(1, Ordering::AcqRel);
            self.factory
                .lock()
                .unwrap()
                .as_ref()
                .expect("test runtime factory is installed")
                .reopen(request)
        }
    }

    impl MetadataCommitRecoveryFenceFactoryV1 for RecordingRuntimeBundle {
        fn old_dispatch_exclusion_installation_v1(
            &self,
        ) -> MetadataOldDispatchExclusionInstallationV1 {
            self.factory
                .lock()
                .unwrap()
                .as_ref()
                .expect("test runtime factory is installed")
                .old_dispatch_exclusion_installation_v1()
        }

        fn reopen_pending_with_old_dispatch_excluded_v1(
            &self,
            command: MetadataPendingRecoveryOpenCommandV1,
        ) -> MetadataPendingRecoveryOpenOutcomeV1 {
            self.recovery_open_calls.fetch_add(1, Ordering::AcqRel);
            self.factory
                .lock()
                .unwrap()
                .as_ref()
                .expect("test runtime factory is installed")
                .reopen_pending_with_old_dispatch_excluded_v1(command)
        }
    }

    impl MetadataCommitReceiptStoreV1 for RecordingRuntimeBundle {
        fn commit_receipt_qualification_v1(&self) -> MetadataCommitReceiptQualificationV1 {
            MetadataCommitReceiptQualificationV1::Durable
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; 32] {
            Self::FROZEN_DIGEST
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: MetadataStoreIdentity,
        ) -> Result<MetadataCommitReceiptStateV1, MetadataCommitReceiptErrorV1> {
            if store_identity != self.identity {
                return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
            }
            Ok(self.state.lock().unwrap().clone())
        }

        fn persist_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptPersistCommandV1,
        ) -> MetadataCommitReceiptPersistOutcomeV1 {
            if self.reject_persist.load(Ordering::Acquire) {
                return command.reject_before_execution(
                    MetadataCommitReceiptPersistNotDispatchedV1::Unavailable,
                );
            }
            let command = command.claim_execution();
            let planned_for_swapped_outcome = command.planned().clone();
            let result = (|| {
                let planned = command.planned();
                planned
                    .validate_binding(self.identity, Self::FROZEN_DIGEST)
                    .map_err(|_| MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect)?;
                let mut state = self.state.lock().unwrap();
                let MetadataCommitReceiptStateV1::Clean { frontier, .. } = &*state else {
                    return Err(MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect);
                };
                if *frontier != planned.prior() {
                    return Err(MetadataCommitReceiptPersistErrorV1::InvalidBindingBeforeEffect);
                }
                *state = MetadataCommitReceiptStateV1::Pending(planned.clone());
                *self.persist_count.lock().unwrap() += 1;
                Ok(())
            })();
            let outcome = command.complete(match result {
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
            });
            if self.swap_persist_outcome.swap(false, Ordering::AcqRel) {
                let authority = MetadataCommitEngineMintAuthorityV1::for_test();
                let (foreign, _) = MetadataCommitReceiptPersistCommandV1::mint(
                    &authority,
                    &planned_for_swapped_outcome,
                );
                foreign
                    .claim_execution()
                    .complete(MetadataCommitReceiptPersistBackendResultV1::Persisted)
            } else {
                outcome
            }
        }

        fn resolve_pending_commit_v1(
            &self,
            command: MetadataCommitReceiptResolveCommandV1,
        ) -> MetadataCommitReceiptResolveOutcomeV1 {
            if self.reject_resolve.swap(false, Ordering::AcqRel) {
                return command.reject_before_execution(
                    MetadataCommitReceiptMutationNotDispatchedV1::Unavailable,
                );
            }
            let command = command.claim_execution();
            let result = (|| {
                let planned = command.planned();
                let mut state = self.state.lock().unwrap();
                let resolution = command.resolution();
                if !resolution.source().matches_state(&state, planned) {
                    return Err(MetadataCommitReceiptErrorV1::InvalidBinding);
                }
                let frontier = match resolution.basis() {
                    MetadataCommitResolutionBasisV1::ExactNextApplied
                        if resolution.applied_exact_next() == Some(planned.exact_next())
                            && resolution.not_applied_exact_prior().is_none() =>
                    {
                        MetadataFrontierPointV1::Exact(planned.exact_next())
                    }
                    MetadataCommitResolutionBasisV1::ExactPriorNotAppliedSettled
                        if resolution.source()
                            == MetadataCommitReceiptDirtySourceV1::PoisonedSettled
                            && resolution.applied_exact_next().is_none()
                            && resolution.not_applied_exact_prior() == Some(planned.prior()) =>
                    {
                        planned.prior()
                    }
                    _ => return Err(MetadataCommitReceiptErrorV1::InvalidBinding),
                };
                *state = Self::clean(self.identity, frontier);
                *self.resolve_count.lock().unwrap() += 1;
                Ok(())
            })();
            command.complete(receipt_mutation_backend_result(result))
        }

        fn poison_commit_receipt_v1(
            &self,
            command: MetadataCommitReceiptPoisonCommandV1,
        ) -> MetadataCommitReceiptPoisonOutcomeV1 {
            let command = command.claim_execution();
            let result = {
                let planned = command.planned();
                let reason = command.reason();
                let mut state = self.state.lock().unwrap();
                match &*state {
                    MetadataCommitReceiptStateV1::Pending(durable) if durable == planned => {
                        *state = match reason {
                            MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome => {
                                MetadataCommitReceiptStateV1::PoisonedSettled(planned.clone())
                            }
                            MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome => {
                                MetadataCommitReceiptStateV1::PoisonedUnsettled(planned.clone())
                            }
                        };
                        Ok(())
                    }
                    MetadataCommitReceiptStateV1::PoisonedSettled(durable)
                        if durable == planned
                            && reason
                                == MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome =>
                    {
                        Ok(())
                    }
                    MetadataCommitReceiptStateV1::PoisonedUnsettled(durable)
                        if durable == planned
                            && reason
                                == MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome =>
                    {
                        Ok(())
                    }
                    _ => Err(MetadataCommitReceiptErrorV1::InvalidBinding),
                }
            };
            command.complete(receipt_mutation_backend_result(result))
        }
    }

    fn receipt_mutation_backend_result(
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

    impl HoltRuntimeGuard for RecordingRuntimeBundle {
        fn bind_store(
            &self,
            _identity: &HoltStoreObjectIdentity,
        ) -> Result<(), HoltRuntimeGuardError> {
            if self.runtime_poisoned.load(Ordering::Acquire) {
                Err(HoltRuntimeGuardError::Poisoned)
            } else {
                Ok(())
            }
        }

        fn validate_runtime(&self) -> Result<(), HoltRuntimeGuardError> {
            if self.runtime_poisoned.load(Ordering::Acquire) {
                Err(HoltRuntimeGuardError::Poisoned)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {
            self.runtime_poisoned.store(true, Ordering::Release);
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

    fn single_shard_install() -> RootFenceAction {
        RootFenceAction::Install {
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
        }
    }

    fn explicit_identity(shard_fill: u8, authority_fill: u8) -> MetadataStoreIdentity {
        MetadataStoreIdentity {
            logical_shard_id: shard(shard_fill),
            authority_id: nokv_types::MetadataAuthorityId::from_bytes([authority_fill; 16]),
            authority_generation: nokv_types::MetadataAuthorityGeneration::new(3).unwrap(),
            consistency_domain_id: nokv_types::ConsistencyDomainId::from_bytes([0x44; 16]),
            profile_fingerprint: [0x55; 32],
            contract_digest: workspace_metadata_contract_digest(),
        }
    }

    fn persisted_origin_for_test(
        authority: &MetadataCommitEngineMintAuthorityV1,
        planned: &PlannedMetadataCommitV1,
    ) -> MetadataCommitLiveResolutionOriginV1 {
        let (command, witness) = MetadataCommitReceiptPersistCommandV1::mint(authority, planned);
        command
            .claim_execution()
            .complete(MetadataCommitReceiptPersistBackendResultV1::Persisted)
            .into_live_resolution_origin_for(witness)
            .unwrap()
    }

    fn poisoned_origin_for_test(
        authority: &MetadataCommitEngineMintAuthorityV1,
        planned: &PlannedMetadataCommitV1,
        reason: MetadataCommitReceiptPoisonReasonV1,
    ) -> MetadataCommitLiveResolutionOriginV1 {
        let origin = persisted_origin_for_test(authority, planned);
        let (command, witness) =
            MetadataCommitReceiptPoisonCommandV1::mint(authority, origin, reason).unwrap();
        command
            .claim_execution()
            .complete(MetadataCommitReceiptMutationBackendResultV1::Completed)
            .into_live_resolution_origin_for(witness)
            .unwrap()
    }

    fn open_explicit_memory(
        identity: MetadataStoreIdentity,
    ) -> Result<AgentMetadataStore, AgentMetadataError> {
        AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            RecordingRuntimeBundle::memory(identity),
            identity,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
    }

    fn scoped_key(root: RootId, suffix: &[u8]) -> Vec<u8> {
        [root.as_bytes().as_slice(), suffix].concat()
    }

    fn raw_put(store: &AgentMetadataStore, space: OrderedSpaceId, key: &[u8], value: &[u8]) {
        let transaction = store.provider.begin_write().unwrap();
        let plan = AtomicPlan {
            operations: vec![AtomicOp::Put {
                space,
                key: key.to_vec(),
                value: value.to_vec(),
            }],
        };
        assert_eq!(
            transaction.commit(plan).unwrap(),
            AtomicCommitOutcome::Committed
        );
    }

    fn raw_delete(store: &AgentMetadataStore, space: OrderedSpaceId, key: &[u8]) {
        let transaction = store.provider.begin_write().unwrap();
        let plan = AtomicPlan {
            operations: vec![AtomicOp::Delete {
                space,
                key: key.to_vec(),
            }],
        };
        assert_eq!(
            transaction.commit(plan).unwrap(),
            AtomicCommitOutcome::Committed
        );
    }

    fn digest_hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
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

    fn current_target_activation_token(store: &AgentMetadataStore) -> TargetActivationToken {
        let transaction = store.provider.begin_write().unwrap();
        let recovery_lsn = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            "System(applied_recovery_lsn)",
        )
        .unwrap();
        let chain_digest = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
            "System(recovery_chain_digest)",
        )
        .unwrap();
        let commit_clock = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )
        .unwrap();
        let receipt = SourceQuiesceReceipt {
            logical_shard_id: store.identity.logical_shard_id,
            migration_id: OperationId::from_bytes([0x33; 16]),
            source_authority_id: nokv_types::MetadataAuthorityId::from_bytes([0x44; 16]),
            source_authority_generation: nokv_types::MetadataAuthorityGeneration::new(1).unwrap(),
            owner_epoch: epoch(1),
            frontier: MetadataRecoveryFrontier {
                recovery_lsn: decode_system_u64(
                    &recovery_lsn.value,
                    "System(applied_recovery_lsn)",
                )
                .unwrap(),
                chain_digest: decode_system_digest(
                    &chain_digest.value,
                    "System(recovery_chain_digest)",
                )
                .unwrap(),
                commit_version: CommitVersion::new(
                    decode_system_u64(&commit_clock.value, "System(commit_clock)").unwrap(),
                )
                .unwrap(),
                state_digest: logical_state_digest(transaction.as_ref()).unwrap(),
            },
            contract_digest: store.identity.contract_digest,
        };
        TargetActivationToken::for_cutover(
            &receipt,
            store.identity.authority_id,
            store.identity.authority_generation,
        )
    }

    fn migration_target_binding(identity: MetadataStoreIdentity) -> MetadataMigrationTargetBinding {
        MetadataMigrationTargetBinding {
            logical_shard_id: identity.logical_shard_id,
            migration_id: OperationId::from_bytes([0x33; 16]),
            source_authority_id: nokv_types::MetadataAuthorityId::from_bytes([0x44; 16]),
            source_authority_generation: nokv_types::MetadataAuthorityGeneration::new(1).unwrap(),
            target_authority_id: identity.authority_id,
            target_authority_generation: identity.authority_generation,
            contract_digest: identity.contract_digest,
        }
    }

    fn open_migration_target_memory(
        identity: MetadataStoreIdentity,
        binding: MetadataMigrationTargetBinding,
    ) -> Result<AgentMetadataStore, AgentMetadataError> {
        validate_migration_target_binding(identity, binding)?;
        AgentMetadataStore::open_memory_with_marker(
            RecordingRuntimeBundle::memory(identity),
            identity,
            MetadataAuthorityMarker {
                evidence: MetadataAuthorityEvidence::MigrationTargetBinding(binding),
                ..MetadataAuthorityMarker::for_identity(
                    identity,
                    MetadataAuthorityState::MigrationTarget,
                )
            },
        )
    }

    fn ready_file_store(path: &std::path::Path) -> AgentMetadataStore {
        let store = AgentMetadataStore::create_file(path, shard(1)).unwrap();
        make_store_ready(store)
    }

    #[cfg(unix)]
    fn held_holt_store_identity(path: &Path) -> HoltStoreObjectIdentity {
        use std::os::unix::fs::MetadataExt as _;

        let canonical_locator = std::fs::canonicalize(path).unwrap();
        let directory = std::fs::symlink_metadata(&canonical_locator).unwrap();
        let lock = std::fs::symlink_metadata(canonical_locator.join("store.lock")).unwrap();
        HoltStoreObjectIdentity::from_parts(
            canonical_locator,
            directory.dev(),
            directory.ino(),
            lock.dev(),
            lock.ino(),
        )
    }

    #[cfg(unix)]
    fn leave_pending_prior_at_path(
        path: &Path,
        identity: MetadataStoreIdentity,
        request_id: RequestId,
    ) -> (
        Arc<Mutex<MetadataCommitReceiptStateV1>>,
        PlannedMetadataCommitV1,
    ) {
        let runtime_bundle = RecordingRuntimeBundle::file(path, identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let command = create_command(
            &store,
            request_id,
            scoped_key(root(2), b"dirty-prior-recovery"),
            b"value",
        );
        runtime_bundle.swap_next_persist_outcome();
        assert_eq!(
            store.execute(&command),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        );
        let planned = match runtime_bundle.state.lock().unwrap().clone() {
            MetadataCommitReceiptStateV1::Pending(planned) => planned,
            state => panic!("expected pending receipt, found {state:?}"),
        };
        let state = Arc::clone(&runtime_bundle.state);
        drop(store);
        drop(runtime_bundle);
        (state, planned)
    }

    #[cfg(unix)]
    fn leave_pending_exact_next_at_path(
        path: &Path,
        identity: MetadataStoreIdentity,
        request_id: RequestId,
    ) -> (
        Arc<Mutex<MetadataCommitReceiptStateV1>>,
        PlannedMetadataCommitV1,
    ) {
        let runtime_bundle = RecordingRuntimeBundle::file(path, identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let command = create_command(
            &store,
            request_id,
            scoped_key(root(2), b"dirty-next-recovery"),
            b"value",
        );
        runtime_bundle.reject_next_resolve();
        assert_eq!(
            store.execute(&command),
            Err(AgentMetadataError::CommitOutcomeUnknown)
        );
        let planned = match runtime_bundle.state.lock().unwrap().clone() {
            MetadataCommitReceiptStateV1::Pending(planned) => planned,
            state => panic!("expected pending receipt, found {state:?}"),
        };
        let state = Arc::clone(&runtime_bundle.state);
        drop(store);
        drop(runtime_bundle);
        (state, planned)
    }

    fn make_store_ready(store: AgentMetadataStore) -> AgentMetadataStore {
        store.advance_owner_epoch(None, epoch(1)).unwrap();
        let install = base_command(&store, request(1), single_shard_install()).seal();
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
    fn exact_command_replay_does_not_advance_the_durable_receipt() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let identity = explicit_identity(1, 2);
        let runtime_bundle = RecordingRuntimeBundle::file(&path, identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let command = create_command(
            &store,
            request(3),
            scoped_key(root(2), b"ack-replay"),
            b"value",
        );
        let first = store.execute(&command).unwrap();
        assert!(!first.replayed);
        let receipt = runtime_bundle.state.lock().unwrap().clone();
        let persist_count = runtime_bundle.persist_count();
        let resolve_count = runtime_bundle.resolve_count();

        let replay = store.execute(&command).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);
        assert_eq!(*runtime_bundle.state.lock().unwrap(), receipt);
        assert_eq!(runtime_bundle.persist_count(), persist_count);
        assert_eq!(runtime_bundle.resolve_count(), resolve_count);
    }

    #[test]
    fn diagnostic_view_holds_the_acknowledgement_gate_for_its_full_lifetime() {
        let identity = explicit_identity(1, 2);
        let runtime_bundle = RecordingRuntimeBundle::memory(identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let command = create_command(
            &store,
            request(18),
            scoped_key(root(2), b"diagnostic-view-gate"),
            b"value",
        );
        let diagnostic = store
            .begin_diagnostic_read(&[ReadScope {
                space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                prefix: Vec::new(),
            }])
            .unwrap();
        let persist_before = runtime_bundle.persist_count();
        let writer_store = store.clone();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let writer = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = writer_store.execute(&command);
            finished_tx.send(result).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(matches!(
            finished_rx.recv_timeout(Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ));
        assert_eq!(runtime_bundle.persist_count(), persist_before);

        drop(diagnostic);
        assert!(finished_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .is_ok());
        writer.join().unwrap();
        assert_eq!(runtime_bundle.persist_count(), persist_before + 1);
    }

    #[test]
    fn pending_persist_failure_precedes_the_provider_effect() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let identity = explicit_identity(1, 2);
        let runtime_bundle = RecordingRuntimeBundle::file(&path, identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let command = create_command(
            &store,
            request(3),
            scoped_key(root(2), b"blocked-before-provider-effect"),
            b"must-not-apply",
        );
        runtime_bundle.reject_persist();

        assert!(matches!(
            store.execute(&command),
            Err(AgentMetadataError::ProviderUnavailable { .. })
        ));
        assert!(store.current_read_version().is_ok());
        assert!(matches!(
            *runtime_bundle.state.lock().unwrap(),
            MetadataCommitReceiptStateV1::Clean { .. }
        ));
    }

    #[test]
    fn foreign_persist_outcome_after_pending_requires_recovery_and_fail_stops_clones() {
        let identity = explicit_identity(1, 2);
        let runtime_bundle = RecordingRuntimeBundle::memory(identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let clone = store.clone();
        let command = create_command(
            &store,
            request(19),
            scoped_key(root(2), b"foreign-persist-outcome"),
            b"value",
        );
        runtime_bundle.swap_next_persist_outcome();

        assert_eq!(
            store.execute(&command),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        );
        assert!(matches!(
            &*runtime_bundle.state.lock().unwrap(),
            MetadataCommitReceiptStateV1::Pending(_)
        ));
        assert!(store.current_read_version().is_err());
        assert!(clone.current_read_version().is_err());

        let calls_before_recovery_attempts = runtime_bundle.provider_call_counts();
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert!(matches!(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::ReconcilePrepared,
                MetadataStoreCreateModeV1::Active,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert_eq!(
            runtime_bundle.provider_call_counts(),
            calls_before_recovery_attempts,
            "unsupported dirty recovery must not call ordinary or typed provider open"
        );
    }

    #[cfg(unix)]
    #[test]
    fn pending_and_unsettled_exact_prior_stay_dirty_under_reserved_exclusion() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let identity = explicit_identity(1, 2);
        let (state, planned) = leave_pending_prior_at_path(&path, identity, request(21));

        let first_reservation =
            acquire_existing_file_store_reservation_v1(held_holt_store_identity(&path)).unwrap();
        let pending_recovery = RecordingRuntimeBundle::reserved_existing_with_state(
            first_reservation,
            identity,
            Arc::clone(&state),
        );
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                pending_recovery.clone(),
                identity,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert_eq!(pending_recovery.provider_call_counts(), (0, 0, 0, 1));
        assert!(matches!(
            &*state.lock().unwrap(),
            MetadataCommitReceiptStateV1::Pending(durable) if durable == &planned
        ));
        drop(pending_recovery);

        *state.lock().unwrap() = MetadataCommitReceiptStateV1::PoisonedUnsettled(planned.clone());
        let second_reservation =
            acquire_existing_file_store_reservation_v1(held_holt_store_identity(&path)).unwrap();
        let unsettled_recovery = RecordingRuntimeBundle::reserved_existing_with_state(
            second_reservation,
            identity,
            Arc::clone(&state),
        );
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                unsettled_recovery.clone(),
                identity,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert_eq!(unsettled_recovery.provider_call_counts(), (0, 0, 0, 1));
        assert!(matches!(
            &*state.lock().unwrap(),
            MetadataCommitReceiptStateV1::PoisonedUnsettled(durable) if durable == &planned
        ));
    }

    #[cfg(unix)]
    #[test]
    fn settled_exact_prior_closes_but_only_a_new_allocation_can_serve() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let identity = explicit_identity(1, 2);
        let (state, planned) = leave_pending_prior_at_path(&path, identity, request(22));
        *state.lock().unwrap() = MetadataCommitReceiptStateV1::PoisonedSettled(planned.clone());

        let reservation =
            acquire_existing_file_store_reservation_v1(held_holt_store_identity(&path)).unwrap();
        let recovery_bundle = RecordingRuntimeBundle::reserved_existing_with_state(
            reservation,
            identity,
            Arc::clone(&state),
        );
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                recovery_bundle.clone(),
                identity,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert_eq!(recovery_bundle.provider_call_counts(), (0, 0, 0, 1));
        assert!(matches!(
            &*state.lock().unwrap(),
            MetadataCommitReceiptStateV1::Clean { frontier, .. } if *frontier == planned.prior()
        ));
        drop(recovery_bundle);

        let serving_bundle =
            RecordingRuntimeBundle::file_with_state(&path, identity, Arc::clone(&state));
        let serving =
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(serving_bundle, identity)
                .unwrap();
        assert_eq!(
            serving.current_read_version().unwrap().get(),
            planned.prior().exact().unwrap().commit_version.get()
        );
    }

    #[cfg(unix)]
    #[test]
    fn exact_next_closes_but_dirty_recovery_allocation_never_serves() {
        let temporary = tempdir().unwrap();
        let path = temporary.path().join("metadata");
        let identity = explicit_identity(1, 2);
        let (state, planned) = leave_pending_exact_next_at_path(&path, identity, request(23));

        let reservation =
            acquire_existing_file_store_reservation_v1(held_holt_store_identity(&path)).unwrap();
        let recovery_bundle = RecordingRuntimeBundle::reserved_existing_with_state(
            reservation,
            identity,
            Arc::clone(&state),
        );
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                recovery_bundle.clone(),
                identity,
            ),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        ));
        assert_eq!(recovery_bundle.provider_call_counts(), (0, 0, 0, 1));
        assert!(matches!(
            &*state.lock().unwrap(),
            MetadataCommitReceiptStateV1::Clean {
                frontier: MetadataFrontierPointV1::Exact(frontier),
                ..
            } if *frontier == planned.exact_next()
        ));
        drop(recovery_bundle);

        let serving_bundle =
            RecordingRuntimeBundle::file_with_state(&path, identity, Arc::clone(&state));
        let serving =
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(serving_bundle, identity)
                .unwrap();
        assert_eq!(
            serving.current_read_version().unwrap().get(),
            planned.exact_next().commit_version.get()
        );
    }

    #[test]
    fn unsettled_poison_is_sticky_against_settled_poison_and_prior_resolution() {
        let identity = explicit_identity(1, 2);
        let runtime_bundle = RecordingRuntimeBundle::memory(identity);
        let store = make_store_ready(
            AgentMetadataStore::create_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                identity,
                CreateRecoveryIntentV1::Fresh,
                MetadataStoreCreateModeV1::Active,
            )
            .unwrap(),
        );
        let command = create_command(
            &store,
            request(20),
            scoped_key(root(2), b"sticky-unsettled-poison"),
            b"value",
        );
        runtime_bundle.swap_next_persist_outcome();
        assert_eq!(
            store.execute(&command),
            Err(AgentMetadataError::CommitReceiptRecoveryRequired)
        );
        let planned = match runtime_bundle.state.lock().unwrap().clone() {
            MetadataCommitReceiptStateV1::Pending(planned) => planned,
            state => panic!("expected pending receipt, found {state:?}"),
        };

        let authority = MetadataCommitEngineMintAuthorityV1::for_test();
        let pending_origin = persisted_origin_for_test(&authority, &planned);
        let (unsettled, unsettled_witness) = MetadataCommitReceiptPoisonCommandV1::mint(
            &authority,
            pending_origin,
            MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
        )
        .unwrap();
        let unsettled_origin = runtime_bundle
            .poison_commit_receipt_v1(unsettled)
            .into_live_resolution_origin_for(unsettled_witness)
            .unwrap();

        assert!(matches!(
            MetadataCommitReceiptResolveCommandV1::mint_live(
                &authority,
                unsettled_origin,
                MetadataCommitResolutionV1::not_applied_settled(
                    &authority,
                    planned.prior(),
                    [0x61; 32],
                ),
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        ));
        let second_unsettled_origin = poisoned_origin_for_test(
            &authority,
            &planned,
            MetadataCommitReceiptPoisonReasonV1::UnsettledCommitOutcome,
        );
        assert!(matches!(
            MetadataCommitReceiptPoisonCommandV1::mint(
                &authority,
                second_unsettled_origin,
                MetadataCommitReceiptPoisonReasonV1::SettledCommitOutcome,
            ),
            Err(MetadataCommitReceiptErrorV1::InvalidBinding)
        ));
        assert!(matches!(
            &*runtime_bundle.state.lock().unwrap(),
            MetadataCommitReceiptStateV1::PoisonedUnsettled(durable) if durable == &planned
        ));
    }

    #[test]
    fn concurrent_post_commit_fail_stop_preserves_settlement_classification() {
        for delegate_result in [
            BarrierCommitResult::Committed,
            BarrierCommitResult::BackendError,
            BarrierCommitResult::UnknownUnsettled,
        ] {
            let barrier = Arc::new(Barrier::new(2));
            let fail_stop = Arc::new(MetadataStoreFailStop::default());
            let commit_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let read_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let transaction = Box::new(FailStopMetadataTransaction {
                delegate: Box::new(BarrierCommitTransaction {
                    barrier: Arc::clone(&barrier),
                    commit_calls: Arc::clone(&commit_calls),
                    read_calls: Arc::clone(&read_calls),
                    result: delegate_result,
                }),
                fail_stop: Arc::clone(&fail_stop),
            });
            let blocked_transaction = Box::new(FailStopMetadataTransaction {
                delegate: Box::new(BarrierCommitTransaction {
                    barrier: Arc::clone(&barrier),
                    commit_calls: Arc::clone(&commit_calls),
                    read_calls: Arc::clone(&read_calls),
                    result: delegate_result,
                }),
                fail_stop: Arc::clone(&fail_stop),
            });

            let commit = thread::spawn(move || transaction.commit(AtomicPlan::default()));
            barrier.wait();
            fail_stop.trip();
            barrier.wait();

            let error = commit.join().unwrap().unwrap_err();
            let expected = match delegate_result {
                BarrierCommitResult::UnknownUnsettled => ProviderErrorKind::UnknownCommitUnsettled,
                BarrierCommitResult::Committed | BarrierCommitResult::BackendError => {
                    ProviderErrorKind::UnknownCommitSettled
                }
            };
            assert_eq!(error.kind(), expected);
            assert_eq!(commit_calls.load(Ordering::Acquire), 1);
            assert_eq!(
                blocked_transaction
                    .get(OrderedSpaceId::new(1), b"blocked")
                    .unwrap_err()
                    .kind(),
                ProviderErrorKind::AuthorityMismatch
            );
            assert_eq!(
                blocked_transaction
                    .prefix_is_empty(OrderedSpaceId::new(1), b"blocked")
                    .unwrap_err()
                    .kind(),
                ProviderErrorKind::AuthorityMismatch
            );
            assert_eq!(
                blocked_transaction
                    .commit(AtomicPlan::default())
                    .unwrap_err()
                    .kind(),
                ProviderErrorKind::AuthorityMismatch
            );
            assert_eq!(read_calls.load(Ordering::Acquire), 0);
            assert_eq!(commit_calls.load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn fresh_store_freezes_schema_shard_and_bootstrap_version() {
        let store = AgentMetadataStore::open_memory(shard(1)).unwrap();
        let provider = HoltProvider::open_memory(shard(1)).unwrap();
        let mut actual = provider.tree_names().unwrap();
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
        let absolute_database = std::path::absolute(&database).unwrap();
        let expected_identity =
            MetadataStoreIdentity::standalone_holt_file(shard(1), &absolute_database);
        {
            let store = AgentMetadataStore::create_file(&database, shard(1)).unwrap();
            assert_eq!(store.metadata_store_identity(), expected_identity);
            store.advance_owner_epoch(None, epoch(1)).unwrap();
        }
        assert!(matches!(
            AgentMetadataStore::reopen_file(&database, shard(2)),
            Err(AgentMetadataError::MetadataStoreIdentityMismatch)
        ));
        let reopened = AgentMetadataStore::reopen_file(&database, shard(1)).unwrap();
        assert_eq!(reopened.metadata_store_identity(), expected_identity);
        assert_eq!(reopened.current_read_version().unwrap().get(), 1);
    }

    #[test]
    fn explicit_identity_rejects_zero_fields_and_foreign_contract() {
        let mut identity = explicit_identity(1, 2);
        identity.authority_id = nokv_types::MetadataAuthorityId::from_bytes([0; 16]);
        assert!(matches!(
            open_explicit_memory(identity),
            Err(AgentMetadataError::SchemaGate { .. })
        ));

        let mut identity = explicit_identity(1, 2);
        identity.consistency_domain_id = nokv_types::ConsistencyDomainId::from_bytes([0; 16]);
        assert!(matches!(
            open_explicit_memory(identity),
            Err(AgentMetadataError::SchemaGate { .. })
        ));

        let mut identity = explicit_identity(1, 2);
        identity.profile_fingerprint = [0; 32];
        assert!(matches!(
            open_explicit_memory(identity),
            Err(AgentMetadataError::SchemaGate { .. })
        ));

        let mut identity = explicit_identity(1, 2);
        identity.contract_digest = nokv_types::MetadataContractDigest::from_bytes([0x99; 32]);
        assert!(matches!(
            open_explicit_memory(identity),
            Err(AgentMetadataError::SchemaGate { .. })
        ));
    }

    #[test]
    fn explicit_reopen_requires_every_identity_field_to_match() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("explicit-metadata");
        let expected = explicit_identity(1, 2);
        let runtime_bundle = RecordingRuntimeBundle::file(&database, expected);
        AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            runtime_bundle.clone(),
            expected,
            CreateRecoveryIntentV1::Fresh,
            MetadataStoreCreateModeV1::Active,
        )
        .unwrap();

        let mut wrong_profile = expected;
        wrong_profile.profile_fingerprint = [0x56; 32];
        assert!(matches!(
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(
                runtime_bundle.clone(),
                wrong_profile,
            ),
            Err(AgentMetadataError::ProviderAuthorityMismatch { .. })
        ));
        let reopened =
            AgentMetadataStore::reopen_with_runtime_commit_bundle_v1(runtime_bundle, expected)
                .unwrap();
        assert_eq!(reopened.metadata_store_identity(), expected);
    }

    #[test]
    fn format_ten_store_is_rejected_without_identity_adoption() {
        let temporary = tempdir().unwrap();
        let database = temporary.path().join("format-ten");
        let store = AgentMetadataStore::create_file(&database, shard(1)).unwrap();
        let mut format_ten = encode_schema_marker();
        let version_start = format_ten.len() - std::mem::size_of::<u32>();
        format_ten[version_start..].copy_from_slice(&10_u32.to_be_bytes());
        raw_put(
            &store,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            &format_ten,
        );
        drop(store);

        assert!(matches!(
            AgentMetadataStore::reopen_file(&database, shard(1)),
            Err(AgentMetadataError::SchemaGate { .. })
        ));
    }

    #[test]
    fn foreign_authority_marker_fails_closed() {
        let store = open_explicit_memory(explicit_identity(1, 2)).unwrap();
        let foreign = MetadataAuthorityMarker {
            authority_id: nokv_types::MetadataAuthorityId::from_bytes([0x77; 16]),
            authority_generation: store.identity.authority_generation,
            state: MetadataAuthorityState::Active,
            write_sequence: 0,
            evidence: MetadataAuthorityEvidence::None,
        };
        raw_put(
            &store,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_METADATA_AUTHORITY_KEY,
            &encode_authority_marker(foreign),
        );

        assert_eq!(
            store.metadata_authority_state(),
            Err(AgentMetadataError::MetadataAuthorityBindingMismatch)
        );
        assert_eq!(
            store.advance_owner_epoch(None, epoch(1)),
            Err(AgentMetadataError::MetadataAuthorityBindingMismatch)
        );
    }

    #[test]
    fn logical_state_digest_freezes_space_order_tags_and_key_value_framing() {
        let store = AgentMetadataStore::open_memory(shard(1)).unwrap();
        for (space, key, value) in [
            (
                crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
                b"rf".as_slice(),
                b"one".as_slice(),
            ),
            (
                crate::workspace::provider_catalog::COMMAND_DEDUPE_SPACE,
                b"dd".as_slice(),
                b"two".as_slice(),
            ),
            (
                crate::workspace::provider_catalog::HISTORY_SPACE,
                b"hh".as_slice(),
                b"three".as_slice(),
            ),
            (
                crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                b"rr".as_slice(),
                b"four".as_slice(),
            ),
            (
                crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
                b"op".as_slice(),
                b"five".as_slice(),
            ),
        ] {
            raw_put(&store, space, key, value);
        }
        let transaction = store.provider.begin_write().unwrap();
        let digest = logical_state_digest(transaction.as_ref()).unwrap();
        assert_eq!(
            digest_hex(&digest),
            "f8f8371b3e3c363426287e2ff4a3f0a359c69caf81f63d2bd0acddcd651e9bd5"
        );

        raw_put(
            &store,
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            b"provider-local-test-record",
            b"ignored",
        );
        let transaction = store.provider.begin_write().unwrap();
        assert_eq!(
            logical_state_digest(transaction.as_ref()).unwrap(),
            digest,
            "provider-local System records are intentionally outside logical migration state"
        );

        raw_put(
            &store,
            crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            b"op",
            b"changed",
        );
        let transaction = store.provider.begin_write().unwrap();
        assert_ne!(logical_state_digest(transaction.as_ref()).unwrap(), digest);

        let tags = logical_state_spaces()
            .into_iter()
            .map(logical_state_space_tag)
            .collect::<Vec<_>>();
        assert_eq!(
            &tags[..5],
            &[
                [0x01, 0x02],
                [0x01, 0x03],
                [0x01, 0x04],
                [0x01, 0x05],
                [0x01, 0x06]
            ]
        );
        assert_eq!(tags.len(), 5 + MetadataFamily::ALL.len());
    }

    #[test]
    fn migration_target_rejects_ordinary_writes_and_can_be_abandoned() {
        let identity = explicit_identity(1, 2);
        let target =
            open_migration_target_memory(identity, migration_target_binding(identity)).unwrap();
        let command = base_command(&target, request(1), single_shard_install()).seal();
        let state_error = AgentMetadataError::MetadataAuthorityStateMismatch {
            expected: MetadataAuthorityState::Active,
            actual: MetadataAuthorityState::MigrationTarget,
        };
        assert_eq!(target.execute(&command), Err(state_error.clone()));
        assert_eq!(
            target.advance_owner_epoch(None, epoch(1)),
            Err(state_error.clone())
        );
        assert_eq!(
            target.observe_lease_clock(root(2), generation(7), epoch(1), 1),
            Err(state_error)
        );
        target.fence_migration_target().unwrap();
        target.fence_migration_target().unwrap();
        let token = current_target_activation_token(&target);
        assert_eq!(
            target.advance_owner_epoch(None, epoch(1)),
            Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::Active,
                actual: MetadataAuthorityState::Fenced,
            })
        );
        assert_eq!(
            target.activate_migration_target(&token),
            Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::MigrationTarget,
                actual: MetadataAuthorityState::Fenced,
            })
        );

        let activated =
            open_migration_target_memory(identity, migration_target_binding(identity)).unwrap();
        let token = current_target_activation_token(&activated);
        let mut foreign_migration = token;
        foreign_migration.migration_id = OperationId::from_bytes([0x34; 16]);
        assert!(matches!(
            activated.activate_migration_target(&foreign_migration),
            Err(AgentMetadataError::MetadataMigrationAdmission { .. })
        ));
        let mut stale_source = token;
        stale_source.source_authority_generation =
            nokv_types::MetadataAuthorityGeneration::new(2).unwrap();
        assert!(matches!(
            activated.activate_migration_target(&stale_source),
            Err(AgentMetadataError::MetadataMigrationAdmission { .. })
        ));
        let mut uncopied_frontier = token;
        uncopied_frontier.frontier.state_digest = [0xee; 32];
        assert!(matches!(
            activated.activate_migration_target(&uncopied_frontier),
            Err(AgentMetadataError::MetadataMigrationAdmission { .. })
        ));
        activated.activate_migration_target(&token).unwrap();
        activated.activate_migration_target(&token).unwrap();
        activated.advance_owner_epoch(None, epoch(1)).unwrap();
    }

    #[test]
    fn quiesce_rejects_ordinary_writes_and_conflicts_a_stale_plan() {
        let store = ready_store();
        let stale_transaction = store.provider.begin_write().unwrap();
        let authority = required_record(
            stale_transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_METADATA_AUTHORITY_KEY,
            "System(metadata_authority)",
        )
        .unwrap();
        let stale_key = scoped_key(root(2), b"stale-authority-plan");
        let stale_plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_METADATA_AUTHORITY_KEY.to_vec(),
                    witness: authority.witness,
                },
                AtomicOp::Put {
                    space: crate::workspace::provider_catalog::domain_space(
                        MetadataFamily::Operation,
                    ),
                    key: stale_key,
                    value: CurrentValue {
                        created_version: CommitVersion::new(4).unwrap(),
                        modified_version: CommitVersion::new(4).unwrap(),
                        payload: b"must-not-commit".to_vec(),
                    }
                    .encode()
                    .unwrap(),
                },
            ],
        };

        let receipt = store
            .quiesce_metadata_authority(OperationId::from_bytes([0x33; 16]), epoch(1))
            .unwrap();
        assert_eq!(
            store
                .quiesce_metadata_authority(OperationId::from_bytes([0x33; 16]), epoch(1))
                .unwrap(),
            receipt
        );
        assert_eq!(
            stale_transaction.commit(stale_plan).unwrap(),
            AtomicCommitOutcome::Conflict
        );
        let command = create_command(
            &store,
            request(9),
            scoped_key(root(2), b"after-quiesce"),
            b"value",
        );
        assert_eq!(
            store.execute(&command),
            Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::Active,
                actual: MetadataAuthorityState::Quiescing,
            })
        );
        assert_eq!(
            store.advance_owner_epoch(Some(epoch(1)), epoch(2)),
            Err(AgentMetadataError::MetadataAuthorityStateMismatch {
                expected: MetadataAuthorityState::Active,
                actual: MetadataAuthorityState::Quiescing,
            })
        );
        store.fence_quiesced_metadata_authority(&receipt).unwrap();
        store.fence_quiesced_metadata_authority(&receipt).unwrap();
        assert_eq!(
            store.metadata_authority_state().unwrap(),
            MetadataAuthorityState::Fenced
        );
    }

    #[test]
    fn source_write_between_receipt_scan_and_barrier_forces_quiesce_retry() {
        let store = ready_store();
        let migration_id = OperationId::from_bytes([0x35; 16]);
        let transaction = store.provider.begin_write().unwrap();
        let schema = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_SCHEMA_KEY,
            "System(schema)",
        )
        .unwrap();
        let (identity, authority, marker) =
            store.required_authority_from(transaction.as_ref()).unwrap();
        let owner = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_OWNER_FENCE_KEY,
            "System(owner_fence)",
        )
        .unwrap();
        let recovery_lsn = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_APPLIED_RECOVERY_LSN_KEY,
            "System(applied_recovery_lsn)",
        )
        .unwrap();
        let chain_digest = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_RECOVERY_CHAIN_DIGEST_KEY,
            "System(recovery_chain_digest)",
        )
        .unwrap();
        let commit_clock = required_record(
            transaction.as_ref(),
            crate::workspace::provider_catalog::SYSTEM_SPACE,
            SYSTEM_COMMIT_CLOCK_KEY,
            "System(commit_clock)",
        )
        .unwrap();
        let stale_receipt = SourceQuiesceReceipt {
            logical_shard_id: store.identity.logical_shard_id,
            migration_id,
            source_authority_id: store.identity.authority_id,
            source_authority_generation: store.identity.authority_generation,
            owner_epoch: epoch(1),
            frontier: MetadataRecoveryFrontier {
                recovery_lsn: decode_system_u64(
                    &recovery_lsn.value,
                    "System(applied_recovery_lsn)",
                )
                .unwrap(),
                chain_digest: decode_system_digest(
                    &chain_digest.value,
                    "System(recovery_chain_digest)",
                )
                .unwrap(),
                commit_version: CommitVersion::new(
                    decode_system_u64(&commit_clock.value, "System(commit_clock)").unwrap(),
                )
                .unwrap(),
                state_digest: logical_state_digest(transaction.as_ref()).unwrap(),
            },
            contract_digest: store.identity.contract_digest,
        };
        let stale_marker = MetadataAuthorityMarker {
            state: MetadataAuthorityState::Quiescing,
            evidence: MetadataAuthorityEvidence::SourceQuiesceReceipt(stale_receipt),
            ..marker
        };
        let stale_plan = AtomicPlan {
            operations: vec![
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_SCHEMA_KEY.to_vec(),
                    witness: schema.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_STORE_IDENTITY_KEY.to_vec(),
                    witness: identity.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_OWNER_FENCE_KEY.to_vec(),
                    witness: owner.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_APPLIED_RECOVERY_LSN_KEY.to_vec(),
                    witness: recovery_lsn.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_RECOVERY_CHAIN_DIGEST_KEY.to_vec(),
                    witness: chain_digest.witness,
                },
                AtomicOp::AssertUnchanged {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_COMMIT_CLOCK_KEY.to_vec(),
                    witness: commit_clock.witness,
                },
                AtomicOp::CompareAndPut {
                    space: crate::workspace::provider_catalog::SYSTEM_SPACE,
                    key: SYSTEM_METADATA_AUTHORITY_KEY.to_vec(),
                    witness: authority.witness,
                    value: encode_authority_marker(stale_marker),
                },
            ],
        };

        let command = create_command(
            &store,
            request(0x35),
            scoped_key(root(2), b"between-receipt-and-barrier"),
            b"committed-before-quiesce",
        );
        let committed = store.execute(&command).unwrap();
        assert_eq!(
            committed.commit_version.get(),
            stale_receipt.frontier.commit_version.get() + 1
        );
        let advanced_marker = store.required_authority_marker().unwrap();
        assert_eq!(advanced_marker.state, MetadataAuthorityState::Active);
        assert_eq!(advanced_marker.write_sequence, marker.write_sequence + 1);
        assert_eq!(
            transaction.commit(stale_plan).unwrap(),
            AtomicCommitOutcome::Conflict,
            "the write-sequence CAS makes the formerly unsafe interleaving retry"
        );

        let receipt = store
            .quiesce_metadata_authority(migration_id, epoch(1))
            .unwrap();
        assert_eq!(receipt.frontier.commit_version, committed.commit_version);
        assert_ne!(
            receipt.frontier.state_digest,
            stale_receipt.frontier.state_digest
        );
    }

    #[test]
    fn durable_dedupe_reconciles_unknown_outcome_after_authority_quiesce() {
        let store = ready_store();
        let command = create_command(
            &store,
            request(8),
            scoped_key(root(2), b"authority-replay"),
            b"value",
        );
        let first = store.execute(&command).unwrap();
        let replay = store.execute(&command).unwrap();
        assert!(!first.replayed);
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);

        store
            .quiesce_metadata_authority(OperationId::from_bytes([0x33; 16]), epoch(1))
            .unwrap();
        let replay = store.execute(&command).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.commit_version, first.commit_version);

        let changed = create_command(
            &store,
            command.request_id,
            scoped_key(root(2), b"authority-replay"),
            b"different-value",
        );
        assert_eq!(
            store.execute(&changed),
            Err(AgentMetadataError::RequestIdReused)
        );
    }

    #[test]
    fn root_install_activate_and_stale_fences_fail_closed() {
        let store = ready_store();
        let duplicate_install = base_command(&store, request(3), single_shard_install()).seal();
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
    fn root_layout_change_conflicts_a_stale_require_active_plan() {
        let store = ready_store();
        let transaction = store.provider.begin_write().unwrap();
        let command = base_command(&store, request(90), RootFenceAction::RequireActive).seal();
        let root_plan = store
            .plan_root_fence(transaction.as_ref(), &command)
            .unwrap();
        let side_effect_key = scoped_key(root(2), b"stale-root-layout-plan");
        let mut stale_plan = AtomicPlan::default();
        enqueue_root_fence(&mut stale_plan, &command, &root_plan);
        stale_plan.operations.push(AtomicOp::Put {
            space: crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            key: side_effect_key.clone(),
            value: b"must-not-commit".to_vec(),
        });

        let mut changed = store.root_fence(root(2)).unwrap().unwrap();
        changed.layout_generation = RootLayoutGeneration::new(2).unwrap();
        raw_put(
            &store,
            crate::workspace::provider_catalog::ROOT_FENCE_SPACE,
            root(2).as_bytes(),
            &changed.encode().unwrap(),
        );

        assert_eq!(
            transaction.commit(stale_plan).unwrap(),
            AtomicCommitOutcome::Conflict
        );
        assert!(store
            .provider
            .get(
                crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
                &side_effect_key,
            )
            .unwrap()
            .is_none());
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
    fn fenced_point_reader_rejects_a_foreign_root_key() {
        let store = ready_store();
        let result =
            store.with_fenced_point_reads(root(2), generation(7), epoch(1), None, |_, reader| {
                reader.get(MetadataFamily::Operation, &scoped_key(root(3), b"foreign"))
            });
        assert!(matches!(
            result,
            Err(AgentMetadataError::InvalidCommand { .. })
        ));
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
        raw_put(
            &store,
            crate::workspace::provider_catalog::HISTORY_SPACE,
            &history_key,
            b"corrupt-history",
        );

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
        raw_put(
            &store,
            crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            &key,
            &encoded,
        );

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
            Err(AgentMetadataError::CorruptRecord {
                record: OPERATION_TREE,
                ..
            })
        ));
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
        raw_put(
            &store,
            crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            &before_cursor,
            b"corrupt-before-cursor",
        );
        raw_put(
            &store,
            crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            &after_limit,
            b"corrupt-after-limit",
        );

        let version = store.current_read_version().unwrap();
        #[cfg(feature = "metadata-read-stats")]
        let stats_session = store.begin_read_stats_session().unwrap();
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
            let stats = stats_session.finish().unwrap();
            assert_eq!(stats.scan_calls, 1);
            assert_eq!(stats.scan_cursors, 1);
            assert_eq!(stats.scan_returned_keys, 2);
            assert!(stats.scan_visited_units >= 2);
            assert_eq!(stats.scan_raw_limit_stops, 1);
            assert!(stats.scan_key_bytes > 0);
            assert!(stats.scan_value_bytes > 0);
        }
    }

    #[test]
    fn fresh_initialization_rejects_a_non_system_row_in_the_authority_namespace() {
        let identity = explicit_identity(1, 2);
        let provider = HoltProvider::open_memory(identity.logical_shard_id).unwrap();
        let retained = provider.clone();
        let transaction = provider.begin_write().unwrap();
        let foreign_key = b"preexisting-domain-row".to_vec();
        assert_eq!(
            transaction
                .commit(AtomicPlan {
                    operations: vec![AtomicOp::Put {
                        space: crate::workspace::provider_catalog::domain_space(
                            MetadataFamily::Operation
                        ),
                        key: foreign_key.clone(),
                        value: b"must-survive".to_vec(),
                    }],
                })
                .unwrap(),
            AtomicCommitOutcome::Committed
        );

        assert!(matches!(
            AgentMetadataStore::initialize_fresh(
                Arc::new(provider),
                identity,
                MetadataAuthorityMarker::for_identity(identity, MetadataAuthorityState::Active,),
                RecordingRuntimeBundle::memory(identity),
            ),
            Err(AgentMetadataError::SchemaGate { .. })
        ));
        assert_eq!(
            retained
                .get(
                    crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
                    &foreign_key,
                )
                .unwrap()
                .unwrap()
                .value,
            b"must-survive"
        );
        assert!(retained
            .get(
                crate::workspace::provider_catalog::SYSTEM_SPACE,
                SYSTEM_SCHEMA_KEY
            )
            .unwrap()
            .is_none());
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
            crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            &nested_a,
            b"corrupt-nested-value",
        );
        drop(store);
        let store = AgentMetadataStore::reopen_file(&database, shard(1)).unwrap();
        #[cfg(feature = "metadata-read-stats")]
        let stats_session = store.begin_read_stats_session().unwrap();

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
            let stats = stats_session.finish().unwrap();
            assert_eq!(stats.scan_calls, 2);
            assert_eq!(stats.scan_cursors, 2);
            assert!(stats.scan_common_prefixes >= 1);
            assert!(
                [
                    stats.provider_cache_hits,
                    stats.provider_cache_misses,
                    stats.provider_full_read_operations,
                    stats.provider_partial_read_cache_hits,
                    stats.provider_partial_read_cache_misses,
                ]
                .into_iter()
                .flatten()
                .sum::<u64>()
                    > 0,
                "file-backed read session should expose provider read activity"
            );
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
        raw_put(
            &store,
            crate::workspace::provider_catalog::domain_space(MetadataFamily::Operation),
            &corrupt_tail,
            b"corrupt-historical-tail",
        );

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
            Err(AgentMetadataError::InvalidCommand { .. })
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
            Err(AgentMetadataError::InvalidCommand { .. })
        ));
    }

    #[test]
    fn command_size_bounds_fit_holt_envelopes_and_reject_before_atomic() {
        let store = ready_store();
        let key = scoped_key(root(2), b"holt-value-boundary");
        let boundary = vec![0x5a; MAX_METADATA_RECORD_PAYLOAD_BYTES];

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

        let replacement = vec![0x6b; MAX_METADATA_RECORD_PAYLOAD_BYTES];
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
        let oversized = vec![0; MAX_METADATA_RECORD_PAYLOAD_BYTES + 1];

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
                .execute(&base_command(&store, request(1), single_shard_install()).seal())
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
            raw_put(
                &store,
                crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                &[2, 0, 0, 0, 0],
                b"unknown recovery row",
            );
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
            raw_delete(
                &store,
                crate::workspace::provider_catalog::RECOVERY_OUTBOX_SPACE,
                &recovery_chunk_key(1, 0),
            );
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
            target.replay_recovery_record(row).unwrap();
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
