//! Provider-neutral durable identity and write-authority records.
//!
//! These records live in the workspace `System` space. The store identity is
//! immutable for the lifetime of one provider installation. The authority
//! marker is the only mutable record and is compared in every ordinary write
//! transaction.

use std::fmt;
use std::path::Path;

use nokv_types::{
    CommitVersion, ConsistencyDomainId, LogicalShardId, MetadataAuthorityGeneration,
    MetadataAuthorityId, MetadataContractDigest, MetadataMigrationTargetBinding,
    MetadataRecoveryFrontier, OperationId, OwnerEpoch, SourceQuiesceReceipt, TargetActivationToken,
};
use sha2::{Digest, Sha256};

const IDENTITY_VALUE_FORMAT_VERSION: u8 = 1;
const AUTHORITY_VALUE_FORMAT_VERSION: u8 = 2;
const AUTHORITY_ID_BYTES: usize = 16;
const CONSISTENCY_DOMAIN_ID_BYTES: usize = 16;
const DIGEST_BYTES: usize = 32;
const IDENTITY_ENCODED_BYTES: usize = 1
    + 16
    + AUTHORITY_ID_BYTES
    + std::mem::size_of::<u64>()
    + CONSISTENCY_DOMAIN_ID_BYTES
    + DIGEST_BYTES
    + DIGEST_BYTES;
const AUTHORITY_BASE_ENCODED_BYTES: usize =
    1 + AUTHORITY_ID_BYTES + std::mem::size_of::<u64>() + 1 + std::mem::size_of::<u64>() + 1;
const RECOVERY_FRONTIER_ENCODED_BYTES: usize =
    std::mem::size_of::<u64>() + DIGEST_BYTES + std::mem::size_of::<u64>() + DIGEST_BYTES;
const SOURCE_RECEIPT_ENCODED_BYTES: usize = 16
    + 16
    + AUTHORITY_ID_BYTES
    + std::mem::size_of::<u64>()
    + std::mem::size_of::<u64>()
    + RECOVERY_FRONTIER_ENCODED_BYTES
    + DIGEST_BYTES;
const MIGRATION_TARGET_BINDING_ENCODED_BYTES: usize = 16
    + 16
    + AUTHORITY_ID_BYTES
    + std::mem::size_of::<u64>()
    + AUTHORITY_ID_BYTES
    + std::mem::size_of::<u64>()
    + DIGEST_BYTES;
const TARGET_TOKEN_ENCODED_BYTES: usize = 16
    + 16
    + AUTHORITY_ID_BYTES
    + std::mem::size_of::<u64>()
    + AUTHORITY_ID_BYTES
    + std::mem::size_of::<u64>()
    + RECOVERY_FRONTIER_ENCODED_BYTES
    + DIGEST_BYTES
    + DIGEST_BYTES;

const CONTRACT_DOMAIN: &[u8] = b"nokv.workspace.metadata-contract.v1\0";
const STANDALONE_AUTHORITY_DOMAIN: &[u8] = b"nokv.metadata.standalone.authority.v1\0";
const STANDALONE_CONSISTENCY_DOMAIN: &[u8] = b"nokv.metadata.standalone.consistency.v1\0";
const STANDALONE_PROFILE_DOMAIN: &[u8] = b"nokv.metadata.standalone.holt-profile.v1\0";

/// External, monotonically advancing evidence for every acknowledged metadata
/// mutation. `write_sequence` covers authority-only writes that deliberately
/// do not append a recovery row; the remaining fields bind the exact logical
/// recovery chain and commit clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcknowledgedMetadataFrontier {
    pub write_sequence: u64,
    pub commit_version: CommitVersion,
    pub recovery_lsn: u64,
    pub chain_digest: [u8; DIGEST_BYTES],
}

/// Immutable, provider-neutral identity of one metadata store installation.
///
/// Provider credentials and provider-specific connection details are never
/// persisted here. `profile_fingerprint` is the SHA-256 digest of the resolved,
/// secret-free provider configuration admitted by the control plane.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetadataStoreIdentity {
    pub logical_shard_id: LogicalShardId,
    pub authority_id: MetadataAuthorityId,
    pub authority_generation: MetadataAuthorityGeneration,
    pub consistency_domain_id: ConsistencyDomainId,
    pub profile_fingerprint: [u8; DIGEST_BYTES],
    pub contract_digest: MetadataContractDigest,
}

impl MetadataStoreIdentity {
    /// Deterministic identity used only by the standalone/test Holt APIs.
    ///
    /// Distributed server bootstrap must construct an identity from the
    /// admitted control-plane authority and use the explicit identity APIs.
    pub(crate) fn standalone_holt_memory(logical_shard_id: LogicalShardId) -> Self {
        Self::standalone_holt(logical_shard_id, b"memory")
    }

    pub(crate) fn standalone_holt_file(logical_shard_id: LogicalShardId, path: &Path) -> Self {
        Self::standalone_holt(logical_shard_id, path.as_os_str().as_encoded_bytes())
    }

    fn standalone_holt(logical_shard_id: LogicalShardId, location: &[u8]) -> Self {
        let authority_digest =
            hash_with_location(STANDALONE_AUTHORITY_DOMAIN, logical_shard_id, location);
        let consistency_digest =
            hash_with_location(STANDALONE_CONSISTENCY_DOMAIN, logical_shard_id, location);
        Self {
            logical_shard_id,
            authority_id: MetadataAuthorityId::from_bytes(
                authority_digest[..AUTHORITY_ID_BYTES]
                    .try_into()
                    .expect("digest prefix has the authority-id width"),
            ),
            authority_generation: MetadataAuthorityGeneration::new(1)
                .expect("standalone authority generation is non-zero"),
            consistency_domain_id: ConsistencyDomainId::from_bytes(
                consistency_digest[..CONSISTENCY_DOMAIN_ID_BYTES]
                    .try_into()
                    .expect("digest prefix has the consistency-domain width"),
            ),
            profile_fingerprint: hash_with_location(
                STANDALONE_PROFILE_DOMAIN,
                logical_shard_id,
                location,
            ),
            contract_digest: workspace_metadata_contract_digest(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataStoreIdentityValidationError {
    ZeroAuthorityId,
    ZeroConsistencyDomainId,
    ZeroProfileFingerprint,
    ZeroContractDigest,
    ContractDigestMismatch,
}

impl fmt::Display for MetadataStoreIdentityValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroAuthorityId => {
                formatter.write_str("metadata authority id must not be all-zero")
            }
            Self::ZeroConsistencyDomainId => {
                formatter.write_str("metadata consistency-domain id must not be all-zero")
            }
            Self::ZeroProfileFingerprint => {
                formatter.write_str("metadata provider profile fingerprint must not be all-zero")
            }
            Self::ZeroContractDigest => {
                formatter.write_str("metadata contract digest must not be all-zero")
            }
            Self::ContractDigestMismatch => {
                formatter.write_str("metadata contract digest does not match this workspace engine")
            }
        }
    }
}

pub(crate) fn validate_metadata_store_identity(
    identity: MetadataStoreIdentity,
) -> Result<(), MetadataStoreIdentityValidationError> {
    let all_zero = |bytes: &[u8]| bytes.iter().all(|byte| *byte == 0);
    if all_zero(identity.authority_id.as_bytes()) {
        return Err(MetadataStoreIdentityValidationError::ZeroAuthorityId);
    }
    if all_zero(identity.consistency_domain_id.as_bytes()) {
        return Err(MetadataStoreIdentityValidationError::ZeroConsistencyDomainId);
    }
    if all_zero(&identity.profile_fingerprint) {
        return Err(MetadataStoreIdentityValidationError::ZeroProfileFingerprint);
    }
    if all_zero(identity.contract_digest.as_bytes()) {
        return Err(MetadataStoreIdentityValidationError::ZeroContractDigest);
    }
    if identity.contract_digest != workspace_metadata_contract_digest() {
        return Err(MetadataStoreIdentityValidationError::ContractDigestMismatch);
    }
    Ok(())
}

/// Digest of the provider-neutral metadata semantics implemented by this
/// workspace schema generation.
pub fn workspace_metadata_contract_digest() -> MetadataContractDigest {
    let mut hasher = Sha256::new();
    hasher.update(CONTRACT_DOMAIN);
    hasher.update(
        b"nokv_workspace\0system-format-11\0cross-space-atomic-batch-v1\0opaque-record-witness-v1\0logical-commit-clock-v1\0recovery-outbox-v3\0authority-migration-receipt-v1\0",
    );
    MetadataContractDigest::from_bytes(hasher.finalize().into())
}

/// Durable admission state for ordinary metadata writes.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MetadataAuthorityState {
    Active = 1,
    MigrationTarget = 2,
    Quiescing = 3,
    Fenced = 4,
}

impl MetadataAuthorityState {
    pub(crate) const fn permits_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Active, Self::Quiescing)
                | (Self::Quiescing, Self::Fenced)
                | (Self::MigrationTarget, Self::Active)
                | (Self::MigrationTarget, Self::Fenced)
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MetadataAuthorityMarker {
    pub authority_id: MetadataAuthorityId,
    pub authority_generation: MetadataAuthorityGeneration,
    pub state: MetadataAuthorityState,
    pub write_sequence: u64,
    pub evidence: MetadataAuthorityEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataAuthorityEvidence {
    None,
    MigrationTargetBinding(MetadataMigrationTargetBinding),
    SourceQuiesceReceipt(SourceQuiesceReceipt),
    TargetActivationToken(TargetActivationToken),
}

impl MetadataAuthorityMarker {
    pub(crate) const fn for_identity(
        identity: MetadataStoreIdentity,
        state: MetadataAuthorityState,
    ) -> Self {
        Self {
            authority_id: identity.authority_id,
            authority_generation: identity.authority_generation,
            state,
            write_sequence: 0,
            evidence: MetadataAuthorityEvidence::None,
        }
    }

    pub(crate) fn matches_identity(self, identity: MetadataStoreIdentity) -> bool {
        self.authority_id == identity.authority_id
            && self.authority_generation == identity.authority_generation
    }

    pub(crate) fn advance_active_write(self) -> Option<Self> {
        if self.state != MetadataAuthorityState::Active {
            return None;
        }
        Some(Self {
            write_sequence: self.write_sequence.checked_add(1)?,
            ..self
        })
    }

    pub(crate) fn advance_write_sequence(self) -> Option<Self> {
        Some(Self {
            write_sequence: self.write_sequence.checked_add(1)?,
            ..self
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataAuthorityCodecError {
    UnsupportedVersion {
        record: &'static str,
        actual: u8,
        expected: u8,
    },
    InvalidLength {
        record: &'static str,
        actual: usize,
        expected: usize,
    },
    ZeroAuthorityGeneration,
    ZeroOwnerEpoch,
    ZeroCommitVersion,
    UnknownState(u8),
    UnknownEvidence(u8),
    InvalidEvidence,
}

impl fmt::Display for MetadataAuthorityCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion {
                record,
                actual,
                expected,
            } => write!(
                formatter,
                "unsupported {record} format version {actual}, expected {expected}"
            ),
            Self::InvalidLength {
                record,
                actual,
                expected,
            } => write!(
                formatter,
                "invalid {record} length {actual}, expected {expected}"
            ),
            Self::ZeroAuthorityGeneration => {
                formatter.write_str("metadata authority generation must be non-zero")
            }
            Self::ZeroOwnerEpoch => formatter.write_str("owner epoch must be non-zero"),
            Self::ZeroCommitVersion => formatter.write_str("commit version must be non-zero"),
            Self::UnknownState(value) => {
                write!(formatter, "unknown metadata authority state {value}")
            }
            Self::UnknownEvidence(value) => {
                write!(formatter, "unknown metadata authority evidence {value}")
            }
            Self::InvalidEvidence => formatter.write_str(
                "metadata authority evidence does not match its durable authority state",
            ),
        }
    }
}

impl std::error::Error for MetadataAuthorityCodecError {}

pub(crate) fn encode_store_identity(identity: MetadataStoreIdentity) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(IDENTITY_ENCODED_BYTES);
    encoded.push(IDENTITY_VALUE_FORMAT_VERSION);
    encoded.extend_from_slice(identity.logical_shard_id.as_bytes());
    encoded.extend_from_slice(identity.authority_id.as_bytes());
    encoded.extend_from_slice(&identity.authority_generation.get().to_be_bytes());
    encoded.extend_from_slice(identity.consistency_domain_id.as_bytes());
    encoded.extend_from_slice(&identity.profile_fingerprint);
    encoded.extend_from_slice(identity.contract_digest.as_bytes());
    encoded
}

pub(crate) fn decode_store_identity(
    encoded: &[u8],
) -> Result<MetadataStoreIdentity, MetadataAuthorityCodecError> {
    require_record(
        encoded,
        "MetadataStoreIdentity",
        IDENTITY_VALUE_FORMAT_VERSION,
        IDENTITY_ENCODED_BYTES,
    )?;
    let mut cursor = 1;
    let logical_shard_id = LogicalShardId::from_bytes(take(&mut cursor, encoded));
    let authority_id = MetadataAuthorityId::from_bytes(take(&mut cursor, encoded));
    let authority_generation =
        MetadataAuthorityGeneration::new(u64::from_be_bytes(take(&mut cursor, encoded)))
            .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?;
    let consistency_domain_id = ConsistencyDomainId::from_bytes(take(&mut cursor, encoded));
    let profile_fingerprint = take(&mut cursor, encoded);
    let contract_digest = MetadataContractDigest::from_bytes(take(&mut cursor, encoded));
    debug_assert_eq!(cursor, encoded.len());
    Ok(MetadataStoreIdentity {
        logical_shard_id,
        authority_id,
        authority_generation,
        consistency_domain_id,
        profile_fingerprint,
        contract_digest,
    })
}

pub(crate) fn encode_authority_marker(marker: MetadataAuthorityMarker) -> Vec<u8> {
    let evidence_bytes = match marker.evidence {
        MetadataAuthorityEvidence::None => 0,
        MetadataAuthorityEvidence::MigrationTargetBinding(_) => {
            MIGRATION_TARGET_BINDING_ENCODED_BYTES
        }
        MetadataAuthorityEvidence::SourceQuiesceReceipt(_) => SOURCE_RECEIPT_ENCODED_BYTES,
        MetadataAuthorityEvidence::TargetActivationToken(_) => TARGET_TOKEN_ENCODED_BYTES,
    };
    let mut encoded = Vec::with_capacity(AUTHORITY_BASE_ENCODED_BYTES + evidence_bytes);
    encoded.push(AUTHORITY_VALUE_FORMAT_VERSION);
    encoded.extend_from_slice(marker.authority_id.as_bytes());
    encoded.extend_from_slice(&marker.authority_generation.get().to_be_bytes());
    encoded.push(marker.state as u8);
    encoded.extend_from_slice(&marker.write_sequence.to_be_bytes());
    match marker.evidence {
        MetadataAuthorityEvidence::None => encoded.push(0),
        MetadataAuthorityEvidence::MigrationTargetBinding(binding) => {
            encoded.push(3);
            encode_migration_target_binding(&mut encoded, binding);
        }
        MetadataAuthorityEvidence::SourceQuiesceReceipt(receipt) => {
            encoded.push(1);
            encode_source_receipt(&mut encoded, receipt);
        }
        MetadataAuthorityEvidence::TargetActivationToken(token) => {
            encoded.push(2);
            encode_target_token(&mut encoded, token);
        }
    }
    encoded
}

pub(crate) fn decode_authority_marker(
    encoded: &[u8],
) -> Result<MetadataAuthorityMarker, MetadataAuthorityCodecError> {
    if encoded.first() != Some(&AUTHORITY_VALUE_FORMAT_VERSION) {
        return Err(MetadataAuthorityCodecError::UnsupportedVersion {
            record: "MetadataAuthorityState",
            actual: encoded.first().copied().unwrap_or_default(),
            expected: AUTHORITY_VALUE_FORMAT_VERSION,
        });
    }
    if encoded.len() < AUTHORITY_BASE_ENCODED_BYTES {
        return Err(MetadataAuthorityCodecError::InvalidLength {
            record: "MetadataAuthorityState",
            actual: encoded.len(),
            expected: AUTHORITY_BASE_ENCODED_BYTES,
        });
    }
    let mut cursor = 1;
    let authority_id = MetadataAuthorityId::from_bytes(take(&mut cursor, encoded));
    let authority_generation =
        MetadataAuthorityGeneration::new(u64::from_be_bytes(take(&mut cursor, encoded)))
            .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?;
    let state = decode_authority_state(encoded[cursor])?;
    cursor += 1;
    let write_sequence = u64::from_be_bytes(take(&mut cursor, encoded));
    let evidence_tag = encoded[cursor];
    cursor += 1;
    let expected_length = AUTHORITY_BASE_ENCODED_BYTES
        + match evidence_tag {
            0 => 0,
            1 => SOURCE_RECEIPT_ENCODED_BYTES,
            2 => TARGET_TOKEN_ENCODED_BYTES,
            3 => MIGRATION_TARGET_BINDING_ENCODED_BYTES,
            value => return Err(MetadataAuthorityCodecError::UnknownEvidence(value)),
        };
    if encoded.len() != expected_length {
        return Err(MetadataAuthorityCodecError::InvalidLength {
            record: "MetadataAuthorityState",
            actual: encoded.len(),
            expected: expected_length,
        });
    }
    let evidence = match evidence_tag {
        0 => MetadataAuthorityEvidence::None,
        1 => MetadataAuthorityEvidence::SourceQuiesceReceipt(decode_source_receipt(
            &mut cursor,
            encoded,
        )?),
        2 => MetadataAuthorityEvidence::TargetActivationToken(decode_target_token(
            &mut cursor,
            encoded,
        )?),
        3 => MetadataAuthorityEvidence::MigrationTargetBinding(decode_migration_target_binding(
            &mut cursor,
            encoded,
        )?),
        _ => unreachable!("evidence tag was checked above"),
    };
    let marker = MetadataAuthorityMarker {
        authority_id,
        authority_generation,
        state,
        write_sequence,
        evidence,
    };
    if !valid_evidence(marker.state, marker.evidence) {
        return Err(MetadataAuthorityCodecError::InvalidEvidence);
    }
    debug_assert_eq!(cursor, encoded.len());
    Ok(marker)
}

fn encode_source_receipt(encoded: &mut Vec<u8>, receipt: SourceQuiesceReceipt) {
    encoded.extend_from_slice(receipt.logical_shard_id.as_bytes());
    encoded.extend_from_slice(receipt.migration_id.as_bytes());
    encoded.extend_from_slice(receipt.source_authority_id.as_bytes());
    encoded.extend_from_slice(&receipt.source_authority_generation.get().to_be_bytes());
    encoded.extend_from_slice(&receipt.owner_epoch.get().to_be_bytes());
    encode_frontier(encoded, receipt.frontier);
    encoded.extend_from_slice(receipt.contract_digest.as_bytes());
}

fn decode_source_receipt(
    cursor: &mut usize,
    encoded: &[u8],
) -> Result<SourceQuiesceReceipt, MetadataAuthorityCodecError> {
    Ok(SourceQuiesceReceipt {
        logical_shard_id: LogicalShardId::from_bytes(take(cursor, encoded)),
        migration_id: OperationId::from_bytes(take(cursor, encoded)),
        source_authority_id: MetadataAuthorityId::from_bytes(take(cursor, encoded)),
        source_authority_generation: MetadataAuthorityGeneration::new(u64::from_be_bytes(take(
            cursor, encoded,
        )))
        .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?,
        owner_epoch: OwnerEpoch::new(u64::from_be_bytes(take(cursor, encoded)))
            .map_err(|_| MetadataAuthorityCodecError::ZeroOwnerEpoch)?,
        frontier: decode_frontier(cursor, encoded)?,
        contract_digest: MetadataContractDigest::from_bytes(take(cursor, encoded)),
    })
}

fn encode_target_token(encoded: &mut Vec<u8>, token: TargetActivationToken) {
    encoded.extend_from_slice(token.logical_shard_id.as_bytes());
    encoded.extend_from_slice(token.migration_id.as_bytes());
    encoded.extend_from_slice(token.source_authority_id.as_bytes());
    encoded.extend_from_slice(&token.source_authority_generation.get().to_be_bytes());
    encoded.extend_from_slice(token.target_authority_id.as_bytes());
    encoded.extend_from_slice(&token.target_authority_generation.get().to_be_bytes());
    encode_frontier(encoded, token.frontier);
    encoded.extend_from_slice(token.contract_digest.as_bytes());
    encoded.extend_from_slice(&token.source_receipt_digest);
}

fn decode_target_token(
    cursor: &mut usize,
    encoded: &[u8],
) -> Result<TargetActivationToken, MetadataAuthorityCodecError> {
    Ok(TargetActivationToken {
        logical_shard_id: LogicalShardId::from_bytes(take(cursor, encoded)),
        migration_id: OperationId::from_bytes(take(cursor, encoded)),
        source_authority_id: MetadataAuthorityId::from_bytes(take(cursor, encoded)),
        source_authority_generation: MetadataAuthorityGeneration::new(u64::from_be_bytes(take(
            cursor, encoded,
        )))
        .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?,
        target_authority_id: MetadataAuthorityId::from_bytes(take(cursor, encoded)),
        target_authority_generation: MetadataAuthorityGeneration::new(u64::from_be_bytes(take(
            cursor, encoded,
        )))
        .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?,
        frontier: decode_frontier(cursor, encoded)?,
        contract_digest: MetadataContractDigest::from_bytes(take(cursor, encoded)),
        source_receipt_digest: take(cursor, encoded),
    })
}

fn encode_migration_target_binding(encoded: &mut Vec<u8>, binding: MetadataMigrationTargetBinding) {
    encoded.extend_from_slice(binding.logical_shard_id.as_bytes());
    encoded.extend_from_slice(binding.migration_id.as_bytes());
    encoded.extend_from_slice(binding.source_authority_id.as_bytes());
    encoded.extend_from_slice(&binding.source_authority_generation.get().to_be_bytes());
    encoded.extend_from_slice(binding.target_authority_id.as_bytes());
    encoded.extend_from_slice(&binding.target_authority_generation.get().to_be_bytes());
    encoded.extend_from_slice(binding.contract_digest.as_bytes());
}

fn decode_migration_target_binding(
    cursor: &mut usize,
    encoded: &[u8],
) -> Result<MetadataMigrationTargetBinding, MetadataAuthorityCodecError> {
    Ok(MetadataMigrationTargetBinding {
        logical_shard_id: LogicalShardId::from_bytes(take(cursor, encoded)),
        migration_id: OperationId::from_bytes(take(cursor, encoded)),
        source_authority_id: MetadataAuthorityId::from_bytes(take(cursor, encoded)),
        source_authority_generation: MetadataAuthorityGeneration::new(u64::from_be_bytes(take(
            cursor, encoded,
        )))
        .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?,
        target_authority_id: MetadataAuthorityId::from_bytes(take(cursor, encoded)),
        target_authority_generation: MetadataAuthorityGeneration::new(u64::from_be_bytes(take(
            cursor, encoded,
        )))
        .map_err(|_| MetadataAuthorityCodecError::ZeroAuthorityGeneration)?,
        contract_digest: MetadataContractDigest::from_bytes(take(cursor, encoded)),
    })
}

fn encode_frontier(encoded: &mut Vec<u8>, frontier: MetadataRecoveryFrontier) {
    encoded.extend_from_slice(&frontier.recovery_lsn.to_be_bytes());
    encoded.extend_from_slice(&frontier.chain_digest);
    encoded.extend_from_slice(&frontier.commit_version.get().to_be_bytes());
    encoded.extend_from_slice(&frontier.state_digest);
}

fn decode_frontier(
    cursor: &mut usize,
    encoded: &[u8],
) -> Result<MetadataRecoveryFrontier, MetadataAuthorityCodecError> {
    Ok(MetadataRecoveryFrontier {
        recovery_lsn: u64::from_be_bytes(take(cursor, encoded)),
        chain_digest: take(cursor, encoded),
        commit_version: CommitVersion::new(u64::from_be_bytes(take(cursor, encoded)))
            .map_err(|_| MetadataAuthorityCodecError::ZeroCommitVersion)?,
        state_digest: take(cursor, encoded),
    })
}

fn valid_evidence(state: MetadataAuthorityState, evidence: MetadataAuthorityEvidence) -> bool {
    matches!(
        (state, evidence),
        (
            MetadataAuthorityState::Active,
            MetadataAuthorityEvidence::None
        ) | (
            MetadataAuthorityState::Active,
            MetadataAuthorityEvidence::TargetActivationToken(_)
        ) | (
            MetadataAuthorityState::MigrationTarget,
            MetadataAuthorityEvidence::MigrationTargetBinding(_)
        ) | (
            MetadataAuthorityState::Quiescing,
            MetadataAuthorityEvidence::SourceQuiesceReceipt(_)
        ) | (
            MetadataAuthorityState::Fenced,
            MetadataAuthorityEvidence::None
        ) | (
            MetadataAuthorityState::Fenced,
            MetadataAuthorityEvidence::MigrationTargetBinding(_)
        ) | (
            MetadataAuthorityState::Fenced,
            MetadataAuthorityEvidence::SourceQuiesceReceipt(_)
        )
    )
}

fn require_record(
    encoded: &[u8],
    record: &'static str,
    expected_version: u8,
    expected_length: usize,
) -> Result<(), MetadataAuthorityCodecError> {
    if encoded.len() != expected_length {
        return Err(MetadataAuthorityCodecError::InvalidLength {
            record,
            actual: encoded.len(),
            expected: expected_length,
        });
    }
    if encoded[0] != expected_version {
        return Err(MetadataAuthorityCodecError::UnsupportedVersion {
            record,
            actual: encoded[0],
            expected: expected_version,
        });
    }
    Ok(())
}

fn decode_authority_state(
    value: u8,
) -> Result<MetadataAuthorityState, MetadataAuthorityCodecError> {
    match value {
        1 => Ok(MetadataAuthorityState::Active),
        2 => Ok(MetadataAuthorityState::MigrationTarget),
        3 => Ok(MetadataAuthorityState::Quiescing),
        4 => Ok(MetadataAuthorityState::Fenced),
        value => Err(MetadataAuthorityCodecError::UnknownState(value)),
    }
}

fn take<const N: usize>(cursor: &mut usize, encoded: &[u8]) -> [u8; N] {
    let end = *cursor + N;
    let value = encoded[*cursor..end]
        .try_into()
        .expect("record length was checked before fixed-width decoding");
    *cursor = end;
    value
}

fn hash_with_location(
    domain: &[u8],
    logical_shard_id: LogicalShardId,
    location: &[u8],
) -> [u8; DIGEST_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(logical_shard_id.as_bytes());
    hasher.update((location.len() as u64).to_be_bytes());
    hasher.update(location);
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> MetadataStoreIdentity {
        MetadataStoreIdentity {
            logical_shard_id: LogicalShardId::from_bytes([0x11; 16]),
            authority_id: MetadataAuthorityId::from_bytes([0x22; 16]),
            authority_generation: MetadataAuthorityGeneration::new(3).unwrap(),
            consistency_domain_id: ConsistencyDomainId::from_bytes([0x44; 16]),
            profile_fingerprint: [0x55; 32],
            contract_digest: MetadataContractDigest::from_bytes([0x66; 32]),
        }
    }

    fn hex_encode(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut output, "{byte:02x}").unwrap();
        }
        output
    }

    #[test]
    fn identity_codec_has_frozen_golden_and_rejects_unknown_version() {
        let encoded = encode_store_identity(identity());
        assert_eq!(
            hex_encode(&encoded),
            concat!(
                "01",
                "11111111111111111111111111111111",
                "22222222222222222222222222222222",
                "0000000000000003",
                "44444444444444444444444444444444",
                "5555555555555555555555555555555555555555555555555555555555555555",
                "6666666666666666666666666666666666666666666666666666666666666666"
            )
        );
        assert_eq!(decode_store_identity(&encoded).unwrap(), identity());

        let mut unknown = encoded;
        unknown[0] = IDENTITY_VALUE_FORMAT_VERSION + 1;
        assert!(matches!(
            decode_store_identity(&unknown),
            Err(MetadataAuthorityCodecError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn authority_codec_has_frozen_golden_and_rejects_unknown_state() {
        let marker = MetadataAuthorityMarker {
            evidence: MetadataAuthorityEvidence::MigrationTargetBinding(
                MetadataMigrationTargetBinding {
                    logical_shard_id: LogicalShardId::from_bytes([0x11; 16]),
                    migration_id: OperationId::from_bytes([0x44; 16]),
                    source_authority_id: MetadataAuthorityId::from_bytes([0x33; 16]),
                    source_authority_generation: MetadataAuthorityGeneration::new(2).unwrap(),
                    target_authority_id: MetadataAuthorityId::from_bytes([0x22; 16]),
                    target_authority_generation: MetadataAuthorityGeneration::new(3).unwrap(),
                    contract_digest: MetadataContractDigest::from_bytes([0x66; 32]),
                },
            ),
            ..MetadataAuthorityMarker::for_identity(
                identity(),
                MetadataAuthorityState::MigrationTarget,
            )
        };
        let encoded = encode_authority_marker(marker);
        assert_eq!(
            hex_encode(&encoded),
            concat!(
                "02",
                "22222222222222222222222222222222",
                "0000000000000003",
                "02",
                "0000000000000000",
                "03",
                "11111111111111111111111111111111",
                "44444444444444444444444444444444",
                "33333333333333333333333333333333",
                "0000000000000002",
                "22222222222222222222222222222222",
                "0000000000000003",
                "6666666666666666666666666666666666666666666666666666666666666666"
            )
        );
        assert_eq!(decode_authority_marker(&encoded).unwrap(), marker);

        let mut unknown_version = encoded.clone();
        unknown_version[0] = AUTHORITY_VALUE_FORMAT_VERSION + 1;
        assert!(matches!(
            decode_authority_marker(&unknown_version),
            Err(MetadataAuthorityCodecError::UnsupportedVersion { .. })
        ));

        let mut unknown = encoded.clone();
        unknown[1 + AUTHORITY_ID_BYTES + std::mem::size_of::<u64>()] = 0xff;
        assert_eq!(
            decode_authority_marker(&unknown),
            Err(MetadataAuthorityCodecError::UnknownState(0xff))
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            decode_authority_marker(&trailing),
            Err(MetadataAuthorityCodecError::InvalidLength { .. })
        ));

        assert!(MetadataAuthorityState::MigrationTarget
            .permits_transition_to(MetadataAuthorityState::Fenced));
        assert!(!MetadataAuthorityState::Quiescing
            .permits_transition_to(MetadataAuthorityState::Active));
    }

    #[test]
    fn standalone_identity_is_deterministic_and_contract_bound() {
        let shard = LogicalShardId::from_bytes([0x77; 16]);
        let first = MetadataStoreIdentity::standalone_holt_memory(shard);
        let second = MetadataStoreIdentity::standalone_holt_memory(shard);
        assert_eq!(first, second);
        assert_eq!(first.contract_digest, workspace_metadata_contract_digest());
        assert_eq!(
            hex_encode(workspace_metadata_contract_digest().as_bytes()),
            "b697ab5ed0fa5629a523337e0a3174e12dbe5a96f5c39122ee0bc6e172963eda"
        );
        assert_ne!(
            first,
            MetadataStoreIdentity::standalone_holt_memory(LogicalShardId::from_bytes([0x78; 16]))
        );
        assert_ne!(
            MetadataStoreIdentity::standalone_holt_file(shard, Path::new("store-a")),
            MetadataStoreIdentity::standalone_holt_file(shard, Path::new("store-b"))
        );
    }
}
