//! Durable, strictly ordered material for exporting and replaying metadata writes.
//!
//! The outbox is committed in the same transaction as the authoritative
//! mutation. It is not a second namespace state machine: a consumer replays the
//! decoded mutation through the ordinary [`MetaShard`] write entrypoints.

use std::fmt;

use nokv_types::{
    CommandDigest, CommitVersion, LogicalShardId, ObjectNamespaceId, OwnerEpoch,
    PlacementGeneration, ReadVersion, RequestId, RootActivationState, RootId,
};
use sha2::{Digest, Sha256};

use super::codec::SCHEMA_ID;
use super::engine::{
    CommandMutation, CommandPredicate, EventProjection, HistoryProjection, MetadataCommand,
    RootFenceAction,
};
use super::keyspace::MetadataFamily;

pub const RECOVERY_OUTBOX_VALUE_FORMAT_VERSION: u8 = 3;
const LEGACY_RECOVERY_OUTBOX_VALUE_FORMAT_VERSION: u8 = 2;
pub const RECOVERY_CHAIN_DIGEST_BYTES: usize = 32;
const MAX_ITEMS: usize = 256;
pub(crate) const MAX_RECOVERY_BYTES: usize = 64 * 1024 * 1024;
const GENESIS_DOMAIN: &[u8] = b"nokv.metadata.recovery.genesis.v1\0";
const CHAIN_DOMAIN: &[u8] = b"nokv.metadata.recovery.chain.v1\0";
const STORAGE_HEADER_VERSION: u8 = 1;
const STORAGE_CHUNK_VERSION: u8 = 1;
const LEGACY_STORAGE_HEADER_KEY_TAG: u8 = 0;
const LEGACY_STORAGE_CHUNK_KEY_TAG: u8 = 1;
const STORAGE_HEADER_KEY_TAG: u8 = 2;
const STORAGE_CHUNK_KEY_TAG: u8 = 3;
const RECOVERY_LSN_DECIMAL_BYTES: usize = 20;
const RECOVERY_OUTBOX_KEY_BYTES: usize = 1 + RECOVERY_LSN_DECIMAL_BYTES;
const RECOVERY_CHUNK_KEY_BYTES: usize = RECOVERY_OUTBOX_KEY_BYTES + 4;
const MAX_STORAGE_CHUNK_DATA_BYTES: usize = 60 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RecoveryKeyLayout {
    Format8,
    Format9,
}

impl RecoveryKeyLayout {
    pub(crate) const ORDERED: [Self; 2] = [Self::Format8, Self::Format9];

    pub(crate) const fn header_tag(self) -> u8 {
        match self {
            Self::Format8 => LEGACY_STORAGE_HEADER_KEY_TAG,
            Self::Format9 => STORAGE_HEADER_KEY_TAG,
        }
    }

    pub(crate) const fn from_header_tag(tag: u8) -> Option<Self> {
        match tag {
            LEGACY_STORAGE_HEADER_KEY_TAG => Some(Self::Format8),
            STORAGE_HEADER_KEY_TAG => Some(Self::Format9),
            _ => None,
        }
    }

    pub(crate) const fn from_chunk_tag(tag: u8) -> Option<Self> {
        match tag {
            LEGACY_STORAGE_CHUNK_KEY_TAG => Some(Self::Format8),
            STORAGE_CHUNK_KEY_TAG => Some(Self::Format9),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DecodedRecoveryOutboxKey {
    pub(crate) recovery_lsn: u64,
    pub(crate) layout: RecoveryKeyLayout,
}

/// One canonical input to a real metadata write entrypoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryMutationV1 {
    MetadataCommand {
        command: Box<MetadataCommand>,
        lease_deadline_ms: Option<u64>,
    },
    ObserveLeaseClock {
        root_id: RootId,
        placement_generation: PlacementGeneration,
        owner_epoch: OwnerEpoch,
        observed_ms: u64,
    },
    AdvanceOwnerEpoch {
        expected: Option<OwnerEpoch>,
        next: OwnerEpoch,
    },
}

/// Deterministic evidence returned by the write represented in the same row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryResultV1 {
    MetadataCommand {
        commit_version: CommitVersion,
        deterministic_result: Vec<u8>,
    },
    LeaseClock {
        effective_high_water_ms: u64,
    },
    OwnerEpoch {
        applied_owner_epoch: OwnerEpoch,
    },
}

/// One hash-chained recovery outbox row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryOutboxRecord {
    format_version: u8,
    pub recovery_lsn: u64,
    pub previous_chain_digest: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
    pub chain_digest: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
    pub mutation: RecoveryMutationV1,
    pub result: RecoveryResultV1,
}

/// Durable tail stored in `System` and atomically advanced with every row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryState {
    pub applied_recovery_lsn: u64,
    pub chain_digest: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveryStorageHeader {
    logical_length: u64,
    chunk_count: u32,
    logical_digest: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecoveryCodecError {
    UnsupportedVersion {
        actual: u8,
        expected: u8,
    },
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
    },
    ZeroScalar {
        field: &'static str,
    },
    InvalidValue {
        field: &'static str,
    },
    LengthOverflow {
        field: &'static str,
        length: usize,
    },
    LengthBound {
        field: &'static str,
        length: usize,
        max: usize,
    },
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
    ChainDigestMismatch,
}

impl fmt::Display for RecoveryCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual, expected } => write!(
                formatter,
                "unsupported recovery format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidValue { field } => write!(formatter, "invalid {field}"),
            Self::LengthOverflow { field, length } => {
                write!(formatter, "{field} length {length} exceeds u32")
            }
            Self::LengthBound { field, length, max } => {
                write!(formatter, "{field} length {length} exceeds {max}")
            }
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(formatter, "recovery value has {count} trailing bytes")
            }
            Self::ChainDigestMismatch => formatter.write_str("recovery chain digest mismatch"),
        }
    }
}

impl std::error::Error for RecoveryCodecError {}

impl RecoveryOutboxRecord {
    pub fn new(
        recovery_lsn: u64,
        previous_chain_digest: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
        mutation: RecoveryMutationV1,
        result: RecoveryResultV1,
    ) -> Result<Self, RecoveryCodecError> {
        if recovery_lsn == 0 {
            return Err(RecoveryCodecError::ZeroScalar {
                field: "recovery_lsn",
            });
        }
        validate_pair(&mutation, &result)?;
        let mutation_bytes = mutation.encode_canonical()?;
        let result_bytes = result.encode_canonical()?;
        let chain_digest = recovery_chain_digest(
            previous_chain_digest,
            recovery_lsn,
            &mutation_bytes,
            &result_bytes,
        );
        Ok(Self {
            format_version: RECOVERY_OUTBOX_VALUE_FORMAT_VERSION,
            recovery_lsn,
            previous_chain_digest,
            chain_digest,
            mutation,
            result,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, RecoveryCodecError> {
        validate_pair(&self.mutation, &self.result)?;
        let mutation = self
            .mutation
            .encode_canonical_for_version(self.format_version)?;
        let result = self.result.encode_canonical()?;
        let expected = recovery_chain_digest(
            self.previous_chain_digest,
            self.recovery_lsn,
            &mutation,
            &result,
        );
        if self.recovery_lsn == 0 {
            return Err(RecoveryCodecError::ZeroScalar {
                field: "recovery_lsn",
            });
        }
        if expected != self.chain_digest {
            return Err(RecoveryCodecError::ChainDigestMismatch);
        }
        let mutation_len = u32_len("mutation", mutation.len())?;
        let result_len = u32_len("result", result.len())?;
        let mut bytes = Vec::with_capacity(1 + 8 + 32 + 32 + 4 + mutation.len() + 4 + result.len());
        bytes.push(self.format_version);
        bytes.extend_from_slice(&self.recovery_lsn.to_be_bytes());
        bytes.extend_from_slice(&self.previous_chain_digest);
        bytes.extend_from_slice(&self.chain_digest);
        bytes.extend_from_slice(&mutation_len.to_be_bytes());
        bytes.extend_from_slice(&mutation);
        bytes.extend_from_slice(&result_len.to_be_bytes());
        bytes.extend_from_slice(&result);
        Ok(bytes)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RecoveryCodecError> {
        let mut decoder = Decoder::new(encoded);
        let format_version = decoder.recovery_version()?;
        let recovery_lsn = decoder.u64("recovery_lsn")?;
        if recovery_lsn == 0 {
            return Err(RecoveryCodecError::ZeroScalar {
                field: "recovery_lsn",
            });
        }
        let previous_chain_digest = decoder.fixed("previous_chain_digest")?;
        let chain_digest = decoder.fixed("chain_digest")?;
        let mutation = RecoveryMutationV1::decode_canonical_for_version(
            decoder.bytes("mutation")?,
            format_version,
        )?;
        let result = RecoveryResultV1::decode_canonical(decoder.bytes("result")?)?;
        decoder.finish()?;
        let record = Self {
            format_version,
            recovery_lsn,
            previous_chain_digest,
            chain_digest,
            mutation,
            result,
        };
        validate_pair(&record.mutation, &record.result)?;
        let mutation_bytes = record
            .mutation
            .encode_canonical_for_version(record.format_version)?;
        let result_bytes = record.result.encode_canonical()?;
        if recovery_chain_digest(
            record.previous_chain_digest,
            record.recovery_lsn,
            &mutation_bytes,
            &result_bytes,
        ) != record.chain_digest
        {
            return Err(RecoveryCodecError::ChainDigestMismatch);
        }
        Ok(record)
    }
}

impl RecoveryMutationV1 {
    pub fn encode_canonical(&self) -> Result<Vec<u8>, RecoveryCodecError> {
        self.encode_canonical_for_version(RECOVERY_OUTBOX_VALUE_FORMAT_VERSION)
    }

    fn encode_canonical_for_version(
        &self,
        format_version: u8,
    ) -> Result<Vec<u8>, RecoveryCodecError> {
        let mut bytes = Vec::new();
        match self {
            Self::MetadataCommand {
                command,
                lease_deadline_ms,
            } => {
                if command.schema_id != SCHEMA_ID
                    || command.command_digest != command.canonical_digest()
                    || lease_deadline_ms == &Some(0)
                {
                    return Err(RecoveryCodecError::InvalidValue {
                        field: "metadata_command",
                    });
                }
                bytes.push(1);
                put_bytes(&mut bytes, "schema_id", command.schema_id.as_bytes())?;
                bytes.extend_from_slice(command.root_id.as_bytes());
                bytes.extend_from_slice(command.logical_shard_id.as_bytes());
                match (format_version, command.object_namespace_id) {
                    (LEGACY_RECOVERY_OUTBOX_VALUE_FORMAT_VERSION, None) => {}
                    (RECOVERY_OUTBOX_VALUE_FORMAT_VERSION, Some(object_namespace_id)) => {
                        bytes.extend_from_slice(object_namespace_id.as_bytes());
                    }
                    _ => {
                        return Err(RecoveryCodecError::InvalidValue {
                            field: "object_namespace_id",
                        });
                    }
                }
                bytes.extend_from_slice(&command.placement_generation.get().to_be_bytes());
                bytes.extend_from_slice(&command.owner_epoch.get().to_be_bytes());
                bytes.extend_from_slice(command.request_id.as_bytes());
                bytes.extend_from_slice(command.command_digest.as_bytes());
                bytes.extend_from_slice(&command.read_version.get().to_be_bytes());
                encode_root_action(&mut bytes, &command.root_fence_action);
                put_count(&mut bytes, "predicates", command.predicates.len())?;
                for predicate in &command.predicates {
                    encode_predicate(&mut bytes, predicate)?;
                }
                put_count(&mut bytes, "mutations", command.mutations.len())?;
                for mutation in &command.mutations {
                    encode_mutation(&mut bytes, mutation)?;
                }
                put_count(
                    &mut bytes,
                    "history_projection",
                    command.history_projection.len(),
                )?;
                for projection in &command.history_projection {
                    bytes.push(projection.family.format_tag());
                    put_bytes(&mut bytes, "history_key", &projection.key)?;
                }
                put_count(
                    &mut bytes,
                    "event_projection",
                    command.event_projection.len(),
                )?;
                for projection in &command.event_projection {
                    put_bytes(&mut bytes, "event_payload", &projection.payload)?;
                }
                put_bytes(
                    &mut bytes,
                    "deterministic_result",
                    &command.deterministic_result,
                )?;
                encode_optional_u64(&mut bytes, *lease_deadline_ms);
            }
            Self::ObserveLeaseClock {
                root_id,
                placement_generation,
                owner_epoch,
                observed_ms,
            } => {
                if *observed_ms == 0 {
                    return Err(RecoveryCodecError::ZeroScalar {
                        field: "observed_ms",
                    });
                }
                bytes.push(2);
                bytes.extend_from_slice(root_id.as_bytes());
                bytes.extend_from_slice(&placement_generation.get().to_be_bytes());
                bytes.extend_from_slice(&owner_epoch.get().to_be_bytes());
                bytes.extend_from_slice(&observed_ms.to_be_bytes());
            }
            Self::AdvanceOwnerEpoch { expected, next } => {
                if expected.is_some_and(|expected| expected.get() >= next.get()) {
                    return Err(RecoveryCodecError::InvalidValue {
                        field: "owner_epoch_transition",
                    });
                }
                bytes.push(3);
                encode_optional_u64(&mut bytes, expected.map(OwnerEpoch::get));
                bytes.extend_from_slice(&next.get().to_be_bytes());
            }
        }
        if bytes.len() > MAX_RECOVERY_BYTES {
            return Err(RecoveryCodecError::LengthBound {
                field: "mutation",
                length: bytes.len(),
                max: MAX_RECOVERY_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn decode_canonical(encoded: &[u8]) -> Result<Self, RecoveryCodecError> {
        Self::decode_canonical_for_version(encoded, RECOVERY_OUTBOX_VALUE_FORMAT_VERSION)
    }

    fn decode_canonical_for_version(
        encoded: &[u8],
        format_version: u8,
    ) -> Result<Self, RecoveryCodecError> {
        if encoded.len() > MAX_RECOVERY_BYTES {
            return Err(RecoveryCodecError::LengthBound {
                field: "mutation",
                length: encoded.len(),
                max: MAX_RECOVERY_BYTES,
            });
        }
        let mut decoder = Decoder::new(encoded);
        let mutation = match decoder.u8("mutation_kind")? {
            1 => {
                let schema_id = String::from_utf8(decoder.bytes("schema_id")?.to_vec())
                    .map_err(|_| RecoveryCodecError::InvalidValue { field: "schema_id" })?;
                let root_id = RootId::from_bytes(decoder.fixed("root_id")?);
                let logical_shard_id =
                    LogicalShardId::from_bytes(decoder.fixed("logical_shard_id")?);
                let object_namespace_id = match format_version {
                    LEGACY_RECOVERY_OUTBOX_VALUE_FORMAT_VERSION => None,
                    RECOVERY_OUTBOX_VALUE_FORMAT_VERSION => Some(ObjectNamespaceId::from_bytes(
                        decoder.fixed("object_namespace_id")?,
                    )),
                    _ => {
                        return Err(RecoveryCodecError::UnsupportedVersion {
                            actual: format_version,
                            expected: RECOVERY_OUTBOX_VALUE_FORMAT_VERSION,
                        });
                    }
                };
                let placement_generation =
                    nonzero_generation(decoder.u64("placement_generation")?)?;
                let owner_epoch = nonzero_owner(decoder.u64("owner_epoch")?, "owner_epoch")?;
                let request_id = RequestId::from_bytes(decoder.fixed("request_id")?);
                let command_digest = CommandDigest::from_bytes(decoder.fixed("command_digest")?);
                let read_version =
                    ReadVersion::new(decoder.u64("read_version")?).map_err(|_| {
                        RecoveryCodecError::ZeroScalar {
                            field: "read_version",
                        }
                    })?;
                let root_fence_action = decode_root_action(&mut decoder)?;
                let predicate_count = decoder.count("predicates")?;
                let mut predicates = Vec::with_capacity(predicate_count);
                for _ in 0..predicate_count {
                    predicates.push(decode_predicate(&mut decoder)?);
                }
                let mutation_count = decoder.count("mutations")?;
                let mut mutations = Vec::with_capacity(mutation_count);
                for _ in 0..mutation_count {
                    mutations.push(decode_mutation(&mut decoder)?);
                }
                let history_count = decoder.count("history_projection")?;
                let mut history_projection = Vec::with_capacity(history_count);
                for _ in 0..history_count {
                    history_projection.push(HistoryProjection {
                        family: decode_family(decoder.u8("history_family")?)?,
                        key: decoder.bytes("history_key")?.to_vec(),
                    });
                }
                let event_count = decoder.count("event_projection")?;
                let mut event_projection = Vec::with_capacity(event_count);
                for _ in 0..event_count {
                    event_projection.push(EventProjection {
                        payload: decoder.bytes("event_payload")?.to_vec(),
                    });
                }
                let deterministic_result = decoder.bytes("deterministic_result")?.to_vec();
                let lease_deadline_ms = decoder.optional_u64("lease_deadline_ms")?;
                let command = MetadataCommand {
                    schema_id,
                    root_id,
                    logical_shard_id,
                    object_namespace_id,
                    placement_generation,
                    owner_epoch,
                    request_id,
                    command_digest,
                    read_version,
                    root_fence_action,
                    predicates,
                    mutations,
                    history_projection,
                    event_projection,
                    deterministic_result,
                };
                if command.command_digest != command.canonical_digest() {
                    return Err(RecoveryCodecError::InvalidValue {
                        field: "command_digest",
                    });
                }
                Self::MetadataCommand {
                    command: Box::new(command),
                    lease_deadline_ms,
                }
            }
            2 => Self::ObserveLeaseClock {
                root_id: RootId::from_bytes(decoder.fixed("root_id")?),
                placement_generation: nonzero_generation(decoder.u64("placement_generation")?)?,
                owner_epoch: nonzero_owner(decoder.u64("owner_epoch")?, "owner_epoch")?,
                observed_ms: decoder.u64("observed_ms")?,
            },
            3 => Self::AdvanceOwnerEpoch {
                expected: decoder
                    .optional_u64("expected_owner_epoch")?
                    .map(|value| nonzero_owner(value, "expected_owner_epoch"))
                    .transpose()?,
                next: nonzero_owner(decoder.u64("next_owner_epoch")?, "next_owner_epoch")?,
            },
            value => {
                return Err(RecoveryCodecError::UnknownDiscriminant {
                    type_name: "RecoveryMutationV1",
                    value,
                })
            }
        };
        decoder.finish()?;
        if mutation.encode_canonical_for_version(format_version)? != encoded {
            return Err(RecoveryCodecError::InvalidValue {
                field: "canonical_mutation",
            });
        }
        Ok(mutation)
    }
}

impl RecoveryResultV1 {
    pub fn encode_canonical(&self) -> Result<Vec<u8>, RecoveryCodecError> {
        let mut bytes = Vec::new();
        match self {
            Self::MetadataCommand {
                commit_version,
                deterministic_result,
            } => {
                bytes.push(1);
                bytes.extend_from_slice(&commit_version.get().to_be_bytes());
                put_bytes(&mut bytes, "deterministic_result", deterministic_result)?;
            }
            Self::LeaseClock {
                effective_high_water_ms,
            } => {
                bytes.push(2);
                bytes.extend_from_slice(&effective_high_water_ms.to_be_bytes());
            }
            Self::OwnerEpoch {
                applied_owner_epoch,
            } => {
                bytes.push(3);
                bytes.extend_from_slice(&applied_owner_epoch.get().to_be_bytes());
            }
        }
        if bytes.len() > MAX_RECOVERY_BYTES {
            return Err(RecoveryCodecError::LengthBound {
                field: "result",
                length: bytes.len(),
                max: MAX_RECOVERY_BYTES,
            });
        }
        Ok(bytes)
    }

    pub fn decode_canonical(encoded: &[u8]) -> Result<Self, RecoveryCodecError> {
        if encoded.len() > MAX_RECOVERY_BYTES {
            return Err(RecoveryCodecError::LengthBound {
                field: "result",
                length: encoded.len(),
                max: MAX_RECOVERY_BYTES,
            });
        }
        let mut decoder = Decoder::new(encoded);
        let result = match decoder.u8("result_kind")? {
            1 => Self::MetadataCommand {
                commit_version: CommitVersion::new(decoder.u64("commit_version")?).map_err(
                    |_| RecoveryCodecError::ZeroScalar {
                        field: "commit_version",
                    },
                )?,
                deterministic_result: decoder.bytes("deterministic_result")?.to_vec(),
            },
            2 => Self::LeaseClock {
                effective_high_water_ms: decoder.u64("effective_high_water_ms")?,
            },
            3 => Self::OwnerEpoch {
                applied_owner_epoch: nonzero_owner(
                    decoder.u64("applied_owner_epoch")?,
                    "applied_owner_epoch",
                )?,
            },
            value => {
                return Err(RecoveryCodecError::UnknownDiscriminant {
                    type_name: "RecoveryResultV1",
                    value,
                })
            }
        };
        decoder.finish()?;
        if result.encode_canonical()? != encoded {
            return Err(RecoveryCodecError::InvalidValue {
                field: "canonical_result",
            });
        }
        Ok(result)
    }
}

pub(crate) fn recovery_genesis_digest(
    logical_shard_id: LogicalShardId,
) -> [u8; RECOVERY_CHAIN_DIGEST_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(GENESIS_DOMAIN);
    hasher.update(logical_shard_id.as_bytes());
    hasher.finalize().into()
}

/// Format-v9 header key. Fixed-width decimal preserves numeric order while
/// keeping sequential LSNs in Holt's well-filled key shape.
pub(crate) fn recovery_outbox_key(recovery_lsn: u64) -> [u8; RECOVERY_OUTBOX_KEY_BYTES] {
    let mut key = [0; RECOVERY_OUTBOX_KEY_BYTES];
    key[0] = STORAGE_HEADER_KEY_TAG;
    key[1..].copy_from_slice(&encode_recovery_lsn(recovery_lsn));
    key
}

pub(crate) fn recovery_outbox_scan_start(layout: RecoveryKeyLayout, recovery_lsn: u64) -> Vec<u8> {
    match layout {
        RecoveryKeyLayout::Format8 => legacy_recovery_outbox_key(recovery_lsn).to_vec(),
        RecoveryKeyLayout::Format9 => recovery_outbox_key(recovery_lsn).to_vec(),
    }
}

pub(crate) fn decode_recovery_outbox_key(
    key: &[u8],
) -> Result<DecodedRecoveryOutboxKey, RecoveryCodecError> {
    let (layout, lsn) = match key.first().copied() {
        Some(LEGACY_STORAGE_HEADER_KEY_TAG) if key.len() == 9 => (
            RecoveryKeyLayout::Format8,
            u64::from_be_bytes(key[1..].try_into().expect("width checked")),
        ),
        Some(STORAGE_HEADER_KEY_TAG) if key.len() == RECOVERY_OUTBOX_KEY_BYTES => {
            (RecoveryKeyLayout::Format9, decode_recovery_lsn(&key[1..])?)
        }
        _ => {
            return Err(RecoveryCodecError::InvalidValue {
                field: "recovery_outbox_key",
            });
        }
    };
    if lsn == 0 {
        return Err(RecoveryCodecError::ZeroScalar {
            field: "recovery_lsn",
        });
    }
    Ok(DecodedRecoveryOutboxKey {
        recovery_lsn: lsn,
        layout,
    })
}

fn legacy_recovery_outbox_key(recovery_lsn: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = LEGACY_STORAGE_HEADER_KEY_TAG;
    key[1..].copy_from_slice(&recovery_lsn.to_be_bytes());
    key
}

fn encode_recovery_lsn(mut recovery_lsn: u64) -> [u8; RECOVERY_LSN_DECIMAL_BYTES] {
    let mut encoded = [b'0'; RECOVERY_LSN_DECIMAL_BYTES];
    for digit in encoded.iter_mut().rev() {
        *digit = b'0' + (recovery_lsn % 10) as u8;
        recovery_lsn /= 10;
    }
    debug_assert_eq!(recovery_lsn, 0);
    encoded
}

fn decode_recovery_lsn(encoded: &[u8]) -> Result<u64, RecoveryCodecError> {
    let mut recovery_lsn = 0_u64;
    for digit in encoded {
        if !digit.is_ascii_digit() {
            return Err(RecoveryCodecError::InvalidValue {
                field: "recovery_outbox_key",
            });
        }
        recovery_lsn = recovery_lsn
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(digit - b'0')))
            .ok_or(RecoveryCodecError::InvalidValue {
                field: "recovery_outbox_key",
            })?;
    }
    Ok(recovery_lsn)
}

pub(crate) fn split_recovery_storage(
    logical: &[u8],
) -> Result<(Vec<u8>, Vec<Vec<u8>>), RecoveryCodecError> {
    let chunk_count = logical.len().div_ceil(MAX_STORAGE_CHUNK_DATA_BYTES);
    let chunk_count_u32 = u32_len("recovery_chunk_count", chunk_count)?;
    let header = RecoveryStorageHeader {
        logical_length: logical.len() as u64,
        chunk_count: chunk_count_u32,
        logical_digest: Sha256::digest(logical).into(),
    }
    .encode();
    let chunks = logical
        .chunks(MAX_STORAGE_CHUNK_DATA_BYTES)
        .enumerate()
        .map(|(index, data)| encode_storage_chunk(index as u32, chunk_count_u32, data))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((header, chunks))
}

pub(crate) fn recovery_chunk_key(recovery_lsn: u64, index: u32) -> [u8; RECOVERY_CHUNK_KEY_BYTES] {
    let mut key = [0; RECOVERY_CHUNK_KEY_BYTES];
    key[0] = STORAGE_CHUNK_KEY_TAG;
    key[1..RECOVERY_OUTBOX_KEY_BYTES].copy_from_slice(&encode_recovery_lsn(recovery_lsn));
    key[RECOVERY_OUTBOX_KEY_BYTES..].copy_from_slice(&index.to_be_bytes());
    key
}

pub(crate) fn recovery_chunk_key_for_layout(
    layout: RecoveryKeyLayout,
    recovery_lsn: u64,
    index: u32,
) -> Vec<u8> {
    match layout {
        RecoveryKeyLayout::Format8 => {
            let mut key = [0; 13];
            key[0] = LEGACY_STORAGE_CHUNK_KEY_TAG;
            key[1..9].copy_from_slice(&recovery_lsn.to_be_bytes());
            key[9..].copy_from_slice(&index.to_be_bytes());
            key.to_vec()
        }
        RecoveryKeyLayout::Format9 => recovery_chunk_key(recovery_lsn, index).to_vec(),
    }
}

pub(crate) fn assemble_recovery_storage(
    header: &[u8],
    chunks: impl IntoIterator<Item = Vec<u8>>,
) -> Result<Vec<u8>, RecoveryCodecError> {
    let header = RecoveryStorageHeader::decode(header)?;
    let mut logical = Vec::with_capacity(usize::try_from(header.logical_length).map_err(|_| {
        RecoveryCodecError::LengthOverflow {
            field: "recovery_logical_length",
            length: usize::MAX,
        }
    })?);
    let mut observed = 0_u32;
    for chunk in chunks {
        let data = decode_storage_chunk(&chunk, observed, header.chunk_count)?;
        logical.extend_from_slice(data);
        observed = observed
            .checked_add(1)
            .ok_or(RecoveryCodecError::LengthOverflow {
                field: "recovery_chunk_count",
                length: usize::MAX,
            })?;
    }
    if observed != header.chunk_count || logical.len() as u64 != header.logical_length {
        return Err(RecoveryCodecError::InvalidValue {
            field: "recovery_storage_shape",
        });
    }
    if <[u8; RECOVERY_CHAIN_DIGEST_BYTES]>::from(Sha256::digest(&logical)) != header.logical_digest
    {
        return Err(RecoveryCodecError::InvalidValue {
            field: "recovery_storage_digest",
        });
    }
    Ok(logical)
}

pub(crate) fn recovery_storage_chunk_count(header: &[u8]) -> Result<u32, RecoveryCodecError> {
    Ok(RecoveryStorageHeader::decode(header)?.chunk_count)
}

pub(crate) fn recovery_storage_logical_length(header: &[u8]) -> Result<usize, RecoveryCodecError> {
    let logical_length = RecoveryStorageHeader::decode(header)?.logical_length;
    usize::try_from(logical_length).map_err(|_| RecoveryCodecError::LengthOverflow {
        field: "recovery_logical_length",
        length: usize::MAX,
    })
}

impl RecoveryStorageHeader {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + 8 + 4 + 32);
        encoded.push(STORAGE_HEADER_VERSION);
        encoded.extend_from_slice(&self.logical_length.to_be_bytes());
        encoded.extend_from_slice(&self.chunk_count.to_be_bytes());
        encoded.extend_from_slice(&self.logical_digest);
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, RecoveryCodecError> {
        let mut decoder = Decoder::new(encoded);
        let actual = decoder.u8("storage_header_version")?;
        if actual != STORAGE_HEADER_VERSION {
            return Err(RecoveryCodecError::UnsupportedVersion {
                actual,
                expected: STORAGE_HEADER_VERSION,
            });
        }
        let logical_length = decoder.u64("recovery_logical_length")?;
        let chunk_count = decoder.u32("recovery_chunk_count")?;
        let logical_digest = decoder.fixed("recovery_logical_digest")?;
        decoder.finish()?;
        if logical_length == 0
            || logical_length > MAX_RECOVERY_BYTES as u64
            || chunk_count == 0
            || u64::from(chunk_count)
                != logical_length.div_ceil(MAX_STORAGE_CHUNK_DATA_BYTES as u64)
        {
            return Err(RecoveryCodecError::InvalidValue {
                field: "recovery_storage_shape",
            });
        }
        Ok(Self {
            logical_length,
            chunk_count,
            logical_digest,
        })
    }
}

fn encode_storage_chunk(
    index: u32,
    total: u32,
    data: &[u8],
) -> Result<Vec<u8>, RecoveryCodecError> {
    if data.is_empty() || data.len() > MAX_STORAGE_CHUNK_DATA_BYTES || index >= total {
        return Err(RecoveryCodecError::InvalidValue {
            field: "recovery_storage_chunk",
        });
    }
    let mut encoded = Vec::with_capacity(1 + 4 + 4 + 4 + data.len());
    encoded.push(STORAGE_CHUNK_VERSION);
    encoded.extend_from_slice(&index.to_be_bytes());
    encoded.extend_from_slice(&total.to_be_bytes());
    encoded.extend_from_slice(&u32_len("recovery_chunk_data", data.len())?.to_be_bytes());
    encoded.extend_from_slice(data);
    Ok(encoded)
}

fn decode_storage_chunk(
    encoded: &[u8],
    expected_index: u32,
    expected_total: u32,
) -> Result<&[u8], RecoveryCodecError> {
    let mut decoder = Decoder::new(encoded);
    let actual = decoder.u8("storage_chunk_version")?;
    if actual != STORAGE_CHUNK_VERSION {
        return Err(RecoveryCodecError::UnsupportedVersion {
            actual,
            expected: STORAGE_CHUNK_VERSION,
        });
    }
    let index = decoder.u32("recovery_chunk_index")?;
    let total = decoder.u32("recovery_chunk_total")?;
    let data = decoder.bytes("recovery_chunk_data")?;
    decoder.finish()?;
    if index != expected_index
        || total != expected_total
        || data.is_empty()
        || data.len() > MAX_STORAGE_CHUNK_DATA_BYTES
    {
        return Err(RecoveryCodecError::InvalidValue {
            field: "recovery_storage_chunk",
        });
    }
    Ok(data)
}

fn recovery_chain_digest(
    previous: [u8; RECOVERY_CHAIN_DIGEST_BYTES],
    recovery_lsn: u64,
    mutation: &[u8],
    result: &[u8],
) -> [u8; RECOVERY_CHAIN_DIGEST_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(CHAIN_DOMAIN);
    hasher.update(previous);
    hasher.update(recovery_lsn.to_be_bytes());
    hash_bytes(&mut hasher, mutation);
    hash_bytes(&mut hasher, result);
    hasher.finalize().into()
}

fn validate_pair(
    mutation: &RecoveryMutationV1,
    result: &RecoveryResultV1,
) -> Result<(), RecoveryCodecError> {
    match (mutation, result) {
        (
            RecoveryMutationV1::MetadataCommand { command, .. },
            RecoveryResultV1::MetadataCommand {
                commit_version,
                deterministic_result,
            },
        ) if deterministic_result == &command.deterministic_result
            && command.read_version.get().checked_add(1) == Some(commit_version.get()) =>
        {
            Ok(())
        }
        (
            RecoveryMutationV1::ObserveLeaseClock { observed_ms, .. },
            RecoveryResultV1::LeaseClock {
                effective_high_water_ms,
            },
        ) if *observed_ms != 0 && observed_ms == effective_high_water_ms => Ok(()),
        (
            RecoveryMutationV1::AdvanceOwnerEpoch { expected, next },
            RecoveryResultV1::OwnerEpoch {
                applied_owner_epoch,
            },
        ) if next == applied_owner_epoch
            && expected.is_none_or(|expected| expected.get() < next.get()) =>
        {
            Ok(())
        }
        _ => Err(RecoveryCodecError::InvalidValue {
            field: "mutation_result_pair",
        }),
    }
}

fn encode_root_action(bytes: &mut Vec<u8>, action: &RootFenceAction) {
    match action {
        RootFenceAction::Install => bytes.push(1),
        RootFenceAction::BindObjectNamespace { expected } => {
            bytes.push(4);
            bytes.push((*expected).into());
        }
        RootFenceAction::RequireActive => bytes.push(2),
        RootFenceAction::Transition { expected, next } => {
            bytes.push(3);
            bytes.push((*expected).into());
            bytes.push((*next).into());
        }
    }
}

fn decode_root_action(decoder: &mut Decoder<'_>) -> Result<RootFenceAction, RecoveryCodecError> {
    match decoder.u8("root_fence_action")? {
        1 => Ok(RootFenceAction::Install),
        2 => Ok(RootFenceAction::RequireActive),
        3 => Ok(RootFenceAction::Transition {
            expected: decode_root_state(decoder.u8("expected_root_state")?)?,
            next: decode_root_state(decoder.u8("next_root_state")?)?,
        }),
        4 => Ok(RootFenceAction::BindObjectNamespace {
            expected: decode_root_state(decoder.u8("expected_root_state")?)?,
        }),
        value => Err(RecoveryCodecError::UnknownDiscriminant {
            type_name: "RootFenceAction",
            value,
        }),
    }
}

fn encode_predicate(
    bytes: &mut Vec<u8>,
    predicate: &CommandPredicate,
) -> Result<(), RecoveryCodecError> {
    match predicate {
        CommandPredicate::Value {
            family,
            key,
            expected,
        } => {
            bytes.push(1);
            bytes.push(family.format_tag());
            put_bytes(bytes, "predicate_key", key)?;
            match expected {
                None => bytes.push(0),
                Some(value) => {
                    bytes.push(1);
                    put_bytes(bytes, "predicate_value", value)?;
                }
            }
        }
        CommandPredicate::PrefixEmpty { family, prefix } => {
            bytes.push(2);
            bytes.push(family.format_tag());
            put_bytes(bytes, "predicate_prefix", prefix)?;
        }
    }
    Ok(())
}

fn decode_predicate(decoder: &mut Decoder<'_>) -> Result<CommandPredicate, RecoveryCodecError> {
    match decoder.u8("predicate_kind")? {
        1 => {
            let family = decode_family(decoder.u8("predicate_family")?)?;
            let key = decoder.bytes("predicate_key")?.to_vec();
            let expected = match decoder.u8("predicate_value_tag")? {
                0 => None,
                1 => Some(decoder.bytes("predicate_value")?.to_vec()),
                value => {
                    return Err(RecoveryCodecError::UnknownDiscriminant {
                        type_name: "predicate_value_tag",
                        value,
                    })
                }
            };
            Ok(CommandPredicate::Value {
                family,
                key,
                expected,
            })
        }
        2 => Ok(CommandPredicate::PrefixEmpty {
            family: decode_family(decoder.u8("predicate_family")?)?,
            prefix: decoder.bytes("predicate_prefix")?.to_vec(),
        }),
        value => Err(RecoveryCodecError::UnknownDiscriminant {
            type_name: "CommandPredicate",
            value,
        }),
    }
}

fn encode_mutation(
    bytes: &mut Vec<u8>,
    mutation: &CommandMutation,
) -> Result<(), RecoveryCodecError> {
    match mutation {
        CommandMutation::Put { family, key, value } => {
            bytes.push(1);
            bytes.push(family.format_tag());
            put_bytes(bytes, "mutation_key", key)?;
            put_bytes(bytes, "mutation_value", value)?;
        }
        CommandMutation::Delete { family, key } => {
            bytes.push(2);
            bytes.push(family.format_tag());
            put_bytes(bytes, "mutation_key", key)?;
        }
    }
    Ok(())
}

fn decode_mutation(decoder: &mut Decoder<'_>) -> Result<CommandMutation, RecoveryCodecError> {
    match decoder.u8("command_mutation_kind")? {
        1 => Ok(CommandMutation::Put {
            family: decode_family(decoder.u8("mutation_family")?)?,
            key: decoder.bytes("mutation_key")?.to_vec(),
            value: decoder.bytes("mutation_value")?.to_vec(),
        }),
        2 => Ok(CommandMutation::Delete {
            family: decode_family(decoder.u8("mutation_family")?)?,
            key: decoder.bytes("mutation_key")?.to_vec(),
        }),
        value => Err(RecoveryCodecError::UnknownDiscriminant {
            type_name: "CommandMutation",
            value,
        }),
    }
}

fn decode_family(value: u8) -> Result<MetadataFamily, RecoveryCodecError> {
    MetadataFamily::from_format_tag(value).ok_or(RecoveryCodecError::UnknownDiscriminant {
        type_name: "MetadataFamily",
        value,
    })
}

fn decode_root_state(value: u8) -> Result<RootActivationState, RecoveryCodecError> {
    RootActivationState::try_from(value).map_err(|_| RecoveryCodecError::UnknownDiscriminant {
        type_name: "RootActivationState",
        value,
    })
}

fn nonzero_generation(value: u64) -> Result<PlacementGeneration, RecoveryCodecError> {
    PlacementGeneration::new(value).map_err(|_| RecoveryCodecError::ZeroScalar {
        field: "placement_generation",
    })
}

fn nonzero_owner(value: u64, field: &'static str) -> Result<OwnerEpoch, RecoveryCodecError> {
    OwnerEpoch::new(value).map_err(|_| RecoveryCodecError::ZeroScalar { field })
}

fn put_count(
    bytes: &mut Vec<u8>,
    field: &'static str,
    count: usize,
) -> Result<(), RecoveryCodecError> {
    if count > MAX_ITEMS {
        return Err(RecoveryCodecError::LengthBound {
            field,
            length: count,
            max: MAX_ITEMS,
        });
    }
    bytes.extend_from_slice(&u32_len(field, count)?.to_be_bytes());
    Ok(())
}

fn put_bytes(
    target: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
) -> Result<(), RecoveryCodecError> {
    target.extend_from_slice(&u32_len(field, value.len())?.to_be_bytes());
    target.extend_from_slice(value);
    Ok(())
}

fn u32_len(field: &'static str, length: usize) -> Result<u32, RecoveryCodecError> {
    u32::try_from(length).map_err(|_| RecoveryCodecError::LengthOverflow { field, length })
}

fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
    }
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn recovery_version(&mut self) -> Result<u8, RecoveryCodecError> {
        let actual = self.u8("value_format_version")?;
        if actual == LEGACY_RECOVERY_OUTBOX_VALUE_FORMAT_VERSION
            || actual == RECOVERY_OUTBOX_VALUE_FORMAT_VERSION
        {
            Ok(actual)
        } else {
            Err(RecoveryCodecError::UnsupportedVersion {
                actual,
                expected: RECOVERY_OUTBOX_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, RecoveryCodecError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, RecoveryCodecError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, RecoveryCodecError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn count(&mut self, field: &'static str) -> Result<usize, RecoveryCodecError> {
        let count = self.u32(field)? as usize;
        if count > MAX_ITEMS {
            return Err(RecoveryCodecError::LengthBound {
                field,
                length: count,
                max: MAX_ITEMS,
            });
        }
        Ok(count)
    }

    fn optional_u64(&mut self, field: &'static str) -> Result<Option<u64>, RecoveryCodecError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => Ok(Some(self.u64(field)?)),
            value => Err(RecoveryCodecError::UnknownDiscriminant {
                type_name: field,
                value,
            }),
        }
    }

    fn bytes(&mut self, field: &'static str) -> Result<&'a [u8], RecoveryCodecError> {
        let length = self.u32(field)? as usize;
        if length > MAX_RECOVERY_BYTES {
            return Err(RecoveryCodecError::LengthBound {
                field,
                length,
                max: MAX_RECOVERY_BYTES,
            });
        }
        self.take(field, length)
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], RecoveryCodecError> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(field, N)?);
        Ok(value)
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], RecoveryCodecError> {
        let remaining = self.encoded.len().saturating_sub(self.offset);
        let end = self
            .offset
            .checked_add(length)
            .ok_or(RecoveryCodecError::Truncated {
                field,
                needed: length,
                remaining,
            })?;
        if end > self.encoded.len() {
            return Err(RecoveryCodecError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let value = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), RecoveryCodecError> {
        let count = self.encoded.len() - self.offset;
        if count == 0 {
            Ok(())
        } else {
            Err(RecoveryCodecError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_encode(bytes: &[u8]) -> String {
        use std::fmt::Write as _;

        let mut encoded = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
        encoded
    }

    fn hex_decode(encoded: &str) -> Vec<u8> {
        encoded
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("test fixture contains non-hex input"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    fn command() -> MetadataCommand {
        let root = RootId::from_bytes([1; 16]);
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root,
            logical_shard_id: LogicalShardId::from_bytes([2; 16]),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([8; 16])),
            placement_generation: PlacementGeneration::new(3).unwrap(),
            owner_epoch: OwnerEpoch::new(4).unwrap(),
            request_id: RequestId::from_bytes([5; 16]),
            command_digest: CommandDigest::from_bytes([0; 32]),
            read_version: ReadVersion::new(6).unwrap(),
            root_fence_action: RootFenceAction::RequireActive,
            predicates: vec![CommandPredicate::Value {
                family: MetadataFamily::Operation,
                key: [root.as_bytes().as_slice(), b"key"].concat(),
                expected: Some(b"before".to_vec()),
            }],
            mutations: vec![CommandMutation::Put {
                family: MetadataFamily::Operation,
                key: [root.as_bytes().as_slice(), b"key"].concat(),
                value: b"after".to_vec(),
            }],
            history_projection: vec![HistoryProjection {
                family: MetadataFamily::Operation,
                key: [root.as_bytes().as_slice(), b"key"].concat(),
            }],
            event_projection: vec![EventProjection {
                payload: b"event".to_vec(),
            }],
            deterministic_result: b"result".to_vec(),
        }
        .seal()
    }

    #[test]
    fn recovery_record_has_frozen_golden_and_strict_codec() {
        let mutation = RecoveryMutationV1::MetadataCommand {
            command: Box::new(command()),
            lease_deadline_ms: Some(99),
        };
        let result = RecoveryResultV1::MetadataCommand {
            commit_version: CommitVersion::new(7).unwrap(),
            deterministic_result: b"result".to_vec(),
        };
        let record = RecoveryOutboxRecord::new(8, [0x11; 32], mutation, result).unwrap();
        let encoded = record.encode().unwrap();
        assert_eq!(hex_encode(&encoded), "030000000000000008111111111111111111111111111111111111111111111111111111111111111167bcc73b77c67e87b06fe9b72e888d6ee6ed531065032bd8a36567af7648db8800000116010000000e6e6f6b765f776f726b73706163650101010101010101010101010101010102020202020202020202020202020202080808080808080808080808080808080000000000000003000000000000000405050505050505050505050505050505a5a3252ff9a303877dbcc8ca4a4bff31d0e14b1e19982ed20a06419d80b9a08700000000000000060200000001011100000013010101010101010101010101010101016b657901000000066265666f726500000001011100000013010101010101010101010101010101016b6579000000056166746572000000011100000013010101010101010101010101010101016b657900000001000000056576656e7400000006726573756c740100000000000000630000001301000000000000000700000006726573756c74");
        assert_eq!(RecoveryOutboxRecord::decode(&encoded).unwrap(), record);

        let mut unknown = encoded.clone();
        unknown[0] = 1;
        assert!(matches!(
            RecoveryOutboxRecord::decode(&unknown),
            Err(RecoveryCodecError::UnsupportedVersion { .. })
        ));
        let mut corrupted = encoded;
        corrupted[1 + 8 + 32] ^= 1;
        assert_eq!(
            RecoveryOutboxRecord::decode(&corrupted),
            Err(RecoveryCodecError::ChainDigestMismatch)
        );
    }

    #[test]
    fn recovery_record_decodes_and_preserves_v2_without_namespace_bytes() {
        let legacy = hex_decode("0200000000000000081111111111111111111111111111111111111111111111111111111111111111f5e3ab61f72ceba583774bcd1e8d98c4f38621f8c5682c7586ac004c35d25d4200000106010000000e6e6f6b765f776f726b737061636501010101010101010101010101010101020202020202020202020202020202020000000000000003000000000000000405050505050505050505050505050505caf0269cc277bd5e16dbed022e7a3fdb4d49948b467ed050725cd6fdbcfc72df00000000000000060200000001011100000013010101010101010101010101010101016b657901000000066265666f726500000001011100000013010101010101010101010101010101016b6579000000056166746572000000011100000013010101010101010101010101010101016b657900000001000000056576656e7400000006726573756c740100000000000000630000001301000000000000000700000006726573756c74");
        let decoded = RecoveryOutboxRecord::decode(&legacy).unwrap();
        let RecoveryMutationV1::MetadataCommand { command, .. } = &decoded.mutation else {
            panic!("legacy fixture must contain a metadata command");
        };
        assert_eq!(command.object_namespace_id, None);
        assert_eq!(decoded.encode().unwrap(), legacy);
    }

    #[test]
    fn canonical_mutation_round_trips_complete_replay_material() {
        let mutation = RecoveryMutationV1::MetadataCommand {
            command: Box::new(command()),
            lease_deadline_ms: Some(123),
        };
        let encoded = mutation.encode_canonical().unwrap();
        assert_eq!(
            RecoveryMutationV1::decode_canonical(&encoded).unwrap(),
            mutation
        );
        for length in 0..encoded.len() {
            assert!(RecoveryMutationV1::decode_canonical(&encoded[..length]).is_err());
        }
    }

    #[test]
    fn canonical_mutation_decoder_enforces_encoder_invariants_directly() {
        let mut zero_deadline = RecoveryMutationV1::MetadataCommand {
            command: Box::new(command()),
            lease_deadline_ms: Some(123),
        }
        .encode_canonical()
        .unwrap();
        let deadline_start = zero_deadline.len() - std::mem::size_of::<u64>();
        zero_deadline[deadline_start..].fill(0);
        assert!(matches!(
            RecoveryMutationV1::decode_canonical(&zero_deadline),
            Err(RecoveryCodecError::InvalidValue {
                field: "metadata_command"
            })
        ));

        let mut zero_clock = RecoveryMutationV1::ObserveLeaseClock {
            root_id: RootId::from_bytes([1; 16]),
            placement_generation: PlacementGeneration::new(2).unwrap(),
            owner_epoch: OwnerEpoch::new(3).unwrap(),
            observed_ms: 4,
        }
        .encode_canonical()
        .unwrap();
        let observed_start = zero_clock.len() - std::mem::size_of::<u64>();
        zero_clock[observed_start..].fill(0);
        assert!(matches!(
            RecoveryMutationV1::decode_canonical(&zero_clock),
            Err(RecoveryCodecError::ZeroScalar {
                field: "observed_ms"
            })
        ));

        let mut nonmonotonic_owner = Vec::new();
        nonmonotonic_owner.push(3);
        encode_optional_u64(&mut nonmonotonic_owner, Some(5));
        nonmonotonic_owner.extend_from_slice(&OwnerEpoch::new(4).unwrap().get().to_be_bytes());
        assert!(matches!(
            RecoveryMutationV1::decode_canonical(&nonmonotonic_owner),
            Err(RecoveryCodecError::InvalidValue {
                field: "owner_epoch_transition"
            })
        ));
    }

    #[test]
    fn recovery_storage_keys_use_fixed_width_decimal_lsns() {
        assert_eq!(
            recovery_outbox_key(42).as_slice(),
            b"\x0200000000000000000042"
        );
        assert_eq!(
            recovery_chunk_key(42, 7).as_slice(),
            b"\x0300000000000000000042\x00\x00\x00\x07"
        );
        assert_eq!(
            decode_recovery_outbox_key(b"\x0218446744073709551615").unwrap(),
            DecodedRecoveryOutboxKey {
                recovery_lsn: u64::MAX,
                layout: RecoveryKeyLayout::Format9,
            }
        );

        let ordered = [0, 1, 9, 10, 99, 100, u64::MAX - 1, u64::MAX];
        for pair in ordered.windows(2) {
            assert!(recovery_outbox_key(pair[0]) < recovery_outbox_key(pair[1]));
        }
    }

    #[test]
    fn recovery_outbox_key_decoder_accepts_format8_binary_lsns() {
        assert_eq!(
            decode_recovery_outbox_key(&legacy_recovery_outbox_key(42)).unwrap(),
            DecodedRecoveryOutboxKey {
                recovery_lsn: 42,
                layout: RecoveryKeyLayout::Format8,
            }
        );
    }

    #[test]
    fn recovery_outbox_key_decoder_rejects_malformed_decimal_lsns() {
        assert!(decode_recovery_outbox_key(b"\x0200000000000000000000").is_err());
        assert!(decode_recovery_outbox_key(b"\x020000000000000000001").is_err());

        let mut non_digit = recovery_outbox_key(1);
        non_digit[10] = b'x';
        assert!(decode_recovery_outbox_key(&non_digit).is_err());

        let mut overflow = [b'9'; RECOVERY_OUTBOX_KEY_BYTES];
        overflow[0] = STORAGE_HEADER_KEY_TAG;
        assert!(decode_recovery_outbox_key(&overflow).is_err());

        let mut wrong_tag = recovery_outbox_key(1);
        wrong_tag[0] = 0xff;
        assert!(decode_recovery_outbox_key(&wrong_tag).is_err());
    }

    #[test]
    fn recovery_storage_fragments_large_rows_and_rejects_missing_chunks() {
        let logical = vec![0x5a; MAX_STORAGE_CHUNK_DATA_BYTES * 2 + 7];
        let (header, chunks) = split_recovery_storage(&logical).unwrap();
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|chunk| chunk.len() < u16::MAX as usize));
        assert_eq!(
            assemble_recovery_storage(&header, chunks.clone()).unwrap(),
            logical
        );
        assert!(assemble_recovery_storage(&header, chunks.into_iter().take(2)).is_err());

        let mut unknown_header = header.clone();
        unknown_header[0] = STORAGE_HEADER_VERSION + 1;
        assert!(matches!(
            assemble_recovery_storage(&unknown_header, Vec::new()),
            Err(RecoveryCodecError::UnsupportedVersion { .. })
        ));

        let (_, mut chunks) = split_recovery_storage(&logical).unwrap();
        chunks[0][0] = STORAGE_CHUNK_VERSION + 1;
        assert!(matches!(
            assemble_recovery_storage(&header, chunks),
            Err(RecoveryCodecError::UnsupportedVersion { .. })
        ));
    }
}
