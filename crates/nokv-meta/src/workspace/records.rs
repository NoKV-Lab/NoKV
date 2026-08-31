use std::fmt;

use nokv_types::{
    CommandDigest, CommitVersion, LogicalShardId, ObjectNamespaceId, PlacementGeneration,
    RootActivationState, SHA256_BYTES,
};

/// Initial value format for every durable workspace record.
pub const VALUE_FORMAT_VERSION: u8 = 1;
const ROOT_FENCE_VALUE_FORMAT_VERSION: u8 = 2;

/// Installed shard-local placement fence for one Agent root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RootFence {
    pub logical_shard_id: LogicalShardId,
    /// `None` is decoded only from the pre-namespace v1 format. New roots are
    /// written bound; legacy roots must bind before a route can be installed.
    pub object_namespace_id: Option<ObjectNamespaceId>,
    pub placement_generation: PlacementGeneration,
    pub activation_state: RootActivationState,
}

/// Versioned envelope stored by a current-state metadata family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CurrentValue {
    pub created_version: CommitVersion,
    pub modified_version: CommitVersion,
    pub payload: Vec<u8>,
}

/// Previous current-state value retained in the ordered history family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryValue {
    pub transition_version: CommitVersion,
    pub previous_created_version: CommitVersion,
    pub previous_modified_version: CommitVersion,
    pub previous_payload: Option<Vec<u8>>,
}

/// Exact replay result persisted for one metadata command request id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandDedupeRecord {
    pub command_digest: CommandDigest,
    pub commit_version: CommitVersion,
    /// Local recovery evidence committed atomically with this result.
    ///
    /// Shared and replicated authorities store `None` because their atomic
    /// transaction store is itself the successor recovery authority.
    pub recovery_receipt: Option<LocalRecoveryReceipt>,
    pub deterministic_result: Vec<u8>,
}

/// Exact local recovery position bound to one metadata command result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalRecoveryReceipt {
    pub recovery_lsn: u64,
    pub chain_digest: [u8; SHA256_BYTES],
}

/// Strict durable-record decode or encode failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordCodecError {
    UnsupportedValueVersion {
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
    InvalidVersionOrder {
        created: u64,
        modified: u64,
    },
    InvalidHistoryTransition {
        transition: u64,
        previous_modified: u64,
    },
    InvalidOptionalTag {
        field: &'static str,
        value: u8,
    },
    LengthOverflow {
        field: &'static str,
        length: usize,
    },
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
}

impl fmt::Display for RecordCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidVersionOrder { created, modified } => write!(
                formatter,
                "modified version {modified} is older than created version {created}"
            ),
            Self::InvalidHistoryTransition {
                transition,
                previous_modified,
            } => write!(
                formatter,
                "history transition {transition} must be newer than previous modified version \
                 {previous_modified}"
            ),
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::LengthOverflow { field, length } => {
                write!(formatter, "{field} length {length} exceeds u32")
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
                write!(formatter, "durable value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for RecordCodecError {}

impl RootFence {
    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        let mut encoded = Vec::with_capacity(
            1 + LogicalShardId::BYTE_WIDTH
                + self
                    .object_namespace_id
                    .map_or(0, |_| ObjectNamespaceId::BYTE_WIDTH)
                + 8
                + 1,
        );
        encoded.push(if self.object_namespace_id.is_some() {
            ROOT_FENCE_VALUE_FORMAT_VERSION
        } else {
            VALUE_FORMAT_VERSION
        });
        encoded.extend_from_slice(self.logical_shard_id.as_bytes());
        if let Some(object_namespace_id) = self.object_namespace_id {
            encoded.extend_from_slice(object_namespace_id.as_bytes());
        }
        encoded.extend_from_slice(&self.placement_generation.get().to_be_bytes());
        encoded.push(self.activation_state.into());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        let version = decoder.u8("value_format_version")?;
        if version != VALUE_FORMAT_VERSION && version != ROOT_FENCE_VALUE_FORMAT_VERSION {
            return Err(RecordCodecError::UnsupportedValueVersion {
                actual: version,
                expected: ROOT_FENCE_VALUE_FORMAT_VERSION,
            });
        }
        let logical_shard_id = LogicalShardId::from_bytes(decoder.fixed("logical_shard_id")?);
        let object_namespace_id = if version == ROOT_FENCE_VALUE_FORMAT_VERSION {
            Some(ObjectNamespaceId::from_bytes(
                decoder.fixed("object_namespace_id")?,
            ))
        } else {
            None
        };
        let placement_generation = PlacementGeneration::new(decoder.u64("placement_generation")?)
            .map_err(|_| RecordCodecError::ZeroScalar {
            field: "placement_generation",
        })?;
        let activation_discriminant = decoder.u8("activation_state")?;
        let activation_state =
            RootActivationState::try_from(activation_discriminant).map_err(|error| {
                RecordCodecError::UnknownDiscriminant {
                    type_name: error.type_name(),
                    value: error.value(),
                }
            })?;
        decoder.finish()?;
        Ok(Self {
            logical_shard_id,
            object_namespace_id,
            placement_generation,
            activation_state,
        })
    }
}

impl CurrentValue {
    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        if self.modified_version < self.created_version {
            return Err(RecordCodecError::InvalidVersionOrder {
                created: self.created_version.get(),
                modified: self.modified_version.get(),
            });
        }
        let payload_length = checked_u32_length("payload", self.payload.len())?;
        let mut encoded = Vec::with_capacity(1 + 8 + 8 + 4 + self.payload.len());
        encoded.push(VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.created_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.modified_version.get().to_be_bytes());
        encoded.extend_from_slice(&payload_length.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let created_version = decoder.commit_version("created_version")?;
        let modified_version = decoder.commit_version("modified_version")?;
        if modified_version < created_version {
            return Err(RecordCodecError::InvalidVersionOrder {
                created: created_version.get(),
                modified: modified_version.get(),
            });
        }
        let payload = decoder.length_prefixed_bytes("payload")?;
        decoder.finish()?;
        Ok(Self {
            created_version,
            modified_version,
            payload,
        })
    }
}

impl HistoryValue {
    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        if self.previous_modified_version < self.previous_created_version {
            return Err(RecordCodecError::InvalidVersionOrder {
                created: self.previous_created_version.get(),
                modified: self.previous_modified_version.get(),
            });
        }
        if self.transition_version <= self.previous_modified_version {
            return Err(RecordCodecError::InvalidHistoryTransition {
                transition: self.transition_version.get(),
                previous_modified: self.previous_modified_version.get(),
            });
        }
        let payload_length = self
            .previous_payload
            .as_ref()
            .map(|payload| checked_u32_length("previous_payload", payload.len()))
            .transpose()?;
        let payload_capacity = self
            .previous_payload
            .as_ref()
            .map_or(0, |payload| 4 + payload.len());
        let mut encoded = Vec::with_capacity(1 + 8 + 8 + 8 + 1 + payload_capacity);
        encoded.push(VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.transition_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.previous_created_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.previous_modified_version.get().to_be_bytes());
        match (&self.previous_payload, payload_length) {
            (None, None) => encoded.push(0),
            (Some(payload), Some(payload_length)) => {
                encoded.push(1);
                encoded.extend_from_slice(&payload_length.to_be_bytes());
                encoded.extend_from_slice(payload);
            }
            _ => unreachable!("payload and its encoded length are derived together"),
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let transition_version = decoder.commit_version("transition_version")?;
        let previous_created_version = decoder.commit_version("previous_created_version")?;
        let previous_modified_version = decoder.commit_version("previous_modified_version")?;
        if previous_modified_version < previous_created_version {
            return Err(RecordCodecError::InvalidVersionOrder {
                created: previous_created_version.get(),
                modified: previous_modified_version.get(),
            });
        }
        if transition_version <= previous_modified_version {
            return Err(RecordCodecError::InvalidHistoryTransition {
                transition: transition_version.get(),
                previous_modified: previous_modified_version.get(),
            });
        }
        let previous_payload = match decoder.u8("previous_payload tag")? {
            0 => None,
            1 => Some(decoder.length_prefixed_bytes("previous_payload")?),
            value => {
                return Err(RecordCodecError::InvalidOptionalTag {
                    field: "previous_payload",
                    value,
                })
            }
        };
        decoder.finish()?;
        Ok(Self {
            transition_version,
            previous_created_version,
            previous_modified_version,
            previous_payload,
        })
    }
}

impl CommandDedupeRecord {
    pub fn encode(&self) -> Result<Vec<u8>, RecordCodecError> {
        const COMMAND_DEDUPE_VALUE_FORMAT_VERSION: u8 = 3;
        if matches!(
            self.recovery_receipt,
            Some(LocalRecoveryReceipt {
                recovery_lsn: 0,
                ..
            })
        ) {
            return Err(RecordCodecError::ZeroScalar {
                field: "recovery_lsn",
            });
        }
        let result_length =
            checked_u32_length("deterministic_result", self.deterministic_result.len())?;
        let receipt_bytes = self
            .recovery_receipt
            .map_or(0, |_| std::mem::size_of::<u64>() + SHA256_BYTES);
        let mut encoded = Vec::with_capacity(
            1 + CommandDigest::BYTE_WIDTH
                + 8
                + 1
                + receipt_bytes
                + 4
                + self.deterministic_result.len(),
        );
        encoded.push(COMMAND_DEDUPE_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.command_digest.as_bytes());
        encoded.extend_from_slice(&self.commit_version.get().to_be_bytes());
        match self.recovery_receipt {
            None => encoded.push(0),
            Some(receipt) => {
                encoded.push(1);
                encoded.extend_from_slice(&receipt.recovery_lsn.to_be_bytes());
                encoded.extend_from_slice(&receipt.chain_digest);
            }
        }
        encoded.extend_from_slice(&result_length.to_be_bytes());
        encoded.extend_from_slice(&self.deterministic_result);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, RecordCodecError> {
        const COMMAND_DEDUPE_VALUE_FORMAT_VERSION: u8 = 3;
        let mut decoder = Decoder::new(encoded);
        let actual = decoder.u8("value_format_version")?;
        if actual != COMMAND_DEDUPE_VALUE_FORMAT_VERSION {
            return Err(RecordCodecError::UnsupportedValueVersion {
                actual,
                expected: COMMAND_DEDUPE_VALUE_FORMAT_VERSION,
            });
        }
        let command_digest = CommandDigest::from_bytes(decoder.fixed("command_digest")?);
        let commit_version = decoder.commit_version("commit_version")?;
        let recovery_receipt = match decoder.u8("recovery_receipt tag")? {
            0 => None,
            1 => {
                let recovery_lsn = decoder.u64("recovery_lsn")?;
                if recovery_lsn == 0 {
                    return Err(RecordCodecError::ZeroScalar {
                        field: "recovery_lsn",
                    });
                }
                Some(LocalRecoveryReceipt {
                    recovery_lsn,
                    chain_digest: decoder.fixed("recovery_chain_digest")?,
                })
            }
            value => {
                return Err(RecordCodecError::InvalidOptionalTag {
                    field: "recovery_receipt",
                    value,
                })
            }
        };
        let deterministic_result = decoder.length_prefixed_bytes("deterministic_result")?;
        decoder.finish()?;
        Ok(Self {
            command_digest,
            commit_version,
            recovery_receipt,
            deterministic_result,
        })
    }
}

fn checked_u32_length(field: &'static str, length: usize) -> Result<u32, RecordCodecError> {
    u32::try_from(length).map_err(|_| RecordCodecError::LengthOverflow { field, length })
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn require_value_version(&mut self) -> Result<(), RecordCodecError> {
        self.require_version(VALUE_FORMAT_VERSION)
    }

    fn require_version(&mut self, expected: u8) -> Result<(), RecordCodecError> {
        let actual = self.u8("value_format_version")?;
        if actual == expected {
            Ok(())
        } else {
            Err(RecordCodecError::UnsupportedValueVersion { actual, expected })
        }
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, RecordCodecError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, RecordCodecError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, RecordCodecError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn commit_version(&mut self, field: &'static str) -> Result<CommitVersion, RecordCodecError> {
        CommitVersion::new(self.u64(field)?).map_err(|_| RecordCodecError::ZeroScalar { field })
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], RecordCodecError> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(field, N)?);
        Ok(value)
    }

    fn length_prefixed_bytes(&mut self, field: &'static str) -> Result<Vec<u8>, RecordCodecError> {
        let length = self.u32(field)? as usize;
        self.take(field, length).map(<[u8]>::to_vec)
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], RecordCodecError> {
        let remaining = self.encoded.len().saturating_sub(self.offset);
        let Some(end) = self.offset.checked_add(length) else {
            return Err(RecordCodecError::Truncated {
                field,
                needed: length,
                remaining,
            });
        };
        if end > self.encoded.len() {
            return Err(RecordCodecError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let value = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), RecordCodecError> {
        let count = self.encoded.len() - self.offset;
        if count == 0 {
            Ok(())
        } else {
            Err(RecordCodecError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_version(value: u64) -> CommitVersion {
        CommitVersion::new(value).unwrap()
    }

    fn assert_every_proper_prefix_is_truncated<T>(
        encoded: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, RecordCodecError>,
    ) {
        for length in 0..encoded.len() {
            assert!(
                matches!(
                    decode(&encoded[..length]),
                    Err(RecordCodecError::Truncated { .. })
                ),
                "prefix length {length} was not rejected as truncated"
            );
        }
    }

    fn assert_trailing_byte_is_rejected<T: fmt::Debug>(
        mut encoded: Vec<u8>,
        decode: impl Fn(&[u8]) -> Result<T, RecordCodecError>,
    ) {
        encoded.push(0);
        assert_eq!(
            decode(&encoded).unwrap_err(),
            RecordCodecError::TrailingBytes { count: 1 }
        );
    }

    #[test]
    fn root_fence_codec_has_frozen_golden_bytes() {
        let record = RootFence {
            logical_shard_id: LogicalShardId::from_bytes([0x11; 16]),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([0x22; 16])),
            placement_generation: PlacementGeneration::new(0x0102_0304_0506_0708).unwrap(),
            activation_state: RootActivationState::Active,
        };
        let expected = [
            &[ROOT_FENCE_VALUE_FORMAT_VERSION][..],
            &[0x11; 16],
            &[0x22; 16],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &[2],
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(RootFence::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, RootFence::decode);
        assert_trailing_byte_is_rejected(expected, RootFence::decode);
    }

    #[test]
    fn root_fence_decodes_and_preserves_the_v1_unbound_format() {
        let legacy = [
            &[VALUE_FORMAT_VERSION][..],
            &[0x11; 16],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &[2],
        ]
        .concat();
        let decoded = RootFence::decode(&legacy).unwrap();
        assert_eq!(decoded.object_namespace_id, None);
        assert_eq!(decoded.encode().unwrap(), legacy);
    }

    #[test]
    fn current_value_codec_has_frozen_golden_bytes() {
        let record = CurrentValue {
            created_version: commit_version(0x0102_0304_0506_0708),
            modified_version: commit_version(0x1112_1314_1516_1718),
            payload: b"abc".to_vec(),
        };
        let expected = [
            &[VALUE_FORMAT_VERSION][..],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &0x1112_1314_1516_1718_u64.to_be_bytes(),
            &3_u32.to_be_bytes(),
            b"abc",
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(CurrentValue::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, CurrentValue::decode);
        assert_trailing_byte_is_rejected(expected, CurrentValue::decode);
    }

    #[test]
    fn history_value_codec_distinguishes_tombstone_from_empty_payload() {
        let record = HistoryValue {
            transition_version: commit_version(9),
            previous_created_version: commit_version(3),
            previous_modified_version: commit_version(7),
            previous_payload: Some(Vec::new()),
        };
        let expected = [
            &[VALUE_FORMAT_VERSION][..],
            &9_u64.to_be_bytes(),
            &3_u64.to_be_bytes(),
            &7_u64.to_be_bytes(),
            &[1],
            &0_u32.to_be_bytes(),
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(HistoryValue::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, HistoryValue::decode);
        assert_trailing_byte_is_rejected(expected, HistoryValue::decode);

        let tombstone = HistoryValue {
            transition_version: commit_version(10),
            previous_created_version: commit_version(3),
            previous_modified_version: commit_version(9),
            previous_payload: None,
        };
        let tombstone_bytes = tombstone.encode().unwrap();
        assert_eq!(tombstone_bytes.len(), 1 + 8 + 8 + 8 + 1);
        assert_eq!(HistoryValue::decode(&tombstone_bytes).unwrap(), tombstone);
    }

    #[test]
    fn command_dedupe_codec_has_frozen_golden_bytes() {
        let record = CommandDedupeRecord {
            command_digest: CommandDigest::from_bytes([0xaa; 32]),
            commit_version: commit_version(11),
            recovery_receipt: Some(LocalRecoveryReceipt {
                recovery_lsn: 17,
                chain_digest: [0xbb; SHA256_BYTES],
            }),
            deterministic_result: b"ok".to_vec(),
        };
        let expected = [
            &[3][..],
            &[0xaa; 32],
            &11_u64.to_be_bytes(),
            &[1],
            &17_u64.to_be_bytes(),
            &[0xbb; SHA256_BYTES],
            &2_u32.to_be_bytes(),
            b"ok",
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(CommandDedupeRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, CommandDedupeRecord::decode);
        assert_trailing_byte_is_rejected(expected, CommandDedupeRecord::decode);
    }

    #[test]
    fn command_dedupe_codec_freezes_shared_authority_without_a_local_receipt() {
        let record = CommandDedupeRecord {
            command_digest: CommandDigest::from_bytes([0xcc; 32]),
            commit_version: commit_version(12),
            recovery_receipt: None,
            deterministic_result: b"shared".to_vec(),
        };
        let expected = [
            &[3][..],
            &[0xcc; 32],
            &12_u64.to_be_bytes(),
            &[0],
            &6_u32.to_be_bytes(),
            b"shared",
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(CommandDedupeRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, CommandDedupeRecord::decode);
        assert_trailing_byte_is_rejected(expected, CommandDedupeRecord::decode);
    }

    #[test]
    fn every_codec_rejects_unknown_value_version() {
        let root_fence = RootFence {
            logical_shard_id: LogicalShardId::from_bytes([1; 16]),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([2; 16])),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            activation_state: RootActivationState::Active,
        };
        let current = CurrentValue {
            created_version: commit_version(1),
            modified_version: commit_version(1),
            payload: Vec::new(),
        };
        let history = HistoryValue {
            transition_version: commit_version(2),
            previous_created_version: commit_version(1),
            previous_modified_version: commit_version(1),
            previous_payload: None,
        };
        let dedupe = CommandDedupeRecord {
            command_digest: CommandDigest::from_bytes([2; 32]),
            commit_version: commit_version(1),
            recovery_receipt: Some(LocalRecoveryReceipt {
                recovery_lsn: 1,
                chain_digest: [3; SHA256_BYTES],
            }),
            deterministic_result: Vec::new(),
        };

        let mut values = [
            root_fence.encode().unwrap(),
            current.encode().unwrap(),
            history.encode().unwrap(),
            dedupe.encode().unwrap(),
        ];
        values[0][0] = ROOT_FENCE_VALUE_FORMAT_VERSION + 1;
        for value in &mut values[1..] {
            value[0] = VALUE_FORMAT_VERSION + 1;
        }
        values[3][0] = 4;
        assert!(matches!(
            RootFence::decode(&values[0]),
            Err(RecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            CurrentValue::decode(&values[1]),
            Err(RecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            HistoryValue::decode(&values[2]),
            Err(RecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            CommandDedupeRecord::decode(&values[3]),
            Err(RecordCodecError::UnsupportedValueVersion { .. })
        ));

        let mut legacy_dedupe = dedupe.encode().unwrap();
        legacy_dedupe[0] = 2;
        assert_eq!(
            CommandDedupeRecord::decode(&legacy_dedupe),
            Err(RecordCodecError::UnsupportedValueVersion {
                actual: 2,
                expected: 3,
            })
        );
    }

    #[test]
    fn codec_rejects_unknown_enum_zero_scalars_and_optional_tags() {
        let record = RootFence {
            logical_shard_id: LogicalShardId::from_bytes([1; 16]),
            object_namespace_id: Some(ObjectNamespaceId::from_bytes([2; 16])),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            activation_state: RootActivationState::Active,
        };

        let mut unknown_state = record.encode().unwrap();
        *unknown_state.last_mut().unwrap() = 0xff;
        assert_eq!(
            RootFence::decode(&unknown_state),
            Err(RecordCodecError::UnknownDiscriminant {
                type_name: "RootActivationState",
                value: 0xff,
            })
        );

        let mut zero_generation = record.encode().unwrap();
        let generation_offset = 1 + LogicalShardId::BYTE_WIDTH + ObjectNamespaceId::BYTE_WIDTH;
        zero_generation[generation_offset..generation_offset + 8].fill(0);
        assert_eq!(
            RootFence::decode(&zero_generation),
            Err(RecordCodecError::ZeroScalar {
                field: "placement_generation",
            })
        );

        let history = HistoryValue {
            transition_version: commit_version(2),
            previous_created_version: commit_version(1),
            previous_modified_version: commit_version(1),
            previous_payload: None,
        };
        let mut invalid_tag = history.encode().unwrap();
        *invalid_tag.last_mut().unwrap() = 2;
        assert_eq!(
            HistoryValue::decode(&invalid_tag),
            Err(RecordCodecError::InvalidOptionalTag {
                field: "previous_payload",
                value: 2,
            })
        );

        let dedupe = CommandDedupeRecord {
            command_digest: CommandDigest::from_bytes([2; 32]),
            commit_version: commit_version(1),
            recovery_receipt: None,
            deterministic_result: Vec::new(),
        };
        let mut invalid_receipt_tag = dedupe.encode().unwrap();
        invalid_receipt_tag[1 + CommandDigest::BYTE_WIDTH + 8] = 2;
        assert_eq!(
            CommandDedupeRecord::decode(&invalid_receipt_tag),
            Err(RecordCodecError::InvalidOptionalTag {
                field: "recovery_receipt",
                value: 2,
            })
        );

        let zero_receipt = CommandDedupeRecord {
            command_digest: CommandDigest::from_bytes([2; 32]),
            commit_version: commit_version(1),
            recovery_receipt: Some(LocalRecoveryReceipt {
                recovery_lsn: 0,
                chain_digest: [3; SHA256_BYTES],
            }),
            deterministic_result: Vec::new(),
        };
        assert_eq!(
            zero_receipt.encode(),
            Err(RecordCodecError::ZeroScalar {
                field: "recovery_lsn",
            })
        );

        let mut zero_receipt_bytes = CommandDedupeRecord {
            recovery_receipt: Some(LocalRecoveryReceipt {
                recovery_lsn: 1,
                chain_digest: [3; SHA256_BYTES],
            }),
            ..dedupe
        }
        .encode()
        .unwrap();
        let recovery_lsn_offset = 1 + CommandDigest::BYTE_WIDTH + 8 + 1;
        zero_receipt_bytes[recovery_lsn_offset..recovery_lsn_offset + 8].fill(0);
        assert_eq!(
            CommandDedupeRecord::decode(&zero_receipt_bytes),
            Err(RecordCodecError::ZeroScalar {
                field: "recovery_lsn",
            })
        );

        let mut zero_created_version = CurrentValue {
            created_version: commit_version(1),
            modified_version: commit_version(1),
            payload: Vec::new(),
        }
        .encode()
        .unwrap();
        zero_created_version[1..9].fill(0);
        assert_eq!(
            CurrentValue::decode(&zero_created_version),
            Err(RecordCodecError::ZeroScalar {
                field: "created_version",
            })
        );
    }

    #[test]
    fn oversized_payload_lengths_fail_before_u32_encoding() {
        let overflow = u32::MAX as usize + 1;
        assert_eq!(
            checked_u32_length("payload", overflow),
            Err(RecordCodecError::LengthOverflow {
                field: "payload",
                length: overflow,
            })
        );
        assert_eq!(
            checked_u32_length("deterministic_result", overflow),
            Err(RecordCodecError::LengthOverflow {
                field: "deterministic_result",
                length: overflow,
            })
        );
        assert_eq!(
            checked_u32_length("previous_payload", u32::MAX as usize),
            Ok(u32::MAX)
        );
    }

    #[test]
    fn declared_payload_length_cannot_run_past_input() {
        let mut current = CurrentValue {
            created_version: commit_version(1),
            modified_version: commit_version(1),
            payload: b"x".to_vec(),
        }
        .encode()
        .unwrap();
        current[17..21].copy_from_slice(&2_u32.to_be_bytes());
        assert_eq!(
            CurrentValue::decode(&current),
            Err(RecordCodecError::Truncated {
                field: "payload",
                needed: 2,
                remaining: 1,
            })
        );
    }
}
