/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable leased snapshots, aliases, and MVCC history-retention holds.

use std::fmt;

use nokv_types::{
    ConsumerEpoch, Generation, HistoryHoldState, ReadVersion, SnapshotAliasName, SnapshotId,
    SnapshotState,
};

/// Only supported value format for snapshot-owned payloads.
pub const SNAPSHOT_VALUE_FORMAT_VERSION: u8 = 1;

/// Maximum canonical annotation projection retained with a snapshot.
pub const MAX_SNAPSHOT_ANNOTATION_BYTES: usize = 16 * 1024;

/// Maximum canonical retirement annotation retained with a snapshot.
pub const MAX_SNAPSHOT_RETIRE_ANNOTATION_BYTES: usize = 4 * 1024;

/// One leased MVCC recovery point.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRefRecord {
    pub read_version: ReadVersion,
    pub alias: Option<SnapshotAliasName>,
    /// Deadline on the shard's persisted lease clock.
    pub lease_deadline_ms: u64,
    pub state: SnapshotState,
    pub consumer_count: u64,
    pub consumer_epoch: ConsumerEpoch,
    /// Canonical, typed facade annotation bytes.
    pub annotation: Vec<u8>,
    /// Canonical, typed facade annotation supplied by an explicit retire.
    /// Automatic expiry/reaping never manufactures this presentation data.
    pub retire_annotation: Option<Vec<u8>>,
}

/// Latest-mint-wins resolution for one exact snapshot alias.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotAliasRecord {
    pub snapshot_id: SnapshotId,
    pub alias_generation: Generation,
    /// Projection of the selected snapshot's lifecycle. A terminal latest
    /// selection never falls back to an older mint.
    pub snapshot_state: SnapshotState,
}

/// Exact MVCC history floor owned by a snapshot, commit build, or restore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryHoldRecord {
    pub read_version: ReadVersion,
    /// Set only when a restore hold consumes a leased snapshot source.
    pub source_snapshot_id: Option<SnapshotId>,
    pub state: HistoryHoldState,
}

/// Strict snapshot payload encode, decode, or invariant failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotRecordError {
    UnsupportedValueVersion {
        actual: u8,
        expected: u8,
    },
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
    },
    InvalidOptionalTag {
        field: &'static str,
        value: u8,
    },
    ZeroScalar {
        field: &'static str,
    },
    InvalidAlias {
        reason: String,
    },
    FieldTooLong {
        field: &'static str,
        length: usize,
        max: usize,
    },
    InvalidConsumerLifetime {
        reason: &'static str,
    },
    InvalidRetireAnnotation {
        reason: &'static str,
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

impl fmt::Display for SnapshotRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported snapshot value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidAlias { reason } => write!(formatter, "invalid snapshot alias: {reason}"),
            Self::FieldTooLong { field, length, max } => {
                write!(formatter, "{field} is {length} bytes, maximum is {max}")
            }
            Self::InvalidConsumerLifetime { reason } => {
                write!(formatter, "invalid snapshot consumer lifetime: {reason}")
            }
            Self::InvalidRetireAnnotation { reason } => {
                write!(formatter, "invalid snapshot retire annotation: {reason}")
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
                write!(formatter, "snapshot value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for SnapshotRecordError {}

impl SnapshotRefRecord {
    pub fn validate(&self) -> Result<(), SnapshotRecordError> {
        if self.lease_deadline_ms == 0 {
            return Err(SnapshotRecordError::ZeroScalar {
                field: "lease_deadline_ms",
            });
        }
        validate_length(
            "annotation",
            self.annotation.len(),
            MAX_SNAPSHOT_ANNOTATION_BYTES,
        )?;
        if let Some(annotation) = &self.retire_annotation {
            validate_length(
                "retire_annotation",
                annotation.len(),
                MAX_SNAPSHOT_RETIRE_ANNOTATION_BYTES,
            )?;
            if self.state != SnapshotState::Retired {
                return Err(SnapshotRecordError::InvalidRetireAnnotation {
                    reason: "only explicitly retired snapshots may retain one",
                });
            }
        }
        validate_snapshot_consumer_lifetime(self.state, self.consumer_count, self.consumer_epoch)
    }

    pub fn encode(&self) -> Result<Vec<u8>, SnapshotRecordError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.push(SNAPSHOT_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.read_version.get().to_be_bytes());
        put_optional_alias(&mut encoded, self.alias.as_ref());
        encoded.extend_from_slice(&self.lease_deadline_ms.to_be_bytes());
        encoded.push(self.state.into());
        encoded.extend_from_slice(&self.consumer_count.to_be_bytes());
        encoded.extend_from_slice(&self.consumer_epoch.get().to_be_bytes());
        put_bounded_bytes(
            &mut encoded,
            "annotation",
            &self.annotation,
            MAX_SNAPSHOT_ANNOTATION_BYTES,
        )?;
        put_optional_bounded_bytes(
            &mut encoded,
            "retire_annotation",
            self.retire_annotation.as_deref(),
            MAX_SNAPSHOT_RETIRE_ANNOTATION_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, SnapshotRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let read_version = decoder.read_version("read_version")?;
        let alias = decoder.optional_alias("alias")?;
        let lease_deadline_ms = decoder.u64("lease_deadline_ms")?;
        let state = decode_durable_enum(decoder.u8("state")?)?;
        let consumer_count = decoder.u64("consumer_count")?;
        let consumer_epoch = ConsumerEpoch::new(decoder.u64("consumer_epoch")?);
        let annotation = decoder.bounded_bytes("annotation", MAX_SNAPSHOT_ANNOTATION_BYTES)?;
        let retire_annotation = decoder
            .optional_bounded_bytes("retire_annotation", MAX_SNAPSHOT_RETIRE_ANNOTATION_BYTES)?;
        decoder.finish()?;
        let record = Self {
            read_version,
            alias,
            lease_deadline_ms,
            state,
            consumer_count,
            consumer_epoch,
            annotation,
            retire_annotation,
        };
        record.validate()?;
        Ok(record)
    }
}

impl SnapshotAliasRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + 8 + 8 + 1);
        encoded.push(SNAPSHOT_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.snapshot_id.get().to_be_bytes());
        encoded.extend_from_slice(&self.alias_generation.get().to_be_bytes());
        encoded.push(self.snapshot_state.into());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, SnapshotRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let snapshot_id = SnapshotId::new(decoder.u64("snapshot_id")?);
        let alias_generation = decoder.generation("alias_generation")?;
        let snapshot_state = decode_durable_enum(decoder.u8("snapshot_state")?)?;
        decoder.finish()?;
        Ok(Self {
            snapshot_id,
            alias_generation,
            snapshot_state,
        })
    }
}

impl HistoryHoldRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + 8 + 1 + 8 + 1);
        encoded.push(SNAPSHOT_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.read_version.get().to_be_bytes());
        match self.source_snapshot_id {
            None => encoded.push(0),
            Some(snapshot_id) => {
                encoded.push(1);
                encoded.extend_from_slice(&snapshot_id.get().to_be_bytes());
            }
        }
        encoded.push(self.state.into());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, SnapshotRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let read_version = decoder.read_version("read_version")?;
        let source_snapshot_id = match decoder.u8("source_snapshot_id")? {
            0 => None,
            1 => Some(SnapshotId::new(decoder.u64("source_snapshot_id")?)),
            value => {
                return Err(SnapshotRecordError::InvalidOptionalTag {
                    field: "source_snapshot_id",
                    value,
                });
            }
        };
        let state = decode_durable_enum(decoder.u8("state")?)?;
        decoder.finish()?;
        Ok(Self {
            read_version,
            source_snapshot_id,
            state,
        })
    }
}

fn validate_snapshot_consumer_lifetime(
    state: SnapshotState,
    consumer_count: u64,
    consumer_epoch: ConsumerEpoch,
) -> Result<(), SnapshotRecordError> {
    if consumer_count > 0 && consumer_epoch == ConsumerEpoch::ZERO {
        return Err(SnapshotRecordError::InvalidConsumerLifetime {
            reason: "live consumers require a non-zero epoch",
        });
    }
    if state != SnapshotState::Active && consumer_count != 0 {
        return Err(SnapshotRecordError::InvalidConsumerLifetime {
            reason: "claimed or terminal snapshots cannot retain consumers",
        });
    }
    Ok(())
}

fn validate_length(
    field: &'static str,
    length: usize,
    max: usize,
) -> Result<(), SnapshotRecordError> {
    if length > max {
        Err(SnapshotRecordError::FieldTooLong { field, length, max })
    } else {
        Ok(())
    }
}

fn put_optional_alias(encoded: &mut Vec<u8>, alias: Option<&SnapshotAliasName>) {
    match alias {
        None => encoded.push(0),
        Some(alias) => {
            encoded.push(1);
            encoded.extend_from_slice(
                &u16::try_from(alias.as_bytes().len())
                    .expect("validated snapshot alias length fits u16")
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(alias.as_bytes());
        }
    }
}

fn put_bounded_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
    max: usize,
) -> Result<(), SnapshotRecordError> {
    validate_length(field, value.len(), max)?;
    encoded.extend_from_slice(
        &u32::try_from(value.len())
            .expect("validated snapshot byte length fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(value);
    Ok(())
}

fn put_optional_bounded_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: Option<&[u8]>,
    max: usize,
) -> Result<(), SnapshotRecordError> {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            put_bounded_bytes(encoded, field, value, max)?;
        }
    }
    Ok(())
}

fn decode_durable_enum<T>(value: u8) -> Result<T, SnapshotRecordError>
where
    T: TryFrom<u8, Error = nokv_types::UnknownDurableDiscriminant>,
{
    T::try_from(value).map_err(|error| SnapshotRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

struct Decoder<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn require_value_version(&mut self) -> Result<(), SnapshotRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == SNAPSHOT_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(SnapshotRecordError::UnsupportedValueVersion {
                actual,
                expected: SNAPSHOT_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], SnapshotRecordError> {
        let bytes = self.take(field, N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, SnapshotRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn optional_bounded_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Option<Vec<u8>>, SnapshotRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.bounded_bytes(field, max).map(Some),
            value => Err(SnapshotRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, SnapshotRecordError> {
        Ok(u16::from_be_bytes(self.fixed(field)?))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, SnapshotRecordError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, SnapshotRecordError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn read_version(&mut self, field: &'static str) -> Result<ReadVersion, SnapshotRecordError> {
        ReadVersion::new(self.u64(field)?).map_err(|_| SnapshotRecordError::ZeroScalar { field })
    }

    fn generation(&mut self, field: &'static str) -> Result<Generation, SnapshotRecordError> {
        Generation::new(self.u64(field)?).map_err(|_| SnapshotRecordError::ZeroScalar { field })
    }

    fn optional_alias(
        &mut self,
        field: &'static str,
    ) -> Result<Option<SnapshotAliasName>, SnapshotRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => {
                let length = usize::from(self.u16(field)?);
                let bytes = self.take(field, length)?;
                SnapshotAliasName::from_bytes(bytes)
                    .map(Some)
                    .map_err(|error| SnapshotRecordError::InvalidAlias {
                        reason: error.to_string(),
                    })
            }
            value => Err(SnapshotRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn bounded_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Vec<u8>, SnapshotRecordError> {
        let length = self.u32(field)? as usize;
        validate_length(field, length, max)?;
        Ok(self.take(field, length)?.to_vec())
    }

    fn take(
        &mut self,
        field: &'static str,
        length: usize,
    ) -> Result<&'a [u8], SnapshotRecordError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(SnapshotRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn finish(self) -> Result<(), SnapshotRecordError> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(SnapshotRecordError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_version(value: u64) -> ReadVersion {
        ReadVersion::new(value).unwrap()
    }

    #[test]
    fn snapshot_ref_round_trip_and_strict_envelope() {
        let record = SnapshotRefRecord {
            read_version: read_version(9),
            alias: Some(SnapshotAliasName::new("handoff").unwrap()),
            lease_deadline_ms: 12_345,
            state: SnapshotState::Active,
            consumer_count: 2,
            consumer_epoch: ConsumerEpoch::new(3),
            annotation: br#"{"run":"train"}"#.to_vec(),
            retire_annotation: None,
        };
        let encoded = record.encode().unwrap();
        assert_eq!(SnapshotRefRecord::decode(&encoded).unwrap(), record);

        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            SnapshotRefRecord::decode(&trailing),
            Err(SnapshotRecordError::TrailingBytes { count: 1 })
        );
    }

    #[test]
    fn alias_and_history_hold_golden_bytes() {
        let alias = SnapshotAliasRecord {
            snapshot_id: SnapshotId::new(0x0102_0304_0506_0708),
            alias_generation: Generation::new(9).unwrap(),
            snapshot_state: SnapshotState::Retired,
        };
        assert_eq!(
            alias.encode(),
            vec![
                SNAPSHOT_VALUE_FORMAT_VERSION,
                0x01,
                0x02,
                0x03,
                0x04,
                0x05,
                0x06,
                0x07,
                0x08,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                9,
                u8::from(SnapshotState::Retired),
            ]
        );
        assert_eq!(SnapshotAliasRecord::decode(&alias.encode()).unwrap(), alias);

        let hold = HistoryHoldRecord {
            read_version: read_version(17),
            source_snapshot_id: Some(SnapshotId::new(4)),
            state: HistoryHoldState::Active,
        };
        assert_eq!(HistoryHoldRecord::decode(&hold.encode()).unwrap(), hold);
    }

    #[test]
    fn terminal_snapshot_cannot_retain_consumers() {
        let record = SnapshotRefRecord {
            read_version: read_version(1),
            alias: None,
            lease_deadline_ms: 2,
            state: SnapshotState::ReapClaimed,
            consumer_count: 1,
            consumer_epoch: ConsumerEpoch::new(1),
            annotation: Vec::new(),
            retire_annotation: None,
        };
        assert_eq!(
            record.encode(),
            Err(SnapshotRecordError::InvalidConsumerLifetime {
                reason: "claimed or terminal snapshots cannot retain consumers",
            })
        );
    }

    #[test]
    fn retire_annotation_is_durable_only_for_explicit_retirement() {
        let annotation = br#"{"metadata":null,"reason":"done"}"#.to_vec();
        let retired = SnapshotRefRecord {
            read_version: read_version(1),
            alias: None,
            lease_deadline_ms: 2,
            state: SnapshotState::Retired,
            consumer_count: 0,
            consumer_epoch: ConsumerEpoch::ZERO,
            annotation: Vec::new(),
            retire_annotation: Some(annotation.clone()),
        };
        assert_eq!(
            SnapshotRefRecord::decode(&retired.encode().unwrap()).unwrap(),
            retired
        );

        let active = SnapshotRefRecord {
            state: SnapshotState::Active,
            ..retired
        };
        assert_eq!(
            active.encode(),
            Err(SnapshotRecordError::InvalidRetireAnnotation {
                reason: "only explicitly retired snapshots may retain one",
            })
        );
    }

    #[test]
    fn zero_required_scalars_and_unknown_states_fail_closed() {
        let mut alias = SnapshotAliasRecord {
            snapshot_id: SnapshotId::ZERO,
            alias_generation: Generation::new(1).unwrap(),
            snapshot_state: SnapshotState::Active,
        }
        .encode();
        alias[9..17].fill(0);
        assert_eq!(
            SnapshotAliasRecord::decode(&alias),
            Err(SnapshotRecordError::ZeroScalar {
                field: "alias_generation",
            })
        );

        let mut hold = HistoryHoldRecord {
            read_version: read_version(1),
            source_snapshot_id: None,
            state: HistoryHoldState::Active,
        }
        .encode();
        *hold.last_mut().unwrap() = 0xff;
        assert_eq!(
            HistoryHoldRecord::decode(&hold),
            Err(SnapshotRecordError::UnknownDiscriminant {
                type_name: "HistoryHoldState",
                value: 0xff,
            })
        );
    }
}
