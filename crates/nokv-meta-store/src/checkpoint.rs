/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use crate::TxnStore;

const ENVELOPE_MAGIC: &[u8; 8] = b"NOKVSCPE";
const ENVELOPE_VERSION: u16 = 1;
const MAX_FORMAT_ID_BYTES: usize = 64;
const FIXED_ENVELOPE_BYTES: usize = ENVELOPE_MAGIC.len() + 2 + 2 + 32 + 8;
/// Hard upper bound for one backend-native whole-store checkpoint image.
pub const MAX_CHECKPOINT_IMAGE_BYTES: usize = 512 * 1024 * 1024;

/// Canonical identifier for one backend checkpoint wire format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointFormatId(String);

/// SHA-256 commitment to the storage-neutral keyspace-to-physical catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointCatalogCommitment([u8; 32]);

/// Canonical storage-neutral envelope around one opaque backend image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreCheckpointEnvelope {
    format_id: CheckpointFormatId,
    catalog_commitment: CheckpointCatalogCommitment,
    image: Vec<u8>,
}

/// Checkpoint export or envelope validation failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointError {
    InvalidFormatId,
    ImageTooLarge { actual: usize, maximum: usize },
    InvalidEnvelope(&'static str),
    Unavailable(String),
    Corrupt(String),
}

/// Physical state of a fresh target after checkpoint installation fails.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointInstallState {
    /// No backend state from this attempt can become visible later.
    Unchanged,
    /// Installation crossed its durable marker; the target must be discarded.
    Poisoned,
}

/// Typed fresh-install failure carrying the target's post-error state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckpointInstallError {
    state: CheckpointInstallState,
    reason: String,
}

/// Optional capability for exporting a consistent opaque whole-store image.
pub trait WholeStoreCheckpointSource: Send + Sync {
    fn export_checkpoint(&self) -> Result<StoreCheckpointEnvelope, CheckpointError>;
}

/// Consuming capability for installing an image into one fresh physical target.
pub trait FreshStoreCheckpointInstaller {
    type Store: TxnStore;

    fn install(
        self,
        checkpoint: &StoreCheckpointEnvelope,
    ) -> Result<Self::Store, CheckpointInstallError>;
}

impl CheckpointFormatId {
    pub fn new(value: impl Into<String>) -> Result<Self, CheckpointError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_FORMAT_ID_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-_".contains(&byte)
            })
        {
            return Err(CheckpointError::InvalidFormatId);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl CheckpointCatalogCommitment {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl StoreCheckpointEnvelope {
    pub fn new(
        format_id: CheckpointFormatId,
        catalog_commitment: CheckpointCatalogCommitment,
        image: Vec<u8>,
    ) -> Result<Self, CheckpointError> {
        if image.len() > MAX_CHECKPOINT_IMAGE_BYTES {
            return Err(CheckpointError::ImageTooLarge {
                actual: image.len(),
                maximum: MAX_CHECKPOINT_IMAGE_BYTES,
            });
        }
        Ok(Self {
            format_id,
            catalog_commitment,
            image,
        })
    }

    pub fn format_id(&self) -> &CheckpointFormatId {
        &self.format_id
    }

    pub const fn catalog_commitment(&self) -> CheckpointCatalogCommitment {
        self.catalog_commitment
    }

    pub fn image(&self) -> &[u8] {
        &self.image
    }

    pub fn encoded_len(&self) -> usize {
        FIXED_ENVELOPE_BYTES + self.format_id.as_str().len() + self.image.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let format = self.format_id.as_str().as_bytes();
        let mut encoded = Vec::with_capacity(self.encoded_len());
        encoded.extend_from_slice(ENVELOPE_MAGIC);
        encoded.extend_from_slice(&ENVELOPE_VERSION.to_be_bytes());
        encoded.extend_from_slice(&(format.len() as u16).to_be_bytes());
        encoded.extend_from_slice(format);
        encoded.extend_from_slice(self.catalog_commitment.as_bytes());
        encoded.extend_from_slice(&(self.image.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&self.image);
        encoded
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CheckpointError> {
        if bytes.len() < FIXED_ENVELOPE_BYTES {
            return Err(CheckpointError::InvalidEnvelope("truncated header"));
        }
        if &bytes[..ENVELOPE_MAGIC.len()] != ENVELOPE_MAGIC {
            return Err(CheckpointError::InvalidEnvelope("wrong magic"));
        }

        let mut offset = ENVELOPE_MAGIC.len();
        let version = read_u16(bytes, &mut offset)?;
        if version != ENVELOPE_VERSION {
            return Err(CheckpointError::InvalidEnvelope("unsupported version"));
        }
        let format_len = usize::from(read_u16(bytes, &mut offset)?);
        if format_len == 0 || format_len > MAX_FORMAT_ID_BYTES {
            return Err(CheckpointError::InvalidEnvelope("invalid format length"));
        }
        let format_end = offset
            .checked_add(format_len)
            .ok_or(CheckpointError::InvalidEnvelope("format length overflow"))?;
        let format_bytes = bytes
            .get(offset..format_end)
            .ok_or(CheckpointError::InvalidEnvelope("truncated format"))?;
        let format = std::str::from_utf8(format_bytes)
            .map_err(|_| CheckpointError::InvalidEnvelope("format is not UTF-8"))?;
        let format_id = CheckpointFormatId::new(format)
            .map_err(|_| CheckpointError::InvalidEnvelope("non-canonical format"))?;
        offset = format_end;

        let commitment_end = offset
            .checked_add(32)
            .ok_or(CheckpointError::InvalidEnvelope("catalog length overflow"))?;
        let commitment_slice =
            bytes
                .get(offset..commitment_end)
                .ok_or(CheckpointError::InvalidEnvelope(
                    "truncated catalog commitment",
                ))?;
        let mut commitment = [0_u8; 32];
        commitment.copy_from_slice(commitment_slice);
        offset = commitment_end;

        let image_len_u64 = read_u64(bytes, &mut offset)?;
        let image_len = usize::try_from(image_len_u64)
            .map_err(|_| CheckpointError::InvalidEnvelope("image length overflow"))?;
        if image_len > MAX_CHECKPOINT_IMAGE_BYTES {
            return Err(CheckpointError::ImageTooLarge {
                actual: image_len,
                maximum: MAX_CHECKPOINT_IMAGE_BYTES,
            });
        }
        let image_end = offset
            .checked_add(image_len)
            .ok_or(CheckpointError::InvalidEnvelope("image length overflow"))?;
        if image_end != bytes.len() {
            return Err(CheckpointError::InvalidEnvelope(
                if image_end < bytes.len() {
                    "trailing bytes"
                } else {
                    "truncated image"
                },
            ));
        }

        Self::new(
            format_id,
            CheckpointCatalogCommitment::from_bytes(commitment),
            bytes[offset..image_end].to_vec(),
        )
    }
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, CheckpointError> {
    let end = offset
        .checked_add(2)
        .ok_or(CheckpointError::InvalidEnvelope("integer offset overflow"))?;
    let encoded = bytes
        .get(*offset..end)
        .ok_or(CheckpointError::InvalidEnvelope("truncated integer"))?;
    *offset = end;
    Ok(u16::from_be_bytes([encoded[0], encoded[1]]))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, CheckpointError> {
    let end = offset
        .checked_add(8)
        .ok_or(CheckpointError::InvalidEnvelope("integer offset overflow"))?;
    let encoded = bytes
        .get(*offset..end)
        .ok_or(CheckpointError::InvalidEnvelope("truncated integer"))?;
    *offset = end;
    let mut array = [0_u8; 8];
    array.copy_from_slice(encoded);
    Ok(u64::from_be_bytes(array))
}

impl CheckpointInstallError {
    pub fn unchanged(reason: impl Into<String>) -> Self {
        Self {
            state: CheckpointInstallState::Unchanged,
            reason: reason.into(),
        }
    }

    pub fn poisoned(reason: impl Into<String>) -> Self {
        Self {
            state: CheckpointInstallState::Poisoned,
            reason: reason.into(),
        }
    }

    pub const fn state(&self) -> CheckpointInstallState {
        self.state
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormatId => formatter.write_str("invalid checkpoint format identifier"),
            Self::ImageTooLarge { actual, maximum } => write!(
                formatter,
                "checkpoint image is {actual} bytes; maximum is {maximum}"
            ),
            Self::InvalidEnvelope(reason) => {
                write!(formatter, "invalid checkpoint envelope: {reason}")
            }
            Self::Unavailable(_) => formatter.write_str("checkpoint source is unavailable"),
            Self::Corrupt(_) => formatter.write_str("checkpoint source is corrupt"),
        }
    }
}

impl std::error::Error for CheckpointError {}

impl fmt::Display for CheckpointInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "checkpoint install failed ({:?}): {}",
            self.state, self.reason
        )
    }
}

impl std::error::Error for CheckpointInstallError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_envelope_has_one_strict_canonical_encoding() {
        let envelope = StoreCheckpointEnvelope::new(
            CheckpointFormatId::new("holt.checkpoint.v1").unwrap(),
            CheckpointCatalogCommitment::from_bytes([0x5a; 32]),
            vec![1, 2, 3, 4],
        )
        .unwrap();

        let encoded = envelope.encode();

        assert!(encoded.starts_with(ENVELOPE_MAGIC));
        assert_eq!(StoreCheckpointEnvelope::decode(&encoded).unwrap(), envelope);
        let mut trailing = encoded;
        trailing.push(0);
        assert!(matches!(
            StoreCheckpointEnvelope::decode(&trailing),
            Err(CheckpointError::InvalidEnvelope(_))
        ));
    }

    #[test]
    fn checkpoint_envelope_rejects_noncanonical_and_unbounded_inputs_before_allocation() {
        assert_eq!(
            CheckpointFormatId::new("Holt.checkpoint.v1"),
            Err(CheckpointError::InvalidFormatId)
        );
        assert_eq!(
            CheckpointFormatId::new("holt/checkpoint/v1"),
            Err(CheckpointError::InvalidFormatId)
        );

        let envelope = StoreCheckpointEnvelope::new(
            CheckpointFormatId::new("holt.checkpoint.v1").unwrap(),
            CheckpointCatalogCommitment::from_bytes([0x5a; 32]),
            vec![7],
        )
        .unwrap();
        let mut encoded = envelope.encode();
        let image_len_offset =
            ENVELOPE_MAGIC.len() + 2 + 2 + envelope.format_id.as_str().len() + 32;
        encoded[image_len_offset..image_len_offset + 8]
            .copy_from_slice(&((MAX_CHECKPOINT_IMAGE_BYTES as u64) + 1).to_be_bytes());
        assert_eq!(
            StoreCheckpointEnvelope::decode(&encoded),
            Err(CheckpointError::ImageTooLarge {
                actual: MAX_CHECKPOINT_IMAGE_BYTES + 1,
                maximum: MAX_CHECKPOINT_IMAGE_BYTES,
            })
        );

        let mut wrong_version = envelope.encode();
        wrong_version[ENVELOPE_MAGIC.len()..ENVELOPE_MAGIC.len() + 2]
            .copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            StoreCheckpointEnvelope::decode(&wrong_version),
            Err(CheckpointError::InvalidEnvelope("unsupported version"))
        );
    }
}
