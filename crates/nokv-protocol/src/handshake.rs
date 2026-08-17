/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

/// Fixed handshake payload size, excluding the ordinary four-byte frame prefix.
pub const HANDSHAKE_PAYLOAD_BYTES: usize = 48;
/// Complete fixed handshake frame size, including its four-byte length prefix.
pub const HANDSHAKE_FRAME_BYTES: usize = 4 + HANDSHAKE_PAYLOAD_BYTES;
/// Width reserved for the one exact operation schema.
pub const HANDSHAKE_SCHEMA_BYTES: usize = 32;
/// Stable v1 handshake magic.
pub const WORKSPACE_HANDSHAKE_MAGIC: [u8; 8] = *b"NOKVHS1\0";

const KIND_OFFSET: usize = 8;
const SCHEMA_LENGTH_OFFSET: usize = 9;
const RESERVED_RANGE: std::ops::Range<usize> = 10..16;
const SCHEMA_OFFSET: usize = 16;

/// Direction and terminal result of one workspace transport handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum HandshakeKind {
    /// The client offers its one exact operation schema.
    ClientHello = 1,
    /// The server accepts that exact operation schema.
    Accepted = 2,
    /// The server rejects the offer and advertises its own exact schema.
    Incompatible = 3,
}

impl HandshakeKind {
    fn from_byte(encoded: u8) -> Result<Self, HandshakeError> {
        match encoded {
            1 => Ok(Self::ClientHello),
            2 => Ok(Self::Accepted),
            3 => Ok(Self::Incompatible),
            other => Err(HandshakeError::InvalidKind(other)),
        }
    }
}

/// Canonical fixed-width handshake value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceHandshake {
    kind: HandshakeKind,
    operation_schema: String,
}

impl WorkspaceHandshake {
    /// Construct a handshake with one nonempty ASCII operation schema.
    pub fn new(
        kind: HandshakeKind,
        operation_schema: impl Into<String>,
    ) -> Result<Self, HandshakeError> {
        let operation_schema = operation_schema.into();
        validate_schema(&operation_schema)?;
        Ok(Self {
            kind,
            operation_schema,
        })
    }

    /// Return the handshake direction or result.
    pub fn kind(&self) -> HandshakeKind {
        self.kind
    }

    /// Return the sole exact operation schema; this is not a version range.
    pub fn operation_schema(&self) -> &str {
        &self.operation_schema
    }
}

/// Failure to construct or decode the canonical fixed-width handshake.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HandshakeError {
    InvalidFrameLength { actual: usize },
    InvalidPayloadLength { actual: usize },
    InvalidLengthPrefix(u32),
    InvalidMagic,
    InvalidKind(u8),
    InvalidSchemaLength(usize),
    NonzeroReserved,
    NonAsciiSchema,
    NonzeroPadding,
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFrameLength { actual } => write!(
                formatter,
                "workspace handshake frame is {actual} bytes, expected {HANDSHAKE_FRAME_BYTES}"
            ),
            Self::InvalidPayloadLength { actual } => write!(
                formatter,
                "workspace handshake payload is {actual} bytes, expected {HANDSHAKE_PAYLOAD_BYTES}"
            ),
            Self::InvalidLengthPrefix(actual) => write!(
                formatter,
                "workspace handshake length prefix is {actual}, expected {HANDSHAKE_PAYLOAD_BYTES}"
            ),
            Self::InvalidMagic => formatter.write_str("invalid workspace handshake magic"),
            Self::InvalidKind(kind) => {
                write!(formatter, "invalid workspace handshake kind {kind}")
            }
            Self::InvalidSchemaLength(length) => write!(
                formatter,
                "workspace handshake schema is {length} bytes, expected 1..={HANDSHAKE_SCHEMA_BYTES}"
            ),
            Self::NonzeroReserved => {
                formatter.write_str("workspace handshake reserved bytes must be zero")
            }
            Self::NonAsciiSchema => {
                formatter.write_str("workspace handshake schema must be ASCII")
            }
            Self::NonzeroPadding => {
                formatter.write_str("workspace handshake schema padding must be zero")
            }
        }
    }
}

impl std::error::Error for HandshakeError {}

/// Encode only the fixed 48-byte handshake payload.
pub fn encode_handshake_payload(
    handshake: &WorkspaceHandshake,
) -> Result<[u8; HANDSHAKE_PAYLOAD_BYTES], HandshakeError> {
    validate_schema(handshake.operation_schema())?;
    let schema = handshake.operation_schema().as_bytes();
    let mut encoded = [0_u8; HANDSHAKE_PAYLOAD_BYTES];
    encoded[..WORKSPACE_HANDSHAKE_MAGIC.len()].copy_from_slice(&WORKSPACE_HANDSHAKE_MAGIC);
    encoded[KIND_OFFSET] = handshake.kind() as u8;
    encoded[SCHEMA_LENGTH_OFFSET] = schema.len() as u8;
    encoded[SCHEMA_OFFSET..SCHEMA_OFFSET + schema.len()].copy_from_slice(schema);
    Ok(encoded)
}

/// Decode and validate every field of the fixed 48-byte handshake payload.
pub fn decode_handshake_payload(encoded: &[u8]) -> Result<WorkspaceHandshake, HandshakeError> {
    if encoded.len() != HANDSHAKE_PAYLOAD_BYTES {
        return Err(HandshakeError::InvalidPayloadLength {
            actual: encoded.len(),
        });
    }
    if !has_handshake_magic(encoded) {
        return Err(HandshakeError::InvalidMagic);
    }
    let kind = HandshakeKind::from_byte(encoded[KIND_OFFSET])?;
    let schema_length = usize::from(encoded[SCHEMA_LENGTH_OFFSET]);
    if !(1..=HANDSHAKE_SCHEMA_BYTES).contains(&schema_length) {
        return Err(HandshakeError::InvalidSchemaLength(schema_length));
    }
    if encoded[RESERVED_RANGE].iter().any(|byte| *byte != 0) {
        return Err(HandshakeError::NonzeroReserved);
    }
    let schema_end = SCHEMA_OFFSET + schema_length;
    let schema = &encoded[SCHEMA_OFFSET..schema_end];
    if !schema.is_ascii() {
        return Err(HandshakeError::NonAsciiSchema);
    }
    if encoded[schema_end..].iter().any(|byte| *byte != 0) {
        return Err(HandshakeError::NonzeroPadding);
    }
    let operation_schema = String::from_utf8(schema.to_vec())
        .expect("ASCII workspace handshake schema is valid UTF-8");
    WorkspaceHandshake::new(kind, operation_schema)
}

/// Encode the four-byte length prefix and fixed handshake payload.
pub fn encode_handshake_frame(
    handshake: &WorkspaceHandshake,
) -> Result<[u8; HANDSHAKE_FRAME_BYTES], HandshakeError> {
    let payload = encode_handshake_payload(handshake)?;
    let mut encoded = [0_u8; HANDSHAKE_FRAME_BYTES];
    encoded[..4].copy_from_slice(&(HANDSHAKE_PAYLOAD_BYTES as u32).to_be_bytes());
    encoded[4..].copy_from_slice(&payload);
    Ok(encoded)
}

/// Decode one complete length-prefixed fixed handshake frame.
pub fn decode_handshake_frame(encoded: &[u8]) -> Result<WorkspaceHandshake, HandshakeError> {
    if encoded.len() != HANDSHAKE_FRAME_BYTES {
        return Err(HandshakeError::InvalidFrameLength {
            actual: encoded.len(),
        });
    }
    let length = u32::from_be_bytes(
        encoded[..4]
            .try_into()
            .expect("workspace handshake length prefix has fixed width"),
    );
    if length != HANDSHAKE_PAYLOAD_BYTES as u32 {
        return Err(HandshakeError::InvalidLengthPrefix(length));
    }
    decode_handshake_payload(&encoded[4..])
}

/// Report whether a payload starts with the stable handshake magic.
///
/// Once this returns true, callers must fail closed on any later decode error
/// rather than trying an operation-first legacy parser.
pub fn has_handshake_magic(payload: &[u8]) -> bool {
    payload.starts_with(&WORKSPACE_HANDSHAKE_MAGIC)
}

fn validate_schema(schema: &str) -> Result<(), HandshakeError> {
    let length = schema.len();
    if !(1..=HANDSHAKE_SCHEMA_BYTES).contains(&length) {
        return Err(HandshakeError::InvalidSchemaLength(length));
    }
    if !schema.is_ascii() {
        return Err(HandshakeError::NonAsciiSchema);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const V9: &str = "nokv.workspace.rpc.v9";

    #[test]
    fn client_hello_has_one_canonical_fixed_width_encoding() {
        let hello = WorkspaceHandshake::new(HandshakeKind::ClientHello, V9).unwrap();
        let encoded = encode_handshake_frame(&hello).unwrap();

        let mut expected = [0_u8; HANDSHAKE_FRAME_BYTES];
        expected[..4].copy_from_slice(&(HANDSHAKE_PAYLOAD_BYTES as u32).to_be_bytes());
        expected[4..12].copy_from_slice(b"NOKVHS1\0");
        expected[12] = 1;
        expected[13] = V9.len() as u8;
        expected[20..20 + V9.len()].copy_from_slice(V9.as_bytes());

        assert_eq!(encoded, expected);
        assert_eq!(decode_handshake_frame(&encoded).unwrap(), hello);
    }

    #[test]
    fn all_kinds_round_trip_without_schema_negotiation_fields() {
        for kind in [
            HandshakeKind::ClientHello,
            HandshakeKind::Accepted,
            HandshakeKind::Incompatible,
        ] {
            let expected = WorkspaceHandshake::new(kind, V9).unwrap();
            let encoded = encode_handshake_payload(&expected).unwrap();
            assert_eq!(decode_handshake_payload(&encoded).unwrap(), expected);
        }
    }

    #[test]
    fn decoder_rejects_every_noncanonical_fixed_field() {
        let canonical = encode_handshake_payload(
            &WorkspaceHandshake::new(HandshakeKind::ClientHello, V9).unwrap(),
        )
        .unwrap();

        let mut wrong_magic = canonical;
        wrong_magic[0] ^= 1;
        assert_eq!(
            decode_handshake_payload(&wrong_magic),
            Err(HandshakeError::InvalidMagic)
        );

        let mut wrong_kind = canonical;
        wrong_kind[8] = 0;
        assert_eq!(
            decode_handshake_payload(&wrong_kind),
            Err(HandshakeError::InvalidKind(0))
        );

        for length in [0_u8, 33] {
            let mut wrong_length = canonical;
            wrong_length[9] = length;
            assert_eq!(
                decode_handshake_payload(&wrong_length),
                Err(HandshakeError::InvalidSchemaLength(usize::from(length)))
            );
        }

        let mut nonzero_reserved = canonical;
        nonzero_reserved[10] = 1;
        assert_eq!(
            decode_handshake_payload(&nonzero_reserved),
            Err(HandshakeError::NonzeroReserved)
        );

        let mut non_ascii_schema = canonical;
        non_ascii_schema[16] = 0x80;
        assert_eq!(
            decode_handshake_payload(&non_ascii_schema),
            Err(HandshakeError::NonAsciiSchema)
        );

        let mut nonzero_padding = canonical;
        nonzero_padding[16 + V9.len()] = 1;
        assert_eq!(
            decode_handshake_payload(&nonzero_padding),
            Err(HandshakeError::NonzeroPadding)
        );
    }

    #[test]
    fn frame_decoder_requires_exact_length_prefix_and_payload_size() {
        let hello = WorkspaceHandshake::new(HandshakeKind::ClientHello, V9).unwrap();
        let canonical = encode_handshake_frame(&hello).unwrap();

        assert_eq!(
            decode_handshake_frame(&canonical[..HANDSHAKE_FRAME_BYTES - 1]),
            Err(HandshakeError::InvalidFrameLength {
                actual: HANDSHAKE_FRAME_BYTES - 1,
            })
        );

        let mut wrong_prefix = canonical;
        wrong_prefix[..4].copy_from_slice(&47_u32.to_be_bytes());
        assert_eq!(
            decode_handshake_frame(&wrong_prefix),
            Err(HandshakeError::InvalidLengthPrefix(47))
        );

        let mut trailing = canonical.to_vec();
        trailing.push(0);
        assert_eq!(
            decode_handshake_frame(&trailing),
            Err(HandshakeError::InvalidFrameLength {
                actual: HANDSHAKE_FRAME_BYTES + 1,
            })
        );
    }

    #[test]
    fn constructor_rejects_empty_oversized_and_non_ascii_schema() {
        assert_eq!(
            WorkspaceHandshake::new(HandshakeKind::ClientHello, ""),
            Err(HandshakeError::InvalidSchemaLength(0))
        );
        assert_eq!(
            WorkspaceHandshake::new(HandshakeKind::ClientHello, "a".repeat(33)),
            Err(HandshakeError::InvalidSchemaLength(33))
        );
        assert_eq!(
            WorkspaceHandshake::new(HandshakeKind::ClientHello, "nokv.协议"),
            Err(HandshakeError::NonAsciiSchema)
        );
    }
}
