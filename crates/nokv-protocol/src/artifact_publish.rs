/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use sha2::{Digest as _, Sha256};

use crate::{
    ArtifactManifestRow, ArtifactRevisionIdentity, Digest, DigestUri, ProtocolError, StagedObject,
};

/// Metadata-command row bound used by every artifact publication batch.
pub const MAX_ARTIFACT_PUBLISH_BATCH_ROWS: usize = 192;
/// Durable count bound for staged objects and manifest rows.
pub const MAX_ARTIFACT_PUBLISH_OBJECTS: usize = 1_048_576;
/// Maximum provider-neutral manifest rows returned by one range-plan page.
pub const MAX_ARTIFACT_READ_PLAN_ROWS: usize = 512;

const PUBLICATION_RECORD_ENCODING: u8 = 4;

/// Canonical closure committed by `BeginArtifactPublish`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactPublishPlanSeal {
    pub staged_object_count: u32,
    pub staged_object_seal: Digest,
    pub manifest_row_count: u32,
    pub manifest_seal: Digest,
}

/// Seal the exact provider-neutral rows that later publication batches append.
///
/// This is the one client/server implementation of the publication closure.
pub fn seal_artifact_publish_plan(
    artifact_revision_id: ArtifactRevisionIdentity,
    staged_objects: &[StagedObject],
    manifest_rows: &[ArtifactManifestRow],
) -> Result<ArtifactPublishPlanSeal, ProtocolError> {
    if staged_objects.len() > MAX_ARTIFACT_PUBLISH_OBJECTS {
        return Err(ProtocolError::invalid(
            "artifact_publish.staged_objects",
            format!("exceeds {MAX_ARTIFACT_PUBLISH_OBJECTS} rows"),
        ));
    }
    if manifest_rows.len() > MAX_ARTIFACT_PUBLISH_OBJECTS {
        return Err(ProtocolError::invalid(
            "artifact_publish.manifest_rows",
            format!("exceeds {MAX_ARTIFACT_PUBLISH_OBJECTS} rows"),
        ));
    }
    let mut staged_digest = [0_u8; 32];
    for (index, object) in staged_objects.iter().enumerate() {
        object.validate()?;
        let expected_sequence = u32::try_from(index).map_err(|_| {
            ProtocolError::invalid("artifact_publish.staged_objects", "sequence exceeds u32")
        })?;
        if object.sequence != expected_sequence {
            return Err(ProtocolError::invalid(
                "artifact_publish.staged_objects",
                format!(
                    "expected contiguous sequence {expected_sequence}, got {}",
                    object.sequence
                ),
            ));
        }
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.publish.staged-row.v1\0");
        hasher.update(staged_digest);
        hasher.update(artifact_revision_id.0);
        hasher.update(object.sequence.to_be_bytes());
        hash_bytes(&mut hasher, object.object_identity.as_str().as_bytes())?;
        hasher.update(object.expected_length.to_be_bytes());
        hash_bytes(&mut hasher, object.expected_digest.as_str().as_bytes())?;
        staged_digest = hasher.finalize().into();
    }

    let mut manifest_digest = [0_u8; 32];
    let mut previous_position = None;
    for row in manifest_rows {
        row.validate()?;
        if previous_position.is_some_and(|previous| previous >= row.object_index) {
            return Err(ProtocolError::invalid(
                "artifact_publish.manifest_rows",
                "object indexes must be strictly increasing",
            ));
        }
        previous_position = Some(row.object_index);

        let encoded_row = encode_durable_manifest_row(row)?;
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.publish.manifest-row.v2\0");
        hasher.update(manifest_digest);
        hasher.update(row.object_index.to_be_bytes());
        hash_bytes(&mut hasher, &encoded_row)?;
        manifest_digest = hasher.finalize().into();
    }

    Ok(ArtifactPublishPlanSeal {
        staged_object_count: u32::try_from(staged_objects.len()).map_err(|_| {
            ProtocolError::invalid("artifact_publish.staged_objects", "count exceeds u32")
        })?,
        staged_object_seal: Digest(staged_digest),
        manifest_row_count: u32::try_from(manifest_rows.len()).map_err(|_| {
            ProtocolError::invalid("artifact_publish.manifest_rows", "count exceeds u32")
        })?,
        manifest_seal: Digest(manifest_digest),
    })
}

pub fn sha256_digest_uri(digest: Digest) -> DigestUri {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(7 + digest.0.len() * 2);
    value.push_str("sha256:");
    for byte in digest.0 {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    DigestUri::new(value).expect("canonical SHA-256 digest URI is valid")
}

/// Decode one canonical lower-case `sha256:` URI.
pub fn parse_sha256_digest_uri(uri: &DigestUri) -> Result<Digest, ProtocolError> {
    let encoded = uri.as_str();
    let Some(hex) = encoded.strip_prefix("sha256:") else {
        return Err(ProtocolError::invalid(
            "digest_uri",
            "must use the sha256 scheme",
        ));
    };
    if hex.len() != 64
        || !hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(ProtocolError::invalid(
            "digest_uri",
            "must contain exactly 64 lower-case hexadecimal digits",
        ));
    }
    let mut digest = [0_u8; 32];
    for (index, pair) in hex.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        digest[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(Digest(digest))
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("canonical digest URI was validated"),
    }
}

fn encode_durable_manifest_row(row: &ArtifactManifestRow) -> Result<Vec<u8>, ProtocolError> {
    let mut encoded = Vec::with_capacity(256);
    encoded.push(PUBLICATION_RECORD_ENCODING);
    encoded.extend_from_slice(&row.physical_owner_revision_id.0);
    encoded.extend_from_slice(&row.physical_object_index.to_be_bytes());
    push_bytes(&mut encoded, row.object_identity.as_str().as_bytes())?;
    encoded.extend_from_slice(&row.logical_offset.to_be_bytes());
    encoded.extend_from_slice(&row.object_offset.to_be_bytes());
    encoded.extend_from_slice(&row.length.to_be_bytes());
    push_bytes(&mut encoded, row.digest.as_str().as_bytes())?;
    match row.append_segment {
        None => encoded.push(0),
        Some(segment) => {
            encoded.push(1);
            encoded.extend_from_slice(&segment.segment_sequence.to_be_bytes());
            encoded.extend_from_slice(&segment.segment_offset.to_be_bytes());
        }
    }
    Ok(encoded)
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> Result<(), ProtocolError> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| ProtocolError::invalid("artifact_publish", "byte length exceeds u32"))?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn push_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ProtocolError> {
    let length = u32::try_from(bytes.len())
        .map_err(|_| ProtocolError::invalid("artifact_publish", "byte length exceeds u32"))?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectIdentity;

    fn digest(value: &str) -> DigestUri {
        DigestUri::new(value).unwrap()
    }

    fn staged(sequence: u32, key: &str) -> StagedObject {
        StagedObject {
            sequence,
            object_identity: ObjectIdentity::new(key).unwrap(),
            expected_length: 4,
            expected_digest: digest("sha256:abcd"),
            multipart_token: None,
        }
    }

    fn manifest(index: u64, key: &str) -> ArtifactManifestRow {
        ArtifactManifestRow {
            object_index: index,
            physical_object_index: index,
            logical_offset: index * 4,
            physical_owner_revision_id: ArtifactRevisionIdentity([3; 16]),
            object_identity: ObjectIdentity::new(key).unwrap(),
            object_offset: 0,
            length: 4,
            digest: digest("sha256:abcd"),
            append_segment: None,
        }
    }

    #[test]
    fn publication_plan_seal_is_deterministic_and_domain_separated() {
        let objects = vec![staged(0, "object-a"), staged(1, "object-b")];
        let rows = vec![manifest(0, "object-a"), manifest(1, "object-b")];
        let expected =
            seal_artifact_publish_plan(ArtifactRevisionIdentity([3; 16]), &objects, &rows).unwrap();
        assert_eq!(
            seal_artifact_publish_plan(ArtifactRevisionIdentity([3; 16]), &objects, &rows).unwrap(),
            expected
        );
        assert_ne!(expected.staged_object_seal, expected.manifest_seal);
        assert_eq!(expected.staged_object_count, 2);
        assert_eq!(expected.manifest_row_count, 2);

        let mut changed_physical_index = rows;
        changed_physical_index[1].physical_object_index = 0;
        assert_ne!(
            seal_artifact_publish_plan(
                ArtifactRevisionIdentity([3; 16]),
                &objects,
                &changed_physical_index,
            )
            .unwrap()
            .manifest_seal,
            expected.manifest_seal,
            "the closure must bind the owner-local physical object index",
        );
    }

    #[test]
    fn publication_plan_requires_exact_sequences_and_order() {
        assert!(seal_artifact_publish_plan(
            ArtifactRevisionIdentity([3; 16]),
            &[staged(1, "object-a")],
            &[manifest(0, "object-a")]
        )
        .is_err());
        assert!(seal_artifact_publish_plan(
            ArtifactRevisionIdentity([3; 16]),
            &[staged(0, "object-a")],
            &[manifest(1, "object-a"), manifest(0, "object-b")]
        )
        .is_err());
    }
}
