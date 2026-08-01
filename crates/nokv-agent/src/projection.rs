/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Canonical Workbench-owned projection artifacts.
//!
//! These JSON bodies are presentation artifacts, not metadata authority. The
//! typed commit and restore records remain authoritative; this module only
//! freezes the bytes stored at the two reserved Workbench paths.

use std::collections::BTreeMap;
use std::fmt;

use nokv_types::WorkbenchId;
use serde_json::{json, Map, Value};
use sha2::{Digest as _, Sha256};

pub const RUN_MANIFEST_V1_SCHEMA: &str = "nokv.workbench.run_manifest.v1";
const RUN_MANIFEST_PROJECTION_INPUT_DOMAIN: &[u8] =
    b"nokv.workbench.run_manifest.projection_input.v1\0";
pub const RESTORE_MANIFEST_V1_SCHEMA: &str = "nokv.workbench.restore_manifest.v1";

const RUN_MANIFEST_FIELDS: [&str; 8] = [
    "commit_identity",
    "committed_at_unix_seconds",
    "content_digest_uri",
    "manifest",
    "manifest_digest_uri",
    "schema",
    "workbench_id",
    "workbench_path",
];
const RESTORE_MANIFEST_FIELDS: [&str; 8] = [
    "destination_path",
    "destination_workbench_id",
    "operation_id",
    "restored_from",
    "schema",
    "snapshot_id",
    "source_path",
    "source_workbench_id",
];
const RESTORED_FROM_FIELDS: [&str; 3] = ["path", "snapshot_id", "workbench_id"];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectionError {
    message: String,
}

impl ProjectionError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ProjectionError {}

#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedRunManifestV1 {
    pub workbench_id: WorkbenchId,
    pub workbench_path: String,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    pub commit_identity: [u8; 32],
    pub committed_at_unix_seconds: u64,
    pub manifest: Value,
    pub canonical_manifest: Vec<u8>,
    pub envelope: Value,
    pub canonical_envelope: Vec<u8>,
    pub envelope_digest_uri: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedRestoreManifestV1 {
    pub operation_id: [u8; 16],
    pub source_workbench_id: WorkbenchId,
    pub source_path: String,
    pub destination_workbench_id: WorkbenchId,
    pub destination_path: String,
    pub snapshot_id: u64,
    pub canonical_envelope: Vec<u8>,
    pub envelope_digest_uri: String,
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, ProjectionError> {
    serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| ProjectionError::new(format!("canonical JSON encoding failed: {error}")))
}

pub fn workbench_commit_identity(
    workbench_id: &WorkbenchId,
    content_digest_uri: &str,
    manifest_digest_uri: &str,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workbench.commit_identity.v1\0");
    hash_length_prefixed(&mut hasher, workbench_id.as_str().as_bytes());
    hash_length_prefixed(&mut hasher, content_digest_uri.as_bytes());
    hash_length_prefixed(&mut hasher, manifest_digest_uri.as_bytes());
    hasher.finalize().into()
}

/// Digest every caller-known input to the canonical run-manifest projection.
/// The first owner-observed commit time is deliberately excluded because it
/// becomes durable metadata preparation rather than caller input.
pub fn run_manifest_projection_input_digest_v1(
    workbench_id: &WorkbenchId,
    workbench_path: &str,
    content_digest_uri: &str,
    canonical_manifest: &[u8],
    manifest_digest_uri: &str,
    commit_identity: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(RUN_MANIFEST_PROJECTION_INPUT_DOMAIN);
    hash_length_prefixed(&mut hasher, workbench_id.as_str().as_bytes());
    hash_length_prefixed(&mut hasher, workbench_path.as_bytes());
    hash_length_prefixed(&mut hasher, content_digest_uri.as_bytes());
    hash_length_prefixed(&mut hasher, canonical_manifest);
    hash_length_prefixed(&mut hasher, manifest_digest_uri.as_bytes());
    hasher.update(commit_identity);
    hasher.finalize().into()
}

pub fn build_run_manifest_v1(
    workbench_id: &WorkbenchId,
    workbench_path: &str,
    content_digest_uri: &str,
    canonical_manifest: &[u8],
    manifest_digest_uri: &str,
    commit_identity: [u8; 32],
    committed_at_unix_seconds: u64,
) -> Result<Vec<u8>, ProjectionError> {
    validate_presentation_path("workbench_path", workbench_path)?;
    validate_digest_uri("content_digest_uri", content_digest_uri)?;
    validate_digest_uri("manifest_digest_uri", manifest_digest_uri)?;
    let manifest: Value = serde_json::from_slice(canonical_manifest)
        .map_err(|error| ProjectionError::new(format!("manifest is not valid JSON: {error}")))?;
    if !manifest.is_object() {
        return Err(ProjectionError::new("manifest must be a JSON object"));
    }
    let recanonicalized = canonical_json_bytes(&manifest)?;
    if recanonicalized != canonical_manifest {
        return Err(ProjectionError::new(
            "manifest bytes are not recursively canonical JSON",
        ));
    }
    let actual_manifest_digest = digest_uri(canonical_manifest);
    if actual_manifest_digest != manifest_digest_uri {
        return Err(ProjectionError::new(format!(
            "manifest_digest_uri mismatch: expected {actual_manifest_digest}, got {manifest_digest_uri}"
        )));
    }
    let expected_commit_identity =
        workbench_commit_identity(workbench_id, content_digest_uri, manifest_digest_uri);
    if commit_identity != expected_commit_identity {
        return Err(ProjectionError::new(
            "commit_identity does not match the canonical Workbench identity",
        ));
    }
    let envelope = json!({
        "schema": RUN_MANIFEST_V1_SCHEMA,
        "workbench_id": workbench_id.as_str(),
        "workbench_path": workbench_path,
        "content_digest_uri": content_digest_uri,
        "manifest_digest_uri": manifest_digest_uri,
        "commit_identity": lowercase_hex(&commit_identity),
        "committed_at_unix_seconds": committed_at_unix_seconds,
        "manifest": manifest,
    });
    let bytes = canonical_json_bytes(&envelope)?;
    verify_run_manifest_v1(&bytes)?;
    Ok(bytes)
}

pub fn verify_run_manifest_v1(bytes: &[u8]) -> Result<VerifiedRunManifestV1, ProjectionError> {
    let envelope: Value = serde_json::from_slice(bytes).map_err(|error| {
        ProjectionError::new(format!("run manifest is not valid JSON: {error}"))
    })?;
    require_canonical_bytes("run manifest", bytes, &envelope)?;
    let object = exact_object("run manifest", &envelope, &RUN_MANIFEST_FIELDS)?;
    require_string(object, "schema", "run manifest")
        .and_then(|schema| require_equal("run manifest schema", schema, RUN_MANIFEST_V1_SCHEMA))?;
    let workbench_id = WorkbenchId::new(require_string(object, "workbench_id", "run manifest")?)
        .map_err(|error| {
            ProjectionError::new(format!("invalid run manifest workbench_id: {error}"))
        })?;
    let workbench_path = require_string(object, "workbench_path", "run manifest")?.to_owned();
    validate_presentation_path("workbench_path", &workbench_path)?;
    let content_digest_uri =
        require_string(object, "content_digest_uri", "run manifest")?.to_owned();
    validate_digest_uri("content_digest_uri", &content_digest_uri)?;
    let manifest_digest_uri =
        require_string(object, "manifest_digest_uri", "run manifest")?.to_owned();
    validate_digest_uri("manifest_digest_uri", &manifest_digest_uri)?;
    let manifest = object
        .get("manifest")
        .filter(|manifest| manifest.is_object())
        .cloned()
        .ok_or_else(|| ProjectionError::new("run manifest manifest must be a JSON object"))?;
    let canonical_manifest = canonical_json_bytes(&manifest)?;
    let actual_manifest_digest = digest_uri(&canonical_manifest);
    if manifest_digest_uri != actual_manifest_digest {
        return Err(ProjectionError::new(format!(
            "run manifest manifest_digest_uri mismatch: expected {actual_manifest_digest}, got {manifest_digest_uri}"
        )));
    }
    let commit_identity_text = require_string(object, "commit_identity", "run manifest")?;
    let commit_identity = decode_lowercase_hex::<32>("commit_identity", commit_identity_text)?;
    let expected_commit_identity =
        workbench_commit_identity(&workbench_id, &content_digest_uri, &manifest_digest_uri);
    if commit_identity != expected_commit_identity {
        return Err(ProjectionError::new(
            "run manifest commit_identity does not match its canonical fields",
        ));
    }
    let committed_at_unix_seconds = object
        .get("committed_at_unix_seconds")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ProjectionError::new(
                "run manifest committed_at_unix_seconds must be an unsigned integer",
            )
        })?;
    Ok(VerifiedRunManifestV1 {
        workbench_id,
        workbench_path,
        content_digest_uri,
        manifest_digest_uri,
        commit_identity,
        committed_at_unix_seconds,
        manifest,
        canonical_manifest,
        envelope,
        canonical_envelope: bytes.to_vec(),
        envelope_digest_uri: digest_uri(bytes),
    })
}

pub fn build_restore_manifest_v1(
    operation_id: [u8; 16],
    source_workbench_id: &WorkbenchId,
    source_path: &str,
    destination_workbench_id: &WorkbenchId,
    destination_path: &str,
    snapshot_id: u64,
) -> Result<Vec<u8>, ProjectionError> {
    validate_presentation_path("source_path", source_path)?;
    validate_presentation_path("destination_path", destination_path)?;
    if source_workbench_id == destination_workbench_id {
        return Err(ProjectionError::new(
            "restore destination workbench must differ from its source",
        ));
    }
    if snapshot_id == 0 {
        return Err(ProjectionError::new(
            "restore snapshot_id must be greater than zero",
        ));
    }
    let operation_id = lowercase_hex(&operation_id);
    let envelope = json!({
        "schema": RESTORE_MANIFEST_V1_SCHEMA,
        "operation_id": operation_id,
        "restored_from": {
            "workbench_id": source_workbench_id.as_str(),
            "path": source_path,
            "snapshot_id": snapshot_id,
        },
        "source_workbench_id": source_workbench_id.as_str(),
        "source_path": source_path,
        "destination_workbench_id": destination_workbench_id.as_str(),
        "destination_path": destination_path,
        "snapshot_id": snapshot_id,
    });
    let bytes = canonical_json_bytes(&envelope)?;
    verify_restore_manifest_v1(&bytes)?;
    Ok(bytes)
}

pub fn verify_restore_manifest_v1(
    bytes: &[u8],
) -> Result<VerifiedRestoreManifestV1, ProjectionError> {
    let envelope: Value = serde_json::from_slice(bytes).map_err(|error| {
        ProjectionError::new(format!("restore manifest is not valid JSON: {error}"))
    })?;
    require_canonical_bytes("restore manifest", bytes, &envelope)?;
    let object = exact_object("restore manifest", &envelope, &RESTORE_MANIFEST_FIELDS)?;
    require_string(object, "schema", "restore manifest").and_then(|schema| {
        require_equal(
            "restore manifest schema",
            schema,
            RESTORE_MANIFEST_V1_SCHEMA,
        )
    })?;
    let operation_id = decode_lowercase_hex::<16>(
        "operation_id",
        require_string(object, "operation_id", "restore manifest")?,
    )?;
    let source_workbench_id = WorkbenchId::new(require_string(
        object,
        "source_workbench_id",
        "restore manifest",
    )?)
    .map_err(|error| ProjectionError::new(format!("invalid source_workbench_id: {error}")))?;
    let destination_workbench_id = WorkbenchId::new(require_string(
        object,
        "destination_workbench_id",
        "restore manifest",
    )?)
    .map_err(|error| ProjectionError::new(format!("invalid destination_workbench_id: {error}")))?;
    if source_workbench_id == destination_workbench_id {
        return Err(ProjectionError::new(
            "restore destination workbench must differ from its source",
        ));
    }
    let source_path = require_string(object, "source_path", "restore manifest")?.to_owned();
    let destination_path =
        require_string(object, "destination_path", "restore manifest")?.to_owned();
    validate_presentation_path("source_path", &source_path)?;
    validate_presentation_path("destination_path", &destination_path)?;
    let snapshot_id = object
        .get("snapshot_id")
        .and_then(Value::as_u64)
        .filter(|snapshot_id| *snapshot_id != 0)
        .ok_or_else(|| {
            ProjectionError::new("restore manifest snapshot_id must be a positive integer")
        })?;
    let restored_from = object
        .get("restored_from")
        .ok_or_else(|| ProjectionError::new("restore manifest restored_from is required"))?;
    let restored_from = exact_object(
        "restore manifest restored_from",
        restored_from,
        &RESTORED_FROM_FIELDS,
    )?;
    if require_string(restored_from, "workbench_id", "restored_from")?
        != source_workbench_id.as_str()
        || require_string(restored_from, "path", "restored_from")? != source_path
        || restored_from.get("snapshot_id").and_then(Value::as_u64) != Some(snapshot_id)
    {
        return Err(ProjectionError::new(
            "restore manifest restored_from does not match its source provenance",
        ));
    }
    Ok(VerifiedRestoreManifestV1 {
        operation_id,
        source_workbench_id,
        source_path,
        destination_workbench_id,
        destination_path,
        snapshot_id,
        canonical_envelope: bytes.to_vec(),
        envelope_digest_uri: digest_uri(bytes),
    })
}

fn canonical_json_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_json_value).collect()),
        Value::Object(values) => {
            let sorted = values
                .iter()
                .map(|(key, value)| (key.clone(), canonical_json_value(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        value => value.clone(),
    }
}

fn require_canonical_bytes(
    label: &str,
    bytes: &[u8],
    value: &Value,
) -> Result<(), ProjectionError> {
    let canonical = canonical_json_bytes(value)?;
    if canonical != bytes {
        return Err(ProjectionError::new(format!(
            "{label} bytes are not recursively canonical compact JSON"
        )));
    }
    Ok(())
}

fn exact_object<'a>(
    label: &str,
    value: &'a Value,
    expected_fields: &[&str],
) -> Result<&'a Map<String, Value>, ProjectionError> {
    let object = value
        .as_object()
        .ok_or_else(|| ProjectionError::new(format!("{label} must be a JSON object")))?;
    let actual = object.keys().map(String::as_str).collect::<Vec<_>>();
    if actual.len() != expected_fields.len()
        || !expected_fields.iter().all(|field| actual.contains(field))
    {
        return Err(ProjectionError::new(format!(
            "{label} fields do not match the v1 contract"
        )));
    }
    Ok(object)
}

fn require_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    label: &str,
) -> Result<&'a str, ProjectionError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ProjectionError::new(format!("{label} {field} must be a string")))
}

fn require_equal(label: &str, actual: &str, expected: &str) -> Result<(), ProjectionError> {
    if actual != expected {
        return Err(ProjectionError::new(format!(
            "{label} must equal {expected:?}"
        )));
    }
    Ok(())
}

fn validate_digest_uri(field: &str, value: &str) -> Result<(), ProjectionError> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ProjectionError::new(format!(
            "{field} must be sha256 followed by 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_presentation_path(field: &str, value: &str) -> Result<(), ProjectionError> {
    if !value.starts_with('/') || value == "/" || value.ends_with('/') {
        return Err(ProjectionError::new(format!(
            "{field} must be a non-root absolute presentation path without a trailing slash"
        )));
    }
    if value.contains('\\') || value.contains('\0') {
        return Err(ProjectionError::new(format!(
            "{field} must not contain backslashes or NUL"
        )));
    }
    if value
        .split('/')
        .skip(1)
        .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        return Err(ProjectionError::new(format!(
            "{field} contains an invalid path component"
        )));
    }
    Ok(())
}

pub(crate) fn digest_uri(bytes: &[u8]) -> String {
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    format!("sha256:{}", lowercase_hex(&digest))
}

fn hash_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value);
}

pub(crate) fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_lowercase_hex<const N: usize>(
    field: &str,
    value: &str,
) -> Result<[u8; N], ProjectionError> {
    if value.len() != N * 2 {
        return Err(ProjectionError::new(format!(
            "{field} must contain exactly {} lowercase hexadecimal characters",
            N * 2
        )));
    }
    let mut decoded = [0; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex_nibble(pair[0]).ok_or_else(|| {
            ProjectionError::new(format!("{field} must contain lowercase hexadecimal"))
        })?;
        let low = decode_hex_nibble(pair[1]).ok_or_else(|| {
            ProjectionError::new(format!("{field} must contain lowercase hexadecimal"))
        })?;
        decoded[index] = (high << 4) | low;
    }
    Ok(decoded)
}

fn decode_hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workbench(value: &str) -> WorkbenchId {
        WorkbenchId::new(value).unwrap()
    }

    #[test]
    fn run_manifest_bytes_are_canonical_and_bind_the_user_manifest() {
        let id = workbench("wb-main");
        let canonical_manifest = br#"{"a":[2,1],"z":{"a":1,"b":2}}"#;
        let manifest_digest_uri = digest_uri(canonical_manifest);
        let content_digest_uri = format!("sha256:{}", "11".repeat(32));
        let commit_identity =
            workbench_commit_identity(&id, &content_digest_uri, &manifest_digest_uri);
        let body = build_run_manifest_v1(
            &id,
            "/agents/test/wb/wb-main",
            &content_digest_uri,
            canonical_manifest,
            &manifest_digest_uri,
            commit_identity,
            1_700_000_000,
        )
        .unwrap();
        let verified = verify_run_manifest_v1(&body).unwrap();
        assert_eq!(verified.commit_identity, commit_identity);
        assert_eq!(verified.canonical_manifest, canonical_manifest);
        assert_eq!(
            canonical_json_bytes(&serde_json::from_slice(&body).unwrap()).unwrap(),
            body
        );
        assert!(!String::from_utf8(body).unwrap().contains("nokv_workspace"));
    }

    #[test]
    fn run_manifest_projection_input_digest_has_frozen_domain_and_field_order() {
        let id = workbench("wb-main");
        let canonical_manifest = br#"{"a":[2,1],"z":{"a":1,"b":2}}"#;
        let manifest_digest_uri = digest_uri(canonical_manifest);
        let content_digest_uri = format!("sha256:{}", "11".repeat(32));
        let commit_identity =
            workbench_commit_identity(&id, &content_digest_uri, &manifest_digest_uri);
        let digest = run_manifest_projection_input_digest_v1(
            &id,
            "/agents/test/wb/wb-main",
            &content_digest_uri,
            canonical_manifest,
            &manifest_digest_uri,
            commit_identity,
        );
        assert_eq!(
            lowercase_hex(&digest),
            "cf564a6d52bca52da05f5b47d4b2030cfc347909077415c977a337974e89fe5f"
        );

        assert_ne!(
            run_manifest_projection_input_digest_v1(
                &id,
                "/agents/test/wb/other",
                &content_digest_uri,
                canonical_manifest,
                &manifest_digest_uri,
                commit_identity,
            ),
            digest
        );
        assert_ne!(
            run_manifest_projection_input_digest_v1(
                &id,
                "/agents/test/wb/wb-main",
                &content_digest_uri,
                br#"{"different":true}"#,
                &manifest_digest_uri,
                commit_identity,
            ),
            digest
        );
    }

    #[test]
    fn run_manifest_rejects_noncanonical_or_wrong_identity_bytes() {
        let pretty = br#"{\n  \"schema\": \"nokv.workbench.run_manifest.v1\"\n}"#;
        assert!(verify_run_manifest_v1(pretty).is_err());

        let id = workbench("wb-main");
        let manifest = br#"{"task":"ptycho"}"#;
        let manifest_digest_uri = digest_uri(manifest);
        let content_digest_uri = format!("sha256:{}", "22".repeat(32));
        assert!(build_run_manifest_v1(
            &id,
            "/agents/test/wb/wb-main",
            &content_digest_uri,
            manifest,
            &manifest_digest_uri,
            [9; 32],
            1,
        )
        .is_err());
    }

    #[test]
    fn run_manifest_timestamp_changes_projection_but_not_commit_identity() {
        let id = workbench("wb-main");
        let manifest = br#"{"task":"ptycho"}"#;
        let manifest_digest_uri = digest_uri(manifest);
        let content_digest_uri = format!("sha256:{}", "33".repeat(32));
        let commit_identity =
            workbench_commit_identity(&id, &content_digest_uri, &manifest_digest_uri);
        let first = build_run_manifest_v1(
            &id,
            "/agents/test/wb/wb-main",
            &content_digest_uri,
            manifest,
            &manifest_digest_uri,
            commit_identity,
            1,
        )
        .unwrap();
        let second = build_run_manifest_v1(
            &id,
            "/agents/test/wb/wb-main",
            &content_digest_uri,
            manifest,
            &manifest_digest_uri,
            commit_identity,
            2,
        )
        .unwrap();

        assert_ne!(first, second);
        assert_ne!(digest_uri(&first), digest_uri(&second));
        assert_eq!(
            verify_run_manifest_v1(&first).unwrap().commit_identity,
            verify_run_manifest_v1(&second).unwrap().commit_identity
        );
    }

    #[test]
    fn restore_manifest_preserves_the_frozen_v1_provenance_only() {
        let body = build_restore_manifest_v1(
            [0x11; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            7,
        )
        .unwrap();
        let verified = verify_restore_manifest_v1(&body).unwrap();
        assert_eq!(verified.operation_id, [0x11; 16]);
        assert_eq!(verified.snapshot_id, 7);
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains(RESTORE_MANIFEST_V1_SCHEMA));
        assert!(!text.contains("member_count"));
        assert!(!text.contains("member_digest"));
        assert!(!text.contains("nokv_workspace"));
    }

    #[test]
    fn restore_manifest_operation_id_changes_projection_identity() {
        let first = build_restore_manifest_v1(
            [0x11; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            7,
        )
        .unwrap();
        let second = build_restore_manifest_v1(
            [0x22; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            7,
        )
        .unwrap();

        assert_ne!(first, second);
        assert_ne!(digest_uri(&first), digest_uri(&second));
        assert_eq!(
            verify_restore_manifest_v1(&first).unwrap().operation_id,
            [0x11; 16]
        );
        assert_eq!(
            verify_restore_manifest_v1(&second).unwrap().operation_id,
            [0x22; 16]
        );
    }
}
