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
const RESTORED_CONTENT_DIGEST_DOMAIN: &[u8] = b"nokv.workbench.restored_content_digest.v1\0";
pub const RESTORE_MANIFEST_V1_SCHEMA: &str = "nokv.workbench.restore_manifest.v1";
pub const RESTORE_MANIFEST_V2_SCHEMA: &str = "nokv.workbench.restore_manifest.v2";

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
// v2 changes exactly one thing about v1: the snapshot id becomes a
// discriminated source that can also name a commit. Every other field keeps
// its v1 place, because a durable format that moves fields it did not need
// to move breaks readers for no reason.
const RESTORE_MANIFEST_V2_FIELDS: [&str; 7] = [
    "destination_path",
    "destination_workbench_id",
    "operation_id",
    "restored_from",
    "schema",
    "source_path",
    "source_workbench_id",
];
const RESTORED_FROM_V2_FIELDS: [&str; 3] = ["path", "source", "workbench_id"];

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

/// The canonical inputs one Workbench commit is built from.
///
/// Every caller -- CLI, MCP, Python SDK -- must derive these the same way:
/// the manifest digest and the stable commit id both feed the durable commit
/// identity and the operation's idempotency, so two callers that shape them
/// differently would write two commits for one intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchCommitInputs {
    pub canonical_manifest: Vec<u8>,
    pub manifest_digest_uri: String,
    pub stable_commit_id: [u8; 32],
}

/// Shapes the canonical inputs for one Workbench commit.
pub fn workbench_commit_inputs(
    workbench_id: &WorkbenchId,
    manifest: &Value,
    content_digest_uri: &str,
) -> Result<WorkbenchCommitInputs, ProjectionError> {
    if !manifest.is_object() {
        return Err(ProjectionError::new("commit manifest must be an object"));
    }
    let canonical_manifest = canonical_json_bytes(manifest)?;
    let manifest_digest_uri = digest_uri(&canonical_manifest);
    let stable_commit_id =
        workbench_commit_identity(workbench_id, content_digest_uri, &manifest_digest_uri);
    Ok(WorkbenchCommitInputs {
        canonical_manifest,
        manifest_digest_uri,
        stable_commit_id,
    })
}

/// Decodes a commit identity as the CLI and SDK present it: 64 lowercase hex.
pub fn decode_commit_identity(value: &str) -> Result<[u8; 32], ProjectionError> {
    decode_lowercase_hex::<32>("commit_id", value)
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

/// Rebuild a committed run-manifest projection for a restored destination.
///
/// The source envelope is accepted only as canonical v1 input. Its user
/// manifest commitment is retained, while the effective content commitment
/// and all Workbench-owned binding fields are rebuilt for the destination
/// through the one canonical run-manifest builder.
pub fn build_restored_run_manifest_v1(
    source_run_manifest: &[u8],
    destination_workbench_id: &WorkbenchId,
    destination_workbench_path: &str,
    effective_content_digest_uri: &str,
    destination_commit_identity: [u8; 32],
    destination_committed_at_unix_seconds: u64,
) -> Result<Vec<u8>, ProjectionError> {
    let source = verify_run_manifest_v1(source_run_manifest)?;
    if source.workbench_id == *destination_workbench_id {
        return Err(ProjectionError::new(
            "restore destination workbench must differ from its source",
        ));
    }
    if destination_committed_at_unix_seconds == 0 {
        return Err(ProjectionError::new(
            "destination committed_at_unix_seconds must be greater than zero",
        ));
    }
    build_run_manifest_v1(
        destination_workbench_id,
        destination_workbench_path,
        effective_content_digest_uri,
        &source.canonical_manifest,
        &source.manifest_digest_uri,
        destination_commit_identity,
        destination_committed_at_unix_seconds,
    )
}

/// Choose the destination content commitment for one frozen restore source.
///
/// An unchanged snapshot retains the caller-owned source commitment. A dirty
/// snapshot instead commits to its exact materialized member seal under a
/// separate domain so two different uncommitted trees cannot reuse one
/// destination commit identity.
pub fn restore_effective_content_digest_uri_v1(
    source_content_digest_uri: &str,
    source_matches_base_commit: bool,
    materialized_member_digest: [u8; 32],
) -> Result<String, ProjectionError> {
    validate_digest_uri("source_content_digest_uri", source_content_digest_uri)?;
    if source_matches_base_commit {
        return Ok(source_content_digest_uri.to_owned());
    }
    let mut hasher = Sha256::new();
    hasher.update(RESTORED_CONTENT_DIGEST_DOMAIN);
    hasher.update(materialized_member_digest);
    let digest: [u8; 32] = hasher.finalize().into();
    Ok(format!("sha256:{}", lowercase_hex(&digest)))
}

/// Where a restore read its frozen state from.
///
/// A snapshot is a lease and expires; a commit is durable. A restore manifest
/// has to say which one it used, or a replayed restore cannot tell whether it
/// is the same restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RestoreManifestSource {
    Snapshot { snapshot_id: u64 },
    Commit { commit_id: [u8; 32] },
}

/// A verified restore manifest of either schema version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedRestoreManifest {
    pub operation_id: [u8; 16],
    pub source_workbench_id: WorkbenchId,
    pub source_path: String,
    pub destination_workbench_id: WorkbenchId,
    pub destination_path: String,
    pub source: RestoreManifestSource,
    pub canonical_envelope: Vec<u8>,
    pub envelope_digest_uri: String,
}

/// Builds the v2 restore manifest, which can name either source.
pub fn build_restore_manifest_v2(
    operation_id: [u8; 16],
    source_workbench_id: &WorkbenchId,
    source_path: &str,
    destination_workbench_id: &WorkbenchId,
    destination_path: &str,
    source: RestoreManifestSource,
) -> Result<Vec<u8>, ProjectionError> {
    validate_presentation_path("source_path", source_path)?;
    validate_presentation_path("destination_path", destination_path)?;
    if source_workbench_id == destination_workbench_id {
        return Err(ProjectionError::new(
            "restore destination workbench must differ from its source",
        ));
    }
    let source_value = match source {
        RestoreManifestSource::Snapshot { snapshot_id } => {
            if snapshot_id == 0 {
                return Err(ProjectionError::new(
                    "restore snapshot_id must be greater than zero",
                ));
            }
            json!({ "kind": "snapshot", "snapshot_id": snapshot_id })
        }
        RestoreManifestSource::Commit { commit_id } => {
            if commit_id == [0u8; 32] {
                return Err(ProjectionError::new("restore commit_id must be non-zero"));
            }
            json!({ "kind": "commit", "commit_id": lowercase_hex(&commit_id) })
        }
    };
    let envelope = json!({
        "schema": RESTORE_MANIFEST_V2_SCHEMA,
        "operation_id": lowercase_hex(&operation_id),
        "restored_from": {
            "workbench_id": source_workbench_id.as_str(),
            "path": source_path,
            "source": source_value,
        },
        "source_workbench_id": source_workbench_id.as_str(),
        "source_path": source_path,
        "destination_workbench_id": destination_workbench_id.as_str(),
        "destination_path": destination_path,
    });
    let bytes = canonical_json_bytes(&envelope)?;
    verify_restore_manifest(&bytes)?;
    Ok(bytes)
}

/// Verifies a restore manifest of either schema version.
///
/// v1 envelopes stay readable: they are durable artifacts inside workbenches
/// that were restored before v2 existed.
pub fn verify_restore_manifest(bytes: &[u8]) -> Result<VerifiedRestoreManifest, ProjectionError> {
    let envelope: Value = serde_json::from_slice(bytes).map_err(|error| {
        ProjectionError::new(format!("restore manifest is not valid JSON: {error}"))
    })?;
    // Canonicality first, exactly as each version checks it: a non-canonical
    // envelope must be reported as such rather than as a missing field.
    require_canonical_bytes("restore manifest", bytes, &envelope)?;
    let schema = envelope
        .get("schema")
        .and_then(Value::as_str)
        .ok_or_else(|| ProjectionError::new("restore manifest schema is required"))?;
    if schema == RESTORE_MANIFEST_V1_SCHEMA {
        let verified = verify_restore_manifest_v1(bytes)?;
        return Ok(VerifiedRestoreManifest {
            operation_id: verified.operation_id,
            source_workbench_id: verified.source_workbench_id,
            source_path: verified.source_path,
            destination_workbench_id: verified.destination_workbench_id,
            destination_path: verified.destination_path,
            source: RestoreManifestSource::Snapshot {
                snapshot_id: verified.snapshot_id,
            },
            canonical_envelope: verified.canonical_envelope,
            envelope_digest_uri: verified.envelope_digest_uri,
        });
    }
    verify_restore_manifest_v2(bytes)
}

/// Verifies exactly the v2 schema.
pub fn verify_restore_manifest_v2(
    bytes: &[u8],
) -> Result<VerifiedRestoreManifest, ProjectionError> {
    let envelope: Value = serde_json::from_slice(bytes).map_err(|error| {
        ProjectionError::new(format!("restore manifest is not valid JSON: {error}"))
    })?;
    require_canonical_bytes("restore manifest", bytes, &envelope)?;
    let object = exact_object("restore manifest", &envelope, &RESTORE_MANIFEST_V2_FIELDS)?;
    require_string(object, "schema", "restore manifest").and_then(|schema| {
        require_equal(
            "restore manifest schema",
            schema,
            RESTORE_MANIFEST_V2_SCHEMA,
        )
    })?;
    let operation_id = decode_lowercase_hex::<16>(
        "operation_id",
        require_string(object, "operation_id", "restore manifest")?,
    )?;
    let source_workbench_id = WorkbenchId::new(
        require_string(object, "source_workbench_id", "restore manifest")?.to_owned(),
    )
    .map_err(|error| ProjectionError::new(format!("invalid source_workbench_id: {error}")))?;
    let source_path = require_string(object, "source_path", "restore manifest")?.to_owned();
    let destination_workbench_id = WorkbenchId::new(
        require_string(object, "destination_workbench_id", "restore manifest")?.to_owned(),
    )
    .map_err(|error| ProjectionError::new(format!("invalid destination_workbench_id: {error}")))?;
    let destination_path =
        require_string(object, "destination_path", "restore manifest")?.to_owned();
    validate_presentation_path("source_path", &source_path)?;
    validate_presentation_path("destination_path", &destination_path)?;

    let restored_from = object
        .get("restored_from")
        .ok_or_else(|| ProjectionError::new("restore manifest restored_from is required"))?;
    let restored_from = exact_object(
        "restore manifest restored_from",
        restored_from,
        &RESTORED_FROM_V2_FIELDS,
    )?;
    // restored_from restates the source so the envelope reads on its own; the
    // two statements have to agree or the manifest contradicts itself.
    if require_string(restored_from, "workbench_id", "restored_from")?
        != source_workbench_id.as_str()
    {
        return Err(ProjectionError::new(
            "restore manifest restored_from disagrees with its source workbench",
        ));
    }
    if source_workbench_id == destination_workbench_id {
        return Err(ProjectionError::new(
            "restore destination workbench must differ from its source",
        ));
    }
    if require_string(restored_from, "path", "restored_from")? != source_path {
        return Err(ProjectionError::new(
            "restore manifest restored_from disagrees with its source path",
        ));
    }
    let source_object = restored_from
        .get("source")
        .and_then(Value::as_object)
        .ok_or_else(|| ProjectionError::new("restored_from source must be an object"))?;
    let kind = source_object
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| ProjectionError::new("restored_from source kind is required"))?;
    let source = match kind {
        "snapshot" => {
            if source_object.len() != 2 {
                return Err(ProjectionError::new(
                    "a snapshot source carries exactly kind and snapshot_id",
                ));
            }
            let snapshot_id = source_object
                .get("snapshot_id")
                .and_then(Value::as_u64)
                .filter(|snapshot_id| *snapshot_id != 0)
                .ok_or_else(|| {
                    ProjectionError::new("restore snapshot_id must be a positive integer")
                })?;
            RestoreManifestSource::Snapshot { snapshot_id }
        }
        "commit" => {
            if source_object.len() != 2 {
                return Err(ProjectionError::new(
                    "a commit source carries exactly kind and commit_id",
                ));
            }
            let commit_id = decode_lowercase_hex::<32>(
                "commit_id",
                source_object
                    .get("commit_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| ProjectionError::new("restore commit_id is required"))?,
            )?;
            if commit_id == [0u8; 32] {
                return Err(ProjectionError::new("restore commit_id must be non-zero"));
            }
            RestoreManifestSource::Commit { commit_id }
        }
        other => {
            return Err(ProjectionError::new(format!(
                "unknown restore source kind: {other}"
            )))
        }
    };
    let canonical_envelope = canonical_json_bytes(&envelope)?;
    let envelope_digest_uri = digest_uri(&canonical_envelope);
    Ok(VerifiedRestoreManifest {
        operation_id,
        source_workbench_id,
        source_path,
        destination_workbench_id,
        destination_path,
        source,
        canonical_envelope,
        envelope_digest_uri,
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

pub(crate) fn hash_length_prefixed(hasher: &mut Sha256, value: &[u8]) {
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
    fn restored_run_manifest_rebinds_source_content_to_the_destination() {
        let source_id = workbench("source");
        let destination_id = workbench("destination");
        let source_manifest = br#"{"nested":{"a":1,"b":2},"task":"ptycho"}"#;
        let manifest_digest_uri = digest_uri(source_manifest);
        let content_digest_uri = format!("sha256:{}", "44".repeat(32));
        let source_commit_identity =
            workbench_commit_identity(&source_id, &content_digest_uri, &manifest_digest_uri);
        let source = build_run_manifest_v1(
            &source_id,
            "/agents/test/wb/source",
            &content_digest_uri,
            source_manifest,
            &manifest_digest_uri,
            source_commit_identity,
            1_700_000_000,
        )
        .unwrap();
        let destination_commit_identity =
            workbench_commit_identity(&destination_id, &content_digest_uri, &manifest_digest_uri);

        let restored = build_restored_run_manifest_v1(
            &source,
            &destination_id,
            "/agents/test/wb/destination",
            &content_digest_uri,
            destination_commit_identity,
            1_800_000_000,
        )
        .unwrap();
        let verified = verify_run_manifest_v1(&restored).unwrap();

        assert_eq!(verified.workbench_id, destination_id);
        assert_eq!(verified.workbench_path, "/agents/test/wb/destination");
        assert_eq!(verified.commit_identity, destination_commit_identity);
        assert_eq!(verified.committed_at_unix_seconds, 1_800_000_000);
        assert_eq!(verified.content_digest_uri, content_digest_uri);
        assert_eq!(verified.manifest_digest_uri, manifest_digest_uri);
        assert_eq!(verified.canonical_manifest, source_manifest);
        assert_ne!(verified.commit_identity, source_commit_identity);
        assert_ne!(restored, source);
    }

    #[test]
    fn restored_run_manifest_is_deterministic_and_destination_owned() {
        let source_id = workbench("source");
        let source_manifest = br#"{"task":"ptycho"}"#;
        let manifest_digest_uri = digest_uri(source_manifest);
        let content_digest_uri = format!("sha256:{}", "55".repeat(32));
        let source_commit_identity =
            workbench_commit_identity(&source_id, &content_digest_uri, &manifest_digest_uri);
        let source = build_run_manifest_v1(
            &source_id,
            "/agents/test/wb/source",
            &content_digest_uri,
            source_manifest,
            &manifest_digest_uri,
            source_commit_identity,
            1,
        )
        .unwrap();
        let first_destination_id = workbench("destination-one");
        let first_destination_commit_identity = workbench_commit_identity(
            &first_destination_id,
            &content_digest_uri,
            &manifest_digest_uri,
        );
        let second_destination_id = workbench("destination-two");
        let second_destination_commit_identity = workbench_commit_identity(
            &second_destination_id,
            &content_digest_uri,
            &manifest_digest_uri,
        );

        let first = build_restored_run_manifest_v1(
            &source,
            &first_destination_id,
            "/agents/test/wb/destination-one",
            &content_digest_uri,
            first_destination_commit_identity,
            2,
        )
        .unwrap();
        let replay = build_restored_run_manifest_v1(
            &source,
            &first_destination_id,
            "/agents/test/wb/destination-one",
            &content_digest_uri,
            first_destination_commit_identity,
            2,
        )
        .unwrap();
        let second = build_restored_run_manifest_v1(
            &source,
            &second_destination_id,
            "/agents/test/wb/destination-two",
            &content_digest_uri,
            second_destination_commit_identity,
            2,
        )
        .unwrap();

        assert_eq!(first, replay);
        assert_ne!(first, second);
        assert_ne!(
            first_destination_commit_identity,
            second_destination_commit_identity
        );
    }

    #[test]
    fn restored_run_manifest_rejects_untrusted_source_or_destination_inputs() {
        let source_id = workbench("source");
        let destination_id = workbench("destination");
        let source_manifest = br#"{"task":"ptycho"}"#;
        let manifest_digest_uri = digest_uri(source_manifest);
        let content_digest_uri = format!("sha256:{}", "66".repeat(32));
        let source_commit_identity =
            workbench_commit_identity(&source_id, &content_digest_uri, &manifest_digest_uri);
        let source = build_run_manifest_v1(
            &source_id,
            "/agents/test/wb/source",
            &content_digest_uri,
            source_manifest,
            &manifest_digest_uri,
            source_commit_identity,
            1,
        )
        .unwrap();
        let destination_commit_identity =
            workbench_commit_identity(&destination_id, &content_digest_uri, &manifest_digest_uri);

        assert!(build_restored_run_manifest_v1(
            &source,
            &destination_id,
            "/agents/test/wb/destination",
            &content_digest_uri,
            [9; 32],
            2,
        )
        .is_err());
        assert!(build_restored_run_manifest_v1(
            &source,
            &source_id,
            "/agents/test/wb/source",
            &content_digest_uri,
            source_commit_identity,
            2,
        )
        .is_err());
        assert!(build_restored_run_manifest_v1(
            &source,
            &destination_id,
            "/agents/test/wb/destination",
            &content_digest_uri,
            destination_commit_identity,
            0,
        )
        .is_err());
        assert!(build_restored_run_manifest_v1(
            br#"{ "not": "canonical" }"#,
            &destination_id,
            "/agents/test/wb/destination",
            &content_digest_uri,
            destination_commit_identity,
            2,
        )
        .is_err());
        assert!(build_restored_run_manifest_v1(
            br#"{"not":"a run manifest"}"#,
            &destination_id,
            "/agents/test/wb/destination",
            &content_digest_uri,
            destination_commit_identity,
            2,
        )
        .is_err());
    }

    #[test]
    fn restore_content_commitment_preserves_clean_and_separates_dirty_snapshots() {
        let source = format!("sha256:{}", "77".repeat(32));
        assert_eq!(
            restore_effective_content_digest_uri_v1(&source, true, [1; 32]).unwrap(),
            source
        );

        let first = restore_effective_content_digest_uri_v1(&source, false, [1; 32]).unwrap();
        let replay = restore_effective_content_digest_uri_v1(&source, false, [1; 32]).unwrap();
        let second = restore_effective_content_digest_uri_v1(&source, false, [2; 32]).unwrap();
        assert_eq!(first, replay);
        assert_ne!(first, source);
        assert_ne!(first, second);
        assert!(first.starts_with("sha256:"));
        assert_eq!(first.len(), 71);
        assert!(restore_effective_content_digest_uri_v1("not-a-digest", true, [1; 32]).is_err());
    }

    #[test]
    fn restore_manifest_v2_records_a_snapshot_source() {
        let body = build_restore_manifest_v2(
            [0x11; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            RestoreManifestSource::Snapshot { snapshot_id: 7 },
        )
        .unwrap();
        let verified = verify_restore_manifest(&body).unwrap();
        assert_eq!(verified.operation_id, [0x11; 16]);
        assert_eq!(
            verified.source,
            RestoreManifestSource::Snapshot { snapshot_id: 7 }
        );
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains(RESTORE_MANIFEST_V2_SCHEMA));
    }

    #[test]
    fn restore_manifest_v2_records_a_commit_source() {
        // A commit outlives every snapshot lease, so a citable decision point
        // has to be restorable from one.
        let body = build_restore_manifest_v2(
            [0x22; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            RestoreManifestSource::Commit {
                commit_id: [0xab; 32],
            },
        )
        .unwrap();
        let verified = verify_restore_manifest(&body).unwrap();
        assert_eq!(
            verified.source,
            RestoreManifestSource::Commit {
                commit_id: [0xab; 32]
            }
        );
        let text = String::from_utf8(body).unwrap();
        assert!(text.contains("commit_id"));
        assert!(!text.contains("snapshot_id"));
    }

    #[test]
    fn restore_manifest_v2_distinguishes_the_two_sources() {
        let snapshot = build_restore_manifest_v2(
            [0x33; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            RestoreManifestSource::Snapshot { snapshot_id: 9 },
        )
        .unwrap();
        let commit = build_restore_manifest_v2(
            [0x33; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            RestoreManifestSource::Commit {
                commit_id: [0x09; 32],
            },
        )
        .unwrap();
        assert_ne!(snapshot, commit, "the source must be part of the envelope");
    }

    #[test]
    fn restore_manifest_reader_still_accepts_v1_envelopes() {
        // v1 manifests are durable artifacts in already-restored workbenches;
        // the reader has to keep understanding them.
        let body = build_restore_manifest_v1(
            [0x44; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            13,
        )
        .unwrap();
        let verified = verify_restore_manifest(&body).unwrap();
        assert_eq!(
            verified.source,
            RestoreManifestSource::Snapshot { snapshot_id: 13 }
        );
    }

    #[test]
    fn restore_manifest_v2_refuses_a_zero_snapshot_and_a_zero_commit() {
        assert!(build_restore_manifest_v2(
            [0x55; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            RestoreManifestSource::Snapshot { snapshot_id: 0 },
        )
        .is_err());
        assert!(build_restore_manifest_v2(
            [0x55; 16],
            &workbench("source"),
            "/agents/test/wb/source",
            &workbench("destination"),
            "/agents/test/wb/destination",
            RestoreManifestSource::Commit {
                commit_id: [0x00; 32]
            },
        )
        .is_err());
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
