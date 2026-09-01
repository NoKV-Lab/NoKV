/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::Path;
use std::process::Command;

use nokv_protocol::RootIdentity;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use super::inspection::{inspect_store, StoreInspection};
use crate::fdb_live_runtime::{
    append_client_arguments, run_checked_command, ObjectProviderOptions,
};
#[cfg(test)]
use crate::qualification_runtime::lowercase_hex;
use crate::qualification_runtime::{sha256_bytes, sha256_file, EvidenceBundle};

const WORKBENCH_ROOT: &str = "/agents/fdb-gate9/wb";
const WORKBENCH_ID: &str = "fdb-gate9-large-object";
const ARTIFACT_PATH: &str = "large-object.bin";

#[derive(Clone, Debug, Serialize)]
pub(crate) struct ObjectSeparationReport {
    schema: &'static str,
    payload_bytes: usize,
    logical_transaction_limit_bytes: usize,
    input_sha256: String,
    collect_digest_uri: String,
    materialized_sha256: String,
    marker_sha256: Vec<String>,
    store_inspection: StoreInspection,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    evidence: &EvidenceBundle,
    candidate: &Path,
    cluster_file: &Path,
    prefix: &str,
    root: RootIdentity,
    seed: std::net::SocketAddr,
    objects: &ObjectProviderOptions,
    run_id: &str,
    payload_bytes: usize,
    logical_transaction_limit_bytes: usize,
) -> Result<ObjectSeparationReport, String> {
    if payload_bytes <= logical_transaction_limit_bytes {
        return Err("Gate 9 object payload must exceed the logical transaction limit".to_owned());
    }
    let markers = payload_markers(run_id);
    let payload = payload(payload_bytes, &markers)?;
    let input_relative = Path::new("objects/large-input.bin");
    let output_relative = Path::new("objects/large-materialized.bin");
    evidence.write_bytes(input_relative, &payload)?;
    let input = evidence.root().join(input_relative);
    let output = evidence.root().join(output_relative);
    let input_sha256 = sha256_bytes(&payload);

    let mut create = Command::new(candidate);
    append_client_arguments(&mut create, root, seed, WORKBENCH_ROOT, objects);
    create.args([
        "workbench",
        "workbench_create",
        &json!({"id": WORKBENCH_ID}).to_string(),
    ]);
    let created = run_checked_command(evidence, "large-object-create", &mut create)?;
    let created: Value = serde_json::from_str(&created)
        .map_err(|error| format!("large-object create output is not JSON: {error}"))?;
    if created.get("status").and_then(Value::as_str) != Some("success") {
        return Err("large-object workbench creation did not succeed".to_owned());
    }

    let mut collect = Command::new(candidate);
    append_client_arguments(&mut collect, root, seed, WORKBENCH_ROOT, objects);
    collect
        .arg("collect")
        .args([WORKBENCH_ID, "outputs"])
        .arg(&input)
        .args([ARTIFACT_PATH, "--content-type", "application/octet-stream"]);
    let collected = run_checked_command(evidence, "large-object-collect", &mut collect)?;
    let collected: Value = serde_json::from_str(&collected)
        .map_err(|error| format!("large-object collect output is not JSON: {error}"))?;
    let collect_digest_uri = collected
        .get("digest_uri")
        .and_then(Value::as_str)
        .ok_or_else(|| "large-object collect omitted digest_uri".to_owned())?
        .to_owned();
    if collected.get("status").and_then(Value::as_str) != Some("success")
        || collected.get("size_bytes").and_then(Value::as_u64) != Some(payload_bytes as u64)
        || collect_digest_uri != format!("sha256:{input_sha256}")
    {
        return Err("large-object collect did not preserve size and digest".to_owned());
    }

    let mut materialize = Command::new(candidate);
    append_client_arguments(&mut materialize, root, seed, WORKBENCH_ROOT, objects);
    materialize
        .arg("materialize")
        .args([WORKBENCH_ID, "outputs", ARTIFACT_PATH])
        .arg(&output);
    let materialized = run_checked_command(evidence, "large-object-materialize", &mut materialize)?;
    let materialized: Value = serde_json::from_str(&materialized)
        .map_err(|error| format!("large-object materialize output is not JSON: {error}"))?;
    let materialized_sha256 = sha256_file(&output)?;
    if materialized.get("status").and_then(Value::as_str) != Some("success")
        || materialized.get("size_bytes").and_then(Value::as_u64) != Some(payload_bytes as u64)
        || materialized.get("digest_uri").and_then(Value::as_str)
            != Some(collect_digest_uri.as_str())
        || materialized_sha256 != input_sha256
    {
        return Err("large-object materialization did not round-trip size and digest".to_owned());
    }

    let store_inspection = inspect_store(cluster_file, prefix, payload_bytes, &markers)?;
    Ok(ObjectSeparationReport {
        schema: "nokv.fdb.limits-qualification.object-separation.v1",
        payload_bytes,
        logical_transaction_limit_bytes,
        input_sha256,
        collect_digest_uri,
        materialized_sha256,
        marker_sha256: markers.iter().map(|marker| sha256_bytes(marker)).collect(),
        store_inspection,
    })
}

fn payload_markers(run_id: &str) -> Vec<Vec<u8>> {
    ["start", "middle", "end"]
        .into_iter()
        .map(|label| {
            let mut digest = Sha256::new();
            digest.update(b"nokv/fdb-gate9/payload-marker/v1\0");
            digest.update(run_id.as_bytes());
            digest.update([0]);
            digest.update(label.as_bytes());
            let first = digest.finalize();
            let second = Sha256::digest(first);
            [first.as_slice(), second.as_slice()].concat()
        })
        .collect()
}

fn payload(bytes: usize, markers: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    if markers.len() != 3 || markers.iter().any(|marker| marker.len() != 64) || bytes < 192 {
        return Err("Gate 9 payload marker geometry is invalid".to_owned());
    }
    let mut payload = (0..bytes)
        .map(|index| (index as u8).wrapping_mul(131).wrapping_add(17))
        .collect::<Vec<_>>();
    let offsets = [0, bytes / 2 - 32, bytes - 64];
    for (offset, marker) in offsets.into_iter().zip(markers) {
        payload[offset..offset + marker.len()].copy_from_slice(marker);
    }
    Ok(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_contains_three_domain_separated_markers() {
        let markers = payload_markers("run");
        let payload = payload(512, &markers).unwrap();
        assert_eq!(&payload[..64], markers[0].as_slice());
        assert_eq!(&payload[224..288], markers[1].as_slice());
        assert_eq!(&payload[448..], markers[2].as_slice());
        assert_ne!(lowercase_hex(&markers[0]), lowercase_hex(&markers[1]));
    }
}
