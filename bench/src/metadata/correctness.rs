/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_protocol::{ArtifactRevisionIdentity, WorkspaceResult, WorkspaceRpcRequest};
use nokv_server::WorkspaceRequestExecutor;
use serde::Serialize;

use super::fixture::{fixed_id, is_direct_fixture_path, Harness, EMPTY_SHA256_URI};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(super) struct CorrectnessSnapshot {
    pub(super) shallow_exact: serde_json::Value,
    pub(super) deep_exact: serde_json::Value,
    pub(super) recursive_first: Vec<SemanticListEntry>,
    pub(super) direct_all_pages: Vec<SemanticListEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(super) struct SemanticListEntry {
    pub(super) kind: String,
    pub(super) workbench: String,
    pub(super) path: String,
    pub(super) metadata: Option<serde_json::Value>,
}

pub(super) struct SemanticPage {
    pub(super) entries: Vec<SemanticListEntry>,
    pub(super) next_cursor: Option<Vec<u8>>,
    pub(super) read_version: u64,
}

impl Harness {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn correctness_snapshot(
        &self,
        shallow_existing: &WorkspaceRpcRequest,
        deep_existing: &WorkspaceRpcRequest,
        shallow_missing: &WorkspaceRpcRequest,
        deep_missing: &WorkspaceRpcRequest,
        recursive_first: &WorkspaceRpcRequest,
        direct_all_pages: Vec<SemanticListEntry>,
        page_limit: u32,
    ) -> Result<CorrectnessSnapshot, String> {
        let shallow_exact = self.assert_exact_expected(shallow_existing, "outputs/hot")?;
        let deep_path = self
            .fixture_paths
            .last()
            .ok_or_else(|| "fixture unexpectedly contains no paths".to_owned())?;
        let deep_exact = self.assert_exact_expected(deep_existing, deep_path)?;
        self.assert_exact_missing(shallow_missing)?;
        self.assert_exact_missing(deep_missing)?;
        let recursive_first = self.execute_semantic_page(recursive_first)?.entries;
        let expected_recursive = usize::try_from(page_limit).expect("u32 fits usize");
        if recursive_first.len() != expected_recursive {
            return Err(format!(
                "recursive first page returned {} entries, expected {expected_recursive}",
                recursive_first.len()
            ));
        }
        let mut expected_recursive_paths = self.fixture_paths.iter().collect::<Vec<_>>();
        expected_recursive_paths.sort_by(|left, right| listing_path_order(left, right));
        for (entry, expected_path) in recursive_first.iter().zip(
            expected_recursive_paths
                .into_iter()
                .take(expected_recursive),
        ) {
            self.assert_expected_artifact(entry, expected_path)?;
        }
        self.assert_expected_direct_entries(&direct_all_pages)?;
        Ok(CorrectnessSnapshot {
            shallow_exact,
            deep_exact,
            recursive_first,
            direct_all_pages,
        })
    }

    pub(super) fn assert_exact_expected(
        &self,
        request: &WorkspaceRpcRequest,
        expected_path: &str,
    ) -> Result<serde_json::Value, String> {
        let outcome = self
            .executor
            .execute(request)
            .map_err(|failure| format!("existing exact get failed: {failure:?}"))?;
        let WorkspaceResult::Path(path) = outcome.result else {
            return Err("existing exact get returned the wrong result variant".to_owned());
        };
        let metadata = path
            .metadata
            .ok_or_else(|| "existing exact get returned no metadata".to_owned())?;
        let entry = semantic_artifact(&metadata)?;
        self.assert_expected_artifact(&entry, expected_path)?;
        serde_json::to_value(metadata).map_err(|error| error.to_string())
    }

    pub(super) fn assert_exact_missing(&self, request: &WorkspaceRpcRequest) -> Result<(), String> {
        self.exact_missing_checksum(request).map(|_| ())
    }

    pub(super) fn assert_expected_direct_entries(
        &self,
        entries: &[SemanticListEntry],
    ) -> Result<(), String> {
        let expected = self
            .fixture_paths
            .iter()
            .filter(|path| path.as_str() != "outputs/hot" && is_direct_fixture_path(path))
            .chain(self.fixture_paths.first())
            .collect::<Vec<_>>();
        if entries.len() != expected.len() {
            return Err(format!(
                "direct listing returned {} entries, expected {}",
                entries.len(),
                expected.len()
            ));
        }
        for (entry, expected_path) in entries.iter().zip(expected) {
            self.assert_expected_artifact(entry, expected_path)?;
        }
        Ok(())
    }

    pub(super) fn assert_expected_artifact(
        &self,
        entry: &SemanticListEntry,
        expected_path: &str,
    ) -> Result<(), String> {
        if entry.kind != "artifact"
            || entry.workbench != self.workbench.as_str()
            || entry.path != expected_path
        {
            return Err(format!(
                "unexpected list entry kind/workbench/path: {entry:?}, expected artifact {}/{expected_path}",
                self.workbench.as_str()
            ));
        }
        let metadata = entry
            .metadata
            .as_ref()
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| format!("artifact {expected_path:?} has no metadata object"))?;
        let fixture_index = self
            .fixture_paths
            .iter()
            .position(|path| path == expected_path)
            .map(|index| index + 1)
            .ok_or_else(|| format!("path {expected_path:?} is not in the fixture"))?;
        let fixture_generation = u64::try_from(fixture_index)
            .map_err(|_| "fixture generation does not fit u64".to_owned())?;
        let expected_revision = serde_json::to_value(ArtifactRevisionIdentity(fixed_id(
            self.seed,
            fixture_generation,
        )))
        .map_err(|error| error.to_string())?;
        let expected_incarnation =
            serde_json::to_value(fixed_id(self.seed, 3)).map_err(|error| error.to_string())?;
        if metadata
            .get("generation")
            .and_then(serde_json::Value::as_u64)
            != Some(fixture_generation)
            || metadata
                .get("workspace_revision")
                .and_then(serde_json::Value::as_u64)
                != Some(self.workspace_revision)
            || metadata.get("artifact_revision_id") != Some(&expected_revision)
            || metadata.get("workspace_incarnation_id") != Some(&expected_incarnation)
        {
            return Err(format!(
                "artifact {expected_path:?} returned unexpected generation, workspace revision, or identity"
            ));
        }
        let descriptor = metadata
            .get("descriptor")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| format!("artifact {expected_path:?} has no descriptor"))?;
        if descriptor
            .get("logical_size")
            .and_then(serde_json::Value::as_u64)
            != Some(0)
            || descriptor
                .get("body_digest")
                .and_then(serde_json::Value::as_str)
                != Some(EMPTY_SHA256_URI)
            || descriptor
                .get("manifest_digest")
                .and_then(serde_json::Value::as_str)
                != Some(EMPTY_SHA256_URI)
        {
            return Err(format!(
                "artifact {expected_path:?} returned unexpected descriptor contents"
            ));
        }
        Ok(())
    }
}

fn listing_path_order(left: &str, right: &str) -> std::cmp::Ordering {
    if left
        .strip_prefix(right)
        .is_some_and(|suffix| suffix.starts_with('/'))
    {
        std::cmp::Ordering::Less
    } else if right
        .strip_prefix(left)
        .is_some_and(|suffix| suffix.starts_with('/'))
    {
        std::cmp::Ordering::Greater
    } else {
        left.as_bytes().cmp(right.as_bytes())
    }
}

fn semantic_artifact(value: &impl Serialize) -> Result<SemanticListEntry, String> {
    normalize_list_entry(serde_json::to_value(value).map_err(|error| error.to_string())?)
}

pub(super) fn normalize_list_entries<T: Serialize>(
    values: &[T],
) -> Result<Vec<SemanticListEntry>, String> {
    values
        .iter()
        .map(|value| {
            serde_json::to_value(value)
                .map_err(|error| error.to_string())
                .and_then(normalize_list_entry)
        })
        .collect()
}

fn normalize_list_entry(value: serde_json::Value) -> Result<SemanticListEntry, String> {
    let (kind, payload) = match value.get("kind").and_then(serde_json::Value::as_str) {
        Some(kind @ ("artifact" | "prefix")) => {
            let payload = value
                .get("value")
                .cloned()
                .ok_or_else(|| format!("{kind} list entry has no value"))?;
            (kind.to_owned(), payload)
        }
        Some(kind) => return Err(format!("unknown list entry kind {kind:?}")),
        None => ("artifact".to_owned(), value),
    };
    let path_value = if kind == "artifact" {
        payload
            .get("path")
            .ok_or_else(|| "artifact list entry has no path".to_owned())?
    } else {
        &payload
    };
    let workbench = path_value
        .get("workbench")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "list entry path has no workbench".to_owned())?
        .to_owned();
    let path = path_value
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "list entry has no relative path".to_owned())?
        .to_owned();
    Ok(SemanticListEntry {
        kind: kind.clone(),
        workbench,
        path,
        metadata: (kind == "artifact").then_some(payload),
    })
}

pub(super) fn semantic_digest(snapshot: &CorrectnessSnapshot) -> Result<u64, String> {
    let bytes = serde_json::to_vec(snapshot).map_err(|error| error.to_string())?;
    Ok(bytes
        .into_iter()
        .fold(0xcbf2_9ce4_8422_2325, |digest, byte| {
            (digest ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3)
        }))
}
