/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use super::correctness::{normalize_list_entries, SemanticListEntry, SemanticPage};
use super::fixture::{fixed_id, Harness};
use nokv_protocol::{
    ErrorCode, GetPathRequest, ListPathsRequest, PageRequest, RelativePath, RequestIdentity,
    WorkspacePath, WorkspaceReadView, WorkspaceRequest, WorkspaceResult, WorkspaceRpcRequest,
};
use nokv_server::WorkspaceRequestExecutor;

pub(super) struct PreparedPages {
    pub(super) first: WorkspaceRpcRequest,
    pub(super) middle: WorkspaceRpcRequest,
    pub(super) final_page: WorkspaceRpcRequest,
}

impl Harness {
    pub(super) fn get_request(
        &self,
        path: &str,
        sequence: u64,
    ) -> Result<WorkspaceRpcRequest, String> {
        Ok(WorkspaceRpcRequest {
            route: self.route,
            request_id: RequestIdentity(fixed_id(sequence, 0)),
            operation: WorkspaceRequest::GetPath(GetPathRequest {
                target: WorkspacePath {
                    workbench: self.workbench.clone(),
                    path: RelativePath::new(path).map_err(|error| error.to_string())?,
                },
                view: WorkspaceReadView::Live,
                range: None,
                plan_page: None,
                if_none_match: None,
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn list_request(
        &self,
        prefix: Option<&str>,
        recursive: bool,
        cursor: Option<Vec<u8>>,
        expected_read_version: Option<u64>,
        limit: u32,
        sequence: u64,
    ) -> Result<WorkspaceRpcRequest, String> {
        Ok(WorkspaceRpcRequest {
            route: self.route,
            request_id: RequestIdentity(fixed_id(sequence, 0)),
            operation: WorkspaceRequest::ListPaths(ListPathsRequest {
                workbench: self.workbench.clone(),
                prefix: prefix
                    .map(RelativePath::new)
                    .transpose()
                    .map_err(|error| error.to_string())?,
                recursive,
                view: WorkspaceReadView::Live,
                expected_read_version,
                page: PageRequest { cursor, limit },
            }),
        })
    }

    pub(super) fn prepare_direct_pages(
        &self,
        limit: u32,
    ) -> Result<(PreparedPages, Vec<SemanticListEntry>), String> {
        let mut requests = Vec::new();
        let mut logical_entries = Vec::new();
        let mut cursor = None;
        let mut expected_read_version = None;
        let mut total_entries = 0_usize;
        let mut sequence = 20_000_u64;
        loop {
            let request = self.list_request(
                Some("outputs/hot"),
                false,
                cursor,
                expected_read_version,
                limit,
                sequence,
            )?;
            sequence += 1;
            let page = self.execute_semantic_page(&request)?;
            if page.entries.is_empty() {
                return Err("direct listing produced an empty intermediate page".to_owned());
            }
            total_entries = total_entries.saturating_add(page.entries.len());
            logical_entries.extend(page.entries);
            requests.push(request);
            let Some(next_cursor) = page.next_cursor else {
                break;
            };
            cursor = Some(next_cursor);
            expected_read_version = Some(page.read_version);
        }
        let expected_entries = self.direct_children + 1;
        if total_entries != expected_entries {
            return Err(format!(
                "direct listing returned {total_entries} logical entries, expected {expected_entries}"
            ));
        }
        if requests.len() < 3 {
            return Err("direct listing must produce at least three cursor pages".to_owned());
        }
        self.assert_expected_direct_entries(&logical_entries)?;
        Ok((
            PreparedPages {
                first: requests[0].clone(),
                middle: requests[requests.len() / 2].clone(),
                final_page: requests
                    .last()
                    .expect("at least three pages were required")
                    .clone(),
            },
            logical_entries,
        ))
    }

    pub(super) fn exact_existing_checksum(
        &self,
        request: &WorkspaceRpcRequest,
    ) -> Result<u64, String> {
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
        let mut checksum = metadata.generation ^ metadata.descriptor.logical_size;
        for byte in metadata
            .path
            .path
            .as_str()
            .as_bytes()
            .iter()
            .chain(metadata.artifact_revision_id.0.iter())
            .chain(metadata.descriptor.body_digest.as_str().as_bytes())
            .chain(metadata.descriptor.manifest_digest.as_str().as_bytes())
        {
            checksum = checksum.wrapping_mul(0x100_0000_01b3) ^ u64::from(*byte);
        }
        Ok(checksum)
    }

    pub(super) fn exact_missing_checksum(
        &self,
        request: &WorkspaceRpcRequest,
    ) -> Result<u64, String> {
        match self.executor.execute(request) {
            Err(failure) if failure.code == ErrorCode::NotFound => Ok(0x4e4f_5446_4f55_4e44),
            Err(failure) => Err(format!("missing exact get returned {failure:?}")),
            Ok(_) => Err("missing exact get unexpectedly succeeded".to_owned()),
        }
    }

    pub(super) fn page_checksum(&self, request: &WorkspaceRpcRequest) -> Result<u64, String> {
        let (entries, cursor, read_version) = self.execute_page(request)?;
        let mut checksum = (entries as u64).rotate_left(11) ^ read_version;
        for byte in cursor.iter().flatten() {
            checksum = checksum.wrapping_mul(0x100_0000_01b3) ^ u64::from(*byte);
        }
        Ok(checksum)
    }

    pub(super) fn execute_page(
        &self,
        request: &WorkspaceRpcRequest,
    ) -> Result<(usize, Option<Vec<u8>>, u64), String> {
        let outcome = self
            .executor
            .execute(request)
            .map_err(|failure| format!("list request failed: {failure:?}"))?;
        let WorkspaceResult::Paths(page) = outcome.result else {
            return Err("list request returned the wrong result variant".to_owned());
        };
        Ok((page.entries.len(), page.next_cursor, page.read_version))
    }

    pub(super) fn execute_semantic_page(
        &self,
        request: &WorkspaceRpcRequest,
    ) -> Result<SemanticPage, String> {
        let outcome = self
            .executor
            .execute(request)
            .map_err(|failure| format!("list request failed: {failure:?}"))?;
        let WorkspaceResult::Paths(page) = outcome.result else {
            return Err("list request returned the wrong result variant".to_owned());
        };
        let entries = normalize_list_entries(&page.entries)?;
        Ok(SemanticPage {
            entries,
            next_cursor: page.next_cursor,
            read_version: page.read_version,
        })
    }
}
