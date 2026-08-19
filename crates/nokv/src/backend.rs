/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Production Workbench facade backed by the typed workspace SDK.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use nokv_agent as agent;
use nokv_client::{
    ArtifactAppendOptions, ArtifactPublishOptions, ArtifactReadAuthority, ClientError,
    SnapshotMintOptions, SnapshotRenewOptions, SnapshotRetireOptions, WorkbenchAdmission,
    WorkbenchCommitRequest, WorkbenchLifecycleError, WorkbenchLifecycleFacade,
    WorkbenchLifecycleOptions, WorkbenchRestoreOrigin, WorkbenchRestoreRequest,
    WorkbenchRestoreSource, WorkbenchSnapshotSelector,
};
use nokv_object::ArtifactObjectStore;
use nokv_protocol as wire;
use nokv_types::{
    ArtifactRevisionId, NormalizedRelativePath, RootId, WorkbenchId, WorkspaceIncarnationId,
};
use nokv_workbench_projection::CanonicalWorkbenchProjection;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use super::connection::CliWorkspaceClient;
use super::object_store::CliObjectStore;
const RUN_MANIFEST_PATH: &str = "metadata/run_manifest.json";
const JSON_CONTENT_TYPE: &str = "application/json";
const CURSOR_VERSION: &[u8] = b"nokv.workspace.cursor\0";
const GREP_CURSOR_VERSION: &[u8] = b"nokv.workspace.grep-cursor.v3\0";
const LIST_CURSOR_VERSION: &[u8] = b"nokv.workspace.list-cursor.v4\0";
const GREP_CURSOR_MAX_ENCODED_BYTES: usize = base64_encoded_upper_bound(
    GREP_CURSOR_VERSION.len() + 16 + 8 + 32 + NormalizedRelativePath::MAX_BYTES,
);
const LIST_CURSOR_MAX_ENCODED_BYTES: usize = base64_encoded_upper_bound(
    LIST_CURSOR_VERSION.len() + 1 + 24 + 32 + NormalizedRelativePath::MAX_BYTES,
);
const QUERY_SERVER_PAGE_LIMIT: u32 = wire::MAX_QUERY_PAGE_LIMIT;
const CONSISTENT_READ_MAX_ATTEMPTS: u32 = 3;
const WORKBENCH_ENTRY_COUNT_PAGE_LIMIT: u32 = wire::PageRequest::MAX_LIMIT;
const WORKBENCH_ENTRY_COUNT_MAX_PAGES: usize = 4_096;
const WORKBENCH_ENTRY_COUNT_MAX_ROWS: usize = 1_000_000;

static ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

const fn base64_encoded_upper_bound(raw_bytes: usize) -> usize {
    raw_bytes.saturating_add(2) / 3 * 4
}

/// Concrete Workbench backend used by the custom CLI and MCP adapter.
#[derive(Clone)]
pub struct CliWorkbenchBackend {
    client: CliWorkspaceClient,
    objects: Arc<dyn ArtifactObjectStore>,
    max_artifact_bytes: usize,
}

#[derive(Default)]
struct WorkbenchEntryCountBudget {
    pages: usize,
    rows: usize,
}

impl WorkbenchEntryCountBudget {
    fn reserve_page(&mut self) -> Result<(), agent::BackendError> {
        if self.pages >= WORKBENCH_ENTRY_COUNT_MAX_PAGES {
            return Err(resource_exhausted(format!(
                "workbench child enumeration exceeds {WORKBENCH_ENTRY_COUNT_MAX_PAGES} pages"
            )));
        }
        self.pages += 1;
        Ok(())
    }

    fn consume_rows(&mut self, rows: usize) -> Result<(), agent::BackendError> {
        self.rows = self.rows.checked_add(rows).ok_or_else(|| {
            resource_exhausted("workbench child enumeration row count overflowed")
        })?;
        if self.rows > WORKBENCH_ENTRY_COUNT_MAX_ROWS {
            return Err(resource_exhausted(format!(
                "workbench child enumeration exceeds {WORKBENCH_ENTRY_COUNT_MAX_ROWS} rows"
            )));
        }
        Ok(())
    }
}

impl CliWorkbenchBackend {
    pub fn new(
        client: CliWorkspaceClient,
        objects: Arc<CliObjectStore>,
        max_artifact_bytes: usize,
    ) -> Self {
        Self {
            client,
            objects,
            max_artifact_bytes,
        }
    }

    fn workspace(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<wire::WorkspaceSummary, agent::BackendError> {
        self.client
            .get_workspace(wire::GetWorkspaceRequest {
                workbench: workbench_name(workbench_id)?,
            })
            .map(|call| call.value)
            .map_err(map_client_error)
    }

    fn optional_workspace(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Option<wire::WorkspaceSummary>, agent::BackendError> {
        match self.client.get_workspace(wire::GetWorkspaceRequest {
            workbench: workbench_name(workbench_id)?,
        }) {
            Ok(call) => Ok(Some(call.value)),
            Err(error) if rpc_code(&error) == Some(wire::ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(map_client_error(error)),
        }
    }

    fn create_or_observe_workbench(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<WorkbenchAdmission, agent::BackendError> {
        self.lifecycle_facade()?
            .admit_workbench(workbench_id)
            .map_err(map_lifecycle_error)
    }

    fn lifecycle_facade(
        &self,
    ) -> Result<
        WorkbenchLifecycleFacade<
            '_,
            nokv_client::FramedTcpTransport,
            Arc<dyn nokv_client::RouteResolver>,
            CanonicalWorkbenchProjection,
        >,
        agent::BackendError,
    > {
        let options =
            WorkbenchLifecycleOptions::new(self.max_artifact_bytes).map_err(map_client_error)?;
        Ok(WorkbenchLifecycleFacade::new(
            &self.client,
            self.objects.as_ref(),
            options,
            CanonicalWorkbenchProjection,
        ))
    }

    fn resolved_view(
        &self,
        _workbench_id: &WorkbenchId,
        view: &agent::ReadView,
    ) -> Result<wire::WorkspaceReadView, agent::BackendError> {
        match view {
            agent::ReadView::Live => Ok(wire::WorkspaceReadView::Live),
            agent::ReadView::Snapshot(selector) => Ok(wire::WorkspaceReadView::Snapshot(
                snapshot_selector(selector)?,
            )),
        }
    }

    fn path_metadata(
        &self,
        path: &agent::ScopedPath,
        view: wire::WorkspaceReadView,
    ) -> Result<Option<wire::PathMetadata>, agent::BackendError> {
        self.path_metadata_at_read_version(path, view, None)
    }

    fn path_metadata_at_read_version(
        &self,
        path: &agent::ScopedPath,
        view: wire::WorkspaceReadView,
        expected_read_version: Option<u64>,
    ) -> Result<Option<wire::PathMetadata>, agent::BackendError> {
        let target = workspace_path(path)?;
        match self.client.get_path(wire::GetPathRequest {
            target: target.clone(),
            view,
            expected_read_version,
            range: None,
            plan_page: None,
            if_none_match: None,
        }) {
            Ok(call) => {
                let metadata = call
                    .value
                    .metadata
                    .ok_or_else(|| protocol_mismatch("path stat omitted metadata"))?;
                if metadata.path != target {
                    return Err(protocol_mismatch(
                        "get_path returned metadata for a different path",
                    ));
                }
                Ok(Some(metadata))
            }
            Err(error) if rpc_code(&error) == Some(wire::ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(map_read_version_fenced_error(error)),
        }
    }

    fn workbench_entry_count_at_read_version(
        &self,
        workbench_id: &WorkbenchId,
        read_version: u64,
        budget: &mut WorkbenchEntryCountBudget,
    ) -> Result<usize, agent::BackendError> {
        let mut direct_children = agent::WORKBENCH_SECTIONS
            .into_iter()
            .map(|section| {
                NormalizedRelativePath::new(section.as_str().to_owned()).map_err(domain_input)
            })
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        loop {
            budget.reserve_page()?;
            let requested_cursor = cursor.clone();
            let call = self
                .client
                .list_paths(wire::ListPathsRequest {
                    workbench: workbench_name(workbench_id)?,
                    prefix: None,
                    recursive: false,
                    view: wire::WorkspaceReadView::Live,
                    expected_read_version: Some(read_version),
                    workspace_continuation_fence: None,
                    page: wire::PageRequest {
                        cursor: requested_cursor,
                        limit: WORKBENCH_ENTRY_COUNT_PAGE_LIMIT,
                    },
                })
                .map_err(|error| {
                    map_read_enumeration_error(error, "workbench child enumeration")
                })?;
            if call.value.read_version != read_version {
                return Err(protocol_mismatch(
                    "workbench child enumeration returned a page outside its read-version fence",
                ));
            }
            if call.value.entries.len() > WORKBENCH_ENTRY_COUNT_PAGE_LIMIT as usize {
                return Err(protocol_mismatch(
                    "workbench child enumeration exceeded its requested page limit",
                ));
            }
            budget.consume_rows(call.value.entries.len())?;
            if call.value.next_cursor.is_some() && call.value.entries.is_empty() {
                return Err(protocol_mismatch(
                    "workbench child enumeration returned an empty non-terminal page",
                ));
            }
            for entry in call.value.entries {
                validate_response_workbench(entry.path(), workbench_id)?;
                let source = NormalizedRelativePath::new(entry.path().path.as_str().to_owned())
                    .map_err(domain_input)?;
                let child = direct_list_child(None, source)?.ok_or_else(|| {
                    protocol_mismatch(
                        "root workbench child enumeration returned an empty direct child",
                    )
                })?;
                direct_children.insert(child);
            }
            let Some(next_cursor) = call.value.next_cursor else {
                return Ok(direct_children.len());
            };
            if !seen_cursors.insert(next_cursor.clone()) {
                return Err(protocol_mismatch(
                    "workbench child enumeration returned a repeated cursor",
                ));
            }
            cursor = Some(next_cursor);
        }
    }

    fn publish_artifact(
        &self,
        options: ArtifactPublishOptions,
        bytes: &[u8],
    ) -> Result<nokv_client::ArtifactPublishOutcome, agent::BackendError> {
        self.ensure_artifact_size(bytes)?;
        self.client
            .publish_artifact(self.objects.as_ref(), options, bytes)
            .map_err(map_client_error)
    }

    fn ensure_artifact_size(&self, bytes: &[u8]) -> Result<(), agent::BackendError> {
        if bytes.len() > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "artifact is {} bytes, maximum is {}",
                bytes.len(),
                self.max_artifact_bytes
            )));
        }
        Ok(())
    }

    fn fresh_fixed_identity(&self, domain: &[u8], pieces: &[&[u8]]) -> [u8; 16] {
        let sequence = ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut hasher = Sha256::new();
        hasher.update(domain);
        hasher.update(self.client.root_id().0);
        hasher.update(std::process::id().to_be_bytes());
        hasher.update(sequence.to_be_bytes());
        hasher.update(now.to_be_bytes());
        for piece in pieces {
            hash_len64(&mut hasher, piece);
        }
        digest_prefix(hasher.finalize().into())
    }
}

impl agent::WorkbenchBackend for CliWorkbenchBackend {
    fn storage_root_id(&self) -> RootId {
        RootId::from(self.client.root_id())
    }

    fn create_workbench(&self, workbench_id: &WorkbenchId) -> Result<bool, agent::BackendError> {
        self.create_or_observe_workbench(workbench_id)
            .map(|admission| admission.created)
    }

    fn stat(
        &self,
        path: &agent::ScopedPath,
        view: &agent::ReadView,
    ) -> Result<Option<agent::StatRecord>, agent::BackendError> {
        if path.section.is_none() && path.relative_path.is_none() {
            return Ok(self
                .optional_workspace(&path.workbench_id)?
                .map(|_| agent::StatRecord {
                    path: path.clone(),
                    kind: agent::ArtifactKind::Workbench,
                    artifact: None,
                    authority: None,
                }));
        }
        let resolved_view = self.resolved_view(&path.workbench_id, view)?;
        if let Some(metadata) = self.path_metadata(path, resolved_view.clone())? {
            return Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Artifact,
                artifact: Some(artifact_metadata(&metadata)),
                authority: Some(grep_candidate_authority(&metadata)),
            }));
        }
        if path.section.is_some() && path.relative_path.is_none() {
            self.workspace(&path.workbench_id)?;
            return Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Section,
                artifact: None,
                authority: None,
            }));
        }
        let prefix_path = full_path(path)?;
        let prefix = relative_path(&prefix_path)?;
        let page = self
            .client
            .list_paths(wire::ListPathsRequest {
                workbench: workbench_name(&path.workbench_id)?,
                prefix: Some(prefix),
                recursive: false,
                view: resolved_view,
                expected_read_version: None,
                workspace_continuation_fence: None,
                page: wire::PageRequest {
                    cursor: None,
                    limit: 1,
                },
            })
            .map_err(map_client_error)?;
        let mut entries = page.value.entries.into_iter();
        let Some(entry) = entries.next() else {
            return Ok(None);
        };
        if entries.next().is_some() {
            return Err(protocol_mismatch(
                "list_paths returned more rows than the requested stat probe limit",
            ));
        }
        validate_response_workbench(entry.path(), &path.workbench_id)?;
        let source = NormalizedRelativePath::new(entry.path().path.as_str().to_owned())
            .map_err(domain_input)?;
        match (direct_list_child(Some(&prefix_path), source)?, entry) {
            (Some(_), _) => Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Directory,
                artifact: None,
                authority: None,
            })),
            (None, wire::PathListEntry::Artifact(metadata)) => Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Artifact,
                artifact: Some(artifact_metadata(&metadata)),
                authority: Some(grep_candidate_authority(&metadata)),
            })),
            (None, wire::PathListEntry::Prefix(_)) => Err(protocol_mismatch(
                "list_paths returned an exact implicit prefix",
            )),
        }
    }

    fn stat_at_read_version(
        &self,
        path: &agent::ScopedPath,
        read_version: u64,
    ) -> Result<Option<agent::StatRecord>, agent::BackendError> {
        if read_version == 0 {
            return Err(invalid_backend_input(
                "stat read version must be greater than zero",
            ));
        }
        if path.section.is_some() || path.relative_path.is_some() {
            if let Some(metadata) = self.path_metadata_at_read_version(
                path,
                wire::WorkspaceReadView::Live,
                Some(read_version),
            )? {
                return Ok(Some(agent::StatRecord {
                    path: path.clone(),
                    kind: agent::ArtifactKind::Artifact,
                    artifact: Some(artifact_metadata(&metadata)),
                    authority: Some(grep_candidate_authority(&metadata)),
                }));
            }
        }

        let prefix = full_path_optional(path)?;
        let call = match self.client.list_paths(wire::ListPathsRequest {
            workbench: workbench_name(&path.workbench_id)?,
            prefix: prefix.as_ref().map(relative_path).transpose()?,
            recursive: false,
            view: wire::WorkspaceReadView::Live,
            expected_read_version: Some(read_version),
            workspace_continuation_fence: None,
            page: wire::PageRequest {
                cursor: None,
                limit: 1,
            },
        }) {
            Ok(call) => call,
            Err(error) if rpc_code(&error) == Some(wire::ErrorCode::NotFound) => return Ok(None),
            Err(error) => return Err(map_read_version_fenced_error(error)),
        };
        if call.value.read_version != read_version {
            return Err(protocol_mismatch(
                "list_paths returned a stat probe outside its expected read version",
            ));
        }
        if path.relative_path.is_none() {
            return Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: if path.section.is_some() {
                    agent::ArtifactKind::Section
                } else {
                    agent::ArtifactKind::Workbench
                },
                artifact: None,
                authority: None,
            }));
        }
        let mut entries = call.value.entries.into_iter();
        let Some(entry) = entries.next() else {
            return Ok(None);
        };
        if entries.next().is_some() {
            return Err(protocol_mismatch(
                "list_paths returned more rows than the stat probe limit",
            ));
        }
        validate_response_workbench(entry.path(), &path.workbench_id)?;
        let source = NormalizedRelativePath::new(entry.path().path.as_str().to_owned())
            .map_err(domain_input)?;
        let prefix = prefix.expect("nested stat has a normalized path prefix");
        match direct_list_child(Some(&prefix), source)? {
            Some(_) => Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Directory,
                artifact: None,
                authority: None,
            })),
            None => Err(protocol_mismatch(
                "get_path missed an exact artifact returned by list_paths at the same version",
            )),
        }
    }

    fn list(&self, request: agent::ListRequest) -> Result<agent::ListPage, agent::BackendError> {
        let limit = request
            .limit
            .max(1)
            .min(wire::PageRequest::MAX_LIMIT as usize);
        let view = self.resolved_view(&request.path.workbench_id, &request.view)?;
        let prefix = full_path_optional(&request.path)?;
        let scope_digest = list_scope_digest(
            self.client.root_id(),
            &request.path.workbench_id,
            prefix.as_ref(),
            &request.view,
        );
        let supplied_cursor = request.cursor.is_some();
        let decoded_cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_list_cursor(cursor, scope_digest))
            .transpose()?;
        if let Some(cursor) = &decoded_cursor {
            let valid_for_view = matches!(
                (&request.view, &cursor.fence),
                (agent::ReadView::Live, ListContinuationFence::Workspace(_))
                    | (
                        agent::ReadView::Snapshot(_),
                        ListContinuationFence::ReadVersion(_)
                    )
            );
            if !valid_for_view {
                return Err(invalid_backend_input(
                    "list cursor fence does not match the requested read view",
                ));
            }
        }
        let after = decoded_cursor.as_ref().map(|cursor| &cursor.anchor);
        let max_attempts = if supplied_cursor {
            1
        } else {
            CONSISTENT_READ_MAX_ATTEMPTS
        };

        'attempt: for attempt in 1..=max_attempts {
            let mut continuation_fence = match decoded_cursor.as_ref() {
                Some(cursor) => Some(cursor.fence.clone()),
                None if matches!(&request.view, agent::ReadView::Live) => {
                    let workspace = self.workspace(&request.path.workbench_id)?;
                    if workspace.workbench.as_str() != request.path.workbench_id.as_str() {
                        return Err(protocol_mismatch(
                            "get_workspace returned a different workbench for list pagination",
                        ));
                    }
                    Some(ListContinuationFence::Workspace(
                        wire::WorkspaceContinuationFence {
                            workspace_incarnation_id: workspace.workspace_incarnation_id,
                            workspace_revision: workspace.workspace_revision,
                        },
                    ))
                }
                None => None,
            };
            let mut candidates = BTreeMap::<NormalizedRelativePath, agent::ListEntry>::new();
            if prefix.is_none() {
                for section in agent::WORKBENCH_SECTIONS {
                    let path = NormalizedRelativePath::new(section.as_str().to_owned())
                        .map_err(domain_input)?;
                    if after.is_none_or(|after| path > *after) {
                        candidates.insert(
                            path,
                            agent::ListEntry {
                                path: agent::ScopedPath {
                                    workbench_id: request.path.workbench_id.clone(),
                                    section: Some(section),
                                    relative_path: None,
                                },
                                kind: agent::ArtifactKind::Section,
                                artifact: None,
                            },
                        );
                    }
                }
            }

            let mut scan_cursor = after.map(|path| path.as_str().as_bytes().to_vec());
            let mut seen_server_cursors = BTreeSet::new();
            let mut first_server_page = true;
            if let Some(cursor) = scan_cursor.clone() {
                seen_server_cursors.insert(cursor);
            }
            let read_version = loop {
                let requested_cursor = scan_cursor.clone();
                let requested_limit =
                    list_server_page_limit(limit, candidates.len(), first_server_page);
                let expected_read_version = match continuation_fence.as_ref() {
                    Some(ListContinuationFence::ReadVersion(read_version)) => Some(*read_version),
                    _ => None,
                };
                let workspace_continuation_fence = match continuation_fence.as_ref() {
                    Some(ListContinuationFence::Workspace(fence)) => Some(fence.clone()),
                    _ => None,
                };
                let call = match self.client.list_paths(wire::ListPathsRequest {
                    workbench: workbench_name(&request.path.workbench_id)?,
                    prefix: prefix.as_ref().map(relative_path).transpose()?,
                    recursive: false,
                    view: view.clone(),
                    expected_read_version,
                    workspace_continuation_fence,
                    page: wire::PageRequest {
                        cursor: requested_cursor.clone(),
                        limit: requested_limit,
                    },
                }) {
                    Ok(call) => call,
                    Err(error)
                        if !supplied_cursor
                            && attempt < max_attempts
                            && is_continuation_fence_conflict(&error) =>
                    {
                        continue 'attempt;
                    }
                    Err(error) => {
                        return Err(map_read_enumeration_error(error, "list"));
                    }
                };
                first_server_page = false;
                let page_read_version = call.value.read_version;
                if expected_read_version.is_some_and(|expected| expected != page_read_version) {
                    return Err(protocol_mismatch(
                        "list_paths returned a page outside the requested read-version fence",
                    ));
                }
                if continuation_fence.is_none() {
                    continuation_fence =
                        Some(ListContinuationFence::ReadVersion(page_read_version));
                }
                if call.value.entries.len()
                    > usize::try_from(requested_limit)
                        .expect("protocol page limit always fits usize")
                {
                    return Err(protocol_mismatch(
                        "list_paths returned more rows than the requested page limit",
                    ));
                }
                let next_server_cursor = call.value.next_cursor;
                if next_server_cursor.is_some() && call.value.entries.is_empty() {
                    return Err(protocol_mismatch(
                        "list_paths returned an empty non-terminal page",
                    ));
                }
                if let Some(next) = next_server_cursor.as_ref() {
                    if !seen_server_cursors.insert(next.clone()) {
                        return Err(protocol_mismatch("list_paths returned a repeated cursor"));
                    }
                }
                let exhausted = next_server_cursor.is_none();
                let mut last_authoritative_path = None;
                for listing in call.value.entries {
                    let (source_path, metadata) = match listing {
                        wire::PathListEntry::Artifact(metadata) => {
                            validate_response_workbench(
                                &metadata.path,
                                &request.path.workbench_id,
                            )?;
                            (metadata.path.path.as_str().to_owned(), Some(metadata))
                        }
                        wire::PathListEntry::Prefix(path) => {
                            validate_response_workbench(&path, &request.path.workbench_id)?;
                            (path.path.as_str().to_owned(), None)
                        }
                    };
                    let source = NormalizedRelativePath::new(source_path).map_err(domain_input)?;
                    last_authoritative_path = Some(source.clone());
                    let Some(child) = direct_list_child(prefix.as_ref(), source)? else {
                        continue;
                    };
                    if after.is_some_and(|after| child <= *after) {
                        continue;
                    }
                    merge_list_candidate(
                        &mut candidates,
                        child,
                        metadata,
                        &request.path.workbench_id,
                        prefix.is_none(),
                    )?;
                }
                let covered_union_cutoff = candidates.len() > limit
                    && last_authoritative_path.as_ref().is_some_and(|last| {
                        candidates
                            .keys()
                            .nth(limit)
                            .is_some_and(|cutoff| last >= cutoff)
                    });
                if exhausted || covered_union_cutoff {
                    break page_read_version;
                }
                scan_cursor = next_server_cursor;
            };

            let continuation_fence = continuation_fence
                .as_ref()
                .expect("every list attempt establishes a continuation fence");
            if prefix.is_some() {
                match self.path_metadata_at_read_version(
                    &request.path,
                    view.clone(),
                    Some(read_version),
                ) {
                    Ok(Some(_)) => {
                        return Err(not_directory_backend(&request.path));
                    }
                    Ok(None) => {}
                    Err(error)
                        if !supplied_cursor
                            && attempt < max_attempts
                            && error.kind == agent::BackendErrorKind::ReadFenceChanged =>
                    {
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                }
            }
            let has_more = candidates.len() > limit;
            let mut ordered = candidates.into_iter().collect::<Vec<_>>();
            ordered.truncate(limit);
            let next_cursor = has_more
                .then(|| {
                    ordered
                        .last()
                        .map(|(path, _)| encode_list_cursor(continuation_fence, scope_digest, path))
                })
                .flatten();
            return Ok(agent::ListPage {
                entries: ordered.into_iter().map(|(_, entry)| entry).collect(),
                next_cursor,
                read_version,
            });
        }

        unreachable!("consistent read attempt count is non-zero")
    }

    fn read(
        &self,
        request: agent::ReadRequest,
    ) -> Result<Option<agent::ArtifactBody>, agent::BackendError> {
        let view = self.resolved_view(&request.path.workbench_id, &request.view)?;
        let Some(metadata) = self.path_metadata(&request.path, view.clone())? else {
            return Ok(None);
        };
        let size = usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX);
        if size > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "artifact is {size} bytes, maximum is {}",
                self.max_artifact_bytes
            )));
        }
        let outcome = self
            .client
            .read_artifact(
                self.objects.as_ref(),
                None,
                workspace_path(&request.path)?,
                view,
            )
            .map_err(map_client_error)?;
        Ok(Some(agent::ArtifactBody {
            path: request.path,
            metadata: artifact_metadata(&outcome.metadata),
            bytes: outcome.bytes,
        }))
    }

    fn publish(
        &self,
        request: agent::PublishRequest,
    ) -> Result<agent::PublishOutcome, agent::BackendError> {
        self.ensure_artifact_size(&request.body)?;
        let full_path = full_path(&request.path)?;
        let body_digest = Sha256::digest(&request.body);
        let condition = match request.condition {
            agent::PublishCondition::CreateOnly => wire::PublishCondition::CreateOnly,
            agent::PublishCondition::ReplaceOnly {
                expected_generation,
            } => wire::PublishCondition::ReplaceOnly {
                expected_generation,
            },
        };
        let condition_bytes = match condition {
            wire::PublishCondition::CreateOnly => 0_u64.to_be_bytes(),
            wire::PublishCondition::ReplaceOnly {
                expected_generation,
            } => expected_generation.to_be_bytes(),
            wire::PublishCondition::Append { .. } => unreachable!("facade publish has no append"),
        };
        let content_type =
            wire::ContentType::new(request.content_type.clone()).map_err(protocol_input)?;
        let workspace = self
            .create_or_observe_workbench(&request.path.workbench_id)?
            .workspace;
        let operation_id = wire::OperationIdentity(self.fresh_fixed_identity(
            b"nokv.cli.artifact-operation\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                &condition_bytes,
                body_digest.as_slice(),
            ],
        ));
        let revision_id = wire::ArtifactRevisionIdentity(self.fresh_fixed_identity(
            b"nokv.cli.artifact-revision\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                body_digest.as_slice(),
            ],
        ));
        let created = matches!(condition, wire::PublishCondition::CreateOnly);
        let options = ArtifactPublishOptions::new(
            operation_id,
            revision_id,
            workspace_path(&request.path)?,
            condition,
            content_type,
        );
        let outcome = self.publish_artifact(options, &request.body)?;
        let publication = outcome.publication.value;
        Ok(agent::PublishOutcome {
            metadata: agent::ArtifactMetadata {
                generation: publication.generation,
                size_bytes: publication.logical_size,
                digest_uri: publication.body_digest.as_str().to_owned(),
                content_type: request.content_type,
                producer: None,
                manifest_id: None,
                indexed_fields: BTreeMap::new(),
            },
            created,
        })
    }

    fn append(
        &self,
        request: agent::AppendRequest,
    ) -> Result<agent::AppendOutcome, agent::BackendError> {
        if request.delta.len() > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "append delta is {} bytes, maximum is {}",
                request.delta.len(),
                self.max_artifact_bytes
            )));
        }
        let full_path = full_path(&request.path)?;
        let create_content_type =
            wire::ContentType::new(request.create_content_type).map_err(protocol_input)?;
        let content_type = request
            .content_type
            .map(wire::ContentType::new)
            .transpose()
            .map_err(protocol_input)?;
        let workspace = self
            .create_or_observe_workbench(&request.path.workbench_id)?
            .workspace;
        let delta_digest: [u8; 32] = Sha256::digest(&request.delta).into();
        let content_type_identity = content_type
            .as_ref()
            .map(wire::ContentType::as_str)
            .unwrap_or("")
            .as_bytes();
        let operation_id = wire::OperationIdentity(self.fresh_fixed_identity(
            b"nokv.cli.append-operation\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                &delta_digest,
                content_type_identity,
            ],
        ));
        let revision_id = wire::ArtifactRevisionIdentity(self.fresh_fixed_identity(
            b"nokv.cli.append-revision\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                &delta_digest,
            ],
        ));
        let mut options = ArtifactAppendOptions::new(
            operation_id,
            revision_id,
            workspace_path(&request.path)?,
            create_content_type,
        );
        if let Some(content_type) = content_type {
            options = options.with_content_type(content_type);
        }
        if let Some(max_logical_size) = request.max_logical_size {
            options = options.with_max_logical_size(max_logical_size);
        }

        let outcome = self
            .client
            .append_artifact(self.objects.as_ref(), options, &request.delta)
            .map_err(map_client_error)?;
        let publication = outcome.publication.value;
        let descriptor = outcome.descriptor;
        Ok(agent::AppendOutcome {
            metadata: agent::ArtifactMetadata {
                generation: publication.generation,
                size_bytes: publication.logical_size,
                digest_uri: publication.body_digest.as_str().to_owned(),
                content_type: descriptor.content_type.as_str().to_owned(),
                producer: descriptor.producer,
                manifest_id: descriptor.manifest_identity,
                indexed_fields: descriptor
                    .index_fields
                    .iter()
                    .map(|field| (field.field_id.clone(), scalar_value(&field.value)))
                    .collect(),
            },
            created: outcome.created,
        })
    }

    fn grep_candidates(
        &self,
        request: agent::GrepCandidateRequest,
    ) -> Result<agent::GrepCandidatePage, agent::BackendError> {
        let Some(workbench_id) = request.scope.workbench_id.as_ref() else {
            if request.scope.section.is_some() || request.scope.path.is_some() {
                return Err(invalid_backend_input(
                    "root grep scope cannot carry a section or relative path",
                ));
            }
            if !request.recursive {
                return Ok(agent::GrepCandidatePage {
                    candidates: Vec::new(),
                    next_cursor: None,
                });
            }
            let scope_digest = root_grep_scope_digest(
                self.client.root_id(),
                request.recursive,
                request.query_commitment,
            );
            let search_cursor = request
                .cursor
                .as_deref()
                .map(|cursor| decode_root_grep_cursor(cursor, scope_digest))
                .transpose()?;
            let call = self
                .client
                .search(wire::SearchRequest {
                    profile: wire::QueryProfile::ArtifactV1,
                    scope: wire::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    projection: Vec::new(),
                    sort: vec![wire::SortField {
                        field_id: "path".to_owned(),
                        direction: wire::SortDirection::Ascending,
                    }],
                    facets: Vec::new(),
                    page: wire::PageRequest {
                        cursor: search_cursor
                            .as_deref()
                            .map(|cursor| decode_cursor("search", cursor))
                            .transpose()?,
                        // Search results do not expose per-hit anchors. Reading
                        // one candidate per page keeps the cursor exact.
                        limit: 1,
                    },
                })
                .map_err(|error| map_read_enumeration_error(error, "grep candidate enumeration"))?;
            if call.value.hits.len() > 1 {
                return Err(protocol_mismatch(
                    "root grep search returned more than its one-row limit",
                ));
            }
            if !call.value.facets.is_empty()
                || call.value.hits.iter().any(|row| match row {
                    wire::SearchRow::Artifact(hit) => !hit.projection.is_empty(),
                    wire::SearchRow::GenericNamespace(_) => true,
                })
            {
                return Err(protocol_mismatch(
                    "root grep search returned an unrequested projection or facet",
                ));
            }
            let next_cursor = call
                .value
                .next_cursor
                .as_deref()
                .map(|cursor| encode_cursor("search", cursor))
                .map(|cursor| encode_root_grep_cursor(scope_digest, &cursor));
            let candidates = call
                .value
                .hits
                .into_iter()
                .map(|row| {
                    let wire::SearchRow::Artifact(hit) = row else {
                        return Err(protocol_mismatch(
                            "root grep artifact query returned a generic namespace row",
                        ));
                    };
                    let authority = grep_candidate_authority(&hit.metadata);
                    let path = scoped_path(&hit.metadata.path)?;
                    Ok(agent::GrepCandidate {
                        path,
                        metadata: artifact_metadata(&hit.metadata),
                        authority,
                        cursor_after: next_cursor.clone(),
                    })
                })
                .collect::<Result<Vec<_>, agent::BackendError>>()?;
            return Ok(agent::GrepCandidatePage {
                candidates,
                next_cursor,
            });
        };
        let scoped_scope = agent::ScopedPath {
            workbench_id: workbench_id.clone(),
            section: request.scope.section,
            relative_path: request.scope.path.clone(),
        };
        let prefix = full_path_optional(&scoped_scope)?;
        let scope_digest = grep_scope_digest(
            self.client.root_id(),
            workbench_id,
            prefix.as_ref(),
            request.recursive,
            request.query_commitment,
        );
        let cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_grep_cursor(cursor, scope_digest))
            .transpose()?;
        let supplied_cursor = cursor.is_some();
        let page_limit = page_limit(request.limit)?;
        let has_exact_scope = prefix.is_some();
        if !supplied_cursor && has_exact_scope {
            if let Some(metadata) =
                self.path_metadata(&scoped_scope, wire::WorkspaceReadView::Live)?
            {
                return exact_grep_candidate_page(metadata);
            }
        }
        let max_attempts = if supplied_cursor {
            1
        } else {
            CONSISTENT_READ_MAX_ATTEMPTS
        };
        for attempt in 1..=max_attempts {
            let fence = match cursor.as_ref() {
                Some(cursor) => cursor.fence.clone(),
                None => {
                    let workspace = self.workspace(workbench_id)?;
                    if workspace.workbench.as_str() != workbench_id.as_str() {
                        return Err(protocol_mismatch(
                            "get_workspace returned a different workbench for grep pagination",
                        ));
                    }
                    wire::WorkspaceContinuationFence {
                        workspace_incarnation_id: workspace.workspace_incarnation_id,
                        workspace_revision: workspace.workspace_revision,
                    }
                }
            };
            let call = match self.client.list_paths(wire::ListPathsRequest {
                workbench: workbench_name(workbench_id)?,
                prefix: prefix.as_ref().map(relative_path).transpose()?,
                recursive: request.recursive,
                view: wire::WorkspaceReadView::Live,
                expected_read_version: None,
                workspace_continuation_fence: Some(fence.clone()),
                page: wire::PageRequest {
                    cursor: cursor.as_ref().map(|cursor| cursor.server_cursor.clone()),
                    limit: page_limit,
                },
            }) {
                Ok(call) => call,
                Err(error)
                    if !supplied_cursor
                        && attempt < max_attempts
                        && is_continuation_fence_conflict(&error) =>
                {
                    continue;
                }
                Err(error) => {
                    return Err(map_read_enumeration_error(
                        error,
                        "grep candidate enumeration",
                    ));
                }
            };
            if !supplied_cursor
                && has_exact_scope
                && (request.scope.path.is_some() || !call.value.entries.is_empty())
            {
                match self.path_metadata_at_read_version(
                    &scoped_scope,
                    wire::WorkspaceReadView::Live,
                    Some(call.value.read_version),
                ) {
                    Ok(Some(metadata)) => return exact_grep_candidate_page(metadata),
                    Ok(None) if call.value.entries.is_empty() => {
                        return Err(agent::BackendError::new(
                            agent::BackendErrorKind::NotFound,
                            "grep scope does not exist",
                            false,
                            json!({"path": scoped_scope.logical_path()}),
                        ));
                    }
                    Ok(None) => {}
                    Err(error)
                        if attempt < max_attempts
                            && error.kind == agent::BackendErrorKind::ReadFenceChanged =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            let server_next_cursor = call.value.next_cursor.clone();
            let mut metadata_rows = Vec::new();
            for entry in call.value.entries {
                validate_response_workbench(entry.path(), workbench_id)?;
                let metadata = match entry {
                    wire::PathListEntry::Artifact(metadata) => metadata,
                    wire::PathListEntry::Prefix(_) if !request.recursive => continue,
                    wire::PathListEntry::Prefix(_) => {
                        return Err(protocol_mismatch(
                            "recursive list_paths returned an implicit prefix",
                        ));
                    }
                };
                metadata_rows.push(metadata);
            }
            let has_more = server_next_cursor.is_some();
            let candidate_count = metadata_rows.len();
            let candidates = metadata_rows
                .into_iter()
                .enumerate()
                .map(|(index, metadata)| {
                    let cursor_after = (index + 1 < candidate_count || has_more).then(|| {
                        encode_grep_cursor(
                            &fence,
                            scope_digest,
                            metadata.path.path.as_str().as_bytes(),
                        )
                    });
                    Ok(agent::GrepCandidate {
                        path: scoped_path(&metadata.path)?,
                        metadata: artifact_metadata(&metadata),
                        authority: grep_candidate_authority(&metadata),
                        cursor_after,
                    })
                })
                .collect::<Result<Vec<_>, agent::BackendError>>()?;
            return Ok(agent::GrepCandidatePage {
                candidates,
                next_cursor: server_next_cursor
                    .as_deref()
                    .map(|cursor| encode_grep_cursor(&fence, scope_digest, cursor)),
            });
        }

        unreachable!("consistent read attempt count is non-zero")
    }

    fn grep_candidate_metadata(
        &self,
        fence: &agent::GrepCandidateReadFence,
    ) -> Result<agent::ArtifactMetadata, agent::BackendError> {
        let authority = ArtifactReadAuthority {
            workspace_incarnation_id: fence.authority.workspace_incarnation_id.into(),
            workspace_revision: fence.authority.workspace_revision,
            artifact_revision_id: fence.authority.artifact_revision_id.into(),
            generation: fence.authority.generation,
        };
        let metadata = self
            .client
            .artifact_metadata_at_authority(
                workspace_path(&fence.path)?,
                wire::WorkspaceReadView::Live,
                authority,
            )
            .map_err(|error| map_grep_candidate_read_error(error, &fence.path))?;
        Ok(artifact_metadata(&metadata))
    }

    fn read_grep_candidate(
        &self,
        fence: &agent::GrepCandidateReadFence,
    ) -> Result<agent::ArtifactBody, agent::BackendError> {
        let target = workspace_path(&fence.path)?;
        let authority = ArtifactReadAuthority {
            workspace_incarnation_id: fence.authority.workspace_incarnation_id.into(),
            workspace_revision: fence.authority.workspace_revision,
            artifact_revision_id: fence.authority.artifact_revision_id.into(),
            generation: fence.authority.generation,
        };
        let frozen = self
            .client
            .artifact_metadata_at_authority(
                target.clone(),
                wire::WorkspaceReadView::Live,
                authority,
            )
            .map_err(|error| map_grep_candidate_read_error(error, &fence.path))?;
        let logical_size = frozen.descriptor.logical_size;
        let size = usize::try_from(logical_size).unwrap_or(usize::MAX);
        if size > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "artifact is {size} bytes, maximum is {}",
                self.max_artifact_bytes
            )));
        }
        let outcome = self
            .client
            .read_artifact_at_authority(
                self.objects.as_ref(),
                None,
                target,
                wire::WorkspaceReadView::Live,
                authority,
            )
            .map_err(|error| map_grep_candidate_read_error(error, &fence.path))?;
        if grep_candidate_authority(&outcome.metadata) != fence.authority
            || outcome.metadata != frozen
            || outcome.bytes.len() != size
        {
            return Err(grep_candidate_read_fence_changed(&fence.path));
        }
        Ok(agent::ArtifactBody {
            path: fence.path.clone(),
            metadata: artifact_metadata(&outcome.metadata),
            bytes: outcome.bytes,
        })
    }

    fn search(
        &self,
        request: agent::SearchRequest,
    ) -> Result<agent::SearchPage, agent::BackendError> {
        let call = self
            .client
            .search(wire::SearchRequest {
                profile: query_profile(&request.profile),
                scope: query_scope(&request.scope)?,
                predicates: request
                    .predicates
                    .iter()
                    .map(query_predicate)
                    .collect::<Result<Vec<_>, _>>()?,
                projection: request.fields,
                sort: query_sort(&request.sort),
                facets: request.facets,
                page: wire::PageRequest {
                    cursor: request
                        .cursor
                        .as_deref()
                        .map(|cursor| decode_cursor("search", cursor))
                        .transpose()?,
                    limit: query_page_limit(request.limit)?,
                },
            })
            .map_err(|error| map_read_enumeration_error(error, "search"))?;
        let mut hits = Vec::new();
        let mut namespace_hits = Vec::new();
        for row in call.value.hits {
            match (&request.profile, row) {
                (agent::QueryProfile::ArtifactV1, wire::SearchRow::Artifact(hit)) => {
                    hits.push(search_hit(hit)?);
                }
                (
                    agent::QueryProfile::GenericNamespaceV1 { .. },
                    wire::SearchRow::GenericNamespace(hit),
                ) => namespace_hits.push(generic_namespace_hit(hit)?),
                _ => {
                    return Err(protocol_mismatch(
                        "search row variant does not match the requested query profile",
                    ));
                }
            }
        }
        Ok(agent::SearchPage {
            hits,
            namespace_hits,
            match_count: call.value.match_count,
            facets: call
                .value
                .facets
                .into_iter()
                .map(facet_result)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: call
                .value
                .next_cursor
                .as_deref()
                .map(|cursor| encode_cursor("search", cursor)),
            read_version: call.value.read_version,
        })
    }

    fn aggregate(
        &self,
        request: agent::AggregateRequest,
    ) -> Result<agent::AggregatePage, agent::BackendError> {
        let call = self
            .client
            .aggregate(wire::AggregateRequest {
                profile: query_profile(&request.profile),
                scope: query_scope(&request.scope)?,
                predicates: request
                    .predicates
                    .iter()
                    .map(query_predicate)
                    .collect::<Result<Vec<_>, _>>()?,
                group_by: request.group_by,
                aggregates: request
                    .measures
                    .into_iter()
                    .map(|measure| wire::AggregateSpec {
                        function: match measure.operator {
                            agent::AggregateOperator::Count => wire::AggregateFunction::Count,
                            agent::AggregateOperator::Sum => wire::AggregateFunction::Sum,
                            agent::AggregateOperator::Average => wire::AggregateFunction::Average,
                            agent::AggregateOperator::Minimum => wire::AggregateFunction::Minimum,
                            agent::AggregateOperator::Maximum => wire::AggregateFunction::Maximum,
                        },
                        field_id: measure.field,
                        result_id: measure.name,
                    })
                    .collect(),
                sort: query_sort(&request.sort),
                page: wire::PageRequest {
                    cursor: request
                        .cursor
                        .as_deref()
                        .map(|cursor| decode_cursor("aggregate", cursor))
                        .transpose()?,
                    limit: query_page_limit(request.limit)?,
                },
            })
            .map_err(|error| map_read_enumeration_error(error, "aggregate"))?;
        Ok(agent::AggregatePage {
            rows: call
                .value
                .groups
                .into_iter()
                .map(|group| {
                    Ok(agent::AggregateRow {
                        groups: field_map(group.keys)?,
                        measures: field_map(group.values)?,
                    })
                })
                .collect::<Result<Vec<_>, agent::BackendError>>()?,
            input_match_count: call.value.input_match_count,
            row_count: call.value.row_count,
            group_count: call.value.group_count,
            next_cursor: call
                .value
                .next_cursor
                .as_deref()
                .map(|cursor| encode_cursor("aggregate", cursor)),
            read_version: call.value.read_version,
        })
    }

    fn catalog(
        &self,
        request: agent::CatalogRequest,
    ) -> Result<agent::CatalogResult, agent::BackendError> {
        let scope = query_scope(&request.scope)?;
        'attempt: for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
            let mut cursor = None;
            let mut fields = Vec::new();
            let mut facets = None;
            let mut read_version = None;
            loop {
                let call = match self.client.catalog(wire::CatalogRequest {
                    profile: query_profile(&request.profile),
                    scope: scope.clone(),
                    path_match: match request.path_match {
                        agent::CatalogPathMatch::Prefix => wire::CatalogPathMatch::Prefix,
                        agent::CatalogPathMatch::Exact => wire::CatalogPathMatch::Exact,
                    },
                    field_prefix: request.field_prefix.clone(),
                    include_facets: request.include_facets,
                    page: wire::PageRequest {
                        cursor,
                        limit: QUERY_SERVER_PAGE_LIMIT,
                    },
                }) {
                    Ok(call) => call,
                    Err(error)
                        if attempt < CONSISTENT_READ_MAX_ATTEMPTS
                            && is_read_version_conflict(&error) =>
                    {
                        continue 'attempt;
                    }
                    Err(error) => {
                        return Err(map_read_enumeration_error(error, "catalog"));
                    }
                };
                if read_version.is_some_and(|expected| expected != call.value.read_version) {
                    return Err(protocol_mismatch(
                        "catalog returned a page outside its cursor read version",
                    ));
                }
                read_version.get_or_insert(call.value.read_version);
                let page_facets = call
                    .value
                    .facets
                    .into_iter()
                    .map(facet_result)
                    .collect::<Result<Vec<_>, _>>()?;
                if facets
                    .as_ref()
                    .is_some_and(|expected: &Vec<agent::FacetResult>| expected != &page_facets)
                {
                    return Err(protocol_mismatch(
                        "catalog facets changed across one cursor read version",
                    ));
                }
                facets.get_or_insert(page_facets);
                fields.extend(call.value.fields.into_iter().map(|field| {
                    agent::CatalogField {
                        field: field.field_id,
                        scalar_type: field.scalar_type,
                        scalar_types: field.scalar_types,
                        generic_custom: field.generic_custom,
                        operators: field
                            .operators
                            .into_iter()
                            .map(query_operator_name)
                            .map(str::to_owned)
                            .collect(),
                        sortable: field.sortable,
                        facetable: field.facetable,
                        aggregatable: field.aggregatable,
                    }
                }));
                let Some(next) = call.value.next_cursor else {
                    return Ok(agent::CatalogResult {
                        fields,
                        facets: facets.unwrap_or_default(),
                        read_version: read_version.expect("catalog always reads one page"),
                    });
                };
                cursor = Some(next);
            }
        }

        unreachable!("consistent read attempt count is non-zero")
    }

    fn find_workbenches(
        &self,
        request: agent::FindRequest,
    ) -> Result<agent::FindPage, agent::BackendError> {
        let cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_cursor("find", cursor))
            .transpose()?;
        let limit = query_page_limit(request.limit)?;
        let mut budget = WorkbenchEntryCountBudget::default();
        let supplied_cursor = cursor.is_some();
        let max_attempts = if supplied_cursor {
            1
        } else {
            CONSISTENT_READ_MAX_ATTEMPTS
        };
        'attempt: for attempt in 1..=max_attempts {
            let call = match self.client.find_workspaces(wire::FindWorkspacesRequest {
                committed_only: request.committed == Some(true),
                page: wire::PageRequest {
                    cursor: cursor.clone(),
                    limit,
                },
            }) {
                Ok(call) => call,
                Err(error)
                    if !supplied_cursor
                        && attempt < max_attempts
                        && is_continuation_fence_conflict(&error) =>
                {
                    continue 'attempt;
                }
                Err(error) => return Err(map_read_enumeration_error(error, "find")),
            };
            let read_version = call.value.read_version;
            let entry_count = call.value.workspaces.len();
            let next_cursor = call.value.next_cursor;
            let mut workbenches = Vec::new();
            for discovered in call.value.workspaces {
                let committed = discovered.workspace.commit_head.is_some();
                if request.committed.is_some_and(|wanted| wanted != committed) {
                    continue;
                }
                let workbench_id =
                    WorkbenchId::new(discovered.workspace.workbench.as_str().to_owned())
                        .map_err(domain_input)?;
                let manifest_projection =
                    if request.include_manifest || request.manifest_pattern.is_some() {
                        match self.read_run_manifest_at_read_version(
                            &discovered.workspace,
                            &workbench_id,
                            read_version,
                        ) {
                            Ok(projection) => projection,
                            Err(error)
                                if !supplied_cursor
                                    && attempt < max_attempts
                                    && error.kind == agent::BackendErrorKind::ReadFenceChanged =>
                            {
                                continue 'attempt;
                            }
                            Err(error) => return Err(error),
                        }
                    } else {
                        None
                    };
                let canonical_manifest = manifest_projection
                    .as_ref()
                    .map(|projection| projection.verified.canonical_envelope.as_slice());
                if !find_request_matches_canonical_manifest(&request, canonical_manifest) {
                    continue;
                }
                let workbench_entry_count = match self.workbench_entry_count_at_read_version(
                    &workbench_id,
                    read_version,
                    &mut budget,
                ) {
                    Ok(entry_count) => entry_count,
                    Err(error)
                        if !supplied_cursor
                            && attempt < max_attempts
                            && error.kind == agent::BackendErrorKind::ReadFenceChanged =>
                    {
                        continue 'attempt;
                    }
                    Err(error) => return Err(error),
                };
                workbenches.push(agent::WorkbenchSummary {
                    workbench_id,
                    committed,
                    commit_id: discovered.workspace.commit_head.map(|identity| identity.0),
                    entry_count: workbench_entry_count,
                    manifest_metadata: manifest_projection
                        .as_ref()
                        .map(|projection| projection.metadata.clone()),
                    manifest: manifest_projection.map(|projection| projection.verified.envelope),
                });
            }
            return Ok(agent::FindPage {
                workbenches,
                entry_count,
                next_cursor: next_cursor
                    .as_deref()
                    .map(|cursor| encode_cursor("find", cursor)),
                read_version,
            });
        }

        unreachable!("consistent read attempt count is non-zero")
    }

    fn commit(
        &self,
        request: agent::CommitRequest,
    ) -> Result<agent::CommitOutcome, agent::BackendError> {
        let request = WorkbenchCommitRequest {
            workbench_id: request.workbench_id,
            canonical_manifest: request.canonical_manifest,
            workbench_path: request.workbench_path,
            content_digest_uri: request.content_digest_uri,
            manifest_digest_uri: request.manifest_digest_uri,
            stable_commit_id: request.stable_commit_id,
            replace: request.replace,
        };
        let outcome = self
            .lifecycle_facade()?
            .commit(request)
            .map_err(map_lifecycle_error)?;
        Ok(agent::CommitOutcome {
            commit_id: outcome.commit_id,
            generation: outcome.commit_head_generation,
            manifest_size_bytes: outcome.manifest_size_bytes,
            envelope_digest_uri: outcome.envelope_digest_uri,
            tree_digest_uri: outcome.tree_digest_uri,
            idempotent_replay: outcome.idempotent_replay,
        })
    }

    fn mint_snapshot(
        &self,
        request: agent::SnapshotMintRequest,
    ) -> Result<agent::SnapshotRecord, agent::BackendError> {
        let workspace = self.workspace(&request.workbench_id)?;
        if workspace.commit_head.is_none() {
            return Err(agent::BackendError::new(
                agent::BackendErrorKind::InvalidState,
                "only a committed workbench can be snapshotted",
                false,
                json!({"workbench_id": request.workbench_id.as_str()}),
            ));
        }
        let lease_deadline_ms = lease_deadline(request.lease_millis)?;
        let annotation = serde_json::to_vec(&request.annotation)
            .map_err(|error| invalid_state("snapshot annotation is not serializable", error))?;
        let alias = request
            .name
            .map(wire::SnapshotAlias::new)
            .transpose()
            .map_err(protocol_input)?;
        self.client
            .mint_snapshot_workflow(SnapshotMintOptions {
                workbench: workspace.workbench,
                workspace_incarnation_id: workspace.workspace_incarnation_id,
                lease_deadline_ms,
                alias,
                annotation,
            })
            .map_err(map_snapshot_client_error)
            .and_then(|call| snapshot_record(call.value))
    }

    fn renew_snapshot(
        &self,
        request: agent::SnapshotRenewRequest,
    ) -> Result<agent::SnapshotRecord, agent::BackendError> {
        let deadline = lease_deadline(request.lease_millis)?;
        self.client
            .renew_snapshot_workflow(SnapshotRenewOptions {
                workbench: workbench_name(&request.workbench_id)?,
                selector: snapshot_selector(&request.selector)?,
                lease_deadline_ms: deadline,
            })
            .map_err(map_snapshot_client_error)
            .and_then(|call| snapshot_record(call.value))
    }

    fn retire_snapshot(
        &self,
        request: agent::SnapshotRetireRequest,
    ) -> Result<agent::SnapshotRetireOutcome, agent::BackendError> {
        let retire_annotation = request
            .reason
            .map(|reason| {
                agent::canonical_json_bytes(&json!({"reason": reason, "metadata": Value::Null}))
                    .map_err(|error| {
                        invalid_state("retire annotation canonicalization failed", error)
                    })
            })
            .transpose()?;
        let outcome = self
            .client
            .retire_snapshot_workflow(SnapshotRetireOptions {
                workbench: workbench_name(&request.workbench_id)?,
                selector: snapshot_selector(&request.selector)?,
                retire_annotation,
            })
            .map_err(map_snapshot_client_error)?;
        let record = snapshot_record(outcome.snapshot)?;
        Ok(agent::SnapshotRetireOutcome {
            snapshot_id: record.snapshot_id,
            name: record.name,
            retired: outcome.retired,
            state: record.state,
            retire_annotation: record.retire_annotation,
        })
    }

    fn list_snapshots(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Vec<agent::SnapshotRecord>, agent::BackendError> {
        self.client
            .list_all_snapshots(workbench_name(workbench_id)?)
            .map_err(map_snapshot_client_error)?
            .into_iter()
            .map(snapshot_record)
            .collect()
    }

    fn restore(
        &self,
        request: agent::RestoreRequest,
    ) -> Result<agent::RestoreOutcome, agent::BackendError> {
        let request = WorkbenchRestoreRequest {
            source_workbench_id: request.source_workbench_id,
            source_workbench_path: request.source_workbench_path,
            origin: match request.origin {
                agent::RestoreOrigin::Snapshot(agent::SnapshotSelector::Id(snapshot_id)) => {
                    WorkbenchRestoreOrigin::Snapshot(WorkbenchSnapshotSelector::Id(snapshot_id))
                }
                agent::RestoreOrigin::Snapshot(agent::SnapshotSelector::Name(name)) => {
                    WorkbenchRestoreOrigin::Snapshot(WorkbenchSnapshotSelector::Name(name))
                }
                agent::RestoreOrigin::Commit(commit_id) => {
                    WorkbenchRestoreOrigin::Commit(commit_id)
                }
            },
            destination_workbench_id: request.destination_workbench_id,
            destination_workbench_path: request.destination_workbench_path,
        };
        let outcome = self
            .lifecycle_facade()?
            .restore(request)
            .map_err(map_lifecycle_error)?;
        let (snapshot_id, commit_id) = match outcome.source {
            WorkbenchRestoreSource::Snapshot { snapshot_id } => (Some(snapshot_id), None),
            WorkbenchRestoreSource::Commit { commit_id } => (None, Some(commit_id)),
        };
        Ok(agent::RestoreOutcome {
            operation_id: outcome.operation_id,
            snapshot_id,
            read_version: outcome.source_snapshot_read_version,
            commit_id,
            destination_generation: outcome.destination_workspace_revision,
            idempotent_replay: outcome.idempotent_replay,
        })
    }
}

impl CliWorkbenchBackend {
    fn read_run_manifest_at_read_version(
        &self,
        workspace: &wire::WorkspaceSummary,
        workbench_id: &WorkbenchId,
        read_version: u64,
    ) -> Result<Option<RunManifestProjection>, agent::BackendError> {
        let path = scoped_full_path(workbench_id, RUN_MANIFEST_PATH)?;
        let view = wire::WorkspaceReadView::Live;
        let Some(metadata) =
            self.path_metadata_at_read_version(&path, view.clone(), Some(read_version))?
        else {
            if workspace.commit_head.is_some() {
                return Err(protocol_mismatch(
                    "committed workbench has no metadata/run_manifest.json",
                ));
            }
            return Ok(None);
        };
        let size = usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX);
        if size > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "run manifest is {size} bytes, maximum is {}",
                self.max_artifact_bytes
            )));
        }
        let authority = ArtifactReadAuthority::from(&metadata);
        let body = self
            .client
            .read_artifact_at_authority(
                self.objects.as_ref(),
                None,
                workspace_path(&path)?,
                view,
                authority,
            )
            .map_err(|error| map_manifest_read_error(error, &path))?;
        if body.metadata != metadata {
            return Err(protocol_mismatch(
                "authority-fenced run manifest read returned different metadata",
            ));
        }
        let verified = agent::verify_run_manifest_v1(&body.bytes)
            .map_err(|error| invalid_state("run manifest violates the v1 projection", error))?;
        if verified.workbench_id != *workbench_id
            || verified.envelope_digest_uri != metadata.descriptor.body_digest.as_str()
            || verified.canonical_envelope.len() as u64 != metadata.descriptor.logical_size
            || metadata.descriptor.content_type.as_str() != JSON_CONTENT_TYPE
            || workspace
                .commit_head
                .is_some_and(|commit_id| commit_id.0 != verified.commit_identity)
        {
            return Err(protocol_mismatch(
                "run manifest envelope does not match its path descriptor or typed commit head",
            ));
        }
        Ok(Some(RunManifestProjection {
            metadata: artifact_metadata(&metadata),
            verified,
        }))
    }
}

struct RunManifestProjection {
    metadata: agent::ArtifactMetadata,
    verified: agent::VerifiedRunManifestV1,
}

fn workbench_name(workbench_id: &WorkbenchId) -> Result<wire::WorkbenchName, agent::BackendError> {
    wire::WorkbenchName::new(workbench_id.as_str().to_owned()).map_err(protocol_input)
}

fn snapshot_selector(
    selector: &agent::SnapshotSelector,
) -> Result<wire::SnapshotSelector, agent::BackendError> {
    match selector {
        agent::SnapshotSelector::Id(snapshot_id) => Ok(wire::SnapshotSelector::Id(*snapshot_id)),
        agent::SnapshotSelector::Name(name) => wire::SnapshotAlias::new(name.clone())
            .map(wire::SnapshotSelector::Alias)
            .map_err(protocol_input),
    }
}

fn relative_path(path: &NormalizedRelativePath) -> Result<wire::RelativePath, agent::BackendError> {
    wire::RelativePath::new(path.as_str().to_owned()).map_err(protocol_input)
}

fn workspace_path(path: &agent::ScopedPath) -> Result<wire::WorkspacePath, agent::BackendError> {
    Ok(wire::WorkspacePath {
        workbench: workbench_name(&path.workbench_id)?,
        path: relative_path(&full_path(path)?)?,
    })
}

fn validate_response_workbench(
    path: &wire::WorkspacePath,
    expected: &WorkbenchId,
) -> Result<(), agent::BackendError> {
    if path.workbench.as_str() != expected.as_str() {
        return Err(protocol_mismatch(
            "list_paths returned an entry for a different workbench",
        ));
    }
    Ok(())
}

fn full_path(path: &agent::ScopedPath) -> Result<NormalizedRelativePath, agent::BackendError> {
    let value = path.logical_path();
    if value.is_empty() {
        return Err(agent::BackendError::new(
            agent::BackendErrorKind::InvalidState,
            "artifact path is required",
            false,
            json!({"workbench_id": path.workbench_id.as_str()}),
        ));
    }
    NormalizedRelativePath::new(value).map_err(domain_input)
}

fn full_path_optional(
    path: &agent::ScopedPath,
) -> Result<Option<NormalizedRelativePath>, agent::BackendError> {
    let value = path.logical_path();
    if value.is_empty() {
        Ok(None)
    } else {
        NormalizedRelativePath::new(value)
            .map(Some)
            .map_err(domain_input)
    }
}

fn scoped_full_path(
    workbench_id: &WorkbenchId,
    full_path: &str,
) -> Result<agent::ScopedPath, agent::BackendError> {
    let path = NormalizedRelativePath::new(full_path.to_owned()).map_err(domain_input)?;
    scoped_normalized_path(workbench_id.clone(), path)
}

fn scoped_path(path: &wire::WorkspacePath) -> Result<agent::ScopedPath, agent::BackendError> {
    let workbench_id =
        WorkbenchId::new(path.workbench.as_str().to_owned()).map_err(domain_input)?;
    let path = NormalizedRelativePath::new(path.path.as_str().to_owned()).map_err(domain_input)?;
    scoped_normalized_path(workbench_id, path)
}

fn scoped_normalized_path(
    workbench_id: WorkbenchId,
    path: NormalizedRelativePath,
) -> Result<agent::ScopedPath, agent::BackendError> {
    let text = path.as_str();
    let (first, remainder) = text
        .split_once('/')
        .map_or((text, None), |(first, rest)| (first, Some(rest)));
    if let Some(section) = section(first) {
        let relative_path = remainder
            .map(|remainder| NormalizedRelativePath::new(remainder.to_owned()))
            .transpose()
            .map_err(domain_input)?;
        Ok(agent::ScopedPath {
            workbench_id,
            section: Some(section),
            relative_path,
        })
    } else {
        Ok(agent::ScopedPath {
            workbench_id,
            section: None,
            relative_path: Some(path),
        })
    }
}

fn section(value: &str) -> Option<agent::Section> {
    agent::WORKBENCH_SECTIONS
        .into_iter()
        .find(|section| section.as_str() == value)
}

fn artifact_metadata(metadata: &wire::PathMetadata) -> agent::ArtifactMetadata {
    agent::ArtifactMetadata {
        generation: metadata.generation,
        size_bytes: metadata.descriptor.logical_size,
        digest_uri: metadata.descriptor.body_digest.as_str().to_owned(),
        content_type: metadata.descriptor.content_type.as_str().to_owned(),
        producer: metadata.descriptor.producer.clone(),
        manifest_id: metadata.descriptor.manifest_identity.clone(),
        indexed_fields: metadata
            .descriptor
            .index_fields
            .iter()
            .map(|field| (field.field_id.clone(), scalar_value(&field.value)))
            .collect(),
    }
}

fn grep_candidate_authority(metadata: &wire::PathMetadata) -> agent::GrepCandidateAuthority {
    agent::GrepCandidateAuthority {
        workspace_incarnation_id: WorkspaceIncarnationId::from(metadata.workspace_incarnation_id),
        workspace_revision: metadata.workspace_revision,
        artifact_revision_id: ArtifactRevisionId::from(metadata.artifact_revision_id),
        generation: metadata.generation,
    }
}

fn exact_grep_candidate_page(
    metadata: wire::PathMetadata,
) -> Result<agent::GrepCandidatePage, agent::BackendError> {
    let path = scoped_path(&metadata.path)?;
    Ok(agent::GrepCandidatePage {
        candidates: vec![agent::GrepCandidate {
            path,
            metadata: artifact_metadata(&metadata),
            authority: grep_candidate_authority(&metadata),
            cursor_after: None,
        }],
        next_cursor: None,
    })
}

fn merge_list_candidate(
    candidates: &mut BTreeMap<NormalizedRelativePath, agent::ListEntry>,
    child: NormalizedRelativePath,
    metadata: Option<wire::PathMetadata>,
    workbench_id: &WorkbenchId,
    root_scope: bool,
) -> Result<(), agent::BackendError> {
    let root_section = root_scope
        .then(|| child.components().next().and_then(section))
        .flatten();
    let (kind, artifact) = if let Some(metadata) = metadata.as_ref() {
        (
            agent::ArtifactKind::Artifact,
            Some(artifact_metadata(metadata)),
        )
    } else if root_section.is_some() {
        (agent::ArtifactKind::Section, None)
    } else {
        (agent::ArtifactKind::Directory, None)
    };
    let candidate = agent::ListEntry {
        path: scoped_normalized_path(workbench_id.clone(), child.clone())?,
        kind,
        artifact,
    };
    match candidates.get(&child) {
        Some(existing)
            if list_kind_priority(&existing.kind) >= list_kind_priority(&candidate.kind) => {}
        _ => {
            candidates.insert(child, candidate);
        }
    }
    Ok(())
}

fn direct_list_child(
    prefix: Option<&NormalizedRelativePath>,
    source: NormalizedRelativePath,
) -> Result<Option<NormalizedRelativePath>, agent::BackendError> {
    match prefix {
        None if source.component_count() == 1 => Ok(Some(source)),
        None => Err(protocol_mismatch(
            "non-recursive root listing returned a nested path",
        )),
        Some(prefix) if &source == prefix => Ok(None),
        Some(prefix) => {
            let remainder = source
                .as_str()
                .strip_prefix(prefix.as_str())
                .and_then(|value| value.strip_prefix('/'))
                .ok_or_else(|| {
                    protocol_mismatch("non-recursive listing returned a path outside its prefix")
                })?;
            if remainder.is_empty() || remainder.contains('/') {
                return Err(protocol_mismatch(
                    "non-recursive listing returned a non-direct descendant",
                ));
            }
            Ok(Some(source))
        }
    }
}

fn list_kind_priority(kind: &agent::ArtifactKind) -> u8 {
    match kind {
        agent::ArtifactKind::Directory => 0,
        agent::ArtifactKind::Section => 1,
        agent::ArtifactKind::Artifact => 2,
        agent::ArtifactKind::Workbench => 3,
    }
}

fn query_scope(scope: &agent::QueryScope) -> Result<wire::QueryScope, agent::BackendError> {
    let path_prefix = match (scope.section, &scope.path) {
        (None, None) => None,
        (None, Some(path)) => Some(path.clone()),
        (Some(section), None) => {
            Some(NormalizedRelativePath::new(section.as_str().to_owned()).map_err(domain_input)?)
        }
        (Some(section), Some(path)) => Some(
            NormalizedRelativePath::new(format!("{}/{path}", section.as_str()))
                .map_err(domain_input)?,
        ),
    };
    let path_prefix = path_prefix.as_ref().map(relative_path).transpose()?;
    match &scope.workbench_id {
        Some(workbench_id) => Ok(wire::QueryScope::Workspace {
            workbench: workbench_name(workbench_id)?,
            path_prefix,
        }),
        None => Ok(wire::QueryScope::Root { path_prefix }),
    }
}

fn query_profile(profile: &agent::QueryProfile) -> wire::QueryProfile {
    match profile {
        agent::QueryProfile::ArtifactV1 => wire::QueryProfile::ArtifactV1,
        agent::QueryProfile::GenericNamespaceV1 {
            presentation_path_root,
        } => wire::QueryProfile::GenericCustomIndexV1 {
            presentation_path_root: presentation_path_root.clone(),
        },
    }
}

fn query_predicate(
    predicate: &agent::QueryPredicate,
) -> Result<wire::QueryPredicate, agent::BackendError> {
    let operator = match predicate.operator {
        agent::PredicateOperator::Equal => wire::QueryOperator::Equal,
        agent::PredicateOperator::NotEqual => wire::QueryOperator::NotEqual,
        agent::PredicateOperator::In => wire::QueryOperator::In,
        agent::PredicateOperator::Prefix => wire::QueryOperator::Prefix,
        agent::PredicateOperator::Suffix => wire::QueryOperator::Suffix,
        agent::PredicateOperator::Contains => wire::QueryOperator::Contains,
        agent::PredicateOperator::Greater => wire::QueryOperator::Greater,
        agent::PredicateOperator::GreaterOrEqual => wire::QueryOperator::GreaterOrEqual,
        agent::PredicateOperator::Less => wire::QueryOperator::Less,
        agent::PredicateOperator::LessOrEqual => wire::QueryOperator::LessOrEqual,
        agent::PredicateOperator::Exists => wire::QueryOperator::Exists,
        agent::PredicateOperator::NotExists => wire::QueryOperator::NotExists,
    };
    let operand = match (&predicate.operator, predicate.value.as_ref()) {
        (agent::PredicateOperator::Exists | agent::PredicateOperator::NotExists, None) => {
            wire::QueryOperand::None
        }
        (agent::PredicateOperator::In, Some(agent::QueryValue::List(values))) => {
            wire::QueryOperand::Set(
                values
                    .iter()
                    .map(query_scalar)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (agent::PredicateOperator::In, _) => {
            return Err(invalid_backend_input(
                "in predicate requires a list operand",
            ));
        }
        (_, Some(value)) => wire::QueryOperand::Scalar(query_scalar(value)?),
        (_, None) => {
            return Err(invalid_backend_input(
                "comparison predicate requires a scalar operand",
            ));
        }
    };
    Ok(wire::QueryPredicate {
        field_id: predicate.field.clone(),
        operator,
        operand,
    })
}

fn query_scalar(value: &agent::QueryValue) -> Result<wire::ScalarValue, agent::BackendError> {
    match value {
        agent::QueryValue::Null => Ok(wire::ScalarValue::Null),
        agent::QueryValue::Boolean(value) => Ok(wire::ScalarValue::Boolean(*value)),
        agent::QueryValue::Unsigned(value) => Ok(wire::ScalarValue::Unsigned(*value)),
        agent::QueryValue::Signed(value) => Ok(wire::ScalarValue::Signed(*value)),
        agent::QueryValue::Float(value) if value.is_finite() => {
            Ok(wire::ScalarValue::Decimal(value.to_string()))
        }
        agent::QueryValue::Float(_) => Err(invalid_backend_input("query float must be finite")),
        agent::QueryValue::String(value) => Ok(wire::ScalarValue::String(value.clone())),
        agent::QueryValue::List(_) => Err(invalid_backend_input(
            "nested query lists are not scalar values",
        )),
    }
}

fn scalar_value(value: &wire::ScalarValue) -> agent::QueryValue {
    match value {
        wire::ScalarValue::Null => agent::QueryValue::Null,
        wire::ScalarValue::Boolean(value) => agent::QueryValue::Boolean(*value),
        wire::ScalarValue::Signed(value) | wire::ScalarValue::Timestamp(value) => {
            agent::QueryValue::Signed(*value)
        }
        wire::ScalarValue::Unsigned(value) => agent::QueryValue::Unsigned(*value),
        wire::ScalarValue::Decimal(value) => value.parse::<f64>().map_or_else(
            |_| agent::QueryValue::String(value.clone()),
            agent::QueryValue::Float,
        ),
        wire::ScalarValue::String(value) => agent::QueryValue::String(value.clone()),
        wire::ScalarValue::Bytes(value) => agent::QueryValue::String(URL_SAFE_NO_PAD.encode(value)),
    }
}

fn query_sort(sort: &[agent::QuerySort]) -> Vec<wire::SortField> {
    sort.iter()
        .map(|sort| wire::SortField {
            field_id: sort.field.clone(),
            direction: match sort.direction {
                agent::SortDirection::Ascending => wire::SortDirection::Ascending,
                agent::SortDirection::Descending => wire::SortDirection::Descending,
            },
        })
        .collect()
}

fn field_map(
    fields: Vec<wire::FieldValue>,
) -> Result<BTreeMap<String, agent::QueryValue>, agent::BackendError> {
    let mut values = BTreeMap::new();
    for field in fields {
        if values
            .insert(field.field_id.clone(), scalar_value(&field.value))
            .is_some()
        {
            return Err(protocol_mismatch("query result contains a duplicate field"));
        }
    }
    Ok(values)
}

fn generic_indexed_value_map(
    fields: Vec<wire::GenericIndexFieldValues>,
) -> Result<BTreeMap<String, Vec<agent::QueryValue>>, agent::BackendError> {
    let mut values = BTreeMap::new();
    for field in fields {
        let field_id = field.field_id;
        let ordered = field.values.iter().map(scalar_value).collect::<Vec<_>>();
        if values.insert(field_id, ordered).is_some() {
            return Err(protocol_mismatch(
                "generic query result contains duplicate indexed values",
            ));
        }
    }
    Ok(values)
}

fn search_hit(hit: wire::SearchHit) -> Result<agent::SearchHit, agent::BackendError> {
    Ok(agent::SearchHit {
        workbench_id: WorkbenchId::new(hit.metadata.path.workbench.as_str().to_owned())
            .map_err(domain_input)?,
        path: NormalizedRelativePath::new(hit.metadata.path.path.as_str().to_owned())
            .map_err(domain_input)?,
        metadata: artifact_metadata(&hit.metadata),
        projection: field_map(hit.projection)?,
    })
}

fn generic_namespace_hit(
    hit: wire::GenericNamespaceHit,
) -> Result<agent::GenericNamespaceHit, agent::BackendError> {
    let projection = field_map(hit.projection)?;
    let indexed_values = generic_indexed_value_map(hit.indexed_values)?;
    let artifact = hit.artifact.map(|artifact| agent::ArtifactMetadata {
        generation: artifact.generation,
        size_bytes: artifact.logical_size,
        digest_uri: artifact.body_digest.as_str().to_owned(),
        content_type: artifact.content_type.as_str().to_owned(),
        producer: artifact.producer,
        manifest_id: artifact.manifest_identity,
        indexed_fields: BTreeMap::new(),
    });
    let kind = match hit.kind {
        wire::GenericNamespaceKind::Directory => agent::ArtifactKind::Directory,
        wire::GenericNamespaceKind::Artifact => agent::ArtifactKind::Artifact,
    };
    if (kind == agent::ArtifactKind::Artifact) != artifact.is_some() {
        return Err(protocol_mismatch(
            "generic namespace artifact metadata does not match its kind",
        ));
    }
    Ok(agent::GenericNamespaceHit {
        workbench_id: WorkbenchId::new(hit.workbench.as_str().to_owned()).map_err(domain_input)?,
        relative_path: hit
            .relative_path
            .map(|path| NormalizedRelativePath::new(path.as_str().to_owned()))
            .transpose()
            .map_err(domain_input)?,
        kind,
        artifact,
        projection,
        indexed_values,
    })
}

fn facet_result(facet: wire::FacetResult) -> Result<agent::FacetResult, agent::BackendError> {
    Ok(agent::FacetResult {
        field: facet.field_id,
        buckets: facet
            .buckets
            .into_iter()
            .map(|bucket| agent::FacetBucket {
                value: scalar_value(&bucket.value),
                count: bucket.count,
            })
            .collect(),
        distinct_count: facet.distinct_count,
        truncated: facet.truncated,
    })
}

fn query_operator_name(operator: wire::QueryOperator) -> &'static str {
    match operator {
        wire::QueryOperator::Equal => "eq",
        wire::QueryOperator::NotEqual => "ne",
        wire::QueryOperator::In => "in",
        wire::QueryOperator::Less => "lt",
        wire::QueryOperator::LessOrEqual => "lte",
        wire::QueryOperator::Greater => "gt",
        wire::QueryOperator::GreaterOrEqual => "gte",
        wire::QueryOperator::Prefix => "prefix",
        wire::QueryOperator::Suffix => "suffix",
        wire::QueryOperator::Contains => "contains",
        wire::QueryOperator::Exists => "exists",
        wire::QueryOperator::NotExists => "not_exists",
    }
}

fn snapshot_record(
    result: wire::SnapshotResult,
) -> Result<agent::SnapshotRecord, agent::BackendError> {
    let annotation = serde_json::from_slice(&result.annotation)
        .map_err(|error| invalid_state("snapshot annotation is not valid JSON", error))?;
    let retire_annotation = result
        .retire_annotation
        .map(|annotation| {
            serde_json::from_slice(&annotation).map_err(|error| {
                invalid_state("snapshot retire annotation is not valid JSON", error)
            })
        })
        .transpose()?;
    Ok(agent::SnapshotRecord {
        snapshot_id: result.snapshot_id,
        name: result.alias.map(|alias| alias.as_str().to_owned()),
        read_version: result.read_version,
        lease_expires_unix_ms: Some(result.lease_deadline_ms),
        annotation,
        retire_annotation,
        state: match result.status {
            wire::SnapshotStatus::Alive => agent::SnapshotLifecycleState::Alive,
            wire::SnapshotStatus::Expired | wire::SnapshotStatus::ReapClaimed => {
                agent::SnapshotLifecycleState::Expired
            }
            wire::SnapshotStatus::Retired => agent::SnapshotLifecycleState::Retired,
            wire::SnapshotStatus::Reaped => agent::SnapshotLifecycleState::Reaped,
        },
    })
}

fn lease_deadline(lease_millis: u64) -> Result<u64, agent::BackendError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
    now.checked_add(lease_millis)
        .filter(|deadline| *deadline != 0)
        .ok_or_else(|| invalid_backend_input("snapshot lease deadline overflows"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ListCursor {
    fence: ListContinuationFence,
    anchor: NormalizedRelativePath,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GrepCursor {
    fence: wire::WorkspaceContinuationFence,
    server_cursor: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ListContinuationFence {
    // Snapshot pages stay pinned to one historical root read version.
    ReadVersion(u64),
    // Live pages may advance the root version while this workspace stays exact.
    Workspace(wire::WorkspaceContinuationFence),
}

fn grep_scope_digest(
    root_id: wire::RootIdentity,
    workbench_id: &WorkbenchId,
    prefix: Option<&NormalizedRelativePath>,
    recursive: bool,
    query_commitment: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workspace.grep-scope.v2\0");
    hasher.update(root_id.0);
    hash_len64(&mut hasher, workbench_id.as_bytes());
    match prefix {
        None => hasher.update([0]),
        Some(prefix) => {
            hasher.update([1]);
            hash_len64(&mut hasher, prefix.as_str().as_bytes());
        }
    }
    hasher.update([u8::from(recursive)]);
    hasher.update(query_commitment);
    hasher.finalize().into()
}

fn root_grep_scope_digest(
    root_id: wire::RootIdentity,
    recursive: bool,
    query_commitment: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workspace.root-grep-scope.v1\0");
    hasher.update(root_id.0);
    hasher.update([u8::from(recursive)]);
    hasher.update(query_commitment);
    hasher.finalize().into()
}

fn encode_root_grep_cursor(scope_digest: [u8; 32], search_cursor: &str) -> String {
    let mut payload = Vec::with_capacity(scope_digest.len() + search_cursor.len());
    payload.extend_from_slice(&scope_digest);
    payload.extend_from_slice(search_cursor.as_bytes());
    encode_cursor("grep-root", &payload)
}

fn decode_root_grep_cursor(
    cursor: &str,
    expected_scope_digest: [u8; 32],
) -> Result<String, agent::BackendError> {
    let payload = decode_cursor("grep-root", cursor)?;
    let (scope_digest, search_cursor) = payload
        .split_first_chunk::<32>()
        .ok_or_else(|| invalid_backend_input("root grep cursor omits its scope digest"))?;
    if scope_digest != &expected_scope_digest {
        return Err(invalid_backend_input(
            "root grep cursor belongs to a different root or query",
        ));
    }
    if search_cursor.is_empty() {
        return Err(invalid_backend_input(
            "root grep cursor omits its search continuation",
        ));
    }
    String::from_utf8(search_cursor.to_vec())
        .map_err(|error| invalid_state("root grep cursor is not valid UTF-8", error))
}

fn encode_grep_cursor(
    fence: &wire::WorkspaceContinuationFence,
    scope_digest: [u8; 32],
    server_cursor: &[u8],
) -> String {
    let mut encoded = Vec::with_capacity(
        GREP_CURSOR_VERSION.len() + 16 + 8 + scope_digest.len() + server_cursor.len(),
    );
    encoded.extend_from_slice(GREP_CURSOR_VERSION);
    encoded.extend_from_slice(&fence.workspace_incarnation_id.0);
    encoded.extend_from_slice(&fence.workspace_revision.to_be_bytes());
    encoded.extend_from_slice(&scope_digest);
    encoded.extend_from_slice(server_cursor);
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_grep_cursor(
    cursor: &str,
    expected_scope_digest: [u8; 32],
) -> Result<GrepCursor, agent::BackendError> {
    if cursor.len() > GREP_CURSOR_MAX_ENCODED_BYTES {
        return Err(invalid_backend_input(
            "grep cursor exceeds the maximum encoded path cursor length",
        ));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| invalid_state("grep cursor is not canonical base64url", error))?;
    let payload = decoded.strip_prefix(GREP_CURSOR_VERSION).ok_or_else(|| {
        invalid_backend_input("grep cursor does not use the current scope-bound schema")
    })?;
    let (workspace_incarnation_id, payload) = payload
        .split_first_chunk::<16>()
        .ok_or_else(|| invalid_backend_input("grep cursor omits its workspace incarnation"))?;
    let (workspace_revision, payload) = payload
        .split_first_chunk::<8>()
        .ok_or_else(|| invalid_backend_input("grep cursor omits its workspace revision"))?;
    let (scope_digest, server_cursor) = payload
        .split_first_chunk::<32>()
        .ok_or_else(|| invalid_backend_input("grep cursor omits its scope digest"))?;
    if scope_digest != &expected_scope_digest {
        return Err(invalid_backend_input(
            "grep cursor belongs to a different workbench, prefix, or recursion mode",
        ));
    }
    if server_cursor.is_empty() {
        return Err(invalid_backend_input("grep cursor omits its server anchor"));
    }
    Ok(GrepCursor {
        fence: wire::WorkspaceContinuationFence {
            workspace_incarnation_id: wire::WorkspaceIdentity(*workspace_incarnation_id),
            workspace_revision: u64::from_be_bytes(*workspace_revision),
        },
        server_cursor: server_cursor.to_vec(),
    })
}

fn list_scope_digest(
    root_id: wire::RootIdentity,
    workbench_id: &WorkbenchId,
    prefix: Option<&NormalizedRelativePath>,
    view: &agent::ReadView,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workspace.list-scope.v2\0");
    hasher.update(root_id.0);
    hash_len64(&mut hasher, workbench_id.as_bytes());
    match prefix {
        None => hasher.update([0]),
        Some(prefix) => {
            hasher.update([1]);
            hash_len64(&mut hasher, prefix.as_str().as_bytes());
        }
    }
    match view {
        agent::ReadView::Live => hasher.update([0]),
        agent::ReadView::Snapshot(agent::SnapshotSelector::Id(snapshot_id)) => {
            hasher.update([1]);
            hasher.update(snapshot_id.to_be_bytes());
        }
        agent::ReadView::Snapshot(agent::SnapshotSelector::Name(alias)) => {
            hasher.update([2]);
            hash_len64(&mut hasher, alias.as_bytes());
        }
    }
    hasher.finalize().into()
}

fn encode_list_cursor(
    fence: &ListContinuationFence,
    scope_digest: [u8; 32],
    anchor: &NormalizedRelativePath,
) -> String {
    let mut encoded = Vec::with_capacity(
        LIST_CURSOR_VERSION.len() + 1 + 24 + scope_digest.len() + anchor.as_str().len(),
    );
    encoded.extend_from_slice(LIST_CURSOR_VERSION);
    match fence {
        ListContinuationFence::ReadVersion(read_version) => {
            encoded.push(0);
            encoded.extend_from_slice(&read_version.to_be_bytes());
        }
        ListContinuationFence::Workspace(fence) => {
            encoded.push(1);
            encoded.extend_from_slice(&fence.workspace_incarnation_id.0);
            encoded.extend_from_slice(&fence.workspace_revision.to_be_bytes());
        }
    }
    encoded.extend_from_slice(&scope_digest);
    encoded.extend_from_slice(anchor.as_str().as_bytes());
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_list_cursor(
    cursor: &str,
    expected_scope_digest: [u8; 32],
) -> Result<ListCursor, agent::BackendError> {
    if cursor.len() > LIST_CURSOR_MAX_ENCODED_BYTES {
        return Err(invalid_backend_input(
            "list cursor exceeds the maximum encoded path cursor length",
        ));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| invalid_state("list cursor is not canonical base64url", error))?;
    let payload = decoded.strip_prefix(LIST_CURSOR_VERSION).ok_or_else(|| {
        invalid_backend_input("list cursor does not use the current scope-bound schema")
    })?;
    let (&kind, payload) = payload
        .split_first()
        .ok_or_else(|| invalid_backend_input("list cursor omits its fence kind"))?;
    let (fence, payload) = match kind {
        0 => {
            let (read_version, payload) = payload
                .split_first_chunk::<8>()
                .ok_or_else(|| invalid_backend_input("list cursor omits its read version"))?;
            let read_version = u64::from_be_bytes(*read_version);
            if read_version == 0 {
                return Err(invalid_backend_input(
                    "list cursor read version must be greater than zero",
                ));
            }
            (ListContinuationFence::ReadVersion(read_version), payload)
        }
        1 => {
            let (workspace_incarnation_id, payload) =
                payload.split_first_chunk::<16>().ok_or_else(|| {
                    invalid_backend_input("list cursor omits its workspace incarnation")
                })?;
            let (workspace_revision, payload) = payload
                .split_first_chunk::<8>()
                .ok_or_else(|| invalid_backend_input("list cursor omits its workspace revision"))?;
            (
                ListContinuationFence::Workspace(wire::WorkspaceContinuationFence {
                    workspace_incarnation_id: wire::WorkspaceIdentity(*workspace_incarnation_id),
                    workspace_revision: u64::from_be_bytes(*workspace_revision),
                }),
                payload,
            )
        }
        _ => {
            return Err(invalid_backend_input(
                "list cursor has an unknown fence kind",
            ))
        }
    };
    let (scope_digest, anchor) = payload
        .split_first_chunk::<32>()
        .ok_or_else(|| invalid_backend_input("list cursor omits its scope digest"))?;
    if scope_digest != &expected_scope_digest {
        return Err(invalid_backend_input(
            "list cursor belongs to a different workbench, prefix, or read view",
        ));
    }
    let anchor = String::from_utf8(anchor.to_vec())
        .map_err(|error| invalid_state("list cursor anchor is not valid UTF-8", error))?;
    let anchor = NormalizedRelativePath::new(anchor).map_err(domain_input)?;
    Ok(ListCursor { fence, anchor })
}

fn encode_cursor(kind: &str, payload: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(CURSOR_VERSION.len() + kind.len() + 1 + payload.len());
    encoded.extend_from_slice(CURSOR_VERSION);
    encoded.extend_from_slice(kind.as_bytes());
    encoded.push(0);
    encoded.extend_from_slice(payload);
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_cursor(kind: &str, cursor: &str) -> Result<Vec<u8>, agent::BackendError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| invalid_state("cursor is not canonical base64url", error))?;
    let expected = [CURSOR_VERSION, kind.as_bytes(), &[0]].concat();
    decoded
        .strip_prefix(expected.as_slice())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_backend_input("cursor belongs to a different operation"))
}

fn page_limit(limit: usize) -> Result<u32, agent::BackendError> {
    let limit = u32::try_from(limit)
        .map_err(|_| invalid_backend_input("page limit does not fit the protocol"))?;
    if !(1..=wire::PageRequest::MAX_LIMIT).contains(&limit) {
        return Err(invalid_backend_input(format!(
            "page limit must be between 1 and {}",
            wire::PageRequest::MAX_LIMIT
        )));
    }
    Ok(limit)
}

fn query_page_limit(limit: usize) -> Result<u32, agent::BackendError> {
    let limit = page_limit(limit)?;
    if limit > wire::MAX_QUERY_PAGE_LIMIT {
        return Err(invalid_backend_input(format!(
            "query page limit must be between 1 and {}",
            wire::MAX_QUERY_PAGE_LIMIT
        )));
    }
    Ok(limit)
}

fn list_server_page_limit(limit: usize, candidate_count: usize, first_page: bool) -> u32 {
    let union_target = limit.saturating_add(1);
    let requested = if first_page {
        union_target
    } else {
        union_target.saturating_sub(candidate_count).max(1)
    };
    u32::try_from(requested.min(wire::PageRequest::MAX_LIMIT as usize))
        .expect("protocol page limit fits u32")
}

fn find_request_matches_canonical_manifest(
    request: &agent::FindRequest,
    canonical_manifest: Option<&[u8]>,
) -> bool {
    let Some(pattern) = request.manifest_pattern.as_deref() else {
        return true;
    };
    let Some(bytes) = canonical_manifest else {
        return false;
    };
    if pattern.is_empty() {
        return true;
    }
    bytes
        .windows(pattern.len())
        .any(|window| window.eq_ignore_ascii_case(pattern.as_bytes()))
}

fn digest_prefix(digest: [u8; 32]) -> [u8; 16] {
    digest[..16]
        .try_into()
        .expect("SHA-256 prefix has fixed width")
}

fn hash_len64(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn rpc_failure(error: &ClientError) -> Option<&wire::RpcFailure> {
    match error {
        ClientError::Rpc(failure) => Some(failure),
        ClientError::ArtifactPublishFailed { source, .. } => rpc_failure(source),
        ClientError::RetryExhausted { last_error, .. } => rpc_failure(last_error),
        _ => None,
    }
}

fn rpc_code(error: &ClientError) -> Option<wire::ErrorCode> {
    rpc_failure(error).map(|failure| failure.code)
}

fn is_read_version_conflict(error: &ClientError) -> bool {
    rpc_failure(error).is_some_and(|failure| {
        failure.code == wire::ErrorCode::PreconditionFailed
            && failure.conflict == Some(wire::ConflictKind::ReadVersion)
    })
}

fn map_read_version_fenced_error(error: ClientError) -> agent::BackendError {
    if is_read_version_conflict(&error) {
        return agent::BackendError::new(
            agent::BackendErrorKind::ReadFenceChanged,
            error.to_string(),
            true,
            json!({"source": "nokv-rpc", "conflict": "ReadVersion"}),
        );
    }
    map_client_error(error)
}

fn is_continuation_fence_conflict(error: &ClientError) -> bool {
    rpc_failure(error).is_some_and(|failure| {
        failure.code == wire::ErrorCode::PreconditionFailed
            && matches!(
                failure.conflict,
                Some(wire::ConflictKind::ReadVersion | wire::ConflictKind::Workspace)
            )
    })
}

fn map_read_enumeration_error(error: ClientError, operation: &'static str) -> agent::BackendError {
    if is_continuation_fence_conflict(&error) {
        let conflict = rpc_failure(&error)
            .and_then(|failure| failure.conflict)
            .map_or("Unknown", |conflict| match conflict {
                wire::ConflictKind::ReadVersion => "ReadVersion",
                wire::ConflictKind::Workspace => "Workspace",
                _ => "Other",
            });
        return agent::BackendError::new(
            agent::BackendErrorKind::ReadFenceChanged,
            format!("{operation} fence changed"),
            true,
            json!({"source": "nokv-rpc", "conflict": conflict}),
        );
    }
    map_client_error(error)
}

fn grep_candidate_read_fence_changed(path: &agent::ScopedPath) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::ReadFenceChanged,
        "grep candidate changed before its immutable body could be read",
        true,
        json!({"path": path.logical_path()}),
    )
}

fn map_grep_candidate_read_error(
    error: ClientError,
    path: &agent::ScopedPath,
) -> agent::BackendError {
    if matches!(&error, ClientError::ArtifactReadFenceChanged)
        || rpc_code(&error) == Some(wire::ErrorCode::NotFound)
        || matches!(
            &error,
            ClientError::RetryExhausted { last_error, .. }
                if matches!(last_error.as_ref(), ClientError::ArtifactReadFenceChanged)
        )
    {
        return grep_candidate_read_fence_changed(path);
    }
    map_client_error(error)
}

fn map_manifest_read_error(error: ClientError, path: &agent::ScopedPath) -> agent::BackendError {
    if matches!(&error, ClientError::ArtifactReadFenceChanged)
        || rpc_code(&error) == Some(wire::ErrorCode::NotFound)
        || matches!(
            &error,
            ClientError::RetryExhausted { last_error, .. }
                if matches!(last_error.as_ref(), ClientError::ArtifactReadFenceChanged)
        )
    {
        return agent::BackendError::new(
            agent::BackendErrorKind::ReadFenceChanged,
            "run manifest changed after workbench discovery",
            true,
            json!({"path": path.logical_path()}),
        );
    }
    map_client_error(error)
}

fn map_client_error(error: ClientError) -> agent::BackendError {
    if let Some(failure) = rpc_failure(&error).cloned() {
        let attempts = retry_attempts(&error);
        let mut mapped = map_rpc_failure(failure);
        if let Some(attempts) = attempts {
            if let Value::Object(details) = &mut mapped.details {
                details.insert("attempts".to_owned(), json!(attempts));
            }
            if mapped.kind == agent::BackendErrorKind::Conflict {
                mapped.retryable = true;
            }
        }
        return mapped;
    }
    let is_object_failure = object_failure(&error);
    let is_artifact_integrity_failure = artifact_integrity_failure(&error);
    let kind = if is_object_failure {
        agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
    } else if is_artifact_integrity_failure {
        agent::BackendErrorKind::InvalidState
    } else {
        match &error {
            ClientError::Protocol(_)
            | ClientError::ResponseMismatch(_)
            | ClientError::MissingCapabilities(_)
            | ClientError::ArtifactIntegrity(_)
            | ClientError::ArtifactReadFenceChanged => agent::BackendErrorKind::InvalidState,
            ClientError::Transport(_) => agent::BackendErrorKind::Other("Transport".to_owned()),
            ClientError::Object(_) | ClientError::ArtifactUpload(_) => unreachable!(),
            ClientError::InvalidOptions(_) | ClientError::InvalidRoute(_) => {
                agent::BackendErrorKind::InvalidState
            }
            ClientError::ArtifactPublishFailed { .. } | ClientError::RetryExhausted { .. } => {
                agent::BackendErrorKind::Other("ClientFailure".to_owned())
            }
            ClientError::Rpc(_) => unreachable!("RPC failures returned above"),
        }
    };
    let message = if is_object_failure {
        "artifact object operation failed".to_owned()
    } else if is_artifact_integrity_failure {
        "artifact integrity verification failed".to_owned()
    } else {
        error.to_string()
    };
    let mut mapped = agent::BackendError::new(
        kind,
        message,
        error.retryable(),
        json!({"source": "nokv-client"}),
    );
    if let Some(attempts) = retry_attempts(&error) {
        if let Value::Object(details) = &mut mapped.details {
            details.insert("attempts".to_owned(), json!(attempts));
        }
    }
    mapped
}

fn object_failure(error: &ClientError) -> bool {
    match error {
        ClientError::Object(_) | ClientError::ArtifactUpload(_) => true,
        ClientError::ArtifactPublishFailed { source, .. } => object_failure(source),
        ClientError::RetryExhausted { last_error, .. } => object_failure(last_error),
        _ => false,
    }
}

fn artifact_integrity_failure(error: &ClientError) -> bool {
    match error {
        ClientError::ArtifactIntegrity(_) => true,
        ClientError::ArtifactPublishFailed { source, .. } => artifact_integrity_failure(source),
        ClientError::RetryExhausted { last_error, .. } => artifact_integrity_failure(last_error),
        _ => false,
    }
}

fn retry_attempts(error: &ClientError) -> Option<u32> {
    match error {
        ClientError::RetryExhausted { attempts, .. } => Some(*attempts),
        ClientError::ArtifactPublishFailed { source, .. } => retry_attempts(source),
        _ => None,
    }
}

fn map_snapshot_client_error(error: ClientError) -> agent::BackendError {
    if rpc_code(&error) == Some(wire::ErrorCode::NotFound) {
        return agent::BackendError::new(
            agent::BackendErrorKind::SnapshotNotFound,
            error.to_string(),
            false,
            json!({"source": "nokv-rpc"}),
        );
    }
    map_client_error(error)
}

fn map_lifecycle_error(
    error: WorkbenchLifecycleError<agent::ProjectionError>,
) -> agent::BackendError {
    match error {
        WorkbenchLifecycleError::Client(error) => map_client_error(error),
        WorkbenchLifecycleError::SnapshotLookup(error) => map_snapshot_client_error(error),
        WorkbenchLifecycleError::ProtocolInput(error) => protocol_input(error),
        WorkbenchLifecycleError::ProtocolMismatch(message) => protocol_mismatch(message),
        WorkbenchLifecycleError::ProjectionInvalid { context, source } => {
            invalid_state(context, source)
        }
        WorkbenchLifecycleError::Conflict(message) => agent::BackendError::conflict(message),
        error @ WorkbenchLifecycleError::ResourceExhausted { .. } => {
            resource_exhausted(error.to_string())
        }
        WorkbenchLifecycleError::RestoreWorkflow(error) => {
            if matches!(
                error.as_ref(),
                nokv_client::RestoreWorkflowError::Prepare(source)
                    if rpc_failure(source).is_some_and(|failure| {
                        failure.code == wire::ErrorCode::NotFound
                            && failure.conflict == Some(wire::ConflictKind::SnapshotLifecycle)
                    })
            ) {
                return agent::BackendError::new(
                    agent::BackendErrorKind::SnapshotNotFound,
                    error.to_string(),
                    false,
                    json!({"source": "nokv-rpc"}),
                );
            }
            map_client_error((*error).into_client_error())
        }
    }
}

fn map_rpc_failure(failure: wire::RpcFailure) -> agent::BackendError {
    let kind = match failure.code {
        wire::ErrorCode::NotFound => agent::BackendErrorKind::NotFound,
        wire::ErrorCode::AlreadyExists => agent::BackendErrorKind::AlreadyExists,
        wire::ErrorCode::Conflict => agent::BackendErrorKind::Conflict,
        wire::ErrorCode::SnapshotExpired | wire::ErrorCode::SnapshotReaped => {
            agent::BackendErrorKind::SnapshotExpired
        }
        wire::ErrorCode::PreconditionFailed
            if failure.conflict == Some(wire::ConflictKind::SnapshotLifecycle)
                && failure.message.contains("active fork consumers") =>
        {
            agent::BackendErrorKind::ForkRetentionActive
        }
        wire::ErrorCode::InvalidArgument
        | wire::ErrorCode::PreconditionFailed
        | wire::ErrorCode::RequestReplayMismatch
        | wire::ErrorCode::CommitRetiring
        | wire::ErrorCode::OperationFailed
        | wire::ErrorCode::Quarantined => agent::BackendErrorKind::InvalidState,
        code => agent::BackendErrorKind::Other(error_code_name(code).to_owned()),
    };
    agent::BackendError::new(
        kind,
        failure.message,
        failure.retryable,
        json!({
            "source": "nokv-rpc",
            "code": error_code_name(failure.code),
            "conflict": failure.conflict.map(|value| format!("{value:?}")),
            "current_generation": failure.current_generation,
        }),
    )
}

fn error_code_name(code: wire::ErrorCode) -> &'static str {
    match code {
        wire::ErrorCode::InvalidArgument => "InvalidArgument",
        wire::ErrorCode::NotFound => "NotFound",
        wire::ErrorCode::AlreadyExists => "AlreadyExists",
        wire::ErrorCode::Conflict => "Conflict",
        wire::ErrorCode::PreconditionFailed => "PreconditionFailed",
        wire::ErrorCode::RequestReplayMismatch => "RequestReplayMismatch",
        wire::ErrorCode::NotOwner => "NotOwner",
        wire::ErrorCode::SnapshotExpired => "SnapshotExpired",
        wire::ErrorCode::SnapshotReaped => "SnapshotReaped",
        wire::ErrorCode::CommitRetiring => "CommitRetiring",
        wire::ErrorCode::OperationFailed => "OperationFailed",
        wire::ErrorCode::ObjectUnavailable => "ObjectUnavailable",
        wire::ErrorCode::Quarantined => "Quarantined",
        wire::ErrorCode::ResourceExhausted => "ResourceExhausted",
        wire::ErrorCode::Internal => "Internal",
    }
}

fn protocol_input(error: wire::ProtocolError) -> agent::BackendError {
    invalid_state("typed protocol input was rejected", error)
}

fn domain_input(error: impl ToString) -> agent::BackendError {
    invalid_backend_input(error.to_string())
}

fn invalid_backend_input(message: impl Into<String>) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::InvalidState,
        message,
        false,
        json!({"source": "workbench-backend"}),
    )
}

fn invalid_state(message: &str, error: impl ToString) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::InvalidState,
        format!("{message}: {}", error.to_string()),
        false,
        json!({"source": "workbench-backend"}),
    )
}

fn protocol_mismatch(message: impl Into<String>) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::InvalidState,
        message,
        false,
        json!({"source": "nokv-protocol"}),
    )
}

fn resource_exhausted(message: impl Into<String>) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::Other("ResourceExhausted".to_owned()),
        message,
        false,
        json!({"source": "workbench-backend"}),
    )
}

fn not_directory_backend(path: &agent::ScopedPath) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::Other("NotDirectory".to_owned()),
        "an exact artifact cannot be listed as a directory",
        false,
        json!({"path": path.logical_path()}),
    )
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::thread;

    use nokv_client::{
        ClientOptions, CommitWorkflowIdentities, FramedTcpOptions, FramedTcpTransport,
        RestoreWorkflowError, RestoreWorkflowIdentities, RouteResolver, StaticRouteResolver,
        WorkspaceClient,
    };
    use nokv_object::{
        ensure_object_namespace, plan_artifact_upload, upload_artifact_from_plan,
        ArtifactUploadOptions, BoundArtifactStore, MemoryArtifactStore,
    };
    use nokv_types::{ArtifactRevisionId, LogicalShardId, ObjectNamespaceId};

    use super::*;
    use crate::encode_lowercase_hex;

    fn test_route() -> wire::RootRoute {
        wire::RootRoute {
            root_id: wire::RootIdentity([1; 16]),
            logical_shard_id: wire::LogicalShardIdentity([2; 16]),
            object_namespace_id: wire::ObjectNamespaceIdentity([8; 16]),
            placement_generation: 3,
            owner_epoch: 4,
        }
    }

    fn read_version_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::PreconditionFailed,
            message: "read version changed".to_owned(),
            retryable: false,
            conflict: Some(wire::ConflictKind::ReadVersion),
            current_generation: None,
            route_hint: None,
        })
    }

    fn workspace_fence_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::PreconditionFailed,
            message: "workspace revision changed".to_owned(),
            retryable: false,
            conflict: Some(wire::ConflictKind::Workspace),
            current_generation: None,
            route_hint: None,
        })
    }

    fn path_generation_precondition_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::PreconditionFailed,
            message: "path generation changed".to_owned(),
            retryable: false,
            conflict: Some(wire::ConflictKind::PathGeneration),
            current_generation: Some(2),
            route_hint: None,
        })
    }

    fn not_found_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::NotFound,
            message: "missing".to_owned(),
            retryable: false,
            conflict: None,
            current_generation: None,
            route_hint: None,
        })
    }

    fn already_exists_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::AlreadyExists,
            message: "workbench already exists".to_owned(),
            retryable: false,
            conflict: Some(wire::ConflictKind::Workspace),
            current_generation: None,
            route_hint: None,
        })
    }

    fn success(result: wire::WorkspaceResult) -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Success(Box::new(result))
    }

    fn aggregate_count_result(field_id: &str, value: u64) -> wire::WorkspaceResult {
        wire::WorkspaceResult::Aggregate(wire::AggregateResult {
            groups: vec![wire::AggregateGroup {
                keys: Vec::new(),
                values: vec![wire::FieldValue {
                    field_id: field_id.to_owned(),
                    value: wire::ScalarValue::Unsigned(value),
                }],
            }],
            input_match_count: value,
            row_count: value,
            group_count: 1,
            next_cursor: None,
            read_version: 41,
        })
    }

    fn scripted_backend(
        outcomes: Vec<wire::WorkspaceRpcOutcome>,
    ) -> (
        CliWorkbenchBackend,
        Arc<Mutex<Vec<wire::WorkspaceRpcRequest>>>,
        thread::JoinHandle<()>,
    ) {
        let objects: Arc<dyn ArtifactObjectStore> = Arc::new(
            CliObjectStore::build(&crate::cli::ObjectConfig {
                bucket: Some("unused-test-bucket".to_owned()),
                ..crate::cli::ObjectConfig::default()
            })
            .unwrap(),
        );
        scripted_backend_with_objects(outcomes, objects, 1024)
    }

    fn scripted_backend_with_objects(
        outcomes: Vec<wire::WorkspaceRpcOutcome>,
        objects: Arc<dyn ArtifactObjectStore>,
        max_artifact_bytes: usize,
    ) -> (
        CliWorkbenchBackend,
        Arc<Mutex<Vec<wire::WorkspaceRpcRequest>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let server = thread::spawn(move || {
            for outcome in outcomes {
                let (mut stream, _) = listener.accept().unwrap();
                let mut handshake = [0_u8; wire::HANDSHAKE_FRAME_BYTES];
                stream.read_exact(&mut handshake).unwrap();
                let handshake = wire::decode_handshake_frame(&handshake).unwrap();
                assert_eq!(handshake.kind(), wire::HandshakeKind::ClientHello);
                assert_eq!(
                    handshake.operation_schema(),
                    wire::WORKSPACE_PROTOCOL_SCHEMA
                );
                let accepted = wire::WorkspaceHandshake::new(
                    wire::HandshakeKind::Accepted,
                    wire::WORKSPACE_PROTOCOL_SCHEMA,
                )
                .unwrap();
                stream
                    .write_all(&wire::encode_handshake_frame(&accepted).unwrap())
                    .unwrap();
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).unwrap();
                let mut encoded = vec![0_u8; u32::from_be_bytes(length) as usize];
                stream.read_exact(&mut encoded).unwrap();
                let request = wire::decode_request(&encoded).unwrap();
                let response = wire::WorkspaceRpcResponse {
                    route: request.route,
                    request_id: request.request_id,
                    commit_version: None,
                    replayed: false,
                    outcome,
                };
                captured.lock().unwrap().push(request);
                let encoded = wire::encode_response(&response).unwrap();
                stream
                    .write_all(&u32::try_from(encoded.len()).unwrap().to_be_bytes())
                    .unwrap();
                stream.write_all(&encoded).unwrap();
                stream.flush().unwrap();
            }
        });

        let route = test_route();
        let resolver: Arc<dyn RouteResolver> =
            Arc::new(StaticRouteResolver::new(route, endpoint).unwrap());
        let client = WorkspaceClient::new(
            route.root_id,
            FramedTcpTransport::new(FramedTcpOptions::default()).unwrap(),
            resolver,
            ClientOptions { max_attempts: 1 },
        )
        .unwrap();
        (
            CliWorkbenchBackend {
                client,
                objects,
                max_artifact_bytes,
            },
            requests,
            server,
        )
    }

    fn list_request(path: agent::ScopedPath, cursor: Option<String>) -> agent::ListRequest {
        agent::ListRequest {
            path,
            view: agent::ReadView::Live,
            cursor,
            limit: 1,
        }
    }

    fn scoped_root() -> agent::ScopedPath {
        agent::ScopedPath {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            section: None,
            relative_path: None,
        }
    }

    fn scoped_outputs() -> agent::ScopedPath {
        agent::ScopedPath {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            section: Some(agent::Section::Outputs),
            relative_path: None,
        }
    }

    fn wire_prefix(path: &str) -> wire::PathListEntry {
        wire_prefix_for("run-42", path)
    }

    fn wire_prefix_for(workbench: &str, path: &str) -> wire::PathListEntry {
        wire::PathListEntry::Prefix(wire::WorkspacePath {
            workbench: wire::WorkbenchName::new(workbench).unwrap(),
            path: wire::RelativePath::new(path).unwrap(),
        })
    }

    fn wire_artifact(path: &str) -> wire::PathListEntry {
        wire::PathListEntry::Artifact(wire::PathMetadata {
            path: wire::WorkspacePath {
                workbench: wire::WorkbenchName::new("run-42").unwrap(),
                path: wire::RelativePath::new(path).unwrap(),
            },
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            workspace_revision: 1,
            generation: 1,
            artifact_revision_id: wire::ArtifactRevisionIdentity([6; 16]),
            dependency_count: 0,
            dependency_depth: 0,
            descriptor: wire::ArtifactDescriptor {
                logical_size: 1,
                body_digest: wire::DigestUri::new(format!("sha256:{}", "07".repeat(32))).unwrap(),
                manifest_digest: wire::DigestUri::new(format!("sha256:{}", "08".repeat(32)))
                    .unwrap(),
                content_type: wire::ContentType::new("application/octet-stream").unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        })
    }

    fn wire_search_hit(path: &str) -> wire::SearchRow {
        let wire::PathListEntry::Artifact(metadata) = wire_artifact(path) else {
            unreachable!("wire_artifact always returns artifact metadata")
        };
        wire::SearchRow::Artifact(wire::SearchHit {
            metadata,
            projection: Vec::new(),
        })
    }

    fn wire_path_result(metadata: wire::PathMetadata) -> wire::PathReadResult {
        wire::PathReadResult {
            not_modified: false,
            metadata: Some(metadata),
            range: None,
            blocks: Vec::new(),
            next_cursor: None,
        }
    }

    fn workspace_summary(workbench: &str, incarnation: [u8; 16]) -> wire::WorkspaceSummary {
        wire::WorkspaceSummary {
            workbench: wire::WorkbenchName::new(workbench).unwrap(),
            workspace_incarnation_id: wire::WorkspaceIdentity(incarnation),
            workspace_revision: 0,
            commit_head: None,
            commit_head_generation: None,
        }
    }

    #[derive(Clone)]
    struct ManifestArtifactFixture {
        objects: Arc<dyn ArtifactObjectStore>,
        metadata_only: wire::PathReadResult,
        range: wire::PathReadResult,
    }

    fn manifest_artifact_fixture(
        workbench: &str,
        path: &str,
        workspace_incarnation_id: wire::WorkspaceIdentity,
        artifact_revision_id: wire::ArtifactRevisionIdentity,
        content_type: &str,
        bytes: &[u8],
    ) -> ManifestArtifactFixture {
        let route = test_route();
        let raw_store = MemoryArtifactStore::new();
        let namespace_id = ObjectNamespaceId::from(route.object_namespace_id);
        ensure_object_namespace(&raw_store, namespace_id).unwrap();
        let upload_plan = plan_artifact_upload(
            ArtifactUploadOptions::new(
                LogicalShardId::from(route.logical_shard_id),
                RootId::from(route.root_id),
                ArtifactRevisionId::from(artifact_revision_id),
            )
            .with_block_size(1024),
            bytes,
        )
        .unwrap();
        upload_artifact_from_plan(&raw_store, &upload_plan, bytes).unwrap();
        let rows = upload_plan
            .manifest
            .blocks
            .iter()
            .map(|block| wire::ArtifactManifestRow {
                object_index: block.object_index,
                physical_object_index: block.object_index,
                logical_offset: block.logical_offset,
                physical_owner_revision_id: artifact_revision_id,
                object_identity: wire::ObjectIdentity::new(block.key.as_str().to_owned()).unwrap(),
                object_offset: 0,
                length: block.len,
                digest: wire::sha256_digest_uri(wire::Digest(block.sha256)),
                append_segment: None,
            })
            .collect::<Vec<_>>();
        let manifest_seal = wire::seal_artifact_publish_plan(artifact_revision_id, &[], &rows)
            .unwrap()
            .manifest_seal;
        let metadata = wire::PathMetadata {
            path: wire::WorkspacePath {
                workbench: wire::WorkbenchName::new(workbench).unwrap(),
                path: wire::RelativePath::new(path).unwrap(),
            },
            workspace_incarnation_id,
            workspace_revision: 1,
            generation: 1,
            artifact_revision_id,
            dependency_count: 0,
            dependency_depth: 0,
            descriptor: wire::ArtifactDescriptor {
                logical_size: bytes.len() as u64,
                body_digest: digest_uri(bytes),
                manifest_digest: wire::sha256_digest_uri(manifest_seal),
                content_type: wire::ContentType::new(content_type).unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        };
        let metadata_only = wire::PathReadResult {
            not_modified: false,
            metadata: Some(metadata.clone()),
            range: None,
            blocks: Vec::new(),
            next_cursor: None,
        };
        let range = wire::PathReadResult {
            not_modified: false,
            metadata: Some(metadata),
            range: Some(wire::ByteRange {
                offset: 0,
                length: bytes.len() as u64,
            }),
            blocks: rows,
            next_cursor: None,
        };
        let objects: Arc<dyn ArtifactObjectStore> = Arc::new(
            BoundArtifactStore::open(raw_store, namespace_id).expect("bind in-memory namespace"),
        );
        ManifestArtifactFixture {
            objects,
            metadata_only,
            range,
        }
    }

    fn manifest_read_outcomes(fixture: &ManifestArtifactFixture) -> Vec<wire::WorkspaceRpcOutcome> {
        vec![
            success(wire::WorkspaceResult::Path(fixture.metadata_only.clone())),
            success(wire::WorkspaceResult::Path(fixture.metadata_only.clone())),
            success(wire::WorkspaceResult::Path(fixture.range.clone())),
        ]
    }

    fn publish_request(condition: agent::PublishCondition) -> agent::PublishRequest {
        agent::PublishRequest {
            path: scoped_full_path(
                &WorkbenchId::new("implicit-publish").unwrap(),
                "outputs/result.json",
            )
            .unwrap(),
            body: br#"{"status":"ok"}"#.to_vec(),
            content_type: JSON_CONTENT_TYPE.to_owned(),
            condition,
        }
    }

    fn append_request() -> agent::AppendRequest {
        agent::AppendRequest {
            path: scoped_full_path(
                &WorkbenchId::new("implicit-append").unwrap(),
                "logs/events.jsonl",
            )
            .unwrap(),
            delta: b"{}\n".to_vec(),
            content_type: None,
            create_content_type: "application/jsonl".to_owned(),
            max_logical_size: None,
        }
    }

    fn catalog_field(field_id: &str) -> wire::CatalogField {
        wire::CatalogField {
            field_id: field_id.to_owned(),
            scalar_type: "string".to_owned(),
            scalar_types: Vec::new(),
            generic_custom: false,
            operators: vec![wire::QueryOperator::Equal],
            sortable: true,
            facetable: true,
            aggregatable: false,
        }
    }

    fn commit_request() -> agent::CommitRequest {
        let workbench_id = WorkbenchId::new("clock-stable-run").unwrap();
        let canonical_manifest = agent::canonical_json_bytes(&json!({
            "model": "viking",
            "steps": [1, 2],
        }))
        .unwrap();
        let manifest_digest_uri = format!(
            "sha256:{}",
            encode_lowercase_hex(&Sha256::digest(&canonical_manifest))
        );
        let content_digest_uri = format!("sha256:{}", "ab".repeat(32));
        let stable_commit_id = agent::workbench_commit_identity(
            &workbench_id,
            &content_digest_uri,
            &manifest_digest_uri,
        );
        agent::CommitRequest {
            workbench_id,
            canonical_manifest,
            workbench_path: "/agents/test/wb/clock-stable-run".to_owned(),
            content_digest_uri,
            manifest_digest_uri,
            stable_commit_id,
            replace: false,
        }
    }

    fn digest_uri(bytes: &[u8]) -> wire::DigestUri {
        wire::DigestUri::new(format!(
            "sha256:{}",
            encode_lowercase_hex(&Sha256::digest(bytes))
        ))
        .unwrap()
    }

    fn test_commit_envelope(
        request: &agent::CommitRequest,
        committed_at_unix_seconds: u64,
    ) -> Vec<u8> {
        agent::build_run_manifest_v1(
            &request.workbench_id,
            &request.workbench_path,
            &request.content_digest_uri,
            &request.canonical_manifest,
            &request.manifest_digest_uri,
            request.stable_commit_id,
            committed_at_unix_seconds,
        )
        .unwrap()
    }

    fn test_commit_projection_input_digest(request: &agent::CommitRequest) -> wire::Digest {
        wire::Digest(agent::run_manifest_projection_input_digest_v1(
            &request.workbench_id,
            &request.workbench_path,
            &request.content_digest_uri,
            &request.canonical_manifest,
            &request.manifest_digest_uri,
            request.stable_commit_id,
        ))
    }

    #[derive(Clone)]
    struct RestorePlanFixture {
        source_workbench_id: WorkbenchId,
        source_workbench_path: String,
        destination_workbench_id: WorkbenchId,
        destination_workbench_path: String,
        snapshot_id: u64,
        preparation: wire::RestorePreparation,
        restore_manifest_identity: wire::RestoreManifestIdentity,
        restore_manifest: Vec<u8>,
    }

    fn restore_plan_fixture(
        source_matches_base_commit: bool,
        materialized_member_digest: wire::Digest,
    ) -> RestorePlanFixture {
        let root_id = test_route().root_id;
        let source_workbench_id = WorkbenchId::new("source-run").unwrap();
        let source_workbench_path = "/agents/test/wb/source-run".to_owned();
        let destination_workbench_id = WorkbenchId::new("destination-run").unwrap();
        let destination_workbench_path = "/agents/test/wb/destination-run".to_owned();
        let source_manifest = agent::canonical_json_bytes(&json!({
            "model": "viking",
            "steps": [1, 2],
        }))
        .unwrap();
        let source_manifest_digest = digest_uri(&source_manifest);
        let source_content_digest =
            wire::DigestUri::new(format!("sha256:{}", "ab".repeat(32))).unwrap();
        let source_commit_identity = agent::workbench_commit_identity(
            &source_workbench_id,
            source_content_digest.as_str(),
            source_manifest_digest.as_str(),
        );
        let source_commit_id = wire::CommitIdentity(source_commit_identity);
        let source_workbench = wire::WorkbenchName::new(source_workbench_id.as_str()).unwrap();
        let destination_workbench =
            wire::WorkbenchName::new(destination_workbench_id.as_str()).unwrap();
        let source_incarnation = wire::WorkspaceIdentity([0x21; 16]);
        let snapshot_id = 7;
        let restore_identities = RestoreWorkflowIdentities::derive(
            root_id,
            &source_workbench,
            source_incarnation,
            WorkbenchRestoreSource::Snapshot { snapshot_id },
            &destination_workbench,
        );
        let restore_manifest = agent::build_restore_manifest_v2(
            restore_identities.operation_id.0,
            &source_workbench_id,
            &source_workbench_path,
            &destination_workbench_id,
            &destination_workbench_path,
            agent::RestoreManifestSource::Snapshot { snapshot_id },
        )
        .unwrap();
        let restore_digest = digest_uri(&restore_manifest);
        let restore_manifest_identities =
            restore_identities.manifest_identities(root_id, restore_digest.as_str());
        let restore_manifest_identity = wire::RestoreManifestIdentity {
            publication_operation_id: restore_manifest_identities.publish_operation_id,
            artifact_revision_id: restore_manifest_identities.revision_id,
        };
        let base_member_count = 3;
        let base_member_digest = wire::Digest([0x31; 32]);
        let (source_member_count, source_member_digest) = if source_matches_base_commit {
            (base_member_count, base_member_digest)
        } else {
            (base_member_count + 1, wire::Digest([0x32; 32]))
        };
        let preparation = wire::RestorePreparation {
            operation_id: restore_identities.operation_id,
            destination_workbench,
            destination_workspace_incarnation_id: restore_identities
                .destination_workspace_incarnation_id,
            source_commit: wire::RestoreSourceCommitBinding {
                commit_id: source_commit_id,
                content_digest: source_content_digest,
                manifest_digest: source_manifest_digest,
                tree_manifest_revision_id: CommitWorkflowIdentities::derive(
                    root_id,
                    source_commit_id,
                )
                .tree_manifest_revision_id,
                member_count: base_member_count,
                member_digest: base_member_digest,
            },
            destination_committed_at_unix_seconds: 1_800_000_000,
            source_member_count,
            source_member_digest,
            materialized_member_count: source_member_count - 1,
            materialized_member_digest,
            source_matches_base_commit,
            destination_binding: None,
        };
        RestorePlanFixture {
            source_workbench_id,
            source_workbench_path,
            destination_workbench_id,
            destination_workbench_path,
            snapshot_id,
            preparation,
            restore_manifest_identity,
            restore_manifest,
        }
    }

    fn prepare_restore_request(fixture: &RestorePlanFixture) -> wire::PrepareRestoreRequest {
        wire::PrepareRestoreRequest {
            operation_id: fixture.preparation.operation_id,
            source_workbench: wire::WorkbenchName::new(fixture.source_workbench_id.as_str())
                .unwrap(),
            source_workspace_incarnation_id: wire::WorkspaceIdentity([0x21; 16]),
            source: wire::RestoreSource::Snapshot(wire::SnapshotSelector::Id(fixture.snapshot_id)),
            destination_workbench: fixture.preparation.destination_workbench.clone(),
            destination_workspace_incarnation_id: fixture
                .preparation
                .destination_workspace_incarnation_id,
            destination_restore_manifest_identity: fixture.restore_manifest_identity,
            restore_manifest: wire::RestoreManifestDescriptor {
                body_digest: digest_uri(&fixture.restore_manifest),
                logical_size: fixture.restore_manifest.len() as u64,
                content_type: wire::ContentType::new(JSON_CONTENT_TYPE).unwrap(),
            },
        }
    }

    fn running_restore_status(fixture: &RestorePlanFixture) -> wire::OperationStatus {
        let request = prepare_restore_request(fixture);
        wire::OperationStatus {
            token: wire::OperationToken {
                operation_id: fixture.preparation.operation_id,
                state_digest: wire::Digest([0x61; 32]),
            },
            kind: wire::OperationKind::Restore,
            commit_preparation: None,
            restore_preparation: Some(Box::new(wire::RestoreOperationPreparation {
                request,
                source_snapshot_read_version: Some(41),
                source_commit: fixture.preparation.source_commit.clone(),
                destination_committed_at_unix_seconds: fixture
                    .preparation
                    .destination_committed_at_unix_seconds,
                source_member_count: Some(fixture.preparation.source_member_count),
                source_member_digest: Some(fixture.preparation.source_member_digest),
                materialized_member_count: Some(fixture.preparation.materialized_member_count),
                materialized_member_digest: Some(fixture.preparation.materialized_member_digest),
                source_matches_base_commit: Some(fixture.preparation.source_matches_base_commit),
                destination_binding: None,
            })),
            state: wire::OperationState::Running,
            progress: wire::OperationProgress {
                completed_rows: fixture.preparation.source_member_count,
                total_rows: Some(fixture.preparation.source_member_count),
                completed_bytes: 0,
                total_bytes: Some(0),
            },
            result: None,
            failure: None,
        }
    }

    fn assert_expired_snapshot_enters_hidden_restore_durable_replay(
        selector: agent::SnapshotSelector,
        alias: Option<wire::SnapshotAlias>,
    ) {
        let fixture = restore_plan_fixture(true, wire::Digest([0x41; 32]));
        let mut source_workspace = workspace_summary("source-run", [0x21; 16]);
        source_workspace.workspace_revision = 9;
        // The live source may advance after Begin. Recovery binds the frozen
        // numeric snapshot and incarnation, never the later live head.
        source_workspace.commit_head = Some(wire::CommitIdentity([0x99; 32]));
        source_workspace.commit_head_generation = Some(2);
        let expired_snapshot = wire::SnapshotResult {
            snapshot_id: fixture.snapshot_id,
            workbench: wire::WorkbenchName::new("source-run").unwrap(),
            workspace_incarnation_id: wire::WorkspaceIdentity([0x21; 16]),
            read_version: 41,
            lease_deadline_ms: 1,
            alias,
            annotation: b"null".to_vec(),
            retire_annotation: None,
            status: wire::SnapshotStatus::Expired,
            consumer_count: 1,
        };
        let running = running_restore_status(&fixture);
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(source_workspace)),
            success(wire::WorkspaceResult::Snapshot(expired_snapshot)),
            success(wire::WorkspaceResult::Operation(running.clone())),
            success(wire::WorkspaceResult::RestorePrepared(
                fixture.preparation.clone(),
            )),
            success(wire::WorkspaceResult::Operation(running)),
        ]);

        let error = agent::WorkbenchBackend::restore(
            &backend,
            agent::RestoreRequest {
                source_workbench_id: fixture.source_workbench_id.clone(),
                source_workbench_path: fixture.source_workbench_path.clone(),
                origin: agent::RestoreOrigin::Snapshot(selector),
                destination_workbench_id: fixture.destination_workbench_id.clone(),
                destination_workbench_path: fixture.destination_workbench_path.clone(),
            },
        )
        .unwrap_err();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 6, "unexpected requests: {requests:#?}");
        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("object store was not verified"));
        let wire::WorkspaceRequest::PrepareRestore(actual_prepare) = &requests[4].operation else {
            panic!("hidden recovery must replay the exact prepare request");
        };
        assert_eq!(actual_prepare, &prepare_restore_request(&fixture));
        assert!(matches!(
            requests[5].operation,
            wire::WorkspaceRequest::GetOperation(_)
        ));
        assert!(!requests.iter().any(|request| matches!(
            request.operation,
            wire::WorkspaceRequest::ReadRestoreSourceRunManifest(_)
                | wire::WorkspaceRequest::BindRestoreDestination(_)
                | wire::WorkspaceRequest::FinalizeRestore(_)
        )));
        drop(requests);
        server.join().unwrap();
    }

    #[test]
    fn expired_snapshot_id_enters_hidden_restore_durable_replay() {
        assert_expired_snapshot_enters_hidden_restore_durable_replay(
            agent::SnapshotSelector::Id(7),
            None,
        );
    }

    #[test]
    fn expired_snapshot_alias_resolves_to_hidden_restore_durable_replay() {
        assert_expired_snapshot_enters_hidden_restore_durable_replay(
            agent::SnapshotSelector::Name("checkpoint".to_owned()),
            Some(wire::SnapshotAlias::new("checkpoint").unwrap()),
        );
    }

    fn terminal_commit_replay_fixture(
        request: &agent::CommitRequest,
    ) -> (
        wire::OperationStatus,
        wire::OperationStatus,
        wire::CommitRequest,
        CommitWorkflowIdentities,
    ) {
        let commit_id = wire::CommitIdentity(request.stable_commit_id);
        let identities = CommitWorkflowIdentities::derive(test_route().root_id, commit_id);
        let workbench = wire::WorkbenchName::new(request.workbench_id.as_str()).unwrap();
        let manifest_target = wire::WorkspacePath {
            workbench: workbench.clone(),
            path: wire::RelativePath::new(RUN_MANIFEST_PATH).unwrap(),
        };
        let committed_at_unix_seconds = 1_700_000_123;
        let manifest_bytes = test_commit_envelope(request, committed_at_unix_seconds);
        let binding = wire::CommitManifestBinding {
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            artifact_revision_id: identities.tree_manifest_revision_id,
            descriptor: wire::ArtifactDescriptor {
                logical_size: manifest_bytes.len() as u64,
                body_digest: digest_uri(&manifest_bytes),
                manifest_digest: digest_uri(b"durable-manifest-plan"),
                content_type: wire::ContentType::new(JSON_CONTENT_TYPE).unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        };
        let exact_request = wire::CommitRequest {
            operation_id: identities.operation_id,
            workbench: workbench.clone(),
            workspace_incarnation_id: binding.workspace_incarnation_id,
            commit_id,
            content_digest: wire::DigestUri::new(request.content_digest_uri.clone()).unwrap(),
            manifest_digest: wire::DigestUri::new(request.manifest_digest_uri.clone()).unwrap(),
            projection_input_digest: test_commit_projection_input_digest(request),
            tree_manifest_revision_id: identities.tree_manifest_revision_id,
            replace: request.replace,
            run_manifest_condition: wire::PublishCondition::CreateOnly,
            expected_head_generation: None,
            parents: Vec::new(),
            producer: None,
            lineage_projection: Vec::new(),
        };
        let commit_status = wire::OperationStatus {
            token: wire::OperationToken {
                operation_id: identities.operation_id,
                state_digest: wire::Digest([0x61; 32]),
            },
            kind: wire::OperationKind::Commit,
            commit_preparation: Some(Box::new(wire::CommitPreparation {
                request: Box::new(exact_request.clone()),
                committed_at_unix_seconds,
                manifest: Some(binding.clone()),
            })),
            restore_preparation: None,
            state: wire::OperationState::Succeeded,
            progress: wire::OperationProgress {
                completed_rows: 3,
                total_rows: Some(3),
                completed_bytes: 0,
                total_bytes: None,
            },
            result: Some(wire::OperationResult::Commit(wire::CommitResult {
                operation_id: identities.operation_id,
                commit_id,
                workbench,
                head_generation: 1,
                member_count: 3,
                member_digest: wire::Digest([0x62; 32]),
            })),
            failure: None,
        };
        let publish_status = wire::OperationStatus {
            token: wire::OperationToken {
                operation_id: identities.manifest_publish_operation_id,
                state_digest: wire::Digest([0x63; 32]),
            },
            kind: wire::OperationKind::ArtifactPublish,
            commit_preparation: None,
            restore_preparation: None,
            state: wire::OperationState::Succeeded,
            progress: wire::OperationProgress {
                completed_rows: 1,
                total_rows: Some(1),
                completed_bytes: binding.descriptor.logical_size,
                total_bytes: Some(binding.descriptor.logical_size),
            },
            result: Some(wire::OperationResult::ArtifactPublish(
                wire::PublishResult {
                    operation_id: identities.manifest_publish_operation_id,
                    target: manifest_target,
                    workspace_revision: 1,
                    generation: 1,
                    artifact_revision_id: binding.artifact_revision_id,
                    logical_size: binding.descriptor.logical_size,
                    body_digest: binding.descriptor.body_digest,
                },
            )),
            failure: None,
        };
        (commit_status, publish_status, exact_request, identities)
    }

    fn running_commit_replay_fixture(
        request: &agent::CommitRequest,
    ) -> (
        wire::OperationStatus,
        wire::CommitRequest,
        CommitWorkflowIdentities,
    ) {
        let (mut status, _, exact_request, identities) = terminal_commit_replay_fixture(request);
        status.state = wire::OperationState::Running;
        status.result = None;
        status.commit_preparation.as_mut().unwrap().manifest = None;
        (status, exact_request, identities)
    }

    #[test]
    fn cursor_is_base64url_opaque_and_operation_bound() {
        let cursor = encode_cursor("search", b"opaque\0bytes");
        assert_eq!(decode_cursor("search", &cursor).unwrap(), b"opaque\0bytes");
        assert!(decode_cursor("find", &cursor).is_err());
        assert!(!cursor.contains("opaque"));
    }

    #[test]
    fn grep_cursor_is_bound_to_workbench_prefix_recursion_and_workspace_revision() {
        let scope = scoped_outputs();
        let prefix = full_path_optional(&scope).unwrap().unwrap();
        let query_commitment = [9; 32];
        let scope_digest = grep_scope_digest(
            test_route().root_id,
            &scope.workbench_id,
            Some(&prefix),
            true,
            query_commitment,
        );
        let fence = wire::WorkspaceContinuationFence {
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            workspace_revision: 1,
        };
        let cursor = encode_grep_cursor(&fence, scope_digest, b"outputs/a.txt");

        assert_eq!(
            decode_grep_cursor(&cursor, scope_digest).unwrap(),
            GrepCursor {
                fence,
                server_cursor: b"outputs/a.txt".to_vec(),
            }
        );
        let non_recursive = grep_scope_digest(
            test_route().root_id,
            &scope.workbench_id,
            Some(&prefix),
            false,
            query_commitment,
        );
        assert!(decode_grep_cursor(&cursor, non_recursive).is_err());
        let different_query = grep_scope_digest(
            test_route().root_id,
            &scope.workbench_id,
            Some(&prefix),
            true,
            [10; 32],
        );
        assert!(decode_grep_cursor(&cursor, different_query).is_err());
        assert!(
            decode_grep_cursor(&encode_cursor("grep", b"outputs/a.txt"), scope_digest).is_err()
        );
        let mut legacy = b"nokv.workspace.grep-cursor.v2\0".to_vec();
        legacy.extend_from_slice(&41_u64.to_be_bytes());
        legacy.extend_from_slice(&scope_digest);
        legacy.extend_from_slice(b"outputs/a.txt");
        assert!(decode_grep_cursor(&URL_SAFE_NO_PAD.encode(legacy), scope_digest).is_err());
        assert!(
            decode_grep_cursor(&"A".repeat(GREP_CURSOR_MAX_ENCODED_BYTES + 1), scope_digest,)
                .is_err()
        );
    }

    #[test]
    fn public_cursor_scopes_are_bound_to_the_storage_root() {
        let scope = scoped_outputs();
        let prefix = full_path_optional(&scope).unwrap().unwrap();
        let workbench = &scope.workbench_id;
        let query_commitment = [9; 32];
        let root_a = wire::RootIdentity([1; 16]);
        let root_b = wire::RootIdentity([2; 16]);

        assert_ne!(
            grep_scope_digest(root_a, workbench, Some(&prefix), true, query_commitment),
            grep_scope_digest(root_b, workbench, Some(&prefix), true, query_commitment),
        );
        assert_ne!(
            list_scope_digest(root_a, workbench, Some(&prefix), &agent::ReadView::Live),
            list_scope_digest(root_b, workbench, Some(&prefix), &agent::ReadView::Live),
        );
        let root_a_scope =
            grep_scope_digest(root_a, workbench, Some(&prefix), true, query_commitment);
        let root_b_scope =
            grep_scope_digest(root_b, workbench, Some(&prefix), true, query_commitment);
        let fence = wire::WorkspaceContinuationFence {
            workspace_incarnation_id: wire::WorkspaceIdentity([7; 16]),
            workspace_revision: 3,
        };
        let cursor = encode_grep_cursor(&fence, root_a_scope, b"outputs/a.txt");
        assert!(decode_grep_cursor(&cursor, root_b_scope).is_err());
        let root_a_list =
            list_scope_digest(root_a, workbench, Some(&prefix), &agent::ReadView::Live);
        let root_b_list =
            list_scope_digest(root_b, workbench, Some(&prefix), &agent::ReadView::Live);
        let anchor = NormalizedRelativePath::new("outputs/a.txt").unwrap();
        let cursor = encode_list_cursor(
            &ListContinuationFence::Workspace(fence),
            root_a_list,
            &anchor,
        );
        assert!(decode_list_cursor(&cursor, root_b_list).is_err());
    }

    #[test]
    fn grep_continuation_sends_the_cursor_workspace_fence() {
        let scope = scoped_outputs();
        let prefix = full_path_optional(&scope).unwrap().unwrap();
        let query_commitment = [9; 32];
        let scope_digest = grep_scope_digest(
            test_route().root_id,
            &scope.workbench_id,
            Some(&prefix),
            true,
            query_commitment,
        );
        let fence = wire::WorkspaceContinuationFence {
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            workspace_revision: 1,
        };
        let cursor = encode_grep_cursor(&fence, scope_digest, b"outputs/a.txt");
        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_artifact("outputs/b.txt")],
                next_cursor: None,
                read_version: 99,
            }),
        )]);

        let page = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(scope.workbench_id),
                    section: scope.section,
                    path: scope.relative_path,
                },
                recursive: true,
                query_commitment,
                cursor: Some(cursor),
                limit: 1,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(page.candidates.len(), 1);
        assert_eq!(page.next_cursor, None);
        assert_eq!(page.candidates[0].cursor_after, None);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::ListPaths(list) = &requests[0].operation else {
            panic!("grep continuation must call list_paths");
        };
        assert_eq!(list.expected_read_version, None);
        assert_eq!(list.workspace_continuation_fence, Some(fence));
        assert_eq!(
            list.page.cursor.as_deref(),
            Some(b"outputs/a.txt".as_slice())
        );
        assert!(list.recursive);
    }

    #[test]
    fn scoped_grep_exact_artifact_wins_without_enumerating_descendants() {
        for recursive in [false, true] {
            let wire::PathListEntry::Artifact(metadata) = wire_artifact("outputs/exact.log") else {
                unreachable!("wire_artifact always returns metadata")
            };
            let (backend, requests, server) = scripted_backend(vec![success(
                wire::WorkspaceResult::Path(wire_path_result(metadata)),
            )]);

            let page = agent::WorkbenchBackend::grep_candidates(
                &backend,
                agent::GrepCandidateRequest {
                    scope: agent::QueryScope {
                        workbench_id: Some(WorkbenchId::new("run-42").unwrap()),
                        section: Some(agent::Section::Outputs),
                        path: Some(NormalizedRelativePath::new("exact.log".to_owned()).unwrap()),
                    },
                    recursive,
                    query_commitment: [0x61; 32],
                    cursor: None,
                    limit: 10,
                },
            )
            .unwrap();
            server.join().unwrap();

            assert_eq!(page.candidates.len(), 1);
            assert_eq!(
                page.candidates[0]
                    .path
                    .relative_path
                    .as_ref()
                    .map(NormalizedRelativePath::as_str),
                Some("exact.log")
            );
            assert_eq!(page.candidates[0].cursor_after, None);
            assert_eq!(page.next_cursor, None);
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(matches!(
                &requests[0].operation,
                wire::WorkspaceRequest::GetPath(get)
                    if get.expected_read_version.is_none()
                        && get.target.path.as_str() == "outputs/exact.log"
            ));
        }
    }

    #[test]
    fn generic_grep_real_backend_accepts_an_exact_section_root_from_root_search() {
        let fixture = manifest_artifact_fixture(
            "run-42",
            "outputs",
            wire::WorkspaceIdentity([0x71; 16]),
            wire::ArtifactRevisionIdentity([0x72; 16]),
            "text/plain",
            b"needle exact section root\n",
        );
        let metadata = fixture
            .metadata_only
            .metadata
            .clone()
            .expect("fixture has exact metadata");
        let mut outcomes = vec![success(wire::WorkspaceResult::Search(wire::SearchResult {
            hits: vec![wire::SearchRow::Artifact(wire::SearchHit {
                metadata,
                projection: Vec::new(),
            })],
            match_count: 1,
            facets: Vec::new(),
            next_cursor: None,
            read_version: 41,
        }))];
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let result = agent::execute_generic_agent_tool(
            &handler,
            "grep",
            &json!({"path": "/", "pattern": "needle", "recursive": true}),
        )
        .unwrap();
        assert_eq!(result["files_scanned"], 1);
        assert_eq!(result["matches"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            result["matches"][0]["path"],
            "/agents/test/wb/run-42/outputs"
        );
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(matches!(
            &requests[0].operation,
            wire::WorkspaceRequest::Search(search)
                if search.profile == wire::QueryProfile::ArtifactV1
        ));
        assert!(requests[1..].iter().all(|request| matches!(
            &request.operation,
            wire::WorkspaceRequest::GetPath(get) if get.target.path.as_str() == "outputs"
        )));
    }

    #[test]
    fn generic_grep_real_backend_accepts_an_exact_section_root_from_workbench_search() {
        let fixture = manifest_artifact_fixture(
            "run-42",
            "outputs",
            wire::WorkspaceIdentity([0x73; 16]),
            wire::ArtifactRevisionIdentity([0x74; 16]),
            "text/plain",
            b"needle exact section root\n",
        );
        let metadata = fixture
            .metadata_only
            .metadata
            .clone()
            .expect("fixture has exact metadata");
        let mut outcomes = vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [0x73; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire::PathListEntry::Artifact(metadata)],
                next_cursor: None,
                read_version: 41,
            })),
        ];
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let result = agent::execute_generic_agent_tool(
            &handler,
            "grep",
            &json!({
                "path": "/agents/test/wb/run-42",
                "pattern": "needle",
                "recursive": true,
            }),
        )
        .unwrap();
        assert_eq!(result["files_scanned"], 1);
        assert_eq!(result["matches"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            result["matches"][0]["path"],
            "/agents/test/wb/run-42/outputs"
        );
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 5);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            &requests[1].operation,
            wire::WorkspaceRequest::ListPaths(list)
                if list.prefix.is_none() && list.recursive
        ));
        assert!(requests[2..].iter().all(|request| matches!(
            &request.operation,
            wire::WorkspaceRequest::GetPath(get) if get.target.path.as_str() == "outputs"
        )));
    }

    #[test]
    fn generic_grep_real_backend_exact_section_root_wins_for_both_recursion_modes() {
        for recursive in [false, true] {
            let fixture = manifest_artifact_fixture(
                "run-42",
                "outputs",
                wire::WorkspaceIdentity([0x75; 16]),
                wire::ArtifactRevisionIdentity([0x76; 16]),
                "text/plain",
                b"needle exact section root\n",
            );
            let mut outcomes = vec![success(wire::WorkspaceResult::Path(
                fixture.metadata_only.clone(),
            ))];
            outcomes.extend(manifest_read_outcomes(&fixture));
            let (backend, requests, server) =
                scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);
            let handler =
                agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

            let result = agent::execute_generic_agent_tool(
                &handler,
                "grep",
                &json!({
                    "path": "/agents/test/wb/run-42/outputs",
                    "pattern": "needle",
                    "recursive": recursive,
                }),
            )
            .unwrap();
            assert_eq!(result["files_scanned"], 1, "recursive={recursive}");
            assert_eq!(
                result["matches"].as_array().map(Vec::len),
                Some(1),
                "recursive={recursive}"
            );
            assert_eq!(
                result["matches"][0]["path"], "/agents/test/wb/run-42/outputs",
                "recursive={recursive}"
            );
            server.join().unwrap();

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 4, "recursive={recursive}");
            assert!(requests.iter().all(|request| matches!(
                &request.operation,
                wire::WorkspaceRequest::GetPath(get) if get.target.path.as_str() == "outputs"
            )));
        }
    }

    #[test]
    fn generic_grep_real_backend_uses_section_root_as_the_candidate_basename() {
        let fixture = manifest_artifact_fixture(
            "run-42",
            "outputs",
            wire::WorkspaceIdentity([0x77; 16]),
            wire::ArtifactRevisionIdentity([0x78; 16]),
            "text/plain",
            b"needle exact section root\n",
        );
        let exact = fixture
            .metadata_only
            .metadata
            .clone()
            .expect("fixture has exact metadata");
        let mut outcomes = vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [0x77; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![
                    wire::PathListEntry::Artifact(exact),
                    wire_artifact("outputs/child.txt"),
                ],
                next_cursor: None,
                read_version: 41,
            })),
        ];
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let result = agent::execute_generic_agent_tool(
            &handler,
            "grep",
            &json!({
                "path": "/agents/test/wb/run-42",
                "pattern": "needle",
                "glob": "outputs",
                "recursive": true,
            }),
        )
        .unwrap();
        assert_eq!(result["files_scanned"], 1);
        assert_eq!(result["matches"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            result["matches"][0]["path"],
            "/agents/test/wb/run-42/outputs"
        );
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 5);
        assert!(requests[2..].iter().all(|request| matches!(
            &request.operation,
            wire::WorkspaceRequest::GetPath(get) if get.target.path.as_str() == "outputs"
        )));
    }

    #[test]
    fn generic_grep_real_backend_keeps_an_empty_virtual_section_non_artifact() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [0x79; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let result = agent::execute_generic_agent_tool(
            &handler,
            "grep",
            &json!({
                "path": "/agents/test/wb/run-42/outputs",
                "pattern": "needle",
                "recursive": true,
            }),
        )
        .unwrap();
        assert_eq!(result["files_scanned"], 0);
        assert_eq!(result["matches"], json!([]));
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::ListPaths(_)
        ));
    }

    #[test]
    fn scoped_grep_missing_nested_path_is_not_an_empty_directory() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
            not_found_failure(),
        ]);

        let error = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(WorkbenchId::new("run-42").unwrap()),
                    section: Some(agent::Section::Outputs),
                    path: Some(NormalizedRelativePath::new("missing".to_owned()).unwrap()),
                },
                recursive: true,
                query_commitment: [0x62; 32],
                cursor: None,
                limit: 10,
            },
        )
        .unwrap_err();
        assert_eq!(
            requests.lock().unwrap().len(),
            4,
            "fresh missing classification must complete its same-version proof"
        );
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::NotFound);
    }

    #[test]
    fn scoped_grep_empty_virtual_section_is_an_empty_success() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);

        let page = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(WorkbenchId::new("run-42").unwrap()),
                    section: Some(agent::Section::Outputs),
                    path: None,
                },
                recursive: true,
                query_commitment: [0x63; 32],
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert!(page.candidates.is_empty());
        assert_eq!(page.next_cursor, None);
        assert_eq!(requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn grep_enumeration_fence_conflicts_are_typed_read_fence_changes() {
        let (backend, _, server) = scripted_backend(vec![read_version_failure()]);
        let root = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                recursive: true,
                query_commitment: [0x64; 32],
                cursor: None,
                limit: 1,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(root.kind, agent::BackendErrorKind::ReadFenceChanged);

        let scope = scoped_outputs();
        let prefix = full_path_optional(&scope).unwrap().unwrap();
        let query_commitment = [0x65; 32];
        let scope_digest = grep_scope_digest(
            test_route().root_id,
            &scope.workbench_id,
            Some(&prefix),
            true,
            query_commitment,
        );
        let cursor = encode_grep_cursor(
            &wire::WorkspaceContinuationFence {
                workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
                workspace_revision: 1,
            },
            scope_digest,
            b"outputs/a.txt",
        );
        let (backend, _, server) = scripted_backend(vec![workspace_fence_failure()]);
        let scoped = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(scope.workbench_id),
                    section: scope.section,
                    path: scope.relative_path,
                },
                recursive: true,
                query_commitment,
                cursor: Some(cursor),
                limit: 1,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(scoped.kind, agent::BackendErrorKind::ReadFenceChanged);
    }

    #[test]
    fn grep_page_two_survives_an_unrelated_read_version_advance() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_artifact("outputs/a.txt")],
                next_cursor: Some(b"outputs/a.txt".to_vec()),
                read_version: 41,
            })),
            not_found_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_artifact("outputs/b.txt")],
                next_cursor: None,
                read_version: 99,
            })),
        ]);
        let scope = scoped_outputs();
        let query_commitment = [9; 32];
        let first = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(scope.workbench_id.clone()),
                    section: scope.section,
                    path: scope.relative_path.clone(),
                },
                recursive: true,
                query_commitment,
                cursor: None,
                limit: 1,
            },
        )
        .unwrap();
        let second = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(scope.workbench_id),
                    section: scope.section,
                    path: scope.relative_path,
                },
                recursive: true,
                query_commitment,
                cursor: first.next_cursor,
                limit: 1,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(
            first.candidates[0]
                .path
                .relative_path
                .as_ref()
                .unwrap()
                .as_str(),
            "a.txt"
        );
        assert_eq!(
            second.candidates[0]
                .path
                .relative_path
                .as_ref()
                .unwrap()
                .as_str(),
            "b.txt"
        );
        let requests = requests.lock().unwrap();
        let lists = requests
            .iter()
            .filter_map(|request| match &request.operation {
                wire::WorkspaceRequest::ListPaths(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lists.len(), 2);
        assert_eq!(
            lists[0].workspace_continuation_fence,
            lists[1].workspace_continuation_fence
        );
        assert_eq!(lists[0].expected_read_version, None);
        assert_eq!(lists[1].expected_read_version, None);
    }

    #[test]
    fn root_grep_pages_one_search_hit_at_a_time_with_a_query_bound_cursor() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Search(wire::SearchResult {
                hits: vec![wire_search_hit("outputs/a.txt")],
                match_count: 2,
                facets: Vec::new(),
                next_cursor: Some(b"after-a".to_vec()),
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Search(wire::SearchResult {
                hits: vec![wire_search_hit("outputs/b.txt")],
                match_count: 2,
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let scope = agent::QueryScope {
            workbench_id: None,
            section: None,
            path: None,
        };
        let query_commitment = [0x31; 32];
        let first = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: scope.clone(),
                recursive: true,
                query_commitment,
                cursor: None,
                limit: 300,
            },
        )
        .unwrap();
        let second = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope,
                recursive: true,
                query_commitment,
                cursor: first.next_cursor.clone(),
                limit: 300,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(first.candidates.len(), 1);
        assert_eq!(first.candidates[0].path.workbench_id.as_str(), "run-42");
        assert_eq!(
            first.candidates[0]
                .path
                .relative_path
                .as_ref()
                .map(NormalizedRelativePath::as_str),
            Some("a.txt")
        );
        assert_eq!(second.candidates.len(), 1);
        assert_eq!(
            second.candidates[0]
                .path
                .relative_path
                .as_ref()
                .map(NormalizedRelativePath::as_str),
            Some("b.txt")
        );
        let cursor = first.next_cursor.expect("first root page must continue");
        let wrong_scope = root_grep_scope_digest(test_route().root_id, true, [0x32; 32]);
        assert!(decode_root_grep_cursor(&cursor, wrong_scope).is_err());

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let searches = requests
            .iter()
            .map(|request| match &request.operation {
                wire::WorkspaceRequest::Search(search) => search,
                other => panic!("root grep must use search, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            searches[0].scope,
            wire::QueryScope::Root { path_prefix: None }
        );
        assert_eq!(searches[0].page.limit, 1);
        assert_eq!(searches[0].page.cursor, None);
        assert_eq!(
            searches[1].page.cursor.as_deref(),
            Some(b"after-a".as_slice())
        );
        assert_eq!(searches[0].sort.len(), 1);
        assert_eq!(searches[0].sort[0].field_id, "path");
    }

    #[test]
    fn generic_ls_rejects_a_real_artifact_before_list_dispatch() {
        let wire::PathListEntry::Artifact(metadata) = wire_artifact("outputs/a.txt") else {
            unreachable!("wire_artifact always returns artifact metadata")
        };
        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Path(wire_path_result(metadata)),
        )]);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let error = agent::execute_generic_agent_tool(
            &handler,
            "ls",
            &json!({"path": "/agents/test/wb/run-42/outputs/a.txt"}),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.code, "NotDirectory");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));
    }

    #[test]
    fn generic_structured_stat_anchors_catalog_then_reads_one_exact_artifact_authority() {
        let fixture = manifest_artifact_fixture(
            "run-42",
            "outputs/records.json",
            wire::WorkspaceIdentity([0x41; 16]),
            wire::ArtifactRevisionIdentity([0x42; 16]),
            "application/json",
            br#"[{"status":"ok"},{"status":"done"}]"#,
        );
        let mut outcomes = vec![success(wire::WorkspaceResult::Catalog(
            wire::CatalogResult {
                fields: Vec::new(),
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            },
        ))];
        outcomes.push(success(wire::WorkspaceResult::Path(
            fixture.metadata_only.clone(),
        )));
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let result = agent::execute_generic_agent_tool(
            &handler,
            "stat",
            &json!({"path": "/agents/test/wb/run-42/outputs/records.json"}),
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(result["card"]["record_count"], 2);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 5);
        assert!(matches!(
            &requests[0].operation,
            wire::WorkspaceRequest::Catalog(catalog)
                if catalog.path_match == wire::CatalogPathMatch::Exact
        ));
        let wire::WorkspaceRequest::GetPath(stat) = &requests[1].operation else {
            panic!("second request must resolve the artifact at the catalog version");
        };
        assert_eq!(stat.expected_read_version, Some(41));
        assert!(stat.range.is_none());
        for request in &requests[2..] {
            let wire::WorkspaceRequest::GetPath(read) = &request.operation else {
                panic!("exact artifact body reads must stay on get_path");
            };
            assert_eq!(read.expected_read_version, None);
        }
        assert!(matches!(
            &requests[4].operation,
            wire::WorkspaceRequest::GetPath(read) if read.range.is_some()
        ));
    }

    #[test]
    fn generic_out_of_band_stat_preserves_the_full_path_and_uses_exact_catalog_mode() {
        let wire::PathListEntry::Artifact(metadata) = wire_artifact("note.bin") else {
            unreachable!("wire_artifact always returns artifact metadata")
        };
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: Vec::new(),
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Path(wire_path_result(metadata))),
        ]);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let result = agent::execute_generic_agent_tool(
            &handler,
            "stat",
            &json!({"path": "/agents/test/wb/run-42/note.bin"}),
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(result["card"]["path"], "/agents/test/wb/run-42/note.bin");
        assert_eq!(result["card"]["kind"], "file");
        let requests = requests.lock().unwrap();
        let wire::WorkspaceRequest::Catalog(catalog) = &requests[0].operation else {
            panic!("first request must anchor the exact catalog");
        };
        assert_eq!(catalog.path_match, wire::CatalogPathMatch::Exact);
        assert_eq!(
            catalog.scope,
            wire::QueryScope::Workspace {
                workbench: wire::WorkbenchName::new("run-42").unwrap(),
                path_prefix: Some(wire::RelativePath::new("note.bin").unwrap()),
            }
        );
        let wire::WorkspaceRequest::GetPath(stat) = &requests[1].operation else {
            panic!("second request must resolve the out-of-band artifact");
        };
        assert_eq!(stat.target.path.as_str(), "note.bin");
        assert_eq!(stat.expected_read_version, Some(41));
    }

    #[test]
    fn generic_find_real_backend_fences_directory_classification_to_the_search_version() {
        let wire::PathListEntry::Artifact(metadata) = wire_artifact("outputs/collision") else {
            unreachable!("wire_artifact always returns artifact metadata")
        };
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Search(wire::SearchResult {
                hits: vec![wire::SearchRow::GenericNamespace(
                    wire::GenericNamespaceHit {
                        workbench: metadata.path.workbench.clone(),
                        relative_path: Some(metadata.path.path.clone()),
                        kind: wire::GenericNamespaceKind::Artifact,
                        artifact: Some(wire::GenericNamespaceArtifact {
                            generation: metadata.generation,
                            logical_size: metadata.descriptor.logical_size,
                            body_digest: metadata.descriptor.body_digest.clone(),
                            content_type: metadata.descriptor.content_type.clone(),
                            producer: metadata.descriptor.producer.clone(),
                            manifest_identity: metadata.descriptor.manifest_identity.clone(),
                        }),
                        projection: Vec::new(),
                        indexed_values: Vec::new(),
                    },
                )],
                match_count: 1,
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Path(wire_path_result(metadata))),
        ]);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let error = agent::execute_generic_agent_tool(
            &handler,
            "find",
            &json!({"path": "/agents/test/wb/run-42/outputs/collision"}),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.code, "NotDirectory");
        let requests = requests.lock().unwrap();
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::Search(_)
        ));
        let wire::WorkspaceRequest::GetPath(stat) = &requests[1].operation else {
            panic!("find directory classification must use get_path");
        };
        assert_eq!(stat.expected_read_version, Some(41));
    }

    #[test]
    fn generic_find_real_backend_rejects_a_missing_nested_scope_at_the_search_version() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Search(wire::SearchResult {
                hits: Vec::new(),
                match_count: 0,
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
            not_found_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let error = agent::execute_generic_agent_tool(
            &handler,
            "find",
            &json!({"path": "/agents/test/wb/run-42/outputs/missing"}),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.code, "NotFound");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::Search(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));
        let wire::WorkspaceRequest::ListPaths(list) = &requests[2].operation else {
            panic!("missing exact path must be classified by a fenced prefix probe");
        };
        assert_eq!(list.expected_read_version, Some(41));
    }

    #[test]
    fn generic_aggregate_real_backend_rejects_an_exact_artifact_at_the_aggregate_version() {
        let wire::PathListEntry::Artifact(metadata) = wire_artifact("outputs/collision") else {
            unreachable!("wire_artifact always returns artifact metadata")
        };
        let (backend, requests, server) = scripted_backend(vec![
            success(aggregate_count_result("files", 1)),
            success(wire::WorkspaceResult::Path(wire_path_result(metadata))),
        ]);
        let handler = agent::SdkGenericAgentToolHandler::new(backend, "/agents/test/wb").unwrap();

        let error = agent::execute_generic_agent_tool(
            &handler,
            "aggregate",
            &json!({
                "path": "/agents/test/wb/run-42/outputs/collision",
                "measures": [{"name": "files", "op": "count"}],
            }),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.code, "NotDirectory");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::Aggregate(_)
        ));
        let wire::WorkspaceRequest::GetPath(stat) = &requests[1].operation else {
            panic!("aggregate directory classification must use get_path");
        };
        assert_eq!(stat.expected_read_version, Some(41));
    }

    #[test]
    fn version_fenced_stat_prefers_an_exact_artifact_and_rejects_root_drift() {
        let path = scoped_full_path(&WorkbenchId::new("run-42").unwrap(), "outputs/a").unwrap();
        let wire::PathListEntry::Artifact(metadata) = wire_artifact("outputs/a") else {
            unreachable!("wire_artifact always returns metadata")
        };
        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Path(wire_path_result(metadata)),
        )]);

        let record = agent::WorkbenchBackend::stat_at_read_version(&backend, &path, 41)
            .unwrap()
            .expect("exact artifact exists at the fenced version");
        server.join().unwrap();

        assert_eq!(record.kind, agent::ArtifactKind::Artifact);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::GetPath(get) = &requests[0].operation else {
            panic!("exact stat must use get_path before any descendant probe");
        };
        assert_eq!(get.expected_read_version, Some(41));

        let (backend, requests, server) = scripted_backend(vec![read_version_failure()]);
        let error = agent::WorkbenchBackend::stat_at_read_version(&backend, &path, 41)
            .expect_err("an unrelated root advance must fail the exact version fence");
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::ReadFenceChanged);
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn version_fenced_stat_uses_the_same_version_for_an_implicit_descendant_prefix() {
        let path = scoped_full_path(&WorkbenchId::new("run-42").unwrap(), "outputs/a").unwrap();
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/a/child")],
                next_cursor: None,
                read_version: 41,
            })),
        ]);

        let record = agent::WorkbenchBackend::stat_at_read_version(&backend, &path, 41)
            .unwrap()
            .expect("implicit prefix exists at the fenced version");
        server.join().unwrap();

        assert_eq!(record.kind, agent::ArtifactKind::Directory);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        let wire::WorkspaceRequest::GetPath(get) = &requests[0].operation else {
            panic!("implicit stat must first rule out an exact artifact");
        };
        assert_eq!(get.expected_read_version, Some(41));
        let wire::WorkspaceRequest::ListPaths(list) = &requests[1].operation else {
            panic!("implicit stat must prove a descendant with list_paths");
        };
        assert_eq!(list.expected_read_version, Some(41));
        assert_eq!(list.page.limit, 1);
    }

    #[test]
    fn exact_artifact_at_a_reserved_section_root_wins_across_read_surfaces() {
        let scope = scoped_outputs();
        let wire::PathListEntry::Artifact(metadata) = wire_artifact("outputs") else {
            unreachable!("wire_artifact always returns metadata")
        };

        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Path(wire_path_result(metadata.clone())),
        )]);
        let stat = agent::WorkbenchBackend::stat(&backend, &scope, &agent::ReadView::Live);
        server.join().unwrap();
        let stat = stat.unwrap().expect("exact section-root artifact exists");
        assert_eq!(stat.kind, agent::ArtifactKind::Artifact);
        assert!(stat.artifact.is_some());
        assert!(matches!(
            requests.lock().unwrap()[0].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));

        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Path(wire_path_result(metadata.clone())),
        )]);
        let versioned = agent::WorkbenchBackend::stat_at_read_version(&backend, &scope, 41);
        server.join().unwrap();
        let versioned = versioned
            .unwrap()
            .expect("exact section-root artifact exists at the fenced version");
        assert_eq!(versioned.kind, agent::ArtifactKind::Artifact);
        let wire::WorkspaceRequest::GetPath(get) = &requests.lock().unwrap()[0].operation else {
            panic!("version-fenced stat must probe the exact section-root artifact first");
        };
        assert_eq!(get.expected_read_version, Some(41));

        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Path(wire_path_result(metadata)),
        )]);
        let grep = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope: agent::QueryScope {
                    workbench_id: Some(scope.workbench_id.clone()),
                    section: scope.section,
                    path: None,
                },
                recursive: true,
                query_commitment: [0x66; 32],
                cursor: None,
                limit: 10,
            },
        );
        server.join().unwrap();
        let grep = grep.unwrap();
        assert_eq!(grep.candidates.len(), 1);
        assert_eq!(grep.candidates[0].path, scope);
        assert!(grep.next_cursor.is_none());
        assert!(matches!(
            requests.lock().unwrap()[0].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));

        let (backend, _, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                // The prefix represents the coexisting `outputs/child` while
                // the artifact is the exact authoritative `outputs` path.
                entries: vec![wire_prefix("outputs"), wire_artifact("outputs")],
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let mut request = list_request(scoped_root(), None);
        request.limit = 10;
        let listed = agent::WorkbenchBackend::list(&backend, request);
        server.join().unwrap();
        let listed = listed.unwrap();
        let outputs = listed
            .entries
            .iter()
            .find(|entry| entry.path.section == Some(agent::Section::Outputs))
            .expect("root union contains outputs");
        assert_eq!(outputs.kind, agent::ArtifactKind::Artifact);
        assert!(outputs.artifact.is_some());
    }

    #[test]
    fn grep_candidate_generation_and_removal_errors_stay_typed_as_fence_changes() {
        let path = scoped_full_path(&WorkbenchId::new("run-42").unwrap(), "outputs/a.txt").unwrap();
        let generation =
            map_grep_candidate_read_error(ClientError::ArtifactReadFenceChanged, &path);
        let removal = map_grep_candidate_read_error(
            ClientError::Rpc(wire::RpcFailure {
                code: wire::ErrorCode::NotFound,
                message: "candidate removed".to_owned(),
                retryable: false,
                conflict: None,
                current_generation: None,
                route_hint: None,
            }),
            &path,
        );
        let invalid_options = map_grep_candidate_read_error(
            ClientError::InvalidOptions("object namespace is not bound".to_owned()),
            &path,
        );

        assert_eq!(generation.kind, agent::BackendErrorKind::ReadFenceChanged);
        assert!(generation.retryable);
        assert_eq!(removal.kind, agent::BackendErrorKind::ReadFenceChanged);
        assert!(removal.retryable);
        assert_eq!(invalid_options.kind, agent::BackendErrorKind::InvalidState);
        assert!(!invalid_options.retryable);
    }

    #[test]
    fn zero_length_grep_candidate_rejects_a_noncanonical_empty_manifest() {
        let mut fixture = manifest_artifact_fixture(
            "run-42",
            "outputs/empty.txt",
            wire::WorkspaceIdentity([0x31; 16]),
            wire::ArtifactRevisionIdentity([0x32; 16]),
            "text/plain",
            &[],
        );
        for result in [&mut fixture.metadata_only, &mut fixture.range] {
            result
                .metadata
                .as_mut()
                .expect("fixture has metadata")
                .descriptor
                .body_digest = wire::DigestUri::new(format!("sha256:{}", "07".repeat(32))).unwrap();
        }
        let metadata = fixture
            .metadata_only
            .metadata
            .as_ref()
            .expect("fixture has metadata");
        let fence = agent::GrepCandidateReadFence {
            path: scoped_path(&metadata.path).unwrap(),
            authority: grep_candidate_authority(metadata),
        };
        let outcomes = vec![
            success(wire::WorkspaceResult::Path(fixture.metadata_only.clone())),
            success(wire::WorkspaceResult::Path(fixture.metadata_only.clone())),
        ];
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

        let error = agent::WorkbenchBackend::read_grep_candidate(&backend, &fence).unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error.kind,
            agent::BackendErrorKind::InvalidState,
            "unexpected empty-manifest error: {error:?}"
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| matches!(
            &request.operation,
            wire::WorkspaceRequest::GetPath(read)
                if read.range.is_none() && read.plan_page.is_none()
        )));
    }

    #[test]
    fn generic_backend_preserves_repeated_values_and_declared_catalog_metadata() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Search(wire::SearchResult {
                hits: vec![wire::SearchRow::GenericNamespace(
                    wire::GenericNamespaceHit {
                        workbench: wire::WorkbenchName::new("generic-values").unwrap(),
                        relative_path: None,
                        kind: wire::GenericNamespaceKind::Directory,
                        artifact: None,
                        projection: vec![wire::FieldValue {
                            field_id: "experiment.labels".to_owned(),
                            value: wire::ScalarValue::String("alpha".to_owned()),
                        }],
                        indexed_values: vec![wire::GenericIndexFieldValues {
                            field_id: "experiment.labels".to_owned(),
                            values: vec![
                                wire::ScalarValue::String("alpha".to_owned()),
                                wire::ScalarValue::Unsigned(7),
                                wire::ScalarValue::String("alpha".to_owned()),
                            ],
                        }],
                    },
                )],
                match_count: 1,
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: vec![wire::CatalogField {
                    field_id: "experiment.labels".to_owned(),
                    scalar_type: "unsigned".to_owned(),
                    scalar_types: vec!["unsigned".to_owned(), "string".to_owned()],
                    generic_custom: true,
                    operators: vec![wire::QueryOperator::Equal],
                    sortable: true,
                    facetable: true,
                    aggregatable: true,
                }],
                facets: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let profile = agent::QueryProfile::GenericNamespaceV1 {
            presentation_path_root: "/agents".to_owned(),
        };
        let scope = agent::QueryScope {
            workbench_id: Some(WorkbenchId::new("generic-values").unwrap()),
            section: None,
            path: None,
        };

        let search = agent::WorkbenchBackend::search(
            &backend,
            agent::SearchRequest {
                profile: profile.clone(),
                scope: scope.clone(),
                predicates: Vec::new(),
                fields: vec!["experiment.labels".to_owned()],
                sort: Vec::new(),
                facets: Vec::new(),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(
            search.namespace_hits[0].indexed_values["experiment.labels"],
            vec![
                agent::QueryValue::String("alpha".to_owned()),
                agent::QueryValue::Unsigned(7),
                agent::QueryValue::String("alpha".to_owned()),
            ]
        );

        let catalog = agent::WorkbenchBackend::catalog(
            &backend,
            agent::CatalogRequest {
                profile,
                scope,
                path_match: agent::CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: false,
            },
        )
        .unwrap();
        assert_eq!(
            catalog.fields[0].scalar_types,
            vec!["unsigned".to_owned(), "string".to_owned()]
        );
        assert!(catalog.fields[0].generic_custom);
        server.join().unwrap();
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn find_without_manifest_filter_or_projection_never_reads_manifest_objects() {
        let mut workspace = workspace_summary("run-42", [5; 16]);
        workspace.commit_head = Some(wire::CommitIdentity([7; 32]));
        workspace.commit_head_generation = Some(1);
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![wire::WorkspaceSummaryWithCommit {
                        workspace,
                        commit: None,
                    }],
                    next_cursor: None,
                    read_version: 41,
                },
            )),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);

        let page = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: None,
                include_manifest: false,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(page.workbenches.len(), 1);
        assert!(page.workbenches[0].committed);
        assert_eq!(page.workbenches[0].manifest_metadata, None);
        assert_eq!(page.workbenches[0].manifest, None);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::FindWorkspaces(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::ListPaths(_)
        ));
    }

    #[test]
    fn find_manifest_projection_is_anchored_to_the_discovery_read_version() {
        let commit = commit_request();
        let envelope = test_commit_envelope(&commit, 123);
        let fixture = manifest_artifact_fixture(
            commit.workbench_id.as_str(),
            RUN_MANIFEST_PATH,
            wire::WorkspaceIdentity([5; 16]),
            wire::ArtifactRevisionIdentity([0x73; 16]),
            JSON_CONTENT_TYPE,
            &envelope,
        );
        let mut workspace = workspace_summary(commit.workbench_id.as_str(), [5; 16]);
        workspace.commit_head = Some(wire::CommitIdentity(commit.stable_commit_id));
        workspace.commit_head_generation = Some(1);
        let mut outcomes = vec![success(wire::WorkspaceResult::Workspaces(
            wire::FindWorkspacesResult {
                workspaces: vec![wire::WorkspaceSummaryWithCommit {
                    workspace,
                    commit: None,
                }],
                next_cursor: None,
                read_version: 41,
            },
        ))];
        outcomes.extend(manifest_read_outcomes(&fixture));
        outcomes.push(success(wire::WorkspaceResult::Paths(wire::PathPage {
            entries: Vec::new(),
            next_cursor: None,
            read_version: 41,
        })));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

        let page = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: Some("viking".to_owned()),
                include_manifest: true,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 41);
        assert_eq!(page.workbenches.len(), 1);
        assert!(page.workbenches[0].manifest.is_some());
        let requests = requests.lock().unwrap();
        let wire::WorkspaceRequest::GetPath(anchor) = &requests[1].operation else {
            panic!("manifest projection must start with a version-fenced metadata read");
        };
        assert_eq!(anchor.expected_read_version, Some(41));
        assert_eq!(anchor.target.path.as_str(), RUN_MANIFEST_PATH);
        assert!(matches!(
            &requests[2].operation,
            wire::WorkspaceRequest::GetPath(read)
                if read.expected_read_version.is_none() && read.range.is_none()
        ));
    }

    #[test]
    fn fresh_find_restarts_the_whole_union_when_manifest_anchor_drifts() {
        let commit = commit_request();
        let envelope = test_commit_envelope(&commit, 123);
        let fixture = manifest_artifact_fixture(
            commit.workbench_id.as_str(),
            RUN_MANIFEST_PATH,
            wire::WorkspaceIdentity([5; 16]),
            wire::ArtifactRevisionIdentity([0x74; 16]),
            JSON_CONTENT_TYPE,
            &envelope,
        );
        let discovered = || {
            let mut workspace = workspace_summary(commit.workbench_id.as_str(), [5; 16]);
            workspace.commit_head = Some(wire::CommitIdentity(commit.stable_commit_id));
            workspace.commit_head_generation = Some(1);
            wire::WorkspaceSummaryWithCommit {
                workspace,
                commit: None,
            }
        };
        let mut outcomes = vec![
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![discovered()],
                    next_cursor: None,
                    read_version: 41,
                },
            )),
            read_version_failure(),
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![discovered()],
                    next_cursor: None,
                    read_version: 42,
                },
            )),
        ];
        outcomes.extend(manifest_read_outcomes(&fixture));
        outcomes.push(success(wire::WorkspaceResult::Paths(wire::PathPage {
            entries: Vec::new(),
            next_cursor: None,
            read_version: 42,
        })));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

        let page = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: Some("viking".to_owned()),
                include_manifest: false,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 42);
        let requests = requests.lock().unwrap();
        for (index, version) in [(1, 41), (3, 42)] {
            let wire::WorkspaceRequest::GetPath(get) = &requests[index].operation else {
                panic!("manifest anchor must use get_path");
            };
            assert_eq!(get.expected_read_version, Some(version));
        }
    }

    #[test]
    fn resumed_find_surfaces_manifest_anchor_drift_without_restarting() {
        let commit = commit_request();
        let mut workspace = workspace_summary(commit.workbench_id.as_str(), [5; 16]);
        workspace.commit_head = Some(wire::CommitIdentity(commit.stable_commit_id));
        workspace.commit_head_generation = Some(1);
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![wire::WorkspaceSummaryWithCommit {
                        workspace,
                        commit: None,
                    }],
                    next_cursor: None,
                    read_version: 41,
                },
            )),
            read_version_failure(),
        ]);

        let error = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: Some("viking".to_owned()),
                include_manifest: false,
                cursor: Some(encode_cursor("find", b"page-two")),
                limit: 10,
            },
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::ReadFenceChanged);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn find_workbench_child_counts_restart_the_whole_version_fenced_union() {
        let discovered = || wire::WorkspaceSummaryWithCommit {
            workspace: workspace_summary("run-42", [5; 16]),
            commit: None,
        };
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![discovered()],
                    next_cursor: None,
                    read_version: 41,
                },
            )),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("notes"), wire_prefix("outputs")],
                next_cursor: Some(b"outputs".to_vec()),
                read_version: 41,
            })),
            read_version_failure(),
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![discovered()],
                    next_cursor: None,
                    read_version: 42,
                },
            )),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![
                    wire_prefix("docs"),
                    wire_prefix("input"),
                    wire_prefix("notes"),
                    wire_artifact("outputs"),
                ],
                next_cursor: None,
                read_version: 42,
            })),
        ]);

        let page = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: None,
                include_manifest: false,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(page.read_version, 42);
        assert_eq!(page.workbenches.len(), 1);
        assert_eq!(page.workbenches[0].entry_count, 7);
        server.join().unwrap();

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 5);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::FindWorkspaces(_)
        ));
        let wire::WorkspaceRequest::ListPaths(first) = &requests[1].operation else {
            panic!("first count page must use list_paths");
        };
        assert_eq!(first.expected_read_version, Some(41));
        assert!(first.prefix.is_none());
        assert!(!first.recursive);
        let wire::WorkspaceRequest::ListPaths(second) = &requests[2].operation else {
            panic!("second count page must use list_paths");
        };
        assert_eq!(second.page.cursor.as_deref(), Some(b"outputs".as_slice()));
        assert_eq!(second.expected_read_version, Some(41));
        assert!(matches!(
            requests[3].operation,
            wire::WorkspaceRequest::FindWorkspaces(_)
        ));
        let wire::WorkspaceRequest::ListPaths(restarted) = &requests[4].operation else {
            panic!("restarted count must use list_paths");
        };
        assert_eq!(restarted.expected_read_version, Some(42));
        assert!(restarted.page.cursor.is_none());
    }

    #[test]
    fn resumed_find_surfaces_child_count_read_fence_change_without_panicking() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![wire::WorkspaceSummaryWithCommit {
                        workspace: workspace_summary("run-42", [5; 16]),
                        commit: None,
                    }],
                    next_cursor: None,
                    read_version: 41,
                },
            )),
            read_version_failure(),
        ]);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            agent::WorkbenchBackend::find_workbenches(
                &backend,
                agent::FindRequest {
                    committed: None,
                    manifest_pattern: None,
                    include_manifest: false,
                    cursor: Some(encode_cursor("find", b"page-two")),
                    limit: 10,
                },
            )
        }));
        server.join().unwrap();

        let error = result
            .expect("resumed find must not panic when child enumeration loses its read fence")
            .unwrap_err();
        assert_eq!(error.kind, agent::BackendErrorKind::ReadFenceChanged);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::FindWorkspaces(_)
        ));
        let wire::WorkspaceRequest::ListPaths(count) = &requests[1].operation else {
            panic!("workbench child count must use list_paths");
        };
        assert_eq!(count.expected_read_version, Some(41));
    }

    #[test]
    fn workbench_child_count_rejects_cursor_cycles_and_enforces_shared_budget_bounds() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspaces(
                wire::FindWorkspacesResult {
                    workspaces: vec![wire::WorkspaceSummaryWithCommit {
                        workspace: workspace_summary("run-42", [5; 16]),
                        commit: None,
                    }],
                    next_cursor: None,
                    read_version: 41,
                },
            )),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("notes")],
                next_cursor: Some(b"cycle".to_vec()),
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("notes")],
                next_cursor: Some(b"cycle".to_vec()),
                read_version: 41,
            })),
        ]);
        let error = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: None,
                include_manifest: false,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert_eq!(requests.lock().unwrap().len(), 3);

        let mut page_budget = WorkbenchEntryCountBudget {
            pages: WORKBENCH_ENTRY_COUNT_MAX_PAGES - 1,
            rows: 0,
        };
        page_budget.reserve_page().unwrap();
        let page_error = page_budget.reserve_page().unwrap_err();
        assert_eq!(
            page_error.kind,
            agent::BackendErrorKind::Other("ResourceExhausted".to_owned())
        );

        let mut row_budget = WorkbenchEntryCountBudget {
            pages: 0,
            rows: WORKBENCH_ENTRY_COUNT_MAX_ROWS - 1,
        };
        row_budget.consume_rows(1).unwrap();
        let row_error = row_budget.consume_rows(1).unwrap_err();
        assert_eq!(
            row_error.kind,
            agent::BackendErrorKind::Other("ResourceExhausted".to_owned())
        );
    }

    #[test]
    fn aggregate_exposes_and_forwards_the_typed_continuation() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Aggregate(wire::AggregateResult {
                groups: Vec::new(),
                input_match_count: 0,
                row_count: 0,
                group_count: 0,
                next_cursor: Some(b"aggregate-next".to_vec()),
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Aggregate(wire::AggregateResult {
                groups: Vec::new(),
                input_match_count: 0,
                row_count: 0,
                group_count: 0,
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let request = agent::AggregateRequest {
            profile: agent::QueryProfile::ArtifactV1,
            scope: agent::QueryScope {
                workbench_id: None,
                section: None,
                path: None,
            },
            predicates: Vec::new(),
            group_by: Vec::new(),
            measures: vec![agent::AggregateMeasure {
                name: "count".to_owned(),
                operator: agent::AggregateOperator::Count,
                field: None,
            }],
            sort: Vec::new(),
            cursor: None,
            limit: 1,
        };
        let first = agent::WorkbenchBackend::aggregate(&backend, request.clone()).unwrap();
        let second = agent::WorkbenchBackend::aggregate(
            &backend,
            agent::AggregateRequest {
                cursor: first.next_cursor.clone(),
                ..request
            },
        )
        .unwrap();
        server.join().unwrap();

        assert!(first.next_cursor.is_some());
        assert_eq!(second.next_cursor, None);
        let requests = requests.lock().unwrap();
        let aggregates = requests
            .iter()
            .map(|request| match &request.operation {
                wire::WorkspaceRequest::Aggregate(aggregate) => aggregate,
                other => panic!("aggregate continuation must stay aggregate, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(aggregates[0].page.cursor, None);
        assert_eq!(
            aggregates[1].page.cursor.as_deref(),
            Some(b"aggregate-next".as_slice())
        );
    }

    #[test]
    fn read_enumeration_continuations_report_typed_fence_changes_only_for_fence_conflicts() {
        let (backend, _, server) = scripted_backend(vec![read_version_failure()]);
        let search = agent::WorkbenchBackend::search(
            &backend,
            agent::SearchRequest {
                profile: agent::QueryProfile::ArtifactV1,
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                predicates: Vec::new(),
                fields: Vec::new(),
                sort: Vec::new(),
                facets: Vec::new(),
                cursor: Some(encode_cursor("search", b"page-two")),
                limit: 1,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(search.kind, agent::BackendErrorKind::ReadFenceChanged);

        let (backend, _, server) = scripted_backend(vec![workspace_fence_failure()]);
        let aggregate = agent::WorkbenchBackend::aggregate(
            &backend,
            agent::AggregateRequest {
                profile: agent::QueryProfile::ArtifactV1,
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                predicates: Vec::new(),
                group_by: Vec::new(),
                measures: vec![agent::AggregateMeasure {
                    name: "count".to_owned(),
                    operator: agent::AggregateOperator::Count,
                    field: None,
                }],
                sort: Vec::new(),
                cursor: Some(encode_cursor("aggregate", b"page-two")),
                limit: 1,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(aggregate.kind, agent::BackendErrorKind::ReadFenceChanged);

        let (backend, _, server) = scripted_backend(vec![read_version_failure()]);
        let find = agent::WorkbenchBackend::find_workbenches(
            &backend,
            agent::FindRequest {
                committed: None,
                manifest_pattern: None,
                include_manifest: false,
                cursor: Some(encode_cursor("find", b"page-two")),
                limit: 1,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(find.kind, agent::BackendErrorKind::ReadFenceChanged);

        let (backend, _, server) = scripted_backend(vec![path_generation_precondition_failure()]);
        let non_fence = agent::WorkbenchBackend::search(
            &backend,
            agent::SearchRequest {
                profile: agent::QueryProfile::ArtifactV1,
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                predicates: Vec::new(),
                fields: Vec::new(),
                sort: Vec::new(),
                facets: Vec::new(),
                cursor: Some(encode_cursor("search", b"page-two")),
                limit: 1,
            },
        )
        .unwrap_err();
        server.join().unwrap();
        assert_eq!(non_fence.kind, agent::BackendErrorKind::InvalidState);
    }

    #[test]
    fn list_cursor_is_bound_to_workbench_prefix_view_and_breaks_the_old_schema() {
        let workbench = WorkbenchId::new("run-42").unwrap();
        let outputs = NormalizedRelativePath::new("outputs".to_owned()).unwrap();
        let anchor = NormalizedRelativePath::new("outputs/a".to_owned()).unwrap();
        let scope = list_scope_digest(
            test_route().root_id,
            &workbench,
            Some(&outputs),
            &agent::ReadView::Live,
        );
        let fence = ListContinuationFence::Workspace(wire::WorkspaceContinuationFence {
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            workspace_revision: 1,
        });
        let cursor = encode_list_cursor(&fence, scope, &anchor);
        assert_eq!(
            decode_list_cursor(&cursor, scope).unwrap(),
            ListCursor { fence, anchor }
        );

        let snapshot_scope = list_scope_digest(
            test_route().root_id,
            &workbench,
            Some(&outputs),
            &agent::ReadView::Snapshot(agent::SnapshotSelector::Name("checkpoint".to_owned())),
        );
        assert!(decode_list_cursor(&cursor, snapshot_scope).is_err());
        assert!(decode_list_cursor(&encode_cursor("list", b"outputs/a"), scope).is_err());
        let mut legacy = b"nokv.workspace.list-cursor.v3\0".to_vec();
        legacy.extend_from_slice(&7_u64.to_be_bytes());
        legacy.extend_from_slice(&scope);
        legacy.extend_from_slice(b"outputs/a");
        assert!(decode_list_cursor(&URL_SAFE_NO_PAD.encode(legacy), scope).is_err());
        assert!(
            decode_list_cursor(&"A".repeat(LIST_CURSOR_MAX_ENCODED_BYTES + 1), scope,).is_err()
        );
    }

    #[test]
    fn list_server_pages_track_only_the_union_lookahead() {
        assert_eq!(list_server_page_limit(1, 0, true), 2);
        assert_eq!(list_server_page_limit(10, 5, true), 11);
        assert_eq!(list_server_page_limit(1_000, 0, true), 1_000);
        assert_eq!(list_server_page_limit(1_000, 1_000, false), 1);
        assert_eq!(list_server_page_limit(10, 4, false), 7);
    }

    #[test]
    fn query_page_limits_match_the_execution_bound() {
        assert_eq!(query_page_limit(256).unwrap(), 256);
        assert!(query_page_limit(257).is_err());
        assert_eq!(page_limit(1_000).unwrap(), 1_000);
        assert_eq!(list_server_page_limit(1_000, 0, true), 1_000);
    }

    fn assert_exact_backend_error(
        actual: agent::BackendError,
        kind: agent::BackendErrorKind,
        message: impl Into<String>,
        source: &'static str,
    ) {
        assert_eq!(
            actual,
            agent::BackendError::new(kind, message, false, json!({"source": source}))
        );
    }

    #[test]
    fn lifecycle_create_incarnation_mismatch_preserves_the_protocol_error_envelope() {
        let observed = workspace_summary("incarnation-mismatch", [0x55; 16]);
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(observed.clone())),
            success(wire::WorkspaceResult::Workspace(observed)),
        ]);

        let error = agent::WorkbenchBackend::create_workbench(
            &backend,
            &WorkbenchId::new("incarnation-mismatch").unwrap(),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_exact_backend_error(
            error,
            agent::BackendErrorKind::InvalidState,
            "created workbench resolved to a different incarnation",
            "nokv-protocol",
        );
        assert_eq!(requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn lifecycle_noncanonical_run_manifest_preserves_the_projection_error_envelope() {
        let fixture = manifest_artifact_fixture(
            "clock-stable-run",
            RUN_MANIFEST_PATH,
            wire::WorkspaceIdentity([5; 16]),
            wire::ArtifactRevisionIdentity([0x71; 16]),
            JSON_CONTENT_TYPE,
            b"{} ",
        );
        let mut outcomes = vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "clock-stable-run",
                [5; 16],
            ))),
        ];
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

        let error = agent::WorkbenchBackend::commit(&backend, commit_request()).unwrap_err();
        server.join().unwrap();

        assert_exact_backend_error(
            error,
            agent::BackendErrorKind::InvalidState,
            "run manifest violates the v1 projection: run manifest bytes are not recursively canonical compact JSON",
            "workbench-backend",
        );
        assert_eq!(requests.lock().unwrap().len(), 5);
    }

    #[test]
    fn lifecycle_noncanonical_restore_manifest_preserves_the_projection_error_envelope() {
        let fixture = manifest_artifact_fixture(
            "destination-run",
            "metadata/restore_manifest.json",
            wire::WorkspaceIdentity([0x22; 16]),
            wire::ArtifactRevisionIdentity([0x72; 16]),
            JSON_CONTENT_TYPE,
            b"{} ",
        );
        let mut outcomes = vec![success(wire::WorkspaceResult::Workspace(
            workspace_summary("destination-run", [0x22; 16]),
        ))];
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, requests, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

        let error = agent::WorkbenchBackend::restore(
            &backend,
            agent::RestoreRequest {
                source_workbench_id: WorkbenchId::new("source-run").unwrap(),
                source_workbench_path: "/agents/test/wb/source-run".to_owned(),
                origin: agent::RestoreOrigin::Snapshot(agent::SnapshotSelector::Id(7)),
                destination_workbench_id: WorkbenchId::new("destination-run").unwrap(),
                destination_workbench_path: "/agents/test/wb/destination-run".to_owned(),
            },
        )
        .unwrap_err();
        server.join().unwrap();

        assert_exact_backend_error(
            error,
            agent::BackendErrorKind::InvalidState,
            "restore manifest violates the v1 projection: restore manifest bytes are not recursively canonical compact JSON",
            "workbench-backend",
        );
        assert_eq!(requests.lock().unwrap().len(), 4);
    }

    #[test]
    fn lifecycle_run_manifest_descriptor_and_head_mismatch_preserve_the_protocol_envelope() {
        let request = commit_request();
        let body = test_commit_envelope(&request, 1_700_000_123);
        for (content_type, commit_head) in [
            ("text/plain", None),
            (JSON_CONTENT_TYPE, Some(wire::CommitIdentity([0xee; 32]))),
        ] {
            let fixture = manifest_artifact_fixture(
                "clock-stable-run",
                RUN_MANIFEST_PATH,
                wire::WorkspaceIdentity([5; 16]),
                wire::ArtifactRevisionIdentity([0x73; 16]),
                content_type,
                &body,
            );
            let mut workspace = workspace_summary("clock-stable-run", [5; 16]);
            workspace.commit_head = commit_head;
            workspace.commit_head_generation = commit_head.map(|_| 1);
            let mut outcomes = vec![
                not_found_failure(),
                success(wire::WorkspaceResult::Workspace(workspace)),
            ];
            outcomes.extend(manifest_read_outcomes(&fixture));
            let (backend, _, server) =
                scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

            let error = agent::WorkbenchBackend::commit(&backend, request.clone()).unwrap_err();
            server.join().unwrap();
            assert_exact_backend_error(
                error,
                agent::BackendErrorKind::InvalidState,
                "run manifest envelope does not match its path descriptor or typed commit head",
                "nokv-protocol",
            );
        }
    }

    #[test]
    fn lifecycle_restore_manifest_incarnation_mismatch_preserves_the_protocol_envelope() {
        let source_id = WorkbenchId::new("source-run").unwrap();
        let destination_id = WorkbenchId::new("destination-run").unwrap();
        let body = agent::build_restore_manifest_v1(
            [0x91; 16],
            &source_id,
            "/agents/test/wb/source-run",
            &destination_id,
            "/agents/test/wb/destination-run",
            7,
        )
        .unwrap();
        let fixture = manifest_artifact_fixture(
            "destination-run",
            "metadata/restore_manifest.json",
            wire::WorkspaceIdentity([0x99; 16]),
            wire::ArtifactRevisionIdentity([0x74; 16]),
            JSON_CONTENT_TYPE,
            &body,
        );
        let mut outcomes = vec![success(wire::WorkspaceResult::Workspace(
            workspace_summary("destination-run", [0x22; 16]),
        ))];
        outcomes.extend(manifest_read_outcomes(&fixture));
        let (backend, _, server) =
            scripted_backend_with_objects(outcomes, Arc::clone(&fixture.objects), 1024);

        let error = agent::WorkbenchBackend::restore(
            &backend,
            agent::RestoreRequest {
                source_workbench_id: source_id,
                source_workbench_path: "/agents/test/wb/source-run".to_owned(),
                origin: agent::RestoreOrigin::Snapshot(agent::SnapshotSelector::Id(7)),
                destination_workbench_id: destination_id,
                destination_workbench_path: "/agents/test/wb/destination-run".to_owned(),
            },
        )
        .unwrap_err();
        server.join().unwrap();

        assert_exact_backend_error(
            error,
            agent::BackendErrorKind::InvalidState,
            "restore manifest envelope does not match its live destination path descriptor",
            "nokv-protocol",
        );
    }

    #[test]
    fn lifecycle_generated_manifest_size_preserves_the_generic_artifact_envelope() {
        let mut request = commit_request();
        request.canonical_manifest = agent::canonical_json_bytes(&json!({
            "payload": "x".repeat(2_048),
        }))
        .unwrap();
        request.manifest_digest_uri = digest_uri(&request.canonical_manifest).as_str().to_owned();
        request.stable_commit_id = agent::workbench_commit_identity(
            &request.workbench_id,
            &request.content_digest_uri,
            &request.manifest_digest_uri,
        );
        let (terminal, _, _, _) = terminal_commit_replay_fixture(&request);
        let (backend, _, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Operation(terminal.clone())),
            success(wire::WorkspaceResult::Operation(terminal)),
        ]);
        let envelope_bytes = test_commit_envelope(&request, 1_700_000_123).len();

        let error = agent::WorkbenchBackend::commit(&backend, request).unwrap_err();
        server.join().unwrap();

        assert_exact_backend_error(
            error,
            agent::BackendErrorKind::Other("ResourceExhausted".to_owned()),
            format!("artifact is {envelope_bytes} bytes, maximum is 1024"),
            "workbench-backend",
        );
    }

    #[test]
    fn lifecycle_generated_restore_size_preserves_the_generic_artifact_envelope() {
        let source_workbench_id = WorkbenchId::new("source-run").unwrap();
        let source_workbench_path = "/agents/test/wb/source-run".to_owned();
        let destination_workbench_id = WorkbenchId::new("destination-run").unwrap();
        let destination_workbench_path = "/agents/test/wb/destination-run".to_owned();
        let source_incarnation = wire::WorkspaceIdentity([0x21; 16]);
        let snapshot_id = 7;
        let identities = nokv_client::RestoreWorkflowIdentities::derive(
            test_route().root_id,
            &wire::WorkbenchName::new(source_workbench_id.as_str()).unwrap(),
            source_incarnation,
            WorkbenchRestoreSource::Snapshot { snapshot_id },
            &wire::WorkbenchName::new(destination_workbench_id.as_str()).unwrap(),
        );
        let envelope_bytes = agent::build_restore_manifest_v2(
            identities.operation_id.0,
            &source_workbench_id,
            &source_workbench_path,
            &destination_workbench_id,
            &destination_workbench_path,
            agent::RestoreManifestSource::Snapshot { snapshot_id },
        )
        .unwrap()
        .len();
        let snapshot = wire::SnapshotResult {
            snapshot_id,
            workbench: wire::WorkbenchName::new(source_workbench_id.as_str()).unwrap(),
            workspace_incarnation_id: source_incarnation,
            read_version: 41,
            lease_deadline_ms: u64::MAX,
            alias: None,
            annotation: b"null".to_vec(),
            retire_annotation: None,
            status: wire::SnapshotStatus::Alive,
            consumer_count: 0,
        };
        let objects: Arc<dyn ArtifactObjectStore> = Arc::new(
            CliObjectStore::build(&crate::cli::ObjectConfig {
                bucket: Some("unused-test-bucket".to_owned()),
                ..crate::cli::ObjectConfig::default()
            })
            .unwrap(),
        );
        let (backend, requests, server) = scripted_backend_with_objects(
            vec![
                not_found_failure(),
                success(wire::WorkspaceResult::Workspace(workspace_summary(
                    source_workbench_id.as_str(),
                    source_incarnation.0,
                ))),
                success(wire::WorkspaceResult::Snapshot(snapshot)),
            ],
            objects,
            1,
        );

        let error = agent::WorkbenchBackend::restore(
            &backend,
            agent::RestoreRequest {
                source_workbench_id,
                source_workbench_path,
                origin: agent::RestoreOrigin::Snapshot(agent::SnapshotSelector::Id(snapshot_id)),
                destination_workbench_id,
                destination_workbench_path,
            },
        )
        .unwrap_err();
        server.join().unwrap();

        assert_exact_backend_error(
            error,
            agent::BackendErrorKind::Other("ResourceExhausted".to_owned()),
            format!("artifact is {envelope_bytes} bytes, maximum is 1"),
            "workbench-backend",
        );
        assert_eq!(requests.lock().unwrap().len(), 3);
    }

    #[test]
    fn publish_implicitly_admits_a_missing_workbench_before_provider_use() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            already_exists_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "implicit-publish",
                [5; 16],
            ))),
        ]);

        let error = agent::WorkbenchBackend::publish(
            &backend,
            publish_request(agent::PublishCondition::CreateOnly),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error.kind,
            agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
        );
        assert_eq!(error.message, "artifact object operation failed");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::CreateWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
    }

    #[test]
    fn append_implicitly_admits_a_missing_workbench_before_provider_use() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            already_exists_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "implicit-append",
                [6; 16],
            ))),
        ]);

        let error = agent::WorkbenchBackend::append(&backend, append_request()).unwrap_err();
        server.join().unwrap();

        assert_eq!(
            error.kind,
            agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
        );
        assert_eq!(error.message, "artifact object operation failed");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::CreateWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
    }

    #[test]
    fn commit_recovers_identity_before_implicitly_admitting_a_missing_workbench() {
        let request = commit_request();
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            not_found_failure(),
            already_exists_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "clock-stable-run",
                [7; 16],
            ))),
            not_found_failure(),
            not_found_failure(),
            not_found_failure(),
        ]);

        let error = agent::WorkbenchBackend::commit(&backend, request).unwrap_err();
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::NotFound);

        let requests = requests.lock().unwrap();
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetOperation(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::CreateWorkspace(_)
        ));
        assert!(matches!(
            requests[3].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[4].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));
        assert!(matches!(
            requests[5].operation,
            wire::WorkspaceRequest::GetOperation(_)
        ));
        assert!(matches!(
            requests[6].operation,
            wire::WorkspaceRequest::Commit(_)
        ));
    }

    #[test]
    fn manifest_literal_matching_is_ascii_case_insensitive_for_nested_json() {
        let manifest = json!({
            "Manifest": {
                "NestedKey": "MiXeD-Value",
                "unicode": "Straße",
            },
        });
        let canonical_manifest = agent::canonical_json_bytes(&manifest).unwrap();
        let request = |pattern: &str| agent::FindRequest {
            committed: None,
            manifest_pattern: Some(pattern.to_owned()),
            include_manifest: false,
            cursor: None,
            limit: 1,
        };

        assert!(find_request_matches_canonical_manifest(
            &request("MANIFEST"),
            Some(&canonical_manifest),
        ));
        assert!(find_request_matches_canonical_manifest(
            &request(r#""nestedkey":"mixed-value""#),
            Some(&canonical_manifest),
        ));
        assert!(!find_request_matches_canonical_manifest(
            &request("mixed.*value"),
            Some(&canonical_manifest),
        ));
        assert!(!find_request_matches_canonical_manifest(
            &request("STRASSE"),
            Some(&canonical_manifest),
        ));
        assert!(!find_request_matches_canonical_manifest(&request(""), None,));
    }

    #[test]
    fn manifest_literal_matching_is_independent_of_projection_and_page_cursor() {
        let manifest = json!({"manifest": {"Task": "DiFfErEnT"}});
        let canonical_manifest = agent::canonical_json_bytes(&manifest).unwrap();
        let page_two = encode_cursor("find", b"page-two");

        for include_manifest in [false, true] {
            for cursor in [None, Some(page_two.clone())] {
                let request = agent::FindRequest {
                    committed: Some(true),
                    manifest_pattern: Some("different".to_owned()),
                    include_manifest,
                    cursor: cursor.clone(),
                    limit: 1,
                };

                assert!(find_request_matches_canonical_manifest(
                    &request,
                    Some(&canonical_manifest),
                ));
                assert_eq!(request.include_manifest, include_manifest);
                assert_eq!(request.cursor, cursor);
            }
        }
        assert_eq!(decode_cursor("find", &page_two).unwrap(), b"page-two");
    }

    #[test]
    fn virtual_only_list_still_performs_one_authoritative_path_read() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);
        let page =
            agent::WorkbenchBackend::list(&backend, list_request(scoped_root(), None)).unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 41);
        assert_eq!(page.entries.len(), 1);
        assert!(page.next_cursor.is_some());
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        let wire::WorkspaceRequest::ListPaths(list) = &requests[1].operation else {
            panic!("second request must be list_paths");
        };
        assert_eq!(list.expected_read_version, None);
        assert!(list.workspace_continuation_fence.is_some());
        assert!(!list.recursive);
        assert_eq!(list.page.limit, 2);
    }

    #[test]
    fn root_list_merges_authoritative_children_with_virtual_sections_without_skipping() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: ["a", "b", "c"].map(wire_prefix).to_vec(),
                next_cursor: Some(b"c".to_vec()),
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: ["c", "z"].map(wire_prefix).to_vec(),
                next_cursor: None,
                read_version: 42,
            })),
        ]);

        let mut request = list_request(scoped_root(), None);
        request.limit = 2;
        let first = agent::WorkbenchBackend::list(&backend, request.clone()).unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.path.relative_path.as_ref().map(|path| path.as_str()))
                .collect::<Vec<_>>(),
            [Some("a"), Some("b")]
        );
        request.cursor = first.next_cursor;
        let second = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();
        assert_eq!(
            second
                .entries
                .iter()
                .map(|entry| {
                    entry
                        .path
                        .relative_path
                        .as_ref()
                        .map(|path| path.as_str())
                        .or_else(|| entry.path.section.map(agent::Section::as_str))
                })
                .collect::<Vec<_>>(),
            [Some("c"), Some("input")]
        );
        let requests = requests.lock().unwrap();
        let lists = requests
            .iter()
            .filter_map(|request| match &request.operation {
                wire::WorkspaceRequest::ListPaths(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lists.len(), 2);
        assert!(!lists[0].recursive);
        assert_eq!(lists[0].page.limit, 3);
        assert_eq!(lists[1].page.cursor.as_deref(), Some(b"b".as_slice()));
        assert_eq!(lists[1].page.limit, 3);
        assert_eq!(lists[1].expected_read_version, None);
        assert_eq!(
            lists[1].workspace_continuation_fence,
            lists[0].workspace_continuation_fence
        );
    }

    #[test]
    fn snapshot_list_continuation_keeps_the_exact_read_version_fence() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: ["outputs/a", "outputs/b"].map(wire_prefix).to_vec(),
                next_cursor: Some(b"outputs/b".to_vec()),
                read_version: 41,
            })),
            not_found_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/c")],
                next_cursor: None,
                read_version: 41,
            })),
            not_found_failure(),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.view =
            agent::ReadView::Snapshot(agent::SnapshotSelector::Name("checkpoint".to_owned()));
        request.limit = 1;
        let first = agent::WorkbenchBackend::list(&backend, request.clone()).unwrap();
        request.cursor = first.next_cursor;
        let second = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();

        assert_eq!(first.read_version, 41);
        assert_eq!(second.read_version, 41);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        let lists = requests
            .iter()
            .filter_map(|request| match &request.operation {
                wire::WorkspaceRequest::ListPaths(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lists.len(), 2);
        assert_eq!(lists[0].expected_read_version, None);
        assert_eq!(lists[0].workspace_continuation_fence, None);
        assert_eq!(lists[1].expected_read_version, Some(41));
        assert_eq!(lists[1].workspace_continuation_fence, None);
    }

    #[test]
    fn prefixed_list_rejects_an_exact_prefix_artifact_even_when_children_exist() {
        let wire::PathListEntry::Artifact(exact) = wire_artifact("outputs") else {
            unreachable!("wire_artifact always returns metadata")
        };
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/child"), wire_artifact("outputs")],
                next_cursor: None,
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Path(wire_path_result(exact))),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;
        let result = agent::WorkbenchBackend::list(&backend, request);
        server.join().unwrap();

        let error = result.expect_err("the exact artifact must win over its descendants");
        assert_eq!(
            error.kind,
            agent::BackendErrorKind::Other("NotDirectory".to_owned())
        );
        let requests = requests.lock().unwrap();
        let wire::WorkspaceRequest::GetPath(get) = &requests[2].operation else {
            panic!("list must classify its exact scope after establishing the page version");
        };
        assert_eq!(get.expected_read_version, Some(41));
        assert_eq!(get.target.path.as_str(), "outputs");
    }

    #[test]
    fn live_list_discards_a_drifted_attempt_and_restarts_from_page_one() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/a")],
                next_cursor: Some(b"page-2".to_vec()),
                read_version: 7,
            })),
            workspace_fence_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 8,
            })),
            not_found_failure(),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;
        let page = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 8);
        let requests = requests.lock().unwrap();
        let lists = requests
            .iter()
            .filter_map(|request| match &request.operation {
                wire::WorkspaceRequest::ListPaths(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lists.len(), 3);
        assert_eq!(lists[0].expected_read_version, None);
        assert_eq!(lists[1].expected_read_version, None);
        assert_eq!(lists[1].page.cursor.as_deref(), Some(b"page-2".as_slice()));
        assert_eq!(lists[2].expected_read_version, None);
        assert!(lists[2].page.cursor.is_none());
        assert_eq!(
            lists[0].workspace_continuation_fence,
            lists[1].workspace_continuation_fence
        );
    }

    #[test]
    fn fresh_prefixed_list_restarts_when_exact_scope_classification_drifts() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/old")],
                next_cursor: None,
                read_version: 7,
            })),
            read_version_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/new")],
                next_cursor: None,
                read_version: 8,
            })),
            not_found_failure(),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;

        let page = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 8);
        assert_eq!(page.entries.len(), 1);
        assert_eq!(
            page.entries[0]
                .path
                .relative_path
                .as_ref()
                .map(NormalizedRelativePath::as_str),
            Some("new")
        );
        let requests = requests.lock().unwrap();
        for (index, version) in [(2, 7), (5, 8)] {
            let wire::WorkspaceRequest::GetPath(get) = &requests[index].operation else {
                panic!("exact scope classification must use get_path");
            };
            assert_eq!(get.expected_read_version, Some(version));
        }
    }

    #[test]
    fn resumed_prefixed_list_surfaces_exact_scope_classification_drift() {
        let path = scoped_outputs();
        let prefix = full_path_optional(&path).unwrap().unwrap();
        let scope = list_scope_digest(
            test_route().root_id,
            &path.workbench_id,
            Some(&prefix),
            &agent::ReadView::Live,
        );
        let cursor = encode_list_cursor(
            &ListContinuationFence::Workspace(wire::WorkspaceContinuationFence {
                workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
                workspace_revision: 1,
            }),
            scope,
            &NormalizedRelativePath::new("outputs/old".to_owned()).unwrap(),
        );
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/new")],
                next_cursor: None,
                read_version: 99,
            })),
            read_version_failure(),
        ]);

        let error =
            agent::WorkbenchBackend::list(&backend, list_request(path, Some(cursor))).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::ReadFenceChanged);
        assert_eq!(requests.lock().unwrap().len(), 2);
    }

    #[test]
    fn list_rejects_a_server_cursor_cycle() {
        let (backend, _, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "run-42", [5; 16],
            ))),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/a")],
                next_cursor: Some(b"cursor-a".to_vec()),
                read_version: 7,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/b")],
                next_cursor: Some(b"cursor-b".to_vec()),
                read_version: 7,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/c")],
                next_cursor: Some(b"cursor-a".to_vec()),
                read_version: 7,
            })),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;
        let error = agent::WorkbenchBackend::list(&backend, request).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("repeated cursor"));
    }

    #[test]
    fn implicit_directory_stat_rejects_a_foreign_workbench_entry() {
        let (backend, _, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix_for("other-run", "outputs/missing/child")],
                next_cursor: None,
                read_version: 7,
            })),
        ]);
        let path = agent::ScopedPath {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            section: Some(agent::Section::Outputs),
            relative_path: Some(NormalizedRelativePath::new("missing").unwrap()),
        };
        let error =
            agent::WorkbenchBackend::stat(&backend, &path, &agent::ReadView::Live).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("different workbench"));
    }

    #[test]
    fn user_list_cursor_staleness_is_a_typed_fence_change_without_an_automatic_restart() {
        let path = scoped_outputs();
        let prefix = full_path_optional(&path).unwrap().unwrap();
        let scope = list_scope_digest(
            test_route().root_id,
            &path.workbench_id,
            Some(&prefix),
            &agent::ReadView::Live,
        );
        let fence = ListContinuationFence::Workspace(wire::WorkspaceContinuationFence {
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            workspace_revision: 1,
        });
        let cursor = encode_list_cursor(
            &fence,
            scope,
            &NormalizedRelativePath::new("outputs/old".to_owned()).unwrap(),
        );
        let (backend, requests, server) = scripted_backend(vec![workspace_fence_failure()]);
        let error =
            agent::WorkbenchBackend::list(&backend, list_request(path, Some(cursor))).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::ReadFenceChanged);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::ListPaths(list) = &requests[0].operation else {
            panic!("only request must be list_paths");
        };
        assert_eq!(list.expected_read_version, None);
        assert_eq!(
            list.workspace_continuation_fence,
            Some(wire::WorkspaceContinuationFence {
                workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
                workspace_revision: 1,
            })
        );
        assert_eq!(list.page.cursor.as_deref(), Some(b"outputs/old".as_slice()));
    }

    #[test]
    fn catalog_uses_its_own_version_and_restarts_without_a_search_probe() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: vec![catalog_field("old.field")],
                facets: Vec::new(),
                next_cursor: Some(b"page-2".to_vec()),
                read_version: 50,
            })),
            read_version_failure(),
            success(wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: vec![catalog_field("fresh.field")],
                facets: Vec::new(),
                next_cursor: None,
                read_version: 51,
            })),
        ]);
        let result = agent::WorkbenchBackend::catalog(
            &backend,
            agent::CatalogRequest {
                profile: agent::QueryProfile::ArtifactV1,
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                path_match: agent::CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: true,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(result.read_version, 51);
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].field, "fresh.field");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests
            .iter()
            .all(|request| matches!(request.operation, wire::WorkspaceRequest::Catalog(_))));
        assert!(requests.iter().all(|request| {
            let wire::WorkspaceRequest::Catalog(catalog) = &request.operation else {
                return false;
            };
            catalog.page.limit == 256
        }));
    }

    #[test]
    fn generic_catalog_empty_field_prefix_round_trips_as_unfiltered() {
        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: vec![catalog_field("path")],
                facets: Vec::new(),
                next_cursor: None,
                read_version: 50,
            }),
        )]);
        let result = agent::WorkbenchBackend::catalog(
            &backend,
            agent::CatalogRequest {
                profile: agent::QueryProfile::GenericNamespaceV1 {
                    presentation_path_root: "/agents".to_owned(),
                },
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                path_match: agent::CatalogPathMatch::Prefix,
                field_prefix: Some(String::new()),
                include_facets: false,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].field, "path");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::Catalog(catalog) = &requests[0].operation else {
            panic!("only request must be catalog");
        };
        assert_eq!(catalog.field_prefix.as_deref(), Some(""));
        assert_eq!(
            catalog.profile,
            wire::QueryProfile::GenericCustomIndexV1 {
                presentation_path_root: "/agents".to_owned(),
            }
        );
    }

    #[test]
    fn catalog_retry_exhaustion_reports_a_typed_read_fence_change() {
        let mut outcomes = Vec::new();
        for read_version in [50, 51, 52] {
            outcomes.push(success(wire::WorkspaceResult::Catalog(
                wire::CatalogResult {
                    fields: vec![catalog_field("stale.field")],
                    facets: Vec::new(),
                    next_cursor: Some(b"page-2".to_vec()),
                    read_version,
                },
            )));
            outcomes.push(read_version_failure());
        }
        let (backend, requests, server) = scripted_backend(outcomes);
        let error = agent::WorkbenchBackend::catalog(
            &backend,
            agent::CatalogRequest {
                profile: agent::QueryProfile::ArtifactV1,
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                path_match: agent::CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: true,
            },
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::ReadFenceChanged);
        assert!(error.retryable);
        assert_eq!(requests.lock().unwrap().len(), 6);
    }

    #[test]
    fn component_safe_scope_joins_section_and_relative_path() {
        let scope = agent::QueryScope {
            workbench_id: Some(WorkbenchId::new("run-1").unwrap()),
            section: Some(agent::Section::Outputs),
            path: Some(NormalizedRelativePath::new("nested/result.json").unwrap()),
        };
        let wire::QueryScope::Workspace { path_prefix, .. } = query_scope(&scope).unwrap() else {
            panic!("workspace scope expected");
        };
        assert_eq!(path_prefix.unwrap().as_str(), "outputs/nested/result.json");
    }

    #[test]
    fn terminal_commit_recovery_never_reads_the_live_workspace_or_manifest() {
        for replace in [false, true] {
            let mut request = commit_request();
            request.replace = replace;
            let (commit_status, publish_status, exact_request, identities) =
                terminal_commit_replay_fixture(&request);
            let (backend, requests, server) = scripted_backend(vec![
                success(wire::WorkspaceResult::Operation(commit_status.clone())),
                success(wire::WorkspaceResult::Operation(commit_status)),
                success(wire::WorkspaceResult::Operation(publish_status)),
            ]);

            let outcome = agent::WorkbenchBackend::commit(&backend, request).unwrap();
            server.join().unwrap();
            assert!(outcome.idempotent_replay);
            assert_eq!(outcome.generation, 1);

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert!(matches!(
                &requests[0].operation,
                wire::WorkspaceRequest::GetOperation(request)
                    if request.operation_id == identities.operation_id
            ));
            assert!(matches!(
                &requests[1].operation,
                wire::WorkspaceRequest::Commit(request) if request == &exact_request
            ));
            assert!(matches!(
                &requests[2].operation,
                wire::WorkspaceRequest::GetOperation(request)
                    if request.operation_id == identities.manifest_publish_operation_id
            ));
            assert!(requests.iter().all(|request| !matches!(
                request.operation,
                wire::WorkspaceRequest::GetWorkspace(_) | wire::WorkspaceRequest::GetPath(_)
            )));
        }
    }

    #[test]
    fn running_commit_recovery_rejects_a_changed_presentation_path_before_publish() {
        let original = commit_request();
        let (running, exact_request, identities) = running_commit_replay_fixture(&original);
        let mut changed = original.clone();
        changed.workbench_path = "/agents/test/wb/different-root".to_owned();
        assert_eq!(changed.stable_commit_id, original.stable_commit_id);
        assert_ne!(
            test_commit_projection_input_digest(&changed),
            exact_request.projection_input_digest
        );
        let (backend, requests, server) =
            scripted_backend(vec![success(wire::WorkspaceResult::Operation(running))]);

        let error = agent::WorkbenchBackend::commit(&backend, changed).unwrap_err();
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0].operation,
            wire::WorkspaceRequest::GetOperation(request)
                if request.operation_id == identities.operation_id
        ));
    }

    #[test]
    fn all_query_operand_shapes_map_without_stringly_operators() {
        let predicate = agent::QueryPredicate {
            field: "producer".to_owned(),
            operator: agent::PredicateOperator::In,
            value: Some(agent::QueryValue::List(vec![
                agent::QueryValue::String("agent".to_owned()),
                agent::QueryValue::Unsigned(3),
            ])),
        };
        let mapped = query_predicate(&predicate).unwrap();
        assert_eq!(mapped.operator, wire::QueryOperator::In);
        assert!(matches!(mapped.operand, wire::QueryOperand::Set(values) if values.len() == 2));
    }

    #[test]
    fn exhausted_sdk_append_conflict_remains_a_retryable_workbench_conflict() {
        let mapped = map_client_error(ClientError::RetryExhausted {
            attempts: 5,
            last_error: Box::new(ClientError::Rpc(wire::RpcFailure {
                code: wire::ErrorCode::Conflict,
                message: "append generation changed".to_owned(),
                retryable: false,
                conflict: Some(wire::ConflictKind::PathGeneration),
                current_generation: Some(9),
                route_hint: None,
            })),
        });
        assert_eq!(mapped.kind, agent::BackendErrorKind::Conflict);
        assert!(mapped.retryable);
        assert_eq!(mapped.details["attempts"], 5);
        assert_eq!(mapped.details["current_generation"], 9);
    }

    #[test]
    fn exhausted_temporary_object_failure_remains_typed_retryable_and_redacted() {
        let mapped = map_client_error(ClientError::RetryExhausted {
            attempts: 3,
            last_error: Box::new(ClientError::Object(
                nokv_object::ObjectError::backend_failure(
                    "endpoint=http://127.0.0.1:9000 bucket=private",
                    true,
                ),
            )),
        });
        assert_eq!(
            mapped.kind,
            agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
        );
        assert!(mapped.retryable);
        assert_eq!(mapped.details["attempts"], 3);
        assert!(!mapped.message.contains("127.0.0.1"));
        assert!(!mapped.message.contains("private"));
    }

    #[test]
    fn artifact_integrity_is_invalid_state_and_redacted_without_reclassifying_object_failures() {
        let integrity = map_client_error(ClientError::ArtifactIntegrity(
            "physical key=nokv/private/object expected=secret".to_owned(),
        ));
        assert_eq!(integrity.kind, agent::BackendErrorKind::InvalidState);
        assert!(!integrity.retryable);
        assert_eq!(integrity.message, "artifact integrity verification failed");
        assert!(!integrity.message.contains("nokv/private"));
        assert!(!integrity.message.contains("secret"));

        let unavailable = map_client_error(ClientError::Object(
            nokv_object::ObjectError::backend_failure("private endpoint", true),
        ));
        assert_eq!(
            unavailable.kind,
            agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
        );
        assert!(unavailable.retryable);
        assert_eq!(unavailable.message, "artifact object operation failed");
    }

    #[test]
    fn ambiguous_provider_outcomes_are_redacted_across_the_agent_boundary() {
        let key = nokv_object::ObjectKey::new("nokv/system/sentinel-private-key").unwrap();
        let errors = [
            nokv_object::ObjectError::ObjectNotFound { key: key.clone() },
            nokv_object::ObjectError::ImmutableCollision {
                key: key.clone(),
                expected_sha256: "sentinel-expected-digest".to_owned(),
                actual_sha256: "sentinel-actual-digest".to_owned(),
            },
            nokv_object::ObjectError::DigestMismatch {
                key: key.clone(),
                expected_sha256: "sentinel-expected-digest".to_owned(),
                actual_sha256: "sentinel-actual-digest".to_owned(),
            },
            nokv_object::ObjectError::InvalidManifest(
                "endpoint=https://sentinel.invalid bucket=sentinel-private key=nokv/system"
                    .to_owned(),
            ),
            nokv_object::ObjectError::CreateAmbiguous {
                key: key.clone(),
                detail: "endpoint=https://sentinel.invalid bucket=sentinel-private".to_owned(),
            },
            nokv_object::ObjectError::DeleteAmbiguous {
                key,
                detail: "endpoint=https://sentinel.invalid bucket=sentinel-private".to_owned(),
            },
        ];
        for error in errors {
            let mapped = map_client_error(ClientError::Object(error));
            assert_eq!(
                mapped.kind,
                agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
            );
            assert!(!mapped.message.contains("sentinel"));
            assert!(!mapped.message.contains("endpoint"));
            assert!(!mapped.message.contains("bucket"));
            assert!(!mapped.message.contains("nokv/system"));
        }
    }

    #[test]
    fn restore_not_found_is_snapshot_specific_only_for_prepare_lifecycle_failures() {
        let prepare_not_found = |conflict| {
            RestoreWorkflowError::Prepare(ClientError::Rpc(wire::RpcFailure {
                code: wire::ErrorCode::NotFound,
                message: "missing durable restore input".to_owned(),
                retryable: false,
                conflict: Some(conflict),
                current_generation: None,
                route_hint: None,
            }))
        };
        assert_eq!(
            map_lifecycle_error(WorkbenchLifecycleError::RestoreWorkflow(Box::new(
                prepare_not_found(wire::ConflictKind::SnapshotLifecycle),
            )))
            .kind,
            agent::BackendErrorKind::SnapshotNotFound
        );
        assert_eq!(
            map_lifecycle_error(WorkbenchLifecycleError::RestoreWorkflow(Box::new(
                prepare_not_found(wire::ConflictKind::Workspace),
            )))
            .kind,
            agent::BackendErrorKind::NotFound
        );
        assert_eq!(
            map_lifecycle_error(WorkbenchLifecycleError::RestoreWorkflow(Box::new(
                prepare_not_found(wire::ConflictKind::OperationState),
            )))
            .kind,
            agent::BackendErrorKind::NotFound
        );
    }
}
