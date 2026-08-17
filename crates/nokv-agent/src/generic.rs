/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Path-native implementation of the explicit seven-tool generic Agent profile.

use std::collections::BTreeMap;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use nokv_types::{
    ArtifactRevisionId, NormalizedRelativePath, RootId, WorkbenchId, WorkspaceIncarnationId,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::facade::{
    glob_matches, grep_query_commitment, optional_bool, optional_string, optional_usize,
    parse_grep_patterns, parse_measures, parse_predicates, parse_sort, parse_string_array,
    projected_kind_name, query_map_value, required_bool, required_string, validate_grep_glob,
};
use crate::{
    normalize_logical_workbench_root, AgentError, AggregateRequest, AggregateRow, ArtifactBody,
    ArtifactKind, CatalogField, CatalogPathMatch, CatalogRequest, FacetResult, FindRequest,
    GenericAgentToolHandler, GenericNamespaceHit, GrepCandidateReadFence, GrepCandidateRequest,
    ListEntry, ListPage, ListRequest, QueryPredicate, QueryProfile, QueryScope, QueryValue,
    ReadRequest, ReadView, ScopedPath, SearchPage, SearchRequest, Section, StatRecord,
    WorkbenchBackend, WorkbenchSummary, DEFAULT_WORKBENCH_MAX_BYTES,
};

const DEFAULT_LIST_LIMIT: usize = 100;
const DEFAULT_READ_LIMIT: usize = 100;
const DEFAULT_FIND_LIMIT: usize = 10;
const DEFAULT_AGGREGATE_LIMIT: usize = 20;
const DEFAULT_GREP_LIMIT: usize = 100;
const DEFAULT_GREP_MAX_PAGES: usize = 256;
const DEFAULT_GREP_MAX_FILES: usize = 10_000;
const DEFAULT_GREP_MAX_BYTES: u64 = 512 * 1024 * 1024;
const MAX_STRUCTURED_READ_RECORDS: usize = 300;
const STAT_SAMPLE_LIMIT: usize = 3;
const COUNT_SCAN_PAGE_LIMIT: usize = 300;
const MAX_COUNT_SCAN_RESULTS: usize = 1_000_000;
const MAX_COUNT_SCAN_PAGES: usize = 4096;
const CONSISTENT_READ_MAX_ATTEMPTS: usize = 3;
const GENERIC_GREP_CURSOR_VERSION: &[u8] = b"nokv.agent.generic.grep-cursor.v4\0";
const GENERIC_LIST_CURSOR_VERSION: &[u8] = b"nokv.agent.generic.list-cursor.v3\0";
const GENERIC_ROOT_LIST_CURSOR_VERSION: &[u8] = b"nokv.agent.generic.root-list-cursor.v3\0";
const GENERIC_FIND_CURSOR_VERSION: &[u8] = b"nokv.agent.generic.find-cursor.v3\0";
const MAX_GENERIC_GREP_CURSOR_BYTES: usize = 64 * 1024;
const MAX_GENERIC_BACKEND_CURSOR_BYTES: usize = 64 * 1024;

/// Per-call work bound for generic body grep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenericGrepScanLimits {
    max_pages: usize,
    max_files: usize,
    max_bytes: u64,
}

impl GenericGrepScanLimits {
    pub fn new(max_pages: usize, max_files: usize, max_bytes: u64) -> Result<Self, AgentError> {
        if max_pages == 0 || max_files == 0 || max_bytes == 0 {
            return Err(AgentError::invalid_arguments(
                "generic grep page, file, and byte limits must be greater than zero",
            ));
        }
        Ok(Self {
            max_pages,
            max_files,
            max_bytes,
        })
    }
}

impl Default for GenericGrepScanLimits {
    fn default() -> Self {
        Self {
            max_pages: DEFAULT_GREP_MAX_PAGES,
            max_files: DEFAULT_GREP_MAX_FILES,
            max_bytes: DEFAULT_GREP_MAX_BYTES,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AgentGrepLineMatch {
    pub path: ScopedPath,
    pub line_number: usize,
    pub snippet: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AgentGrepScanResult {
    pub matches: Vec<AgentGrepLineMatch>,
    pub files_scanned: usize,
    pub next_cursor: Option<String>,
}

pub(crate) struct AgentGrepScanRequest<'a> {
    pub storage_root_id: RootId,
    pub logical_root: &'a str,
    pub scope: QueryScope,
    pub patterns: &'a [String],
    pub glob: Option<&'a str>,
    pub recursive: bool,
    pub query_commitment: [u8; 32],
    pub cursor: Option<&'a str>,
    pub limit: usize,
    pub scan_limits: GenericGrepScanLimits,
}

/// Shared path-native body scanner for both public Agent profiles.
///
/// Candidate enumeration, immutable read authority, bounded work, and
/// continuation state stay in one implementation. Callers only own argument
/// compatibility and result projection.
pub(crate) fn scan_agent_grep<B: WorkbenchBackend>(
    backend: &B,
    request: AgentGrepScanRequest<'_>,
) -> Result<AgentGrepScanResult, AgentError> {
    let AgentGrepScanRequest {
        storage_root_id,
        logical_root,
        scope,
        patterns,
        glob,
        recursive,
        query_commitment,
        cursor,
        limit,
        scan_limits,
    } = request;
    let folded = patterns
        .iter()
        .map(|pattern| pattern.to_lowercase())
        .collect::<Vec<_>>();
    let projected_scope = projected_query_scope_path(logical_root, &scope);
    let cursor_commitment =
        generic_grep_cursor_commitment(storage_root_id, logical_root, query_commitment);
    let resume = cursor
        .map(|cursor| decode_generic_grep_cursor(cursor, cursor_commitment))
        .transpose()?
        .unwrap_or_default();
    if let Some(pending) = resume.pending.as_ref() {
        if !grep_candidate_is_in_scope(&pending.fence, &scope, recursive)
            || !grep_candidate_matches_glob(&pending.fence, glob)
        {
            return Err(AgentError::invalid_arguments(
                "grep cursor candidate is outside the requested scope",
            ));
        }
    }
    let mut backend_cursor = resume.backend_cursor;
    let mut pending = resume.pending;
    let mut matches = Vec::new();
    let mut files_scanned = 0_usize;
    let mut pages_scanned = 0_usize;
    let mut bytes_scanned = 0_u64;
    let output = |matches: Vec<AgentGrepLineMatch>,
                  files_scanned: usize,
                  next_cursor: Option<String>| AgentGrepScanResult {
        matches,
        files_scanned,
        next_cursor,
    };

    loop {
        if let Some(resume) = pending.take() {
            let candidate_path = projected_scoped_path(logical_root, &resume.fence.path);
            let metadata = backend
                .grep_candidate_metadata(&resume.fence)
                .map_err(|error| project_grep_path_error(error.into(), &candidate_path))?;
            if metadata.size_bytes > scan_limits.max_bytes {
                return Err(grep_scan_exhausted("bytes", scan_limits.max_bytes));
            }
            let body = backend
                .read_grep_candidate(&resume.fence)
                .map_err(|error| project_grep_path_error(error.into(), &candidate_path))?;
            if body.path != resume.fence.path || body.metadata != metadata {
                return Err(read_fence_changed("grep"));
            }
            files_scanned += 1;
            bytes_scanned = metadata.size_bytes;
            if !body.bytes.contains(&0) {
                let text = String::from_utf8_lossy(&body.bytes);
                for (line_index, line) in text.lines().enumerate().skip(resume.line_index) {
                    let lower = line.to_lowercase();
                    if folded.iter().any(|pattern| lower.contains(pattern)) {
                        if matches.len() == limit {
                            let next_cursor = encode_generic_grep_cursor(
                                cursor_commitment,
                                &GenericGrepCursor {
                                    backend_cursor: None,
                                    pending: Some(GenericGrepPendingCandidate {
                                        fence: resume.fence,
                                        cursor_after: resume.cursor_after,
                                        line_index,
                                    }),
                                },
                            )?;
                            return Ok(output(matches, files_scanned, Some(next_cursor)));
                        }
                        matches.push(AgentGrepLineMatch {
                            path: body.path.clone(),
                            line_number: line_index + 1,
                            snippet: line.chars().take(240).collect(),
                        });
                    }
                }
            }
            backend_cursor = resume.cursor_after;
            if backend_cursor.is_none() {
                break;
            }
        }

        if pages_scanned >= scan_limits.max_pages {
            let next_cursor = encode_generic_grep_cursor(
                cursor_commitment,
                &GenericGrepCursor {
                    backend_cursor,
                    pending: None,
                },
            )?;
            return Ok(output(matches, files_scanned, Some(next_cursor)));
        }
        let page_cursor = backend_cursor.clone();
        let page = backend
            .grep_candidates(GrepCandidateRequest {
                scope: scope.clone(),
                recursive,
                query_commitment,
                cursor: page_cursor,
                limit: 300,
            })
            .map_err(|error| project_grep_path_error(error.into(), &projected_scope))?;
        pages_scanned += 1;
        for candidate in page.candidates {
            let candidate_fence = candidate.read_fence();
            if !grep_candidate_is_in_scope(&candidate_fence, &scope, recursive) {
                return Err(protocol_mismatch(
                    "grep backend returned a candidate outside the requested scope",
                ));
            }
            if !grep_candidate_matches_glob(&candidate_fence, glob) {
                continue;
            }
            if files_scanned >= scan_limits.max_files {
                let next_cursor = encode_generic_grep_cursor(
                    cursor_commitment,
                    &GenericGrepCursor {
                        backend_cursor: None,
                        pending: Some(GenericGrepPendingCandidate {
                            fence: candidate.read_fence(),
                            cursor_after: candidate.cursor_after,
                            line_index: 0,
                        }),
                    },
                )?;
                return Ok(output(matches, files_scanned, Some(next_cursor)));
            }
            let next_bytes_scanned = bytes_scanned.checked_add(candidate.metadata.size_bytes);
            let candidate_fits =
                next_bytes_scanned.is_some_and(|bytes| bytes <= scan_limits.max_bytes);
            if !candidate_fits && files_scanned == 0 {
                return Err(grep_scan_exhausted("bytes", scan_limits.max_bytes));
            }
            if !candidate_fits {
                let next_cursor = encode_generic_grep_cursor(
                    cursor_commitment,
                    &GenericGrepCursor {
                        backend_cursor: None,
                        pending: Some(GenericGrepPendingCandidate {
                            fence: candidate.read_fence(),
                            cursor_after: candidate.cursor_after,
                            line_index: 0,
                        }),
                    },
                )?;
                return Ok(output(matches, files_scanned, Some(next_cursor)));
            }
            let candidate_path = projected_scoped_path(logical_root, &candidate.path);
            let body = backend
                .read_grep_candidate(&candidate.read_fence())
                .map_err(|error| project_grep_path_error(error.into(), &candidate_path))?;
            if body.path != candidate.path || body.metadata != candidate.metadata {
                return Err(read_fence_changed("grep"));
            }
            files_scanned += 1;
            bytes_scanned = next_bytes_scanned.expect("candidate fit requires byte total");
            if body.bytes.contains(&0) {
                continue;
            }
            let text = String::from_utf8_lossy(&body.bytes);
            for (line_index, line) in text.lines().enumerate() {
                let lower = line.to_lowercase();
                if folded.iter().any(|pattern| lower.contains(pattern)) {
                    if matches.len() == limit {
                        let next_cursor = encode_generic_grep_cursor(
                            cursor_commitment,
                            &GenericGrepCursor {
                                backend_cursor: None,
                                pending: Some(GenericGrepPendingCandidate {
                                    fence: candidate.read_fence(),
                                    cursor_after: candidate.cursor_after,
                                    line_index,
                                }),
                            },
                        )?;
                        return Ok(output(matches, files_scanned, Some(next_cursor)));
                    }
                    matches.push(AgentGrepLineMatch {
                        path: candidate.path.clone(),
                        line_number: line_index + 1,
                        snippet: line.chars().take(240).collect(),
                    });
                }
            }
        }
        let Some(next) = page.next_cursor else {
            break;
        };
        if backend_cursor.as_ref() == Some(&next) {
            return Err(AgentError::backend(
                "BackendProtocolMismatch",
                "grep candidate cursor did not advance",
                true,
                json!({"cursor": next}),
            ));
        }
        if pages_scanned >= scan_limits.max_pages {
            let next_cursor = encode_generic_grep_cursor(
                cursor_commitment,
                &GenericGrepCursor {
                    backend_cursor: Some(next),
                    pending: None,
                },
            )?;
            return Ok(output(matches, files_scanned, Some(next_cursor)));
        }
        backend_cursor = Some(next);
    }
    Ok(output(matches, files_scanned, None))
}

#[derive(Clone)]
pub struct SdkGenericAgentToolHandler<B> {
    backend: B,
    storage_root_id: RootId,
    max_bytes: usize,
    grep_scan_limits: GenericGrepScanLimits,
    logical_root: String,
}

impl<B> SdkGenericAgentToolHandler<B> {
    pub fn new(backend: B, logical_root: &str) -> Result<Self, AgentError>
    where
        B: WorkbenchBackend,
    {
        Self::with_max_bytes(backend, DEFAULT_WORKBENCH_MAX_BYTES, logical_root)
    }

    pub fn with_max_bytes(
        backend: B,
        max_bytes: usize,
        logical_root: &str,
    ) -> Result<Self, AgentError>
    where
        B: WorkbenchBackend,
    {
        Self::with_limits(
            backend,
            max_bytes,
            GenericGrepScanLimits::default(),
            logical_root,
        )
    }

    pub fn with_limits(
        backend: B,
        max_bytes: usize,
        grep_scan_limits: GenericGrepScanLimits,
        logical_root: &str,
    ) -> Result<Self, AgentError>
    where
        B: WorkbenchBackend,
    {
        let storage_root_id = backend.storage_root_id();
        Ok(Self {
            backend,
            storage_root_id,
            max_bytes,
            grep_scan_limits,
            logical_root: normalize_logical_workbench_root(logical_root)?,
        })
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn logical_root(&self) -> &str {
        &self.logical_root
    }

    fn workbench_path(&self, workbench_id: &WorkbenchId) -> String {
        format!("{}/{}", self.logical_root, workbench_id.as_str())
    }

    fn projected_path(&self, path: &ScopedPath) -> String {
        projected_scoped_path(&self.logical_root, path)
    }

    fn query_path(&self, scope: &QueryScope) -> String {
        let Some(workbench_id) = &scope.workbench_id else {
            return self.logical_root.clone();
        };
        self.projected_path(&ScopedPath {
            workbench_id: workbench_id.clone(),
            section: scope.section,
            relative_path: scope.path.clone(),
        })
    }

    fn parse_path(&self, raw: &str) -> Result<GenericPath, AgentError> {
        if raw == "/" || raw == self.logical_root {
            return Ok(GenericPath::Root);
        }
        if raw.contains('\\') || raw.contains('\0') || !raw.starts_with('/') {
            return Err(AgentError::invalid_arguments(
                "path must be an absolute path without backslashes or NUL",
            ));
        }
        let relative = raw
            .strip_prefix(&self.logical_root)
            .and_then(|suffix| suffix.strip_prefix('/'))
            .ok_or_else(|| {
                AgentError::invalid_arguments(format!(
                    "path must be / or descend from {}",
                    self.logical_root
                ))
            })?;
        if relative.is_empty() || relative.ends_with('/') || relative.contains("//") {
            return Err(AgentError::invalid_arguments(
                "path must not contain empty components",
            ));
        }
        let components = relative.split('/').collect::<Vec<_>>();
        if components
            .iter()
            .any(|component| component.is_empty() || matches!(*component, "." | ".."))
        {
            return Err(AgentError::invalid_arguments(
                "path must not contain empty, '.' or '..' components",
            ));
        }
        let workbench_id = WorkbenchId::new(components[0].to_owned())
            .map_err(|error| AgentError::invalid_arguments(error.to_string()))?;
        let section = components
            .get(1)
            .and_then(|value| Section::parse(value).ok());
        let relative_components = if section.is_some() {
            &components[2..]
        } else {
            &components[1..]
        };
        let relative_path = (!relative_components.is_empty())
            .then(|| NormalizedRelativePath::new(relative_components.join("/")))
            .transpose()
            .map_err(|error| AgentError::invalid_arguments(error.to_string()))?;
        Ok(GenericPath::Scoped(ScopedPath {
            workbench_id,
            section,
            relative_path,
        }))
    }

    fn parse_argument_path(&self, arguments: &Value) -> Result<GenericPath, AgentError> {
        self.parse_path(required_string(arguments, "path")?)
    }

    fn query_scope(&self, path: &GenericPath) -> QueryScope {
        match path {
            GenericPath::Root => QueryScope {
                workbench_id: None,
                section: None,
                path: None,
            },
            GenericPath::Scoped(path) => QueryScope {
                workbench_id: Some(path.workbench_id.clone()),
                section: path.section,
                path: path.relative_path.clone(),
            },
        }
    }

    fn require_directory_at_read_version(
        &self,
        path: &GenericPath,
        read_version: u64,
    ) -> Result<(), AgentError>
    where
        B: WorkbenchBackend,
    {
        let GenericPath::Scoped(path) = path else {
            return Ok(());
        };
        let projected = self.projected_path(path);
        let record = self
            .backend
            .stat_at_read_version(path, read_version)?
            .ok_or_else(|| not_found(&projected))?;
        if record.kind == ArtifactKind::Artifact {
            return Err(not_directory(&projected));
        }
        Ok(())
    }

    fn list(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let path = self.parse_argument_path(arguments)?;
        let cursor = optional_string(arguments, "cursor")?.map(str::to_owned);
        let limit = optional_usize(arguments, "limit")?.unwrap_or(DEFAULT_LIST_LIMIT);
        match path {
            GenericPath::Root => {
                let (page, entry_count) = self.find_workbenches_with_total(FindRequest {
                    committed: None,
                    manifest_pattern: None,
                    include_manifest: false,
                    cursor,
                    limit,
                })?;
                let entries = page
                    .workbenches
                    .iter()
                    .map(|summary| self.workbench_list_entry(summary))
                    .collect::<Vec<_>>();
                Ok(json!({
                    "path": self.logical_root,
                    "entry_count": entry_count,
                    "entries": entries,
                    "next_cursor": page.next_cursor,
                    "truncated": page.next_cursor.is_some(),
                }))
            }
            GenericPath::Scoped(path) => {
                for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
                    match self.scoped_list(&path, cursor.clone(), limit) {
                        Err(error)
                            if attempt < CONSISTENT_READ_MAX_ATTEMPTS
                                && is_read_fence_changed(&error) =>
                        {
                            continue;
                        }
                        result => return result,
                    }
                }
                Err(read_fence_changed("ls"))
            }
        }
    }

    fn scoped_list(
        &self,
        path: &ScopedPath,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let projected = self.projected_path(path);
        let (request, cursor_commitment) = self.scoped_list_request(path.clone(), cursor, limit)?;
        let initial = self
            .backend
            .stat(path, &ReadView::Live)?
            .ok_or_else(|| not_found(&projected))?;
        if initial.kind == ArtifactKind::Artifact {
            return Err(not_directory(&projected));
        }
        let (mut page, entry_count) = self.list_backend_with_total(request)?;
        page.next_cursor = page.next_cursor.map(|cursor| {
            encode_generic_committed_backend_cursor(
                GENERIC_LIST_CURSOR_VERSION,
                cursor_commitment,
                &cursor,
            )
        });
        let record = self
            .backend
            .stat_at_read_version(path, page.read_version)?
            .ok_or_else(|| not_found(&projected))?;
        if record.kind == ArtifactKind::Artifact {
            return Err(not_directory(&projected));
        }
        let entries = page
            .entries
            .iter()
            .map(|entry| self.list_entry(entry))
            .collect::<Vec<_>>();
        Ok(json!({
            "path": projected,
            "entry_count": entry_count,
            "entries": entries,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor.is_some(),
        }))
    }

    fn list_with_total(&self, request: ListRequest) -> Result<(ListPage, usize), AgentError>
    where
        B: WorkbenchBackend,
    {
        let (request, cursor_commitment) =
            self.scoped_list_request(request.path, request.cursor, request.limit)?;
        let (mut page, total) = self.list_backend_with_total(request)?;
        page.next_cursor = page.next_cursor.map(|cursor| {
            encode_generic_committed_backend_cursor(
                GENERIC_LIST_CURSOR_VERSION,
                cursor_commitment,
                &cursor,
            )
        });
        Ok((page, total))
    }

    fn scoped_list_request(
        &self,
        path: ScopedPath,
        cursor: Option<String>,
        limit: usize,
    ) -> Result<(ListRequest, [u8; 32]), AgentError> {
        let mut request = ListRequest {
            path,
            view: ReadView::Live,
            cursor,
            limit,
        };
        let commitment = generic_scoped_list_cursor_commitment(
            self.storage_root_id,
            &self.logical_root,
            &request,
        );
        request.cursor = request
            .cursor
            .as_deref()
            .map(|cursor| {
                decode_generic_committed_backend_cursor(
                    cursor,
                    GENERIC_LIST_CURSOR_VERSION,
                    commitment,
                )
            })
            .transpose()?
            .map(|cursor| cursor.backend_cursor);
        Ok((request, commitment))
    }

    fn list_backend_with_total(&self, request: ListRequest) -> Result<(ListPage, usize), AgentError>
    where
        B: WorkbenchBackend,
    {
        if request.cursor.is_some() {
            // The backend continuation owns the read fence. Recompute the
            // authoritative total before consuming that continuation, then
            // let the backend reject it if the fenced state changed.
            let (_, total) = self.list_backend_with_total(ListRequest {
                cursor: None,
                limit: COUNT_SCAN_PAGE_LIMIT,
                ..request.clone()
            })?;
            let page = self.backend.list(request)?;
            return Ok((page, total));
        }
        let first = self.backend.list(request.clone())?;
        let mut total = first.entries.len();
        let mut cursor = first.next_cursor.clone();
        let mut pages = 1_usize;
        while let Some(next) = cursor {
            pages = pages.saturating_add(1);
            if pages > MAX_COUNT_SCAN_PAGES || total > MAX_COUNT_SCAN_RESULTS {
                return Err(count_scan_exhausted("list"));
            }
            let page = self.backend.list(ListRequest {
                path: request.path.clone(),
                view: request.view.clone(),
                cursor: Some(next.clone()),
                limit: COUNT_SCAN_PAGE_LIMIT,
            })?;
            if page.read_version != first.read_version {
                return Err(read_fence_changed("list"));
            }
            if page.next_cursor.as_ref() == Some(&next) {
                return Err(protocol_mismatch("list cursor did not advance"));
            }
            total = total.checked_add(page.entries.len()).ok_or_else(|| {
                AgentError::backend(
                    "ResourceExhausted",
                    "list total exceeds the supported result count",
                    false,
                    json!({"maximum_results": MAX_COUNT_SCAN_RESULTS}),
                )
            })?;
            cursor = page.next_cursor;
        }
        if total > MAX_COUNT_SCAN_RESULTS {
            return Err(count_scan_exhausted("list"));
        }
        Ok((first, total))
    }

    fn search_page(&self, mut request: SearchRequest) -> Result<SearchPage, AgentError>
    where
        B: WorkbenchBackend,
    {
        let commitment = generic_query_cursor_commitment(self.storage_root_id, &self.logical_root);
        request.cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_generic_query_cursor(cursor, commitment))
            .transpose()?
            .map(|cursor| cursor.backend_cursor);
        let mut page = self.backend.search(request)?;
        page.next_cursor = page
            .next_cursor
            .map(|cursor| encode_generic_query_cursor(commitment, &cursor));
        Ok(page)
    }

    fn find_workbenches_with_total(
        &self,
        request: FindRequest,
    ) -> Result<(crate::FindPage, usize), AgentError>
    where
        B: WorkbenchBackend,
    {
        let cursor_commitment =
            generic_root_list_cursor_commitment(self.storage_root_id, &self.logical_root, &request);
        let continued = request
            .cursor
            .as_deref()
            .map(|cursor| {
                decode_generic_committed_backend_cursor(
                    cursor,
                    GENERIC_ROOT_LIST_CURSOR_VERSION,
                    cursor_commitment,
                )
            })
            .transpose()?;
        let backend_cursor = continued
            .as_ref()
            .map(|cursor| cursor.backend_cursor.clone());
        if continued.is_some() {
            let (_, total) = self.find_workbenches_with_total(FindRequest {
                cursor: None,
                limit: COUNT_SCAN_PAGE_LIMIT,
                ..request.clone()
            })?;
            let mut page = self.backend.find_workbenches(FindRequest {
                cursor: backend_cursor,
                ..request
            })?;
            page.next_cursor = page.next_cursor.map(|cursor| {
                encode_generic_committed_backend_cursor(
                    GENERIC_ROOT_LIST_CURSOR_VERSION,
                    cursor_commitment,
                    &cursor,
                )
            });
            return Ok((page, total));
        }
        let mut first = self.backend.find_workbenches(FindRequest {
            cursor: backend_cursor,
            ..request.clone()
        })?;
        let mut total = first.workbenches.len();
        let mut cursor = first.next_cursor.clone();
        let mut pages = 1_usize;
        while let Some(next) = cursor {
            pages = pages.saturating_add(1);
            if pages > MAX_COUNT_SCAN_PAGES || total > MAX_COUNT_SCAN_RESULTS {
                return Err(count_scan_exhausted("ls"));
            }
            let page = self.backend.find_workbenches(FindRequest {
                cursor: Some(next.clone()),
                limit: COUNT_SCAN_PAGE_LIMIT,
                ..request.clone()
            })?;
            if page.read_version != first.read_version {
                return Err(read_fence_changed("ls"));
            }
            if page.next_cursor.as_ref() == Some(&next) {
                return Err(protocol_mismatch("workbench list cursor did not advance"));
            }
            total = total.saturating_add(page.workbenches.len());
            cursor = page.next_cursor;
        }
        if total > MAX_COUNT_SCAN_RESULTS {
            return Err(count_scan_exhausted("ls"));
        }
        first.next_cursor = first.next_cursor.map(|cursor| {
            encode_generic_committed_backend_cursor(
                GENERIC_ROOT_LIST_CURSOR_VERSION,
                cursor_commitment,
                &cursor,
            )
        });
        Ok((first, total))
    }

    fn load_catalog(
        &self,
        scope: &QueryScope,
        path_match: CatalogPathMatch,
        field_prefix: Option<&str>,
        include_facets: bool,
        expected_read_version: Option<u64>,
    ) -> Result<GenericCatalog, AgentError>
    where
        B: WorkbenchBackend,
    {
        let raw = self.backend.catalog(CatalogRequest {
            profile: QueryProfile::GenericNamespaceV1 {
                presentation_path_root: self.logical_root.clone(),
            },
            scope: scope.clone(),
            path_match,
            field_prefix: field_prefix.map(str::to_owned),
            include_facets,
        })?;
        if expected_read_version.is_some_and(|expected| expected != raw.read_version) {
            return Err(read_fence_changed("catalog"));
        }
        Ok(GenericCatalog {
            fields: raw.fields,
            facets: raw.facets,
            read_version: raw.read_version,
        })
    }

    fn child_catalogs(
        &self,
        path: &GenericPath,
        include_facets: bool,
        read_version: u64,
    ) -> Result<Vec<Value>, AgentError>
    where
        B: WorkbenchBackend,
    {
        let children = match path {
            GenericPath::Root => {
                let page = self.backend.find_workbenches(FindRequest {
                    committed: None,
                    manifest_pattern: None,
                    include_manifest: false,
                    cursor: None,
                    limit: 20,
                })?;
                if page.read_version != read_version {
                    return Err(read_fence_changed("catalog"));
                }
                page.workbenches
                    .into_iter()
                    .map(|summary| {
                        GenericPath::Scoped(ScopedPath {
                            workbench_id: summary.workbench_id,
                            section: None,
                            relative_path: None,
                        })
                    })
                    .collect::<Vec<_>>()
            }
            GenericPath::Scoped(path) => {
                let page = self.backend.list(ListRequest {
                    path: path.clone(),
                    view: ReadView::Live,
                    cursor: None,
                    limit: 20,
                })?;
                if page.read_version != read_version {
                    return Err(read_fence_changed("catalog"));
                }
                page.entries
                    .into_iter()
                    .filter(|entry| entry.kind != ArtifactKind::Artifact)
                    .map(|entry| GenericPath::Scoped(entry.path))
                    .collect::<Vec<_>>()
            }
        };
        let mut output = Vec::new();
        for child in children {
            let scope = self.query_scope(&child);
            let catalog = self.load_catalog(
                &scope,
                CatalogPathMatch::Prefix,
                None,
                include_facets,
                Some(read_version),
            )?;
            if catalog_is_empty(&catalog.fields) {
                continue;
            }
            output.push(json!({
                "path": self.query_path(&scope),
                "catalog": catalog_value(&catalog.fields, &catalog.facets),
            }));
            if output.len() == 5 {
                break;
            }
        }
        Ok(output)
    }

    fn stat(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        match self.parse_argument_path(arguments)? {
            GenericPath::Root => {
                for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
                    let result = (|| {
                        let (page, entry_count) =
                            self.find_workbenches_with_total(FindRequest {
                                committed: None,
                                manifest_pattern: None,
                                include_manifest: false,
                                cursor: None,
                                limit: STAT_SAMPLE_LIMIT,
                            })?;
                        let catalog = self.load_catalog(
                            &QueryScope {
                                workbench_id: None,
                                section: None,
                                path: None,
                            },
                            CatalogPathMatch::Prefix,
                            None,
                            true,
                            Some(page.read_version),
                        )?;
                        Ok(json!({
                            "card": self.root_card(
                                entry_count,
                                page.workbenches
                                    .iter()
                                    .take(STAT_SAMPLE_LIMIT)
                                    .map(|summary| summary.workbench_id.as_str().to_owned())
                                    .collect(),
                                &catalog,
                            ),
                        }))
                    })();
                    match result {
                        Err(error)
                            if attempt < CONSISTENT_READ_MAX_ATTEMPTS
                                && is_read_fence_changed(&error) =>
                        {
                            continue;
                        }
                        result => return result,
                    }
                }
                Err(read_fence_changed("stat"))
            }
            GenericPath::Scoped(path) => {
                for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
                    match self.scoped_stat(&path) {
                        Err(error)
                            if attempt < CONSISTENT_READ_MAX_ATTEMPTS
                                && is_read_fence_changed(&error) =>
                        {
                            continue;
                        }
                        result => return result,
                    }
                }
                Err(read_fence_changed("stat"))
            }
        }
    }

    fn scoped_stat(&self, path: &ScopedPath) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let scope = self.query_scope(&GenericPath::Scoped(path.clone()));
        let anchor_match = catalog_anchor_match(&GenericPath::Scoped(path.clone()));
        let anchor = self.load_catalog(&scope, anchor_match, None, true, None)?;
        let record = self
            .backend
            .stat_at_read_version(path, anchor.read_version)?
            .ok_or_else(|| not_found(&self.projected_path(path)))?;
        let catalog =
            if record.kind == ArtifactKind::Artifact || anchor_match == CatalogPathMatch::Prefix {
                anchor
            } else {
                self.load_catalog(
                    &scope,
                    CatalogPathMatch::Prefix,
                    None,
                    true,
                    Some(anchor.read_version),
                )?
            };
        let structured = if record.kind == ArtifactKind::Artifact
            && record
                .artifact
                .as_ref()
                .is_some_and(|metadata| generic_structured_artifact(path, metadata))
        {
            let authority = record.authority.ok_or_else(|| {
                protocol_mismatch("artifact stat omitted its exact read authority")
            })?;
            let body = self.backend.read_grep_candidate(&GrepCandidateReadFence {
                path: path.clone(),
                authority,
            })?;
            if body.path != *path || record.artifact.as_ref() != Some(&body.metadata) {
                return Err(read_fence_changed("stat"));
            }
            Some(generic_structured_records(
                &body,
                &self.projected_path(path),
            )?)
        } else {
            None
        };
        let directory = if record.kind == ArtifactKind::Artifact {
            None
        } else {
            let (page, entry_count) = self.list_with_total(ListRequest {
                path: path.clone(),
                view: ReadView::Live,
                cursor: None,
                limit: STAT_SAMPLE_LIMIT,
            })?;
            if page.read_version != catalog.read_version {
                return Err(read_fence_changed("stat"));
            }
            Some(self.directory_summary(&record, &page, entry_count))
        };
        Ok(json!({
            "card": self.stat_card(
                &record,
                &catalog,
                structured.as_ref(),
                directory.as_ref(),
            )
        }))
    }

    fn directory_summary(
        &self,
        _record: &StatRecord,
        page: &ListPage,
        entry_count: usize,
    ) -> GenericDirectorySummary {
        GenericDirectorySummary {
            entry_count,
            sample: page
                .entries
                .iter()
                .map(list_entry_name)
                .take(STAT_SAMPLE_LIMIT)
                .collect(),
        }
    }

    fn catalog(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let path = self.parse_argument_path(arguments)?;
        let scope = self.query_scope(&path);
        let projected = self.query_path(&scope);
        let field_prefix = optional_string(arguments, "field_prefix")?;
        let include_facets = optional_bool(arguments, "include_facets")?.unwrap_or(false);
        for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
            match self.catalog_once(&path, &scope, &projected, field_prefix, include_facets) {
                Err(error)
                    if attempt < CONSISTENT_READ_MAX_ATTEMPTS && is_read_fence_changed(&error) =>
                {
                    continue;
                }
                result => return result,
            }
        }
        Err(read_fence_changed("catalog"))
    }

    fn catalog_once(
        &self,
        path: &GenericPath,
        scope: &QueryScope,
        projected: &str,
        field_prefix: Option<&str>,
        include_facets: bool,
    ) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let anchor_match = catalog_anchor_match(path);
        let anchor = self.load_catalog(scope, anchor_match, field_prefix, include_facets, None)?;
        let (result, directory) = match path {
            GenericPath::Root => (anchor, true),
            GenericPath::Scoped(scoped) => {
                let record = self
                    .backend
                    .stat_at_read_version(scoped, anchor.read_version)?
                    .ok_or_else(|| not_found(&self.projected_path(scoped)))?;
                if record.kind == ArtifactKind::Artifact || anchor_match == CatalogPathMatch::Prefix
                {
                    (anchor, record.kind != ArtifactKind::Artifact)
                } else {
                    let prefix = self.load_catalog(
                        scope,
                        CatalogPathMatch::Prefix,
                        field_prefix,
                        include_facets,
                        Some(anchor.read_version),
                    )?;
                    (prefix, true)
                }
            }
        };
        let catalog_empty = catalog_is_empty(&result.fields);
        let child_catalogs = if catalog_empty && directory {
            self.child_catalogs(path, include_facets, result.read_version)?
        } else {
            Vec::new()
        };
        Ok(json!({
            "path": projected,
            "catalog_empty": catalog_empty,
            "catalog": catalog_value(&result.fields, &result.facets),
            "child_catalogs": child_catalogs,
        }))
    }

    fn read(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let path = match self.parse_argument_path(arguments)? {
            GenericPath::Root => {
                return Err(AgentError::invalid_arguments(
                    "read requires an artifact path",
                ));
            }
            GenericPath::Scoped(path) => path,
        };
        let format = optional_string(arguments, "format")?.unwrap_or("structured");
        let limit = optional_usize(arguments, "limit")?.unwrap_or(DEFAULT_READ_LIMIT);
        let inspection = self.backend.inspect_artifact(ReadRequest {
            path: path.clone(),
            view: ReadView::Live,
        })?;
        let inspection = match inspection {
            Some(inspection) => inspection,
            None => match self.backend.stat(&path, &ReadView::Live)? {
                Some(record) if record.kind != ArtifactKind::Artifact => {
                    return Err(AgentError::invalid_arguments(
                        "read requires an artifact path",
                    ));
                }
                Some(_) => {
                    return Err(protocol_mismatch(
                        "artifact stat succeeded after its exact inspection was absent",
                    ));
                }
                None => return Err(not_found(&self.projected_path(&path))),
            },
        };
        let artifact = inspection.artifact;
        if artifact.bytes.len() > self.max_bytes {
            return Err(AgentError::backend(
                "PayloadTooLarge",
                format!(
                    "artifact is {} bytes, maximum is {}",
                    artifact.bytes.len(),
                    self.max_bytes
                ),
                false,
                json!({"actual_bytes": artifact.bytes.len(), "maximum_bytes": self.max_bytes}),
            ));
        }
        let structured = if format == "structured" {
            let records = generic_structured_records(&artifact, &self.projected_path(&path))?;
            if records.items.len() > MAX_STRUCTURED_READ_RECORDS {
                return Err(AgentError::invalid_arguments(format!(
                    "structured pagination for {} has {} records; use bytes format with offset and limit, grep to locate lines, or stat record_count",
                    self.projected_path(&path),
                    records.items.len()
                )));
            }
            Some(records)
        } else {
            None
        };
        self.read_value(arguments, &path, artifact, format, limit, structured)
    }

    fn read_value(
        &self,
        arguments: &Value,
        path: &ScopedPath,
        artifact: ArtifactBody,
        format: &str,
        limit: usize,
        structured: Option<GenericStructuredRecords>,
    ) -> Result<Value, AgentError> {
        let requested_cursor = optional_string(arguments, "cursor")?.map(str::to_owned);
        let explicit_offset = optional_usize(arguments, "offset")?.unwrap_or(0);
        let start = match requested_cursor.as_deref() {
            Some(_) if explicit_offset != 0 => {
                return Err(AgentError::invalid_arguments(
                    "offset and cursor cannot both select a position",
                ));
            }
            Some(cursor) => cursor
                .parse::<usize>()
                .map_err(|_| AgentError::invalid_arguments("cursor offset is invalid"))?,
            None => explicit_offset,
        };
        let projected = self.projected_path(path);
        match format {
            "bytes" => {
                if start > artifact.bytes.len() {
                    return Err(AgentError::invalid_arguments(
                        "byte offset is past the end of the artifact",
                    ));
                }
                let end = start.saturating_add(limit).min(artifact.bytes.len());
                let next_cursor = (end < artifact.bytes.len()).then(|| end.to_string());
                Ok(json!({
                    "path": projected,
                    "generation": artifact.metadata.generation,
                    "total_size_bytes": artifact.metadata.size_bytes,
                    "format": "bytes",
                    "record_type": Value::Null,
                    "record_count": Value::Null,
                    "cursor": requested_cursor,
                    "next_cursor": next_cursor,
                    "truncated": next_cursor.is_some(),
                    "items": [],
                    "bytes": artifact.bytes[start..end],
                }))
            }
            "structured" => {
                let records = structured.expect("structured reads parse records before paging");
                if start > records.items.len() {
                    return Err(AgentError::invalid_arguments(
                        "structured cursor is past the end of the artifact",
                    ));
                }
                let end = start.saturating_add(limit).min(records.items.len());
                let next_cursor = (end < records.items.len()).then(|| end.to_string());
                let items = records.items[start..end]
                    .iter()
                    .enumerate()
                    .map(|(offset, value)| json!({"index": start + offset, "value": value}))
                    .collect::<Vec<_>>();
                Ok(json!({
                    "path": projected,
                    "generation": artifact.metadata.generation,
                    "total_size_bytes": artifact.metadata.size_bytes,
                    "format": "structured",
                    "record_type": records.record_type,
                    "record_count": records.items.len(),
                    "cursor": requested_cursor,
                    "next_cursor": next_cursor,
                    "truncated": next_cursor.is_some(),
                    "items": items,
                    "bytes": Value::Null,
                }))
            }
            _ => Err(AgentError::invalid_arguments("unknown read format")),
        }
    }

    fn find(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let path = self.parse_argument_path(arguments)?;
        let scope = self.query_scope(&path);
        let projected = self.query_path(&scope);
        let fields_requested = arguments.get("fields").is_some();
        let fields = parse_string_array(arguments, "fields")?;
        let mut predicates = parse_predicates(arguments)?;
        normalize_generic_boolean_predicates(&mut predicates);
        let request = SearchRequest {
            profile: QueryProfile::GenericNamespaceV1 {
                presentation_path_root: self.logical_root.clone(),
            },
            scope,
            predicates,
            fields: fields.clone(),
            sort: parse_sort(arguments)?,
            facets: parse_string_array(arguments, "facets")?,
            cursor: optional_string(arguments, "cursor")?.map(str::to_owned),
            limit: optional_usize(arguments, "limit")?.unwrap_or(DEFAULT_FIND_LIMIT),
        };
        let max_attempts = if request.cursor.is_some() {
            1
        } else {
            CONSISTENT_READ_MAX_ATTEMPTS
        };
        let page = 'consistent: {
            for attempt in 1..=max_attempts {
                let result = self.search_page(request.clone()).and_then(|result| {
                    self.require_directory_at_read_version(&path, result.read_version)?;
                    Ok(result)
                });
                match result {
                    Err(error) if attempt < max_attempts && is_read_fence_changed(&error) => {
                        continue;
                    }
                    result => break 'consistent result?,
                }
            }
            return Err(read_fence_changed("find"));
        };
        let matches = page
            .namespace_hits
            .iter()
            .map(|hit| {
                let hit_path = generic_namespace_scoped_path(hit)?;
                let path = self.projected_path(&hit_path);
                if fields_requested {
                    Ok(json!({
                        "path": path,
                        "values": generic_projected_values(hit, &fields, &path),
                    }))
                } else {
                    Ok(json!({"path": path}))
                }
            })
            .collect::<Result<Vec<_>, AgentError>>()?;
        let facets = page
            .facets
            .iter()
            .map(|facet| {
                json!({
                    "field": facet.field,
                    "values": facet.buckets.iter().map(|bucket| json!({
                        "value": bucket.value.to_json(),
                        "count": bucket.count,
                    })).collect::<Vec<_>>(),
                    "distinct_count": facet.distinct_count,
                    "truncated": facet.truncated,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "path": projected,
            "match_count": page.match_count,
            "matches": matches,
            "facets": facets,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor.is_some(),
        }))
    }

    fn aggregate(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let path = self.parse_argument_path(arguments)?;
        let scope = self.query_scope(&path);
        let projected = self.query_path(&scope);
        let measures = parse_measures(arguments)?;
        let mut predicates = parse_predicates(arguments)?;
        normalize_generic_boolean_predicates(&mut predicates);
        let request = AggregateRequest {
            profile: QueryProfile::GenericNamespaceV1 {
                presentation_path_root: self.logical_root.clone(),
            },
            scope,
            predicates,
            group_by: parse_string_array(arguments, "group_by")?,
            measures,
            sort: parse_sort(arguments)?,
            cursor: None,
            limit: optional_usize(arguments, "limit")?.unwrap_or(DEFAULT_AGGREGATE_LIMIT),
        };
        for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
            let page = self.backend.aggregate(request.clone())?;
            let groups = page
                .rows
                .iter()
                .map(generic_aggregate_group)
                .collect::<Vec<_>>();
            match self.require_directory_at_read_version(&path, page.read_version) {
                Err(error)
                    if attempt < CONSISTENT_READ_MAX_ATTEMPTS && is_read_fence_changed(&error) =>
                {
                    continue;
                }
                Err(error) => return Err(error),
                Ok(()) => {}
            }
            return Ok(json!({
                "path": projected,
                "input_match_count": page.input_match_count,
                "row_count": page.row_count,
                "group_count": page.group_count,
                "groups": groups,
                "truncated": page.next_cursor.is_some(),
            }));
        }
        Err(read_fence_changed("aggregate"))
    }

    fn grep(&self, arguments: &Value) -> Result<Value, AgentError>
    where
        B: WorkbenchBackend,
    {
        let path = self.parse_argument_path(arguments)?;
        let scope = self.query_scope(&path);
        let patterns = parse_grep_patterns(arguments)?;
        let glob = optional_string(arguments, "glob")?;
        validate_grep_glob(glob)?;
        let recursive = required_bool(arguments, "recursive")?;
        let query_commitment = grep_query_commitment(&scope, &patterns, glob, recursive);
        let limit = optional_usize(arguments, "limit")?.unwrap_or(DEFAULT_GREP_LIMIT);
        if limit == 0 {
            return Err(AgentError::invalid_arguments(
                "grep limit must be greater than zero",
            ));
        }
        let primary_pattern = required_string(arguments, "pattern")?.to_owned();
        let scan = scan_agent_grep(
            &self.backend,
            AgentGrepScanRequest {
                storage_root_id: self.storage_root_id,
                logical_root: &self.logical_root,
                scope: scope.clone(),
                patterns: &patterns,
                glob,
                recursive,
                query_commitment,
                cursor: optional_string(arguments, "cursor")?,
                limit,
                scan_limits: self.grep_scan_limits,
            },
        )?;
        let matches = scan
            .matches
            .into_iter()
            .map(|match_| {
                json!({
                    "path": self.projected_path(&match_.path),
                    "line_number": match_.line_number,
                    "snippet": match_.snippet,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "path": self.query_path(&scope),
            "pattern": primary_pattern,
            "recursive": recursive,
            "matches": matches,
            "files_scanned": scan.files_scanned,
            "next_cursor": scan.next_cursor,
            "truncated": scan.next_cursor.is_some(),
        }))
    }

    fn root_card(
        &self,
        entry_count: usize,
        sample: Vec<String>,
        catalog: &GenericCatalog,
    ) -> Value {
        json!({
            "path": self.logical_root,
            "name": self.logical_root.rsplit('/').next().unwrap_or("/"),
            "kind": "directory",
            "size_bytes": Value::Null,
            "entry_count": entry_count,
            "record_count": entry_count,
            "schema": directory_schema(),
            "sample": sample,
            "body": Value::Null,
            "catalog": catalog_value(&catalog.fields, &catalog.facets),
            "indexed_values": [],
        })
    }

    fn stat_card(
        &self,
        record: &StatRecord,
        catalog: &GenericCatalog,
        structured: Option<&GenericStructuredRecords>,
        directory: Option<&GenericDirectorySummary>,
    ) -> Value {
        let artifact = record.artifact.as_ref();
        let name = record
            .path
            .relative_path
            .as_ref()
            .and_then(|path| path.components().last())
            .map(str::to_owned)
            .or_else(|| {
                record
                    .path
                    .section
                    .map(|section| section.as_str().to_owned())
            })
            .unwrap_or_else(|| record.path.workbench_id.as_str().to_owned());
        let body = artifact.map(|artifact| {
            json!({
                "producer": artifact.producer,
                "size": artifact.size_bytes,
                "content_type": artifact.content_type,
            })
        });
        let indexed_values = artifact
            .map(|artifact| {
                artifact
                    .indexed_fields
                    .iter()
                    .map(|(field, value)| json!({"field": field, "value": value.to_json()}))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        json!({
            "path": self.projected_path(&record.path),
            "name": name,
            "kind": projected_kind_name(&record.kind),
            "size_bytes": artifact.map(|artifact| artifact.size_bytes),
            "entry_count": directory.map(|summary| summary.entry_count),
            "record_count": structured
                .map(|records| records.items.len())
                .or_else(|| directory.map(|summary| summary.entry_count)),
            "schema": structured.map(|records| json!({
                    "record_type": records.record_type,
                    "fields": records.fields,
                })).or_else(|| directory.map(|_| directory_schema())),
            "sample": structured
                .map(structured_sample)
                .or_else(|| directory.map(|summary| summary.sample.clone()))
                .unwrap_or_default(),
            "body": body,
            "catalog": catalog_value(&catalog.fields, &catalog.facets),
            "indexed_values": indexed_values,
        })
    }

    fn list_entry(&self, entry: &ListEntry) -> Value {
        let artifact = entry.artifact.as_ref();
        let name = entry
            .path
            .relative_path
            .as_ref()
            .and_then(|path| path.components().last())
            .map(str::to_owned)
            .or_else(|| {
                entry
                    .path
                    .section
                    .map(|section| section.as_str().to_owned())
            })
            .unwrap_or_else(|| entry.path.workbench_id.as_str().to_owned());
        json!({
            "path": self.projected_path(&entry.path),
            "name": name,
            "kind": projected_kind_name(&entry.kind),
            "size_bytes": artifact.map(|artifact| artifact.size_bytes),
            "entry_count": Value::Null,
        })
    }

    fn workbench_list_entry(&self, summary: &WorkbenchSummary) -> Value {
        json!({
            "path": self.workbench_path(&summary.workbench_id),
            "name": summary.workbench_id.as_str(),
            "kind": "directory",
            "size_bytes": Value::Null,
            "entry_count": summary.entry_count,
        })
    }
}

impl<B: WorkbenchBackend> GenericAgentToolHandler for SdkGenericAgentToolHandler<B> {
    fn execute(&self, name: &str, arguments: &Value) -> Result<Value, AgentError> {
        match name {
            "ls" => self.list(arguments),
            "stat" => self.stat(arguments),
            "catalog" => self.catalog(arguments),
            "read" => self.read(arguments),
            "aggregate" => self.aggregate(arguments),
            "find" => self.find(arguments),
            "grep" => self.grep(arguments),
            other => Err(AgentError::unknown_generic_agent_tool(other)),
        }
    }
}

#[derive(Clone)]
enum GenericPath {
    Root,
    Scoped(ScopedPath),
}

#[derive(Default)]
struct GenericGrepCursor {
    backend_cursor: Option<String>,
    pending: Option<GenericGrepPendingCandidate>,
}

struct GenericGrepPendingCandidate {
    fence: GrepCandidateReadFence,
    cursor_after: Option<String>,
    line_index: usize,
}

struct GenericBackendCursor {
    backend_cursor: String,
}

struct GenericCatalog {
    fields: Vec<CatalogField>,
    facets: Vec<FacetResult>,
    read_version: u64,
}

struct GenericStructuredRecords {
    record_type: &'static str,
    fields: Vec<String>,
    items: Vec<Value>,
}

struct GenericDirectorySummary {
    entry_count: usize,
    sample: Vec<String>,
}

fn catalog_anchor_match(path: &GenericPath) -> CatalogPathMatch {
    match path {
        GenericPath::Root => CatalogPathMatch::Prefix,
        GenericPath::Scoped(path) if path.logical_path().is_empty() => CatalogPathMatch::Prefix,
        GenericPath::Scoped(_) => CatalogPathMatch::Exact,
    }
}

fn not_found(path: &str) -> AgentError {
    AgentError::backend(
        "NotFound",
        format!("path not found: {path}"),
        false,
        json!({"path": path}),
    )
}

fn not_directory(path: &str) -> AgentError {
    AgentError::backend(
        "NotDirectory",
        format!("path is not a directory: {path}"),
        false,
        json!({"path": path}),
    )
}

fn protocol_mismatch(message: &str) -> AgentError {
    AgentError::backend("BackendProtocolMismatch", message, true, json!({}))
}

fn read_fence_changed(operation: &str) -> AgentError {
    AgentError::backend(
        "ReadFenceChanged",
        format!("{operation} pages crossed an authoritative read fence"),
        true,
        json!({"operation": operation}),
    )
}

fn is_read_fence_changed(error: &AgentError) -> bool {
    error.code == "ReadFenceChanged"
}

fn count_scan_exhausted(operation: &str) -> AgentError {
    AgentError::backend(
        "ResourceExhausted",
        format!("{operation} total exceeds the bounded compatibility scan"),
        false,
        json!({
            "operation": operation,
            "maximum_results": MAX_COUNT_SCAN_RESULTS,
            "maximum_pages": MAX_COUNT_SCAN_PAGES,
        }),
    )
}

fn grep_scan_exhausted(resource: &str, maximum: u64) -> AgentError {
    AgentError::backend(
        "ResourceExhausted",
        format!("grep exceeded its bounded {resource} scan budget"),
        false,
        json!({"operation": "grep", "resource": resource, "maximum": maximum}),
    )
}

fn grep_candidate_is_in_scope(
    candidate: &GrepCandidateReadFence,
    scope: &QueryScope,
    recursive: bool,
) -> bool {
    let path = &candidate.path;
    let candidate_path = path.logical_path();
    if candidate_path.is_empty() {
        return false;
    }
    let Some(workbench_id) = scope.workbench_id.as_ref() else {
        return recursive;
    };
    if workbench_id != &path.workbench_id {
        return false;
    }
    let prefix = match (scope.section, scope.path.as_ref()) {
        (Some(section), Some(relative_path)) => format!("{section}/{relative_path}"),
        (Some(section), None) => section.to_string(),
        (None, Some(relative_path)) => relative_path.to_string(),
        (None, None) => String::new(),
    };
    if prefix.is_empty() {
        return recursive || !candidate_path.contains('/');
    }
    if candidate_path == prefix {
        return true;
    }
    let Some(descendant) = candidate_path
        .strip_prefix(&prefix)
        .and_then(|tail| tail.strip_prefix('/'))
    else {
        return false;
    };
    recursive || !descendant.contains('/')
}

fn grep_candidate_matches_glob(candidate: &GrepCandidateReadFence, glob: Option<&str>) -> bool {
    let logical_path = candidate.path.logical_path();
    let basename = logical_path.rsplit('/').next().unwrap_or_default();
    glob.is_none_or(|glob| glob_matches(glob, basename))
}

fn projected_scoped_path(logical_root: &str, path: &ScopedPath) -> String {
    let mut projected = format!("{logical_root}/{}", path.workbench_id.as_str());
    if let Some(section) = path.section {
        projected.push('/');
        projected.push_str(section.as_str());
    }
    if let Some(relative_path) = &path.relative_path {
        projected.push('/');
        projected.push_str(relative_path.as_str());
    }
    projected
}

fn projected_query_scope_path(logical_root: &str, scope: &QueryScope) -> String {
    let Some(workbench_id) = scope.workbench_id.as_ref() else {
        return logical_root.to_owned();
    };
    projected_scoped_path(
        logical_root,
        &ScopedPath {
            workbench_id: workbench_id.clone(),
            section: scope.section,
            relative_path: scope.path.clone(),
        },
    )
}

fn project_grep_path_error(mut error: AgentError, presentation_path: &str) -> AgentError {
    if let Some(details) = error.details.as_object_mut() {
        details.insert("path".to_owned(), json!(presentation_path));
    } else {
        error.details = json!({"path": presentation_path});
    }
    error
}

fn generic_grep_cursor_commitment(
    storage_root_id: RootId,
    logical_root: &str,
    query_commitment: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.agent.generic.grep-cursor-authority.v1\0");
    hasher.update(storage_root_id.as_bytes());
    hasher.update(
        u64::try_from(logical_root.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(logical_root.as_bytes());
    hasher.update(query_commitment);
    hasher.finalize().into()
}

fn encode_generic_grep_cursor(
    cursor_commitment: [u8; 32],
    cursor: &GenericGrepCursor,
) -> Result<String, AgentError> {
    let mut encoded = Vec::with_capacity(256);
    encoded.extend_from_slice(GENERIC_GREP_CURSOR_VERSION);
    encoded.extend_from_slice(&cursor_commitment);
    match (&cursor.backend_cursor, &cursor.pending) {
        (Some(backend_cursor), None) if !backend_cursor.is_empty() => {
            encoded.push(0);
            push_generic_grep_cursor_string(&mut encoded, backend_cursor)?;
        }
        (None, Some(pending)) => {
            encoded.push(1);
            push_generic_grep_cursor_string(
                &mut encoded,
                pending.fence.path.workbench_id.as_str(),
            )?;
            encoded.push(match pending.fence.path.section {
                None => 0,
                Some(Section::Input) => 1,
                Some(Section::Scripts) => 2,
                Some(Section::Outputs) => 3,
                Some(Section::Logs) => 4,
                Some(Section::Metadata) => 5,
            });
            match pending.fence.path.relative_path.as_ref() {
                None if pending.fence.path.section.is_some() => encoded.push(0),
                Some(relative_path) => {
                    encoded.push(1);
                    push_generic_grep_cursor_string(&mut encoded, relative_path.as_str())?;
                }
                None => {
                    return Err(AgentError::invalid_arguments(
                        "grep cursor path is not an artifact",
                    ));
                }
            }
            encoded.extend_from_slice(pending.fence.authority.workspace_incarnation_id.as_bytes());
            encoded.extend_from_slice(&pending.fence.authority.workspace_revision.to_be_bytes());
            encoded.extend_from_slice(pending.fence.authority.artifact_revision_id.as_bytes());
            encoded.extend_from_slice(&pending.fence.authority.generation.to_be_bytes());
            match &pending.cursor_after {
                None => encoded.push(0),
                Some(cursor_after) if !cursor_after.is_empty() => {
                    encoded.push(1);
                    push_generic_grep_cursor_string(&mut encoded, cursor_after)?;
                }
                Some(_) => {
                    return Err(AgentError::invalid_arguments(
                        "grep cursor contains an empty backend continuation",
                    ));
                }
            }
            encoded.extend_from_slice(
                &u64::try_from(pending.line_index)
                    .map_err(|_| AgentError::invalid_arguments("grep line offset is too large"))?
                    .to_be_bytes(),
            );
        }
        _ => {
            return Err(AgentError::invalid_arguments(
                "grep cursor must contain exactly one continuation state",
            ));
        }
    }
    let encoded = URL_SAFE_NO_PAD.encode(encoded);
    if encoded.len() > MAX_GENERIC_GREP_CURSOR_BYTES {
        return Err(AgentError::invalid_arguments(
            "grep cursor exceeds the maximum encoded size",
        ));
    }
    Ok(encoded)
}

fn decode_generic_grep_cursor(
    cursor: &str,
    expected_cursor_commitment: [u8; 32],
) -> Result<GenericGrepCursor, AgentError> {
    if cursor.len() > MAX_GENERIC_GREP_CURSOR_BYTES {
        return Err(AgentError::invalid_arguments(
            "grep cursor exceeds the maximum encoded size",
        ));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AgentError::invalid_arguments("grep cursor is not canonical base64url"))?;
    let payload = decoded
        .strip_prefix(GENERIC_GREP_CURSOR_VERSION)
        .ok_or_else(|| AgentError::invalid_arguments("grep cursor uses an unknown schema"))?;
    let (cursor_commitment, payload) = payload.split_first_chunk::<32>().ok_or_else(|| {
        AgentError::invalid_arguments("grep cursor omits its authority commitment")
    })?;
    if cursor_commitment != &expected_cursor_commitment {
        return Err(AgentError::invalid_arguments(
            "grep cursor belongs to a different root or query",
        ));
    }
    let mut decoder = GenericGrepCursorDecoder::new(payload);
    let state = decoder.byte("grep cursor omits its continuation state")?;
    let result = match state {
        0 => {
            let backend_cursor = decoder.string("grep backend cursor")?;
            if backend_cursor.is_empty() {
                return Err(AgentError::invalid_arguments(
                    "grep cursor omits its backend continuation",
                ));
            }
            GenericGrepCursor {
                backend_cursor: Some(backend_cursor),
                pending: None,
            }
        }
        1 => {
            let workbench_id =
                WorkbenchId::new(decoder.string("grep workbench id")?).map_err(|_| {
                    AgentError::invalid_arguments("grep cursor has an invalid workbench id")
                })?;
            let section = match decoder.byte("grep cursor omits its section")? {
                0 => None,
                1 => Some(Section::Input),
                2 => Some(Section::Scripts),
                3 => Some(Section::Outputs),
                4 => Some(Section::Logs),
                5 => Some(Section::Metadata),
                _ => {
                    return Err(AgentError::invalid_arguments(
                        "grep cursor has an invalid section",
                    ));
                }
            };
            let relative_path = match decoder.byte("grep cursor omits its relative path marker")? {
                0 if section.is_some() => None,
                0 => {
                    return Err(AgentError::invalid_arguments(
                        "grep cursor has a root artifact path",
                    ));
                }
                1 => Some(
                    NormalizedRelativePath::new(decoder.string("grep relative path")?).map_err(
                        |_| {
                            AgentError::invalid_arguments(
                                "grep cursor has an invalid relative path",
                            )
                        },
                    )?,
                ),
                _ => {
                    return Err(AgentError::invalid_arguments(
                        "grep cursor has an invalid relative path marker",
                    ));
                }
            };
            let workspace_incarnation_id = WorkspaceIncarnationId::from_bytes(
                decoder.array("grep cursor omits its workspace incarnation")?,
            );
            let workspace_revision =
                u64::from_be_bytes(decoder.array("grep cursor omits its workspace revision")?);
            let artifact_revision_id = ArtifactRevisionId::from_bytes(
                decoder.array("grep cursor omits its artifact revision")?,
            );
            let generation = u64::from_be_bytes(decoder.array("grep cursor omits its generation")?);
            if generation == 0 {
                return Err(AgentError::invalid_arguments(
                    "grep cursor generation must be greater than zero",
                ));
            }
            let cursor_after = match decoder.byte("grep cursor omits its backend marker")? {
                0 => None,
                1 => {
                    let cursor = decoder.string("grep candidate continuation")?;
                    if cursor.is_empty() {
                        return Err(AgentError::invalid_arguments(
                            "grep cursor contains an empty backend continuation",
                        ));
                    }
                    Some(cursor)
                }
                _ => {
                    return Err(AgentError::invalid_arguments(
                        "grep cursor has an invalid backend marker",
                    ));
                }
            };
            let line_index = usize::try_from(u64::from_be_bytes(
                decoder.array("grep cursor omits its line offset")?,
            ))
            .map_err(|_| AgentError::invalid_arguments("grep line offset is too large"))?;
            GenericGrepCursor {
                backend_cursor: None,
                pending: Some(GenericGrepPendingCandidate {
                    fence: GrepCandidateReadFence {
                        path: ScopedPath {
                            workbench_id,
                            section,
                            relative_path,
                        },
                        authority: crate::GrepCandidateAuthority {
                            workspace_incarnation_id,
                            workspace_revision,
                            artifact_revision_id,
                            generation,
                        },
                    },
                    cursor_after,
                    line_index,
                }),
            }
        }
        _ => {
            return Err(AgentError::invalid_arguments(
                "grep cursor has an invalid continuation state",
            ));
        }
    };
    decoder.finish()?;
    Ok(result)
}

fn push_generic_grep_cursor_string(encoded: &mut Vec<u8>, value: &str) -> Result<(), AgentError> {
    let length = u32::try_from(value.len())
        .map_err(|_| AgentError::invalid_arguments("grep cursor field is too large"))?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

struct GenericGrepCursorDecoder<'a> {
    remaining: &'a [u8],
}

impl<'a> GenericGrepCursorDecoder<'a> {
    fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn byte(&mut self, message: &'static str) -> Result<u8, AgentError> {
        let (&value, remaining) = self
            .remaining
            .split_first()
            .ok_or_else(|| AgentError::invalid_arguments(message))?;
        self.remaining = remaining;
        Ok(value)
    }

    fn array<const N: usize>(&mut self, message: &'static str) -> Result<[u8; N], AgentError> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(N)
            .ok_or_else(|| AgentError::invalid_arguments(message))?;
        self.remaining = remaining;
        Ok(value.try_into().expect("split length matches array width"))
    }

    fn string(&mut self, field: &'static str) -> Result<String, AgentError> {
        let length = usize::try_from(u32::from_be_bytes(
            self.array("grep cursor omits a string length")?,
        ))
        .expect("u32 always fits usize on supported targets");
        let (value, remaining) = self.remaining.split_at_checked(length).ok_or_else(|| {
            AgentError::invalid_arguments(format!("grep cursor truncates {field}"))
        })?;
        self.remaining = remaining;
        std::str::from_utf8(value)
            .map(str::to_owned)
            .map_err(|_| AgentError::invalid_arguments(format!("{field} is not valid UTF-8")))
    }

    fn finish(self) -> Result<(), AgentError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(AgentError::invalid_arguments(
                "grep cursor has trailing bytes",
            ))
        }
    }
}

fn encode_generic_committed_backend_cursor(
    version: &[u8],
    commitment: [u8; 32],
    backend_cursor: &str,
) -> String {
    let mut encoded = Vec::with_capacity(version.len() + commitment.len() + backend_cursor.len());
    encoded.extend_from_slice(version);
    encoded.extend_from_slice(&commitment);
    encoded.extend_from_slice(backend_cursor.as_bytes());
    URL_SAFE_NO_PAD.encode(encoded)
}

fn generic_scoped_list_cursor_commitment(
    root_id: RootId,
    logical_root: &str,
    request: &ListRequest,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.agent.generic.scoped-list-query.v1\0");
    hasher.update(root_id.as_bytes());
    hash_generic_cursor_text(&mut hasher, logical_root);
    hash_generic_cursor_text(&mut hasher, request.path.workbench_id.as_str());
    hash_generic_cursor_text(&mut hasher, &request.path.logical_path());
    match &request.view {
        ReadView::Live => hasher.update([1]),
        ReadView::Snapshot(crate::SnapshotSelector::Id(snapshot_id)) => {
            hasher.update([2]);
            hasher.update(snapshot_id.to_be_bytes());
        }
        ReadView::Snapshot(crate::SnapshotSelector::Name(name)) => {
            hasher.update([3]);
            hash_generic_cursor_text(&mut hasher, name);
        }
    }
    hasher.finalize().into()
}

fn generic_root_list_cursor_commitment(
    root_id: RootId,
    logical_root: &str,
    request: &FindRequest,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.agent.generic.root-list-query.v1\0");
    hasher.update(root_id.as_bytes());
    hash_generic_cursor_text(&mut hasher, logical_root);
    hasher.update([match request.committed {
        None => 0,
        Some(false) => 1,
        Some(true) => 2,
    }]);
    match request.manifest_pattern.as_deref() {
        None => hasher.update([0]),
        Some(pattern) => {
            hasher.update([1]);
            hash_generic_cursor_text(&mut hasher, pattern);
        }
    }
    hasher.update([u8::from(request.include_manifest)]);
    hasher.finalize().into()
}

fn hash_generic_cursor_text(hasher: &mut Sha256, value: &str) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

fn generic_query_cursor_commitment(root_id: RootId, logical_root: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.agent.generic.query-layout.v1\0");
    hasher.update(root_id.as_bytes());
    hasher.update((logical_root.len() as u64).to_be_bytes());
    hasher.update(logical_root.as_bytes());
    hasher.finalize().into()
}

fn encode_generic_query_cursor(commitment: [u8; 32], backend_cursor: &str) -> String {
    encode_generic_committed_backend_cursor(GENERIC_FIND_CURSOR_VERSION, commitment, backend_cursor)
}

fn decode_generic_query_cursor(
    cursor: &str,
    expected_commitment: [u8; 32],
) -> Result<GenericBackendCursor, AgentError> {
    decode_generic_committed_backend_cursor(
        cursor,
        GENERIC_FIND_CURSOR_VERSION,
        expected_commitment,
    )
}

fn decode_generic_committed_backend_cursor(
    cursor: &str,
    version: &[u8],
    expected_commitment: [u8; 32],
) -> Result<GenericBackendCursor, AgentError> {
    if cursor.len() > MAX_GENERIC_BACKEND_CURSOR_BYTES {
        return Err(AgentError::invalid_arguments(
            "cursor exceeds the maximum encoded size",
        ));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AgentError::invalid_arguments("cursor is not canonical base64url"))?;
    let payload = decoded
        .strip_prefix(version)
        .ok_or_else(|| AgentError::invalid_arguments("cursor belongs to a different operation"))?;
    if payload.len() < 32 {
        return Err(AgentError::invalid_arguments(
            "cursor omits its query layout commitment",
        ));
    }
    let (commitment, backend_cursor) = payload.split_at(32);
    if commitment != expected_commitment {
        return Err(AgentError::invalid_arguments(
            "cursor belongs to a different storage root or presentation layout",
        ));
    }
    if backend_cursor.is_empty() {
        return Err(AgentError::invalid_arguments(
            "cursor omits its backend continuation",
        ));
    }
    Ok(GenericBackendCursor {
        backend_cursor: String::from_utf8(backend_cursor.to_vec())
            .map_err(|_| AgentError::invalid_arguments("cursor is not valid UTF-8"))?,
    })
}

fn catalog_is_empty(fields: &[CatalogField]) -> bool {
    !fields
        .iter()
        .any(|field| !field.operators.is_empty() || field.sortable || field.facetable)
}

fn directory_schema() -> Value {
    json!({
        "record_type": "directory_entries",
        "fields": ["path", "file.name", "file.type", "file.size_bytes"],
    })
}

fn list_entry_name(entry: &ListEntry) -> String {
    entry
        .path
        .relative_path
        .as_ref()
        .and_then(|path| path.components().last())
        .map(str::to_owned)
        .or_else(|| {
            entry
                .path
                .section
                .map(|section| section.as_str().to_owned())
        })
        .unwrap_or_else(|| entry.path.workbench_id.as_str().to_owned())
}

fn facet_value(facet: &FacetResult) -> Value {
    json!({
        "field": facet.field,
        "values": facet.buckets.iter().map(|bucket| json!({
            "value": bucket.value.to_json(),
            "count": bucket.count,
        })).collect::<Vec<_>>(),
        "distinct_count": facet.distinct_count,
        "truncated": facet.truncated,
    })
}

fn generic_structured_artifact(path: &ScopedPath, metadata: &crate::ArtifactMetadata) -> bool {
    let content_type = metadata.content_type.to_ascii_lowercase();
    let path = path.logical_path().to_ascii_lowercase();
    content_type == "application/json"
        || path.ends_with(".json")
        || matches!(
            content_type.as_str(),
            "application/yaml" | "application/x-yaml" | "text/yaml"
        )
        || path.ends_with(".yaml")
        || path.ends_with(".yml")
        || content_type.starts_with("text/")
        || path.ends_with(".txt")
        || path.ends_with(".log")
}

fn generic_structured_records(
    artifact: &ArtifactBody,
    presentation_path: &str,
) -> Result<GenericStructuredRecords, AgentError> {
    let content_type = artifact.metadata.content_type.to_ascii_lowercase();
    let path = artifact.path.logical_path().to_ascii_lowercase();
    if content_type == "application/json" || path.ends_with(".json") {
        let value = serde_json::from_slice::<Value>(&artifact.bytes).map_err(|error| {
            structured_decode_failed(
                presentation_path,
                "json",
                format!("JSON decoding failed: {error}"),
            )
        })?;
        return match value {
            Value::Array(items) => Ok(GenericStructuredRecords {
                record_type: "json_array",
                fields: infer_json_array_fields(&items),
                items,
            }),
            Value::Object(map) => {
                let mut entries = map.into_iter().collect::<Vec<_>>();
                entries.sort_by(|left, right| left.0.cmp(&right.0));
                Ok(GenericStructuredRecords {
                    record_type: "json_object",
                    fields: vec!["key".to_owned(), "value".to_owned()],
                    items: entries
                        .into_iter()
                        .map(|(key, value)| json!({"key": key, "value": value}))
                        .collect(),
                })
            }
            _ => Err(structured_decode_failed(
                presentation_path,
                "json",
                "structured JSON read supports arrays and objects",
            )),
        };
    }
    if matches!(
        content_type.as_str(),
        "application/yaml" | "application/x-yaml" | "text/yaml"
    ) || path.ends_with(".yaml")
        || path.ends_with(".yml")
    {
        let value =
            serde_yaml::from_slice::<serde_yaml::Value>(&artifact.bytes).map_err(|error| {
                structured_decode_failed(
                    presentation_path,
                    "yaml",
                    format!("YAML decoding failed: {error}"),
                )
            })?;
        let serde_yaml::Value::Mapping(map) = value else {
            return Err(structured_decode_failed(
                presentation_path,
                "yaml",
                "structured YAML read supports mappings",
            ));
        };
        let mut entries = map
            .into_iter()
            .filter_map(|(key, value)| {
                key.as_str().map(|key| {
                    (
                        key.to_owned(),
                        serde_json::to_value(value).unwrap_or(Value::Null),
                    )
                })
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        return Ok(GenericStructuredRecords {
            record_type: "yaml_mapping",
            fields: vec!["key".to_owned(), "value".to_owned()],
            items: entries
                .into_iter()
                .map(|(key, value)| json!({"key": key, "value": value}))
                .collect(),
        });
    }
    if content_type.starts_with("text/") || path.ends_with(".txt") || path.ends_with(".log") {
        let text = std::str::from_utf8(&artifact.bytes).map_err(|error| {
            structured_decode_failed(
                presentation_path,
                "text",
                format!("UTF-8 decoding failed: {error}"),
            )
        })?;
        return Ok(GenericStructuredRecords {
            record_type: "text_lines",
            fields: vec!["line".to_owned(), "text".to_owned()],
            items: text
                .lines()
                .enumerate()
                .map(|(index, line)| json!({"line": index + 1, "text": line}))
                .collect(),
        });
    }
    Err(structured_decode_failed(
        presentation_path,
        "unknown",
        format!(
            "structured read does not support content type {}",
            artifact.metadata.content_type
        ),
    ))
}

fn structured_decode_failed(
    presentation_path: &str,
    format: &str,
    message: impl Into<String>,
) -> AgentError {
    AgentError::backend(
        "StructuredDecodeFailed",
        message,
        false,
        json!({"path": presentation_path, "format": format}),
    )
}

fn infer_json_array_fields(items: &[Value]) -> Vec<String> {
    let mut fields = BTreeMap::<String, ()>::new();
    for item in items {
        if let Some(object) = item.as_object() {
            for field in object.keys() {
                fields.insert(field.clone(), ());
            }
        }
    }
    fields.into_keys().collect()
}

fn structured_sample(records: &GenericStructuredRecords) -> Vec<String> {
    records
        .items
        .iter()
        .take(STAT_SAMPLE_LIMIT)
        .filter_map(|item| serde_json::to_string(item).ok())
        .collect()
}

fn normalize_generic_boolean_predicates(predicates: &mut [QueryPredicate]) {
    for predicate in predicates {
        if let Some(value) = &mut predicate.value {
            normalize_generic_boolean_value(value);
        }
    }
}

fn normalize_generic_boolean_value(value: &mut QueryValue) {
    match value {
        QueryValue::Boolean(boolean) => {
            *value = QueryValue::Unsigned(u64::from(*boolean));
        }
        QueryValue::List(values) => {
            for value in values {
                normalize_generic_boolean_value(value);
            }
        }
        _ => {}
    }
}

fn generic_namespace_scoped_path(hit: &GenericNamespaceHit) -> Result<ScopedPath, AgentError> {
    let Some(path) = &hit.relative_path else {
        return Ok(ScopedPath {
            workbench_id: hit.workbench_id.clone(),
            section: None,
            relative_path: None,
        });
    };
    let raw = path.as_str();
    let (first, remainder) = raw
        .split_once('/')
        .map_or((raw, None), |(first, rest)| (first, Some(rest)));
    match Section::parse(first) {
        Ok(section) => Ok(ScopedPath {
            workbench_id: hit.workbench_id.clone(),
            section: Some(section),
            relative_path: remainder
                .map(NormalizedRelativePath::new)
                .transpose()
                .map_err(|error| AgentError::invalid_arguments(error.to_string()))?,
        }),
        Err(_) => Ok(ScopedPath {
            workbench_id: hit.workbench_id.clone(),
            section: None,
            relative_path: Some(path.clone()),
        }),
    }
}

fn generic_projected_values(hit: &GenericNamespaceHit, fields: &[String], path: &str) -> Value {
    let mut values = serde_json::Map::new();
    for field in fields {
        let value = match field.as_str() {
            "path" => Some(json!(path)),
            other => hit
                .indexed_values
                .get(other)
                .and_then(|indexed| match indexed.as_slice() {
                    [] => None,
                    [value] => Some(value.to_json()),
                    values => Some(Value::Array(
                        values.iter().map(QueryValue::to_json).collect(),
                    )),
                })
                .or_else(|| hit.projection.get(other).map(QueryValue::to_json)),
        };
        if let Some(value) = value {
            values.insert(field.clone(), value);
        }
    }
    Value::Object(values)
}

fn generic_aggregate_group(row: &AggregateRow) -> Value {
    json!({
        "key": query_map_value(&row.groups),
        "values": query_map_value(&row.measures),
    })
}

fn catalog_value(fields: &[CatalogField], facets: &[FacetResult]) -> Value {
    let mut filterable = BTreeMap::<Vec<String>, Vec<String>>::new();
    let mut sortable = Vec::new();
    let mut facetable = Vec::new();
    for field in fields {
        if !field.operators.is_empty() {
            filterable
                .entry(field.operators.clone())
                .or_default()
                .push(field.field.clone());
        }
        if field.sortable {
            sortable.push(field.field.clone());
        }
        if field.facetable {
            facetable.push(field.field.clone());
        }
    }
    let filterable = filterable
        .into_iter()
        .map(|(operators, fields)| json!({"operators": operators, "fields": fields}))
        .collect::<Vec<_>>();
    json!({
        "filterable": filterable,
        "sortable": sortable,
        "facetable": facetable,
        "facets": facets.iter().map(facet_value).collect::<Vec<_>>(),
    })
}
