/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use base64::Engine;
use nokv_agent::*;
use nokv_types::{
    ArtifactRevisionId, NormalizedRelativePath, RootId, WorkbenchId, WorkspaceIncarnationId,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const TEST_WORKBENCH_ROOT: &str = "/agents/test/wb";

fn generic_query_profile() -> QueryProfile {
    QueryProfile::GenericNamespaceV1 {
        presentation_path_root: TEST_WORKBENCH_ROOT.to_owned(),
    }
}

#[derive(Clone)]
struct FakeBackend {
    state: Arc<Mutex<FakeState>>,
    root_id: RootId,
}

impl Default for FakeBackend {
    fn default() -> Self {
        Self::with_root(1)
    }
}

#[derive(Clone)]
enum GrepCandidateMutation {
    Replace(Vec<u8>),
    Remove,
}

#[derive(Default)]
struct FakeState {
    read_version: u64,
    workbenches: BTreeSet<String>,
    files: BTreeMap<(String, String), ArtifactBody>,
    grep_authorities: BTreeMap<(String, String), GrepCandidateAuthority>,
    workspace_incarnations: BTreeMap<String, WorkspaceIncarnationId>,
    workspace_revisions: BTreeMap<String, u64>,
    next_identity: u64,
    publish_calls: usize,
    publish_conflicts: usize,
    append_calls: usize,
    append_conflicts: usize,
    append_requests: Vec<AppendRequest>,
    create_error: Option<BackendError>,
    stat_views: Vec<ReadView>,
    list_requests: Vec<ListRequest>,
    grep_requests: Vec<GrepCandidateRequest>,
    grep_metadata_requests: Vec<GrepCandidateReadFence>,
    grep_candidate_mutation: Option<GrepCandidateMutation>,
    replace_before_next_grep_body: Option<Vec<u8>>,
    grep_empty_pages_remaining: usize,
    remove_after_next_list: Option<(String, String)>,
    read_requests: Vec<ReadRequest>,
    inspection_requests: Vec<ReadRequest>,
    search_requests: Vec<SearchRequest>,
    generic_indexed_values: BTreeMap<String, Vec<QueryValue>>,
    aggregate_requests: Vec<AggregateRequest>,
    aggregate_exact_totals: bool,
    catalog_requests: Vec<CatalogRequest>,
    generic_catalog_fields: Option<Vec<CatalogField>>,
    find_requests: Vec<FindRequest>,
    search_two_pages: bool,
    filtered_catalog_is_empty: bool,
    catalog_from_artifacts: bool,
    catalog_advances_remaining: usize,
    remove_after_next_catalog: Option<(String, String)>,
    commit_requests: Vec<CommitRequest>,
    commit_error: Option<BackendError>,
    snapshots: BTreeMap<(String, u64), SnapshotRecord>,
    next_snapshot_id: u64,
    restore_requests: Vec<RestoreRequest>,
    restore_error: Option<BackendError>,
    find_two_pages: bool,
}

impl FakeBackend {
    fn with_root(fill: u8) -> Self {
        Self {
            state: Arc::new(Mutex::new(FakeState::default())),
            root_id: RootId::from_bytes([fill; nokv_types::FIXED_ID_BYTES]),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state
            .lock()
            .expect("fake backend lock must not poison")
    }

    fn body(&self, workbench_id: &str, logical_path: &str) -> Vec<u8> {
        self.lock()
            .files
            .get(&(workbench_id.to_owned(), logical_path.to_owned()))
            .expect("test artifact must exist")
            .bytes
            .clone()
    }

    fn generation(&self, workbench_id: &str, logical_path: &str) -> u64 {
        self.lock()
            .files
            .get(&(workbench_id.to_owned(), logical_path.to_owned()))
            .expect("test artifact must exist")
            .metadata
            .generation
    }

    fn set_publish_conflicts(&self, count: usize) {
        self.lock().publish_conflicts = count;
    }

    fn publish_calls(&self) -> usize {
        self.lock().publish_calls
    }

    fn set_append_conflicts(&self, count: usize) {
        self.lock().append_conflicts = count;
    }

    fn append_calls(&self) -> usize {
        self.lock().append_calls
    }

    fn mutate_after_next_grep_candidates(&self, mutation: GrepCandidateMutation) {
        self.lock().grep_candidate_mutation = Some(mutation);
    }

    fn replace_before_next_grep_body(&self, bytes: Vec<u8>) {
        self.lock().replace_before_next_grep_body = Some(bytes);
    }

    fn remove_after_next_list(&self, workbench_id: &str, logical_path: &str) {
        self.lock().remove_after_next_list =
            Some((workbench_id.to_owned(), logical_path.to_owned()));
    }

    fn remove_after_next_catalog(&self, workbench_id: &str, logical_path: &str) {
        self.lock().remove_after_next_catalog =
            Some((workbench_id.to_owned(), logical_path.to_owned()));
    }

    fn recreate_grep_candidate_with_same_generation(&self, path: &ScopedPath) {
        let mut state = self.lock();
        let key = path_key(path);
        let generation = state
            .files
            .get(&key)
            .expect("candidate exists before recreation")
            .metadata
            .generation;
        let incarnation = WorkspaceIncarnationId::from_bytes(next_fake_identity(&mut state));
        let artifact_revision_id = ArtifactRevisionId::from_bytes(next_fake_identity(&mut state));
        state
            .workspace_incarnations
            .insert(key.0.clone(), incarnation);
        bump_fake_workspace_revision(&mut state, &key.0);
        let workspace_revision = state.workspace_revisions[&key.0];
        state.grep_authorities.insert(
            key,
            GrepCandidateAuthority {
                workspace_incarnation_id: incarnation,
                workspace_revision,
                artifact_revision_id,
                generation,
            },
        );
    }

    fn advance_workspace_revision(&self, workbench_id: &str) {
        bump_fake_workspace_revision(&mut self.lock(), workbench_id);
    }
}

impl WorkbenchBackend for FakeBackend {
    fn storage_root_id(&self) -> RootId {
        self.root_id
    }

    fn create_workbench(&self, workbench_id: &WorkbenchId) -> Result<bool, BackendError> {
        let mut state = self.lock();
        if let Some(error) = state.create_error.take() {
            return Err(error);
        }
        let created = state.workbenches.insert(workbench_id.as_str().to_owned());
        if created {
            ensure_fake_workspace(&mut state, workbench_id);
        }
        Ok(created)
    }

    fn stat(&self, path: &ScopedPath, view: &ReadView) -> Result<Option<StatRecord>, BackendError> {
        let mut state = self.lock();
        state.stat_views.push(view.clone());
        Ok(fake_stat_record(&state, path))
    }

    fn stat_at_read_version(
        &self,
        path: &ScopedPath,
        read_version: u64,
    ) -> Result<Option<StatRecord>, BackendError> {
        let mut state = self.lock();
        state.stat_views.push(ReadView::Live);
        if read_version != current_fake_read_version(&state) {
            return Err(BackendError::new(
                BackendErrorKind::ReadFenceChanged,
                "fake root read version changed before stat",
                true,
                json!({"expected_read_version": read_version}),
            ));
        }
        Ok(fake_stat_record(&state, path))
    }

    fn list(&self, request: ListRequest) -> Result<ListPage, BackendError> {
        let mut state = self.lock();
        state.list_requests.push(request.clone());
        let prefix = request.path.logical_path();
        let mut entries = BTreeMap::<String, ListEntry>::new();
        if prefix.is_empty()
            && state
                .workbenches
                .contains(request.path.workbench_id.as_str())
        {
            for section in WORKBENCH_SECTIONS {
                entries.insert(
                    section.to_string(),
                    ListEntry {
                        path: ScopedPath {
                            workbench_id: request.path.workbench_id.clone(),
                            section: Some(section),
                            relative_path: None,
                        },
                        kind: ArtifactKind::Section,
                        artifact: None,
                    },
                );
            }
        }
        for body in state
            .files
            .values()
            .filter(|body| body.path.workbench_id == request.path.workbench_id)
        {
            let logical_path = body.path.logical_path();
            let tail = if prefix.is_empty() {
                logical_path.as_str()
            } else {
                let Some(tail) = logical_path
                    .strip_prefix(&prefix)
                    .and_then(|tail| tail.strip_prefix('/'))
                else {
                    continue;
                };
                tail
            };
            let direct_name = tail.split('/').next().expect("artifact path is non-empty");
            let direct_logical = if prefix.is_empty() {
                direct_name.to_owned()
            } else {
                format!("{prefix}/{direct_name}")
            };
            if tail.contains('/') {
                entries
                    .entry(direct_logical.clone())
                    .or_insert_with(|| ListEntry {
                        path: fake_scoped_path(&request.path.workbench_id, &direct_logical),
                        kind: ArtifactKind::Directory,
                        artifact: None,
                    });
            } else {
                entries.insert(
                    direct_logical,
                    ListEntry {
                        path: body.path.clone(),
                        kind: ArtifactKind::Artifact,
                        artifact: Some(body.metadata.clone()),
                    },
                );
            }
        }
        let entries = entries.into_values().collect::<Vec<_>>();
        let start = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| backend_error(BackendErrorKind::InvalidState, "bad list cursor"))?;
        let end = start.saturating_add(request.limit).min(entries.len());
        let next_cursor = (end < entries.len()).then(|| end.to_string());
        let read_version = current_fake_read_version(&state);
        let page = ListPage {
            entries: entries[start..end].to_vec(),
            next_cursor,
            read_version,
        };
        if let Some(remove) = state.remove_after_next_list.take() {
            state.files.remove(&remove);
            state.grep_authorities.remove(&remove);
            bump_fake_workspace_revision(&mut state, &remove.0);
        }
        Ok(page)
    }

    fn read(&self, request: ReadRequest) -> Result<Option<ArtifactBody>, BackendError> {
        let mut state = self.lock();
        state.read_requests.push(request.clone());
        Ok(state.files.get(&path_key(&request.path)).cloned())
    }

    fn inspect_artifact(
        &self,
        request: ReadRequest,
    ) -> Result<Option<ArtifactInspection>, BackendError> {
        let mut state = self.lock();
        state.inspection_requests.push(request.clone());
        Ok(state
            .files
            .get(&path_key(&request.path))
            .cloned()
            .map(|artifact| ArtifactInspection { artifact }))
    }

    fn publish(&self, request: PublishRequest) -> Result<PublishOutcome, BackendError> {
        let mut state = self.lock();
        state.publish_calls += 1;
        if state.publish_conflicts > 0 {
            state.publish_conflicts -= 1;
            return Err(BackendError::conflict("injected publication conflict"));
        }
        let key = path_key(&request.path);
        let current = state.files.get(&key);
        let created = current.is_none();
        let generation = match (&request.condition, current) {
            (PublishCondition::CreateOnly, None) => 1,
            (PublishCondition::CreateOnly, Some(_)) => {
                return Err(backend_error(
                    BackendErrorKind::AlreadyExists,
                    "artifact already exists",
                ));
            }
            (PublishCondition::ReplaceOnly { .. }, None) => {
                return Err(backend_error(
                    BackendErrorKind::NotFound,
                    "artifact is absent",
                ));
            }
            (
                PublishCondition::ReplaceOnly {
                    expected_generation,
                },
                Some(current),
            ) if *expected_generation != current.metadata.generation => {
                return Err(BackendError::conflict("generation changed"));
            }
            (PublishCondition::ReplaceOnly { .. }, Some(current)) => {
                current.metadata.generation + 1
            }
        };
        let metadata = artifact_metadata(&request.body, generation, &request.content_type);
        state
            .workbenches
            .insert(request.path.workbench_id.as_str().to_owned());
        let authority =
            next_fake_grep_authority(&mut state, &request.path.workbench_id, metadata.generation);
        state.grep_authorities.insert(key.clone(), authority);
        state.files.insert(
            key,
            ArtifactBody {
                path: request.path,
                metadata: metadata.clone(),
                bytes: request.body,
            },
        );
        Ok(PublishOutcome { metadata, created })
    }

    fn append(&self, request: AppendRequest) -> Result<AppendOutcome, BackendError> {
        let mut state = self.lock();
        state.append_calls += 1;
        state.append_requests.push(request.clone());
        if state.append_conflicts > 0 {
            state.append_conflicts -= 1;
            return Err(BackendError::conflict("injected append conflict"));
        }
        let key = path_key(&request.path);
        let (mut bytes, base_size, generation, content_type, created) = match state.files.get(&key)
        {
            Some(current) => (
                current.bytes.clone(),
                current.metadata.size_bytes,
                current.metadata.generation + 1,
                request
                    .content_type
                    .clone()
                    .unwrap_or_else(|| current.metadata.content_type.clone()),
                false,
            ),
            None => (
                Vec::new(),
                0,
                1,
                request
                    .content_type
                    .clone()
                    .unwrap_or_else(|| request.create_content_type.clone()),
                true,
            ),
        };
        bytes.extend_from_slice(&request.delta);
        let logical_size = base_size
            .checked_add(request.delta.len() as u64)
            .ok_or_else(|| backend_error(BackendErrorKind::InvalidState, "append size overflow"))?;
        if request
            .max_logical_size
            .is_some_and(|maximum| logical_size > maximum)
        {
            return Err(backend_error(
                BackendErrorKind::Other("ResourceExhausted".to_owned()),
                "append result exceeds the configured limit",
            ));
        }
        let mut metadata = artifact_metadata(&bytes, generation, &content_type);
        metadata.size_bytes = logical_size;
        state
            .workbenches
            .insert(request.path.workbench_id.as_str().to_owned());
        let authority =
            next_fake_grep_authority(&mut state, &request.path.workbench_id, metadata.generation);
        state.grep_authorities.insert(key.clone(), authority);
        state.files.insert(
            key,
            ArtifactBody {
                path: request.path,
                metadata: metadata.clone(),
                bytes,
            },
        );
        Ok(AppendOutcome { metadata, created })
    }

    fn grep_candidates(
        &self,
        request: GrepCandidateRequest,
    ) -> Result<GrepCandidatePage, BackendError> {
        let mut state = self.lock();
        state.grep_requests.push(request.clone());
        if state.grep_empty_pages_remaining > 0 {
            state.grep_empty_pages_remaining -= 1;
            let next = request
                .cursor
                .as_deref()
                .unwrap_or("0")
                .parse::<usize>()
                .map_err(|_| backend_error(BackendErrorKind::InvalidState, "bad grep cursor"))?
                .saturating_add(1);
            return Ok(GrepCandidatePage {
                candidates: Vec::new(),
                next_cursor: (state.grep_empty_pages_remaining > 0).then(|| next.to_string()),
            });
        }
        let prefix = match (request.scope.section, &request.scope.path) {
            (Some(section), Some(path)) => format!("{section}/{path}"),
            (Some(section), None) => section.to_string(),
            (None, Some(path)) => path.to_string(),
            (None, None) => String::new(),
        };
        let fresh_nested_scope = request.cursor.is_none() && request.scope.path.is_some();
        let fresh_exact_scope = request.cursor.is_none()
            && (request.scope.section.is_some() || request.scope.path.is_some());
        let exact_exists = fresh_exact_scope
            && request
                .scope
                .workbench_id
                .as_ref()
                .is_some_and(|workbench_id| {
                    state
                        .files
                        .contains_key(&(workbench_id.as_str().to_owned(), prefix.clone()))
                });
        let mut paths = state
            .files
            .values()
            .filter(|body| {
                request
                    .scope
                    .workbench_id
                    .as_ref()
                    .is_none_or(|workbench_id| body.path.workbench_id == *workbench_id)
                    && if exact_exists {
                        body.path.logical_path() == prefix
                    } else if prefix.is_empty() {
                        request.recursive || !body.path.logical_path().contains('/')
                    } else if body.path.logical_path() == prefix {
                        true
                    } else {
                        body.path
                            .logical_path()
                            .strip_prefix(&format!("{prefix}/"))
                            .is_some_and(|tail| request.recursive || !tail.contains('/'))
                    }
            })
            .map(|body| {
                let authority = *state
                    .grep_authorities
                    .get(&path_key(&body.path))
                    .expect("published fake artifact has grep authority");
                (body.path.clone(), body.metadata.clone(), authority)
            })
            .collect::<Vec<_>>();
        paths.sort_by_key(|(path, _, _)| path.logical_path());
        if fresh_nested_scope && paths.is_empty() {
            return Err(backend_error(
                BackendErrorKind::NotFound,
                "grep scope does not exist",
            ));
        }
        let start = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| backend_error(BackendErrorKind::InvalidState, "bad grep cursor"))?;
        let end = start.saturating_add(request.limit).min(paths.len());
        let page = GrepCandidatePage {
            candidates: paths[start..end]
                .iter()
                .enumerate()
                .map(|(index, (path, metadata, authority))| GrepCandidate {
                    path: path.clone(),
                    metadata: metadata.clone(),
                    authority: *authority,
                    cursor_after: (start + index + 1 < paths.len())
                        .then(|| (start + index + 1).to_string()),
                })
                .collect(),
            next_cursor: (end < paths.len()).then(|| end.to_string()),
        };
        if let (Some(mutation), Some(candidate)) = (
            state.grep_candidate_mutation.take(),
            page.candidates.first(),
        ) {
            let key = path_key(&candidate.path);
            match mutation {
                GrepCandidateMutation::Replace(bytes) => {
                    let replacement = if let Some(current) = state.files.get_mut(&key) {
                        let generation = current.metadata.generation + 1;
                        let content_type = current.metadata.content_type.clone();
                        current.metadata = artifact_metadata(&bytes, generation, &content_type);
                        current.bytes = bytes;
                        Some((current.path.workbench_id.clone(), generation))
                    } else {
                        None
                    };
                    if let Some((workbench_id, generation)) = replacement {
                        let authority =
                            next_fake_grep_authority(&mut state, &workbench_id, generation);
                        state.grep_authorities.insert(key, authority);
                    }
                }
                GrepCandidateMutation::Remove => {
                    state.files.remove(&key);
                    state.grep_authorities.remove(&key);
                    bump_fake_workspace_revision(&mut state, &key.0);
                }
            }
        }
        Ok(page)
    }

    fn grep_candidate_metadata(
        &self,
        fence: &GrepCandidateReadFence,
    ) -> Result<ArtifactMetadata, BackendError> {
        let mut state = self.lock();
        state.grep_metadata_requests.push(fence.clone());
        let key = path_key(&fence.path);
        if !state
            .grep_authorities
            .get(&key)
            .is_some_and(|authority| authority == &fence.authority)
        {
            return Err(BackendError::new(
                BackendErrorKind::ReadFenceChanged,
                "grep candidate authority changed before metadata read",
                true,
                json!({"path": fence.path.logical_path()}),
            ));
        }
        state
            .files
            .get(&key)
            .map(|body| body.metadata.clone())
            .ok_or_else(|| {
                BackendError::new(
                    BackendErrorKind::ReadFenceChanged,
                    "grep candidate disappeared before metadata read",
                    true,
                    json!({"path": fence.path.logical_path()}),
                )
            })
    }

    fn read_grep_candidate(
        &self,
        fence: &GrepCandidateReadFence,
    ) -> Result<ArtifactBody, BackendError> {
        let mut state = self.lock();
        state.read_requests.push(ReadRequest {
            path: fence.path.clone(),
            view: ReadView::Live,
        });
        let key = path_key(&fence.path);
        if let Some(bytes) = state.replace_before_next_grep_body.take() {
            let replacement = state.files.get_mut(&key).map(|current| {
                let generation = current.metadata.generation.saturating_add(1);
                let content_type = current.metadata.content_type.clone();
                current.metadata = artifact_metadata(&bytes, generation, &content_type);
                current.bytes = bytes;
                (current.path.workbench_id.clone(), generation)
            });
            if let Some((workbench_id, generation)) = replacement {
                let authority = next_fake_grep_authority(&mut state, &workbench_id, generation);
                state.grep_authorities.insert(key.clone(), authority);
            }
        }
        let authority_matches = state
            .grep_authorities
            .get(&key)
            .is_some_and(|authority| authority == &fence.authority);
        let body = state.files.get(&key).cloned();
        if !authority_matches {
            return Err(BackendError::new(
                BackendErrorKind::ReadFenceChanged,
                "grep candidate authority changed before body read",
                true,
                json!({"path": fence.path.logical_path()}),
            ));
        }
        body.ok_or_else(|| {
            BackendError::new(
                BackendErrorKind::ReadFenceChanged,
                "grep candidate disappeared before body read",
                true,
                json!({"path": fence.path.logical_path()}),
            )
        })
    }

    fn search(&self, request: SearchRequest) -> Result<SearchPage, BackendError> {
        let mut state = self.lock();
        let second_page = request.cursor.as_deref() == Some("search-next");
        let two_pages = state.search_two_pages;
        state.search_requests.push(request);
        let read_version = current_fake_read_version(&state);
        let request = state.search_requests.last().unwrap().clone();
        let path = relative_path(if second_page {
            "outputs/second.txt"
        } else {
            "outputs/report.txt"
        });
        let metadata = artifact_metadata(b"query", 7, "text/plain");
        let projection = BTreeMap::from([(
            "artifact.producer".to_owned(),
            QueryValue::String("agent".to_owned()),
        )]);
        let (hits, namespace_hits) = match &request.profile {
            QueryProfile::ArtifactV1 => (
                vec![SearchHit {
                    workbench_id: workbench_id("wb-main"),
                    path,
                    metadata,
                    projection,
                }],
                Vec::new(),
            ),
            QueryProfile::GenericNamespaceV1 {
                presentation_path_root,
            } => {
                let mut projection = projection;
                let indexed_values = state
                    .generic_indexed_values
                    .iter()
                    .filter(|(field, _)| request.fields.contains(field))
                    .map(|(field, values)| (field.clone(), values.clone()))
                    .collect();
                for field in &request.fields {
                    let value = match field.as_str() {
                        "path" => {
                            QueryValue::String(format!("{presentation_path_root}/wb-main/{path}"))
                        }
                        "name" => QueryValue::String(
                            path.components().last().unwrap_or(path.as_str()).to_owned(),
                        ),
                        "kind" => QueryValue::String("file".to_owned()),
                        "size_bytes" => QueryValue::Unsigned(metadata.size_bytes),
                        "body.content_type" => QueryValue::String(metadata.content_type.clone()),
                        "body.producer" => match metadata.producer.as_ref() {
                            Some(producer) => QueryValue::String(producer.clone()),
                            None => continue,
                        },
                        "body.manifest_id" => match metadata.manifest_id.as_ref() {
                            Some(manifest_id) => QueryValue::String(manifest_id.clone()),
                            None => continue,
                        },
                        _ => continue,
                    };
                    projection.insert(field.clone(), value);
                }
                (
                    Vec::new(),
                    vec![GenericNamespaceHit {
                        workbench_id: workbench_id("wb-main"),
                        relative_path: Some(path),
                        kind: ArtifactKind::Artifact,
                        artifact: Some(metadata),
                        projection,
                        indexed_values,
                    }],
                )
            }
        };
        Ok(SearchPage {
            hits,
            namespace_hits,
            match_count: if two_pages { 2 } else { 1 },
            facets: vec![FacetResult {
                field: match &request.profile {
                    QueryProfile::ArtifactV1 => "artifact.producer",
                    QueryProfile::GenericNamespaceV1 { .. } => "body.producer",
                }
                .to_owned(),
                buckets: vec![FacetBucket {
                    value: QueryValue::String("agent".to_owned()),
                    count: 1,
                }],
                distinct_count: 1,
                truncated: false,
            }],
            next_cursor: ((two_pages && !second_page)
                || state
                    .search_requests
                    .last()
                    .is_some_and(|request| request.cursor.as_deref() == Some("search-in")))
            .then(|| "search-next".to_owned()),
            read_version,
        })
    }

    fn aggregate(&self, request: AggregateRequest) -> Result<AggregatePage, BackendError> {
        let mut state = self.lock();
        let read_version = current_fake_read_version(&state);
        let exact_totals = state.aggregate_exact_totals;
        let second_page = request.cursor.as_deref() == Some("aggregate-next");
        let measures = request
            .measures
            .iter()
            .map(|measure| {
                let value = if exact_totals {
                    if second_page {
                        1
                    } else {
                        2
                    }
                } else {
                    1
                };
                (measure.name.clone(), QueryValue::Unsigned(value))
            })
            .collect();
        state.aggregate_requests.push(request);
        let request = state.aggregate_requests.last().unwrap();
        let group_field = request.group_by.first().map_or_else(
            || match &request.profile {
                QueryProfile::ArtifactV1 => "artifact.producer",
                QueryProfile::GenericNamespaceV1 { .. } => "body.producer",
            },
            String::as_str,
        );
        let group_value = match (&request.profile, group_field) {
            (
                QueryProfile::GenericNamespaceV1 {
                    presentation_path_root,
                },
                "path",
            ) => QueryValue::String(format!(
                "{presentation_path_root}/wb-main/outputs/report.txt"
            )),
            _ => QueryValue::String("agent".to_owned()),
        };
        Ok(AggregatePage {
            rows: vec![AggregateRow {
                groups: BTreeMap::from([(group_field.to_owned(), group_value)]),
                measures,
            }],
            input_match_count: if exact_totals { 5 } else { 1 },
            row_count: if exact_totals { 3 } else { 1 },
            group_count: if exact_totals { 2 } else { 1 },
            next_cursor: (exact_totals && !second_page).then(|| "aggregate-next".to_owned()),
            read_version,
        })
    }

    fn catalog(&self, request: CatalogRequest) -> Result<CatalogResult, BackendError> {
        let mut state = self.lock();
        let read_version = current_fake_read_version(&state);
        let scope_logical_path = match (request.scope.section, request.scope.path.as_ref()) {
            (Some(section), Some(path)) => format!("{section}/{path}"),
            (Some(section), None) => section.to_string(),
            (None, Some(path)) => path.to_string(),
            (None, None) => String::new(),
        };
        let empty = request
            .field_prefix
            .as_deref()
            .is_some_and(|prefix| prefix == "missing" && state.filtered_catalog_is_empty);
        let artifact_fields = (request.profile == QueryProfile::ArtifactV1
            && state.catalog_from_artifacts)
            .then(|| {
                let prefix = match (request.scope.section, request.scope.path.as_ref()) {
                    (Some(section), Some(path)) => format!("{section}/{path}"),
                    (Some(section), None) => section.to_string(),
                    (None, Some(path)) => path.to_string(),
                    (None, None) => String::new(),
                };
                state
                    .files
                    .values()
                    .filter(|body| {
                        request
                            .scope
                            .workbench_id
                            .as_ref()
                            .is_none_or(|workbench_id| body.path.workbench_id == *workbench_id)
                            && match request.path_match {
                                CatalogPathMatch::Exact => body.path.logical_path() == prefix,
                                CatalogPathMatch::Prefix => {
                                    prefix.is_empty()
                                        || body.path.logical_path() == prefix
                                        || body
                                            .path
                                            .logical_path()
                                            .starts_with(&format!("{prefix}/"))
                                }
                            }
                    })
                    .flat_map(|body| body.metadata.indexed_fields.keys().cloned())
                    .collect::<BTreeSet<_>>()
            });
        let generic_exact_artifact =
            matches!(&request.profile, QueryProfile::GenericNamespaceV1 { .. })
                && request.path_match == CatalogPathMatch::Exact
                && request
                    .scope
                    .workbench_id
                    .as_ref()
                    .is_some_and(|workbench_id| {
                        state.files.contains_key(&(
                            workbench_id.as_str().to_owned(),
                            scope_logical_path.clone(),
                        ))
                    });
        let include_facets = request.include_facets;
        state.catalog_requests.push(request.clone());
        let result = CatalogResult {
            fields: if empty {
                Vec::new()
            } else if let Some(fields) = artifact_fields {
                fields
                    .into_iter()
                    .map(|field| CatalogField {
                        field,
                        scalar_type: "string".to_owned(),
                        scalar_types: vec!["string".to_owned()],
                        generic_custom: false,
                        operators: vec!["eq".to_owned(), "prefix".to_owned()],
                        sortable: true,
                        facetable: false,
                        aggregatable: false,
                    })
                    .collect()
            } else if let Some(fields) = state.generic_catalog_fields.as_ref() {
                fields.clone()
            } else if request.profile == QueryProfile::ArtifactV1 {
                vec![CatalogField {
                    field: "artifact.producer".to_owned(),
                    scalar_type: "string".to_owned(),
                    scalar_types: vec!["string".to_owned()],
                    generic_custom: false,
                    operators: vec!["eq".to_owned(), "prefix".to_owned()],
                    sortable: true,
                    facetable: true,
                    aggregatable: false,
                }]
            } else {
                if generic_exact_artifact {
                    Vec::new()
                } else {
                    generic_builtin_catalog_fields(request.field_prefix.as_deref())
                }
            },
            facets: (include_facets && request.profile == QueryProfile::ArtifactV1)
                .then(|| FacetResult {
                    field: "artifact.producer".to_owned(),
                    buckets: vec![FacetBucket {
                        value: QueryValue::String("agent".to_owned()),
                        count: 1,
                    }],
                    distinct_count: 1,
                    truncated: false,
                })
                .into_iter()
                .chain(
                    (include_facets
                        && matches!(&request.profile, QueryProfile::GenericNamespaceV1 { .. })
                        && !generic_exact_artifact)
                        .then(|| FacetResult {
                            field: "body.producer".to_owned(),
                            buckets: vec![FacetBucket {
                                value: QueryValue::String("test-agent".to_owned()),
                                count: 1,
                            }],
                            distinct_count: 1,
                            truncated: false,
                        }),
                )
                .collect(),
            read_version,
        };
        if state.catalog_advances_remaining > 0 {
            state.catalog_advances_remaining -= 1;
            state.read_version = current_fake_read_version(&state).saturating_add(1);
        }
        if let Some(remove) = state.remove_after_next_catalog.take() {
            state.files.remove(&remove);
            state.grep_authorities.remove(&remove);
            bump_fake_workspace_revision(&mut state, &remove.0);
        }
        Ok(result)
    }

    fn find_workbenches(&self, request: FindRequest) -> Result<FindPage, BackendError> {
        let continued = request.cursor.as_deref() == Some("find-in");
        let mut state = self.lock();
        state.find_requests.push(request);
        let workbenches = if state.find_two_pages {
            let mut summary = fake_committed_summary();
            if continued {
                summary.workbench_id = workbench_id("wb-next");
            }
            vec![summary]
        } else if continued || state.workbenches.is_empty() {
            vec![fake_committed_summary()]
        } else {
            state
                .workbenches
                .iter()
                .map(|workbench| {
                    let mut summary = fake_committed_summary();
                    summary.workbench_id = workbench_id(workbench);
                    summary.entry_count = fake_workbench_entry_count(&state, workbench);
                    summary
                })
                .collect::<Vec<_>>()
        };
        let read_version = current_fake_read_version(&state);
        Ok(FindPage {
            entry_count: workbenches.len(),
            workbenches,
            next_cursor: if state.find_two_pages {
                (!continued).then(|| "find-in".to_owned())
            } else {
                continued.then(|| "find-next".to_owned())
            },
            read_version,
        })
    }

    fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, BackendError> {
        let commit_id = request.stable_commit_id;
        let envelope_bytes = build_run_manifest_v1(
            &request.workbench_id,
            &request.workbench_path,
            &request.content_digest_uri,
            &request.canonical_manifest,
            &request.manifest_digest_uri,
            commit_id,
            1_700_000_000,
        )
        .unwrap();
        let manifest_size_bytes = envelope_bytes.len() as u64;
        let envelope_digest_uri = sha256_uri(&envelope_bytes);
        let mut state = self.lock();
        if let Some(error) = state.commit_error.take() {
            return Err(error);
        }
        let idempotent_replay = state
            .commit_requests
            .iter()
            .any(|existing| existing.stable_commit_id == commit_id);
        state.commit_requests.push(request);
        Ok(CommitOutcome {
            commit_id,
            generation: 9,
            manifest_size_bytes,
            envelope_digest_uri,
            tree_digest_uri: format!("sha256:{}", "ab".repeat(32)),
            idempotent_replay,
        })
    }

    fn mint_snapshot(&self, request: SnapshotMintRequest) -> Result<SnapshotRecord, BackendError> {
        let mut state = self.lock();
        state.next_snapshot_id += 1;
        let snapshot_id = state.next_snapshot_id;
        let record = SnapshotRecord {
            snapshot_id,
            name: request.name,
            read_version: 46,
            lease_expires_unix_ms: Some(request.lease_millis),
            annotation: request.annotation,
            retire_annotation: None,
            state: SnapshotLifecycleState::Alive,
        };
        state.snapshots.insert(
            (request.workbench_id.as_str().to_owned(), snapshot_id),
            record.clone(),
        );
        Ok(record)
    }

    fn renew_snapshot(
        &self,
        request: SnapshotRenewRequest,
    ) -> Result<SnapshotRecord, BackendError> {
        let mut state = self.lock();
        let key = resolve_snapshot_key(&state, &request.workbench_id, &request.selector)?;
        let record = state
            .snapshots
            .get_mut(&key)
            .expect("resolved snapshot key must exist");
        if record.state != SnapshotLifecycleState::Alive {
            return Err(backend_error(
                BackendErrorKind::SnapshotExpired,
                "snapshot is not alive",
            ));
        }
        record.lease_expires_unix_ms = Some(
            record
                .lease_expires_unix_ms
                .unwrap_or(0)
                .max(request.lease_millis),
        );
        Ok(record.clone())
    }

    fn retire_snapshot(
        &self,
        request: SnapshotRetireRequest,
    ) -> Result<SnapshotRetireOutcome, BackendError> {
        let mut state = self.lock();
        let key = resolve_snapshot_key(&state, &request.workbench_id, &request.selector)?;
        let record = state
            .snapshots
            .get_mut(&key)
            .expect("resolved snapshot key must exist");
        let retired = record.state != SnapshotLifecycleState::Retired;
        if retired {
            record.retire_annotation = request
                .reason
                .map(|reason| json!({"reason": reason, "metadata": Value::Null}));
        }
        record.state = SnapshotLifecycleState::Retired;
        Ok(SnapshotRetireOutcome {
            snapshot_id: record.snapshot_id,
            name: record.name.clone(),
            retired,
            state: record.state.clone(),
            retire_annotation: record.retire_annotation.clone(),
        })
    }

    fn list_snapshots(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Vec<SnapshotRecord>, BackendError> {
        Ok(self
            .lock()
            .snapshots
            .iter()
            .filter(|((id, _), _)| id == workbench_id.as_str())
            .map(|(_, record)| record.clone())
            .collect())
    }

    fn restore(&self, request: RestoreRequest) -> Result<RestoreOutcome, BackendError> {
        let mut state = self.lock();
        if let Some(error) = state.restore_error.take() {
            return Err(error);
        }
        if state.restore_requests.contains(&request) {
            return Ok(RestoreOutcome {
                operation_id: [0x22; 16],
                snapshot_id: 1,
                read_version: 46,
                destination_generation: 1,
                idempotent_replay: true,
            });
        }
        if !state
            .workbenches
            .insert(request.destination_workbench_id.as_str().to_owned())
        {
            return Err(backend_error(
                BackendErrorKind::AlreadyExists,
                "restore destination exists",
            ));
        }
        state.restore_requests.push(request);
        Ok(RestoreOutcome {
            operation_id: [0x22; 16],
            snapshot_id: 1,
            read_version: 46,
            destination_generation: 1,
            idempotent_replay: false,
        })
    }
}

fn resolve_snapshot_key(
    state: &FakeState,
    workbench_id: &WorkbenchId,
    selector: &SnapshotSelector,
) -> Result<(String, u64), BackendError> {
    let workbench_id = workbench_id.as_str();
    match selector {
        SnapshotSelector::Id(snapshot_id) => state
            .snapshots
            .contains_key(&(workbench_id.to_owned(), *snapshot_id))
            .then(|| (workbench_id.to_owned(), *snapshot_id)),
        SnapshotSelector::Name(name) => state
            .snapshots
            .iter()
            .rev()
            .find(|((id, _), record)| id == workbench_id && record.name.as_deref() == Some(name))
            .map(|((id, snapshot_id), _)| (id.clone(), *snapshot_id)),
    }
    .ok_or_else(|| backend_error(BackendErrorKind::SnapshotNotFound, "snapshot not found"))
}

fn backend_error(kind: BackendErrorKind, message: &str) -> BackendError {
    BackendError::new(kind, message, false, json!({"source": "fake"}))
}

fn path_key(path: &ScopedPath) -> (String, String) {
    (path.workbench_id.as_str().to_owned(), path.logical_path())
}

fn fake_scoped_path(workbench_id: &WorkbenchId, logical_path: &str) -> ScopedPath {
    let (first, remainder) = logical_path
        .split_once('/')
        .map_or((logical_path, None), |(first, remainder)| {
            (first, Some(remainder))
        });
    let section = match first {
        "input" => Some(Section::Input),
        "scripts" => Some(Section::Scripts),
        "outputs" => Some(Section::Outputs),
        "logs" => Some(Section::Logs),
        "metadata" => Some(Section::Metadata),
        _ => None,
    };
    match section {
        Some(section) => ScopedPath {
            workbench_id: workbench_id.clone(),
            section: Some(section),
            relative_path: remainder.map(relative_path),
        },
        None => ScopedPath {
            workbench_id: workbench_id.clone(),
            section: None,
            relative_path: Some(relative_path(logical_path)),
        },
    }
}

fn tamper_pending_cursor(cursor: &str, from: &str, to: &str) -> String {
    assert_eq!(from.len(), to.len(), "cursor rewrite must preserve framing");
    let mut payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .expect("test cursor is URL-safe base64");
    let offset = payload
        .windows(from.len())
        .position(|window| window == from.as_bytes())
        .expect("test cursor contains the targeted pending field");
    payload[offset..offset + from.len()].copy_from_slice(to.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
}

fn current_fake_read_version(state: &FakeState) -> u64 {
    state.read_version.max(1)
}

fn fake_stat_record(state: &FakeState, path: &ScopedPath) -> Option<StatRecord> {
    let key = path_key(path);
    if let Some(body) = state.files.get(&key) {
        return Some(StatRecord {
            path: path.clone(),
            kind: ArtifactKind::Artifact,
            artifact: Some(body.metadata.clone()),
            authority: state.grep_authorities.get(&key).copied(),
        });
    }
    if path.relative_path.is_none() && state.workbenches.contains(path.workbench_id.as_str()) {
        return Some(StatRecord {
            path: path.clone(),
            kind: if path.section.is_some() {
                ArtifactKind::Section
            } else {
                ArtifactKind::Workbench
            },
            artifact: None,
            authority: None,
        });
    }
    let prefix = format!("{}/", path.logical_path());
    state
        .files
        .keys()
        .any(|(workbench_id, logical_path)| {
            workbench_id == path.workbench_id.as_str() && logical_path.starts_with(&prefix)
        })
        .then(|| StatRecord {
            path: path.clone(),
            kind: ArtifactKind::Directory,
            artifact: None,
            authority: None,
        })
}

fn next_fake_identity(state: &mut FakeState) -> [u8; nokv_types::FIXED_ID_BYTES] {
    state.next_identity = state.next_identity.saturating_add(1);
    let mut identity = [0_u8; nokv_types::FIXED_ID_BYTES];
    identity[8..].copy_from_slice(&state.next_identity.to_be_bytes());
    identity
}

fn ensure_fake_workspace(
    state: &mut FakeState,
    workbench_id: &WorkbenchId,
) -> WorkspaceIncarnationId {
    let key = workbench_id.as_str().to_owned();
    if let Some(incarnation) = state.workspace_incarnations.get(&key) {
        return *incarnation;
    }
    let incarnation = WorkspaceIncarnationId::from_bytes(next_fake_identity(state));
    state
        .workspace_incarnations
        .insert(key.clone(), incarnation);
    state.workspace_revisions.insert(key, 0);
    incarnation
}

fn next_fake_grep_authority(
    state: &mut FakeState,
    workbench_id: &WorkbenchId,
    generation: u64,
) -> GrepCandidateAuthority {
    let workspace_incarnation_id = ensure_fake_workspace(state, workbench_id);
    let artifact_revision_id = ArtifactRevisionId::from_bytes(next_fake_identity(state));
    bump_fake_workspace_revision(state, workbench_id.as_str());
    let workspace_revision = *state
        .workspace_revisions
        .get(workbench_id.as_str())
        .expect("fake workspace revision exists");
    GrepCandidateAuthority {
        workspace_incarnation_id,
        workspace_revision,
        artifact_revision_id,
        generation,
    }
}

fn bump_fake_workspace_revision(state: &mut FakeState, workbench_id: &str) {
    state.read_version = current_fake_read_version(state).saturating_add(1);
    let revision = state
        .workspace_revisions
        .entry(workbench_id.to_owned())
        .or_default();
    *revision = revision.saturating_add(1);
    let revision = *revision;
    for ((candidate_workbench, _), authority) in &mut state.grep_authorities {
        if candidate_workbench == workbench_id {
            authority.workspace_revision = revision;
        }
    }
}

fn artifact_metadata(bytes: &[u8], generation: u64, content_type: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        generation,
        size_bytes: bytes.len() as u64,
        digest_uri: sha256_uri(bytes),
        content_type: content_type.to_owned(),
        producer: Some("test-agent".to_owned()),
        manifest_id: None,
        indexed_fields: BTreeMap::new(),
    }
}

fn generic_builtin_catalog_fields(field_prefix: Option<&str>) -> Vec<CatalogField> {
    [
        ("path", "string", true, false, false),
        ("name", "string", true, false, false),
        ("kind", "string", false, true, false),
        ("size_bytes", "unsigned", true, false, true),
        ("body.content_type", "string", false, true, false),
        ("body.producer", "string", false, true, false),
        ("body.manifest_id", "string", false, false, false),
    ]
    .into_iter()
    .filter(|(field, ..)| {
        field_prefix.is_none_or(|prefix| field.starts_with(prefix) || field.contains(prefix))
    })
    .map(
        |(field, scalar_type, sortable, facetable, aggregatable)| CatalogField {
            field: field.to_owned(),
            scalar_type: scalar_type.to_owned(),
            scalar_types: vec![scalar_type.to_owned()],
            generic_custom: false,
            operators: if scalar_type == "string" {
                vec![
                    "eq".to_owned(),
                    "ne".to_owned(),
                    "in".to_owned(),
                    "prefix".to_owned(),
                    "suffix".to_owned(),
                    "contains".to_owned(),
                    "exists".to_owned(),
                    "not_exists".to_owned(),
                ]
            } else {
                vec![
                    "eq".to_owned(),
                    "ne".to_owned(),
                    "in".to_owned(),
                    "gt".to_owned(),
                    "gte".to_owned(),
                    "lt".to_owned(),
                    "lte".to_owned(),
                    "exists".to_owned(),
                    "not_exists".to_owned(),
                ]
            },
            sortable,
            facetable,
            aggregatable,
        },
    )
    .collect()
}

fn sha256_uri(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn fake_committed_summary() -> WorkbenchSummary {
    let workbench_id = workbench_id("wb-main");
    let manifest = json!({"model": "viking", "task": "ptycho"});
    let canonical_manifest = serde_json::to_vec(&manifest).unwrap();
    let manifest_digest_uri = sha256_uri(&canonical_manifest);
    let content_digest_uri = format!("sha256:{}", "01".repeat(32));
    let commit_id = test_commit_identity(&workbench_id, &content_digest_uri, &manifest_digest_uri);
    let envelope_bytes = build_run_manifest_v1(
        &workbench_id,
        "/agents/test/wb/wb-main",
        &content_digest_uri,
        &canonical_manifest,
        &manifest_digest_uri,
        commit_id,
        1_700_000_000,
    )
    .unwrap();
    let envelope = serde_json::from_slice(&envelope_bytes).unwrap();
    WorkbenchSummary {
        workbench_id,
        committed: true,
        commit_id: Some(commit_id),
        entry_count: 5,
        manifest_metadata: Some(artifact_metadata(&envelope_bytes, 3, "application/json")),
        manifest: Some(envelope),
    }
}

fn fake_workbench_entry_count(state: &FakeState, workbench_id: &str) -> usize {
    let mut children = WORKBENCH_SECTIONS
        .into_iter()
        .map(|section| section.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    children.extend(
        state
            .files
            .keys()
            .filter(|(candidate, _)| candidate == workbench_id)
            .filter_map(|(_, path)| path.split('/').next().map(str::to_owned)),
    );
    children.len()
}

fn test_commit_identity(
    workbench_id: &WorkbenchId,
    content_digest_uri: &str,
    manifest_digest_uri: &str,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workbench.commit_identity.v1\0");
    for value in [
        workbench_id.as_str().as_bytes(),
        content_digest_uri.as_bytes(),
        manifest_digest_uri.as_bytes(),
    ] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    hasher.finalize().into()
}

fn workbench_id(value: &str) -> WorkbenchId {
    WorkbenchId::new(value).expect("test workbench id must be valid")
}

fn relative_path(value: &str) -> NormalizedRelativePath {
    NormalizedRelativePath::new(value).expect("test relative path must be valid")
}

fn run(
    handler: &SdkWorkbenchToolHandler<FakeBackend>,
    name: &str,
    arguments: Value,
) -> Result<Value, AgentError> {
    execute_tool(handler, name, &arguments)
}

fn test_handler(backend: FakeBackend) -> SdkWorkbenchToolHandler<FakeBackend> {
    SdkWorkbenchToolHandler::new(backend, TEST_WORKBENCH_ROOT)
        .expect("test workbench root must be valid")
}

fn test_handler_with_limits(
    backend: FakeBackend,
    max_bytes: usize,
    edit_conflict_attempts: usize,
) -> SdkWorkbenchToolHandler<FakeBackend> {
    SdkWorkbenchToolHandler::with_limits(
        backend,
        max_bytes,
        edit_conflict_attempts,
        TEST_WORKBENCH_ROOT,
    )
    .expect("test workbench root must be valid")
}

fn run_generic(
    handler: &SdkGenericAgentToolHandler<FakeBackend>,
    name: &str,
    arguments: Value,
) -> Result<Value, AgentError> {
    execute_generic_agent_tool(handler, name, &arguments)
}

fn tamper_generic_total_cursor(cursor: &Value, total: u64) -> String {
    let cursor = cursor.as_str().expect("test cursor must be a string");
    let mut decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .expect("test cursor must be canonical base64url");
    let version_end = decoded
        .iter()
        .position(|byte| *byte == 0)
        .expect("test cursor must terminate its version");
    if decoded[..version_end].ends_with(b".v1") {
        decoded[version_end + 1..version_end + 9].copy_from_slice(&total.to_be_bytes());
    } else {
        decoded.splice(version_end + 1..version_end + 1, total.to_be_bytes());
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(decoded)
}

#[test]
fn all_seven_generic_tools_dispatch_through_path_native_backend_primitives() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(&workbench, "workbench_create", json!({"id": "wb-main"})).unwrap();
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main",
            "section": "outputs",
            "path": "report.txt",
            "text": "Hello generic Agent\nsecond line\n",
            "content_type": "text/plain"
        }),
    )
    .unwrap();

    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
    let root = TEST_WORKBENCH_ROOT;
    let artifact = format!("{root}/wb-main/outputs/report.txt");
    let section = format!("{root}/wb-main/outputs");

    let ls = run_generic(&handler, "ls", json!({"path": "/"})).unwrap();
    assert_eq!(ls["path"], root);
    assert_eq!(ls["entries"][0]["path"], format!("{root}/wb-main"));
    let stat = run_generic(&handler, "stat", json!({"path": artifact})).unwrap();
    assert_eq!(stat["card"]["kind"], "file");
    assert_eq!(stat["card"]["path"], artifact);
    let catalog = run_generic(
        &handler,
        "catalog",
        json!({"path": root, "include_facets": true}),
    )
    .unwrap();
    assert!(catalog["catalog"]["sortable"]
        .as_array()
        .is_some_and(|fields| fields.contains(&json!("path"))));
    let read = run_generic(
        &handler,
        "read",
        json!({"path": artifact, "format": "structured", "limit": 100}),
    )
    .unwrap();
    assert_eq!(
        read["items"][0]["value"],
        json!({"line": 1, "text": "Hello generic Agent"})
    );
    let aggregate = run_generic(
        &handler,
        "aggregate",
        json!({"path": root, "measures": [{"name": "files", "op": "count"}]}),
    )
    .unwrap();
    assert_eq!(aggregate["input_match_count"], 1);
    let find = run_generic(
        &handler,
        "find",
        json!({"path": root, "fields": ["body.producer"]}),
    )
    .unwrap();
    assert_eq!(find["matches"][0]["path"], artifact);
    let grep = run_generic(
        &handler,
        "grep",
        json!({"path": section, "pattern": "hello", "recursive": true}),
    )
    .unwrap();
    assert_eq!(grep["matches"][0]["path"], artifact);
    assert_eq!(grep["matches"][0]["line_number"], 1);

    for value in [&ls, &stat, &catalog, &read, &aggregate, &find, &grep] {
        assert!(value.get("status").is_none());
        assert!(value.get("workbench_id").is_none());
        assert!(value.get("section").is_none());
    }
    let state = backend.lock();
    assert_eq!(
        state.list_requests.len(),
        0,
        "root ls and catalog use root discovery/query while artifact stat uses catalog as its fence"
    );
    assert_eq!(state.find_requests.len(), 1);
    assert_eq!(state.stat_views, vec![ReadView::Live]);
    assert_eq!(state.search_requests.len(), 1);
    assert_eq!(state.search_requests[0].profile, generic_query_profile());
    assert_eq!(state.aggregate_requests.len(), 1);
    assert_eq!(state.aggregate_requests[0].profile, generic_query_profile());
    assert!(state
        .catalog_requests
        .iter()
        .all(|request| request.profile == generic_query_profile()));
    assert_eq!(state.grep_requests.len(), 1);
    assert!(state
        .search_requests
        .iter()
        .any(|request| request.scope.workbench_id.is_none()));
}

#[test]
fn generic_paths_are_confined_to_the_admitted_presentation_root() {
    let backend = FakeBackend::default();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
    for path in [
        "/agents/other/wb/wb-main/outputs/report.txt",
        "/agents/test/wb/../other",
        "/agents/test/wb/wb-main//report.txt",
    ] {
        let error = run_generic(&handler, "stat", json!({"path": path}))
            .expect_err("path escape or unknown section must fail");
        assert_eq!(error.code, "InvalidArguments");
    }
    let state = backend.lock();
    assert!(state.stat_views.is_empty());
    assert!(state.catalog_requests.is_empty());
}

#[test]
fn generic_structured_read_fails_loudly_above_three_hundred_records() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    let records = (0..301)
        .map(|index| json!({"index": index}))
        .collect::<Vec<_>>();
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main",
            "section": "outputs",
            "path": "large.json",
            "text": serde_json::to_string(&records).unwrap(),
            "content_type": "application/json"
        }),
    )
    .unwrap();
    backend.lock().read_requests.clear();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let error = run_generic(
        &handler,
        "read",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/large.json"),
            "format": "structured",
            "limit": 100
        }),
    )
    .expect_err("large structured reads must direct callers to bounded byte reads");
    assert_eq!(error.code, "InvalidArguments");
    assert!(error.message.contains(
        "use bytes format with offset and limit, grep to locate lines, or stat record_count"
    ));
    assert!(
        backend.lock().read_requests.is_empty(),
        "the guard must use typed inspection before the normal read primitive"
    );
}

#[test]
fn generic_structured_decode_error_reports_the_presentation_path() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main",
            "section": "outputs",
            "path": "broken.json",
            "text": "{",
            "content_type": "application/json"
        }),
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let path = format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/broken.json");

    let error = run_generic(
        &handler,
        "read",
        json!({"path": path, "format": "structured", "limit": 100}),
    )
    .expect_err("malformed structured data must fail explicitly");

    assert_eq!(error.code, "StructuredDecodeFailed");
    assert_eq!(error.details["path"], path);
    assert_eq!(error.details["format"], "json");
}

#[test]
fn generic_stat_restores_structured_summary_with_an_empty_exact_catalog() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main",
            "section": "outputs",
            "path": "records.json",
            "text": "[{\"status\":\"complete\"},{\"status\":\"failed\"}]",
            "content_type": "application/json"
        }),
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let output = run_generic(
        &handler,
        "stat",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/records.json")}),
    )
    .unwrap();

    assert_eq!(output["card"]["record_count"], 2);
    assert_eq!(output["card"]["schema"]["record_type"], "json_array");
    assert_eq!(output["card"]["schema"]["fields"], json!(["status"]));
    assert_eq!(output["card"]["sample"].as_array().map(Vec::len), Some(2));
    assert_eq!(output["card"]["catalog"]["facets"], json!([]));
}

#[test]
fn generic_stat_retries_instead_of_mixing_a_fenced_card_with_replaced_structured_bytes() {
    let backend = FakeBackend::default();
    WorkbenchBackend::publish(
        &backend,
        PublishRequest {
            path: ScopedPath {
                workbench_id: workbench_id("wb-main"),
                section: Some(Section::Outputs),
                relative_path: Some(relative_path("records.json")),
            },
            body: br#"[{"old":1}]"#.to_vec(),
            content_type: "application/json".to_owned(),
            condition: PublishCondition::CreateOnly,
        },
    )
    .unwrap();
    backend.replace_before_next_grep_body(br#"[{"new":1},{"new":2}]"#.to_vec());
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let output = run_generic(
        &handler,
        "stat",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/records.json")}),
    )
    .unwrap();

    assert_eq!(output["card"]["record_count"], 2);
    assert_eq!(output["card"]["schema"]["fields"], json!(["new"]));
    assert_eq!(backend.lock().read_requests.len(), 2);
}

#[test]
fn generic_stat_restores_directory_count_schema_and_sample() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    for name in ["a.txt", "b.txt", "c.txt", "d.txt"] {
        run(
            &workbench,
            "workbench_put_file",
            json!({
                "id": "wb-main", "section": "outputs", "path": name,
                "text": name, "content_type": "text/plain"
            }),
        )
        .unwrap();
    }
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let output = run_generic(
        &handler,
        "stat",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs")}),
    )
    .unwrap();

    assert_eq!(output["card"]["entry_count"], 4);
    assert_eq!(output["card"]["record_count"], 4);
    assert_eq!(
        output["card"]["schema"],
        json!({
            "record_type": "directory_entries",
            "fields": ["path", "file.name", "file.type", "file.size_bytes"]
        })
    );
    assert_eq!(output["card"]["sample"], json!(["a.txt", "b.txt", "c.txt"]));
}

#[test]
fn generic_stat_restores_admitted_root_directory_summary() {
    let backend = FakeBackend::default();
    WorkbenchBackend::publish(
        &backend,
        PublishRequest {
            path: ScopedPath {
                workbench_id: workbench_id("wb-main"),
                section: Some(Section::Outputs),
                relative_path: Some(relative_path("indexed.json")),
            },
            body: br#"{"score":7}"#.to_vec(),
            content_type: "application/json".to_owned(),
            condition: PublishCondition::CreateOnly,
        },
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let output = run_generic(&handler, "stat", json!({"path": "/"})).unwrap();

    assert_eq!(output["card"]["path"], TEST_WORKBENCH_ROOT);
    assert_eq!(output["card"]["entry_count"], 1);
    assert_eq!(output["card"]["record_count"], 1);
    assert_eq!(output["card"]["schema"]["record_type"], "directory_entries");
    assert_eq!(output["card"]["sample"], json!(["wb-main"]));
    assert!(output["card"]["catalog"]["sortable"]
        .as_array()
        .is_some_and(|fields| fields.contains(&json!("path"))));
}

#[test]
fn generic_list_and_find_report_total_counts_without_consuming_the_returned_cursor() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    for name in ["a.txt", "b.txt", "c.txt"] {
        run(
            &workbench,
            "workbench_put_file",
            json!({
                "id": "wb-main", "section": "outputs", "path": name,
                "text": name, "content_type": "text/plain"
            }),
        )
        .unwrap();
    }
    backend.lock().search_two_pages = true;
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let listed = run_generic(
        &handler,
        "ls",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "limit": 1
        }),
    )
    .unwrap();
    assert_eq!(listed["entry_count"], 3);
    assert_eq!(listed["entries"].as_array().map(Vec::len), Some(1));
    assert!(listed["next_cursor"].is_string());
    assert_eq!(listed["truncated"], true);
    let listed_cursor = listed["next_cursor"].clone();
    let listed_second = run_generic(
        &handler,
        "ls",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "cursor": listed_cursor,
            "limit": 1
        }),
    )
    .unwrap();
    assert_eq!(listed_second["entry_count"], 3);
    assert_eq!(listed_second["entries"].as_array().map(Vec::len), Some(1));
    if let Ok(tampered) = run_generic(
        &handler,
        "ls",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "cursor": tamper_generic_total_cursor(&listed["next_cursor"], 999),
            "limit": 1
        }),
    ) {
        assert_eq!(tampered["entry_count"], 3);
    }

    let found = run_generic(
        &handler,
        "find",
        json!({"path": TEST_WORKBENCH_ROOT, "limit": 1}),
    )
    .unwrap();
    assert_eq!(found["match_count"], 2);
    assert_eq!(found["matches"].as_array().map(Vec::len), Some(1));
    assert!(found["next_cursor"].is_string());
    assert_eq!(found["truncated"], true);
    let found_cursor = found["next_cursor"].clone();
    let found_second = run_generic(
        &handler,
        "find",
        json!({
            "path": TEST_WORKBENCH_ROOT,
            "cursor": found_cursor,
            "limit": 1
        }),
    )
    .unwrap();
    assert_eq!(found_second["match_count"], 2);
    assert_eq!(
        found_second["matches"][0]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/second.txt")
    );
    if let Ok(tampered) = run_generic(
        &handler,
        "find",
        json!({
            "path": TEST_WORKBENCH_ROOT,
            "cursor": tamper_generic_total_cursor(&found["next_cursor"], 999),
            "limit": 1
        }),
    ) {
        assert_eq!(tampered["match_count"], 2);
    }

    backend.lock().find_two_pages = true;
    let root_listed = run_generic(
        &handler,
        "ls",
        json!({"path": TEST_WORKBENCH_ROOT, "limit": 1}),
    )
    .unwrap();
    assert_eq!(root_listed["entry_count"], 2);
    assert!(root_listed["next_cursor"].is_string());
    let root_listed_cursor = root_listed["next_cursor"].clone();
    let root_listed_second = run_generic(
        &handler,
        "ls",
        json!({
            "path": TEST_WORKBENCH_ROOT,
            "cursor": root_listed_cursor,
            "limit": 1
        }),
    )
    .unwrap();
    assert_eq!(root_listed_second["entry_count"], 2);
    if let Ok(tampered) = run_generic(
        &handler,
        "ls",
        json!({
            "path": TEST_WORKBENCH_ROOT,
            "cursor": tamper_generic_total_cursor(&root_listed["next_cursor"], 999),
            "limit": 1
        }),
    ) {
        assert_eq!(tampered["entry_count"], 2);
    }
}

#[test]
fn generic_find_and_aggregate_require_a_directory_at_the_query_read_version() {
    let backend = FakeBackend::default();
    for path in ["collision", "collision/child.txt"] {
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path(path)),
                },
                body: path.as_bytes().to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
    }
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let exact = format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/collision");
    let missing = format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/missing");

    for tool in ["find", "aggregate"] {
        let arguments = |path: &str| match tool {
            "find" => json!({
                "path": path,
                "fields": ["path"],
                "predicates": [{"field": "kind", "op": "eq", "value": "file"}]
            }),
            "aggregate" => {
                json!({
                    "path": path,
                    "group_by": ["path"],
                    "predicates": [{"field": "kind", "op": "eq", "value": "file"}],
                    "measures": [{"name": "files", "op": "count"}]
                })
            }
            _ => unreachable!(),
        };
        let exact_error = run_generic(&handler, tool, arguments(&exact))
            .expect_err("an exact artifact must win over same-name descendants");
        assert_eq!(exact_error.code, "NotDirectory", "{tool}");

        let missing_error = run_generic(&handler, tool, arguments(&missing))
            .expect_err("a missing nested query scope must fail closed");
        assert_eq!(missing_error.code, "NotFound", "{tool}");
    }
}

#[test]
fn generic_paths_round_trip_out_of_band_artifacts_and_workbench_stat_uses_list_total() {
    let backend = FakeBackend::default();
    for (section, path, body) in [
        (None, "note.txt", b"direct note".as_slice()),
        (None, "junk/scratch.txt", b"freq rogue".as_slice()),
        (
            Some(Section::Outputs),
            "spectrum.csv",
            b"freq,power\n1,2\n".as_slice(),
        ),
    ] {
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-oob"),
                    section,
                    relative_path: Some(relative_path(path)),
                },
                body: body.to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
    }
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let workbench_path = format!("{TEST_WORKBENCH_ROOT}/wb-oob");
    let note_path = format!("{workbench_path}/note.txt");
    let scratch_path = format!("{workbench_path}/junk/scratch.txt");

    let root_listed = run_generic(&handler, "ls", json!({"path": TEST_WORKBENCH_ROOT})).unwrap();
    let root_workbench = root_listed["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["path"] == workbench_path))
        .expect("root listing must expose the out-of-band workbench");
    assert_eq!(root_workbench["entry_count"], 7);

    let listed = run_generic(&handler, "ls", json!({"path": workbench_path})).unwrap();
    let listed_paths = listed["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["path"].as_str())
        .collect::<Vec<_>>();
    assert!(listed_paths.contains(&note_path.as_str()));
    assert!(listed_paths.contains(&format!("{workbench_path}/junk").as_str()));
    assert!(!listed_paths.contains(&scratch_path.as_str()));
    assert_eq!(listed["entry_count"], 7);

    let workbench_stat = run_generic(&handler, "stat", json!({"path": workbench_path})).unwrap();
    assert_eq!(workbench_stat["card"]["entry_count"], 7);
    let expected_sample = listed["entries"]
        .as_array()
        .unwrap()
        .iter()
        .take(3)
        .map(|entry| entry["name"].clone())
        .collect::<Vec<_>>();
    assert_eq!(workbench_stat["card"]["sample"], json!(expected_sample));

    for path in [&note_path, &scratch_path] {
        let stat = run_generic(&handler, "stat", json!({"path": path})).unwrap();
        assert_eq!(stat["card"]["kind"], "file");
        let read = run_generic(
            &handler,
            "read",
            json!({"path": path, "format": "bytes", "limit": 300}),
        )
        .unwrap();
        assert!(!read["bytes"].as_array().unwrap().is_empty());
    }

    for path in ["/", workbench_path.as_str()] {
        let grep = run_generic(
            &handler,
            "grep",
            json!({"path": path, "pattern": "freq", "recursive": true}),
        )
        .unwrap();
        let paths = grep["matches"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|entry| entry["path"].as_str())
            .collect::<Vec<_>>();
        assert!(paths.contains(&scratch_path.as_str()));
        assert!(paths.contains(&format!("{workbench_path}/outputs/spectrum.csv").as_str()));
    }
    let scoped = run_generic(
        &handler,
        "grep",
        json!({
            "path": format!("{workbench_path}/junk"),
            "pattern": "freq", "recursive": true
        }),
    )
    .unwrap();
    assert_eq!(scoped["matches"][0]["path"], scratch_path);
}

#[test]
fn generic_find_preserves_presentation_paths_and_projects_legacy_builtins() {
    let backend = FakeBackend::default();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let output = run_generic(
        &handler,
        "find",
        json!({
            "path": "/",
            "predicates": [{
                "field": "path",
                "op": "eq",
                "value": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")
            }],
            "fields": ["path", "body.producer", "body.manifest_id"]
        }),
    )
    .unwrap();

    let state = backend.lock();
    assert_eq!(state.search_requests[0].profile, generic_query_profile());
    assert_eq!(
        state.search_requests[0].predicates[0].value,
        Some(QueryValue::String(format!(
            "{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt"
        )))
    );
    assert_eq!(
        state.search_requests[0].fields,
        vec!["path", "body.producer", "body.manifest_id"]
    );
    assert_eq!(
        output["matches"][0]["values"]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")
    );
    assert_eq!(
        output["matches"][0]["values"]["body.producer"],
        "test-agent"
    );
    assert!(output["matches"][0]["values"]
        .get("body.manifest_id")
        .is_none());
}

#[test]
fn generic_find_preserves_ordered_repeated_custom_values() {
    let backend = FakeBackend::default();
    backend.lock().generic_indexed_values.insert(
        "experiment.labels".to_owned(),
        vec![
            QueryValue::String("alpha".to_owned()),
            QueryValue::Unsigned(7),
            QueryValue::String("alpha".to_owned()),
        ],
    );
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let output = run_generic(
        &handler,
        "find",
        json!({"path": "/", "fields": ["experiment.labels"]}),
    )
    .unwrap();

    assert_eq!(
        output["matches"][0]["values"]["experiment.labels"],
        json!(["alpha", 7, "alpha"])
    );
}

#[test]
fn generic_catalog_exposes_a_declared_zero_row_custom_field() {
    let backend = FakeBackend::default();
    backend.lock().generic_catalog_fields = Some(vec![CatalogField {
        field: "experiment.labels".to_owned(),
        scalar_type: "string".to_owned(),
        scalar_types: Vec::new(),
        generic_custom: true,
        operators: vec!["eq".to_owned(), "in".to_owned()],
        sortable: true,
        facetable: true,
        aggregatable: true,
    }]);
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let output = run_generic(
        &handler,
        "catalog",
        json!({"path": "/", "include_facets": false}),
    )
    .unwrap();

    assert_eq!(output["catalog"]["sortable"], json!(["experiment.labels"]));
    assert_eq!(output["catalog"]["facetable"], json!(["experiment.labels"]));
    assert_eq!(
        output["catalog"]["filterable"][0],
        json!({"operators": ["eq", "in"], "fields": ["experiment.labels"]})
    );
}

#[test]
fn generic_path_predicates_preserve_the_complete_presentation_value_domain() {
    let backend = FakeBackend::default();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    run_generic(
        &handler,
        "find",
        json!({
            "path": "/",
            "predicates": [
                {"field": "path", "op": "prefix", "value": TEST_WORKBENCH_ROOT},
                {"field": "path", "op": "contains", "value": "agents/test"},
                {"field": "path", "op": "suffix", "value": "wb/wb-main"},
                {"field": "path", "op": "in", "value": [
                    TEST_WORKBENCH_ROOT,
                    format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")
                ]}
            ]
        }),
    )
    .unwrap();

    let state = backend.lock();
    let predicates = &state.search_requests[0].predicates;
    assert_eq!(predicates.len(), 4);
    assert_eq!(
        predicates[0].value,
        Some(QueryValue::String(TEST_WORKBENCH_ROOT.to_owned()))
    );
    assert_eq!(
        predicates[1].value,
        Some(QueryValue::String("agents/test".to_owned()))
    );
    assert_eq!(
        predicates[2].value,
        Some(QueryValue::String("wb/wb-main".to_owned()))
    );
    assert_eq!(
        predicates[3].value,
        Some(QueryValue::List(vec![
            QueryValue::String(TEST_WORKBENCH_ROOT.to_owned()),
            QueryValue::String(format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")),
        ]))
    );
}

#[test]
fn generic_list_cursors_bind_storage_root_and_presentation_layout_before_dispatch() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    for name in ["a.txt", "b.txt"] {
        run(
            &workbench,
            "workbench_put_file",
            json!({
                "id": "wb-main", "section": "outputs", "path": name,
                "text": name, "content_type": "text/plain"
            }),
        )
        .unwrap();
    }
    backend.lock().find_two_pages = true;
    let original = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
    let scoped = run_generic(
        &original,
        "ls",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "limit": 1
        }),
    )
    .unwrap();
    let root = run_generic(
        &original,
        "ls",
        json!({"path": TEST_WORKBENCH_ROOT, "limit": 1}),
    )
    .unwrap();

    let alternate_root = "/agents/alternate/wb";
    let alternate = SdkGenericAgentToolHandler::new(backend.clone(), alternate_root).unwrap();
    let dispatches_before = {
        let state = backend.lock();
        (
            state.stat_views.len(),
            state.list_requests.len(),
            state.find_requests.len(),
        )
    };
    let scoped_error = run_generic(
        &alternate,
        "ls",
        json!({
            "path": format!("{alternate_root}/wb-main/outputs"),
            "cursor": scoped["next_cursor"],
            "limit": 1
        }),
    )
    .expect_err("scoped cursor must bind the presentation layout");
    assert_eq!(scoped_error.code, "InvalidArguments");
    let root_error = run_generic(
        &alternate,
        "ls",
        json!({
            "path": alternate_root,
            "cursor": root["next_cursor"],
            "limit": 1
        }),
    )
    .expect_err("root cursor must bind the presentation layout");
    assert_eq!(root_error.code, "InvalidArguments");
    {
        let state = backend.lock();
        assert_eq!(
            (
                state.stat_views.len(),
                state.list_requests.len(),
                state.find_requests.len(),
            ),
            dispatches_before
        );
    }

    let other_backend = FakeBackend::with_root(2);
    other_backend.lock().find_two_pages = true;
    let other =
        SdkGenericAgentToolHandler::new(other_backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
    let root_error = run_generic(
        &other,
        "ls",
        json!({
            "path": TEST_WORKBENCH_ROOT,
            "cursor": root["next_cursor"],
            "limit": 1
        }),
    )
    .expect_err("root cursor must bind the storage root");
    assert_eq!(root_error.code, "InvalidArguments");
    assert!(other_backend.lock().find_requests.is_empty());
}

#[test]
fn generic_read_exact_section_root_artifact_wins_over_the_virtual_section() {
    let backend = FakeBackend::default();
    WorkbenchBackend::publish(
        &backend,
        PublishRequest {
            path: ScopedPath {
                workbench_id: workbench_id("wb-main"),
                section: None,
                relative_path: Some(relative_path("outputs")),
            },
            body: b"exact outputs".to_vec(),
            content_type: "text/plain".to_owned(),
            condition: PublishCondition::CreateOnly,
        },
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let read = run_generic(
        &handler,
        "read",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "format": "bytes",
            "limit": 100
        }),
    )
    .unwrap();
    assert_eq!(read["bytes"], json!(b"exact outputs"));
    assert_eq!(backend.lock().inspection_requests.len(), 1);

    let virtual_error = run_generic(
        &handler,
        "read",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/scripts"),
            "format": "bytes",
            "limit": 100
        }),
    )
    .expect_err("a virtual empty section is not a file");
    assert_eq!(virtual_error.code, "InvalidArguments");
}

#[test]
fn generic_aggregate_reports_distinct_input_row_and_group_totals() {
    let backend = FakeBackend::default();
    backend.lock().aggregate_exact_totals = true;
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let output = run_generic(
        &handler,
        "aggregate",
        json!({
            "path": "/",
            "group_by": ["body.producer"],
            "measures": [{"name": "produced", "op": "count", "field": "body.producer"}],
            "limit": 1
        }),
    )
    .unwrap();

    assert_eq!(output["input_match_count"], 5);
    assert_eq!(output["row_count"], 3);
    assert_eq!(output["group_count"], 2);
    assert_eq!(output["groups"].as_array().map(Vec::len), Some(1));
    assert_eq!(output["truncated"], true);
}

#[test]
fn generic_aggregate_path_group_key_uses_the_presentation_value_domain() {
    let backend = FakeBackend::default();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let output = run_generic(
        &handler,
        "aggregate",
        json!({
            "path": "/",
            "group_by": ["path"],
            "measures": [{"name": "rows", "op": "count"}]
        }),
    )
    .unwrap();
    assert_eq!(
        output["groups"][0]["key"]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")
    );
}

#[test]
fn generic_catalog_matches_infixes_and_suggests_bounded_child_catalogs() {
    let backend = FakeBackend::default();
    backend.lock().filtered_catalog_is_empty = true;
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let infix = run_generic(
        &handler,
        "catalog",
        json!({"path": "/", "field_prefix": "producer", "include_facets": false}),
    )
    .unwrap();
    assert_eq!(infix["catalog"]["sortable"], json!([]));
    assert_eq!(infix["catalog"]["facetable"], json!(["body.producer"]));

    let output = run_generic(
        &handler,
        "catalog",
        json!({"path": "/", "field_prefix": "missing", "include_facets": false}),
    )
    .unwrap();
    assert_eq!(output["catalog_empty"], true);
    assert_eq!(
        output["child_catalogs"][0]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main")
    );
    assert_eq!(
        output["child_catalogs"][0]["catalog"]["sortable"],
        json!(["path", "name", "size_bytes"])
    );
}

#[test]
fn generic_grep_recursively_scans_the_admitted_root() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main", "section": "logs", "path": "run.log",
            "text": "first\nNeedle at root\n", "content_type": "text/plain"
        }),
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let output = run_generic(
        &handler,
        "grep",
        json!({"path": "/", "pattern": "needle", "recursive": true}),
    )
    .unwrap();
    assert_eq!(output["path"], TEST_WORKBENCH_ROOT);
    assert_eq!(output["matches"][0]["line_number"], 2);
    assert_eq!(
        output["matches"][0]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/logs/run.log")
    );
}

#[test]
fn generic_grep_exact_artifact_wins_over_descendants_for_both_recursion_modes() {
    for recursive in [false, true] {
        let backend = FakeBackend::default();
        for (path, body) in [
            ("exact.log", b"needle exact\n".as_slice()),
            ("exact.log/child.log", b"needle child\n".as_slice()),
        ] {
            WorkbenchBackend::publish(
                &backend,
                PublishRequest {
                    path: ScopedPath {
                        workbench_id: workbench_id("wb-main"),
                        section: Some(Section::Outputs),
                        relative_path: Some(relative_path(path)),
                    },
                    body: body.to_vec(),
                    content_type: "text/plain".to_owned(),
                    condition: PublishCondition::CreateOnly,
                },
            )
            .unwrap();
        }
        let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

        let output = run_generic(
            &handler,
            "grep",
            json!({
                "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/exact.log"),
                "pattern": "needle",
                "recursive": recursive,
            }),
        )
        .unwrap();

        assert_eq!(output["files_scanned"], 1, "recursive={recursive}");
        assert_eq!(output["matches"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            output["matches"][0]["path"],
            format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/exact.log")
        );
    }
}

#[test]
fn generic_grep_accepts_an_exact_reserved_section_root_across_scopes_and_glob() {
    struct Case {
        path: String,
        recursive: bool,
        glob: Option<&'static str>,
        include_descendant: bool,
    }

    for case in [
        Case {
            path: "/".to_owned(),
            recursive: true,
            glob: None,
            include_descendant: false,
        },
        Case {
            path: format!("{TEST_WORKBENCH_ROOT}/wb-main"),
            recursive: true,
            glob: None,
            include_descendant: false,
        },
        Case {
            path: format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            recursive: false,
            glob: None,
            include_descendant: true,
        },
        Case {
            path: format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            recursive: true,
            glob: None,
            include_descendant: true,
        },
        Case {
            path: format!("{TEST_WORKBENCH_ROOT}/wb-main"),
            recursive: true,
            glob: Some("outputs"),
            include_descendant: true,
        },
    ] {
        let backend = FakeBackend::default();
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: None,
                },
                body: b"needle exact section root\n".to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
        if case.include_descendant {
            WorkbenchBackend::publish(
                &backend,
                PublishRequest {
                    path: ScopedPath {
                        workbench_id: workbench_id("wb-main"),
                        section: Some(Section::Outputs),
                        relative_path: Some(relative_path("child.txt")),
                    },
                    body: b"needle descendant must not leak\n".to_vec(),
                    content_type: "text/plain".to_owned(),
                    condition: PublishCondition::CreateOnly,
                },
            )
            .unwrap();
        }
        let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
        let mut arguments = json!({
            "path": case.path,
            "pattern": "needle",
            "recursive": case.recursive,
        });
        if let Some(glob) = case.glob {
            arguments["glob"] = Value::String(glob.to_owned());
        }

        let output = run_generic(&handler, "grep", arguments).unwrap();

        assert_eq!(output["files_scanned"], 1);
        assert_eq!(output["matches"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            output["matches"][0]["path"],
            format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs")
        );
    }
}

#[test]
fn generic_grep_v4_cursor_round_trips_an_exact_section_root_and_rejects_marker_tampering() {
    let backend = FakeBackend::default();
    WorkbenchBackend::publish(
        &backend,
        PublishRequest {
            path: ScopedPath {
                workbench_id: workbench_id("wb-main"),
                section: Some(Section::Outputs),
                relative_path: None,
            },
            body: b"needle one\nneedle two\n".to_vec(),
            content_type: "text/plain".to_owned(),
            condition: PublishCondition::CreateOnly,
        },
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
    let mut arguments = json!({
        "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
        "pattern": "needle",
        "recursive": true,
        "limit": 1,
    });

    let first = run_generic(&handler, "grep", arguments.clone()).unwrap();
    assert_eq!(first["matches"][0]["line_number"], 1);
    assert_eq!(
        first["matches"][0]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs")
    );
    let cursor = first["next_cursor"]
        .as_str()
        .expect("mid-file section-root result has a pending cursor");
    let cursor_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .expect("test cursor is URL-safe base64");
    assert!(cursor_bytes.starts_with(b"nokv.agent.generic.grep-cursor.v4\0"));

    let mut tampered_bytes = cursor_bytes.clone();
    let section_root_path = b"\0\0\0\x07wb-main\x03\0";
    let marker_offset = tampered_bytes
        .windows(section_root_path.len())
        .position(|window| window == section_root_path)
        .map(|offset| offset + section_root_path.len() - 1)
        .expect("v4 cursor carries an explicit section-root relative-path marker");
    tampered_bytes[marker_offset] = 2;
    let tampered = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(tampered_bytes);
    arguments["cursor"] = Value::String(tampered);
    let calls_before = {
        let state = backend.lock();
        (
            state.grep_requests.len(),
            state.grep_metadata_requests.len(),
            state.read_requests.len(),
        )
    };
    let error = run_generic(&handler, "grep", arguments.clone())
        .expect_err("tampered section-root marker must fail before backend dispatch");
    assert_eq!(error.code, "InvalidArguments");
    {
        let state = backend.lock();
        assert_eq!(state.grep_requests.len(), calls_before.0);
        assert_eq!(state.grep_metadata_requests.len(), calls_before.1);
        assert_eq!(state.read_requests.len(), calls_before.2);
    }

    arguments["cursor"] = Value::String(cursor.to_owned());
    let second = run_generic(&handler, "grep", arguments).unwrap();
    assert_eq!(second["matches"][0]["line_number"], 2);
    assert_eq!(
        second["matches"][0]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs")
    );
    assert!(second["next_cursor"].is_null());
    assert_eq!(backend.lock().grep_requests.len(), 1);
}

#[test]
fn generic_grep_distinguishes_a_missing_nested_scope_from_an_empty_virtual_section() {
    let backend = FakeBackend::default();
    WorkbenchBackend::create_workbench(&backend, &workbench_id("wb-main")).unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let missing = run_generic(
        &handler,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/missing"),
            "pattern": "needle",
            "recursive": true,
        }),
    )
    .expect_err("missing nested grep scope must fail closed");
    assert_eq!(missing.code, "NotFound");
    assert_eq!(
        missing.details["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/missing")
    );

    let empty_section = run_generic(
        &handler,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "pattern": "needle",
            "recursive": true,
        }),
    )
    .unwrap();
    assert_eq!(empty_section["matches"], json!([]));
    assert_eq!(empty_section["files_scanned"], 0);
    assert_eq!(empty_section["truncated"], false);
}

#[test]
fn generic_grep_limits_matching_lines_and_resumes_inside_one_artifact() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main", "section": "logs", "path": "many.log",
            "text": "needle one\nneedle two\nneedle three\n", "content_type": "text/plain"
        }),
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let arguments = json!({
        "path": "/", "pattern": "needle", "recursive": true, "limit": 1
    });

    let first = run_generic(&handler, "grep", arguments.clone()).unwrap();
    assert_eq!(first["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(first["matches"][0]["line_number"], 1);
    assert_eq!(first["truncated"], true);
    let second = run_generic(
        &handler,
        "grep",
        json!({
            "path": "/", "pattern": "needle", "recursive": true, "limit": 1,
            "cursor": first["next_cursor"]
        }),
    )
    .unwrap();
    assert_eq!(second["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(second["matches"][0]["line_number"], 2);
    assert_eq!(second["truncated"], true);
    let third = run_generic(
        &handler,
        "grep",
        json!({
            "path": "/", "pattern": "needle", "recursive": true, "limit": 1,
            "cursor": second["next_cursor"]
        }),
    )
    .unwrap();
    assert_eq!(third["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(third["matches"][0]["line_number"], 3);
    assert_eq!(third["truncated"], false);
    assert_eq!(third["next_cursor"], Value::Null);
}

#[test]
fn grep_patterns_allow_an_empty_primary_and_sixteen_raw_alternatives() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-patterns", "section": "logs", "path": "events.log",
            "text": "alternative\nprimary\n", "content_type": "text/plain"
        }),
    )
    .unwrap();
    let generic = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let empty_primary = run_generic(
        &generic,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-patterns/logs/events.log"),
            "pattern": "", "patterns": ["alternative"], "recursive": false
        }),
    )
    .expect("a non-empty alternative makes an empty primary valid");
    assert_eq!(empty_primary["matches"].as_array().map(Vec::len), Some(1));

    let alternatives = (0..16)
        .map(|index| format!("alternative-{index}"))
        .collect::<Vec<_>>();
    let primary_plus_sixteen = run_generic(
        &generic,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-patterns/logs/events.log"),
            "pattern": "primary", "patterns": alternatives, "recursive": false
        }),
    )
    .expect("the sixteen-item cap applies to raw alternatives, not primary plus alternatives");
    assert_eq!(
        primary_plus_sixteen["matches"].as_array().map(Vec::len),
        Some(1)
    );

    let both_empty = run_generic(
        &generic,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-patterns/logs/events.log"),
            "pattern": "", "patterns": [], "recursive": false
        }),
    )
    .expect_err("grep must still reject an entirely empty pattern set");
    assert_eq!(both_empty.code, "InvalidArguments");
}

#[test]
fn generic_ls_rejects_artifacts_and_missing_prefixes_instead_of_inventing_directories() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(&workbench, "workbench_create", json!({"id": "wb-main"})).unwrap();
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main",
            "section": "outputs",
            "path": "report.txt",
            "text": "report",
            "content_type": "text/plain"
        }),
    )
    .unwrap();
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let artifact_error = run_generic(
        &handler,
        "ls",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")}),
    )
    .unwrap_err();
    assert_eq!(artifact_error.code, "NotDirectory");

    let missing_error = run_generic(
        &handler,
        "ls",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/missing")}),
    )
    .unwrap_err();
    assert_eq!(missing_error.code, "NotFound");
}

#[test]
fn generic_catalog_rejects_a_missing_path_before_query_dispatch() {
    let backend = FakeBackend::default();
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let error = run_generic(
        &handler,
        "catalog",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/missing/outputs")}),
    )
    .unwrap_err();

    assert_eq!(error.code, "NotFound");
    let state = backend.lock();
    assert_eq!(state.catalog_requests.len(), 1);
    assert_eq!(
        state.catalog_requests[0].path_match,
        CatalogPathMatch::Exact
    );
}

#[test]
fn generic_grep_outer_cursor_is_bound_to_the_storage_root_before_dispatch() {
    let root_a = FakeBackend::with_root(1);
    let workbench = test_handler(root_a.clone());
    run(&workbench, "workbench_create", json!({"id": "wb-main"})).unwrap();
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-main",
            "section": "outputs",
            "path": "report.txt",
            "text": "needle one\nneedle two\n",
            "content_type": "text/plain"
        }),
    )
    .unwrap();
    let handler_a = SdkGenericAgentToolHandler::new(root_a, TEST_WORKBENCH_ROOT).unwrap();
    let first = run_generic(
        &handler_a,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "pattern": "needle",
            "recursive": true,
            "limit": 1
        }),
    )
    .unwrap();
    let cursor = first["next_cursor"]
        .as_str()
        .expect("line-truncated first page must carry an outer cursor");

    let root_b = FakeBackend::with_root(2);
    let handler_b = SdkGenericAgentToolHandler::new(root_b.clone(), TEST_WORKBENCH_ROOT).unwrap();
    let error = run_generic(
        &handler_b,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "pattern": "needle",
            "recursive": true,
            "limit": 1,
            "cursor": cursor
        }),
    )
    .unwrap_err();

    assert_eq!(error.code, "InvalidArguments");
    let state = root_b.lock();
    assert!(state.grep_requests.is_empty());
    assert!(state.read_requests.is_empty());
    drop(state);

    let other_presentation = FakeBackend::with_root(1);
    let handler_other =
        SdkGenericAgentToolHandler::new(other_presentation.clone(), "/agents/other/wb").unwrap();
    let error = run_generic(
        &handler_other,
        "grep",
        json!({
            "path": "/agents/other/wb/wb-main/outputs",
            "pattern": "needle",
            "recursive": true,
            "limit": 1,
            "cursor": cursor
        }),
    )
    .unwrap_err();
    assert_eq!(error.code, "InvalidArguments");
    assert!(other_presentation.lock().grep_requests.is_empty());
}

#[test]
fn generic_grep_pending_cursor_rejects_scope_recursive_and_glob_tampering_before_dispatch() {
    struct Case {
        relative_path: &'static str,
        query_suffix: &'static str,
        recursive: bool,
        glob: Option<&'static str>,
        from: &'static str,
        to: &'static str,
    }

    for case in [
        Case {
            relative_path: "scope.log",
            query_suffix: "outputs",
            recursive: true,
            glob: None,
            from: "wb-main",
            to: "wb-evil",
        },
        Case {
            relative_path: "dir/a.x",
            query_suffix: "outputs/dir",
            recursive: false,
            glob: None,
            from: "dir/a.x",
            to: "dir/x/a",
        },
        Case {
            relative_path: "scope.log",
            query_suffix: "outputs",
            recursive: true,
            glob: Some("*.log"),
            from: "scope.log",
            to: "scope.txt",
        },
    ] {
        let backend = FakeBackend::default();
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path(case.relative_path)),
                },
                body: b"needle one\nneedle two\n".to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
        let handler =
            SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
        let mut arguments = json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/{}", case.query_suffix),
            "pattern": "needle",
            "recursive": case.recursive,
            "limit": 1,
        });
        if let Some(glob) = case.glob {
            arguments["glob"] = Value::String(glob.to_owned());
        }
        let first = run_generic(&handler, "grep", arguments.clone()).unwrap();
        let cursor = first["next_cursor"]
            .as_str()
            .expect("mid-file result has a pending cursor");
        let tampered = tamper_pending_cursor(cursor, case.from, case.to);
        arguments["cursor"] = Value::String(tampered);
        let calls_before = {
            let state = backend.lock();
            (
                state.grep_requests.len(),
                state.grep_metadata_requests.len(),
                state.read_requests.len(),
            )
        };

        let error = run_generic(&handler, "grep", arguments)
            .expect_err("tampered pending scope must fail before backend dispatch");
        assert_eq!(error.code, "InvalidArguments");
        let state = backend.lock();
        assert_eq!(state.grep_requests.len(), calls_before.0);
        assert_eq!(state.grep_metadata_requests.len(), calls_before.1);
        assert_eq!(state.read_requests.len(), calls_before.2);
    }
}

#[test]
fn generic_grep_pending_cursor_rejects_replace_prepend_and_authority_aba() {
    enum Drift {
        Replace,
        Prepend,
        Recreate,
        WorkspaceRevision,
    }

    for drift in [
        Drift::Replace,
        Drift::Prepend,
        Drift::Recreate,
        Drift::WorkspaceRevision,
    ] {
        let backend = FakeBackend::default();
        let path = ScopedPath {
            workbench_id: workbench_id("wb-main"),
            section: Some(Section::Outputs),
            relative_path: Some(relative_path("z.log")),
        };
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: path.clone(),
                body: b"needle one\nneedle two\n".to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
        let handler =
            SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();
        let first = run_generic(
            &handler,
            "grep",
            json!({
                "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
                "pattern": "needle",
                "recursive": true,
                "limit": 1
            }),
        )
        .unwrap();
        let cursor = first["next_cursor"]
            .as_str()
            .expect("mid-file result has a pending cursor")
            .to_owned();
        let grep_requests_before_resume = backend.lock().grep_requests.len();

        match drift {
            Drift::Replace => {
                WorkbenchBackend::publish(
                    &backend,
                    PublishRequest {
                        path: path.clone(),
                        body: b"needle replacement\nneedle two\n".to_vec(),
                        content_type: "text/plain".to_owned(),
                        condition: PublishCondition::ReplaceOnly {
                            expected_generation: 1,
                        },
                    },
                )
                .unwrap();
            }
            Drift::Prepend => {
                WorkbenchBackend::publish(
                    &backend,
                    PublishRequest {
                        path: ScopedPath {
                            workbench_id: workbench_id("wb-main"),
                            section: Some(Section::Outputs),
                            relative_path: Some(relative_path("a.log")),
                        },
                        body: b"needle prepended\n".to_vec(),
                        content_type: "text/plain".to_owned(),
                        condition: PublishCondition::CreateOnly,
                    },
                )
                .unwrap();
            }
            Drift::Recreate => backend.recreate_grep_candidate_with_same_generation(&path),
            Drift::WorkspaceRevision => backend.advance_workspace_revision("wb-main"),
        }

        let error = run_generic(
            &handler,
            "grep",
            json!({
                "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
                "pattern": "needle",
                "recursive": true,
                "limit": 1,
                "cursor": cursor
            }),
        )
        .unwrap_err();
        assert_eq!(error.code, "ReadFenceChanged");
        assert_eq!(
            backend.lock().grep_requests.len(),
            grep_requests_before_resume
        );
    }
}

#[test]
fn generic_grep_fails_loudly_when_a_candidate_is_replaced_or_removed_before_body_read() {
    for mutation in [
        GrepCandidateMutation::Replace(b"needle replacement\n".to_vec()),
        GrepCandidateMutation::Remove,
    ] {
        let backend = FakeBackend::default();
        let workbench = test_handler(backend.clone());
        run(&workbench, "workbench_create", json!({"id": "wb-main"})).unwrap();
        run(
            &workbench,
            "workbench_put_file",
            json!({
                "id": "wb-main",
                "section": "outputs",
                "path": "report.txt",
                "text": "needle original\n",
                "content_type": "text/plain"
            }),
        )
        .unwrap();
        backend.mutate_after_next_grep_candidates(mutation);
        let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

        let error = run_generic(
            &handler,
            "grep",
            json!({
                "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
                "pattern": "needle",
                "recursive": true
            }),
        )
        .unwrap_err();
        assert_eq!(error.code, "ReadFenceChanged");
        assert_eq!(
            error.details["path"],
            format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/report.txt")
        );
    }
}

#[test]
fn generic_grep_skips_nul_bodies_and_decodes_other_invalid_utf8_lossily() {
    let backend = FakeBackend::default();
    for (path, bytes) in [
        ("a-nul.txt", b"needle\0hidden\n".to_vec()),
        ("b-invalid.txt", b"\xff needle\n".to_vec()),
    ] {
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path(path)),
                },
                body: bytes,
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
    }
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();

    let output = run_generic(
        &handler,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs"),
            "pattern": "needle",
            "recursive": true
        }),
    )
    .unwrap();

    assert_eq!(output["files_scanned"], 2);
    assert_eq!(output["matches"].as_array().unwrap().len(), 1);
    assert_eq!(output["matches"][0]["line_number"], 1);
    assert_eq!(output["matches"][0]["snippet"], "� needle");
    assert_eq!(
        output["matches"][0]["path"],
        format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/b-invalid.txt")
    );
}

#[test]
fn generic_grep_budget_boundaries_return_progress_cursors_and_only_oversize_files_fail() {
    let root_path = format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs");

    let page_backend = FakeBackend::default();
    page_backend.lock().grep_empty_pages_remaining = 2;
    let page_handler = SdkGenericAgentToolHandler::with_limits(
        page_backend.clone(),
        DEFAULT_WORKBENCH_MAX_BYTES,
        GenericGrepScanLimits::new(1, 10, 1024).unwrap(),
        TEST_WORKBENCH_ROOT,
    )
    .unwrap();
    let first_page = run_generic(
        &page_handler,
        "grep",
        json!({"path": root_path, "pattern": "absent", "recursive": true}),
    )
    .unwrap();
    assert_eq!(first_page["truncated"], true);
    assert!(first_page["next_cursor"].is_string());
    assert_eq!(page_backend.lock().grep_requests.len(), 1);
    let second_page = run_generic(
        &page_handler,
        "grep",
        json!({
            "path": root_path,
            "pattern": "absent",
            "recursive": true,
            "cursor": first_page["next_cursor"]
        }),
    )
    .unwrap();
    assert_eq!(second_page["truncated"], false);

    let file_backend = FakeBackend::default();
    for path in ["a.txt", "b.txt"] {
        WorkbenchBackend::publish(
            &file_backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path(path)),
                },
                body: b"no match\n".to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
    }
    let file_handler = SdkGenericAgentToolHandler::with_limits(
        file_backend.clone(),
        DEFAULT_WORKBENCH_MAX_BYTES,
        GenericGrepScanLimits::new(10, 1, 1024).unwrap(),
        TEST_WORKBENCH_ROOT,
    )
    .unwrap();
    let first_file = run_generic(
        &file_handler,
        "grep",
        json!({"path": root_path, "pattern": "absent", "recursive": true}),
    )
    .unwrap();
    assert_eq!(first_file["truncated"], true);
    assert!(first_file["next_cursor"].is_string());
    assert_eq!(file_backend.lock().read_requests.len(), 1);
    let second_file = run_generic(
        &file_handler,
        "grep",
        json!({
            "path": root_path,
            "pattern": "absent",
            "recursive": true,
            "cursor": first_file["next_cursor"]
        }),
    )
    .unwrap();
    assert_eq!(second_file["truncated"], false);
    assert_eq!(file_backend.lock().read_requests.len(), 2);

    let byte_backend = FakeBackend::default();
    for (path, body) in [("a.txt", b"four".as_slice()), ("b.txt", b"also".as_slice())] {
        WorkbenchBackend::publish(
            &byte_backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path(path)),
                },
                body: body.to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
    }
    let byte_handler = SdkGenericAgentToolHandler::with_limits(
        byte_backend.clone(),
        DEFAULT_WORKBENCH_MAX_BYTES,
        GenericGrepScanLimits::new(10, 10, 5).unwrap(),
        TEST_WORKBENCH_ROOT,
    )
    .unwrap();
    let first_bytes = run_generic(
        &byte_handler,
        "grep",
        json!({"path": root_path, "pattern": "absent", "recursive": true}),
    )
    .unwrap();
    assert_eq!(first_bytes["truncated"], true);
    assert!(first_bytes["next_cursor"].is_string());
    assert_eq!(byte_backend.lock().read_requests.len(), 1);
    let second_bytes = run_generic(
        &byte_handler,
        "grep",
        json!({
            "path": root_path,
            "pattern": "absent",
            "recursive": true,
            "cursor": first_bytes["next_cursor"]
        }),
    )
    .unwrap();
    assert_eq!(second_bytes["truncated"], false);
    assert_eq!(byte_backend.lock().read_requests.len(), 2);

    let oversize_backend = FakeBackend::default();
    WorkbenchBackend::publish(
        &oversize_backend,
        PublishRequest {
            path: ScopedPath {
                workbench_id: workbench_id("wb-main"),
                section: Some(Section::Outputs),
                relative_path: Some(relative_path("oversize.txt")),
            },
            body: b"six!!!".to_vec(),
            content_type: "text/plain".to_owned(),
            condition: PublishCondition::CreateOnly,
        },
    )
    .unwrap();
    let oversize_handler = SdkGenericAgentToolHandler::with_limits(
        oversize_backend.clone(),
        DEFAULT_WORKBENCH_MAX_BYTES,
        GenericGrepScanLimits::new(10, 10, 5).unwrap(),
        TEST_WORKBENCH_ROOT,
    )
    .unwrap();
    let error = run_generic(
        &oversize_handler,
        "grep",
        json!({"path": root_path, "pattern": "absent", "recursive": true}),
    )
    .unwrap_err();
    assert_eq!(error.code, "ResourceExhausted");
    assert!(oversize_backend.lock().read_requests.is_empty());
}

#[test]
fn generic_ls_catalog_and_stat_never_turn_a_deleted_last_child_into_an_empty_success() {
    for tool in ["ls", "catalog", "stat"] {
        let backend = FakeBackend::default();
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path("only/child.txt")),
                },
                body: b"only child".to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
        if matches!(tool, "catalog" | "stat") {
            backend.remove_after_next_catalog("wb-main", "outputs/only/child.txt");
        } else {
            backend.remove_after_next_list("wb-main", "outputs/only/child.txt");
        }
        let handler =
            SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

        let result = run_generic(
            &handler,
            tool,
            json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/only")}),
        );
        let error = result.expect_err(
            "the tool must not return an empty success after its last child disappears",
        );
        assert_eq!(error.code, "NotFound", "{tool}");
    }
}

#[test]
fn generic_catalog_never_mixes_main_and_child_catalogs_across_root_versions() {
    let backend = FakeBackend::default();
    backend.lock().catalog_advances_remaining = 3;
    let handler = SdkGenericAgentToolHandler::new(backend.clone(), TEST_WORKBENCH_ROOT).unwrap();

    let error = run_generic(
        &handler,
        "catalog",
        json!({"path": "/", "field_prefix": "missing", "include_facets": false}),
    )
    .expect_err("persistent root drift must not return mixed child suggestions");

    assert_eq!(error.code, "ReadFenceChanged");
    let state = backend.lock();
    assert_eq!(state.catalog_requests.len(), 3);
    assert_eq!(state.find_requests.len(), 3);
}

#[test]
fn generic_stat_and_catalog_keep_exact_artifact_catalogs_empty() {
    let backend = FakeBackend::default();
    for path in ["collision", "collision/child.txt", "only/child.txt"] {
        WorkbenchBackend::publish(
            &backend,
            PublishRequest {
                path: ScopedPath {
                    workbench_id: workbench_id("wb-main"),
                    section: Some(Section::Outputs),
                    relative_path: Some(relative_path(path)),
                },
                body: path.as_bytes().to_vec(),
                content_type: "text/plain".to_owned(),
                condition: PublishCondition::CreateOnly,
            },
        )
        .unwrap();
    }
    {
        let mut state = backend.lock();
        state.catalog_from_artifacts = true;
        state
            .files
            .get_mut(&("wb-main".to_owned(), "outputs/collision".to_owned()))
            .unwrap()
            .metadata
            .indexed_fields
            .insert(
                "exact.only".to_owned(),
                QueryValue::String("exact".to_owned()),
            );
        state
            .files
            .get_mut(&(
                "wb-main".to_owned(),
                "outputs/collision/child.txt".to_owned(),
            ))
            .unwrap()
            .metadata
            .indexed_fields
            .insert(
                "child.only".to_owned(),
                QueryValue::String("child".to_owned()),
            );
        state
            .files
            .get_mut(&("wb-main".to_owned(), "outputs/only/child.txt".to_owned()))
            .unwrap()
            .metadata
            .indexed_fields
            .insert(
                "directory.child".to_owned(),
                QueryValue::String("child".to_owned()),
            );
    }
    let handler = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let exact_path = format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/collision");

    let catalog = run_generic(&handler, "catalog", json!({"path": exact_path})).unwrap();
    assert_eq!(catalog["catalog"]["sortable"], json!([]));
    assert_eq!(catalog["catalog"]["filterable"], json!([]));

    let stat = run_generic(&handler, "stat", json!({"path": exact_path})).unwrap();
    assert_eq!(stat["card"]["catalog"]["sortable"], json!([]));
    assert_eq!(stat["card"]["catalog"]["filterable"], json!([]));

    let directory = run_generic(
        &handler,
        "catalog",
        json!({"path": format!("{TEST_WORKBENCH_ROOT}/wb-main/outputs/only")}),
    )
    .unwrap();
    assert!(directory["catalog"]["sortable"]
        .as_array()
        .is_some_and(|fields| fields.contains(&json!("path"))));
    assert!(!directory["catalog"]["sortable"]
        .as_array()
        .unwrap()
        .contains(&json!("directory.child")));
}

#[test]
fn all_eighteen_tools_execute_against_typed_backend_primitives() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    let mut executed = BTreeSet::new();
    let mut success_transcript = BTreeMap::new();
    let mut call = |name: &str, arguments: Value| {
        let result = run(&handler, name, arguments).expect("tool call must succeed");
        assert_eq!(result["status"], "success", "{name}");
        executed.insert(name.to_owned());
        success_transcript.insert(name.to_owned(), result.clone());
        result
    };

    let created = call("workbench_create", json!({"id": "wb-main"}));
    assert_eq!(created["sections"].as_array().map(Vec::len), Some(5));

    call(
        "workbench_put_file",
        json!({
            "id": "wb-main", "section": "outputs", "path": "report.txt",
            "text": "Hello world\nSECOND\n", "content_type": "text/plain"
        }),
    );
    let appended = call(
        "workbench_append",
        json!({
            "id": "wb-main", "section": "outputs", "path": "report.txt",
            "text": "third\n"
        }),
    );
    assert_eq!(appended["digest"], sha256_uri(b"third\n"));
    call(
        "workbench_edit",
        json!({
            "id": "wb-main", "section": "outputs", "path": "report.txt",
            "old_string": "SECOND", "new_string": "second"
        }),
    );
    assert_eq!(
        call(
            "workbench_list",
            json!({
                "id": "wb-main", "section": "outputs", "at_snapshot": 7, "limit": 10
            }),
        )["entry_count"],
        1
    );
    assert_eq!(
        call(
            "workbench_stat",
            json!({
                "id": "wb-main", "section": "outputs", "path": "report.txt",
                "at_snapshot": "frozen"
            }),
        )["card"]["kind"],
        "file"
    );
    assert_eq!(
        call(
            "workbench_read",
            json!({
                "id": "wb-main", "section": "outputs", "path": "report.txt",
                "at_snapshot": 7, "format": "structured", "limit": 10
            }),
        )["record_count"],
        3
    );
    let grep = call(
        "workbench_grep",
        json!({
            "id": "wb-main", "section": "outputs", "pattern": "HELLO",
            "patterns": ["SECOND"], "glob": "*.txt", "recursive": true,
            "limit": 10
        }),
    );
    assert_eq!(grep["matches"].as_array().map(Vec::len), Some(2));

    let search = call(
        "workbench_search",
        json!({
            "id": "wb-main", "section": "outputs",
            "predicates": [
                {"field": "artifact.size_bytes", "op": "gte", "value": 1},
                {"field": "artifact.delta", "op": "lt", "value": -1},
                {"field": "artifact.producer", "op": "exists"}
            ],
            "fields": ["artifact.producer"],
            "sort": [{"field": "artifact.size_bytes", "direction": "desc"}],
            "facets": ["artifact.producer"], "cursor": "search-in", "limit": 10
        }),
    );
    assert_eq!(search["next_cursor"], "search-next");
    assert_eq!(search["truncated"], true);
    assert_eq!(search["facets"][0]["values"][0]["count"], 1);
    let aggregate = call(
        "workbench_aggregate",
        json!({
            "id": "wb-main", "section": "outputs",
            "group_by": ["artifact.producer"],
            "measures": [{"name": "artifacts", "op": "count"}],
            "sort": [{"field": "artifacts"}], "limit": 20
        }),
    );
    assert_eq!(aggregate["groups"][0]["values"]["artifacts"], 1);
    assert_eq!(aggregate["input_match_count"], 1);
    assert_eq!(
        call(
            "workbench_catalog",
            json!({"section": "outputs", "include_facets": true}),
        )["catalog"]["filterable"][0]["fields"][0],
        "artifact.producer"
    );
    assert_eq!(
        call(
            "workbench_find",
            json!({
                "committed": true, "manifest_pattern": "viking", "include_manifest": true,
                "cursor": "find-in"
            }),
        )["matches"][0]["workbench_id"],
        "wb-main"
    );

    let commit = call(
        "workbench_commit",
        json!({
            "id": "wb-main", "manifest": {"model": "viking", "steps": [2, 1]},
            "content_digest_uri": format!("sha256:{}", "01".repeat(32))
        }),
    );
    assert_eq!(commit["commit_identity"].as_str().map(str::len), Some(64));
    assert_eq!(
        commit["path"],
        "/agents/test/wb/wb-main/metadata/run_manifest.json"
    );

    let snapshot = call(
        "workbench_snapshot",
        json!({
            "id": "wb-main", "name": "before-edit", "ttl_days": 3,
            "reason": "test", "metadata": {"suite": "facade"}
        }),
    );
    assert_eq!(snapshot["snapshot_id"], 1);
    assert_eq!(snapshot["ttl_days"], 3);
    assert_eq!(
        call(
            "workbench_snapshot_renew",
            json!({"id": "wb-main", "name": "before-edit", "ttl_days": 5}),
        )["renewed"],
        true
    );
    assert_eq!(
        call(
            "workbench_snapshot_retire",
            json!({"id": "wb-main", "snapshot_id": 1, "reason": "done"}),
        )["retired"],
        true
    );
    assert_eq!(
        call("workbench_snapshot_list", json!({"id": "wb-main"}),)["snapshots"][0]["state"],
        "retired"
    );
    let restored = call(
        "workbench_restore",
        json!({"id": "wb-main", "at_snapshot": 1, "destination_id": "wb-restored"}),
    );
    assert_eq!(
        restored["restore_manifest"],
        "/agents/test/wb/wb-restored/metadata/restore_manifest.json"
    );

    let create_replay = run(&handler, "workbench_create", json!({"id": "wb-main"})).unwrap();
    let commit_replay = run(
        &handler,
        "workbench_commit",
        json!({
            "id": "wb-main", "manifest": {"model": "viking", "steps": [2, 1]},
            "content_digest_uri": format!("sha256:{}", "01".repeat(32))
        }),
    )
    .unwrap();

    let invalid_arguments = run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-main", "section": "outputs", "path": "invalid.txt",
            "text": "x", "base64": "eA=="
        }),
    )
    .unwrap_err()
    .as_value();
    backend.lock().commit_error = Some(BackendError::new(
        BackendErrorKind::Conflict,
        "stored run manifest names a different canonical commit",
        false,
        json!({"stored_generation": 8, "requested_replace": false}),
    ));
    let commit_conflict = run(
        &handler,
        "workbench_commit",
        json!({
            "id": "wb-main", "manifest": {"model": "other"},
            "content_digest_uri": format!("sha256:{}", "02".repeat(32))
        }),
    )
    .unwrap_err()
    .as_value();
    let snapshot_expired = run(
        &handler,
        "workbench_snapshot_renew",
        json!({"id": "wb-main", "snapshot_id": 1, "ttl_days": 1}),
    )
    .unwrap_err()
    .as_value();
    backend.lock().restore_error = Some(BackendError::new(
        BackendErrorKind::Other("BackendProtocolMismatch".to_owned()),
        "restore operation identity did not match the durable request",
        true,
        json!({"expected_snapshot_id": 1, "actual_snapshot_id": 2}),
    ));
    let restore_protocol_mismatch = run(
        &handler,
        "workbench_restore",
        json!({"id": "wb-main", "at_snapshot": 1, "destination_id": "wb-bad"}),
    )
    .unwrap_err()
    .as_value();
    let routing_backend = FakeBackend::default();
    routing_backend.lock().create_error = Some(BackendError::new(
        BackendErrorKind::Other("RoutingUnavailable".to_owned()),
        "owner is moving",
        true,
        json!({"retry_after_ms": 25}),
    ));
    let routing_unavailable = run(
        &test_handler(routing_backend),
        "workbench_create",
        json!({"id": "wb-routing"}),
    )
    .unwrap_err()
    .as_value();

    let transcript = json!({
        "schema": "nokv.workbench.result_transcript.v1",
        "logical_workbench_root": TEST_WORKBENCH_ROOT,
        "success": success_transcript,
        "replays": {
            "workbench_create": create_replay,
            "workbench_commit": commit_replay,
        },
        "errors": {
            "invalid_arguments": invalid_arguments,
            "routing_unavailable": routing_unavailable,
            "workbench_commit_conflict": commit_conflict,
            "snapshot_expired": snapshot_expired,
            "restore_protocol_mismatch": restore_protocol_mismatch,
        },
    });
    let expected: Value =
        serde_json::from_str(include_str!("golden/workbench_result_transcript.json"))
            .expect("checked-in Workbench result transcript must be valid JSON");
    assert_eq!(transcript, expected);

    assert_eq!(executed.len(), WORKBENCH_TOOL_COUNT);
    assert_eq!(
        executed,
        tool_definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<BTreeSet<_>>()
    );

    let state = backend.lock();
    assert_eq!(state.search_requests.len(), 1);
    assert_eq!(
        state.search_requests[0].predicates[0].operator,
        PredicateOperator::GreaterOrEqual
    );
    assert_eq!(
        state.search_requests[0].predicates[1].value,
        Some(QueryValue::Signed(-1))
    );
    assert_eq!(
        state.search_requests[0].cursor.as_deref(),
        Some("search-in")
    );
    assert_eq!(
        state.search_requests[0].sort[0].direction,
        SortDirection::Descending
    );
    assert_eq!(state.search_requests[0].profile, QueryProfile::ArtifactV1);
    assert_eq!(
        state.aggregate_requests[0].measures[0].operator,
        AggregateOperator::Count
    );
    assert_eq!(state.aggregate_requests[0].measures.len(), 1);
    assert_eq!(
        state.aggregate_requests[0].profile,
        QueryProfile::ArtifactV1
    );
    assert_eq!(state.catalog_requests.len(), 1);
    assert_eq!(state.catalog_requests[0].profile, QueryProfile::ArtifactV1);
    assert_eq!(state.find_requests.len(), 1);
    assert_eq!(state.find_requests[0].cursor.as_deref(), Some("find-in"));
    assert_eq!(state.commit_requests.len(), 2);
    assert_eq!(state.restore_requests.len(), 1);
    assert!(state
        .list_requests
        .iter()
        .any(|request| matches!(request.view, ReadView::Snapshot(SnapshotSelector::Id(7)))));
    assert!(state.stat_views.iter().any(|view| matches!(
        view,
        ReadView::Snapshot(SnapshotSelector::Name(name)) if name == "frozen"
    )));
    assert!(state
        .read_requests
        .iter()
        .any(|request| matches!(request.view, ReadView::Snapshot(SnapshotSelector::Id(7)))));
}

#[test]
fn logical_workbench_root_is_presentation_not_storage_identity() {
    let first_backend = FakeBackend::default();
    let second_backend = FakeBackend::default();
    let first = SdkWorkbenchToolHandler::new(first_backend.clone(), "/agents/one/wb").unwrap();
    let second = SdkWorkbenchToolHandler::new(second_backend.clone(), "/agents/two/wb/").unwrap();
    let arguments = json!({
        "id": "wb-root", "section": "logs", "path": "events.jsonl", "text": "{}\n"
    });

    let first_result = run(&first, "workbench_append", arguments.clone()).unwrap();
    let second_result = run(&second, "workbench_append", arguments).unwrap();

    assert_eq!(first.logical_root(), "/agents/one/wb");
    assert_eq!(second.logical_root(), "/agents/two/wb");
    assert_eq!(
        first_backend.lock().append_requests[0].path,
        second_backend.lock().append_requests[0].path
    );
    assert_eq!(
        first_result["path"],
        "/agents/one/wb/wb-root/logs/events.jsonl"
    );
    assert_eq!(
        second_result["path"],
        "/agents/two/wb/wb-root/logs/events.jsonl"
    );

    for invalid in ["relative", "/", "/agents/../wb", "/agents\\wb"] {
        let error = match SdkWorkbenchToolHandler::new(FakeBackend::default(), invalid) {
            Ok(_) => panic!("invalid presentation root {invalid:?} must fail"),
            Err(error) => error,
        };
        assert_eq!(error.code, "InvalidArguments");
    }
}

#[test]
fn generic_find_cursor_is_bound_to_storage_and_presentation_roots() {
    let first_backend = FakeBackend::with_root(1);
    first_backend.lock().search_two_pages = true;
    let first = SdkGenericAgentToolHandler::new(first_backend.clone(), "/agents/one/wb").unwrap();
    let page = run_generic(&first, "find", json!({"path": "/", "limit": 1})).unwrap();
    let cursor = page["next_cursor"]
        .as_str()
        .expect("the first page must return a cursor")
        .to_owned();
    assert_eq!(first_backend.lock().search_requests.len(), 1);

    let different_presentation =
        SdkGenericAgentToolHandler::new(first_backend.clone(), "/agents/two/wb").unwrap();
    let presentation_error = run_generic(
        &different_presentation,
        "find",
        json!({"path": "/", "cursor": cursor, "limit": 1}),
    )
    .unwrap_err();
    assert_eq!(presentation_error.code, "InvalidArguments");
    assert_eq!(first_backend.lock().search_requests.len(), 1);

    let second_backend = FakeBackend::with_root(2);
    let different_storage =
        SdkGenericAgentToolHandler::new(second_backend.clone(), "/agents/one/wb").unwrap();
    let storage_error = run_generic(
        &different_storage,
        "find",
        json!({"path": "/", "cursor": page["next_cursor"], "limit": 1}),
    )
    .unwrap_err();
    assert_eq!(storage_error.code, "InvalidArguments");
    assert!(second_backend.lock().search_requests.is_empty());
}

#[test]
fn empty_optional_scope_path_is_equivalent_to_omission() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    run(
        &handler,
        "workbench_create",
        json!({"id": "wb-empty-scope"}),
    )
    .unwrap();

    let scoped_cases = [
        (
            "workbench_list",
            json!({"id": "wb-empty-scope", "section": "outputs"}),
        ),
        (
            "workbench_stat",
            json!({"id": "wb-empty-scope", "section": "outputs"}),
        ),
        (
            "workbench_grep",
            json!({
                "id": "wb-empty-scope", "section": "outputs",
                "pattern": "needle", "recursive": true
            }),
        ),
        (
            "workbench_search",
            json!({"id": "wb-empty-scope", "section": "outputs"}),
        ),
        (
            "workbench_aggregate",
            json!({
                "id": "wb-empty-scope", "section": "outputs",
                "measures": [{"name": "artifacts", "op": "count"}]
            }),
        ),
        (
            "workbench_catalog",
            json!({"id": "wb-empty-scope", "section": "outputs"}),
        ),
    ];

    for (tool, omitted) in scoped_cases {
        let mut explicit_empty = omitted.clone();
        explicit_empty
            .as_object_mut()
            .unwrap()
            .insert("path".to_owned(), Value::String(String::new()));
        let omitted_result = run(&handler, tool, omitted).unwrap();
        let empty_result = run(&handler, tool, explicit_empty).unwrap();
        assert_eq!(empty_result, omitted_result, "{tool}");
    }

    let state = backend.lock();
    assert_eq!(state.list_requests[0].path, state.list_requests[1].path);
    assert_eq!(
        state.search_requests[0].scope,
        state.search_requests[1].scope
    );
    assert_eq!(
        state.aggregate_requests[0].scope,
        state.aggregate_requests[1].scope
    );
    assert_eq!(
        state.catalog_requests[0].scope,
        state.catalog_requests[1].scope
    );
}

#[test]
fn empty_required_artifact_paths_remain_invalid() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    run(
        &handler,
        "workbench_create",
        json!({"id": "wb-empty-required"}),
    )
    .unwrap();

    for (tool, arguments) in [
        (
            "workbench_put_file",
            json!({
                "id": "wb-empty-required", "section": "outputs", "path": "", "text": "x"
            }),
        ),
        (
            "workbench_append",
            json!({
                "id": "wb-empty-required", "section": "logs", "path": "", "text": "x"
            }),
        ),
        (
            "workbench_edit",
            json!({
                "id": "wb-empty-required", "section": "outputs", "path": "",
                "old_string": "x", "new_string": "y"
            }),
        ),
        (
            "workbench_read",
            json!({"id": "wb-empty-required", "section": "outputs", "path": ""}),
        ),
    ] {
        let error = run(&handler, tool, arguments)
            .expect_err("an empty required artifact path must fail closed");
        assert_eq!(error.code, "InvalidArguments", "{tool}");
    }

    assert_eq!(backend.publish_calls(), 0);
    assert_eq!(backend.append_calls(), 0);
}

#[test]
fn path_jail_and_put_modes_fail_closed() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    run(&handler, "workbench_create", json!({"id": "wb-jail"})).unwrap();

    for path in ["/secret", "../secret", "a\\secret"] {
        let error = run(
            &handler,
            "workbench_put_file",
            json!({
                "id": "wb-jail", "section": "outputs", "path": path, "text": "x"
            }),
        )
        .expect_err("escaped path must fail");
        assert_eq!(error.code, "InvalidArguments");
    }
    let duplicated = run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-jail", "section": "outputs", "path": "outputs/x", "text": "x"
        }),
    )
    .expect_err("duplicated section prefix must fail");
    assert_eq!(duplicated.code, "InvalidArguments");

    for manifest in ["run_manifest.json", "restore_manifest.json"] {
        for (tool, extra) in [
            ("workbench_put_file", json!({"text": "{}"})),
            ("workbench_append", json!({"text": "{}"})),
            (
                "workbench_edit",
                json!({"old_string": "old", "new_string": "new"}),
            ),
        ] {
            let mut arguments = json!({
                "id": "wb-jail", "section": "metadata", "path": manifest
            });
            arguments
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let error = run(&handler, tool, arguments)
                .expect_err("reserved manifest projections must reject generic writes");
            assert_eq!(error.code, "InvalidArguments");
        }
    }

    let payload_error = run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-jail", "section": "outputs", "path": "x", "text": "x", "base64": "eA=="
        }),
    )
    .expect_err("dual payload must fail");
    assert_eq!(payload_error.code, "InvalidArguments");

    let create = json!({
        "id": "wb-jail", "section": "outputs", "path": "x", "text": "one"
    });
    run(&handler, "workbench_put_file", create.clone()).unwrap();
    let exists = run(&handler, "workbench_put_file", create)
        .expect_err("create-only publication must not overwrite");
    assert_eq!(exists.code, "AlreadyExists");

    let missing = run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-jail", "section": "outputs", "path": "missing", "text": "x", "replace": true
        }),
    )
    .expect_err("replace-only publication must require an artifact");
    assert_eq!(missing.code, "NotFound");
    let replaced = run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-jail", "section": "outputs", "path": "x", "text": "two", "replace": true
        }),
    )
    .unwrap();
    assert_eq!(replaced["generation"], 2);
    assert_eq!(backend.body("wb-jail", "outputs/x"), b"two");
}

#[test]
fn append_is_one_backend_operation_and_edit_revalidates_generation_conflicts() {
    let backend = FakeBackend::default();
    let handler = test_handler_with_limits(backend.clone(), 1024, 2);
    run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt", "text": "alpha alpha"
        }),
    )
    .unwrap();

    let before = backend.append_calls();
    let append = run(
        &handler,
        "workbench_append",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt", "text": " beta"
        }),
    )
    .unwrap();
    assert_eq!(backend.append_calls() - before, 1);
    assert_eq!(
        backend
            .lock()
            .append_requests
            .last()
            .unwrap()
            .max_logical_size,
        None
    );
    assert_eq!(
        append,
        json!({
            "status": "success",
            "workbench_id": "wb-cas",
            "section": "logs",
            "relative_path": "events.txt",
            "path": "/agents/test/wb/wb-cas/logs/events.txt",
            "appended_bytes": 5,
            "size_bytes": 16,
            "generation": 2,
            "created": false,
            "digest": sha256_uri(b" beta"),
        })
    );
    assert_eq!(
        backend.body("wb-cas", "logs/events.txt"),
        b"alpha alpha beta"
    );

    backend.set_append_conflicts(1);
    let before = backend.append_calls();
    let conflict = run(
        &handler,
        "workbench_append",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt", "text": "!"
        }),
    )
    .expect_err("facade must surface the SDK append conflict without replaying the delta");
    assert_eq!(conflict.code, "Conflict");
    assert_eq!(backend.append_calls() - before, 1);
    assert_eq!(
        backend.body("wb-cas", "logs/events.txt"),
        b"alpha alpha beta"
    );

    let ambiguous = run(
        &handler,
        "workbench_edit",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt",
            "old_string": "alpha", "new_string": "A"
        }),
    )
    .expect_err("ambiguous edit must not guess");
    assert_eq!(ambiguous.code, "AmbiguousEdit");

    backend.set_publish_conflicts(1);
    let edit = run(
        &handler,
        "workbench_edit",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt",
            "old_string": "alpha", "new_string": "A", "replace_all": true
        }),
    )
    .unwrap();
    assert_eq!(edit["replacements"], 2);
    assert_eq!(backend.body("wb-cas", "logs/events.txt"), b"A A beta");

    let generation = backend.generation("wb-cas", "logs/events.txt");
    let calls = backend.publish_calls();
    let no_op = run(
        &handler,
        "workbench_edit",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt",
            "old_string": "A", "new_string": "A", "replace_all": true
        }),
    )
    .unwrap();
    assert_eq!(no_op["no_change"], true);
    assert_eq!(backend.publish_calls(), calls);
    assert_eq!(backend.generation("wb-cas", "logs/events.txt"), generation);

    backend.set_publish_conflicts(2);
    let conflict = run(
        &handler,
        "workbench_edit",
        json!({
            "id": "wb-cas", "section": "logs", "path": "events.txt",
            "old_string": "beta", "new_string": "B"
        }),
    )
    .expect_err("bounded retry must surface exhaustion");
    assert_eq!(conflict.code, "Conflict");
    assert!(conflict.retryable);
    assert_eq!(conflict.details["attempts"], 2);
}

#[test]
fn append_limits_the_delta_without_capping_the_existing_logical_artifact() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-large", "section": "logs", "path": "events.txt", "text": "x"
        }),
    )
    .unwrap();
    let existing_size = DEFAULT_WORKBENCH_MAX_BYTES as u64 + 1;
    backend
        .lock()
        .files
        .get_mut(&("wb-large".to_owned(), "logs/events.txt".to_owned()))
        .unwrap()
        .metadata
        .size_bytes = existing_size;

    let appended = run(
        &handler,
        "workbench_append",
        json!({
            "id": "wb-large", "section": "logs", "path": "events.txt", "text": "!"
        }),
    )
    .expect("a small delta may extend an artifact already larger than the payload limit");
    assert_eq!(appended["appended_bytes"], 1);
    assert_eq!(appended["size_bytes"], existing_size + 1);
    assert_eq!(backend.lock().append_requests[0].max_logical_size, None);
}

#[test]
fn structured_and_byte_reads_preserve_explicit_cursor_semantics() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend);
    run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-read", "section": "input", "path": "rows.json",
            "text": "[{\"n\":1},{\"n\":2}]", "content_type": "application/json"
        }),
    )
    .unwrap();
    let first = run(
        &handler,
        "workbench_read",
        json!({
            "id": "wb-read", "section": "input", "path": "rows.json",
            "format": "structured", "limit": 1
        }),
    )
    .unwrap();
    assert_eq!(first["items"], json!([{"index": 0, "value": {"n": 1}}]));
    assert_eq!(first["next_cursor"], "r:1");
    let second = run(
        &handler,
        "workbench_read",
        json!({
            "id": "wb-read", "section": "input", "path": "rows.json",
            "format": "structured", "cursor": "r:1", "limit": 1
        }),
    )
    .unwrap();
    assert_eq!(second["items"], json!([{"index": 1, "value": {"n": 2}}]));

    let bytes = run(
        &handler,
        "workbench_read",
        json!({
            "id": "wb-read", "section": "input", "path": "rows.json",
            "format": "bytes", "offset": 1, "limit": 3
        }),
    )
    .unwrap();
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(bytes["bytes"].as_str().unwrap())
            .unwrap(),
        b"{\"n"
    );
    let generation = bytes["generation"].as_u64().unwrap();
    let not_modified = run(
        &handler,
        "workbench_read",
        json!({
            "id": "wb-read", "section": "input", "path": "rows.json",
            "if_none_match": generation
        }),
    )
    .unwrap();
    assert_eq!(not_modified["not_modified"], true);
}

#[test]
fn grep_treats_patterns_as_case_insensitive_literals_and_globs_as_basenames() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend);
    run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-grep", "section": "outputs", "path": "nested/报告.txt",
            "text": "Alpha|Beta\nalpha beta\n"
        }),
    )
    .unwrap();
    run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-grep", "section": "outputs", "path": "nested/报告.log",
            "text": "Alpha|Beta\n"
        }),
    )
    .unwrap();

    let result = run(
        &handler,
        "workbench_grep",
        json!({
            "id": "wb-grep", "section": "outputs", "path": "nested",
            "pattern": "ALPHA|BETA", "patterns": ["not present"],
            "glob": "报?.txt", "recursive": true
        }),
    )
    .unwrap();
    assert_eq!(result["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        result["matches"][0]["path"],
        "/agents/test/wb/wb-grep/outputs/nested/报告.txt"
    );
    assert_eq!(result["matches"][0]["line_number"], 1);
}

#[test]
fn workbench_grep_limits_matching_lines_and_resumes_inside_one_artifact() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend);
    run(
        &handler,
        "workbench_put_file",
        json!({
            "id": "wb-grep-lines", "section": "logs", "path": "events.log",
            "text": "needle one\nneedle two\n", "content_type": "text/plain"
        }),
    )
    .unwrap();
    let arguments = json!({
        "id": "wb-grep-lines", "section": "logs", "path": "events.log",
        "pattern": "needle", "recursive": false, "limit": 1
    });

    let first = run(&handler, "workbench_grep", arguments.clone()).unwrap();
    assert_eq!(first["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(first["matches"][0]["line_number"], 1);
    assert_eq!(first["truncated"], true);

    let second = run(
        &handler,
        "workbench_grep",
        json!({
            "id": "wb-grep-lines", "section": "logs", "path": "events.log",
            "pattern": "needle", "recursive": false, "limit": 1,
            "cursor": first["next_cursor"]
        }),
    )
    .unwrap();
    assert_eq!(second["matches"].as_array().map(Vec::len), Some(1));
    assert_eq!(second["matches"][0]["line_number"], 2);
    assert_eq!(second["truncated"], false);
    assert_eq!(second["next_cursor"], Value::Null);
}

#[test]
fn workbench_grep_uses_lossy_text_skips_nul_and_caps_snippets_at_240_chars() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend);
    for (path, bytes) in [
        ("invalid.log", [&[0xff_u8][..], b"needle invalid"].concat()),
        ("nul.log", b"needle\0hidden".to_vec()),
        (
            "long.log",
            format!("needle {}", "x".repeat(300)).into_bytes(),
        ),
    ] {
        run(
            &handler,
            "workbench_put_file",
            json!({
                "id": "wb-grep-text", "section": "logs", "path": path,
                "base64": base64::engine::general_purpose::STANDARD.encode(bytes),
                "content_type": "application/octet-stream"
            }),
        )
        .unwrap();
    }

    let invalid = run(
        &handler,
        "workbench_grep",
        json!({
            "id": "wb-grep-text", "section": "logs", "path": "invalid.log",
            "pattern": "needle", "recursive": false
        }),
    )
    .unwrap();
    assert_eq!(invalid["matches"].as_array().map(Vec::len), Some(1));
    assert!(invalid["matches"][0]["snippet"]
        .as_str()
        .is_some_and(|snippet| snippet.contains('\u{fffd}')));

    let nul = run(
        &handler,
        "workbench_grep",
        json!({
            "id": "wb-grep-text", "section": "logs", "path": "nul.log",
            "pattern": "needle", "recursive": false
        }),
    )
    .unwrap();
    assert_eq!(nul["matches"], json!([]));

    let long = run(
        &handler,
        "workbench_grep",
        json!({
            "id": "wb-grep-text", "section": "logs", "path": "long.log",
            "pattern": "needle", "recursive": false
        }),
    )
    .unwrap();
    assert_eq!(
        long["matches"][0]["snippet"]
            .as_str()
            .map(|snippet| snippet.chars().count()),
        Some(240)
    );
}

#[test]
fn workbench_grep_pipe_compatibility_is_not_enabled_for_generic_grep() {
    let backend = FakeBackend::default();
    let workbench = test_handler(backend.clone());
    run(
        &workbench,
        "workbench_put_file",
        json!({
            "id": "wb-grep-pipe", "section": "logs", "path": "events.log",
            "text": "beta", "content_type": "text/plain"
        }),
    )
    .unwrap();

    let compatible = run(
        &workbench,
        "workbench_grep",
        json!({
            "id": "wb-grep-pipe", "section": "logs", "path": "events.log",
            "pattern": "alpha|beta", "recursive": false
        }),
    )
    .unwrap();
    assert_eq!(compatible["matches"].as_array().map(Vec::len), Some(1));

    let empty_segments_are_ignored = run(
        &workbench,
        "workbench_grep",
        json!({
            "id": "wb-grep-pipe", "section": "logs", "path": "events.log",
            "pattern": "alpha||beta", "recursive": false
        }),
    )
    .expect("empty compatibility segments must be ignored");
    assert_eq!(
        empty_segments_are_ignored["matches"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    let all_empty_segments_are_literal = run(
        &workbench,
        "workbench_grep",
        json!({
            "id": "wb-grep-pipe", "section": "logs", "path": "events.log",
            "pattern": "|||", "recursive": false
        }),
    )
    .expect("an all-empty compatibility split must fall back to the literal primary");
    assert_eq!(all_empty_segments_are_literal["matches"], json!([]));

    let seventeen = (0..17)
        .map(|index| format!("pattern-{index}"))
        .collect::<Vec<_>>()
        .join("|");
    let too_many = run(
        &workbench,
        "workbench_grep",
        json!({
            "id": "wb-grep-pipe", "section": "logs", "path": "events.log",
            "pattern": seventeen, "recursive": false
        }),
    )
    .expect_err("the compatibility form must retain the sixteen-pattern bound");
    assert_eq!(too_many.code, "InvalidArguments");

    let generic = SdkGenericAgentToolHandler::new(backend, TEST_WORKBENCH_ROOT).unwrap();
    let literal = run_generic(
        &generic,
        "grep",
        json!({
            "path": format!("{TEST_WORKBENCH_ROOT}/wb-grep-pipe/logs/events.log"),
            "pattern": "alpha|beta", "recursive": false
        }),
    )
    .unwrap();
    assert_eq!(literal["matches"], json!([]));

    let empty_glob = run(
        &workbench,
        "workbench_grep",
        json!({
            "id": "wb-grep-pipe", "section": "logs", "path": "events.log",
            "pattern": "beta", "glob": "", "recursive": false
        }),
    )
    .expect_err("an empty basename glob is not a meaningful filter");
    assert_eq!(empty_glob.code, "InvalidArguments");
}

#[test]
fn grep_cursor_commitment_is_derived_from_patterns_and_glob() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    for arguments in [
        json!({"id": "wb-grep", "pattern": "alpha", "glob": "*.txt", "recursive": true}),
        json!({"id": "wb-grep", "pattern": "beta", "glob": "*.txt", "recursive": true}),
        json!({"id": "wb-grep", "pattern": "alpha", "glob": "*.log", "recursive": true}),
    ] {
        run(&handler, "workbench_grep", arguments).unwrap();
    }

    let state = backend.lock();
    let commitments = state
        .grep_requests
        .iter()
        .map(|request| request.query_commitment)
        .collect::<Vec<_>>();
    assert_eq!(commitments.len(), 3);
    assert_ne!(commitments[0], commitments[1]);
    assert_ne!(commitments[0], commitments[2]);
}

#[test]
fn commit_identity_uses_recursively_canonical_manifest_json() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend.clone());
    let mut first_manifest = serde_json::Map::new();
    first_manifest.insert("z".to_owned(), json!({"b": 2, "a": 1}));
    first_manifest.insert("a".to_owned(), json!([2, 1]));
    let mut second_manifest = serde_json::Map::new();
    second_manifest.insert("a".to_owned(), json!([2, 1]));
    second_manifest.insert("z".to_owned(), json!({"a": 1, "b": 2}));
    let content_digest_uri = format!("sha256:{}", "cd".repeat(32));

    let first = run(
        &handler,
        "workbench_commit",
        json!({
            "id": "wb-commit", "manifest": Value::Object(first_manifest),
            "content_digest_uri": content_digest_uri
        }),
    )
    .unwrap();
    let second = run(
        &handler,
        "workbench_commit",
        json!({
            "id": "wb-commit", "manifest": Value::Object(second_manifest),
            "content_digest_uri": format!("sha256:{}", "cd".repeat(32)), "replace": true
        }),
    )
    .unwrap();
    assert_eq!(first["manifest_digest_uri"], second["manifest_digest_uri"]);
    assert_eq!(first["commit_identity"], second["commit_identity"]);
    assert_eq!(second["idempotent_replay"], true);
    let state = backend.lock();
    assert_eq!(
        state.commit_requests[0].canonical_manifest,
        br#"{"a":[2,1],"z":{"a":1,"b":2}}"#
    );
    assert_eq!(
        first["manifest_digest_uri"],
        sha256_uri(&state.commit_requests[0].canonical_manifest)
    );
}

#[test]
fn commit_request_is_clock_free_across_handler_reconstruction() {
    let backend = FakeBackend::default();
    let arguments = json!({
        "id": "wb-clock-free",
        "manifest": {"model": "viking", "steps": [1, 2]},
        "content_digest_uri": format!("sha256:{}", "ef".repeat(32)),
    });

    run(
        &test_handler(backend.clone()),
        "workbench_commit",
        arguments.clone(),
    )
    .unwrap();
    run(
        &test_handler(backend.clone()),
        "workbench_commit",
        arguments,
    )
    .unwrap();

    let state = backend.lock();
    assert_eq!(state.commit_requests.len(), 2);
    assert_eq!(state.commit_requests[0], state.commit_requests[1]);
}

#[test]
fn snapshots_shape_annotations_and_restore_without_internal_roots() {
    let backend = FakeBackend::default();
    let handler = test_handler(backend);
    run(&handler, "workbench_create", json!({"id": "wb-source"})).unwrap();
    let minted = run(
        &handler,
        "workbench_snapshot",
        json!({
            "id": "wb-source", "name": "checkpoint", "reason": "before run",
            "metadata": {"model": "viking"}
        }),
    )
    .unwrap();
    assert_eq!(minted["annotation"]["reason"], "before run");
    assert_eq!(minted["ttl_days"], DEFAULT_SNAPSHOT_TTL_DAYS);

    let neither = run(
        &handler,
        "workbench_snapshot_renew",
        json!({"id": "wb-source"}),
    )
    .expect_err("renew must identify exactly one snapshot");
    assert_eq!(neither.code, "InvalidArguments");
    let both = run(
        &handler,
        "workbench_snapshot_renew",
        json!({"id": "wb-source", "snapshot_id": 1, "name": "checkpoint"}),
    )
    .expect_err("renew must reject ambiguous selectors");
    assert_eq!(both.code, "InvalidArguments");

    let restored = run(
        &handler,
        "workbench_restore",
        json!({"id": "wb-source", "at_snapshot": "checkpoint", "destination_id": "wb-fork"}),
    )
    .unwrap();
    assert!(restored.get("source_root").is_none());
    assert!(restored.get("destination_root").is_none());
    assert!(restored.get("inode").is_none());
    let replay = run(
        &handler,
        "workbench_restore",
        json!({
            "id": "wb-source", "at_snapshot": "checkpoint", "destination_id": "wb-fork"
        }),
    )
    .unwrap();
    assert_eq!(replay["operation_id"], restored["operation_id"]);
    assert_eq!(replay["idempotent_replay"], true);
    let same = run(
        &handler,
        "workbench_restore",
        json!({"id": "wb-source", "at_snapshot": 1, "destination_id": "wb-source"}),
    )
    .expect_err("restore cannot mutate its source in place");
    assert_eq!(same.code, "InvalidArguments");

    let retired = run(
        &handler,
        "workbench_snapshot_retire",
        json!({"id": "wb-source", "snapshot_id": 1, "reason": "done"}),
    )
    .unwrap();
    assert_eq!(
        retired["retire_annotation"],
        json!({"reason": "done", "metadata": Value::Null})
    );
    let replay = run(
        &handler,
        "workbench_snapshot_retire",
        json!({"id": "wb-source", "snapshot_id": 1, "reason": "changed"}),
    )
    .unwrap();
    assert_eq!(replay["retired"], false);
    assert_eq!(replay["retire_annotation"], retired["retire_annotation"]);
}

#[test]
fn backend_errors_keep_typed_code_retryability_and_details() {
    let backend = FakeBackend::default();
    backend.lock().create_error = Some(BackendError::new(
        BackendErrorKind::Other("RoutingUnavailable".to_owned()),
        "owner is moving",
        true,
        json!({"retry_after_ms": 25}),
    ));
    let handler = test_handler(backend);
    let error = run(&handler, "workbench_create", json!({"id": "wb-error"}))
        .expect_err("backend error must surface");
    assert_eq!(error.code, "RoutingUnavailable");
    assert_eq!(error.message, "owner is moving");
    assert!(error.retryable);
    assert_eq!(error.details["retry_after_ms"], 25);
}
