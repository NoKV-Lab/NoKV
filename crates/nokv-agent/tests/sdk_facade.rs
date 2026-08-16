/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use base64::Engine;
use nokv_agent::*;
use nokv_types::{NormalizedRelativePath, WorkbenchId};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const TEST_WORKBENCH_ROOT: &str = "/agents/test/wb";

#[derive(Clone, Default)]
struct FakeBackend {
    state: Arc<Mutex<FakeState>>,
}

#[derive(Default)]
struct FakeState {
    workbenches: BTreeSet<String>,
    files: BTreeMap<(String, String), ArtifactBody>,
    publish_calls: usize,
    publish_conflicts: usize,
    append_calls: usize,
    append_conflicts: usize,
    append_requests: Vec<AppendRequest>,
    create_error: Option<BackendError>,
    stat_views: Vec<ReadView>,
    list_requests: Vec<ListRequest>,
    grep_requests: Vec<GrepCandidateRequest>,
    read_requests: Vec<ReadRequest>,
    search_requests: Vec<SearchRequest>,
    aggregate_requests: Vec<AggregateRequest>,
    catalog_requests: Vec<CatalogRequest>,
    find_requests: Vec<FindRequest>,
    commit_requests: Vec<CommitRequest>,
    commit_error: Option<BackendError>,
    snapshots: BTreeMap<(String, u64), SnapshotRecord>,
    next_snapshot_id: u64,
    restore_requests: Vec<RestoreRequest>,
    restore_error: Option<BackendError>,
}

impl FakeBackend {
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
}

impl WorkbenchBackend for FakeBackend {
    fn create_workbench(&self, workbench_id: &WorkbenchId) -> Result<bool, BackendError> {
        let mut state = self.lock();
        if let Some(error) = state.create_error.take() {
            return Err(error);
        }
        Ok(state.workbenches.insert(workbench_id.as_str().to_owned()))
    }

    fn stat(&self, path: &ScopedPath, view: &ReadView) -> Result<Option<StatRecord>, BackendError> {
        let mut state = self.lock();
        state.stat_views.push(view.clone());
        let key = path_key(path);
        if let Some(body) = state.files.get(&key) {
            return Ok(Some(StatRecord {
                path: path.clone(),
                kind: ArtifactKind::Artifact,
                artifact: Some(body.metadata.clone()),
            }));
        }
        if path.relative_path.is_none()
            && (path.section.is_some() || state.workbenches.contains(path.workbench_id.as_str()))
        {
            return Ok(Some(StatRecord {
                path: path.clone(),
                kind: if path.section.is_some() {
                    ArtifactKind::Section
                } else {
                    ArtifactKind::Workbench
                },
                artifact: None,
            }));
        }
        Ok(None)
    }

    fn list(&self, request: ListRequest) -> Result<ListPage, BackendError> {
        let mut state = self.lock();
        state.list_requests.push(request.clone());
        let prefix = request.path.logical_path();
        let mut entries = state
            .files
            .values()
            .filter(|body| {
                body.path.workbench_id == request.path.workbench_id
                    && (prefix.is_empty()
                        || body.path.logical_path() == prefix
                        || body.path.logical_path().starts_with(&format!("{prefix}/")))
            })
            .map(|body| ListEntry {
                path: body.path.clone(),
                kind: ArtifactKind::Artifact,
                artifact: Some(body.metadata.clone()),
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.path.logical_path());
        let start = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| backend_error(BackendErrorKind::InvalidState, "bad list cursor"))?;
        let end = start.saturating_add(request.limit).min(entries.len());
        let next_cursor = (end < entries.len()).then(|| end.to_string());
        Ok(ListPage {
            entries: entries[start..end].to_vec(),
            next_cursor,
            read_version: 41,
        })
    }

    fn read(&self, request: ReadRequest) -> Result<Option<ArtifactBody>, BackendError> {
        let mut state = self.lock();
        state.read_requests.push(request.clone());
        Ok(state.files.get(&path_key(&request.path)).cloned())
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
        let prefix = request.scope.logical_path();
        let mut paths = state
            .files
            .values()
            .filter(|body| {
                body.path.workbench_id == request.scope.workbench_id
                    && (prefix.is_empty()
                        || body.path.logical_path() == prefix
                        || body.path.logical_path().starts_with(&format!("{prefix}/")))
            })
            .map(|body| body.path.clone())
            .collect::<Vec<_>>();
        paths.sort_by_key(ScopedPath::logical_path);
        let start = request
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| backend_error(BackendErrorKind::InvalidState, "bad grep cursor"))?;
        let end = start.saturating_add(request.limit).min(paths.len());
        Ok(GrepCandidatePage {
            candidates: paths[start..end]
                .iter()
                .enumerate()
                .map(|(index, path)| GrepCandidate {
                    path: path.clone(),
                    cursor_after: (start + index + 1).to_string(),
                })
                .collect(),
            next_cursor: (end < paths.len()).then(|| end.to_string()),
        })
    }

    fn search(&self, request: SearchRequest) -> Result<SearchPage, BackendError> {
        self.lock().search_requests.push(request);
        Ok(SearchPage {
            hits: vec![SearchHit {
                workbench_id: workbench_id("wb-main"),
                path: relative_path("outputs/report.txt"),
                metadata: artifact_metadata(b"query", 7, "text/plain"),
                projection: BTreeMap::from([(
                    "artifact.producer".to_owned(),
                    QueryValue::String("agent".to_owned()),
                )]),
            }],
            facets: vec![FacetResult {
                field: "artifact.producer".to_owned(),
                buckets: vec![FacetBucket {
                    value: QueryValue::String("agent".to_owned()),
                    count: 1,
                }],
                distinct_count: 1,
                truncated: false,
            }],
            next_cursor: Some("search-next".to_owned()),
            read_version: 42,
        })
    }

    fn aggregate(&self, request: AggregateRequest) -> Result<AggregatePage, BackendError> {
        let measures = request
            .measures
            .iter()
            .map(|measure| (measure.name.clone(), QueryValue::Unsigned(1)))
            .collect();
        self.lock().aggregate_requests.push(request);
        Ok(AggregatePage {
            rows: vec![AggregateRow {
                groups: BTreeMap::from([(
                    "artifact.producer".to_owned(),
                    QueryValue::String("agent".to_owned()),
                )]),
                measures,
            }],
            read_version: 43,
        })
    }

    fn catalog(&self, request: CatalogRequest) -> Result<CatalogResult, BackendError> {
        self.lock().catalog_requests.push(request);
        Ok(CatalogResult {
            fields: vec![CatalogField {
                field: "artifact.producer".to_owned(),
                scalar_type: "string".to_owned(),
                operators: vec!["eq".to_owned(), "prefix".to_owned()],
                sortable: true,
                facetable: true,
                aggregatable: false,
            }],
            read_version: 44,
        })
    }

    fn find_workbenches(&self, request: FindRequest) -> Result<FindPage, BackendError> {
        self.lock().find_requests.push(request);
        Ok(FindPage {
            workbenches: vec![fake_committed_summary()],
            entry_count: 1,
            next_cursor: Some("find-next".to_owned()),
            read_version: 45,
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
        manifest_metadata: Some(artifact_metadata(&envelope_bytes, 3, "application/json")),
        manifest: Some(envelope),
    }
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
    assert_eq!(
        state.aggregate_requests[0].measures[0].operator,
        AggregateOperator::Count
    );
    assert_eq!(
        state.aggregate_requests[0].measures[1].name,
        "__nokv_workbench_input_match_count"
    );
    assert_eq!(state.catalog_requests.len(), 1);
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
