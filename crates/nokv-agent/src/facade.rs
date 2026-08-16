/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Storage-neutral Workbench SDK boundary and concrete 18-tool handler.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::{
    canonical_json_bytes, projection::digest_uri, projection::hash_length_prefixed,
    projection::lowercase_hex, verify_run_manifest_v1, workbench_commit_identity, AgentError,
    WorkbenchToolHandler,
};
use base64::Engine;
use nokv_types::{NormalizedRelativePath, WorkbenchId};
use serde_json::{json, Map, Number, Value};
use sha2::{Digest, Sha256};

pub const WORKBENCH_SECTIONS: [Section; 5] = [
    Section::Input,
    Section::Scripts,
    Section::Outputs,
    Section::Logs,
    Section::Metadata,
];
pub const DEFAULT_WORKBENCH_MAX_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_EDIT_ATTEMPTS: usize = 5;
pub const DEFAULT_SNAPSHOT_TTL_DAYS: u64 = 7;
pub const MAX_GREP_PATTERNS: usize = 16;

const MILLIS_PER_DAY: u64 = 86_400_000;
const RUN_MANIFEST_PATH: &str = "run_manifest.json";
const RESTORE_MANIFEST_PATH: &str = "restore_manifest.json";

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Section {
    Input,
    Scripts,
    Outputs,
    Logs,
    Metadata,
}

impl Section {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Scripts => "scripts",
            Self::Outputs => "outputs",
            Self::Logs => "logs",
            Self::Metadata => "metadata",
        }
    }

    fn parse(value: &str) -> Result<Self, AgentError> {
        match value {
            "input" => Ok(Self::Input),
            "scripts" => Ok(Self::Scripts),
            "outputs" => Ok(Self::Outputs),
            "logs" => Ok(Self::Logs),
            "metadata" => Ok(Self::Metadata),
            _ => Err(AgentError::invalid_arguments(format!(
                "unknown Workbench section {value}"
            ))),
        }
    }
}

impl fmt::Display for Section {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScopedPath {
    pub workbench_id: WorkbenchId,
    pub section: Option<Section>,
    pub relative_path: Option<NormalizedRelativePath>,
}

impl ScopedPath {
    pub fn logical_path(&self) -> String {
        match (self.section, &self.relative_path) {
            (Some(section), Some(path)) => format!("{section}/{path}"),
            (Some(section), None) => section.to_string(),
            (None, Some(path)) => path.to_string(),
            (None, None) => String::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotSelector {
    Id(u64),
    Name(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadView {
    Live,
    Snapshot(SnapshotSelector),
}

#[derive(Clone, Debug, PartialEq)]
pub enum QueryValue {
    Null,
    Boolean(bool),
    Unsigned(u64),
    Signed(i64),
    Float(f64),
    String(String),
    List(Vec<QueryValue>),
}

impl QueryValue {
    pub fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Boolean(value) => Value::Bool(*value),
            Self::Unsigned(value) => Value::Number(Number::from(*value)),
            Self::Signed(value) => Value::Number(Number::from(*value)),
            Self::Float(value) => Number::from_f64(*value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            Self::String(value) => Value::String(value.clone()),
            Self::List(values) => Value::Array(values.iter().map(Self::to_json).collect()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateOperator {
    Equal,
    NotEqual,
    In,
    Prefix,
    Suffix,
    Contains,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    Exists,
    NotExists,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryPredicate {
    pub field: String,
    pub operator: PredicateOperator,
    pub value: Option<QueryValue>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuerySort {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggregateOperator {
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateMeasure {
    pub name: String,
    pub operator: AggregateOperator,
    pub field: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArtifactKind {
    Workbench,
    Section,
    Directory,
    Artifact,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArtifactMetadata {
    pub generation: u64,
    pub size_bytes: u64,
    pub digest_uri: String,
    pub content_type: String,
    pub producer: Option<String>,
    pub manifest_id: Option<String>,
    pub indexed_fields: BTreeMap<String, QueryValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StatRecord {
    pub path: ScopedPath,
    pub kind: ArtifactKind,
    pub artifact: Option<ArtifactMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ArtifactBody {
    pub path: ScopedPath,
    pub metadata: ArtifactMetadata,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListEntry {
    pub path: ScopedPath,
    pub kind: ArtifactKind,
    pub artifact: Option<ArtifactMetadata>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListRequest {
    pub path: ScopedPath,
    pub view: ReadView,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    pub next_cursor: Option<String>,
    pub read_version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ReadRequest {
    pub path: ScopedPath,
    pub view: ReadView,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublishCondition {
    CreateOnly,
    ReplaceOnly { expected_generation: u64 },
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishRequest {
    pub path: ScopedPath,
    pub body: Vec<u8>,
    pub content_type: String,
    pub condition: PublishCondition,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublishOutcome {
    pub metadata: ArtifactMetadata,
    pub created: bool,
}

/// One logical immutable-segment append delegated to the SDK boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct AppendRequest {
    pub path: ScopedPath,
    pub delta: Vec<u8>,
    /// Explicit override for an existing artifact. `None` inherits its type.
    pub content_type: Option<String>,
    /// Payload-derived type used only when the path is created.
    pub create_content_type: String,
    /// Optional lower-layer policy; the Workbench tool leaves this unset and
    /// limits only the appended payload.
    pub max_logical_size: Option<u64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendOutcome {
    pub metadata: ArtifactMetadata,
    pub created: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GrepCandidateRequest {
    pub scope: ScopedPath,
    pub recursive: bool,
    /// Facade-owned commitment to the exact pattern and basename-glob semantics.
    pub query_commitment: [u8; 32],
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrepCandidate {
    pub path: ScopedPath,
    pub cursor_after: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrepCandidatePage {
    pub candidates: Vec<GrepCandidate>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct QueryScope {
    pub workbench_id: Option<WorkbenchId>,
    pub section: Option<Section>,
    pub path: Option<NormalizedRelativePath>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchRequest {
    pub scope: QueryScope,
    pub predicates: Vec<QueryPredicate>,
    pub fields: Vec<String>,
    pub sort: Vec<QuerySort>,
    pub facets: Vec<String>,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchHit {
    pub workbench_id: WorkbenchId,
    pub path: NormalizedRelativePath,
    pub metadata: ArtifactMetadata,
    pub projection: BTreeMap<String, QueryValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FacetBucket {
    pub value: QueryValue,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FacetResult {
    pub field: String,
    pub buckets: Vec<FacetBucket>,
    pub distinct_count: u64,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    pub facets: Vec<FacetResult>,
    pub next_cursor: Option<String>,
    pub read_version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateRequest {
    pub scope: QueryScope,
    pub predicates: Vec<QueryPredicate>,
    pub group_by: Vec<String>,
    pub measures: Vec<AggregateMeasure>,
    pub sort: Vec<QuerySort>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateRow {
    pub groups: BTreeMap<String, QueryValue>,
    pub measures: BTreeMap<String, QueryValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregatePage {
    pub rows: Vec<AggregateRow>,
    pub read_version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogRequest {
    pub scope: QueryScope,
    pub field_prefix: Option<String>,
    pub include_facets: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogField {
    pub field: String,
    pub scalar_type: String,
    pub operators: Vec<String>,
    pub sortable: bool,
    pub facetable: bool,
    pub aggregatable: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogResult {
    pub fields: Vec<CatalogField>,
    pub read_version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FindRequest {
    pub committed: Option<bool>,
    pub manifest_pattern: Option<String>,
    pub include_manifest: bool,
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WorkbenchSummary {
    pub workbench_id: WorkbenchId,
    pub committed: bool,
    pub commit_id: Option<[u8; 32]>,
    pub manifest_metadata: Option<ArtifactMetadata>,
    pub manifest: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FindPage {
    pub workbenches: Vec<WorkbenchSummary>,
    pub entry_count: usize,
    pub next_cursor: Option<String>,
    pub read_version: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommitRequest {
    pub workbench_id: WorkbenchId,
    /// Recursively canonical caller manifest. The backend combines this with
    /// the first durable commit preparation; the Agent facade deliberately
    /// carries no process-local timestamp or prebuilt envelope.
    pub canonical_manifest: Vec<u8>,
    pub workbench_path: String,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    pub stable_commit_id: [u8; 32],
    pub replace: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CommitOutcome {
    pub commit_id: [u8; 32],
    pub generation: u64,
    pub manifest_size_bytes: u64,
    pub envelope_digest_uri: String,
    pub tree_digest_uri: String,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotMintRequest {
    pub workbench_id: WorkbenchId,
    pub name: Option<String>,
    pub lease_millis: u64,
    pub annotation: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRenewRequest {
    pub workbench_id: WorkbenchId,
    pub selector: SnapshotSelector,
    pub lease_millis: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRetireRequest {
    pub workbench_id: WorkbenchId,
    pub selector: SnapshotSelector,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotLifecycleState {
    Alive,
    Expired,
    Retired,
    Reaped,
}

impl SnapshotLifecycleState {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Alive => "alive",
            Self::Expired => "expired",
            Self::Retired => "retired",
            Self::Reaped => "reaped",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRecord {
    pub snapshot_id: u64,
    pub name: Option<String>,
    pub read_version: u64,
    pub lease_expires_unix_ms: Option<u64>,
    pub annotation: Value,
    pub retire_annotation: Option<Value>,
    pub state: SnapshotLifecycleState,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRetireOutcome {
    pub snapshot_id: u64,
    pub name: Option<String>,
    pub retired: bool,
    pub state: SnapshotLifecycleState,
    pub retire_annotation: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RestoreRequest {
    pub source_workbench_id: WorkbenchId,
    pub source_workbench_path: String,
    pub selector: SnapshotSelector,
    pub destination_workbench_id: WorkbenchId,
    pub destination_workbench_path: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RestoreOutcome {
    pub operation_id: [u8; 16],
    pub snapshot_id: u64,
    pub read_version: u64,
    pub destination_generation: u64,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendErrorKind {
    NotFound,
    AlreadyExists,
    Conflict,
    SnapshotNotFound,
    SnapshotExpired,
    ForkRetentionActive,
    InvalidState,
    Other(String),
}

impl BackendErrorKind {
    fn code(&self) -> &str {
        match self {
            Self::NotFound => "NotFound",
            Self::AlreadyExists => "AlreadyExists",
            Self::Conflict => "Conflict",
            Self::SnapshotNotFound => "SnapshotNotFound",
            Self::SnapshotExpired => "SnapshotExpired",
            Self::ForkRetentionActive => "ForkRetentionActive",
            Self::InvalidState => "InvalidState",
            Self::Other(code) => code,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BackendError {
    pub kind: BackendErrorKind,
    pub message: String,
    pub retryable: bool,
    pub details: Value,
}

impl BackendError {
    pub fn new(
        kind: BackendErrorKind,
        message: impl Into<String>,
        retryable: bool,
        details: Value,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable,
            details,
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(BackendErrorKind::Conflict, message, true, json!({}))
    }

    fn is_conflict(&self) -> bool {
        self.kind == BackendErrorKind::Conflict
    }
}

impl fmt::Display for BackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for BackendError {}

impl From<BackendError> for AgentError {
    fn from(error: BackendError) -> Self {
        AgentError::backend(
            error.kind.code(),
            error.message,
            error.retryable,
            error.details,
        )
    }
}

/// Storage-neutral primitives implemented by the real NoKV SDK boundary.
///
/// This trait deliberately exposes complete operations rather than metadata
/// keys, object-provider calls, routing, or publication state-machine steps.
pub trait WorkbenchBackend: Send + Sync {
    fn create_workbench(&self, workbench_id: &WorkbenchId) -> Result<bool, BackendError>;
    fn stat(&self, path: &ScopedPath, view: &ReadView) -> Result<Option<StatRecord>, BackendError>;
    fn list(&self, request: ListRequest) -> Result<ListPage, BackendError>;
    fn read(&self, request: ReadRequest) -> Result<Option<ArtifactBody>, BackendError>;
    fn publish(&self, request: PublishRequest) -> Result<PublishOutcome, BackendError>;
    fn append(&self, request: AppendRequest) -> Result<AppendOutcome, BackendError>;
    fn grep_candidates(
        &self,
        request: GrepCandidateRequest,
    ) -> Result<GrepCandidatePage, BackendError>;
    fn search(&self, request: SearchRequest) -> Result<SearchPage, BackendError>;
    fn aggregate(&self, request: AggregateRequest) -> Result<AggregatePage, BackendError>;
    fn catalog(&self, request: CatalogRequest) -> Result<CatalogResult, BackendError>;
    fn find_workbenches(&self, request: FindRequest) -> Result<FindPage, BackendError>;
    fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, BackendError>;
    fn mint_snapshot(&self, request: SnapshotMintRequest) -> Result<SnapshotRecord, BackendError>;
    fn renew_snapshot(&self, request: SnapshotRenewRequest)
        -> Result<SnapshotRecord, BackendError>;
    fn retire_snapshot(
        &self,
        request: SnapshotRetireRequest,
    ) -> Result<SnapshotRetireOutcome, BackendError>;
    fn list_snapshots(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Vec<SnapshotRecord>, BackendError>;
    fn restore(&self, request: RestoreRequest) -> Result<RestoreOutcome, BackendError>;
}

#[derive(Clone)]
pub struct SdkWorkbenchToolHandler<B> {
    backend: B,
    max_bytes: usize,
    edit_conflict_attempts: usize,
    logical_root: String,
}

impl<B> SdkWorkbenchToolHandler<B> {
    pub fn new(backend: B, logical_root: &str) -> Result<Self, AgentError> {
        Ok(Self {
            backend,
            max_bytes: DEFAULT_WORKBENCH_MAX_BYTES,
            edit_conflict_attempts: DEFAULT_EDIT_ATTEMPTS,
            logical_root: normalize_logical_workbench_root(logical_root)?,
        })
    }

    pub fn with_limits(
        backend: B,
        max_bytes: usize,
        edit_conflict_attempts: usize,
        logical_root: &str,
    ) -> Result<Self, AgentError> {
        Ok(Self {
            backend,
            max_bytes,
            edit_conflict_attempts: edit_conflict_attempts.max(1),
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
        let workbench = self.workbench_path(&path.workbench_id);
        let relative = path.logical_path();
        if relative.is_empty() {
            workbench
        } else {
            format!("{workbench}/{relative}")
        }
    }

    fn query_path(&self, scope: &QueryScope) -> String {
        let Some(workbench_id) = &scope.workbench_id else {
            return self.logical_root.clone();
        };
        let scoped = ScopedPath {
            workbench_id: workbench_id.clone(),
            section: scope.section,
            relative_path: scope.path.clone(),
        };
        self.projected_path(&scoped)
    }

    fn projected_name(&self, path: &ScopedPath) -> String {
        path.relative_path
            .as_ref()
            .and_then(|path| path.components().last())
            .map(str::to_owned)
            .or_else(|| path.section.map(|section| section.as_str().to_owned()))
            .unwrap_or_else(|| path.workbench_id.as_str().to_owned())
    }

    fn stat_value(&self, record: &StatRecord) -> Value {
        let artifact = record.artifact.as_ref();
        json!({
            "name": self.projected_name(&record.path),
            "path": self.projected_path(&record.path),
            "section": record.path.section.map(Section::as_str),
            "relative_path": relative_path_value(&record.path),
            "kind": projected_kind_name(&record.kind),
            "size_bytes": artifact.map(|artifact| artifact.size_bytes),
            "entry_count": Value::Null,
            "record_count": Value::Null,
            "generation": artifact.map(|artifact| artifact.generation),
            "content_type": artifact.map(|artifact| artifact.content_type.clone()),
            "digest_uri": artifact.map(|artifact| artifact.digest_uri.clone()),
            "producer": artifact.and_then(|artifact| artifact.producer.clone()),
            "manifest_id": artifact.and_then(|artifact| artifact.manifest_id.clone()),
        })
    }

    fn list_entry_value(&self, record: &ListEntry) -> Value {
        let artifact = record.artifact.as_ref();
        json!({
            "name": self.projected_name(&record.path),
            "path": self.projected_path(&record.path),
            "section": record.path.section.map(Section::as_str),
            "relative_path": relative_path_value(&record.path),
            "kind": projected_kind_name(&record.kind),
            "size_bytes": artifact.map(|artifact| artifact.size_bytes),
            "entry_count": Value::Null,
        })
    }

    fn workbench_summary_value(&self, summary: &WorkbenchSummary, include_manifest: bool) -> Value {
        let verified = verified_commit_projection(summary);
        let metadata = summary.manifest_metadata.as_ref();
        let manifest_path = metadata.map(|_| {
            let path = ScopedPath {
                workbench_id: summary.workbench_id.clone(),
                section: Some(Section::Metadata),
                relative_path: Some(
                    NormalizedRelativePath::new(RUN_MANIFEST_PATH)
                        .expect("reserved manifest path is valid"),
                ),
            };
            self.projected_path(&path)
        });
        json!({
            "workbench_id": summary.workbench_id.as_str(),
            "path": self.workbench_path(&summary.workbench_id),
            "committed": summary.committed,
            "manifest_path": manifest_path,
            "manifest_size_bytes": metadata.map(|metadata| metadata.size_bytes),
            "manifest_generation": metadata.map(|metadata| metadata.generation),
            "content_digest_uri": verified.as_ref().map(|verified| verified.content_digest_uri.clone()),
            "manifest_digest_uri": verified.as_ref().map(|verified| verified.manifest_digest_uri.clone()),
            "commit_identity": verified.as_ref().map(|verified| verified.commit_identity.clone()),
            "commit_identity_verified": verified.is_some(),
            "envelope_digest_uri": metadata.map(|metadata| metadata.digest_uri.clone()),
            "manifest_summary": manifest_summary_value(summary.manifest.as_ref()),
            "manifest": if include_manifest {
                summary.manifest.clone().unwrap_or(Value::Null)
            } else {
                Value::Null
            },
        })
    }
}

impl<B: WorkbenchBackend> WorkbenchToolHandler for SdkWorkbenchToolHandler<B> {
    fn execute(&self, name: &str, arguments: &Value) -> Result<Value, AgentError> {
        match name {
            "workbench_create" => self.create(arguments),
            "workbench_put_file" => self.put_file(arguments),
            "workbench_append" => self.append(arguments),
            "workbench_edit" => self.edit(arguments),
            "workbench_list" => self.list(arguments),
            "workbench_stat" => self.stat(arguments),
            "workbench_read" => self.read(arguments),
            "workbench_grep" => self.grep(arguments),
            "workbench_search" => self.search(arguments),
            "workbench_aggregate" => self.aggregate(arguments),
            "workbench_catalog" => self.catalog(arguments),
            "workbench_find" => self.find(arguments),
            "workbench_commit" => self.commit(arguments),
            "workbench_snapshot" => self.snapshot(arguments),
            "workbench_snapshot_renew" => self.snapshot_renew(arguments),
            "workbench_snapshot_retire" => self.snapshot_retire(arguments),
            "workbench_snapshot_list" => self.snapshot_list(arguments),
            "workbench_restore" => self.restore(arguments),
            other => Err(AgentError::unknown_tool(other)),
        }
    }
}

impl<B: WorkbenchBackend> SdkWorkbenchToolHandler<B> {
    fn create(&self, arguments: &Value) -> Result<Value, AgentError> {
        let workbench_id = parse_workbench_id(arguments)?;
        let _created = self.backend.create_workbench(&workbench_id)?;
        Ok(json!({
            "status": "success",
            "workbench_id": workbench_id.as_str(),
            "path": self.workbench_path(&workbench_id),
            "sections": WORKBENCH_SECTIONS.map(Section::as_str),
        }))
    }

    fn put_file(&self, arguments: &Value) -> Result<Value, AgentError> {
        let path = parse_write_path(arguments)?;
        let (body, default_content_type) = parse_payload(arguments, self.max_bytes)?;
        let content_type = optional_string(arguments, "content_type")?
            .unwrap_or(default_content_type)
            .to_owned();
        let replace = optional_bool(arguments, "replace")?.unwrap_or(false);
        let condition = if replace {
            let current = self
                .backend
                .stat(&path, &ReadView::Live)?
                .ok_or_else(|| not_found(&path))?;
            let artifact = current.artifact.ok_or_else(|| not_artifact(&path))?;
            PublishCondition::ReplaceOnly {
                expected_generation: artifact.generation,
            }
        } else {
            PublishCondition::CreateOnly
        };
        let outcome = self.backend.publish(PublishRequest {
            path: path.clone(),
            body,
            content_type,
            condition,
        })?;
        Ok(publish_result(
            &path,
            &self.projected_path(&path),
            &outcome,
            replace,
        ))
    }

    fn append(&self, arguments: &Value) -> Result<Value, AgentError> {
        let path = parse_write_path(arguments)?;
        let (delta, default_content_type) = parse_payload(arguments, self.max_bytes)?;
        let requested_content_type = optional_string(arguments, "content_type")?.map(str::to_owned);
        let appended_bytes = delta.len();
        let delta_digest = digest_uri(&delta);
        let outcome = self.backend.append(AppendRequest {
            path: path.clone(),
            delta,
            content_type: requested_content_type,
            create_content_type: default_content_type.to_owned(),
            max_logical_size: None,
        })?;
        Ok(json!({
            "status": "success",
            "workbench_id": path.workbench_id.as_str(),
            "section": path.section.map(Section::as_str),
            "relative_path": relative_path_value(&path),
            "path": self.projected_path(&path),
            "appended_bytes": appended_bytes,
            "size_bytes": outcome.metadata.size_bytes,
            "generation": outcome.metadata.generation,
            "created": outcome.created,
            "digest": delta_digest,
        }))
    }

    fn edit(&self, arguments: &Value) -> Result<Value, AgentError> {
        let path = parse_write_path(arguments)?;
        let old_string = required_string(arguments, "old_string")?;
        if old_string.is_empty() {
            return Err(AgentError::invalid_arguments(
                "old_string must not be empty",
            ));
        }
        let new_string = required_string(arguments, "new_string")?;
        let replace_all = optional_bool(arguments, "replace_all")?.unwrap_or(false);
        for attempt in 1..=self.edit_conflict_attempts {
            let current = self
                .backend
                .read(ReadRequest {
                    path: path.clone(),
                    view: ReadView::Live,
                })?
                .ok_or_else(|| not_found(&path))?;
            if current.bytes.len() > self.max_bytes {
                return Err(payload_too_large(current.bytes.len(), self.max_bytes));
            }
            let text = String::from_utf8(current.bytes).map_err(|_| {
                AgentError::backend(
                    "InvalidUtf8",
                    format!("{} is not valid UTF-8", path.logical_path()),
                    false,
                    json!({"path": path.logical_path()}),
                )
            })?;
            let matches = text.match_indices(old_string).count();
            if matches == 0 {
                return Err(AgentError::backend(
                    "NoMatch",
                    "old_string was not found",
                    false,
                    json!({"path": path.logical_path()}),
                ));
            }
            if matches > 1 && !replace_all {
                return Err(AgentError::backend(
                    "AmbiguousEdit",
                    format!("old_string occurs {matches} times; set replace_all=true"),
                    false,
                    json!({"matches": matches}),
                ));
            }
            let replacements = if replace_all { matches } else { 1 };
            let next = if replace_all {
                text.replace(old_string, new_string)
            } else {
                text.replacen(old_string, new_string, 1)
            };
            if next == text {
                return Ok(json!({
                    "status": "success",
                    "workbench_id": path.workbench_id.as_str(),
                    "section": path.section.map(Section::as_str),
                    "relative_path": relative_path_value(&path),
                    "path": self.projected_path(&path),
                    "replacements": replacements,
                    "size_bytes": current.metadata.size_bytes,
                    "generation": current.metadata.generation,
                    "no_change": true,
                }));
            }
            let next = next.into_bytes();
            if next.len() > self.max_bytes {
                return Err(payload_too_large(next.len(), self.max_bytes));
            }
            match self.backend.publish(PublishRequest {
                path: path.clone(),
                body: next,
                content_type: current.metadata.content_type,
                condition: PublishCondition::ReplaceOnly {
                    expected_generation: current.metadata.generation,
                },
            }) {
                Ok(outcome) => {
                    return Ok(json!({
                        "status": "success",
                        "workbench_id": path.workbench_id.as_str(),
                        "section": path.section.map(Section::as_str),
                        "relative_path": relative_path_value(&path),
                        "path": self.projected_path(&path),
                        "replacements": replacements,
                        "size_bytes": outcome.metadata.size_bytes,
                        "generation": outcome.metadata.generation,
                        "no_change": false,
                    }));
                }
                Err(error) if error.is_conflict() && attempt < self.edit_conflict_attempts => {}
                Err(error) if error.is_conflict() => {
                    return Err(conflict_exhausted(attempt, error));
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!("positive attempt count always returns")
    }

    fn list(&self, arguments: &Value) -> Result<Value, AgentError> {
        let path = parse_read_scope(arguments, true)?;
        let view = parse_read_view(arguments)?;
        let at_snapshot = read_view_selector_value(&view);
        let page = self.backend.list(ListRequest {
            path: path.clone(),
            view,
            cursor: optional_string(arguments, "cursor")?.map(str::to_owned),
            limit: optional_usize(arguments, "limit")?.unwrap_or(100),
        })?;
        let entries = page
            .entries
            .iter()
            .map(|entry| self.list_entry_value(entry))
            .collect::<Vec<_>>();
        let mut result = json!({
            "status": "success",
            "workbench_id": path.workbench_id.as_str(),
            "workbench_path": self.workbench_path(&path.workbench_id),
            "section": path.section.map(Section::as_str),
            "relative_path": relative_path_value(&path),
            "path": self.projected_path(&path),
            "entry_count": entries.len(),
            "entries": entries,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor.is_some(),
        });
        insert_optional_snapshot(&mut result, at_snapshot);
        Ok(result)
    }

    fn stat(&self, arguments: &Value) -> Result<Value, AgentError> {
        let path = parse_read_scope(arguments, true)?;
        let view = parse_read_view(arguments)?;
        let at_snapshot = read_view_selector_value(&view);
        let record = self
            .backend
            .stat(&path, &view)?
            .ok_or_else(|| not_found(&path))?;
        let mut result = json!({
            "status": "success",
            "workbench_id": path.workbench_id.as_str(),
            "workbench_path": self.workbench_path(&path.workbench_id),
            "section": path.section.map(Section::as_str),
            "relative_path": relative_path_value(&path),
            "path": self.projected_path(&path),
            "card": self.stat_value(&record),
        });
        insert_optional_snapshot(&mut result, at_snapshot);
        Ok(result)
    }

    fn read(&self, arguments: &Value) -> Result<Value, AgentError> {
        let path = parse_read_scope(arguments, false)?;
        let view = parse_read_view(arguments)?;
        let at_snapshot = read_view_selector_value(&view);
        if let Some(generation) = optional_u64(arguments, "if_none_match")? {
            if self
                .backend
                .stat(&path, &view)?
                .and_then(|record| record.artifact)
                .is_some_and(|artifact| artifact.generation == generation)
            {
                let mut result = json!({
                    "status": "success",
                    "workbench_id": path.workbench_id.as_str(),
                    "workbench_path": self.workbench_path(&path.workbench_id),
                    "section": path.section.map(Section::as_str),
                    "relative_path": relative_path_value(&path),
                    "path": self.projected_path(&path),
                    "not_modified": true,
                    "generation": generation,
                });
                insert_optional_snapshot(&mut result, at_snapshot);
                return Ok(result);
            }
        }
        let artifact = self
            .backend
            .read(ReadRequest {
                path: path.clone(),
                view,
            })?
            .ok_or_else(|| not_found(&path))?;
        if artifact.bytes.len() > self.max_bytes {
            return Err(payload_too_large(artifact.bytes.len(), self.max_bytes));
        }
        let format = optional_string(arguments, "format")?.unwrap_or("structured");
        let limit = optional_usize(arguments, "limit")?.unwrap_or(100);
        match format {
            "bytes" => shape_bytes_read(
                arguments,
                &path,
                artifact,
                limit,
                &self.workbench_path(&path.workbench_id),
                &self.projected_path(&path),
                at_snapshot,
            ),
            "structured" => shape_structured_read(
                arguments,
                &path,
                artifact,
                limit,
                &self.workbench_path(&path.workbench_id),
                &self.projected_path(&path),
                at_snapshot,
            ),
            _ => Err(AgentError::invalid_arguments("unknown read format")),
        }
    }

    fn grep(&self, arguments: &Value) -> Result<Value, AgentError> {
        let scope = parse_read_scope(arguments, true)?;
        let patterns = parse_grep_patterns(arguments)?;
        let folded = patterns
            .iter()
            .map(|pattern| pattern.to_lowercase())
            .collect::<Vec<_>>();
        let glob = optional_string(arguments, "glob")?;
        if glob.is_some_and(|glob| glob.contains('/') || glob.contains('\\')) {
            return Err(AgentError::invalid_arguments(
                "glob must match a basename and cannot contain a path separator",
            ));
        }
        let limit = optional_usize(arguments, "limit")?.unwrap_or(100);
        let recursive = required_bool(arguments, "recursive")?;
        let query_commitment = grep_query_commitment(&scope, &patterns, glob, recursive);
        let mut cursor = optional_string(arguments, "cursor")?.map(str::to_owned);
        let mut matches = Vec::new();
        let mut matched_files = 0_usize;
        let mut files_scanned = 0_usize;
        let mut next_cursor = None;
        loop {
            let page = self.backend.grep_candidates(GrepCandidateRequest {
                scope: scope.clone(),
                recursive,
                query_commitment,
                cursor: cursor.clone(),
                limit: 300,
            })?;
            let candidate_count = page.candidates.len();
            let page_has_more = page.next_cursor.is_some();
            for (candidate_index, candidate) in page.candidates.into_iter().enumerate() {
                let basename = candidate
                    .path
                    .relative_path
                    .as_ref()
                    .and_then(|path| path.components().last())
                    .unwrap_or_default();
                if glob.is_some_and(|glob| !glob_matches(glob, basename)) {
                    continue;
                }
                let Some(body) = self.backend.read(ReadRequest {
                    path: candidate.path.clone(),
                    view: ReadView::Live,
                })?
                else {
                    continue;
                };
                files_scanned += 1;
                let Ok(text) = std::str::from_utf8(&body.bytes) else {
                    continue;
                };
                let mut line_matches = Vec::new();
                for (index, line) in text.lines().enumerate() {
                    let lower = line.to_lowercase();
                    let matched = folded
                        .iter()
                        .enumerate()
                        .filter(|(_, pattern)| lower.contains(pattern.as_str()))
                        .map(|(index, _)| patterns[index].clone())
                        .collect::<Vec<_>>();
                    if !matched.is_empty() {
                        line_matches.push(json!({
                            "line_number": index + 1,
                            "line": line,
                            "patterns": matched,
                        }));
                    }
                }
                if line_matches.is_empty() {
                    continue;
                }
                matched_files += 1;
                matches.extend(line_matches.into_iter().map(|line_match| {
                    json!({
                        "path": self.projected_path(&candidate.path),
                        "section": candidate.path.section.map(Section::as_str),
                        "relative_path": relative_path_value(&candidate.path),
                        "line_number": line_match.get("line_number").cloned().unwrap_or(Value::Null),
                        "snippet": line_match.get("line").cloned().unwrap_or(Value::Null),
                    })
                }));
                if matched_files == limit {
                    if candidate_index + 1 < candidate_count || page_has_more {
                        next_cursor = Some(candidate.cursor_after);
                    }
                    break;
                }
            }
            if matched_files == limit {
                break;
            }
            let Some(next) = page.next_cursor else {
                break;
            };
            if cursor.as_ref() == Some(&next) {
                return Err(AgentError::backend(
                    "BackendProtocolMismatch",
                    "grep candidate cursor did not advance",
                    true,
                    json!({"cursor": next}),
                ));
            }
            cursor = Some(next);
        }
        Ok(json!({
            "status": "success",
            "workbench_id": scope.workbench_id.as_str(),
            "workbench_path": self.workbench_path(&scope.workbench_id),
            "section": scope.section.map(Section::as_str),
            "relative_path": relative_path_value(&scope),
            "path": self.projected_path(&scope),
            "pattern": required_string(arguments, "pattern")?,
            "recursive": recursive,
            "matches": matches,
            "files_scanned": files_scanned,
            "next_cursor": next_cursor,
            "truncated": next_cursor.is_some(),
        }))
    }

    fn search(&self, arguments: &Value) -> Result<Value, AgentError> {
        let scope = parse_query_scope(arguments)?;
        let projected_scope = self.query_path(&scope);
        let request = SearchRequest {
            scope,
            predicates: parse_predicates(arguments)?,
            fields: parse_string_array(arguments, "fields")?,
            sort: parse_sort(arguments)?,
            facets: parse_string_array(arguments, "facets")?,
            cursor: optional_string(arguments, "cursor")?.map(str::to_owned),
            limit: optional_usize(arguments, "limit")?.unwrap_or(10),
        };
        let page = self.backend.search(request)?;
        let hits = page
            .hits
            .iter()
            .map(|hit| {
                let path = indexed_scoped_path(hit)?;
                Ok(json!({
                    "workbench_id": hit.workbench_id.as_str(),
                    "path": self.projected_path(&path),
                    "section": path.section.map(Section::as_str),
                    "relative_path": relative_path_value(&path),
                    "values": query_map_value(&hit.projection),
                }))
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
            "status": "success",
            "path": projected_scope,
            "match_count": hits.len(),
            "matches": hits,
            "facets": facets,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor.is_some(),
        }))
    }

    fn aggregate(&self, arguments: &Value) -> Result<Value, AgentError> {
        const INPUT_COUNT_FIELD: &str = "__nokv_workbench_input_match_count";
        let scope = parse_query_scope(arguments)?;
        let projected_scope = self.query_path(&scope);
        let mut measures = parse_measures(arguments)?;
        if measures
            .iter()
            .any(|measure| measure.name == INPUT_COUNT_FIELD)
        {
            return Err(AgentError::invalid_arguments(format!(
                "aggregate measure name {INPUT_COUNT_FIELD} is reserved"
            )));
        }
        measures.push(AggregateMeasure {
            name: INPUT_COUNT_FIELD.to_owned(),
            operator: AggregateOperator::Count,
            field: None,
        });
        let request = AggregateRequest {
            scope,
            predicates: parse_predicates(arguments)?,
            group_by: parse_string_array(arguments, "group_by")?,
            measures,
            sort: parse_sort(arguments)?,
            limit: optional_usize(arguments, "limit")?.unwrap_or(20),
        };
        let page = self.backend.aggregate(request)?;
        let mut input_match_count = 0_u64;
        let groups = page
            .rows
            .iter()
            .map(|row| -> Result<Value, AgentError> {
                let count = row.measures.get(INPUT_COUNT_FIELD).ok_or_else(|| {
                    AgentError::backend(
                        "BackendProtocolMismatch",
                        "aggregate backend omitted the hidden input count",
                        true,
                        json!({"measure": INPUT_COUNT_FIELD}),
                    )
                })?;
                input_match_count = input_match_count.saturating_add(query_count_value(count)?);
                let mut values = row.measures.clone();
                values.remove(INPUT_COUNT_FIELD);
                Ok(json!({
                    "key": query_map_value(&row.groups),
                    "values": query_map_value(&values),
                }))
            })
            .collect::<Result<Vec<_>, AgentError>>()?;
        Ok(json!({
            "status": "success",
            "path": projected_scope,
            "input_match_count": input_match_count,
            "row_count": input_match_count,
            "group_count": groups.len(),
            "groups": groups,
            "truncated": false,
        }))
    }

    fn catalog(&self, arguments: &Value) -> Result<Value, AgentError> {
        let scope = parse_query_scope(arguments)?;
        let projected_scope = self.query_path(&scope);
        let result = self.backend.catalog(CatalogRequest {
            scope,
            field_prefix: optional_string(arguments, "field_prefix")?.map(str::to_owned),
            include_facets: optional_bool(arguments, "include_facets")?.unwrap_or(false),
        })?;
        let mut filterable = BTreeMap::<Vec<String>, Vec<String>>::new();
        let mut sortable = Vec::new();
        let mut facetable = Vec::new();
        for field in &result.fields {
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
        Ok(json!({
            "status": "success",
            "path": projected_scope,
            "catalog_empty": result.fields.is_empty(),
            "catalog": {
                "filterable": filterable,
                "sortable": sortable,
                "facetable": facetable,
                "facets": [],
            },
            "child_catalogs": [],
        }))
    }

    fn find(&self, arguments: &Value) -> Result<Value, AgentError> {
        let include_manifest = optional_bool(arguments, "include_manifest")?.unwrap_or(false);
        let page = self.backend.find_workbenches(FindRequest {
            committed: optional_bool(arguments, "committed")?,
            manifest_pattern: optional_string(arguments, "manifest_pattern")?.map(str::to_owned),
            include_manifest,
            cursor: optional_string(arguments, "cursor")?.map(str::to_owned),
            limit: optional_usize(arguments, "limit")?.unwrap_or(50),
        })?;
        let workbenches = page
            .workbenches
            .iter()
            .map(|workbench| self.workbench_summary_value(workbench, include_manifest))
            .collect::<Vec<_>>();
        Ok(json!({
            "status": "success",
            "path": self.logical_root,
            "matches": workbenches,
            "match_count": page.workbenches.len(),
            "entry_count": page.entry_count,
            "next_cursor": page.next_cursor,
            "truncated": page.next_cursor.is_some(),
        }))
    }

    fn commit(&self, arguments: &Value) -> Result<Value, AgentError> {
        let workbench_id = parse_workbench_id(arguments)?;
        let manifest = arguments
            .get("manifest")
            .and_then(Value::as_object)
            .ok_or_else(|| AgentError::invalid_arguments("manifest must be an object"))?;
        let canonical_manifest = canonical_json(&Value::Object(manifest.clone()))?;
        let manifest_digest_uri = digest_uri(&canonical_manifest);
        let content_digest_uri = required_string(arguments, "content_digest_uri")?.to_owned();
        let stable_commit_id =
            commit_identity(&workbench_id, &content_digest_uri, &manifest_digest_uri);
        let workbench_path = self.workbench_path(&workbench_id);
        let replace = optional_bool(arguments, "replace")?.unwrap_or(false);
        let outcome = self
            .backend
            .commit(CommitRequest {
                workbench_id: workbench_id.clone(),
                canonical_manifest,
                workbench_path: workbench_path.clone(),
                content_digest_uri: content_digest_uri.clone(),
                manifest_digest_uri: manifest_digest_uri.clone(),
                stable_commit_id,
                replace,
            })
            .map_err(|error| {
                if error.kind == BackendErrorKind::Conflict {
                    AgentError::backend(
                        "WorkbenchCommitConflict",
                        error.message,
                        error.retryable,
                        error.details,
                    )
                } else {
                    error.into()
                }
            })?;
        if outcome.commit_id != stable_commit_id {
            return Err(AgentError::backend(
                "WorkbenchCommitProtocolMismatch",
                "backend returned a commit identity different from the canonical request",
                true,
                json!({
                    "expected_commit_id": lowercase_hex(&stable_commit_id),
                    "actual_commit_id": lowercase_hex(&outcome.commit_id),
                }),
            ));
        }
        let manifest_path = ScopedPath {
            workbench_id: workbench_id.clone(),
            section: Some(Section::Metadata),
            relative_path: Some(
                NormalizedRelativePath::new(RUN_MANIFEST_PATH)
                    .expect("reserved manifest path is valid"),
            ),
        };
        Ok(json!({
            "status": "success",
            "workbench_id": workbench_id.as_str(),
            "workbench_path": workbench_path,
            "path": self.projected_path(&manifest_path),
            "size_bytes": outcome.manifest_size_bytes,
            "generation": outcome.generation,
            "content_digest_uri": content_digest_uri,
            "manifest_digest_uri": manifest_digest_uri,
            "commit_identity": lowercase_hex(&outcome.commit_id),
            "envelope_digest_uri": outcome.envelope_digest_uri,
            "tree_digest_uri": outcome.tree_digest_uri,
            "replace": replace,
            "idempotent_replay": outcome.idempotent_replay,
        }))
    }

    fn snapshot(&self, arguments: &Value) -> Result<Value, AgentError> {
        let workbench_id = parse_workbench_id(arguments)?;
        let ttl_defaulted = arguments.get("ttl_days").is_none();
        let ttl_days = optional_u64(arguments, "ttl_days")?.unwrap_or(DEFAULT_SNAPSHOT_TTL_DAYS);
        let annotation = snapshot_annotation(arguments)?;
        let record = self.backend.mint_snapshot(SnapshotMintRequest {
            workbench_id: workbench_id.clone(),
            name: optional_string(arguments, "name")?.map(str::to_owned),
            lease_millis: ttl_days.saturating_mul(MILLIS_PER_DAY),
            annotation,
        })?;
        let mut result = snapshot_value(
            &workbench_id,
            &self.workbench_path(&workbench_id),
            &record,
            Some(ttl_days),
            None,
        );
        if ttl_defaulted {
            result["expiry_warning"] = Value::String(format!(
                "lease defaulted to {DEFAULT_SNAPSHOT_TTL_DAYS} days; renew it before expiry if the frozen view must remain available"
            ));
        }
        Ok(result)
    }

    fn snapshot_renew(&self, arguments: &Value) -> Result<Value, AgentError> {
        let workbench_id = parse_workbench_id(arguments)?;
        let selector = parse_snapshot_selector_fields(arguments)?;
        let ttl_days = optional_u64(arguments, "ttl_days")?.unwrap_or(DEFAULT_SNAPSHOT_TTL_DAYS);
        let record = self.backend.renew_snapshot(SnapshotRenewRequest {
            workbench_id: workbench_id.clone(),
            selector,
            lease_millis: ttl_days.saturating_mul(MILLIS_PER_DAY),
        })?;
        Ok(snapshot_value(
            &workbench_id,
            &self.workbench_path(&workbench_id),
            &record,
            Some(ttl_days),
            Some("renewed"),
        ))
    }

    fn snapshot_retire(&self, arguments: &Value) -> Result<Value, AgentError> {
        let workbench_id = parse_workbench_id(arguments)?;
        let selector = parse_snapshot_selector_fields(arguments)?;
        let reason = optional_string(arguments, "reason")?.map(str::to_owned);
        let outcome = self.backend.retire_snapshot(SnapshotRetireRequest {
            workbench_id: workbench_id.clone(),
            selector,
            reason,
        })?;
        Ok(json!({
            "status": "success",
            "workbench_id": workbench_id.as_str(),
            "path": self.workbench_path(&workbench_id),
            "snapshot_id": outcome.snapshot_id,
            "name": outcome.name,
            "retired": outcome.retired,
            "state": outcome.state.as_str(),
            "retire_annotation": outcome.retire_annotation,
        }))
    }

    fn snapshot_list(&self, arguments: &Value) -> Result<Value, AgentError> {
        let workbench_id = parse_workbench_id(arguments)?;
        let snapshots = self.backend.list_snapshots(&workbench_id)?;
        let values = snapshots
            .iter()
            .map(|record| {
                let mut value = snapshot_record_value(record);
                value
                    .as_object_mut()
                    .expect("snapshot record is an object")
                    .insert(
                        "retire_annotation".to_owned(),
                        record.retire_annotation.clone().unwrap_or(Value::Null),
                    );
                value
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "status": "success",
            "workbench_id": workbench_id.as_str(),
            "path": self.workbench_path(&workbench_id),
            "snapshot_count": values.len(),
            "snapshots": values,
        }))
    }

    fn restore(&self, arguments: &Value) -> Result<Value, AgentError> {
        let source_workbench_id = parse_workbench_id(arguments)?;
        let destination_workbench_id =
            WorkbenchId::new(required_string(arguments, "destination_id")?.to_owned())
                .map_err(|error| AgentError::invalid_arguments(error.to_string()))?;
        if source_workbench_id == destination_workbench_id {
            return Err(AgentError::invalid_arguments(
                "destination_id must differ from id",
            ));
        }
        let selector = parse_snapshot_selector_value(
            arguments
                .get("at_snapshot")
                .ok_or_else(|| AgentError::invalid_arguments("missing at_snapshot"))?,
        )?;
        let outcome = self
            .backend
            .restore(RestoreRequest {
                source_workbench_id: source_workbench_id.clone(),
                source_workbench_path: self.workbench_path(&source_workbench_id),
                selector,
                destination_workbench_id: destination_workbench_id.clone(),
                destination_workbench_path: self.workbench_path(&destination_workbench_id),
            })
            .map_err(|error| match &error.kind {
                BackendErrorKind::Other(code) if code == "BackendProtocolMismatch" => {
                    AgentError::backend(
                        "RestoreProtocolMismatch",
                        error.message,
                        error.retryable,
                        error.details,
                    )
                }
                _ => error.into(),
            })?;
        let restore_manifest = ScopedPath {
            workbench_id: destination_workbench_id.clone(),
            section: Some(Section::Metadata),
            relative_path: Some(
                NormalizedRelativePath::new(RESTORE_MANIFEST_PATH)
                    .expect("reserved restore manifest path is valid"),
            ),
        };
        Ok(json!({
            "status": "success",
            "state": "complete",
            "operation_id": lowercase_hex(&outcome.operation_id),
            "source_workbench_id": source_workbench_id.as_str(),
            "destination_workbench_id": destination_workbench_id.as_str(),
            "snapshot_id": outcome.snapshot_id,
            "read_version": outcome.read_version,
            "destination_generation": outcome.destination_generation,
            "idempotent_replay": outcome.idempotent_replay,
            "cleanup_pending": false,
            "restore_manifest": self.projected_path(&restore_manifest),
        }))
    }
}

pub fn normalize_logical_workbench_root(raw: &str) -> Result<String, AgentError> {
    if !raw.starts_with('/') {
        return Err(AgentError::invalid_arguments(
            "logical Workbench root must be absolute",
        ));
    }
    if raw.contains('\\') || raw.contains('\0') {
        return Err(AgentError::invalid_arguments(
            "logical Workbench root must not contain backslashes or NUL",
        ));
    }
    let mut components = Vec::new();
    for component in raw.split('/') {
        if component.is_empty() {
            continue;
        }
        if matches!(component, "." | "..") {
            return Err(AgentError::invalid_arguments(
                "logical Workbench root must not contain '.' or '..' components",
            ));
        }
        components.push(component);
    }
    if components.is_empty() {
        return Err(AgentError::invalid_arguments(
            "logical Workbench root must not be /",
        ));
    }
    Ok(format!("/{}", components.join("/")))
}

fn parse_workbench_id(arguments: &Value) -> Result<WorkbenchId, AgentError> {
    WorkbenchId::new(required_string(arguments, "id")?.to_owned())
        .map_err(|error| AgentError::invalid_arguments(error.to_string()))
}

fn parse_write_path(arguments: &Value) -> Result<ScopedPath, AgentError> {
    let workbench_id = parse_workbench_id(arguments)?;
    let section = Section::parse(required_string(arguments, "section")?)?;
    let raw_path = required_string(arguments, "path")?;
    let relative_path = parse_relative_path(raw_path)?;
    reject_duplicated_section(section, &relative_path)?;
    if section == Section::Metadata
        && matches!(
            relative_path.as_str(),
            RUN_MANIFEST_PATH | RESTORE_MANIFEST_PATH
        )
    {
        return Err(AgentError::invalid_arguments(format!(
            "metadata/{} is a reserved Workbench projection and cannot be changed by generic artifact tools",
            relative_path.as_str()
        )));
    }
    Ok(ScopedPath {
        workbench_id,
        section: Some(section),
        relative_path: Some(relative_path),
    })
}

fn parse_read_scope(arguments: &Value, allow_empty_path: bool) -> Result<ScopedPath, AgentError> {
    let workbench_id = parse_workbench_id(arguments)?;
    let section = optional_string(arguments, "section")?
        .map(Section::parse)
        .transpose()?;
    let relative_path = parse_optional_scope_path(arguments)?;
    if !allow_empty_path && relative_path.is_none() {
        return Err(AgentError::invalid_arguments("path is required"));
    }
    if let (Some(section), Some(path)) = (section, &relative_path) {
        reject_duplicated_section(section, path)?;
    }
    Ok(ScopedPath {
        workbench_id,
        section,
        relative_path,
    })
}

fn parse_query_scope(arguments: &Value) -> Result<QueryScope, AgentError> {
    let workbench_id = optional_string(arguments, "id")?
        .map(|value| {
            WorkbenchId::new(value.to_owned())
                .map_err(|error| AgentError::invalid_arguments(error.to_string()))
        })
        .transpose()?;
    let section = optional_string(arguments, "section")?
        .map(Section::parse)
        .transpose()?;
    let path = parse_optional_scope_path(arguments)?;
    if let (Some(section), Some(path)) = (section, &path) {
        reject_duplicated_section(section, path)?;
    }
    Ok(QueryScope {
        workbench_id,
        section,
        path,
    })
}

fn indexed_scoped_path(hit: &SearchHit) -> Result<ScopedPath, AgentError> {
    let raw = hit.path.as_str();
    let (first, remainder) = raw
        .split_once('/')
        .map_or((raw, None), |(first, rest)| (first, Some(rest)));
    match Section::parse(first) {
        Ok(section) => Ok(ScopedPath {
            workbench_id: hit.workbench_id.clone(),
            section: Some(section),
            relative_path: remainder.map(parse_relative_path).transpose()?,
        }),
        Err(_) => Ok(ScopedPath {
            workbench_id: hit.workbench_id.clone(),
            section: None,
            relative_path: Some(hit.path.clone()),
        }),
    }
}

fn parse_relative_path(value: &str) -> Result<NormalizedRelativePath, AgentError> {
    NormalizedRelativePath::new(value.to_owned())
        .map_err(|error| AgentError::invalid_arguments(error.to_string()))
}

fn parse_optional_scope_path(
    arguments: &Value,
) -> Result<Option<NormalizedRelativePath>, AgentError> {
    optional_string(arguments, "path")?
        .filter(|path| !path.is_empty())
        .map(parse_relative_path)
        .transpose()
}

fn reject_duplicated_section(
    section: Section,
    path: &NormalizedRelativePath,
) -> Result<(), AgentError> {
    if path.components().next() == Some(section.as_str()) {
        return Err(AgentError::invalid_arguments(format!(
            "path is relative to section {section}; remove the duplicated section prefix"
        )));
    }
    Ok(())
}

fn parse_read_view(arguments: &Value) -> Result<ReadView, AgentError> {
    match arguments.get("at_snapshot") {
        None | Some(Value::Null) => Ok(ReadView::Live),
        Some(value) => parse_snapshot_selector_value(value).map(ReadView::Snapshot),
    }
}

fn read_view_selector_value(view: &ReadView) -> Option<Value> {
    match view {
        ReadView::Live => None,
        ReadView::Snapshot(SnapshotSelector::Id(snapshot_id)) => Some(json!(snapshot_id)),
        ReadView::Snapshot(SnapshotSelector::Name(name)) => Some(json!(name)),
    }
}

fn insert_optional_snapshot(result: &mut Value, at_snapshot: Option<Value>) {
    if let (Some(object), Some(at_snapshot)) = (result.as_object_mut(), at_snapshot) {
        object.insert("at_snapshot".to_owned(), at_snapshot);
    }
}

fn parse_snapshot_selector_value(value: &Value) -> Result<SnapshotSelector, AgentError> {
    if let Some(id) = value.as_u64() {
        return Ok(SnapshotSelector::Id(id));
    }
    if let Some(name) = value.as_str() {
        if name.is_empty() {
            return Err(AgentError::invalid_arguments(
                "snapshot name must not be empty",
            ));
        }
        return Ok(SnapshotSelector::Name(name.to_owned()));
    }
    Err(AgentError::invalid_arguments(
        "snapshot selector must be a non-negative id or non-empty name",
    ))
}

fn parse_snapshot_selector_fields(arguments: &Value) -> Result<SnapshotSelector, AgentError> {
    let id = arguments
        .get("snapshot_id")
        .filter(|value| !value.is_null());
    let name = arguments.get("name").filter(|value| !value.is_null());
    match (id, name) {
        (Some(id), None) => parse_snapshot_selector_value(id),
        (None, Some(name)) => parse_snapshot_selector_value(name),
        _ => Err(AgentError::invalid_arguments(
            "exactly one of snapshot_id or name is required",
        )),
    }
}

fn parse_payload(arguments: &Value, max_bytes: usize) -> Result<(Vec<u8>, &str), AgentError> {
    let text = arguments.get("text");
    let encoded = arguments.get("base64");
    let (bytes, content_type) = match (text, encoded) {
        (Some(Value::String(text)), None) => {
            (text.as_bytes().to_vec(), "text/plain; charset=utf-8")
        }
        (None, Some(Value::String(encoded))) => (
            base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|error| {
                    AgentError::invalid_arguments(format!("base64 payload is invalid: {error}"))
                })?,
            "application/octet-stream",
        ),
        (Some(_), Some(_)) => {
            return Err(AgentError::invalid_arguments(
                "text and base64 are mutually exclusive",
            ))
        }
        (None, None) => {
            return Err(AgentError::invalid_arguments(
                "exactly one of text or base64 is required",
            ))
        }
        _ => {
            return Err(AgentError::invalid_arguments(
                "text and base64 payloads must be strings",
            ))
        }
    };
    if bytes.len() > max_bytes {
        return Err(payload_too_large(bytes.len(), max_bytes));
    }
    Ok((bytes, content_type))
}

fn payload_too_large(actual: usize, maximum: usize) -> AgentError {
    AgentError::backend(
        "PayloadTooLarge",
        format!("payload is {actual} bytes, maximum is {maximum}"),
        false,
        json!({"actual_bytes": actual, "maximum_bytes": maximum}),
    )
}

fn not_found(path: &ScopedPath) -> AgentError {
    AgentError::backend(
        "NotFound",
        format!("path not found: {}", path.logical_path()),
        false,
        json!({"path": path.logical_path()}),
    )
}

fn not_artifact(path: &ScopedPath) -> AgentError {
    AgentError::backend(
        "NotArtifact",
        format!("path is not an artifact: {}", path.logical_path()),
        false,
        json!({"path": path.logical_path()}),
    )
}

fn conflict_exhausted(attempts: usize, error: BackendError) -> AgentError {
    AgentError::backend(
        "Conflict",
        format!("conditional publication conflicted after {attempts} attempts"),
        true,
        json!({"attempts": attempts, "last_error": error.message}),
    )
}

fn publish_result(
    path: &ScopedPath,
    projected_path: &str,
    outcome: &PublishOutcome,
    replace: bool,
) -> Value {
    json!({
        "status": "success",
        "workbench_id": path.workbench_id.as_str(),
        "section": path.section.map(Section::as_str),
        "relative_path": relative_path_value(path),
        "path": projected_path,
        "size_bytes": outcome.metadata.size_bytes,
        "generation": outcome.metadata.generation,
        "digest_uri": outcome.metadata.digest_uri,
        "content_type": outcome.metadata.content_type,
        "replace": replace,
    })
}

fn relative_path_value(path: &ScopedPath) -> Value {
    path.relative_path
        .as_ref()
        .map(|path| Value::String(path.to_string()))
        .unwrap_or(Value::Null)
}

fn projected_kind_name(kind: &ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Workbench | ArtifactKind::Section | ArtifactKind::Directory => "directory",
        ArtifactKind::Artifact => "file",
    }
}

fn shape_bytes_read(
    arguments: &Value,
    path: &ScopedPath,
    artifact: ArtifactBody,
    limit: usize,
    workbench_path: &str,
    projected_path: &str,
    at_snapshot: Option<Value>,
) -> Result<Value, AgentError> {
    let explicit_offset = optional_usize(arguments, "offset")?.unwrap_or(0);
    let offset = match optional_string(arguments, "cursor")? {
        Some(cursor) => {
            if explicit_offset != 0 {
                return Err(AgentError::invalid_arguments(
                    "offset and cursor cannot both select a byte position",
                ));
            }
            decode_cursor(cursor, "b")?
        }
        None => explicit_offset,
    };
    if offset > artifact.bytes.len() {
        return Err(AgentError::invalid_arguments(
            "byte offset is past the end of the artifact",
        ));
    }
    let end = offset.saturating_add(limit).min(artifact.bytes.len());
    let next_cursor = (end < artifact.bytes.len()).then(|| encode_cursor("b", end));
    let cursor = optional_string(arguments, "cursor")?.map(str::to_owned);
    let mut result = json!({
        "status": "success",
        "workbench_id": path.workbench_id.as_str(),
        "workbench_path": workbench_path,
        "section": path.section.map(Section::as_str),
        "relative_path": relative_path_value(path),
        "path": projected_path,
        "generation": artifact.metadata.generation,
        "total_size_bytes": artifact.metadata.size_bytes,
        "format": "bytes",
        "record_type": Value::Null,
        "record_count": Value::Null,
        "cursor": cursor,
        "items": [],
        "bytes": base64::engine::general_purpose::STANDARD.encode(&artifact.bytes[offset..end]),
        "bytes_encoding": "base64",
        "next_cursor": next_cursor,
        "truncated": next_cursor.is_some(),
    });
    insert_optional_snapshot(&mut result, at_snapshot);
    Ok(result)
}

fn shape_structured_read(
    arguments: &Value,
    path: &ScopedPath,
    artifact: ArtifactBody,
    limit: usize,
    workbench_path: &str,
    projected_path: &str,
    at_snapshot: Option<Value>,
) -> Result<Value, AgentError> {
    if optional_usize(arguments, "offset")?.unwrap_or(0) != 0 {
        return Err(AgentError::invalid_arguments(
            "offset is supported only for bytes reads",
        ));
    }
    let start = optional_string(arguments, "cursor")?
        .map(|cursor| decode_cursor(cursor, "r"))
        .transpose()?
        .unwrap_or(0);
    let (record_type, records) = structured_records(&artifact)?;
    if start > records.len() {
        return Err(AgentError::invalid_arguments(
            "structured cursor is past the end of the artifact",
        ));
    }
    let end = start.saturating_add(limit).min(records.len());
    let next_cursor = (end < records.len()).then(|| encode_cursor("r", end));
    let items = records[start..end]
        .iter()
        .enumerate()
        .map(|(offset, value)| json!({"index": start + offset, "value": value}))
        .collect::<Vec<_>>();
    let cursor = optional_string(arguments, "cursor")?.map(str::to_owned);
    let mut result = json!({
        "status": "success",
        "workbench_id": path.workbench_id.as_str(),
        "workbench_path": workbench_path,
        "section": path.section.map(Section::as_str),
        "relative_path": relative_path_value(path),
        "path": projected_path,
        "generation": artifact.metadata.generation,
        "total_size_bytes": artifact.metadata.size_bytes,
        "format": "structured",
        "record_type": record_type,
        "record_count": records.len(),
        "cursor": cursor,
        "items": items,
        "bytes": Value::Null,
        "bytes_encoding": Value::Null,
        "next_cursor": next_cursor,
        "truncated": next_cursor.is_some(),
    });
    insert_optional_snapshot(&mut result, at_snapshot);
    Ok(result)
}

fn structured_records(artifact: &ArtifactBody) -> Result<(&'static str, Vec<Value>), AgentError> {
    let content_type = artifact.metadata.content_type.to_ascii_lowercase();
    let path = artifact.path.logical_path().to_ascii_lowercase();
    let parsed = if content_type.contains("json") || path.ends_with(".json") {
        serde_json::from_slice::<Value>(&artifact.bytes).map_err(|error| {
            AgentError::backend(
                "StructuredDecodeFailed",
                format!("JSON decoding failed: {error}"),
                false,
                json!({"path": artifact.path.logical_path(), "format": "json"}),
            )
        })?
    } else if content_type.contains("yaml") || path.ends_with(".yaml") || path.ends_with(".yml") {
        serde_yaml::from_slice::<Value>(&artifact.bytes).map_err(|error| {
            AgentError::backend(
                "StructuredDecodeFailed",
                format!("YAML decoding failed: {error}"),
                false,
                json!({"path": artifact.path.logical_path(), "format": "yaml"}),
            )
        })?
    } else {
        let text = std::str::from_utf8(&artifact.bytes).map_err(|_| {
            AgentError::backend(
                "InvalidUtf8",
                "structured text read requires UTF-8",
                false,
                json!({"path": artifact.path.logical_path()}),
            )
        })?;
        return Ok((
            "text_lines",
            text.lines()
                .map(|line| Value::String(line.to_owned()))
                .collect(),
        ));
    };
    let yaml = content_type.contains("yaml") || path.ends_with(".yaml") || path.ends_with(".yml");
    Ok(match parsed {
        Value::Array(records) if yaml => ("yaml_mapping", records),
        Value::Array(records) => ("json_array", records),
        value if yaml => ("yaml_mapping", vec![value]),
        value => ("json_object", vec![value]),
    })
}

fn encode_cursor(kind: &str, offset: usize) -> String {
    format!("{kind}:{offset}")
}

fn decode_cursor(cursor: &str, expected_kind: &str) -> Result<usize, AgentError> {
    let (kind, value) = cursor
        .split_once(':')
        .ok_or_else(|| AgentError::invalid_arguments("cursor has an invalid shape"))?;
    if kind != expected_kind {
        return Err(AgentError::invalid_arguments(
            "cursor belongs to a different read format",
        ));
    }
    value
        .parse::<usize>()
        .map_err(|_| AgentError::invalid_arguments("cursor offset is invalid"))
}

fn argument_object(arguments: &Value) -> Result<&Map<String, Value>, AgentError> {
    arguments
        .as_object()
        .ok_or_else(|| AgentError::invalid_arguments("arguments must be an object"))
}

fn required_string<'a>(arguments: &'a Value, name: &str) -> Result<&'a str, AgentError> {
    argument_object(arguments)?
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| AgentError::invalid_arguments(format!("{name} must be a string")))
}

fn optional_string<'a>(arguments: &'a Value, name: &str) -> Result<Option<&'a str>, AgentError> {
    match argument_object(arguments)?.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(AgentError::invalid_arguments(format!(
            "{name} must be a string or null"
        ))),
    }
}

fn required_bool(arguments: &Value, name: &str) -> Result<bool, AgentError> {
    argument_object(arguments)?
        .get(name)
        .and_then(Value::as_bool)
        .ok_or_else(|| AgentError::invalid_arguments(format!("{name} must be a boolean")))
}

fn optional_bool(arguments: &Value, name: &str) -> Result<Option<bool>, AgentError> {
    match argument_object(arguments)?.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(AgentError::invalid_arguments(format!(
            "{name} must be a boolean or null"
        ))),
    }
}

fn optional_u64(arguments: &Value, name: &str) -> Result<Option<u64>, AgentError> {
    match argument_object(arguments)?.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            AgentError::invalid_arguments(format!("{name} must be a non-negative integer"))
        }),
    }
}

fn optional_usize(arguments: &Value, name: &str) -> Result<Option<usize>, AgentError> {
    optional_u64(arguments, name)?
        .map(|value| {
            usize::try_from(value).map_err(|_| {
                AgentError::invalid_arguments(format!("{name} exceeds this platform's range"))
            })
        })
        .transpose()
}

fn parse_grep_patterns(arguments: &Value) -> Result<Vec<String>, AgentError> {
    let mut patterns = Vec::new();
    let primary = required_string(arguments, "pattern")?;
    if primary.is_empty() {
        return Err(AgentError::invalid_arguments(
            "grep patterns must not be empty",
        ));
    }
    patterns.push(primary.to_owned());
    match argument_object(arguments)?.get("patterns") {
        None => {}
        Some(Value::Array(values)) => {
            for value in values {
                let pattern = value.as_str().ok_or_else(|| {
                    AgentError::invalid_arguments("patterns must contain only strings")
                })?;
                if pattern.is_empty() {
                    return Err(AgentError::invalid_arguments(
                        "grep patterns must not be empty",
                    ));
                }
                patterns.push(pattern.to_owned());
            }
        }
        Some(_) => {
            return Err(AgentError::invalid_arguments(
                "patterns must be an array of strings",
            ));
        }
    }
    let mut seen = BTreeSet::new();
    patterns.retain(|pattern| seen.insert(pattern.to_lowercase()));
    if patterns.len() > MAX_GREP_PATTERNS {
        return Err(AgentError::invalid_arguments(format!(
            "at most {MAX_GREP_PATTERNS} grep patterns are allowed"
        )));
    }
    Ok(patterns)
}

fn grep_query_commitment(
    scope: &ScopedPath,
    patterns: &[String],
    glob: Option<&str>,
    recursive: bool,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workbench.grep-query.v1\0");
    hash_length_prefixed(&mut hasher, scope.workbench_id.as_str().as_bytes());
    match scope.section {
        Some(section) => {
            hasher.update([1]);
            hash_length_prefixed(&mut hasher, section.as_str().as_bytes());
        }
        None => hasher.update([0]),
    }
    match &scope.relative_path {
        Some(path) => {
            hasher.update([1]);
            hash_length_prefixed(&mut hasher, path.as_str().as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update([u8::from(recursive)]);
    hasher.update(
        u64::try_from(patterns.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for pattern in patterns {
        hash_length_prefixed(&mut hasher, pattern.as_bytes());
    }
    match glob {
        Some(glob) => {
            hasher.update([1]);
            hash_length_prefixed(&mut hasher, glob.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.finalize().into()
}

fn glob_matches(pattern: &str, text: &str) -> bool {
    let text = text.chars().collect::<Vec<_>>();
    let mut previous = vec![false; text.len() + 1];
    previous[0] = true;
    for token in pattern.chars() {
        let mut current = vec![false; text.len() + 1];
        if token == '*' {
            current[0] = previous[0];
        }
        for (index, character) in text.iter().enumerate() {
            current[index + 1] = match token {
                '*' => previous[index + 1] || current[index],
                '?' => previous[index],
                exact => previous[index] && exact == *character,
            };
        }
        previous = current;
    }
    previous[text.len()]
}

fn parse_string_array(arguments: &Value, name: &str) -> Result<Vec<String>, AgentError> {
    let Some(value) = argument_object(arguments)?.get(name) else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| AgentError::invalid_arguments(format!("{name} must be an array")))?;
    let mut result = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values {
        let value = value.as_str().ok_or_else(|| {
            AgentError::invalid_arguments(format!("{name} must contain only strings"))
        })?;
        if value.is_empty() {
            return Err(AgentError::invalid_arguments(format!(
                "{name} must not contain empty field names"
            )));
        }
        if !seen.insert(value.to_owned()) {
            return Err(AgentError::invalid_arguments(format!(
                "{name} contains duplicate field {value}"
            )));
        }
        result.push(value.to_owned());
    }
    Ok(result)
}

fn parse_predicates(arguments: &Value) -> Result<Vec<QueryPredicate>, AgentError> {
    let Some(value) = argument_object(arguments)?.get("predicates") else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| AgentError::invalid_arguments("predicates must be an array"))?;
    values
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| AgentError::invalid_arguments("each predicate must be an object"))?;
            let field = object
                .get("field")
                .and_then(Value::as_str)
                .filter(|field| !field.is_empty())
                .ok_or_else(|| {
                    AgentError::invalid_arguments("predicate field must be a non-empty string")
                })?
                .to_owned();
            let operator = match object.get("op").and_then(Value::as_str) {
                Some("eq") => PredicateOperator::Equal,
                Some("ne") => PredicateOperator::NotEqual,
                Some("in") => PredicateOperator::In,
                Some("prefix") => PredicateOperator::Prefix,
                Some("suffix") => PredicateOperator::Suffix,
                Some("contains") => PredicateOperator::Contains,
                Some("gt") => PredicateOperator::Greater,
                Some("gte") => PredicateOperator::GreaterOrEqual,
                Some("lt") => PredicateOperator::Less,
                Some("lte") => PredicateOperator::LessOrEqual,
                Some("exists") => PredicateOperator::Exists,
                Some("not_exists") => PredicateOperator::NotExists,
                _ => {
                    return Err(AgentError::invalid_arguments(
                        "predicate op is not supported",
                    ));
                }
            };
            let value = match operator {
                PredicateOperator::Exists | PredicateOperator::NotExists => {
                    if object.get("value").is_some_and(|value| !value.is_null()) {
                        return Err(AgentError::invalid_arguments(
                            "exists predicates cannot carry a value",
                        ));
                    }
                    None
                }
                _ => Some(query_value_from_json(object.get("value").ok_or_else(
                    || AgentError::invalid_arguments("predicate value is required for this op"),
                )?)?),
            };
            if operator == PredicateOperator::In && !matches!(value, Some(QueryValue::List(_))) {
                return Err(AgentError::invalid_arguments(
                    "in predicate value must be an array",
                ));
            }
            Ok(QueryPredicate {
                field,
                operator,
                value,
            })
        })
        .collect()
}

fn query_value_from_json(value: &Value) -> Result<QueryValue, AgentError> {
    match value {
        Value::Null => Ok(QueryValue::Null),
        Value::Bool(value) => Ok(QueryValue::Boolean(*value)),
        Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                Ok(QueryValue::Unsigned(value))
            } else if let Some(value) = value.as_i64() {
                Ok(QueryValue::Signed(value))
            } else if let Some(value) = value.as_f64() {
                Ok(QueryValue::Float(value))
            } else {
                Err(AgentError::invalid_arguments(
                    "query numbers must be non-negative or finite",
                ))
            }
        }
        Value::String(value) => Ok(QueryValue::String(value.clone())),
        Value::Array(values) => values
            .iter()
            .map(query_value_from_json)
            .collect::<Result<Vec<_>, _>>()
            .map(QueryValue::List),
        Value::Object(_) => Err(AgentError::invalid_arguments(
            "query values cannot be objects",
        )),
    }
}

fn parse_sort(arguments: &Value) -> Result<Vec<QuerySort>, AgentError> {
    let Some(value) = argument_object(arguments)?.get("sort") else {
        return Ok(Vec::new());
    };
    let values = value
        .as_array()
        .ok_or_else(|| AgentError::invalid_arguments("sort must be an array"))?;
    let mut seen = BTreeSet::new();
    values
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| AgentError::invalid_arguments("each sort must be an object"))?;
            let field = object
                .get("field")
                .and_then(Value::as_str)
                .filter(|field| !field.is_empty())
                .ok_or_else(|| {
                    AgentError::invalid_arguments("sort field must be a non-empty string")
                })?
                .to_owned();
            if !seen.insert(field.clone()) {
                return Err(AgentError::invalid_arguments(format!(
                    "sort contains duplicate field {field}"
                )));
            }
            let direction = match object.get("direction").and_then(Value::as_str) {
                None | Some("asc") => SortDirection::Ascending,
                Some("desc") => SortDirection::Descending,
                Some(_) => {
                    return Err(AgentError::invalid_arguments(
                        "sort direction must be asc or desc",
                    ));
                }
            };
            Ok(QuerySort { field, direction })
        })
        .collect()
}

fn parse_measures(arguments: &Value) -> Result<Vec<AggregateMeasure>, AgentError> {
    let values = argument_object(arguments)?
        .get("measures")
        .and_then(Value::as_array)
        .ok_or_else(|| AgentError::invalid_arguments("measures must be an array"))?;
    let mut names = BTreeSet::new();
    values
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .ok_or_else(|| AgentError::invalid_arguments("each measure must be an object"))?;
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .filter(|name| !name.is_empty())
                .ok_or_else(|| {
                    AgentError::invalid_arguments("measure name must be a non-empty string")
                })?
                .to_owned();
            if !names.insert(name.clone()) {
                return Err(AgentError::invalid_arguments(format!(
                    "duplicate measure name {name}"
                )));
            }
            let operator = match object.get("op").and_then(Value::as_str) {
                Some("count") => AggregateOperator::Count,
                Some("sum") => AggregateOperator::Sum,
                Some("avg") => AggregateOperator::Average,
                Some("min") => AggregateOperator::Minimum,
                Some("max") => AggregateOperator::Maximum,
                _ => {
                    return Err(AgentError::invalid_arguments(
                        "aggregate op is not supported",
                    ));
                }
            };
            let field = match object.get("field") {
                None | Some(Value::Null) => None,
                Some(Value::String(field)) if !field.is_empty() => Some(field.clone()),
                Some(_) => {
                    return Err(AgentError::invalid_arguments(
                        "measure field must be a non-empty string or null",
                    ));
                }
            };
            if operator != AggregateOperator::Count && field.is_none() {
                return Err(AgentError::invalid_arguments(
                    "non-count measures require a field",
                ));
            }
            Ok(AggregateMeasure {
                name,
                operator,
                field,
            })
        })
        .collect()
}

fn query_map_value(values: &BTreeMap<String, QueryValue>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(key, value)| (key.clone(), value.to_json()))
            .collect(),
    )
}

fn query_count_value(value: &QueryValue) -> Result<u64, AgentError> {
    match value {
        QueryValue::Unsigned(value) => Ok(*value),
        QueryValue::Signed(value) if *value >= 0 => Ok(*value as u64),
        _ => Err(AgentError::backend(
            "BackendProtocolMismatch",
            "aggregate backend returned a non-integer hidden count",
            true,
            json!({"value": value.to_json()}),
        )),
    }
}

struct VerifiedCommitProjection {
    content_digest_uri: String,
    manifest_digest_uri: String,
    commit_identity: String,
}

fn verified_commit_projection(summary: &WorkbenchSummary) -> Option<VerifiedCommitProjection> {
    let envelope = summary.manifest.as_ref()?;
    let canonical_envelope = canonical_json_bytes(envelope).ok()?;
    let verified = verify_run_manifest_v1(&canonical_envelope).ok()?;
    if verified.workbench_id != summary.workbench_id
        || summary
            .commit_id
            .is_some_and(|typed_identity| typed_identity != verified.commit_identity)
    {
        return None;
    }
    Some(VerifiedCommitProjection {
        content_digest_uri: verified.content_digest_uri,
        manifest_digest_uri: verified.manifest_digest_uri,
        commit_identity: lowercase_hex(&verified.commit_identity),
    })
}

fn manifest_summary_value(envelope: Option<&Value>) -> Value {
    let Some(envelope) = envelope else {
        return Value::Null;
    };
    let mut manifest_keys = envelope
        .get("manifest")
        .and_then(Value::as_object)
        .map(|manifest| manifest.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    manifest_keys.sort();
    json!({
        "schema": envelope.get("schema").cloned().unwrap_or(Value::Null),
        "workbench_id": envelope.get("workbench_id").cloned().unwrap_or(Value::Null),
        "content_digest_uri": envelope.get("content_digest_uri").cloned().unwrap_or(Value::Null),
        "manifest_digest_uri": envelope.get("manifest_digest_uri").cloned().unwrap_or(Value::Null),
        "commit_identity": envelope.get("commit_identity").cloned().unwrap_or(Value::Null),
        "committed_at_unix_seconds": envelope.get("committed_at_unix_seconds").cloned().unwrap_or(Value::Null),
        "manifest_keys": manifest_keys,
        "manifest_task": envelope
            .get("manifest")
            .and_then(|manifest| manifest.get("task"))
            .cloned()
            .unwrap_or(Value::Null),
    })
}

fn canonical_json(value: &Value) -> Result<Vec<u8>, AgentError> {
    canonical_json_bytes(value).map_err(|error| {
        AgentError::backend(
            "CanonicalEncodingFailed",
            format!("canonical JSON encoding failed: {error}"),
            false,
            json!({}),
        )
    })
}

fn commit_identity(
    workbench_id: &WorkbenchId,
    content_digest_uri: &str,
    manifest_digest_uri: &str,
) -> [u8; 32] {
    workbench_commit_identity(workbench_id, content_digest_uri, manifest_digest_uri)
}

fn snapshot_annotation(arguments: &Value) -> Result<Value, AgentError> {
    if optional_string(arguments, "name")?.is_some_and(str::is_empty) {
        return Err(AgentError::invalid_arguments(
            "snapshot name must not be empty",
        ));
    }
    let reason = optional_string(arguments, "reason")?.map(str::to_owned);
    let metadata = match argument_object(arguments)?.get("metadata") {
        None | Some(Value::Null) => Value::Null,
        Some(value @ Value::Object(_)) => value.clone(),
        Some(_) => {
            return Err(AgentError::invalid_arguments(
                "metadata must be an object or null",
            ));
        }
    };
    if value_depth(&metadata) > 8 {
        return Err(AgentError::invalid_arguments(
            "snapshot metadata nesting exceeds 8 levels",
        ));
    }
    let annotation = json!({"reason": reason, "metadata": metadata});
    let encoded = canonical_json(&annotation)?;
    if encoded.len() > 4096 {
        return Err(AgentError::invalid_arguments(
            "snapshot annotation exceeds 4096 encoded bytes",
        ));
    }
    Ok(annotation)
}

fn value_depth(value: &Value) -> usize {
    match value {
        Value::Array(values) => 1 + values.iter().map(value_depth).max().unwrap_or(0),
        Value::Object(values) => 1 + values.values().map(value_depth).max().unwrap_or(0),
        _ => 0,
    }
}

fn snapshot_record_value(record: &SnapshotRecord) -> Value {
    json!({
        "snapshot_id": record.snapshot_id,
        "name": record.name,
        "read_version": record.read_version,
        "lease_expires_at": record.lease_expires_unix_ms,
        "lease_expires_unix_ms": record.lease_expires_unix_ms,
        "annotation": record.annotation,
        "state": record.state.as_str(),
    })
}

fn snapshot_value(
    workbench_id: &WorkbenchId,
    workbench_path: &str,
    record: &SnapshotRecord,
    ttl_days: Option<u64>,
    action: Option<&str>,
) -> Value {
    let mut value = snapshot_record_value(record);
    let object = value
        .as_object_mut()
        .expect("snapshot_record_value always returns an object");
    object.insert("status".to_owned(), Value::String("success".to_owned()));
    object.insert(
        "workbench_id".to_owned(),
        Value::String(workbench_id.as_str().to_owned()),
    );
    object.insert("path".to_owned(), Value::String(workbench_path.to_owned()));
    if let Some(ttl_days) = ttl_days {
        object.insert("ttl_days".to_owned(), Value::Number(ttl_days.into()));
    }
    if let Some(action) = action {
        object.insert(action.to_owned(), Value::Bool(true));
    }
    value
}
