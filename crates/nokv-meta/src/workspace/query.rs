/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Correctness-first workspace metadata queries.
//!
//! All results are derived from authoritative `WorkspaceCurrent`,
//! `PathCurrent`, commit-head, and change-event scans at one exact
//! [`RootReadContext`]. Secondary projections are decoded once from each path
//! row; query execution never follows them with per-entry point reads.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use nokv_types::{
    CommitId, CommitVersion, Generation, GenericIndexGenerationState, NormalizedRelativePath,
    ReadVersion, RootId, WorkbenchId, WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState,
    FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::codec::{
    change_event_key, decode_change_event_key as decode_change_event_key_bytes,
    decode_generic_index_row_key, decode_path_current_key, decode_workspace_current_key,
    generic_index_current_key, generic_index_generation_key, generic_index_row_prefix,
    path_child_prefix, workbench_commit_head_key,
};
use super::commit_records::{CommitRecordError, WorkbenchCommitHeadRecord};
use super::engine::{MetaError, MetaShard, MetadataScanItem};
use super::generic_index_records::{
    advance_generic_index_row_rolling_digest, empty_generic_index_row_digest,
    generic_index_capability_digest, generic_index_row_digest,
    verify_generic_index_generation_seal, GenericIndexCurrentRecord, GenericIndexFieldCapability,
    GenericIndexOperator, GenericIndexRecordError, GenericIndexRowBinding, GenericIndexRowRecord,
};
use super::keyspace::MetadataFamily;
use super::namespace::{get_visible_workspace_at, NamespaceError, RootReadContext};
use super::publication_records::{PathEntry, PublicationRecordCodecError, WorkspaceRecord};
use super::query_records::{
    ChangeEventRecord, FiniteFloat, QueryFieldId, QueryRecordError, QueryScalar, QueryScalarType,
    TypedProjection,
};

/// Maximum page size for query, aggregate, catalog, discovery, and event APIs.
pub const MAX_QUERY_PAGE_SIZE: usize = 256;
/// Maximum metadata predicates.
pub const MAX_QUERY_PREDICATES: usize = 64;
/// Maximum distinct scalar members in one canonical `In` operand.
pub const MAX_QUERY_IN_VALUES: usize = 64;
/// Maximum selected fields.
pub const MAX_QUERY_PROJECTION_FIELDS: usize = 64;
/// Maximum sort fields.
pub const MAX_QUERY_SORT_FIELDS: usize = 8;
/// Maximum facet fields.
pub const MAX_QUERY_FACET_FIELDS: usize = 16;
/// Maximum group fields.
pub const MAX_QUERY_GROUP_FIELDS: usize = 8;
/// Maximum aggregate measures.
pub const MAX_QUERY_AGGREGATES: usize = 16;
/// Maximum returned buckets for one facet.
pub const MAX_FACET_BUCKETS_PER_FIELD: usize = 256;
/// Maximum opaque cursor size.
pub const MAX_QUERY_CURSOR_BYTES: usize = 8 * 1024;

const META_SCAN_BATCH: usize = 256;
const CURSOR_FORMAT_VERSION: u8 = 1;

/// Bounded normalized display-only root for generic namespace `path` values.
/// It is never a storage identity and is never written to metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresentationPathRoot(String);

impl PresentationPathRoot {
    pub const MAX_BYTES: usize = 4096;
    pub const MAX_COMPONENTS: usize = 64;

    pub fn new(value: impl Into<String>) -> Result<Self, QueryError> {
        let value = value.into();
        validate_presentation_path_root(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Explicit row/capability model selected by a query caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryProfile {
    /// Current Workbench artifact metadata fields and artifact-only rows.
    ArtifactV1,
    /// Generic Agent v1 namespace cards, including virtual and implicit directories.
    GenericNamespaceV1 {
        presentation_path_root: PresentationPathRoot,
    },
}

impl QueryProfile {
    fn is_generic_namespace(&self) -> bool {
        matches!(self, Self::GenericNamespaceV1 { .. })
    }

    fn presentation_path_root(&self) -> Option<&PresentationPathRoot> {
        match self {
            Self::ArtifactV1 => None,
            Self::GenericNamespaceV1 {
                presentation_path_root,
            } => Some(presentation_path_root),
        }
    }
}

/// Root-wide or exact-workbench query scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryScope {
    Root,
    Workspace(WorkbenchId),
}

/// Comparison supported by metadata predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryOperator {
    Equal,
    NotEqual,
    In,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Prefix,
    Suffix,
    Contains,
    Exists,
    NotExists,
}

/// Typed, canonical operand shape for one metadata predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryOperand {
    /// Required by `Exists` and `NotExists` only.
    None,
    /// Required by every single-value comparison operator.
    Scalar(QueryScalar),
    /// Required by `In`; ordered set identity makes cursor hashing deterministic.
    Set(BTreeSet<QueryScalar>),
}

impl From<QueryScalar> for QueryOperand {
    fn from(value: QueryScalar) -> Self {
        Self::Scalar(value)
    }
}

/// One bounded metadata predicate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPredicate {
    pub field_id: QueryFieldId,
    pub operator: QueryOperator,
    pub operand: QueryOperand,
}

/// Stable sort direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuerySortDirection {
    Ascending,
    Descending,
}

/// One metadata or aggregate-result sort field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuerySort {
    pub field_id: QueryFieldId,
    pub direction: QuerySortDirection,
}

/// Search paths using only metadata projections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchRequest {
    pub profile: QueryProfile,
    pub scope: QueryScope,
    /// Match this exact component path and its descendants.
    pub path_prefix: Option<NormalizedRelativePath>,
    pub predicates: Vec<QueryPredicate>,
    pub projection: Vec<QueryFieldId>,
    pub sort: Vec<QuerySort>,
    pub facets: Vec<QueryFieldId>,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

/// Compact authoritative path card plus requested projection fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchHit {
    pub workbench_id: WorkbenchId,
    pub path: NormalizedRelativePath,
    pub generation: Generation,
    pub body_digest_uri: String,
    pub logical_size: u64,
    pub content_type: String,
    pub producer: Option<String>,
    pub manifest_id: Option<String>,
    pub projection: BTreeMap<QueryFieldId, QueryScalar>,
}

/// Generic Agent namespace row kind. No filesystem inode or dentry semantics
/// are implied; directories are presentation rows derived from workspace
/// visibility and authoritative artifact prefixes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenericNamespaceKind {
    Directory,
    Artifact,
}

/// Compact immutable artifact fields carried by one generic namespace row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericArtifactMetadata {
    pub generation: Generation,
    pub logical_size: u64,
    pub body_digest_uri: String,
    pub content_type: String,
    pub producer: Option<String>,
    pub manifest_id: Option<String>,
}

/// Root-independent generic namespace row. `relative_path == None` names the
/// visible Workbench directory; all other paths are relative to that
/// Workbench and never contain an Agent presentation root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericNamespaceHit {
    pub workbench_id: WorkbenchId,
    pub relative_path: Option<NormalizedRelativePath>,
    pub kind: GenericNamespaceKind,
    pub artifact: Option<GenericArtifactMetadata>,
    pub projection: BTreeMap<QueryFieldId, QueryScalar>,
    /// Exact ordered custom values selected from the durable Generic index.
    /// A transport may render one value as a scalar and multiple values as an
    /// array without changing ArtifactV1's scalar projection contract.
    pub indexed_values: BTreeMap<QueryFieldId, Vec<QueryScalar>>,
}

/// One deterministic facet bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FacetBucket {
    pub value: QueryScalar,
    pub count: u64,
}

/// Facet buckets are ordered by descending count and then scalar value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FacetResult {
    pub field_id: QueryFieldId,
    pub buckets: Vec<FacetBucket>,
    pub distinct_count: u64,
    pub truncated: bool,
}

/// One stable search page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    pub namespace_hits: Vec<GenericNamespaceHit>,
    pub match_count: u64,
    pub facets: Vec<FacetResult>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: ReadVersion,
}

/// Supported aggregate operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
}

/// One named aggregate output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateSpec {
    pub result_id: QueryFieldId,
    pub function: AggregateFunction,
    /// `count` accepts `None` for row count. Other functions require a field.
    pub field_id: Option<QueryFieldId>,
}

/// Aggregate over the same authoritative candidate set as search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateRequest {
    pub profile: QueryProfile,
    pub scope: QueryScope,
    pub path_prefix: Option<NormalizedRelativePath>,
    pub predicates: Vec<QueryPredicate>,
    pub group_by: Vec<QueryFieldId>,
    pub aggregates: Vec<AggregateSpec>,
    /// Sort ids name either a group field or an aggregate `result_id`.
    pub sort: Vec<QuerySort>,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

/// One deterministic aggregate group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregateGroup {
    pub keys: BTreeMap<QueryFieldId, QueryScalar>,
    pub values: BTreeMap<QueryFieldId, QueryScalar>,
}

/// One stable aggregate page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggregatePage {
    pub groups: Vec<AggregateGroup>,
    pub input_match_count: u64,
    pub row_count: u64,
    pub group_count: u64,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: ReadVersion,
}

/// Catalog capabilities for one stable field id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogField {
    pub field_id: QueryFieldId,
    pub scalar_type: QueryScalarType,
    /// All scalar kinds observed in a Generic custom generation. This is empty
    /// for a declared zero-row field and has one entry for ordinary built-ins
    /// and ArtifactV1 fields.
    pub scalar_types: Vec<QueryScalarType>,
    pub operators: Vec<QueryOperator>,
    pub sortable: bool,
    pub facetable: bool,
    pub aggregatable: bool,
    pub generic_custom: bool,
}

/// Discover built-in and typed projection fields in one visible scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogRequest {
    pub profile: QueryProfile,
    pub scope: QueryScope,
    pub path_prefix: Option<NormalizedRelativePath>,
    pub path_match: CatalogPathMatch,
    pub field_prefix: Option<String>,
    pub include_facets: bool,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogPathMatch {
    Prefix,
    Exact,
}

/// One stable catalog page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogPage {
    pub fields: Vec<CatalogField>,
    pub facets: Vec<FacetResult>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: ReadVersion,
}

/// Filter workspaces by durable commit-head presence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommittedFilter {
    Any,
    Committed,
    Uncommitted,
}

/// Root-wide visible-workspace discovery request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindWorkspacesRequest {
    pub committed: CommittedFilter,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

/// Visible workbench summary with its optional exact durable head.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceDiscovery {
    pub workbench_id: WorkbenchId,
    pub workspace_incarnation_id: WorkspaceIncarnationId,
    pub workspace_revision: WorkspaceRevision,
    pub commit_id: Option<CommitId>,
    pub commit_head_generation: Option<Generation>,
}

/// One stable root-wide workspace page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FindWorkspacesPage {
    pub workspaces: Vec<WorkspaceDiscovery>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: ReadVersion,
}

/// Change-event page request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangePageRequest {
    pub scope: QueryScope,
    pub after_commit_version: Option<CommitVersion>,
    pub cursor: Option<Vec<u8>>,
    pub limit: usize,
}

/// Canonical event together with its ordered durable position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeEvent {
    pub workbench_id: WorkbenchId,
    pub commit_version: CommitVersion,
    pub sequence: u32,
    pub event: ChangeEventRecord,
}

/// One append-only change-event page. Its cursor is a durable event position
/// and may resume against a later root read version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangePage {
    pub events: Vec<ChangeEvent>,
    pub next_cursor: Option<Vec<u8>>,
    pub read_version: ReadVersion,
}

/// Metadata-query failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryError {
    Namespace(NamespaceError),
    InvalidLimit {
        requested: usize,
        max: usize,
    },
    BoundExceeded {
        field: &'static str,
        count: usize,
        max: usize,
    },
    InvalidPredicate {
        field_id: QueryFieldId,
        reason: &'static str,
    },
    UnknownField {
        field_id: QueryFieldId,
    },
    FieldTypeConflict {
        field_id: QueryFieldId,
        left: QueryScalarType,
        right: QueryScalarType,
    },
    InvalidAggregate {
        result_id: QueryFieldId,
        reason: &'static str,
    },
    AggregateOverflow {
        result_id: QueryFieldId,
    },
    InvalidFieldPrefix {
        reason: String,
    },
    InvalidPresentationPathRoot {
        reason: String,
    },
    ExactCatalogRequiresPath,
    CursorTooLarge {
        length: usize,
        max: usize,
    },
    InvalidCursor {
        reason: &'static str,
    },
    CursorQueryMismatch,
    CursorReadVersionMismatch {
        cursor: u64,
        request: u64,
    },
    CursorAnchorMissing,
    CorruptKey {
        family: &'static str,
        reason: String,
    },
    Projection {
        source: QueryRecordError,
    },
    WorkspaceCodec {
        source: PublicationRecordCodecError,
    },
    PathCodec {
        source: PublicationRecordCodecError,
    },
    CommitHeadCodec {
        source: CommitRecordError,
    },
    GenericIndexCodec {
        source: GenericIndexRecordError,
    },
    GenericIndexClosureMismatch {
        reason: &'static str,
    },
    GenericIndexCapabilityConflict {
        field_id: QueryFieldId,
    },
    Meta(MetaError),
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Namespace(source) => source.fmt(formatter),
            Self::InvalidLimit { requested, max } => {
                write!(
                    formatter,
                    "query page limit {requested} is outside 1..={max}"
                )
            }
            Self::BoundExceeded { field, count, max } => {
                write!(formatter, "{field} has {count} items, maximum is {max}")
            }
            Self::InvalidPredicate { field_id, reason } => {
                write!(formatter, "invalid predicate for {field_id}: {reason}")
            }
            Self::UnknownField { field_id } => write!(formatter, "unknown query field {field_id}"),
            Self::FieldTypeConflict {
                field_id,
                left,
                right,
            } => write!(
                formatter,
                "query field {field_id} has conflicting types {left:?} and {right:?}"
            ),
            Self::InvalidAggregate { result_id, reason } => {
                write!(formatter, "invalid aggregate {result_id}: {reason}")
            }
            Self::AggregateOverflow { result_id } => {
                write!(
                    formatter,
                    "aggregate {result_id} overflowed its scalar type"
                )
            }
            Self::InvalidFieldPrefix { reason } => {
                write!(formatter, "invalid catalog field prefix: {reason}")
            }
            Self::InvalidPresentationPathRoot { reason } => {
                write!(formatter, "invalid presentation path root: {reason}")
            }
            Self::ExactCatalogRequiresPath => {
                formatter.write_str("exact catalog matching requires a path prefix")
            }
            Self::CursorTooLarge { length, max } => {
                write!(
                    formatter,
                    "query cursor is {length} bytes, maximum is {max}"
                )
            }
            Self::InvalidCursor { reason } => write!(formatter, "invalid query cursor: {reason}"),
            Self::CursorQueryMismatch => {
                formatter.write_str("query cursor belongs to a different request")
            }
            Self::CursorReadVersionMismatch { cursor, request } => write!(
                formatter,
                "query cursor read version {cursor} does not match request version {request}"
            ),
            Self::CursorAnchorMissing => {
                formatter.write_str("query cursor anchor is absent at the fenced read version")
            }
            Self::CorruptKey { family, reason } => {
                write!(formatter, "corrupt {family} key: {reason}")
            }
            Self::Projection { source } => write!(formatter, "invalid typed projection: {source}"),
            Self::WorkspaceCodec { source } => {
                write!(formatter, "invalid WorkspaceCurrent payload: {source}")
            }
            Self::PathCodec { source } => {
                write!(formatter, "invalid PathCurrent payload: {source}")
            }
            Self::CommitHeadCodec { source } => {
                write!(formatter, "invalid WorkbenchCommitHead payload: {source}")
            }
            Self::GenericIndexCodec { source } => {
                write!(formatter, "invalid Generic index payload: {source}")
            }
            Self::GenericIndexClosureMismatch { reason } => {
                write!(formatter, "Generic index closure mismatch: {reason}")
            }
            Self::GenericIndexCapabilityConflict { field_id } => write!(
                formatter,
                "Generic index capability for {field_id} conflicts across the query scope"
            ),
            Self::Meta(source) => source.fmt(formatter),
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Namespace(source) => Some(source),
            Self::Projection { source } => Some(source),
            Self::WorkspaceCodec { source } | Self::PathCodec { source } => Some(source),
            Self::CommitHeadCodec { source } => Some(source),
            Self::GenericIndexCodec { source } => Some(source),
            Self::Meta(source) => Some(source),
            _ => None,
        }
    }
}

impl From<MetaError> for QueryError {
    fn from(source: MetaError) -> Self {
        Self::Meta(source)
    }
}

impl From<NamespaceError> for QueryError {
    fn from(source: NamespaceError) -> Self {
        Self::Namespace(source)
    }
}

impl From<QueryRecordError> for QueryError {
    fn from(source: QueryRecordError) -> Self {
        Self::Projection { source }
    }
}

impl From<GenericIndexRecordError> for QueryError {
    fn from(source: GenericIndexRecordError) -> Self {
        Self::GenericIndexCodec { source }
    }
}

#[derive(Clone)]
struct MaterializedRow {
    workbench_id: WorkbenchId,
    path: NormalizedRelativePath,
    entry: PathEntry,
    projection: TypedProjection,
}

trait QueryRow {
    const SORT_MISSING_LAST: bool = false;

    fn filter_value(&self, field: &QueryFieldId) -> Option<QueryScalar>;
    fn filter_values(&self, field: &QueryFieldId) -> Vec<QueryScalar> {
        self.filter_value(field).into_iter().collect()
    }
    fn aggregate_value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        self.filter_value(field)
    }
    fn projection_value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        self.filter_value(field)
    }
    fn sort_value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        self.projection_value(field)
    }
    fn query_identity(&self) -> Vec<u8>;
    fn stable_cmp(&self, other: &Self) -> Ordering;
}

impl MaterializedRow {
    fn value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        let value = match field.as_str() {
            "workbench_id" => QueryScalar::String(self.workbench_id.as_str().to_owned()),
            "path" => QueryScalar::String(self.path.as_str().to_owned()),
            "generation" => QueryScalar::Unsigned(self.entry.generation.get()),
            "logical_size" => QueryScalar::Unsigned(self.entry.logical_size),
            "body_digest_uri" => QueryScalar::String(self.entry.body_digest_uri.clone()),
            "content_type" => QueryScalar::String(self.entry.content_type.clone()),
            "producer" => self
                .entry
                .producer
                .clone()
                .map_or(QueryScalar::Null, QueryScalar::String),
            "manifest_id" => self
                .entry
                .manifest_id
                .clone()
                .map_or(QueryScalar::Null, QueryScalar::String),
            _ => return self.projection.get(field).cloned(),
        };
        Some(value)
    }

    fn identity(&self) -> Vec<u8> {
        encode_row_identity(&self.workbench_id, &self.path)
    }
}

impl QueryRow for MaterializedRow {
    fn filter_value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        self.value(field)
    }

    fn query_identity(&self) -> Vec<u8> {
        self.identity()
    }

    fn stable_cmp(&self, other: &Self) -> Ordering {
        self.workbench_id
            .cmp(&other.workbench_id)
            .then_with(|| self.path.cmp(&other.path))
    }
}

#[derive(Clone)]
struct GenericMaterializedRow {
    presentation_path_root: Arc<str>,
    workbench_id: WorkbenchId,
    relative_path: Option<NormalizedRelativePath>,
    kind: GenericNamespaceKind,
    entry: Option<PathEntry>,
    _projection: TypedProjection,
    /// Values from the exact durable Generic index registered for the query
    /// root. Field order is canonical in the map; value order is the caller's
    /// durable registration order and may contain duplicates.
    custom_values: BTreeMap<QueryFieldId, Vec<QueryScalar>>,
}

impl GenericMaterializedRow {
    fn canonical_path(&self) -> String {
        self.relative_path.as_ref().map_or_else(
            || format!("{}/{}", self.presentation_path_root, self.workbench_id),
            |path| {
                format!(
                    "{}/{}/{}",
                    self.presentation_path_root, self.workbench_id, path
                )
            },
        )
    }

    fn name(&self) -> String {
        self.relative_path
            .as_ref()
            .and_then(|path| path.components().last())
            .map_or_else(|| self.workbench_id.as_str().to_owned(), ToOwned::to_owned)
    }
}

impl QueryRow for GenericMaterializedRow {
    const SORT_MISSING_LAST: bool = true;

    fn filter_value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        let builtin = match field.as_str() {
            "path" => Some(QueryScalar::String(self.canonical_path())),
            "name" => Some(QueryScalar::String(self.name())),
            "kind" => Some(QueryScalar::String(
                match self.kind {
                    GenericNamespaceKind::Directory => "directory",
                    GenericNamespaceKind::Artifact => "file",
                }
                .to_owned(),
            )),
            "size_bytes" => Some(QueryScalar::Unsigned(
                self.entry.as_ref().map_or(0, |entry| entry.logical_size),
            )),
            "body.content_type" => self
                .entry
                .as_ref()
                .map(|entry| QueryScalar::String(entry.content_type.clone())),
            "body.producer" => self
                .entry
                .as_ref()
                .and_then(|entry| entry.producer.clone())
                .map(QueryScalar::String),
            "body.manifest_id" => self
                .entry
                .as_ref()
                .and_then(|entry| entry.manifest_id.clone())
                .map(QueryScalar::String),
            _ => None,
        };
        if builtin.is_some() {
            return builtin;
        }
        self.custom_values
            .get(field)
            .and_then(|values| values.first())
            .cloned()
    }

    fn filter_values(&self, field: &QueryFieldId) -> Vec<QueryScalar> {
        if is_generic_builtin_field(field) {
            return self.filter_value(field).into_iter().collect();
        }
        self.custom_values.get(field).cloned().unwrap_or_default()
    }

    fn projection_value(&self, field: &QueryFieldId) -> Option<QueryScalar> {
        if self.kind == GenericNamespaceKind::Directory && field.as_str() == "size_bytes" {
            None
        } else {
            self.filter_value(field)
        }
    }

    fn query_identity(&self) -> Vec<u8> {
        let mut encoded = Vec::new();
        hash_cursor_bytes(&mut encoded, self.workbench_id.as_bytes());
        match &self.relative_path {
            None => encoded.push(0),
            Some(path) => {
                encoded.push(1);
                hash_cursor_bytes(&mut encoded, path.as_str().as_bytes());
            }
        }
        encoded
    }

    fn stable_cmp(&self, other: &Self) -> Ordering {
        self.workbench_id
            .cmp(&other.workbench_id)
            .then_with(|| self.relative_path.cmp(&other.relative_path))
    }
}

struct CollectedRows {
    rows: Vec<MaterializedRow>,
    scope_visible: bool,
}

struct GenericCollectedRows {
    rows: Vec<GenericMaterializedRow>,
    scope_visible: bool,
    exact_kind: Option<GenericNamespaceKind>,
    custom_capabilities: BTreeMap<QueryFieldId, GenericIndexFieldCapability>,
    custom_scalar_types: BTreeMap<QueryFieldId, BTreeSet<QueryScalarType>>,
    index_commitment: [u8; SHA256_BYTES],
}

/// Execute one metadata-only search at the exact fenced read version.
pub fn search_paths_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &SearchRequest,
) -> Result<SearchPage, QueryError> {
    validate_page(request.limit)?;
    validate_predicates(&request.predicates)?;
    validate_bound(
        "projection",
        request.projection.len(),
        MAX_QUERY_PROJECTION_FIELDS,
    )?;
    validate_bound("sort", request.sort.len(), MAX_QUERY_SORT_FIELDS)?;
    validate_bound("facets", request.facets.len(), MAX_QUERY_FACET_FIELDS)?;
    require_unique_fields("projection", &request.projection)?;
    require_unique_fields("facets", &request.facets)?;

    if request.profile.is_generic_namespace() {
        return search_generic_namespace_at(store, context, request);
    }

    let collected = collect_rows(
        store,
        context,
        &request.scope,
        request.path_prefix.as_ref(),
        CatalogPathMatch::Prefix,
    )?;
    let catalog = build_catalog(&collected)?;
    validate_query_fields(&catalog, request)?;

    let mut matching = collected
        .rows
        .into_iter()
        .filter(|row| predicates_match(row, &request.predicates, false))
        .collect::<Vec<_>>();
    let match_count = u64::try_from(matching.len()).map_err(|_| QueryError::BoundExceeded {
        field: "match_count",
        count: matching.len(),
        max: u64::MAX as usize,
    })?;
    matching.sort_by(|left, right| compare_rows(left, right, &request.sort));

    let facets = build_facets(&matching, &request.facets, MAX_FACET_BUCKETS_PER_FIELD)?;
    let digest = search_digest(context.root_id, request);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Search,
        context.read_version,
        digest,
        matching.iter().map(MaterializedRow::identity),
    )?;
    let end = start.saturating_add(request.limit).min(matching.len());
    let hits = matching[start..end]
        .iter()
        .map(|row| SearchHit {
            workbench_id: row.workbench_id.clone(),
            path: row.path.clone(),
            generation: row.entry.generation,
            body_digest_uri: row.entry.body_digest_uri.clone(),
            logical_size: row.entry.logical_size,
            content_type: row.entry.content_type.clone(),
            producer: row.entry.producer.clone(),
            manifest_id: row.entry.manifest_id.clone(),
            projection: request
                .projection
                .iter()
                .filter_map(|field| {
                    row.projection_value(field)
                        .map(|value| (field.clone(), value))
                })
                .collect(),
        })
        .collect();
    let next_cursor = (end < matching.len()).then(|| {
        encode_cursor(
            CursorKind::Search,
            context.read_version,
            digest,
            &matching[end - 1].identity(),
        )
    });
    Ok(SearchPage {
        hits,
        namespace_hits: Vec::new(),
        match_count,
        facets,
        next_cursor,
        read_version: context.read_version,
    })
}

fn search_generic_namespace_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &SearchRequest,
) -> Result<SearchPage, QueryError> {
    let collected = collect_generic_rows(
        store,
        context,
        &request.scope,
        request.path_prefix.as_ref(),
        CatalogPathMatch::Prefix,
        request
            .profile
            .presentation_path_root()
            .expect("generic search has a presentation path root"),
    )?;
    let index_commitment = collected.index_commitment;
    let catalog = build_generic_validation_catalog(&collected)?;
    validate_generic_query_fields(&catalog, request)?;

    let mut matching = collected
        .rows
        .into_iter()
        .filter(|row| predicates_match(row, &request.predicates, true))
        .collect::<Vec<_>>();
    let match_count = checked_count("match_count", matching.len())?;
    matching.sort_by(|left, right| compare_rows(left, right, &request.sort));
    let facets = build_facets(&matching, &request.facets, 10)?;
    let digest =
        bind_generic_index_digest(search_digest(context.root_id, request), index_commitment);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Search,
        context.read_version,
        digest,
        matching.iter().map(QueryRow::query_identity),
    )?;
    let end = start.saturating_add(request.limit).min(matching.len());
    let namespace_hits = matching[start..end]
        .iter()
        .map(|row| GenericNamespaceHit {
            workbench_id: row.workbench_id.clone(),
            relative_path: row.relative_path.clone(),
            kind: row.kind,
            artifact: row.entry.as_ref().map(|entry| GenericArtifactMetadata {
                generation: entry.generation,
                logical_size: entry.logical_size,
                body_digest_uri: entry.body_digest_uri.clone(),
                content_type: entry.content_type.clone(),
                producer: entry.producer.clone(),
                manifest_id: entry.manifest_id.clone(),
            }),
            projection: request
                .projection
                .iter()
                .filter_map(|field| {
                    row.projection_value(field)
                        .map(|value| (field.clone(), value))
                })
                .collect(),
            indexed_values: request
                .projection
                .iter()
                .filter_map(|field| {
                    row.custom_values
                        .get(field)
                        .filter(|values| !values.is_empty())
                        .cloned()
                        .map(|values| (field.clone(), values))
                })
                .collect(),
        })
        .collect();
    let next_cursor = (end < matching.len()).then(|| {
        encode_cursor(
            CursorKind::Search,
            context.read_version,
            digest,
            &matching[end - 1].query_identity(),
        )
    });
    Ok(SearchPage {
        hits: Vec::new(),
        namespace_hits,
        match_count,
        facets,
        next_cursor,
        read_version: context.read_version,
    })
}

/// Execute one bounded metadata aggregate at the exact fenced read version.
pub fn aggregate_paths_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &AggregateRequest,
) -> Result<AggregatePage, QueryError> {
    validate_page(request.limit)?;
    validate_predicates(&request.predicates)?;
    validate_bound("group_by", request.group_by.len(), MAX_QUERY_GROUP_FIELDS)?;
    validate_bound("aggregates", request.aggregates.len(), MAX_QUERY_AGGREGATES)?;
    validate_bound("sort", request.sort.len(), MAX_QUERY_SORT_FIELDS)?;
    if request.aggregates.is_empty() {
        return Err(QueryError::BoundExceeded {
            field: "aggregates",
            count: 0,
            max: MAX_QUERY_AGGREGATES,
        });
    }
    require_unique_fields("group_by", &request.group_by)?;
    validate_aggregates(request)?;

    if request.profile.is_generic_namespace() {
        return aggregate_generic_namespace_at(store, context, request);
    }

    let collected = collect_rows(
        store,
        context,
        &request.scope,
        request.path_prefix.as_ref(),
        CatalogPathMatch::Prefix,
    )?;
    let catalog = build_catalog(&collected)?;
    validate_aggregate_fields(&catalog, request)?;

    let matching = collected
        .rows
        .iter()
        .filter(|row| predicates_match(*row, &request.predicates, false))
        .collect::<Vec<_>>();
    let input_match_count =
        u64::try_from(matching.len()).map_err(|_| QueryError::BoundExceeded {
            field: "aggregate_input_match_count",
            count: matching.len(),
            max: u64::MAX as usize,
        })?;
    let mut builders = BTreeMap::<Vec<QueryScalar>, AggregateBuilder>::new();
    for row in matching {
        let key = request
            .group_by
            .iter()
            .map(|field| row.aggregate_value(field).unwrap_or(QueryScalar::Null))
            .collect::<Vec<_>>();
        let builder = builders
            .entry(key.clone())
            .or_insert_with(|| AggregateBuilder::new(key, &request.aggregates));
        builder.observe(row, &request.aggregates)?;
    }
    if request.group_by.is_empty() && builders.is_empty() {
        builders.insert(
            Vec::new(),
            AggregateBuilder::new(Vec::new(), &request.aggregates),
        );
    }

    let mut groups = builders
        .into_values()
        .map(|builder| builder.finish(request))
        .collect::<Result<Vec<_>, _>>()?;
    groups.sort_by(|left, right| compare_groups(left, right, &request.sort));
    let group_count = u64::try_from(groups.len()).map_err(|_| QueryError::BoundExceeded {
        field: "aggregate_group_count",
        count: groups.len(),
        max: u64::MAX as usize,
    })?;

    let digest = aggregate_digest(context.root_id, request);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Aggregate,
        context.read_version,
        digest,
        groups.iter().map(aggregate_group_identity),
    )?;
    let end = start.saturating_add(request.limit).min(groups.len());
    let next_cursor = (end < groups.len()).then(|| {
        encode_cursor(
            CursorKind::Aggregate,
            context.read_version,
            digest,
            &aggregate_group_identity(&groups[end - 1]),
        )
    });
    Ok(AggregatePage {
        groups: groups[start..end].to_vec(),
        input_match_count,
        row_count: input_match_count,
        group_count,
        next_cursor,
        read_version: context.read_version,
    })
}

fn aggregate_generic_namespace_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &AggregateRequest,
) -> Result<AggregatePage, QueryError> {
    let collected = collect_generic_rows(
        store,
        context,
        &request.scope,
        request.path_prefix.as_ref(),
        CatalogPathMatch::Prefix,
        request
            .profile
            .presentation_path_root()
            .expect("generic aggregate has a presentation path root"),
    )?;
    let index_commitment = collected.index_commitment;
    let catalog = build_generic_validation_catalog(&collected)?;
    validate_generic_aggregate_fields(&catalog, request)?;
    let matching = collected
        .rows
        .iter()
        .filter(|row| predicates_match(*row, &request.predicates, true))
        .collect::<Vec<_>>();
    let input_match_count = checked_count("aggregate_input_match_count", matching.len())?;
    let mut row_count = 0_u64;
    let mut builders = BTreeMap::<Vec<QueryScalar>, GenericAggregateBuilder>::new();
    for row in matching {
        let key = request
            .group_by
            .iter()
            .map(|field| row.aggregate_value(field))
            .collect::<Option<Vec<_>>>();
        let Some(key) = key else {
            continue;
        };
        if request.aggregates.iter().any(|aggregate| {
            aggregate.function != AggregateFunction::Count
                && aggregate
                    .field_id
                    .as_ref()
                    .and_then(|field| row.aggregate_value(field))
                    .and_then(|value| generic_numeric_scalar(&value))
                    .is_none()
        }) {
            continue;
        }
        row_count = row_count.checked_add(1).ok_or(QueryError::BoundExceeded {
            field: "aggregate_row_count",
            count: usize::MAX,
            max: usize::MAX,
        })?;
        let builder = builders
            .entry(key.clone())
            .or_insert_with(|| GenericAggregateBuilder::new(key, &request.aggregates));
        builder.observe(row, &request.aggregates)?;
    }
    let mut groups = builders
        .into_values()
        .map(|builder| builder.finish(request))
        .collect::<Result<Vec<_>, _>>()?;
    groups.sort_by(|left, right| compare_groups(left, right, &request.sort));
    let group_count = checked_count("aggregate_group_count", groups.len())?;
    let digest =
        bind_generic_index_digest(aggregate_digest(context.root_id, request), index_commitment);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Aggregate,
        context.read_version,
        digest,
        groups.iter().map(aggregate_group_identity),
    )?;
    let end = start.saturating_add(request.limit).min(groups.len());
    let next_cursor = (end < groups.len()).then(|| {
        encode_cursor(
            CursorKind::Aggregate,
            context.read_version,
            digest,
            &aggregate_group_identity(&groups[end - 1]),
        )
    });
    Ok(AggregatePage {
        groups: groups[start..end].to_vec(),
        input_match_count,
        row_count,
        group_count,
        next_cursor,
        read_version: context.read_version,
    })
}

/// Discover stable built-in and typed projection fields.
pub fn catalog_fields_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &CatalogRequest,
) -> Result<CatalogPage, QueryError> {
    validate_page(request.limit)?;
    if request.path_match == CatalogPathMatch::Exact && request.path_prefix.is_none() {
        return Err(QueryError::ExactCatalogRequiresPath);
    }
    if let Some(prefix) = &request.field_prefix {
        if prefix.len() > 128 {
            return Err(QueryError::InvalidFieldPrefix {
                reason: "prefix exceeds 128 bytes".to_owned(),
            });
        }
        if let Some((index, byte)) = prefix
            .bytes()
            .enumerate()
            .find(|(_, byte)| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(QueryError::InvalidFieldPrefix {
                reason: format!("unsupported byte 0x{byte:02x} at offset {index}"),
            });
        }
    }

    if request.profile.is_generic_namespace() {
        return catalog_generic_namespace_at(store, context, request);
    }

    let collected = collect_rows(
        store,
        context,
        &request.scope,
        request.path_prefix.as_ref(),
        request.path_match,
    )?;
    let catalog = build_catalog(&collected)?;
    let mut fields = if collected.scope_visible {
        catalog.values().cloned().collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if let Some(prefix) = &request.field_prefix {
        fields.retain(|field| field.field_id.as_str().starts_with(prefix));
    }
    fields.sort_by(|left, right| left.field_id.cmp(&right.field_id));
    let facets = if request.include_facets {
        let facet_fields = fields
            .iter()
            .filter(|field| field.facetable)
            .map(|field| field.field_id.clone())
            .collect::<Vec<_>>();
        build_facets(&collected.rows, &facet_fields, MAX_FACET_BUCKETS_PER_FIELD)?
    } else {
        Vec::new()
    };

    let digest = catalog_digest(context.root_id, request);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Catalog,
        context.read_version,
        digest,
        fields
            .iter()
            .map(|field| field.field_id.as_bytes().to_vec()),
    )?;
    let end = start.saturating_add(request.limit).min(fields.len());
    let next_cursor = (end < fields.len()).then(|| {
        encode_cursor(
            CursorKind::Catalog,
            context.read_version,
            digest,
            fields[end - 1].field_id.as_bytes(),
        )
    });
    Ok(CatalogPage {
        fields: fields[start..end].to_vec(),
        facets,
        next_cursor,
        read_version: context.read_version,
    })
}

fn catalog_generic_namespace_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &CatalogRequest,
) -> Result<CatalogPage, QueryError> {
    let collected = collect_generic_rows(
        store,
        context,
        &request.scope,
        request.path_prefix.as_ref(),
        request.path_match,
        request
            .profile
            .presentation_path_root()
            .expect("generic catalog has a presentation path root"),
    )?;
    let index_commitment = collected.index_commitment;
    let catalog = build_generic_catalog(&collected)?;
    let mut fields = catalog.into_values().collect::<Vec<_>>();
    if let Some(prefix) = &request.field_prefix {
        fields.retain(|field| {
            field.field_id.as_str().starts_with(prefix) || field.field_id.as_str().contains(prefix)
        });
    }
    fields.sort_by(|left, right| left.field_id.cmp(&right.field_id));
    let facets = if request.include_facets {
        let facet_fields = fields
            .iter()
            .filter(|field| field.facetable)
            .map(|field| field.field_id.clone())
            .collect::<Vec<_>>();
        build_facets(&collected.rows, &facet_fields, 10)?
    } else {
        Vec::new()
    };
    let digest =
        bind_generic_index_digest(catalog_digest(context.root_id, request), index_commitment);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Catalog,
        context.read_version,
        digest,
        fields
            .iter()
            .map(|field| field.field_id.as_bytes().to_vec()),
    )?;
    let end = start.saturating_add(request.limit).min(fields.len());
    let next_cursor = (end < fields.len()).then(|| {
        encode_cursor(
            CursorKind::Catalog,
            context.read_version,
            digest,
            fields[end - 1].field_id.as_bytes(),
        )
    });
    Ok(CatalogPage {
        fields: fields[start..end].to_vec(),
        facets,
        next_cursor,
        read_version: context.read_version,
    })
}

/// Discover every visible workbench and its optional durable commit head.
pub fn find_workspaces_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &FindWorkspacesRequest,
) -> Result<FindWorkspacesPage, QueryError> {
    validate_page(request.limit)?;
    let visible = scan_visible_workspaces(store, context)?;
    let heads = scan_commit_heads(store, context)?;
    let mut workspaces = visible
        .into_iter()
        .filter_map(|(workbench_id, workspace)| {
            let head = heads.get(&workspace.incarnation_id).copied();
            let include = match request.committed {
                CommittedFilter::Any => true,
                CommittedFilter::Committed => head.is_some(),
                CommittedFilter::Uncommitted => head.is_none(),
            };
            include.then(|| WorkspaceDiscovery {
                workbench_id,
                workspace_incarnation_id: workspace.incarnation_id,
                workspace_revision: workspace.workspace_revision,
                commit_id: head.map(|head| head.commit_id),
                commit_head_generation: head.map(|head| head.head_generation),
            })
        })
        .collect::<Vec<_>>();
    workspaces.sort_by(|left, right| left.workbench_id.cmp(&right.workbench_id));

    let digest = find_digest(context.root_id, request);
    let start = cursor_start(
        request.cursor.as_deref(),
        CursorKind::Find,
        context.read_version,
        digest,
        workspaces
            .iter()
            .map(|workspace| workspace.workbench_id.as_bytes().to_vec()),
    )?;
    let end = start.saturating_add(request.limit).min(workspaces.len());
    let next_cursor = (end < workspaces.len()).then(|| {
        encode_cursor(
            CursorKind::Find,
            context.read_version,
            digest,
            workspaces[end - 1].workbench_id.as_bytes(),
        )
    });
    Ok(FindWorkspacesPage {
        workspaces: workspaces[start..end].to_vec(),
        next_cursor,
        read_version: context.read_version,
    })
}

/// Resolve one visible workbench and its optional commit head without a scan.
pub fn get_workspace_at(
    store: &MetaShard,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
) -> Result<Option<WorkspaceDiscovery>, QueryError> {
    let Some(workspace) = get_visible_workspace_at(store, context, workbench_id)? else {
        return Ok(None);
    };
    let head = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::WorkbenchCommitHead,
            &workbench_commit_head_key(context.root_id, workspace.incarnation_id),
            context.read_version,
        )?
        .map(|payload| {
            WorkbenchCommitHeadRecord::decode(&payload)
                .map_err(|source| QueryError::CommitHeadCodec { source })
        })
        .transpose()?;
    Ok(Some(WorkspaceDiscovery {
        workbench_id: workbench_id.clone(),
        workspace_incarnation_id: workspace.incarnation_id,
        workspace_revision: workspace.workspace_revision,
        commit_id: head.map(|head| head.commit_id),
        commit_head_generation: head.map(|head| head.head_generation),
    }))
}

/// Page strict typed events from an append-only durable position, rechecking
/// workspace visibility at each event's commit version.
pub fn read_changes_at(
    store: &MetaShard,
    context: RootReadContext,
    request: &ChangePageRequest,
) -> Result<ChangePage, QueryError> {
    validate_page(request.limit)?;
    if request
        .after_commit_version
        .is_some_and(|version| version.get() > context.read_version.get())
    {
        return Err(QueryError::Meta(MetaError::ReadVersionInFuture {
            requested: request
                .after_commit_version
                .expect("checked as present")
                .get(),
            current: context.read_version.get(),
        }));
    }
    let prefix = context.root_id.as_bytes();
    let digest = change_digest(context.root_id, request);
    let mut visible_cache = None;
    let mut marker = match request.cursor.as_deref() {
        Some(cursor) => {
            let (commit_version, sequence) =
                decode_change_cursor(cursor, context, digest, request.after_commit_version)?;
            let marker = change_event_key(context.root_id, commit_version, sequence);
            let payload = store
                .read_change_event_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    &marker,
                    context.read_version,
                )?
                .ok_or(QueryError::CursorAnchorMissing)?;
            let anchor = visible_change_event(
                store,
                context,
                &request.scope,
                MetadataScanItem {
                    key: marker.clone(),
                    value: payload,
                },
                &mut visible_cache,
            )?;
            if anchor.is_none() {
                return Err(QueryError::CursorAnchorMissing);
            }
            Some(marker)
        }
        None => request
            .after_commit_version
            .map(|version| change_event_key(context.root_id, version, u32::MAX)),
    };

    let mut events = Vec::with_capacity(request.limit.saturating_add(1));
    'scan: loop {
        let batch = store.scan_change_events_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            prefix,
            context.read_version,
            marker.as_deref(),
            META_SCAN_BATCH,
        )?;
        if batch.is_empty() {
            break;
        }
        let short = batch.len() < META_SCAN_BATCH;
        for item in batch {
            marker = Some(item.key.clone());
            if let Some(event) =
                visible_change_event(store, context, &request.scope, item, &mut visible_cache)?
            {
                events.push(event);
                if events.len() > request.limit {
                    break 'scan;
                }
            }
        }
        if short {
            break;
        }
    }

    let has_more = events.len() > request.limit;
    if has_more {
        events.truncate(request.limit);
    }
    let next_cursor = has_more.then(|| {
        encode_cursor(
            CursorKind::Changes,
            context.read_version,
            digest,
            &change_event_identity(
                events
                    .last()
                    .expect("a change page with lookahead returns one event"),
            ),
        )
    });
    Ok(ChangePage {
        events,
        next_cursor,
        read_version: context.read_version,
    })
}

fn visible_change_event(
    store: &MetaShard,
    context: RootReadContext,
    scope: &QueryScope,
    item: MetadataScanItem,
    visible_cache: &mut Option<(
        CommitVersion,
        BTreeMap<WorkbenchId, Option<WorkspaceIncarnationId>>,
    )>,
) -> Result<Option<ChangeEvent>, QueryError> {
    let (commit_version, sequence) = decode_change_event_key(context.root_id, &item.key)?;
    let event = ChangeEventRecord::decode(&item.value)?;
    if matches!(scope, QueryScope::Workspace(expected) if expected != &event.workbench_id) {
        return Ok(None);
    }
    if visible_cache
        .as_ref()
        .is_none_or(|(cached_version, _)| *cached_version != commit_version)
    {
        *visible_cache = Some((commit_version, BTreeMap::new()));
    }
    let visible = &mut visible_cache
        .as_mut()
        .expect("event visibility was cached for the current commit")
        .1;
    if !visible.contains_key(&event.workbench_id) {
        let event_context = RootReadContext {
            read_version: ReadVersion::new(commit_version.get())
                .expect("commit version is a readable version"),
            ..context
        };
        let incarnation = get_visible_workspace_at(store, event_context, &event.workbench_id)?
            .map(|marker| marker.incarnation_id);
        visible.insert(event.workbench_id.clone(), incarnation);
    }
    if visible.get(&event.workbench_id).copied().flatten() != Some(event.workspace_incarnation_id) {
        return Ok(None);
    }
    let workbench_id = event.workbench_id.clone();
    debug_assert_eq!(
        visible_cache
            .as_ref()
            .expect("event visibility was cached for the current commit")
            .0,
        commit_version
    );
    Ok(Some(ChangeEvent {
        workbench_id,
        commit_version,
        sequence,
        event,
    }))
}

fn collect_rows(
    store: &MetaShard,
    context: RootReadContext,
    scope: &QueryScope,
    path_prefix: Option<&NormalizedRelativePath>,
    path_match: CatalogPathMatch,
) -> Result<CollectedRows, QueryError> {
    let visible = visible_workspaces_for_scope(store, context, scope)?;
    let scope_visible = !visible.is_empty();
    let mut rows = Vec::new();
    for (workbench_id, workspace) in visible {
        let prefix = path_child_prefix(context.root_id, workspace.incarnation_id, None);
        let items = scan_all_prefix(store, context, MetadataFamily::PathCurrent, &prefix)?;
        for item in items {
            let path =
                decode_path_current_key(context.root_id, workspace.incarnation_id, &item.key)
                    .ok_or_else(|| QueryError::CorruptKey {
                        family: "PathCurrent",
                        reason: "key does not use the canonical root/incarnation/path encoding"
                            .to_owned(),
                    })?;
            if path_prefix.is_some_and(|prefix| match path_match {
                CatalogPathMatch::Prefix => !path_is_at_or_below(&path, prefix),
                CatalogPathMatch::Exact => path != *prefix,
            }) {
                continue;
            }
            let entry = PathEntry::decode(&item.value)
                .map_err(|source| QueryError::PathCodec { source })?;
            let projection = TypedProjection::decode_stored(&entry.typed_index_projection)?;
            rows.push(MaterializedRow {
                workbench_id: workbench_id.clone(),
                path,
                entry,
                projection,
            });
        }
    }
    Ok(CollectedRows {
        rows,
        scope_visible,
    })
}

fn visible_workspaces_for_scope(
    store: &MetaShard,
    context: RootReadContext,
    scope: &QueryScope,
) -> Result<Vec<(WorkbenchId, WorkspaceRecord)>, QueryError> {
    Ok(match scope {
        QueryScope::Root => scan_visible_workspaces(store, context)?,
        QueryScope::Workspace(workbench_id) => {
            get_visible_workspace_at(store, context, workbench_id)
                .map_err(|error| match error {
                    super::namespace::NamespaceError::Meta(source) => QueryError::Meta(source),
                    super::namespace::NamespaceError::Codec { source, .. } => {
                        QueryError::WorkspaceCodec { source }
                    }
                    other => QueryError::CorruptKey {
                        family: "WorkspaceCurrent",
                        reason: other.to_string(),
                    },
                })?
                .map(|workspace| vec![(workbench_id.clone(), workspace)])
                .unwrap_or_default()
        }
    })
}

const GENERIC_WORKBENCH_SECTIONS: [&str; 5] = ["input", "scripts", "outputs", "logs", "metadata"];

fn collect_generic_rows(
    store: &MetaShard,
    context: RootReadContext,
    scope: &QueryScope,
    path_prefix: Option<&NormalizedRelativePath>,
    path_match: CatalogPathMatch,
    presentation_path_root: &PresentationPathRoot,
) -> Result<GenericCollectedRows, QueryError> {
    let presentation_path_root = Arc::<str>::from(presentation_path_root.as_str());
    let visible = visible_workspaces_for_scope(store, context, scope)?;
    let workspace_scope_visible = !visible.is_empty();
    let mut materialized =
        BTreeMap::<(WorkbenchId, Option<NormalizedRelativePath>), GenericMaterializedRow>::new();
    let mut custom_capabilities = BTreeMap::new();
    let mut custom_scalar_types = BTreeMap::<QueryFieldId, BTreeSet<QueryScalarType>>::new();
    let mut index_commitment = Sha256::new();
    index_commitment.update(b"nokv.query.generic-index-commitment.v1\0");
    for (workbench_id, workspace) in visible {
        materialized.insert(
            (workbench_id.clone(), None),
            generic_directory_row(presentation_path_root.clone(), workbench_id.clone(), None),
        );
        for section in GENERIC_WORKBENCH_SECTIONS {
            let section = NormalizedRelativePath::new(section)
                .expect("generic Workbench section paths are canonical");
            materialized.insert(
                (workbench_id.clone(), Some(section.clone())),
                generic_directory_row(
                    presentation_path_root.clone(),
                    workbench_id.clone(),
                    Some(section),
                ),
            );
        }
        let prefix = path_child_prefix(context.root_id, workspace.incarnation_id, None);
        let items = scan_all_prefix(store, context, MetadataFamily::PathCurrent, &prefix)?;
        for item in items {
            let path =
                decode_path_current_key(context.root_id, workspace.incarnation_id, &item.key)
                    .ok_or_else(|| QueryError::CorruptKey {
                        family: "PathCurrent",
                        reason: "key does not use the canonical root/incarnation/path encoding"
                            .to_owned(),
                    })?;
            for parent in proper_parent_paths(&path) {
                materialized
                    .entry((workbench_id.clone(), Some(parent.clone())))
                    .or_insert_with(|| {
                        generic_directory_row(
                            presentation_path_root.clone(),
                            workbench_id.clone(),
                            Some(parent),
                        )
                    });
            }
            let entry = PathEntry::decode(&item.value)
                .map_err(|source| QueryError::PathCodec { source })?;
            let projection = TypedProjection::decode_stored(&entry.typed_index_projection)?;
            materialized.insert(
                (workbench_id.clone(), Some(path.clone())),
                GenericMaterializedRow {
                    presentation_path_root: presentation_path_root.clone(),
                    workbench_id: workbench_id.clone(),
                    relative_path: Some(path),
                    kind: GenericNamespaceKind::Artifact,
                    entry: Some(entry),
                    _projection: projection,
                    custom_values: BTreeMap::new(),
                },
            );
        }
        load_generic_index_for_workspace(
            store,
            context,
            &workbench_id,
            workspace.incarnation_id,
            path_prefix,
            &mut materialized,
            &mut custom_capabilities,
            &mut custom_scalar_types,
            &mut index_commitment,
        )?;
    }
    let exact_kind = match (scope, path_prefix) {
        (QueryScope::Workspace(workbench_id), Some(path)) => materialized
            .get(&(workbench_id.clone(), Some(path.clone())))
            .map(|row| row.kind),
        _ => None,
    };
    let scope_visible = match (scope, path_prefix) {
        (QueryScope::Root, None) => true,
        (QueryScope::Workspace(_), None) => workspace_scope_visible,
        (_, Some(_)) => exact_kind.is_some(),
    };
    let rows = materialized
        .into_values()
        .filter(|row| match (scope, path_prefix, path_match) {
            (QueryScope::Root, None, _) => true,
            (QueryScope::Workspace(_), None, _) => row.relative_path.is_some(),
            (_, Some(prefix), CatalogPathMatch::Prefix) => row
                .relative_path
                .as_ref()
                .is_some_and(|path| path_is_strictly_below(path, prefix)),
            (_, Some(prefix), CatalogPathMatch::Exact) => {
                exact_kind == Some(GenericNamespaceKind::Directory)
                    && row
                        .relative_path
                        .as_ref()
                        .is_some_and(|path| path_is_strictly_below(path, prefix))
            }
        })
        .collect();
    Ok(GenericCollectedRows {
        rows,
        scope_visible,
        exact_kind,
        custom_capabilities,
        custom_scalar_types,
        index_commitment: index_commitment.finalize().into(),
    })
}

#[allow(clippy::too_many_arguments)]
fn load_generic_index_for_workspace(
    store: &MetaShard,
    context: RootReadContext,
    workbench_id: &WorkbenchId,
    workspace_incarnation_id: WorkspaceIncarnationId,
    index_path: Option<&NormalizedRelativePath>,
    materialized: &mut BTreeMap<
        (WorkbenchId, Option<NormalizedRelativePath>),
        GenericMaterializedRow,
    >,
    capabilities: &mut BTreeMap<QueryFieldId, GenericIndexFieldCapability>,
    scalar_types: &mut BTreeMap<QueryFieldId, BTreeSet<QueryScalarType>>,
    commitment: &mut Sha256,
) -> Result<(), QueryError> {
    hash_bytes(commitment, workbench_id.as_bytes());
    let current_key =
        generic_index_current_key(context.root_id, workspace_incarnation_id, index_path);
    let Some(current_payload) = store.read_at(
        context.root_id,
        context.placement_generation,
        context.owner_epoch,
        MetadataFamily::GenericIndexCurrent,
        &current_key,
        context.read_version,
    )?
    else {
        commitment.update([0]);
        return Ok(());
    };
    commitment.update([1]);
    hash_bytes(commitment, &current_payload);
    let current = GenericIndexCurrentRecord::decode(&current_payload)?;
    let generation_payload = store
        .read_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            MetadataFamily::GenericIndexGeneration,
            &generic_index_generation_key(context.root_id, current.generation_id),
            context.read_version,
        )?
        .ok_or(QueryError::GenericIndexClosureMismatch {
            reason: "current pointer generation is missing",
        })?;
    let generation =
        super::generic_index_records::GenericIndexGenerationRecord::decode(&generation_payload)?;
    if generation.state != GenericIndexGenerationState::Sealed {
        return Err(QueryError::GenericIndexClosureMismatch {
            reason: "current pointer generation is not sealed",
        });
    }
    verify_generic_index_generation_seal(
        &generation,
        current.capability_digest,
        current.row_count,
        current.row_digest,
    )?;
    for capability in &generation.capabilities {
        match capabilities.get(&capability.field) {
            Some(existing) if existing != capability => {
                return Err(QueryError::GenericIndexCapabilityConflict {
                    field_id: capability.field.clone(),
                });
            }
            Some(_) => {}
            None => {
                capabilities.insert(capability.field.clone(), capability.clone());
            }
        }
    }

    let rows = scan_all_prefix(
        store,
        context,
        MetadataFamily::GenericIndexGeneration,
        &generic_index_row_prefix(context.root_id, current.generation_id),
    )?;
    if u64::try_from(rows.len()).ok() != Some(current.row_count) {
        return Err(QueryError::GenericIndexClosureMismatch {
            reason: "row count does not match current pointer",
        });
    }
    let declared_fields = generation
        .capabilities
        .iter()
        .map(|capability| &capability.field)
        .collect::<BTreeSet<_>>();
    let mut rolling = empty_generic_index_row_digest();
    let mut previous_path: Option<Option<NormalizedRelativePath>> = None;
    for (expected_sequence, item) in rows.into_iter().enumerate() {
        let sequence =
            decode_generic_index_row_key(context.root_id, current.generation_id, &item.key)
                .ok_or_else(|| QueryError::CorruptKey {
                    family: "GenericIndexGeneration(row)",
                    reason: "row key is not the canonical generation/sequence encoding".to_owned(),
                })?;
        if sequence != expected_sequence as u64 {
            return Err(QueryError::GenericIndexClosureMismatch {
                reason: "row sequences are not contiguous from zero",
            });
        }
        let row = GenericIndexRowRecord::decode(&item.value)?;
        if previous_path
            .as_ref()
            .is_some_and(|previous| previous >= &row.relative_path)
        {
            return Err(QueryError::GenericIndexClosureMismatch {
                reason: "row paths are not strictly ordered",
            });
        }
        previous_path = Some(row.relative_path.clone());
        rolling = advance_generic_index_row_rolling_digest(
            rolling,
            generic_index_row_digest(sequence, &row)?,
        );
        for field_values in &row.values {
            if !declared_fields.contains(&field_values.field) {
                return Err(QueryError::GenericIndexClosureMismatch {
                    reason: "row contains a field absent from the sealed catalog",
                });
            }
        }
        let full_path = join_generic_index_path(index_path, row.relative_path.as_ref())?;
        let Some(materialized_row) = materialized.get_mut(&(workbench_id.clone(), full_path))
        else {
            continue;
        };
        let binding_matches = match (
            row.binding,
            materialized_row.kind,
            materialized_row.entry.as_ref(),
        ) {
            (GenericIndexRowBinding::Directory, GenericNamespaceKind::Directory, None) => true,
            (GenericIndexRowBinding::Unbound, _, _) => true,
            (
                GenericIndexRowBinding::Artifact(binding),
                GenericNamespaceKind::Artifact,
                Some(entry),
            ) => {
                binding.artifact_revision_id == entry.artifact_revision_id
                    && binding.path_generation == entry.generation
            }
            _ => false,
        };
        if !binding_matches {
            continue;
        }
        for field_values in &row.values {
            let observed = scalar_types.entry(field_values.field.clone()).or_default();
            for value in &field_values.values {
                observed.insert(value.scalar_type());
            }
        }
        materialized_row.custom_values = row
            .values
            .into_iter()
            .map(|field_values| (field_values.field, field_values.values))
            .collect();
    }
    if rolling != current.row_digest
        || generic_index_capability_digest(&generation.capabilities)? != current.capability_digest
    {
        return Err(QueryError::GenericIndexClosureMismatch {
            reason: "generation closure digest does not match current pointer",
        });
    }
    Ok(())
}

fn join_generic_index_path(
    index_path: Option<&NormalizedRelativePath>,
    row_path: Option<&NormalizedRelativePath>,
) -> Result<Option<NormalizedRelativePath>, QueryError> {
    match (index_path, row_path) {
        (None, None) => Ok(None),
        (Some(path), None) | (None, Some(path)) => Ok(Some(path.clone())),
        (Some(index_path), Some(row_path)) => {
            NormalizedRelativePath::new(format!("{}/{}", index_path.as_str(), row_path.as_str()))
                .map(Some)
                .map_err(|error| QueryError::GenericIndexClosureMismatch {
                    reason: match error {
                        nokv_types::NormalizedRelativePathError::TooLong { .. } => {
                            "joined row path exceeds the maximum byte length"
                        }
                        nokv_types::NormalizedRelativePathError::TooManyComponents { .. } => {
                            "joined row path exceeds the maximum component count"
                        }
                        _ => "joined row path is not canonical",
                    },
                })
        }
    }
}

fn generic_directory_row(
    presentation_path_root: Arc<str>,
    workbench_id: WorkbenchId,
    relative_path: Option<NormalizedRelativePath>,
) -> GenericMaterializedRow {
    GenericMaterializedRow {
        presentation_path_root,
        workbench_id,
        relative_path,
        kind: GenericNamespaceKind::Directory,
        entry: None,
        _projection: TypedProjection::empty(),
        custom_values: BTreeMap::new(),
    }
}

fn proper_parent_paths(path: &NormalizedRelativePath) -> Vec<NormalizedRelativePath> {
    let components = path.components().collect::<Vec<_>>();
    (1..components.len())
        .map(|length| {
            NormalizedRelativePath::new(components[..length].join("/"))
                .expect("a prefix of a canonical relative path remains canonical")
        })
        .collect()
}

fn scan_visible_workspaces(
    store: &MetaShard,
    context: RootReadContext,
) -> Result<Vec<(WorkbenchId, WorkspaceRecord)>, QueryError> {
    let items = scan_all_prefix(
        store,
        context,
        MetadataFamily::WorkspaceCurrent,
        context.root_id.as_bytes(),
    )?;
    let mut visible = Vec::new();
    let mut incarnations = BTreeSet::new();
    for item in items {
        let workbench_id =
            decode_workspace_current_key(context.root_id, &item.key).ok_or_else(|| {
                QueryError::CorruptKey {
                    family: "WorkspaceCurrent",
                    reason: "key is not the canonical root/workbench encoding".to_owned(),
                }
            })?;
        let workspace = WorkspaceRecord::decode(&item.value)
            .map_err(|source| QueryError::WorkspaceCodec { source })?;
        if workspace.state != WorkspaceState::Visible {
            continue;
        }
        if !incarnations.insert(workspace.incarnation_id) {
            return Err(QueryError::CorruptKey {
                family: "WorkspaceCurrent",
                reason: "one visible incarnation is claimed by multiple workbench ids".to_owned(),
            });
        }
        visible.push((workbench_id, workspace));
    }
    Ok(visible)
}

fn scan_commit_heads(
    store: &MetaShard,
    context: RootReadContext,
) -> Result<BTreeMap<WorkspaceIncarnationId, WorkbenchCommitHeadRecord>, QueryError> {
    let items = scan_all_prefix(
        store,
        context,
        MetadataFamily::WorkbenchCommitHead,
        context.root_id.as_bytes(),
    )?;
    let mut heads = BTreeMap::new();
    for item in items {
        if item.key.len() != FIXED_ID_BYTES * 2 || !item.key.starts_with(context.root_id.as_bytes())
        {
            return Err(QueryError::CorruptKey {
                family: "WorkbenchCommitHead",
                reason: "key has a noncanonical root/incarnation width".to_owned(),
            });
        }
        let incarnation = WorkspaceIncarnationId::from_bytes(
            item.key[FIXED_ID_BYTES..]
                .try_into()
                .expect("validated incarnation width"),
        );
        let head = WorkbenchCommitHeadRecord::decode(&item.value)
            .map_err(|source| QueryError::CommitHeadCodec { source })?;
        if heads.insert(incarnation, head).is_some() {
            return Err(QueryError::CorruptKey {
                family: "WorkbenchCommitHead",
                reason: "duplicate incarnation key".to_owned(),
            });
        }
    }
    Ok(heads)
}

fn scan_all_prefix(
    store: &MetaShard,
    context: RootReadContext,
    family: MetadataFamily,
    prefix: &[u8],
) -> Result<Vec<MetadataScanItem>, QueryError> {
    let mut marker = None;
    let mut items = Vec::new();
    loop {
        let batch = store.scan_prefix_at(
            context.root_id,
            context.placement_generation,
            context.owner_epoch,
            family,
            prefix,
            context.read_version,
            marker.as_deref(),
            META_SCAN_BATCH,
        )?;
        if batch.is_empty() {
            break;
        }
        marker = batch.last().map(|item| item.key.clone());
        let short = batch.len() < META_SCAN_BATCH;
        items.extend(batch);
        if short {
            break;
        }
    }
    Ok(items)
}

fn decode_change_event_key(root: RootId, key: &[u8]) -> Result<(CommitVersion, u32), QueryError> {
    decode_change_event_key_bytes(root, key).ok_or_else(|| QueryError::CorruptKey {
        family: "ChangeEvent",
        reason: "key has a noncanonical root/version/sequence encoding".to_owned(),
    })
}

fn path_is_at_or_below(
    candidate: &NormalizedRelativePath,
    prefix: &NormalizedRelativePath,
) -> bool {
    let mut candidate = candidate.components();
    for component in prefix.components() {
        if candidate.next() != Some(component) {
            return false;
        }
    }
    true
}

fn path_is_strictly_below(
    candidate: &NormalizedRelativePath,
    prefix: &NormalizedRelativePath,
) -> bool {
    candidate != prefix && path_is_at_or_below(candidate, prefix)
}

fn build_catalog(
    collected: &CollectedRows,
) -> Result<BTreeMap<QueryFieldId, CatalogField>, QueryError> {
    let mut fields = builtin_catalog();
    for row in &collected.rows {
        for (field_id, value) in row.projection.fields() {
            let scalar_type = value.scalar_type();
            match fields.get(field_id) {
                Some(existing) if existing.scalar_type != scalar_type => {
                    return Err(QueryError::FieldTypeConflict {
                        field_id: field_id.clone(),
                        left: existing.scalar_type,
                        right: scalar_type,
                    });
                }
                Some(_) => {}
                None => {
                    fields.insert(
                        field_id.clone(),
                        catalog_field(field_id.clone(), scalar_type),
                    );
                }
            }
        }
    }
    Ok(fields)
}

fn builtin_catalog() -> BTreeMap<QueryFieldId, CatalogField> {
    [
        ("body_digest_uri", QueryScalarType::String),
        ("content_type", QueryScalarType::String),
        ("generation", QueryScalarType::Unsigned),
        ("logical_size", QueryScalarType::Unsigned),
        ("manifest_id", QueryScalarType::String),
        ("path", QueryScalarType::String),
        ("producer", QueryScalarType::String),
        ("workbench_id", QueryScalarType::String),
    ]
    .into_iter()
    .map(|(id, scalar_type)| {
        let id = QueryFieldId::new(id).expect("built-in query field ids are valid");
        (id.clone(), catalog_field(id, scalar_type))
    })
    .collect()
}

fn build_generic_catalog(
    collected: &GenericCollectedRows,
) -> Result<BTreeMap<QueryFieldId, CatalogField>, QueryError> {
    if !collected.scope_visible || collected.exact_kind == Some(GenericNamespaceKind::Artifact) {
        return Ok(BTreeMap::new());
    }
    let mut fields = generic_builtin_catalog();
    for (field_id, capability) in &collected.custom_capabilities {
        let scalar_types = collected
            .custom_scalar_types
            .get(field_id)
            .map(|types| types.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        // `scalar_type` remains as a compatibility summary for callers that
        // predate mixed Generic values. `scalar_types` is authoritative; an
        // empty vector represents a declared zero-row field.
        let scalar_type = scalar_types
            .first()
            .copied()
            .unwrap_or(QueryScalarType::String);
        fields.insert(
            field_id.clone(),
            CatalogField {
                field_id: field_id.clone(),
                scalar_type,
                scalar_types,
                operators: capability
                    .operators
                    .iter()
                    .copied()
                    .map(query_operator_from_generic)
                    .collect(),
                sortable: capability.sortable,
                facetable: capability.facetable,
                aggregatable: capability.sortable,
                generic_custom: true,
            },
        );
    }
    Ok(fields)
}

fn build_generic_validation_catalog(
    collected: &GenericCollectedRows,
) -> Result<BTreeMap<QueryFieldId, CatalogField>, QueryError> {
    let mut fields = build_generic_catalog(collected)?;
    for (field_id, field) in generic_builtin_catalog() {
        fields.entry(field_id).or_insert(field);
    }
    Ok(fields)
}

fn generic_builtin_catalog() -> BTreeMap<QueryFieldId, CatalogField> {
    [
        generic_catalog_field("path", QueryScalarType::String, true, false, false),
        generic_catalog_field("name", QueryScalarType::String, true, false, false),
        generic_catalog_field("kind", QueryScalarType::String, false, true, false),
        generic_catalog_field("size_bytes", QueryScalarType::Unsigned, true, false, true),
        generic_catalog_field(
            "body.content_type",
            QueryScalarType::String,
            false,
            true,
            false,
        ),
        generic_catalog_field("body.producer", QueryScalarType::String, false, true, false),
        generic_catalog_field(
            "body.manifest_id",
            QueryScalarType::String,
            false,
            false,
            false,
        ),
    ]
    .into_iter()
    .map(|field| (field.field_id.clone(), field))
    .collect()
}

fn is_generic_builtin_field(field: &QueryFieldId) -> bool {
    matches!(
        field.as_str(),
        "path"
            | "name"
            | "kind"
            | "size_bytes"
            | "body.content_type"
            | "body.producer"
            | "body.manifest_id"
    )
}

fn generic_catalog_field(
    field_id: &'static str,
    scalar_type: QueryScalarType,
    sortable: bool,
    facetable: bool,
    aggregatable: bool,
) -> CatalogField {
    let operators = if scalar_type == QueryScalarType::String {
        vec![
            QueryOperator::Equal,
            QueryOperator::NotEqual,
            QueryOperator::In,
            QueryOperator::Prefix,
            QueryOperator::Suffix,
            QueryOperator::Contains,
            QueryOperator::Exists,
            QueryOperator::NotExists,
        ]
    } else {
        vec![
            QueryOperator::Equal,
            QueryOperator::NotEqual,
            QueryOperator::In,
            QueryOperator::Greater,
            QueryOperator::GreaterOrEqual,
            QueryOperator::Less,
            QueryOperator::LessOrEqual,
            QueryOperator::Exists,
            QueryOperator::NotExists,
        ]
    };
    CatalogField {
        field_id: QueryFieldId::new(field_id).expect("generic built-in field ids are canonical"),
        scalar_type,
        scalar_types: vec![scalar_type],
        operators,
        sortable,
        facetable,
        aggregatable,
        generic_custom: false,
    }
}

const fn query_operator_from_generic(operator: GenericIndexOperator) -> QueryOperator {
    match operator {
        GenericIndexOperator::Equal => QueryOperator::Equal,
        GenericIndexOperator::NotEqual => QueryOperator::NotEqual,
        GenericIndexOperator::In => QueryOperator::In,
        GenericIndexOperator::Prefix => QueryOperator::Prefix,
        GenericIndexOperator::Suffix => QueryOperator::Suffix,
        GenericIndexOperator::Contains => QueryOperator::Contains,
        GenericIndexOperator::Greater => QueryOperator::Greater,
        GenericIndexOperator::GreaterOrEqual => QueryOperator::GreaterOrEqual,
        GenericIndexOperator::Less => QueryOperator::Less,
        GenericIndexOperator::LessOrEqual => QueryOperator::LessOrEqual,
        GenericIndexOperator::Exists => QueryOperator::Exists,
        GenericIndexOperator::NotExists => QueryOperator::NotExists,
    }
}

fn catalog_field(field_id: QueryFieldId, scalar_type: QueryScalarType) -> CatalogField {
    let mut operators = vec![
        QueryOperator::Equal,
        QueryOperator::NotEqual,
        QueryOperator::In,
        QueryOperator::Less,
        QueryOperator::LessOrEqual,
        QueryOperator::Greater,
        QueryOperator::GreaterOrEqual,
    ];
    if matches!(
        scalar_type,
        QueryScalarType::String | QueryScalarType::Bytes
    ) {
        operators.extend([
            QueryOperator::Prefix,
            QueryOperator::Suffix,
            QueryOperator::Contains,
        ]);
    }
    operators.extend([QueryOperator::Exists, QueryOperator::NotExists]);
    CatalogField {
        field_id,
        scalar_type,
        scalar_types: vec![scalar_type],
        operators,
        sortable: scalar_type != QueryScalarType::Null,
        facetable: true,
        aggregatable: matches!(
            scalar_type,
            QueryScalarType::Signed | QueryScalarType::Unsigned | QueryScalarType::Float
        ),
        generic_custom: false,
    }
}

fn validate_presentation_path_root(value: &str) -> Result<(), QueryError> {
    if value.len() > PresentationPathRoot::MAX_BYTES {
        return Err(QueryError::InvalidPresentationPathRoot {
            reason: format!(
                "root is {} bytes, maximum is {}",
                value.len(),
                PresentationPathRoot::MAX_BYTES
            ),
        });
    }
    let Some(relative) = value.strip_prefix('/') else {
        return Err(QueryError::InvalidPresentationPathRoot {
            reason: "root must be absolute".to_owned(),
        });
    };
    if relative.is_empty() {
        return Err(QueryError::InvalidPresentationPathRoot {
            reason: "root must not be /".to_owned(),
        });
    }
    let mut components = 0_usize;
    for component in relative.split('/') {
        components += 1;
        if components > PresentationPathRoot::MAX_COMPONENTS {
            return Err(QueryError::InvalidPresentationPathRoot {
                reason: format!(
                    "root has {components} components, maximum is {}",
                    PresentationPathRoot::MAX_COMPONENTS
                ),
            });
        }
        if component.is_empty() || matches!(component, "." | "..") {
            return Err(QueryError::InvalidPresentationPathRoot {
                reason: "root must use non-empty components without '.' or '..'".to_owned(),
            });
        }
        if component.contains('\\') || component.contains('\0') {
            return Err(QueryError::InvalidPresentationPathRoot {
                reason: "root must not contain backslashes or NUL".to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_query_fields(
    catalog: &BTreeMap<QueryFieldId, CatalogField>,
    request: &SearchRequest,
) -> Result<(), QueryError> {
    for predicate in &request.predicates {
        validate_predicate_against_catalog(catalog, predicate)?;
    }
    for field in request
        .projection
        .iter()
        .chain(request.facets.iter())
        .chain(request.sort.iter().map(|sort| &sort.field_id))
    {
        if !catalog.contains_key(field) {
            return Err(QueryError::UnknownField {
                field_id: field.clone(),
            });
        }
    }
    Ok(())
}

fn validate_generic_query_fields(
    catalog: &BTreeMap<QueryFieldId, CatalogField>,
    request: &SearchRequest,
) -> Result<(), QueryError> {
    for predicate in &request.predicates {
        validate_predicate_against_generic_catalog(catalog, predicate)?;
    }
    for field in &request.projection {
        if !catalog.contains_key(field) {
            return Err(QueryError::UnknownField {
                field_id: field.clone(),
            });
        }
    }
    for sort in &request.sort {
        let field = catalog
            .get(&sort.field_id)
            .ok_or_else(|| QueryError::UnknownField {
                field_id: sort.field_id.clone(),
            })?;
        if !field.sortable {
            return Err(QueryError::InvalidPredicate {
                field_id: sort.field_id.clone(),
                reason: "field is not sortable",
            });
        }
    }
    for facet in &request.facets {
        let field = catalog.get(facet).ok_or_else(|| QueryError::UnknownField {
            field_id: facet.clone(),
        })?;
        if !field.facetable {
            return Err(QueryError::InvalidPredicate {
                field_id: facet.clone(),
                reason: "field is not facetable",
            });
        }
    }
    Ok(())
}

fn validate_aggregate_fields(
    catalog: &BTreeMap<QueryFieldId, CatalogField>,
    request: &AggregateRequest,
) -> Result<(), QueryError> {
    for predicate in &request.predicates {
        validate_predicate_against_catalog(catalog, predicate)?;
    }
    for field in &request.group_by {
        if !catalog.contains_key(field) {
            return Err(QueryError::UnknownField {
                field_id: field.clone(),
            });
        }
    }
    let result_ids = request
        .aggregates
        .iter()
        .map(|aggregate| aggregate.result_id.clone())
        .collect::<BTreeSet<_>>();
    for aggregate in &request.aggregates {
        if let Some(field) = &aggregate.field_id {
            let catalog_field = catalog.get(field).ok_or_else(|| QueryError::UnknownField {
                field_id: field.clone(),
            })?;
            match aggregate.function {
                AggregateFunction::Sum | AggregateFunction::Average
                    if !catalog_field.aggregatable =>
                {
                    return Err(QueryError::InvalidAggregate {
                        result_id: aggregate.result_id.clone(),
                        reason: "sum and average require a numeric field",
                    });
                }
                AggregateFunction::Minimum | AggregateFunction::Maximum
                    if !catalog_field.sortable =>
                {
                    return Err(QueryError::InvalidAggregate {
                        result_id: aggregate.result_id.clone(),
                        reason: "minimum and maximum require a sortable field",
                    });
                }
                _ => {}
            }
        }
    }
    for sort in &request.sort {
        if !request.group_by.contains(&sort.field_id) && !result_ids.contains(&sort.field_id) {
            return Err(QueryError::UnknownField {
                field_id: sort.field_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_generic_aggregate_fields(
    catalog: &BTreeMap<QueryFieldId, CatalogField>,
    request: &AggregateRequest,
) -> Result<(), QueryError> {
    for predicate in &request.predicates {
        validate_predicate_against_generic_catalog(catalog, predicate)?;
    }
    for field in &request.group_by {
        let catalog_field = catalog.get(field).ok_or_else(|| QueryError::UnknownField {
            field_id: field.clone(),
        })?;
        if catalog_field.generic_custom && !catalog_field.sortable {
            return Err(QueryError::InvalidAggregate {
                result_id: field.clone(),
                reason: "group field is not sortable",
            });
        }
    }
    let result_ids = request
        .aggregates
        .iter()
        .map(|aggregate| aggregate.result_id.clone())
        .collect::<BTreeSet<_>>();
    for aggregate in &request.aggregates {
        if let Some(field_id) = &aggregate.field_id {
            let field = catalog
                .get(field_id)
                .ok_or_else(|| QueryError::UnknownField {
                    field_id: field_id.clone(),
                })?;
            if aggregate.function != AggregateFunction::Count && !field.aggregatable {
                return Err(QueryError::InvalidAggregate {
                    result_id: aggregate.result_id.clone(),
                    reason: "numeric aggregate requires a numeric field",
                });
            }
        }
    }
    for sort in &request.sort {
        if !request.group_by.contains(&sort.field_id) && !result_ids.contains(&sort.field_id) {
            return Err(QueryError::UnknownField {
                field_id: sort.field_id.clone(),
            });
        }
    }
    Ok(())
}

fn validate_predicates(predicates: &[QueryPredicate]) -> Result<(), QueryError> {
    validate_bound("predicates", predicates.len(), MAX_QUERY_PREDICATES)?;
    for predicate in predicates {
        match (&predicate.operator, &predicate.operand) {
            (QueryOperator::Exists | QueryOperator::NotExists, QueryOperand::None) => {}
            (QueryOperator::Exists | QueryOperator::NotExists, _) => {
                return Err(QueryError::InvalidPredicate {
                    field_id: predicate.field_id.clone(),
                    reason: "existence operators require the no-value operand",
                });
            }
            (QueryOperator::In, QueryOperand::Set(values)) => {
                validate_bound("predicate_in_values", values.len(), MAX_QUERY_IN_VALUES)?;
            }
            (QueryOperator::In, _) => {
                return Err(QueryError::InvalidPredicate {
                    field_id: predicate.field_id.clone(),
                    reason: "in requires a canonical set operand",
                });
            }
            (_, QueryOperand::Scalar(_)) => {}
            (_, _) => {
                return Err(QueryError::InvalidPredicate {
                    field_id: predicate.field_id.clone(),
                    reason: "comparison operator requires one scalar operand",
                });
            }
        }
    }
    Ok(())
}

fn validate_predicate_against_catalog(
    catalog: &BTreeMap<QueryFieldId, CatalogField>,
    predicate: &QueryPredicate,
) -> Result<(), QueryError> {
    let field = catalog
        .get(&predicate.field_id)
        .ok_or_else(|| QueryError::UnknownField {
            field_id: predicate.field_id.clone(),
        })?;
    if !field.operators.contains(&predicate.operator) {
        return Err(QueryError::InvalidPredicate {
            field_id: predicate.field_id.clone(),
            reason: "operator is unsupported for this field type",
        });
    }
    let validate_value = |value: &QueryScalar| {
        if value.scalar_type() != field.scalar_type && !matches!(value, QueryScalar::Null) {
            Err(QueryError::InvalidPredicate {
                field_id: predicate.field_id.clone(),
                reason: "comparison value has a different scalar type",
            })
        } else {
            Ok(())
        }
    };
    match &predicate.operand {
        QueryOperand::None => {}
        QueryOperand::Scalar(value) => validate_value(value)?,
        QueryOperand::Set(values) => {
            for value in values {
                validate_value(value)?;
            }
        }
    }
    Ok(())
}

fn validate_predicate_against_generic_catalog(
    catalog: &BTreeMap<QueryFieldId, CatalogField>,
    predicate: &QueryPredicate,
) -> Result<(), QueryError> {
    let field = catalog
        .get(&predicate.field_id)
        .ok_or_else(|| QueryError::UnknownField {
            field_id: predicate.field_id.clone(),
        })?;
    if !field.operators.contains(&predicate.operator) {
        return Err(QueryError::InvalidPredicate {
            field_id: predicate.field_id.clone(),
            reason: "operator is unsupported for this field type",
        });
    }
    if field.generic_custom {
        let validate_value = |value: &QueryScalar| {
            let supported = matches!(
                value,
                QueryScalar::String(_) | QueryScalar::Unsigned(_) | QueryScalar::Float(_)
            );
            let string_operand_required = matches!(
                predicate.operator,
                QueryOperator::Prefix | QueryOperator::Suffix | QueryOperator::Contains
            ) && !matches!(value, QueryScalar::String(_));
            if supported && !string_operand_required {
                Ok(())
            } else {
                Err(QueryError::InvalidPredicate {
                    field_id: predicate.field_id.clone(),
                    reason: "comparison value is not a supported Generic scalar",
                })
            }
        };
        match &predicate.operand {
            QueryOperand::None => {}
            QueryOperand::Scalar(value) => validate_value(value)?,
            QueryOperand::Set(values) => {
                for value in values {
                    validate_value(value)?;
                }
            }
        }
        return Ok(());
    }
    let validate_value = |value: &QueryScalar| {
        let same_type = value.scalar_type() == field.scalar_type;
        let numeric_coercion = numeric_scalar(value).is_some()
            && matches!(
                field.scalar_type,
                QueryScalarType::Signed | QueryScalarType::Unsigned | QueryScalarType::Float
            );
        let size_bytes_numeric_string = predicate.field_id.as_str() == "size_bytes"
            && finite_numeric_string_scalar(value).is_some();
        if value == &QueryScalar::Null
            || (!same_type && !numeric_coercion && !size_bytes_numeric_string)
        {
            Err(QueryError::InvalidPredicate {
                field_id: predicate.field_id.clone(),
                reason: "comparison value has a different scalar type",
            })
        } else {
            Ok(())
        }
    };
    match &predicate.operand {
        QueryOperand::None => {}
        QueryOperand::Scalar(value) => validate_value(value)?,
        QueryOperand::Set(values) => {
            for value in values {
                validate_value(value)?;
            }
        }
    }
    Ok(())
}

fn validate_aggregates(request: &AggregateRequest) -> Result<(), QueryError> {
    let mut result_ids = BTreeSet::new();
    for aggregate in &request.aggregates {
        if !result_ids.insert(aggregate.result_id.clone()) {
            return Err(QueryError::InvalidAggregate {
                result_id: aggregate.result_id.clone(),
                reason: "result ids must be unique",
            });
        }
        if request.group_by.contains(&aggregate.result_id) {
            return Err(QueryError::InvalidAggregate {
                result_id: aggregate.result_id.clone(),
                reason: "result id shadows a group field",
            });
        }
        match (aggregate.function, aggregate.field_id.is_some()) {
            (AggregateFunction::Count, _) => {}
            (_, true) => {}
            (_, false) => {
                return Err(QueryError::InvalidAggregate {
                    result_id: aggregate.result_id.clone(),
                    reason: "aggregate function requires a field",
                });
            }
        }
    }
    Ok(())
}

fn predicates_match<R: QueryRow>(
    row: &R,
    predicates: &[QueryPredicate],
    numeric_coercion: bool,
) -> bool {
    predicates.iter().all(|predicate| {
        let actual = row.filter_values(&predicate.field_id);
        match (&predicate.operator, &predicate.operand) {
            (QueryOperator::Exists, QueryOperand::None) => !actual.is_empty(),
            (QueryOperator::NotExists, QueryOperand::None) => actual.is_empty(),
            (QueryOperator::In, QueryOperand::Set(expected)) => actual.iter().any(|actual| {
                expected.iter().any(|expected| {
                    compare_query_values(actual, expected, numeric_coercion)
                        == Some(Ordering::Equal)
                })
            }),
            (QueryOperator::Equal, QueryOperand::Scalar(expected)) => actual.iter().any(|actual| {
                compare_query_values(actual, expected, numeric_coercion) == Some(Ordering::Equal)
            }),
            (QueryOperator::NotEqual, QueryOperand::Scalar(expected)) => {
                !actual.is_empty()
                    && actual.iter().all(|actual| {
                        compare_query_values(actual, expected, numeric_coercion)
                            != Some(Ordering::Equal)
                    })
            }
            (QueryOperator::Less, QueryOperand::Scalar(expected)) => actual.iter().any(|actual| {
                compare_query_values(actual, expected, numeric_coercion) == Some(Ordering::Less)
            }),
            (QueryOperator::LessOrEqual, QueryOperand::Scalar(expected)) => {
                actual.iter().any(|actual| {
                    matches!(
                        compare_query_values(actual, expected, numeric_coercion),
                        Some(Ordering::Less | Ordering::Equal)
                    )
                })
            }
            (QueryOperator::Greater, QueryOperand::Scalar(expected)) => {
                actual.iter().any(|actual| {
                    compare_query_values(actual, expected, numeric_coercion)
                        == Some(Ordering::Greater)
                })
            }
            (QueryOperator::GreaterOrEqual, QueryOperand::Scalar(expected)) => {
                actual.iter().any(|actual| {
                    matches!(
                        compare_query_values(actual, expected, numeric_coercion),
                        Some(Ordering::Greater | Ordering::Equal)
                    )
                })
            }
            (QueryOperator::Prefix, QueryOperand::Scalar(expected)) => actual
                .iter()
                .any(|actual| string_or_bytes_affix(Some(actual), expected, Affix::Prefix)),
            (QueryOperator::Suffix, QueryOperand::Scalar(expected)) => actual
                .iter()
                .any(|actual| string_or_bytes_affix(Some(actual), expected, Affix::Suffix)),
            (QueryOperator::Contains, QueryOperand::Scalar(expected)) => actual
                .iter()
                .any(|actual| string_or_bytes_contains(Some(actual), expected)),
            _ => false,
        }
    })
}

#[derive(Clone, Copy)]
enum Affix {
    Prefix,
    Suffix,
}

fn string_or_bytes_affix(
    actual: Option<&QueryScalar>,
    expected: &QueryScalar,
    affix: Affix,
) -> bool {
    match (actual, expected) {
        (Some(QueryScalar::String(actual)), QueryScalar::String(expected)) => match affix {
            Affix::Prefix => actual.starts_with(expected),
            Affix::Suffix => actual.ends_with(expected),
        },
        (Some(QueryScalar::Bytes(actual)), QueryScalar::Bytes(expected)) => match affix {
            Affix::Prefix => actual.starts_with(expected),
            Affix::Suffix => actual.ends_with(expected),
        },
        _ => false,
    }
}

fn string_or_bytes_contains(actual: Option<&QueryScalar>, expected: &QueryScalar) -> bool {
    match (actual, expected) {
        (Some(QueryScalar::String(actual)), QueryScalar::String(expected)) => {
            actual.contains(expected)
        }
        (Some(QueryScalar::Bytes(actual)), QueryScalar::Bytes(expected)) => {
            expected.is_empty()
                || actual
                    .windows(expected.len())
                    .any(|window| window == expected.as_slice())
        }
        _ => false,
    }
}

fn compare_query_values(
    left: &QueryScalar,
    right: &QueryScalar,
    numeric_coercion: bool,
) -> Option<Ordering> {
    if numeric_coercion {
        if let (Some(left), Some(right)) = (numeric_scalar(left), numeric_scalar(right)) {
            return Some(left.total_cmp(&right));
        }
        if let (Some(left), Some(right)) =
            (numeric_scalar(left), finite_numeric_string_scalar(right))
        {
            return Some(left.total_cmp(&right));
        }
        if let (Some(left), Some(right)) =
            (finite_numeric_string_scalar(left), numeric_scalar(right))
        {
            return Some(left.total_cmp(&right));
        }
    }
    (left.scalar_type() == right.scalar_type()).then(|| left.cmp(right))
}

fn numeric_scalar(value: &QueryScalar) -> Option<f64> {
    match value {
        QueryScalar::Signed(value) => Some(*value as f64),
        QueryScalar::Unsigned(value) => Some(*value as f64),
        QueryScalar::Float(value) => Some(value.get()),
        _ => None,
    }
}

fn finite_numeric_string_scalar(value: &QueryScalar) -> Option<f64> {
    let QueryScalar::String(value) = value else {
        return None;
    };
    value.parse::<f64>().ok().filter(|value| value.is_finite())
}

fn generic_numeric_scalar(value: &QueryScalar) -> Option<f64> {
    numeric_scalar(value).or_else(|| finite_numeric_string_scalar(value))
}

fn compare_rows<R: QueryRow>(left: &R, right: &R, sort: &[QuerySort]) -> Ordering {
    for field in sort {
        let left_value = left.sort_value(&field.field_id);
        let right_value = right.sort_value(&field.field_id);
        let ordering = match (&left_value, &right_value) {
            (None, Some(_)) if R::SORT_MISSING_LAST => Ordering::Greater,
            (Some(_), None) if R::SORT_MISSING_LAST => Ordering::Less,
            _ => {
                let ordering = compare_optional_values(left_value, right_value);
                match field.direction {
                    QuerySortDirection::Ascending => ordering,
                    QuerySortDirection::Descending => ordering.reverse(),
                }
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.stable_cmp(right)
}

fn compare_optional_values(left: Option<QueryScalar>, right: Option<QueryScalar>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => left.cmp(&right),
    }
}

fn build_facets<R: QueryRow>(
    matching: &[R],
    fields: &[QueryFieldId],
    bucket_limit: usize,
) -> Result<Vec<FacetResult>, QueryError> {
    fields
        .iter()
        .map(|field_id| {
            let mut counts = BTreeMap::<QueryScalar, u64>::new();
            for row in matching {
                for value in row.filter_values(field_id) {
                    let count = counts.entry(value).or_default();
                    *count = count.checked_add(1).ok_or(QueryError::BoundExceeded {
                        field: "facet_count",
                        count: usize::MAX,
                        max: usize::MAX,
                    })?;
                }
            }
            let distinct_count =
                u64::try_from(counts.len()).map_err(|_| QueryError::BoundExceeded {
                    field: "facet_distinct_count",
                    count: counts.len(),
                    max: u64::MAX as usize,
                })?;
            let mut buckets = counts
                .into_iter()
                .map(|(value, count)| FacetBucket { value, count })
                .collect::<Vec<_>>();
            buckets.sort_by(|left, right| {
                right
                    .count
                    .cmp(&left.count)
                    .then_with(|| left.value.cmp(&right.value))
            });
            let truncated = buckets.len() > bucket_limit;
            buckets.truncate(bucket_limit);
            Ok(FacetResult {
                field_id: field_id.clone(),
                buckets,
                distinct_count,
                truncated,
            })
        })
        .collect()
}

#[derive(Clone)]
enum AggregateAccumulator {
    Count(u64),
    Sum(Option<QueryScalar>),
    Average { sum: f64, count: u64 },
    Minimum(Option<QueryScalar>),
    Maximum(Option<QueryScalar>),
}

struct AggregateBuilder {
    key: Vec<QueryScalar>,
    accumulators: Vec<AggregateAccumulator>,
}

impl AggregateBuilder {
    fn new(key: Vec<QueryScalar>, specs: &[AggregateSpec]) -> Self {
        let accumulators = specs
            .iter()
            .map(|spec| match spec.function {
                AggregateFunction::Count => AggregateAccumulator::Count(0),
                AggregateFunction::Sum => AggregateAccumulator::Sum(None),
                AggregateFunction::Average => AggregateAccumulator::Average { sum: 0.0, count: 0 },
                AggregateFunction::Minimum => AggregateAccumulator::Minimum(None),
                AggregateFunction::Maximum => AggregateAccumulator::Maximum(None),
            })
            .collect();
        Self { key, accumulators }
    }

    fn observe<R: QueryRow>(&mut self, row: &R, specs: &[AggregateSpec]) -> Result<(), QueryError> {
        for (accumulator, spec) in self.accumulators.iter_mut().zip(specs) {
            let value = spec
                .field_id
                .as_ref()
                .and_then(|field| row.aggregate_value(field));
            match accumulator {
                AggregateAccumulator::Count(count) => {
                    if spec.field_id.is_none()
                        || value
                            .as_ref()
                            .is_some_and(|value| value != &QueryScalar::Null)
                    {
                        *count =
                            count
                                .checked_add(1)
                                .ok_or_else(|| QueryError::AggregateOverflow {
                                    result_id: spec.result_id.clone(),
                                })?;
                    }
                }
                AggregateAccumulator::Sum(sum) => {
                    if let Some(value) = value.filter(|value| value != &QueryScalar::Null) {
                        *sum = Some(add_scalar(sum.take(), value, &spec.result_id)?);
                    }
                }
                AggregateAccumulator::Average { sum, count } => {
                    if let Some(value) = value.filter(|value| value != &QueryScalar::Null) {
                        let numeric =
                            scalar_as_f64(&value).ok_or_else(|| QueryError::InvalidAggregate {
                                result_id: spec.result_id.clone(),
                                reason: "average requires numeric values",
                            })?;
                        *sum += numeric;
                        *count =
                            count
                                .checked_add(1)
                                .ok_or_else(|| QueryError::AggregateOverflow {
                                    result_id: spec.result_id.clone(),
                                })?;
                    }
                }
                AggregateAccumulator::Minimum(minimum) => {
                    if let Some(value) = value.filter(|value| value != &QueryScalar::Null) {
                        if minimum.as_ref().is_none_or(|current| value < *current) {
                            *minimum = Some(value);
                        }
                    }
                }
                AggregateAccumulator::Maximum(maximum) => {
                    if let Some(value) = value.filter(|value| value != &QueryScalar::Null) {
                        if maximum.as_ref().is_none_or(|current| value > *current) {
                            *maximum = Some(value);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(self, request: &AggregateRequest) -> Result<AggregateGroup, QueryError> {
        let keys = request.group_by.iter().cloned().zip(self.key).collect();
        let values = request
            .aggregates
            .iter()
            .zip(self.accumulators)
            .map(|(spec, accumulator)| {
                let value = match accumulator {
                    AggregateAccumulator::Count(value) => QueryScalar::Unsigned(value),
                    AggregateAccumulator::Sum(value)
                    | AggregateAccumulator::Minimum(value)
                    | AggregateAccumulator::Maximum(value) => value.unwrap_or(QueryScalar::Null),
                    AggregateAccumulator::Average { sum, count } => {
                        if count == 0 {
                            QueryScalar::Null
                        } else {
                            QueryScalar::Float(FiniteFloat::new(sum / count as f64)?)
                        }
                    }
                };
                Ok((spec.result_id.clone(), value))
            })
            .collect::<Result<_, QueryError>>()?;
        Ok(AggregateGroup { keys, values })
    }
}

#[derive(Clone)]
enum GenericAggregateAccumulator {
    Count(u64),
    Numeric {
        function: AggregateFunction,
        count: u64,
        sum: f64,
        minimum: Option<f64>,
        maximum: Option<f64>,
    },
}

struct GenericAggregateBuilder {
    key: Vec<QueryScalar>,
    accumulators: Vec<GenericAggregateAccumulator>,
}

impl GenericAggregateBuilder {
    fn new(key: Vec<QueryScalar>, specs: &[AggregateSpec]) -> Self {
        let accumulators = specs
            .iter()
            .map(|spec| match spec.function {
                AggregateFunction::Count => GenericAggregateAccumulator::Count(0),
                function => GenericAggregateAccumulator::Numeric {
                    function,
                    count: 0,
                    sum: 0.0,
                    minimum: None,
                    maximum: None,
                },
            })
            .collect();
        Self { key, accumulators }
    }

    fn observe(
        &mut self,
        row: &GenericMaterializedRow,
        specs: &[AggregateSpec],
    ) -> Result<(), QueryError> {
        for (accumulator, spec) in self.accumulators.iter_mut().zip(specs) {
            let value = spec
                .field_id
                .as_ref()
                .and_then(|field| row.aggregate_value(field));
            match accumulator {
                GenericAggregateAccumulator::Count(count) => {
                    if spec.field_id.is_none() || value.is_some() {
                        *count =
                            count
                                .checked_add(1)
                                .ok_or_else(|| QueryError::AggregateOverflow {
                                    result_id: spec.result_id.clone(),
                                })?;
                    }
                }
                GenericAggregateAccumulator::Numeric {
                    count,
                    sum,
                    minimum,
                    maximum,
                    ..
                } => {
                    let numeric =
                        value
                            .as_ref()
                            .and_then(generic_numeric_scalar)
                            .ok_or_else(|| QueryError::InvalidAggregate {
                                result_id: spec.result_id.clone(),
                                reason: "numeric aggregate requires a present numeric value",
                            })?;
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| QueryError::AggregateOverflow {
                            result_id: spec.result_id.clone(),
                        })?;
                    *sum += numeric;
                    *minimum = Some(minimum.map_or(numeric, |current| current.min(numeric)));
                    *maximum = Some(maximum.map_or(numeric, |current| current.max(numeric)));
                }
            }
        }
        Ok(())
    }

    fn finish(self, request: &AggregateRequest) -> Result<AggregateGroup, QueryError> {
        let keys = request.group_by.iter().cloned().zip(self.key).collect();
        let values = request
            .aggregates
            .iter()
            .zip(self.accumulators)
            .map(|(spec, accumulator)| {
                let value = match accumulator {
                    GenericAggregateAccumulator::Count(value) => QueryScalar::Unsigned(value),
                    GenericAggregateAccumulator::Numeric {
                        function,
                        count,
                        sum,
                        minimum,
                        maximum,
                    } => {
                        let value = match function {
                            AggregateFunction::Sum => sum,
                            AggregateFunction::Average if count > 0 => sum / count as f64,
                            AggregateFunction::Minimum => minimum.unwrap_or(0.0),
                            AggregateFunction::Maximum => maximum.unwrap_or(0.0),
                            AggregateFunction::Average | AggregateFunction::Count => 0.0,
                        };
                        QueryScalar::Float(FiniteFloat::new(value)?)
                    }
                };
                Ok((spec.result_id.clone(), value))
            })
            .collect::<Result<_, QueryError>>()?;
        Ok(AggregateGroup { keys, values })
    }
}

fn add_scalar(
    current: Option<QueryScalar>,
    value: QueryScalar,
    result_id: &QueryFieldId,
) -> Result<QueryScalar, QueryError> {
    match (current, value) {
        (None, value @ QueryScalar::Signed(_))
        | (None, value @ QueryScalar::Unsigned(_))
        | (None, value @ QueryScalar::Float(_)) => Ok(value),
        (Some(QueryScalar::Signed(left)), QueryScalar::Signed(right)) => left
            .checked_add(right)
            .map(QueryScalar::Signed)
            .ok_or_else(|| QueryError::AggregateOverflow {
                result_id: result_id.clone(),
            }),
        (Some(QueryScalar::Unsigned(left)), QueryScalar::Unsigned(right)) => left
            .checked_add(right)
            .map(QueryScalar::Unsigned)
            .ok_or_else(|| QueryError::AggregateOverflow {
                result_id: result_id.clone(),
            }),
        (Some(QueryScalar::Float(left)), QueryScalar::Float(right)) => Ok(QueryScalar::Float(
            FiniteFloat::new(left.get() + right.get())?,
        )),
        _ => Err(QueryError::InvalidAggregate {
            result_id: result_id.clone(),
            reason: "sum requires one stable numeric scalar type",
        }),
    }
}

fn scalar_as_f64(value: &QueryScalar) -> Option<f64> {
    match value {
        QueryScalar::Signed(value) => Some(*value as f64),
        QueryScalar::Unsigned(value) => Some(*value as f64),
        QueryScalar::Float(value) => Some(value.get()),
        _ => None,
    }
}

fn compare_groups(left: &AggregateGroup, right: &AggregateGroup, sort: &[QuerySort]) -> Ordering {
    for field in sort {
        let left_value = left
            .keys
            .get(&field.field_id)
            .or_else(|| left.values.get(&field.field_id));
        let right_value = right
            .keys
            .get(&field.field_id)
            .or_else(|| right.values.get(&field.field_id));
        let ordering = match (left_value, right_value) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(left), Some(right)) => left.cmp(right),
        };
        let ordering = match field.direction {
            QuerySortDirection::Ascending => ordering,
            QuerySortDirection::Descending => ordering.reverse(),
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    aggregate_group_identity(left).cmp(&aggregate_group_identity(right))
}

fn aggregate_group_identity(group: &AggregateGroup) -> Vec<u8> {
    let mut encoded = Vec::new();
    for (field, value) in &group.keys {
        hash_cursor_bytes(&mut encoded, field.as_bytes());
        encode_cursor_scalar(&mut encoded, value);
    }
    encoded
}

fn change_event_identity(event: &ChangeEvent) -> Vec<u8> {
    [
        event.commit_version.get().to_be_bytes().as_slice(),
        event.sequence.to_be_bytes().as_slice(),
    ]
    .concat()
}

fn encode_row_identity(workbench_id: &WorkbenchId, path: &NormalizedRelativePath) -> Vec<u8> {
    let mut encoded = Vec::new();
    hash_cursor_bytes(&mut encoded, workbench_id.as_bytes());
    hash_cursor_bytes(&mut encoded, path.as_str().as_bytes());
    encoded
}

fn encode_cursor_scalar(encoded: &mut Vec<u8>, value: &QueryScalar) {
    encoded.push(value.scalar_type() as u8);
    match value {
        QueryScalar::Null => {}
        QueryScalar::Boolean(value) => encoded.push(u8::from(*value)),
        QueryScalar::Signed(value) | QueryScalar::Timestamp(value) => {
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        QueryScalar::Unsigned(value) => encoded.extend_from_slice(&value.to_be_bytes()),
        QueryScalar::Float(value) => {
            encoded.extend_from_slice(&value.get().to_bits().to_be_bytes())
        }
        QueryScalar::Bytes(value) => hash_cursor_bytes(encoded, value),
        QueryScalar::String(value) => hash_cursor_bytes(encoded, value.as_bytes()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum CursorKind {
    Search = 1,
    Aggregate = 2,
    Catalog = 3,
    Find = 4,
    Changes = 5,
}

struct DecodedCursor {
    kind: CursorKind,
    read_version: u64,
    request_digest: [u8; SHA256_BYTES],
    anchor: Vec<u8>,
}

fn encode_cursor(
    kind: CursorKind,
    read_version: ReadVersion,
    request_digest: [u8; SHA256_BYTES],
    anchor: &[u8],
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + 1 + 8 + SHA256_BYTES + 4 + anchor.len());
    encoded.push(CURSOR_FORMAT_VERSION);
    encoded.push(kind as u8);
    encoded.extend_from_slice(&read_version.get().to_be_bytes());
    encoded.extend_from_slice(&request_digest);
    hash_cursor_bytes(&mut encoded, anchor);
    encoded
}

fn decode_cursor(encoded: &[u8]) -> Result<DecodedCursor, QueryError> {
    if encoded.len() > MAX_QUERY_CURSOR_BYTES {
        return Err(QueryError::CursorTooLarge {
            length: encoded.len(),
            max: MAX_QUERY_CURSOR_BYTES,
        });
    }
    const HEADER: usize = 1 + 1 + 8 + SHA256_BYTES + 4;
    if encoded.len() < HEADER || encoded[0] != CURSOR_FORMAT_VERSION {
        return Err(QueryError::InvalidCursor {
            reason: "unsupported version or truncated header",
        });
    }
    let kind = match encoded[1] {
        1 => CursorKind::Search,
        2 => CursorKind::Aggregate,
        3 => CursorKind::Catalog,
        4 => CursorKind::Find,
        5 => CursorKind::Changes,
        _ => {
            return Err(QueryError::InvalidCursor {
                reason: "unknown cursor kind",
            })
        }
    };
    let read_version = u64::from_be_bytes(
        encoded[2..10]
            .try_into()
            .expect("validated cursor version width"),
    );
    let request_digest = encoded[10..10 + SHA256_BYTES]
        .try_into()
        .expect("validated cursor digest width");
    let length_offset = 10 + SHA256_BYTES;
    let anchor_length = u32::from_be_bytes(
        encoded[length_offset..length_offset + 4]
            .try_into()
            .expect("validated cursor anchor length width"),
    ) as usize;
    if encoded.len() != HEADER + anchor_length {
        return Err(QueryError::InvalidCursor {
            reason: "anchor length does not consume the exact cursor",
        });
    }
    Ok(DecodedCursor {
        kind,
        read_version,
        request_digest,
        anchor: encoded[HEADER..].to_vec(),
    })
}

fn decode_change_cursor(
    encoded: &[u8],
    context: RootReadContext,
    request_digest: [u8; SHA256_BYTES],
    after_commit_version: Option<CommitVersion>,
) -> Result<(CommitVersion, u32), QueryError> {
    let cursor = decode_cursor(encoded)?;
    if cursor.kind != CursorKind::Changes {
        return Err(QueryError::InvalidCursor {
            reason: "cursor kind does not match operation",
        });
    }
    if cursor.read_version == 0 {
        return Err(QueryError::InvalidCursor {
            reason: "change cursor read version is zero",
        });
    }
    if cursor.read_version > context.read_version.get() {
        return Err(QueryError::CursorReadVersionMismatch {
            cursor: cursor.read_version,
            request: context.read_version.get(),
        });
    }
    if cursor.request_digest != request_digest {
        return Err(QueryError::CursorQueryMismatch);
    }
    if cursor.anchor.len() != 8 + 4 {
        return Err(QueryError::InvalidCursor {
            reason: "change cursor anchor must contain one version and sequence",
        });
    }
    let commit_version = CommitVersion::new(u64::from_be_bytes(
        cursor.anchor[..8]
            .try_into()
            .expect("validated change cursor version width"),
    ))
    .map_err(|_| QueryError::InvalidCursor {
        reason: "change cursor anchor version is zero",
    })?;
    let sequence = u32::from_be_bytes(
        cursor.anchor[8..]
            .try_into()
            .expect("validated change cursor sequence width"),
    );
    if commit_version.get() > cursor.read_version {
        return Err(QueryError::InvalidCursor {
            reason: "change cursor anchor is newer than its read version",
        });
    }
    if after_commit_version.is_some_and(|after| commit_version <= after) {
        return Err(QueryError::InvalidCursor {
            reason: "change cursor anchor does not follow after_commit_version",
        });
    }
    Ok((commit_version, sequence))
}

fn cursor_start(
    cursor: Option<&[u8]>,
    expected_kind: CursorKind,
    read_version: ReadVersion,
    request_digest: [u8; SHA256_BYTES],
    identities: impl IntoIterator<Item = Vec<u8>>,
) -> Result<usize, QueryError> {
    let Some(cursor) = cursor else {
        return Ok(0);
    };
    let cursor = decode_cursor(cursor)?;
    if cursor.kind != expected_kind {
        return Err(QueryError::InvalidCursor {
            reason: "cursor kind does not match operation",
        });
    }
    if cursor.read_version != read_version.get() {
        return Err(QueryError::CursorReadVersionMismatch {
            cursor: cursor.read_version,
            request: read_version.get(),
        });
    }
    if cursor.request_digest != request_digest {
        return Err(QueryError::CursorQueryMismatch);
    }
    identities
        .into_iter()
        .position(|identity| identity == cursor.anchor)
        .map(|index| index + 1)
        .ok_or(QueryError::CursorAnchorMissing)
}

fn search_digest(root_id: RootId, request: &SearchRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = query_hasher(
        b"nokv.query.search.v2\0",
        root_id,
        &request.profile,
        &request.scope,
        &request.path_prefix,
    );
    hash_predicates(&mut hasher, &request.predicates);
    hash_fields(&mut hasher, &request.projection);
    hash_sort(&mut hasher, &request.sort);
    hash_fields(&mut hasher, &request.facets);
    hasher.finalize().into()
}

fn bind_generic_index_digest(
    request_digest: [u8; SHA256_BYTES],
    index_commitment: [u8; SHA256_BYTES],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.query.generic-index-cursor.v1\0");
    hasher.update(request_digest);
    hasher.update(index_commitment);
    hasher.finalize().into()
}

fn aggregate_digest(root_id: RootId, request: &AggregateRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = query_hasher(
        b"nokv.query.aggregate.v2\0",
        root_id,
        &request.profile,
        &request.scope,
        &request.path_prefix,
    );
    hash_predicates(&mut hasher, &request.predicates);
    hash_fields(&mut hasher, &request.group_by);
    hasher.update((request.aggregates.len() as u64).to_be_bytes());
    for aggregate in &request.aggregates {
        hash_bytes(&mut hasher, aggregate.result_id.as_bytes());
        hasher.update([match aggregate.function {
            AggregateFunction::Count => 1,
            AggregateFunction::Sum => 2,
            AggregateFunction::Average => 3,
            AggregateFunction::Minimum => 4,
            AggregateFunction::Maximum => 5,
        }]);
        match &aggregate.field_id {
            None => hasher.update([0]),
            Some(field) => {
                hasher.update([1]);
                hash_bytes(&mut hasher, field.as_bytes());
            }
        }
    }
    hash_sort(&mut hasher, &request.sort);
    hasher.finalize().into()
}

fn catalog_digest(root_id: RootId, request: &CatalogRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = query_hasher(
        b"nokv.query.catalog.v3\0",
        root_id,
        &request.profile,
        &request.scope,
        &request.path_prefix,
    );
    hasher.update([match request.path_match {
        CatalogPathMatch::Prefix => 1,
        CatalogPathMatch::Exact => 2,
    }]);
    match &request.field_prefix {
        None => hasher.update([0]),
        Some(prefix) => {
            hasher.update([1]);
            hash_bytes(&mut hasher, prefix.as_bytes());
        }
    }
    hasher.update([u8::from(request.include_facets)]);
    hasher.finalize().into()
}

fn find_digest(root_id: RootId, request: &FindWorkspacesRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.query.find-workspaces.v2\0");
    hasher.update(root_id.as_bytes());
    hasher.update([match request.committed {
        CommittedFilter::Any => 1,
        CommittedFilter::Committed => 2,
        CommittedFilter::Uncommitted => 3,
    }]);
    hasher.finalize().into()
}

fn change_digest(root_id: RootId, request: &ChangePageRequest) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.query.changes.v2\0");
    hasher.update(root_id.as_bytes());
    hash_scope(&mut hasher, &request.scope);
    match request.after_commit_version {
        None => hasher.update([0]),
        Some(version) => {
            hasher.update([1]);
            hasher.update(version.get().to_be_bytes());
        }
    }
    hasher.finalize().into()
}

fn query_hasher(
    domain: &[u8],
    root_id: RootId,
    profile: &QueryProfile,
    scope: &QueryScope,
    path_prefix: &Option<NormalizedRelativePath>,
) -> Sha256 {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(root_id.as_bytes());
    match profile {
        QueryProfile::ArtifactV1 => hasher.update([1]),
        QueryProfile::GenericNamespaceV1 {
            presentation_path_root,
        } => {
            hasher.update([2]);
            hash_bytes(&mut hasher, presentation_path_root.as_str().as_bytes());
        }
    }
    hash_scope(&mut hasher, scope);
    match path_prefix {
        None => hasher.update([0]),
        Some(path) => {
            hasher.update([1]);
            hash_bytes(&mut hasher, path.as_str().as_bytes());
        }
    }
    hasher
}

fn hash_scope(hasher: &mut Sha256, scope: &QueryScope) {
    match scope {
        QueryScope::Root => hasher.update([1]),
        QueryScope::Workspace(workbench_id) => {
            hasher.update([2]);
            hash_bytes(hasher, workbench_id.as_bytes());
        }
    }
}

fn hash_predicates(hasher: &mut Sha256, predicates: &[QueryPredicate]) {
    hasher.update((predicates.len() as u64).to_be_bytes());
    for predicate in predicates {
        hash_bytes(hasher, predicate.field_id.as_bytes());
        hasher.update([match predicate.operator {
            QueryOperator::Equal => 1,
            QueryOperator::NotEqual => 2,
            QueryOperator::Less => 3,
            QueryOperator::LessOrEqual => 4,
            QueryOperator::Greater => 5,
            QueryOperator::GreaterOrEqual => 6,
            QueryOperator::Prefix => 7,
            QueryOperator::Contains => 8,
            QueryOperator::Exists => 9,
            QueryOperator::In => 10,
            QueryOperator::Suffix => 11,
            QueryOperator::NotExists => 12,
        }]);
        match &predicate.operand {
            QueryOperand::None => hasher.update([0]),
            QueryOperand::Scalar(value) => {
                hasher.update([1]);
                let mut encoded = Vec::new();
                encode_cursor_scalar(&mut encoded, value);
                hash_bytes(hasher, &encoded);
            }
            QueryOperand::Set(values) => {
                hasher.update([2]);
                hasher.update((values.len() as u64).to_be_bytes());
                for value in values {
                    let mut encoded = Vec::new();
                    encode_cursor_scalar(&mut encoded, value);
                    hash_bytes(hasher, &encoded);
                }
            }
        }
    }
}

fn hash_fields(hasher: &mut Sha256, fields: &[QueryFieldId]) {
    hasher.update((fields.len() as u64).to_be_bytes());
    for field in fields {
        hash_bytes(hasher, field.as_bytes());
    }
}

fn hash_sort(hasher: &mut Sha256, sort: &[QuerySort]) {
    hasher.update((sort.len() as u64).to_be_bytes());
    for field in sort {
        hash_bytes(hasher, field.field_id.as_bytes());
        hasher.update([match field.direction {
            QuerySortDirection::Ascending => 1,
            QuerySortDirection::Descending => 2,
        }]);
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hash_cursor_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) {
    encoded.extend_from_slice(
        &u32::try_from(bytes.len())
            .expect("bounded query identity fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(bytes);
}

fn validate_page(limit: usize) -> Result<(), QueryError> {
    if (1..=MAX_QUERY_PAGE_SIZE).contains(&limit) {
        Ok(())
    } else {
        Err(QueryError::InvalidLimit {
            requested: limit,
            max: MAX_QUERY_PAGE_SIZE,
        })
    }
}

fn validate_bound(field: &'static str, count: usize, max: usize) -> Result<(), QueryError> {
    if count <= max {
        Ok(())
    } else {
        Err(QueryError::BoundExceeded { field, count, max })
    }
}

fn checked_count(field: &'static str, count: usize) -> Result<u64, QueryError> {
    u64::try_from(count).map_err(|_| QueryError::BoundExceeded {
        field,
        count,
        max: u64::MAX as usize,
    })
}

fn require_unique_fields(field: &'static str, fields: &[QueryFieldId]) -> Result<(), QueryError> {
    if fields.iter().collect::<BTreeSet<_>>().len() == fields.len() {
        Ok(())
    } else {
        Err(QueryError::BoundExceeded {
            field,
            count: fields.len(),
            max: fields.len().saturating_sub(1),
        })
    }
}

#[cfg(test)]
mod tests {
    use nokv_types::{
        ArtifactRevisionId, CommandDigest, GenericIndexGenerationId, LogicalShardId,
        ObjectNamespaceId, OperationId, OwnerEpoch, PlacementGeneration, RequestId,
        RootActivationState,
    };

    use super::super::codec::{
        path_current_key, workbench_commit_head_key, workspace_current_key, SCHEMA_ID,
    };
    use super::super::engine::{
        CommandMutation, CommandPredicate, EventProjection, HistoryProjection, MetadataCommand,
        RootFenceAction,
    };
    use super::super::generic_index::{
        AppendGenericIndexRowsRequest, BeginGenericIndexRegistrationRequest,
        FinalizeGenericIndexRegistrationRequest, GenericIndexRegistrationService,
        GenericIndexRowInput,
    };
    use super::super::generic_index_records::GenericIndexFieldValues;
    use super::super::namespace::RootWriteContext;
    use super::*;

    fn shard() -> LogicalShardId {
        LogicalShardId::from_bytes([1; FIXED_ID_BYTES])
    }

    fn root() -> RootId {
        RootId::from_bytes([2; FIXED_ID_BYTES])
    }

    fn placement() -> PlacementGeneration {
        PlacementGeneration::new(7).unwrap()
    }

    fn owner() -> OwnerEpoch {
        OwnerEpoch::new(1).unwrap()
    }

    fn request(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn incarnation(fill: u8) -> WorkspaceIncarnationId {
        WorkspaceIncarnationId::from_bytes([fill; FIXED_ID_BYTES])
    }

    fn workbench(value: &str) -> WorkbenchId {
        WorkbenchId::new(value).unwrap()
    }

    fn path(value: &str) -> NormalizedRelativePath {
        NormalizedRelativePath::new(value).unwrap()
    }

    fn field(value: &str) -> QueryFieldId {
        QueryFieldId::new(value).unwrap()
    }

    fn revision(value: u64) -> ArtifactRevisionId {
        let mut bytes = [0; FIXED_ID_BYTES];
        bytes[..8].copy_from_slice(&value.to_be_bytes());
        ArtifactRevisionId::from_bytes(bytes)
    }

    fn projection(values: Vec<(&str, QueryScalar)>) -> Vec<u8> {
        TypedProjection::new(
            values
                .into_iter()
                .map(|(name, value)| (field(name), value))
                .collect(),
        )
        .unwrap()
        .encode()
        .unwrap()
    }

    fn entry(value: u64, generation: u64, fields: Vec<(&str, QueryScalar)>) -> PathEntry {
        PathEntry {
            generation: Generation::new(generation).unwrap(),
            artifact_revision_id: revision(value),
            body_digest_uri: format!("sha256:{value:064x}"),
            manifest_digest_uri: format!("sha256:{:064x}", value.saturating_add(1)),
            logical_size: value,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: Some("query-test".to_owned()),
            manifest_id: Some(format!("manifest-{value}")),
            typed_index_projection: projection(fields),
        }
    }

    fn fence_command(
        store: &MetaShard,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        fence_command_at(store, root(), request_id, action)
    }

    fn fence_command_at(
        store: &MetaShard,
        root_id: RootId,
        request_id: RequestId,
        action: RootFenceAction,
    ) -> MetadataCommand {
        MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id,
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id,
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal()
    }

    fn activate(store: &MetaShard) {
        store.advance_owner_epoch(None, owner()).unwrap();
        store
            .execute(&fence_command(store, request(1), RootFenceAction::Install))
            .unwrap();
        store
            .execute(&fence_command(
                store,
                request(2),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();
    }

    fn ready_store() -> MetaShard {
        let store = crate::workspace::test_support::memory(shard()).unwrap();
        activate(&store);
        store
    }

    fn context(store: &MetaShard) -> RootReadContext {
        RootReadContext::current(store, root(), placement(), owner()).unwrap()
    }

    fn context_at(version: CommitVersion) -> RootReadContext {
        RootReadContext {
            root_id: root(),
            placement_generation: placement(),
            owner_epoch: owner(),
            read_version: ReadVersion::new(version.get()).unwrap(),
        }
    }

    fn execute(
        store: &MetaShard,
        request_fill: u8,
        predicates: Vec<CommandPredicate>,
        mutations: Vec<CommandMutation>,
        history_projection: Vec<HistoryProjection>,
        events: Vec<ChangeEventRecord>,
    ) -> CommitVersion {
        execute_at(
            store,
            root(),
            request_fill,
            predicates,
            mutations,
            history_projection,
            events,
        )
    }

    fn execute_at(
        store: &MetaShard,
        root_id: RootId,
        request_fill: u8,
        predicates: Vec<CommandPredicate>,
        mutations: Vec<CommandMutation>,
        history_projection: Vec<HistoryProjection>,
        events: Vec<ChangeEventRecord>,
    ) -> CommitVersion {
        store
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id,
                    logical_shard_id: shard(),
                    object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                        [10; FIXED_ID_BYTES],
                    )),
                    placement_generation: placement(),
                    owner_epoch: owner(),
                    request_id: request(request_fill),
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: RootFenceAction::RequireActive,
                    predicates,
                    mutations,
                    history_projection,
                    event_projection: events
                        .into_iter()
                        .map(|event| EventProjection {
                            payload: event.encode().unwrap(),
                        })
                        .collect(),
                    deterministic_result: vec![request_fill],
                }
                .seal(),
            )
            .unwrap()
            .commit_version
    }

    fn put_records(
        store: &MetaShard,
        request_fill: u8,
        records: Vec<(MetadataFamily, Vec<u8>, Vec<u8>)>,
    ) -> CommitVersion {
        let predicates = records
            .iter()
            .map(|(family, key, _)| CommandPredicate::Value {
                family: *family,
                key: key.clone(),
                expected: None,
            })
            .collect();
        let mutations = records
            .into_iter()
            .map(|(family, key, value)| CommandMutation::Put { family, key, value })
            .collect();
        execute(
            store,
            request_fill,
            predicates,
            mutations,
            Vec::new(),
            Vec::new(),
        )
    }

    fn workspace_record(
        incarnation_id: WorkspaceIncarnationId,
        state: WorkspaceState,
    ) -> WorkspaceRecord {
        WorkspaceRecord {
            incarnation_id,
            workspace_revision: WorkspaceRevision::new(1),
            state,
            owning_operation_id: (state == WorkspaceState::Staging)
                .then(|| OperationId::from_bytes([9; FIXED_ID_BYTES])),
        }
    }

    fn workspace_row(
        name: &WorkbenchId,
        incarnation_id: WorkspaceIncarnationId,
        state: WorkspaceState,
    ) -> (MetadataFamily, Vec<u8>, Vec<u8>) {
        (
            MetadataFamily::WorkspaceCurrent,
            workspace_current_key(root(), name),
            workspace_record(incarnation_id, state).encode().unwrap(),
        )
    }

    fn path_row(
        incarnation_id: WorkspaceIncarnationId,
        name: &str,
        entry: &PathEntry,
    ) -> (MetadataFamily, Vec<u8>, Vec<u8>) {
        (
            MetadataFamily::PathCurrent,
            path_current_key(root(), incarnation_id, &path(name)),
            entry.encode().unwrap(),
        )
    }

    fn search_request(scope: QueryScope) -> SearchRequest {
        SearchRequest {
            profile: QueryProfile::ArtifactV1,
            scope,
            path_prefix: None,
            predicates: Vec::new(),
            projection: Vec::new(),
            sort: Vec::new(),
            facets: Vec::new(),
            cursor: None,
            limit: MAX_QUERY_PAGE_SIZE,
        }
    }

    fn scalar_operand(value: QueryScalar) -> QueryOperand {
        QueryOperand::Scalar(value)
    }

    fn set_operand(values: impl IntoIterator<Item = QueryScalar>) -> QueryOperand {
        QueryOperand::Set(values.into_iter().collect())
    }

    fn predicate_store() -> (MetaShard, WorkbenchId) {
        let store = ready_store();
        let name = workbench("predicate-fixture");
        let incarnation_id = incarnation(31);
        let mut gamma = entry(
            30,
            1,
            vec![
                ("run.bytes", QueryScalar::Bytes(b"gamma-end".to_vec())),
                ("run.score", QueryScalar::Unsigned(30)),
            ],
        );
        gamma.producer = None;
        put_records(
            &store,
            3,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(
                    incarnation_id,
                    "outputs/alpha.log",
                    &entry(
                        10,
                        1,
                        vec![
                            ("run.bytes", QueryScalar::Bytes(b"alpha-tail".to_vec())),
                            ("run.label", QueryScalar::String("blue".to_owned())),
                            ("run.null", QueryScalar::Null),
                            ("run.score", QueryScalar::Unsigned(10)),
                        ],
                    ),
                ),
                path_row(
                    incarnation_id,
                    "outputs/beta.txt",
                    &entry(
                        20,
                        1,
                        vec![
                            ("run.bytes", QueryScalar::Bytes(b"beta-tail".to_vec())),
                            ("run.label", QueryScalar::String("red".to_owned())),
                            ("run.score", QueryScalar::Unsigned(20)),
                        ],
                    ),
                ),
                path_row(incarnation_id, "outputs/gamma.log", &gamma),
                path_row(
                    incarnation_id,
                    "outputs/delta.log",
                    &entry(
                        40,
                        1,
                        vec![
                            ("run.bytes", QueryScalar::Bytes(b"delta-tail".to_vec())),
                            ("run.label", QueryScalar::String("green".to_owned())),
                            ("run.score", QueryScalar::Unsigned(40)),
                        ],
                    ),
                ),
            ],
        );
        (store, name)
    }

    fn generic_namespace_store() -> (MetaShard, WorkbenchId) {
        let store = ready_store();
        let name = workbench("run-42");
        let incarnation_id = incarnation(42);
        let mut collision = entry(10, 1, Vec::new());
        collision.producer = Some("alpha".to_owned());
        collision.content_type = "text/plain".to_owned();
        let mut child = entry(20, 1, vec![("run.score", QueryScalar::Unsigned(2))]);
        child.producer = Some("alpha".to_owned());
        child.content_type = "text/plain".to_owned();
        let mut nested = entry(30, 1, vec![("run.score", QueryScalar::Unsigned(3))]);
        nested.producer = Some("beta".to_owned());
        nested.content_type = "application/json".to_owned();
        put_records(
            &store,
            3,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(incarnation_id, "outputs/collision", &collision),
                path_row(incarnation_id, "outputs/collision/child.txt", &child),
                path_row(incarnation_id, "outputs/nested/file.json", &nested),
            ],
        );
        (store, name)
    }

    fn generic_search_request(scope: QueryScope) -> SearchRequest {
        SearchRequest {
            profile: generic_profile(),
            scope,
            path_prefix: None,
            predicates: Vec::new(),
            projection: Vec::new(),
            sort: vec![QuerySort {
                field_id: field("path"),
                direction: QuerySortDirection::Ascending,
            }],
            facets: Vec::new(),
            cursor: None,
            limit: MAX_QUERY_PAGE_SIZE,
        }
    }

    fn generic_profile() -> QueryProfile {
        QueryProfile::GenericNamespaceV1 {
            presentation_path_root: PresentationPathRoot::new("/agents").unwrap(),
        }
    }

    fn generic_registration_context(store: &MetaShard, request_fill: u8) -> RootWriteContext {
        RootWriteContext::current(
            store,
            root(),
            shard(),
            ObjectNamespaceId::from_bytes([10; FIXED_ID_BYTES]),
            placement(),
            owner(),
            request(request_fill),
        )
        .unwrap()
    }

    fn generic_capability(
        field_id: &str,
        operators: Vec<GenericIndexOperator>,
        sortable: bool,
        facetable: bool,
    ) -> GenericIndexFieldCapability {
        GenericIndexFieldCapability {
            field: field(field_id),
            operators,
            sortable,
            facetable,
        }
    }

    fn register_generic_index(
        store: &MetaShard,
        capabilities: Vec<GenericIndexFieldCapability>,
        rows: Vec<GenericIndexRowInput>,
    ) {
        let service = GenericIndexRegistrationService::new(store);
        service
            .begin(BeginGenericIndexRegistrationRequest {
                context: generic_registration_context(store, 80),
                operation_id: OperationId::from_bytes([81; FIXED_ID_BYTES]),
                generation_id: GenericIndexGenerationId::from_bytes([82; FIXED_ID_BYTES]),
                workbench_id: workbench("run-42"),
                expected_workspace_incarnation_id: incarnation(42),
                index_path: None,
                expected_current_generation: None,
                capabilities,
                declared_row_count: rows.len() as u64,
            })
            .unwrap();
        if !rows.is_empty() {
            service
                .append(AppendGenericIndexRowsRequest {
                    context: generic_registration_context(store, 83),
                    operation_id: OperationId::from_bytes([81; FIXED_ID_BYTES]),
                    first_sequence: 0,
                    rows,
                })
                .unwrap();
        }
        service
            .finalize(FinalizeGenericIndexRegistrationRequest {
                context: generic_registration_context(store, 84),
                operation_id: OperationId::from_bytes([81; FIXED_ID_BYTES]),
            })
            .unwrap();
    }

    fn indexed_row(
        relative_path: &str,
        values: Vec<(&str, Vec<QueryScalar>)>,
    ) -> GenericIndexRowInput {
        GenericIndexRowInput {
            relative_path: Some(path(relative_path)),
            values: values
                .into_iter()
                .map(|(field_id, values)| GenericIndexFieldValues {
                    field: field(field_id),
                    values,
                })
                .collect(),
        }
    }

    fn generic_row_with_custom_values(
        relative_path: &str,
        values: Vec<(&str, Vec<QueryScalar>)>,
    ) -> GenericMaterializedRow {
        let mut row = generic_directory_row(
            Arc::from("/agents"),
            workbench("custom-index"),
            Some(path(relative_path)),
        );
        row.custom_values = values
            .into_iter()
            .map(|(field_id, values)| (field(field_id), values))
            .collect();
        row
    }

    #[test]
    fn generic_custom_multivalues_use_any_all_occurrence_and_first_value_semantics() {
        let row = generic_row_with_custom_values(
            "outputs/a.json",
            vec![
                (
                    "custom.label",
                    vec![
                        QueryScalar::String("blue".to_owned()),
                        QueryScalar::String("green".to_owned()),
                        QueryScalar::String("blue".to_owned()),
                    ],
                ),
                (
                    "custom.score",
                    vec![
                        QueryScalar::String("10".to_owned()),
                        QueryScalar::Unsigned(30),
                    ],
                ),
            ],
        );
        let predicate = |field_id: &str, operator, value| QueryPredicate {
            field_id: field(field_id),
            operator,
            operand: QueryOperand::Scalar(value),
        };

        assert!(predicates_match(
            &row,
            &[predicate(
                "custom.label",
                QueryOperator::Equal,
                QueryScalar::String("green".to_owned()),
            )],
            true,
        ));
        assert!(!predicates_match(
            &row,
            &[predicate(
                "custom.label",
                QueryOperator::NotEqual,
                QueryScalar::String("green".to_owned()),
            )],
            true,
        ));
        assert!(predicates_match(
            &row,
            &[predicate(
                "custom.label",
                QueryOperator::NotEqual,
                QueryScalar::String("red".to_owned()),
            )],
            true,
        ));
        assert!(!predicates_match(
            &row,
            &[predicate(
                "custom.missing",
                QueryOperator::NotEqual,
                QueryScalar::String("red".to_owned()),
            )],
            true,
        ));
        assert!(predicates_match(
            &row,
            &[predicate(
                "custom.score",
                QueryOperator::Greater,
                QueryScalar::Unsigned(20),
            )],
            true,
        ));
        assert_eq!(
            row.projection_value(&field("custom.score")),
            Some(QueryScalar::String("10".to_owned()))
        );

        let facets =
            build_facets(std::slice::from_ref(&row), &[field("custom.label")], 10).unwrap();
        assert_eq!(facets[0].distinct_count, 2);
        assert_eq!(
            facets[0].buckets,
            vec![
                FacetBucket {
                    value: QueryScalar::String("blue".to_owned()),
                    count: 2,
                },
                FacetBucket {
                    value: QueryScalar::String("green".to_owned()),
                    count: 1,
                },
            ]
        );

        let earlier_first = generic_row_with_custom_values(
            "outputs/z.json",
            vec![(
                "custom.score",
                vec![QueryScalar::Unsigned(2), QueryScalar::Unsigned(0)],
            )],
        );
        assert_eq!(
            compare_rows(
                &row,
                &earlier_first,
                &[QuerySort {
                    field_id: field("custom.score"),
                    direction: QuerySortDirection::Ascending,
                }],
            ),
            Ordering::Greater
        );

        let aggregate_request = AggregateRequest {
            profile: generic_profile(),
            scope: QueryScope::Workspace(workbench("custom-index")),
            path_prefix: Some(path("outputs")),
            predicates: Vec::new(),
            group_by: vec![field("custom.label")],
            aggregates: vec![AggregateSpec {
                result_id: field("sum"),
                function: AggregateFunction::Sum,
                field_id: Some(field("custom.score")),
            }],
            sort: Vec::new(),
            cursor: None,
            limit: 10,
        };
        let key = aggregate_request
            .group_by
            .iter()
            .map(|field_id| row.aggregate_value(field_id).unwrap())
            .collect();
        let mut aggregate = GenericAggregateBuilder::new(key, &aggregate_request.aggregates);
        aggregate
            .observe(&row, &aggregate_request.aggregates)
            .unwrap();
        let group = aggregate.finish(&aggregate_request).unwrap();
        assert_eq!(
            group.keys.get(&field("custom.label")),
            Some(&QueryScalar::String("blue".to_owned()))
        );
        assert_eq!(
            group.values.get(&field("sum")),
            Some(&QueryScalar::Float(FiniteFloat::new(10.0).unwrap()))
        );
    }

    #[test]
    fn durable_generic_zero_row_catalog_is_exact_and_capability_restricted() {
        let (store, name) = generic_namespace_store();
        register_generic_index(
            &store,
            vec![generic_capability(
                "custom.zero",
                vec![GenericIndexOperator::Equal],
                false,
                false,
            )],
            Vec::new(),
        );

        let catalog_request = CatalogRequest {
            profile: generic_profile(),
            scope: QueryScope::Workspace(name.clone()),
            path_prefix: None,
            path_match: CatalogPathMatch::Prefix,
            field_prefix: Some("custom.zero".to_owned()),
            include_facets: true,
            cursor: None,
            limit: 10,
        };
        let page = catalog_fields_at(&store, context(&store), &catalog_request).unwrap();
        assert_eq!(page.fields.len(), 1);
        assert_eq!(page.fields[0].field_id, field("custom.zero"));
        assert!(page.fields[0].scalar_types.is_empty());
        assert_eq!(page.fields[0].operators, vec![QueryOperator::Equal]);
        assert!(!page.fields[0].sortable);
        assert!(!page.fields[0].facetable);
        assert!(page.facets.is_empty());

        let exact_child = catalog_fields_at(
            &store,
            context(&store),
            &CatalogRequest {
                path_prefix: Some(path("outputs")),
                path_match: CatalogPathMatch::Exact,
                ..catalog_request.clone()
            },
        )
        .unwrap();
        assert!(exact_child.fields.is_empty());

        let mut search = generic_search_request(QueryScope::Workspace(name));
        search.predicates = vec![QueryPredicate {
            field_id: field("custom.zero"),
            operator: QueryOperator::Greater,
            operand: QueryScalar::Unsigned(0).into(),
        }];
        assert!(matches!(
            search_paths_at(&store, context(&store), &search),
            Err(QueryError::InvalidPredicate { field_id, .. })
                if field_id == field("custom.zero")
        ));
        search.predicates.clear();
        search.sort = vec![QuerySort {
            field_id: field("custom.zero"),
            direction: QuerySortDirection::Ascending,
        }];
        assert!(matches!(
            search_paths_at(&store, context(&store), &search),
            Err(QueryError::InvalidPredicate { field_id, .. })
                if field_id == field("custom.zero")
        ));
        search.sort.clear();
        search.facets = vec![field("custom.zero")];
        assert!(matches!(
            search_paths_at(&store, context(&store), &search),
            Err(QueryError::InvalidPredicate { field_id, .. })
                if field_id == field("custom.zero")
        ));
    }

    #[test]
    fn durable_generic_multivalues_drive_projection_predicates_facets_sort_group_and_numeric_aggregate(
    ) {
        let (store, name) = generic_namespace_store();
        let label_operators = vec![
            GenericIndexOperator::Equal,
            GenericIndexOperator::NotEqual,
            GenericIndexOperator::In,
            GenericIndexOperator::Prefix,
            GenericIndexOperator::Suffix,
            GenericIndexOperator::Contains,
            GenericIndexOperator::Exists,
            GenericIndexOperator::NotExists,
        ];
        let score_operators = vec![
            GenericIndexOperator::Equal,
            GenericIndexOperator::NotEqual,
            GenericIndexOperator::In,
            GenericIndexOperator::Greater,
            GenericIndexOperator::GreaterOrEqual,
            GenericIndexOperator::Less,
            GenericIndexOperator::LessOrEqual,
            GenericIndexOperator::Exists,
            GenericIndexOperator::NotExists,
        ];
        register_generic_index(
            &store,
            vec![
                generic_capability("custom.label", label_operators, true, true),
                generic_capability("custom.score", score_operators, true, true),
            ],
            vec![
                indexed_row(
                    "outputs/collision/child.txt",
                    vec![
                        (
                            "custom.label",
                            vec![
                                QueryScalar::String("blue".to_owned()),
                                QueryScalar::String("green".to_owned()),
                                QueryScalar::String("blue".to_owned()),
                            ],
                        ),
                        (
                            "custom.score",
                            vec![
                                QueryScalar::String("10".to_owned()),
                                QueryScalar::Unsigned(30),
                            ],
                        ),
                    ],
                ),
                indexed_row(
                    "outputs/nested/file.json",
                    vec![
                        ("custom.label", vec![QueryScalar::String("red".to_owned())]),
                        (
                            "custom.score",
                            vec![QueryScalar::Float(FiniteFloat::new(5.0).unwrap())],
                        ),
                    ],
                ),
            ],
        );

        let mut search = generic_search_request(QueryScope::Workspace(name.clone()));
        search.predicates = vec![QueryPredicate {
            field_id: field("custom.score"),
            operator: QueryOperator::Greater,
            operand: QueryScalar::Unsigned(20).into(),
        }];
        search.projection = vec![field("custom.label"), field("custom.score")];
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 1);
        assert_eq!(
            page.namespace_hits[0].projection[&field("custom.score")],
            QueryScalar::String("10".to_owned())
        );
        assert_eq!(
            page.namespace_hits[0].indexed_values[&field("custom.label")],
            vec![
                QueryScalar::String("blue".to_owned()),
                QueryScalar::String("green".to_owned()),
                QueryScalar::String("blue".to_owned()),
            ]
        );
        assert_eq!(
            page.namespace_hits[0].indexed_values[&field("custom.score")],
            vec![
                QueryScalar::String("10".to_owned()),
                QueryScalar::Unsigned(30),
            ]
        );

        search.predicates = vec![QueryPredicate {
            field_id: field("custom.score"),
            operator: QueryOperator::NotEqual,
            operand: QueryScalar::Unsigned(30).into(),
        }];
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 1);
        assert_eq!(
            page.namespace_hits[0].relative_path,
            Some(path("outputs/nested/file.json"))
        );

        search.predicates = vec![QueryPredicate {
            field_id: field("custom.label"),
            operator: QueryOperator::Exists,
            operand: QueryOperand::None,
        }];
        search.projection.clear();
        search.facets = vec![field("custom.label"), field("custom.score")];
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 2);
        let label_facet = page
            .facets
            .iter()
            .find(|facet| facet.field_id == field("custom.label"))
            .unwrap();
        assert_eq!(
            label_facet.buckets,
            vec![
                FacetBucket {
                    value: QueryScalar::String("blue".to_owned()),
                    count: 2,
                },
                FacetBucket {
                    value: QueryScalar::String("green".to_owned()),
                    count: 1,
                },
                FacetBucket {
                    value: QueryScalar::String("red".to_owned()),
                    count: 1,
                },
            ]
        );
        let score_facet = page
            .facets
            .iter()
            .find(|facet| facet.field_id == field("custom.score"))
            .unwrap();
        assert_eq!(score_facet.distinct_count, 3);

        search.facets.clear();
        search.predicates = vec![QueryPredicate {
            field_id: field("custom.score"),
            operator: QueryOperator::Exists,
            operand: QueryOperand::None,
        }];
        search.sort = vec![QuerySort {
            field_id: field("custom.score"),
            direction: QuerySortDirection::Ascending,
        }];
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(
            page.namespace_hits
                .iter()
                .map(|hit| hit.relative_path.clone().unwrap())
                .collect::<Vec<_>>(),
            vec![
                path("outputs/nested/file.json"),
                path("outputs/collision/child.txt"),
            ]
        );

        let sum = aggregate_paths_at(
            &store,
            context(&store),
            &AggregateRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name.clone()),
                path_prefix: None,
                predicates: vec![QueryPredicate {
                    field_id: field("custom.score"),
                    operator: QueryOperator::Exists,
                    operand: QueryOperand::None,
                }],
                group_by: Vec::new(),
                aggregates: vec![AggregateSpec {
                    result_id: field("sum"),
                    function: AggregateFunction::Sum,
                    field_id: Some(field("custom.score")),
                }],
                sort: Vec::new(),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(sum.row_count, 2);
        assert_eq!(
            sum.groups[0].values[&field("sum")],
            QueryScalar::Float(FiniteFloat::new(15.0).unwrap())
        );

        let grouped = aggregate_paths_at(
            &store,
            context(&store),
            &AggregateRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name.clone()),
                path_prefix: None,
                predicates: vec![QueryPredicate {
                    field_id: field("custom.label"),
                    operator: QueryOperator::Exists,
                    operand: QueryOperand::None,
                }],
                group_by: vec![field("custom.label")],
                aggregates: vec![AggregateSpec {
                    result_id: field("count"),
                    function: AggregateFunction::Count,
                    field_id: None,
                }],
                sort: vec![QuerySort {
                    field_id: field("custom.label"),
                    direction: QuerySortDirection::Ascending,
                }],
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(grouped.group_count, 2);
        assert_eq!(
            grouped
                .groups
                .iter()
                .map(|group| group.keys[&field("custom.label")].clone())
                .collect::<Vec<_>>(),
            vec![
                QueryScalar::String("blue".to_owned()),
                QueryScalar::String("red".to_owned()),
            ]
        );

        let catalog = catalog_fields_at(
            &store,
            context(&store),
            &CatalogRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name),
                path_prefix: None,
                path_match: CatalogPathMatch::Prefix,
                field_prefix: Some("custom.score".to_owned()),
                include_facets: false,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(
            catalog.fields[0].scalar_types,
            vec![
                QueryScalarType::Unsigned,
                QueryScalarType::Float,
                QueryScalarType::String,
            ]
        );
    }

    #[test]
    fn durable_generic_unbound_directory_and_artifact_fences_are_distinct() {
        let (store, name) = generic_namespace_store();
        register_generic_index(
            &store,
            vec![generic_capability(
                "custom.label",
                vec![GenericIndexOperator::Exists],
                false,
                false,
            )],
            vec![
                indexed_row(
                    "outputs/ghost.txt",
                    vec![(
                        "custom.label",
                        vec![QueryScalar::String("ghost".to_owned())],
                    )],
                ),
                indexed_row(
                    "outputs/nested",
                    vec![(
                        "custom.label",
                        vec![QueryScalar::String("directory".to_owned())],
                    )],
                ),
                indexed_row(
                    "outputs/nested/file.json",
                    vec![(
                        "custom.label",
                        vec![QueryScalar::String("artifact".to_owned())],
                    )],
                ),
            ],
        );
        let mut search = generic_search_request(QueryScope::Workspace(name));
        search.predicates = vec![QueryPredicate {
            field_id: field("custom.label"),
            operator: QueryOperator::Exists,
            operand: QueryOperand::None,
        }];
        search.projection = vec![field("custom.label")];
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 2);
        assert!(page
            .namespace_hits
            .iter()
            .all(|hit| hit.relative_path != Some(path("outputs/ghost.txt"))));

        let ghost_path = path("outputs/ghost.txt");
        let ghost_key = path_current_key(root(), incarnation(42), &ghost_path);
        execute(
            &store,
            89,
            vec![CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: ghost_key.clone(),
                expected: None,
            }],
            vec![CommandMutation::Put {
                family: MetadataFamily::PathCurrent,
                key: ghost_key,
                value: entry(77, 1, Vec::new()).encode().unwrap(),
            }],
            Vec::new(),
            Vec::new(),
        );
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 3);
        assert!(page.namespace_hits.iter().any(|hit| {
            hit.relative_path == Some(path("outputs/ghost.txt"))
                && hit.projection[&field("custom.label")] == QueryScalar::String("ghost".to_owned())
        }));

        let artifact_path = path("outputs/nested/file.json");
        let artifact_key = path_current_key(root(), incarnation(42), &artifact_path);
        let old_artifact = store
            .read_at(
                root(),
                placement(),
                owner(),
                MetadataFamily::PathCurrent,
                &artifact_key,
                context(&store).read_version,
            )
            .unwrap()
            .unwrap();
        let replacement = entry(99, 2, Vec::new()).encode().unwrap();
        execute(
            &store,
            90,
            vec![CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: artifact_key.clone(),
                expected: Some(old_artifact),
            }],
            vec![CommandMutation::Put {
                family: MetadataFamily::PathCurrent,
                key: artifact_key.clone(),
                value: replacement.clone(),
            }],
            vec![HistoryProjection {
                family: MetadataFamily::PathCurrent,
                key: artifact_key.clone(),
            }],
            Vec::new(),
        );
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 2);
        assert!(page
            .namespace_hits
            .iter()
            .any(|hit| hit.relative_path == Some(path("outputs/nested"))));

        execute(
            &store,
            91,
            vec![CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: artifact_key.clone(),
                expected: Some(replacement),
            }],
            vec![CommandMutation::Delete {
                family: MetadataFamily::PathCurrent,
                key: artifact_key.clone(),
            }],
            vec![HistoryProjection {
                family: MetadataFamily::PathCurrent,
                key: artifact_key,
            }],
            Vec::new(),
        );
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 1);
        assert_eq!(
            page.namespace_hits[0].relative_path,
            Some(path("outputs/ghost.txt"))
        );

        let former_directory_key =
            path_current_key(root(), incarnation(42), &path("outputs/nested"));
        execute(
            &store,
            92,
            vec![CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: former_directory_key.clone(),
                expected: None,
            }],
            vec![CommandMutation::Put {
                family: MetadataFamily::PathCurrent,
                key: former_directory_key,
                value: entry(88, 1, Vec::new()).encode().unwrap(),
            }],
            Vec::new(),
            Vec::new(),
        );
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(page.match_count, 1);
        assert_eq!(
            page.namespace_hits[0].relative_path,
            Some(path("outputs/ghost.txt"))
        );
    }

    #[test]
    fn generic_cursor_digest_rejects_generation_commitment_aba() {
        let request_digest = [7; SHA256_BYTES];
        let first = bind_generic_index_digest(request_digest, [8; SHA256_BYTES]);
        let replacement = bind_generic_index_digest(request_digest, [9; SHA256_BYTES]);
        assert_ne!(first, replacement);
        let read_version = ReadVersion::new(3).unwrap();
        let cursor = encode_cursor(CursorKind::Search, read_version, first, b"anchor");
        assert_eq!(
            cursor_start(
                Some(&cursor),
                CursorKind::Search,
                read_version,
                replacement,
                [b"anchor".to_vec()],
            ),
            Err(QueryError::CursorQueryMismatch)
        );
    }

    fn aggregate_count(
        store: &MetaShard,
        name: WorkbenchId,
        predicates: Vec<QueryPredicate>,
    ) -> u64 {
        let page = aggregate_paths_at(
            store,
            context(store),
            &AggregateRequest {
                profile: QueryProfile::ArtifactV1,
                scope: QueryScope::Workspace(name),
                path_prefix: Some(path("outputs")),
                predicates,
                group_by: Vec::new(),
                aggregates: vec![AggregateSpec {
                    result_id: field("rows"),
                    function: AggregateFunction::Count,
                    field_id: None,
                }],
                sort: Vec::new(),
                cursor: None,
                limit: 1,
            },
        )
        .unwrap();
        match page.groups[0].values.get(&field("rows")) {
            Some(QueryScalar::Unsigned(count)) => *count,
            other => panic!("count aggregate returned {other:?}"),
        }
    }

    #[test]
    fn component_prefix_root_scope_and_staging_gate_are_exact() {
        let store = ready_store();
        let first = workbench("first");
        let second = workbench("second");
        let hidden = workbench("hidden");
        let first_incarnation = incarnation(3);
        let second_incarnation = incarnation(4);
        let hidden_incarnation = incarnation(5);
        put_records(
            &store,
            3,
            vec![
                workspace_row(&first, first_incarnation, WorkspaceState::Visible),
                path_row(
                    first_incarnation,
                    "a",
                    &entry(
                        1,
                        1,
                        vec![("visible.kind", QueryScalar::String("a".into()))],
                    ),
                ),
                path_row(
                    first_incarnation,
                    "a/child",
                    &entry(
                        2,
                        1,
                        vec![("visible.kind", QueryScalar::String("child".into()))],
                    ),
                ),
                path_row(
                    first_incarnation,
                    "ab",
                    &entry(
                        3,
                        1,
                        vec![("visible.kind", QueryScalar::String("ab".into()))],
                    ),
                ),
                workspace_row(&second, second_incarnation, WorkspaceState::Visible),
                path_row(
                    second_incarnation,
                    "a/other",
                    &entry(
                        4,
                        1,
                        vec![("visible.kind", QueryScalar::String("other".into()))],
                    ),
                ),
                workspace_row(&hidden, hidden_incarnation, WorkspaceState::Staging),
                path_row(
                    hidden_incarnation,
                    "a/secret",
                    &entry(5, 1, vec![("secret.only", QueryScalar::Boolean(true))]),
                ),
            ],
        );

        let mut request = search_request(QueryScope::Workspace(first.clone()));
        request.path_prefix = Some(path("a"));
        let page = search_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "a/child"]
        );

        request.scope = QueryScope::Root;
        let page = search_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| (hit.workbench_id.as_str(), hit.path.as_str()))
                .collect::<Vec<_>>(),
            vec![("first", "a"), ("first", "a/child"), ("second", "a/other")]
        );

        let catalog = catalog_fields_at(
            &store,
            context(&store),
            &CatalogRequest {
                profile: QueryProfile::ArtifactV1,
                scope: QueryScope::Root,
                path_prefix: None,
                path_match: CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: false,
                cursor: None,
                limit: MAX_QUERY_PAGE_SIZE,
            },
        )
        .unwrap();
        assert!(catalog
            .fields
            .iter()
            .any(|catalog| catalog.field_id == field("visible.kind")));
        assert!(!catalog
            .fields
            .iter()
            .any(|catalog| catalog.field_id == field("secret.only")));
        assert!(catalog_fields_at(
            &store,
            context(&store),
            &CatalogRequest {
                profile: QueryProfile::ArtifactV1,
                scope: QueryScope::Workspace(hidden),
                path_prefix: None,
                path_match: CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: false,
                cursor: None,
                limit: MAX_QUERY_PAGE_SIZE,
            },
        )
        .unwrap()
        .fields
        .is_empty());
    }

    #[test]
    fn generic_namespace_profile_materializes_virtual_and_implicit_directories_exact_first() {
        let (store, name) = generic_namespace_store();
        let mut request = generic_search_request(QueryScope::Root);
        request.facets = vec![field("kind"), field("body.producer")];
        let page = search_paths_at(&store, context(&store), &request).unwrap();

        assert!(page.hits.is_empty());
        assert_eq!(page.match_count, 10);
        assert_eq!(
            page.namespace_hits
                .iter()
                .map(|hit| {
                    hit.relative_path.as_ref().map_or_else(
                        || hit.workbench_id.as_str().to_owned(),
                        |path| format!("{}/{}", hit.workbench_id, path),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                "run-42",
                "run-42/input",
                "run-42/logs",
                "run-42/metadata",
                "run-42/outputs",
                "run-42/outputs/collision",
                "run-42/outputs/collision/child.txt",
                "run-42/outputs/nested",
                "run-42/outputs/nested/file.json",
                "run-42/scripts",
            ]
        );
        let collision = page
            .namespace_hits
            .iter()
            .find(|hit| {
                hit.relative_path
                    .as_ref()
                    .is_some_and(|path| path.as_str() == "outputs/collision")
            })
            .unwrap();
        assert_eq!(collision.kind, GenericNamespaceKind::Artifact);
        assert_eq!(collision.artifact.as_ref().unwrap().logical_size, 10);
        let nested = page
            .namespace_hits
            .iter()
            .find(|hit| {
                hit.relative_path
                    .as_ref()
                    .is_some_and(|path| path.as_str() == "outputs/nested")
            })
            .unwrap();
        assert_eq!(nested.kind, GenericNamespaceKind::Directory);
        assert!(nested.artifact.is_none());
        assert_eq!(
            page.facets,
            vec![
                FacetResult {
                    field_id: field("kind"),
                    buckets: vec![
                        FacetBucket {
                            value: QueryScalar::String("directory".to_owned()),
                            count: 7,
                        },
                        FacetBucket {
                            value: QueryScalar::String("file".to_owned()),
                            count: 3,
                        },
                    ],
                    distinct_count: 2,
                    truncated: false,
                },
                FacetResult {
                    field_id: field("body.producer"),
                    buckets: vec![
                        FacetBucket {
                            value: QueryScalar::String("alpha".to_owned()),
                            count: 2,
                        },
                        FacetBucket {
                            value: QueryScalar::String("beta".to_owned()),
                            count: 1,
                        },
                    ],
                    distinct_count: 2,
                    truncated: false,
                },
            ]
        );

        request.scope = QueryScope::Workspace(name);
        request.path_prefix = Some(path("outputs"));
        request.facets.clear();
        let scoped = search_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(scoped.match_count, 4);
        assert!(scoped.namespace_hits.iter().all(|hit| {
            hit.relative_path
                .as_ref()
                .is_some_and(|path| path.as_str() != "outputs")
        }));
    }

    #[test]
    fn generic_size_bytes_accepts_only_finite_numeric_string_predicates() {
        let (store, name) = generic_namespace_store();
        let matching_paths = |operator, operand| {
            let mut request = generic_search_request(QueryScope::Workspace(name.clone()));
            request.path_prefix = Some(path("outputs"));
            request.predicates = vec![QueryPredicate {
                field_id: field("size_bytes"),
                operator,
                operand,
            }];
            search_paths_at(&store, context(&store), &request).map(|page| {
                page.namespace_hits
                    .into_iter()
                    .map(|hit| hit.relative_path.unwrap().as_str().to_owned())
                    .collect::<Vec<_>>()
            })
        };

        assert_eq!(
            matching_paths(
                QueryOperator::Equal,
                scalar_operand(QueryScalar::String("10".to_owned())),
            )
            .unwrap(),
            vec!["outputs/collision"]
        );
        assert_eq!(
            matching_paths(
                QueryOperator::GreaterOrEqual,
                scalar_operand(QueryScalar::String("10".to_owned())),
            )
            .unwrap(),
            vec![
                "outputs/collision",
                "outputs/collision/child.txt",
                "outputs/nested/file.json",
            ]
        );
        assert_eq!(
            matching_paths(
                QueryOperator::In,
                set_operand([
                    QueryScalar::String("10".to_owned()),
                    QueryScalar::String("30".to_owned()),
                ]),
            )
            .unwrap(),
            vec!["outputs/collision", "outputs/nested/file.json"]
        );

        for invalid in ["NaN", "inf", "", "not-a-number"] {
            assert!(matches!(
                matching_paths(
                    QueryOperator::Equal,
                    scalar_operand(QueryScalar::String(invalid.to_owned())),
                ),
                Err(QueryError::InvalidPredicate { field_id, .. })
                    if field_id == field("size_bytes")
            ));
        }

        let mut native = search_request(QueryScope::Workspace(name));
        native.predicates = vec![QueryPredicate {
            field_id: field("logical_size"),
            operator: QueryOperator::Equal,
            operand: scalar_operand(QueryScalar::String("10".to_owned())),
        }];
        assert!(matches!(
            search_paths_at(&store, context(&store), &native),
            Err(QueryError::InvalidPredicate { field_id, .. })
                if field_id == field("logical_size")
        ));
    }

    #[test]
    fn generic_directory_size_is_filterable_but_omitted_from_projection_and_sorted_last() {
        let store = ready_store();
        let name = workbench("generic-size-view");
        let incarnation_id = incarnation(24);
        put_records(
            &store,
            3,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(
                    incarnation_id,
                    "outputs/a-dir/child",
                    &entry(1, 1, Vec::new()),
                ),
                path_row(
                    incarnation_id,
                    "outputs/z-file",
                    &entry(0, 1, vec![("run.optional", QueryScalar::Unsigned(0))]),
                ),
            ],
        );
        let selected_names = QueryPredicate {
            field_id: field("name"),
            operator: QueryOperator::In,
            operand: set_operand([
                QueryScalar::String("a-dir".to_owned()),
                QueryScalar::String("z-file".to_owned()),
            ]),
        };

        for direction in [
            QuerySortDirection::Ascending,
            QuerySortDirection::Descending,
        ] {
            let page = search_paths_at(
                &store,
                context(&store),
                &SearchRequest {
                    profile: generic_profile(),
                    scope: QueryScope::Workspace(name.clone()),
                    path_prefix: Some(path("outputs")),
                    predicates: vec![selected_names.clone()],
                    projection: vec![field("size_bytes")],
                    sort: vec![QuerySort {
                        field_id: field("size_bytes"),
                        direction,
                    }],
                    facets: Vec::new(),
                    cursor: None,
                    limit: 10,
                },
            )
            .unwrap();
            assert_eq!(
                page.namespace_hits
                    .iter()
                    .map(|hit| hit.relative_path.as_ref().unwrap().as_str())
                    .collect::<Vec<_>>(),
                vec!["outputs/z-file", "outputs/a-dir"]
            );
            assert_eq!(
                page.namespace_hits[0].projection,
                BTreeMap::from([(field("size_bytes"), QueryScalar::Unsigned(0))])
            );
            assert!(page.namespace_hits[1].projection.is_empty());
        }

        let zero_size = QueryPredicate {
            field_id: field("size_bytes"),
            operator: QueryOperator::Equal,
            operand: scalar_operand(QueryScalar::Unsigned(0)),
        };
        let filtered = search_paths_at(
            &store,
            context(&store),
            &SearchRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name.clone()),
                path_prefix: Some(path("outputs")),
                predicates: vec![zero_size.clone()],
                projection: Vec::new(),
                sort: vec![QuerySort {
                    field_id: field("path"),
                    direction: QuerySortDirection::Ascending,
                }],
                facets: Vec::new(),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(
            filtered
                .namespace_hits
                .iter()
                .map(|hit| hit.relative_path.as_ref().unwrap().as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/a-dir", "outputs/z-file"]
        );

        let aggregate = aggregate_paths_at(
            &store,
            context(&store),
            &AggregateRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name),
                path_prefix: Some(path("outputs")),
                predicates: vec![zero_size],
                group_by: Vec::new(),
                aggregates: vec![AggregateSpec {
                    result_id: field("sized_rows"),
                    function: AggregateFunction::Count,
                    field_id: Some(field("size_bytes")),
                }],
                sort: Vec::new(),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(aggregate.input_match_count, 2);
        assert_eq!(aggregate.row_count, 2);
        assert_eq!(aggregate.group_count, 1);
        assert_eq!(
            aggregate.groups[0].values.get(&field("sized_rows")),
            Some(&QueryScalar::Unsigned(2))
        );

        for (direction, expected) in [
            (
                QuerySortDirection::Ascending,
                vec!["outputs/a-dir/child", "outputs/z-file"],
            ),
            (
                QuerySortDirection::Descending,
                vec!["outputs/z-file", "outputs/a-dir/child"],
            ),
        ] {
            let native = search_paths_at(
                &store,
                context(&store),
                &SearchRequest {
                    profile: QueryProfile::ArtifactV1,
                    scope: QueryScope::Workspace(workbench("generic-size-view")),
                    path_prefix: Some(path("outputs")),
                    predicates: Vec::new(),
                    projection: Vec::new(),
                    sort: vec![QuerySort {
                        field_id: field("run.optional"),
                        direction,
                    }],
                    facets: Vec::new(),
                    cursor: None,
                    limit: 10,
                },
            )
            .unwrap();
            assert_eq!(
                native
                    .hits
                    .iter()
                    .map(|hit| hit.path.as_str())
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[test]
    fn generic_presentation_paths_drive_predicates_projection_groups_and_cursor_binding() {
        let (store, name) = generic_namespace_store();
        let matching_paths = |operator, value: &str| {
            let mut request = generic_search_request(QueryScope::Root);
            request.predicates = vec![QueryPredicate {
                field_id: field("path"),
                operator,
                operand: scalar_operand(QueryScalar::String(value.to_owned())),
            }];
            request.projection = vec![field("path")];
            search_paths_at(&store, context(&store), &request)
                .unwrap()
                .namespace_hits
                .into_iter()
                .map(|hit| hit.projection[&field("path")].clone())
                .collect::<Vec<_>>()
        };

        assert_eq!(matching_paths(QueryOperator::Prefix, "/agents").len(), 10);
        assert_eq!(
            matching_paths(QueryOperator::Contains, "agents/run-42").len(),
            10
        );
        assert_eq!(
            matching_paths(QueryOperator::Suffix, "agents/run-42"),
            vec![QueryScalar::String("/agents/run-42".to_owned())]
        );

        let aggregate = aggregate_paths_at(
            &store,
            context(&store),
            &AggregateRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name),
                path_prefix: Some(path("outputs")),
                predicates: Vec::new(),
                group_by: vec![field("path")],
                aggregates: vec![AggregateSpec {
                    result_id: field("rows"),
                    function: AggregateFunction::Count,
                    field_id: None,
                }],
                sort: vec![QuerySort {
                    field_id: field("path"),
                    direction: QuerySortDirection::Ascending,
                }],
                cursor: None,
                limit: MAX_QUERY_PAGE_SIZE,
            },
        )
        .unwrap();
        assert_eq!(aggregate.group_count, 4);
        assert!(aggregate.groups.iter().all(|group| {
            matches!(
                group.keys.get(&field("path")),
                Some(QueryScalar::String(path)) if path.starts_with("/agents/run-42/outputs/")
            )
        }));

        let mut first_request = generic_search_request(QueryScope::Root);
        first_request.limit = 1;
        let first = search_paths_at(&store, context(&store), &first_request).unwrap();
        let mut drifted = first_request;
        drifted.profile = QueryProfile::GenericNamespaceV1 {
            presentation_path_root: PresentationPathRoot::new("/other-agents").unwrap(),
        };
        drifted.cursor = first.next_cursor;
        assert_eq!(
            search_paths_at(&store, context(&store), &drifted),
            Err(QueryError::CursorQueryMismatch)
        );
    }

    #[test]
    fn generic_builtin_validation_does_not_mask_missing_or_exact_artifact_scope() {
        let (store, name) = generic_namespace_store();
        let search = |path_prefix: &str, field_id: &str| {
            let mut request = generic_search_request(QueryScope::Workspace(name.clone()));
            request.path_prefix = Some(path(path_prefix));
            request.predicates = vec![QueryPredicate {
                field_id: field(field_id),
                operator: QueryOperator::Equal,
                operand: scalar_operand(QueryScalar::String("file".to_owned())),
            }];
            request.projection = vec![field("path")];
            search_paths_at(&store, context(&store), &request)
        };
        let aggregate = |path_prefix: &str| {
            aggregate_paths_at(
                &store,
                context(&store),
                &AggregateRequest {
                    profile: generic_profile(),
                    scope: QueryScope::Workspace(name.clone()),
                    path_prefix: Some(path(path_prefix)),
                    predicates: vec![QueryPredicate {
                        field_id: field("kind"),
                        operator: QueryOperator::Equal,
                        operand: scalar_operand(QueryScalar::String("file".to_owned())),
                    }],
                    group_by: vec![field("path")],
                    aggregates: vec![AggregateSpec {
                        result_id: field("rows"),
                        function: AggregateFunction::Count,
                        field_id: None,
                    }],
                    sort: Vec::new(),
                    cursor: None,
                    limit: MAX_QUERY_PAGE_SIZE,
                },
            )
        };

        for path_prefix in ["outputs/missing", "outputs/collision"] {
            search(path_prefix, "kind").unwrap();
            aggregate(path_prefix).unwrap();
        }
        assert!(matches!(
            search("outputs/missing", "run.score"),
            Err(QueryError::UnknownField { field_id }) if field_id == field("run.score")
        ));
    }

    #[test]
    fn generic_catalog_exposes_only_legacy_builtins_and_exact_artifacts_are_empty() {
        let (store, name) = generic_namespace_store();
        let mut request = CatalogRequest {
            profile: generic_profile(),
            scope: QueryScope::Workspace(name),
            path_prefix: Some(path("outputs")),
            path_match: CatalogPathMatch::Prefix,
            field_prefix: None,
            include_facets: false,
            cursor: None,
            limit: MAX_QUERY_PAGE_SIZE,
        };
        let page = catalog_fields_at(&store, context(&store), &request).unwrap();
        let builtin_ids = page
            .fields
            .iter()
            .filter(|field_| !field_.field_id.as_str().starts_with("run."))
            .map(|field_| field_.field_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            builtin_ids,
            vec![
                "body.content_type",
                "body.manifest_id",
                "body.producer",
                "kind",
                "name",
                "path",
                "size_bytes",
            ]
        );
        for forbidden in [
            "workbench_id",
            "generation",
            "logical_size",
            "body_digest_uri",
            "content_type",
            "producer",
            "manifest_id",
        ] {
            assert!(!page
                .fields
                .iter()
                .any(|field_| field_.field_id.as_str() == forbidden));
        }
        let path_field = page
            .fields
            .iter()
            .find(|field_| field_.field_id == field("path"))
            .unwrap();
        assert_eq!(
            path_field.operators,
            vec![
                QueryOperator::Equal,
                QueryOperator::NotEqual,
                QueryOperator::In,
                QueryOperator::Prefix,
                QueryOperator::Suffix,
                QueryOperator::Contains,
                QueryOperator::Exists,
                QueryOperator::NotExists,
            ]
        );
        assert!(path_field.sortable);
        assert!(!path_field.facetable);
        assert!(!path_field.aggregatable);

        request.field_prefix = Some("producer".to_owned());
        let filtered = catalog_fields_at(&store, context(&store), &request).unwrap();
        assert_eq!(
            filtered
                .fields
                .iter()
                .map(|field_| field_.field_id.as_str())
                .collect::<Vec<_>>(),
            vec!["body.producer"]
        );

        request.field_prefix = None;
        request.path_prefix = Some(path("outputs/collision"));
        request.path_match = CatalogPathMatch::Exact;
        assert!(catalog_fields_at(&store, context(&store), &request)
            .unwrap()
            .fields
            .is_empty());
    }

    #[test]
    fn generic_aggregate_reports_legacy_input_row_and_group_totals() {
        let (store, name) = generic_namespace_store();
        let request = AggregateRequest {
            profile: generic_profile(),
            scope: QueryScope::Workspace(name.clone()),
            path_prefix: Some(path("outputs")),
            predicates: Vec::new(),
            group_by: vec![field("body.producer")],
            aggregates: vec![
                AggregateSpec {
                    result_id: field("rows"),
                    function: AggregateFunction::Count,
                    field_id: None,
                },
                AggregateSpec {
                    result_id: field("bytes"),
                    function: AggregateFunction::Sum,
                    field_id: Some(field("size_bytes")),
                },
            ],
            sort: vec![QuerySort {
                field_id: field("body.producer"),
                direction: QuerySortDirection::Ascending,
            }],
            cursor: None,
            limit: MAX_QUERY_PAGE_SIZE,
        };
        let page = aggregate_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(page.input_match_count, 4);
        assert_eq!(page.row_count, 3);
        assert_eq!(page.group_count, 2);
        assert_eq!(page.groups.len(), 2);
        assert_eq!(
            page.groups[0].keys.get(&field("body.producer")),
            Some(&QueryScalar::String("alpha".to_owned()))
        );
        assert_eq!(
            page.groups[0].values.get(&field("rows")),
            Some(&QueryScalar::Unsigned(2))
        );
        assert_eq!(
            page.groups[0].values.get(&field("bytes")),
            Some(&QueryScalar::Float(FiniteFloat::new(30.0).unwrap()))
        );

        let ungrouped = aggregate_paths_at(
            &store,
            context(&store),
            &AggregateRequest {
                profile: generic_profile(),
                scope: QueryScope::Workspace(name),
                path_prefix: Some(path("outputs")),
                predicates: Vec::new(),
                group_by: Vec::new(),
                aggregates: vec![AggregateSpec {
                    result_id: field("rows"),
                    function: AggregateFunction::Count,
                    field_id: None,
                }],
                sort: Vec::new(),
                cursor: None,
                limit: MAX_QUERY_PAGE_SIZE,
            },
        )
        .unwrap();
        assert_eq!(ungrouped.input_match_count, 4);
        assert_eq!(ungrouped.row_count, 4);
        assert_eq!(ungrouped.group_count, 1);
        assert_eq!(
            ungrouped.groups[0].values.get(&field("rows")),
            Some(&QueryScalar::Unsigned(4))
        );
    }

    #[test]
    fn query_cursor_commitment_binds_the_profile() {
        let (store, name) = generic_namespace_store();
        let mut request = generic_search_request(QueryScope::Workspace(name));
        request.limit = 1;
        let first = search_paths_at(&store, context(&store), &request).unwrap();
        assert!(first.next_cursor.is_some());
        request.cursor = first.next_cursor;
        request.profile = QueryProfile::ArtifactV1;
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::CursorQueryMismatch)
        );
    }

    /// Commit seals the run-manifest path row with a zero-byte stored
    /// projection; queries must materialize it as an empty projection
    /// instead of failing the whole page.
    #[test]
    fn search_materializes_zero_byte_stored_projection_as_empty() {
        let store = ready_store();
        let name = workbench("committed");
        let incarnation_id = incarnation(8);
        let mut manifest_entry = entry(7, 1, Vec::new());
        manifest_entry.typed_index_projection = Vec::new();
        put_records(
            &store,
            5,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(
                    incarnation_id,
                    "input/note.txt",
                    &entry(
                        6,
                        1,
                        vec![("visible.kind", QueryScalar::String("note".into()))],
                    ),
                ),
                path_row(
                    incarnation_id,
                    "metadata/run_manifest.json",
                    &manifest_entry,
                ),
            ],
        );

        let request = search_request(QueryScope::Workspace(name.clone()));
        let page = search_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["input/note.txt", "metadata/run_manifest.json"]
        );
    }

    #[test]
    fn search_predicates_sort_projection_facets_and_aggregate_are_deterministic() {
        let store = ready_store();
        let name = workbench("analytics");
        let incarnation_id = incarnation(6);
        put_records(
            &store,
            3,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(
                    incarnation_id,
                    "outputs/one",
                    &entry(
                        10,
                        1,
                        vec![
                            ("run.group", QueryScalar::String("blue".into())),
                            ("run.score", QueryScalar::Unsigned(10)),
                        ],
                    ),
                ),
                path_row(
                    incarnation_id,
                    "outputs/two",
                    &entry(
                        20,
                        1,
                        vec![
                            ("run.group", QueryScalar::String("blue".into())),
                            ("run.score", QueryScalar::Unsigned(21)),
                        ],
                    ),
                ),
                path_row(
                    incarnation_id,
                    "outputs/three",
                    &entry(
                        30,
                        1,
                        vec![
                            ("run.group", QueryScalar::String("red".into())),
                            ("run.score", QueryScalar::Unsigned(30)),
                        ],
                    ),
                ),
            ],
        );

        let search = SearchRequest {
            profile: QueryProfile::ArtifactV1,
            scope: QueryScope::Workspace(name.clone()),
            path_prefix: Some(path("outputs")),
            predicates: vec![QueryPredicate {
                field_id: field("run.score"),
                operator: QueryOperator::GreaterOrEqual,
                operand: QueryScalar::Unsigned(20).into(),
            }],
            projection: vec![field("run.group"), field("run.score")],
            sort: vec![QuerySort {
                field_id: field("run.score"),
                direction: QuerySortDirection::Descending,
            }],
            facets: vec![field("run.group")],
            cursor: None,
            limit: 10,
        };
        let page = search_paths_at(&store, context(&store), &search).unwrap();
        assert_eq!(
            page.hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/three", "outputs/two"]
        );
        assert_eq!(
            page.facets,
            vec![FacetResult {
                field_id: field("run.group"),
                buckets: vec![
                    FacetBucket {
                        value: QueryScalar::String("blue".into()),
                        count: 1,
                    },
                    FacetBucket {
                        value: QueryScalar::String("red".into()),
                        count: 1,
                    },
                ],
                distinct_count: 2,
                truncated: false,
            }]
        );

        let aggregate = aggregate_paths_at(
            &store,
            context(&store),
            &AggregateRequest {
                profile: QueryProfile::ArtifactV1,
                scope: QueryScope::Workspace(name),
                path_prefix: Some(path("outputs")),
                predicates: Vec::new(),
                group_by: vec![field("run.group")],
                aggregates: vec![
                    AggregateSpec {
                        result_id: field("rows"),
                        function: AggregateFunction::Count,
                        field_id: None,
                    },
                    AggregateSpec {
                        result_id: field("score_sum"),
                        function: AggregateFunction::Sum,
                        field_id: Some(field("run.score")),
                    },
                    AggregateSpec {
                        result_id: field("score_avg"),
                        function: AggregateFunction::Average,
                        field_id: Some(field("run.score")),
                    },
                    AggregateSpec {
                        result_id: field("score_min"),
                        function: AggregateFunction::Minimum,
                        field_id: Some(field("run.score")),
                    },
                    AggregateSpec {
                        result_id: field("score_max"),
                        function: AggregateFunction::Maximum,
                        field_id: Some(field("run.score")),
                    },
                ],
                sort: vec![QuerySort {
                    field_id: field("score_sum"),
                    direction: QuerySortDirection::Descending,
                }],
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(aggregate.groups.len(), 2);
        assert_eq!(
            aggregate.groups[0].keys.get(&field("run.group")),
            Some(&QueryScalar::String("blue".into()))
        );
        assert_eq!(
            aggregate.groups[0].values.get(&field("rows")),
            Some(&QueryScalar::Unsigned(2))
        );
        assert_eq!(
            aggregate.groups[0].values.get(&field("score_sum")),
            Some(&QueryScalar::Unsigned(31))
        );
        assert_eq!(
            aggregate.groups[0].values.get(&field("score_avg")),
            Some(&QueryScalar::Float(FiniteFloat::new(15.5).unwrap()))
        );
        assert_eq!(
            aggregate.groups[0].values.get(&field("score_min")),
            Some(&QueryScalar::Unsigned(10))
        );
        assert_eq!(
            aggregate.groups[0].values.get(&field("score_max")),
            Some(&QueryScalar::Unsigned(21))
        );
    }

    #[test]
    fn suffix_is_strictly_typed_and_pages_in_stable_sort_order() {
        let (store, name) = predicate_store();
        let predicate = QueryPredicate {
            field_id: field("path"),
            operator: QueryOperator::Suffix,
            operand: scalar_operand(QueryScalar::String(".log".to_owned())),
        };
        let mut request = SearchRequest {
            profile: QueryProfile::ArtifactV1,
            scope: QueryScope::Workspace(name.clone()),
            path_prefix: Some(path("outputs")),
            predicates: vec![predicate.clone()],
            projection: vec![field("path")],
            sort: vec![QuerySort {
                field_id: field("path"),
                direction: QuerySortDirection::Ascending,
            }],
            facets: Vec::new(),
            cursor: None,
            limit: 1,
        };
        let first = search_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(first.hits[0].path.as_str(), "outputs/alpha.log");
        assert!(first.next_cursor.is_some());

        let mut changed = request.clone();
        changed.predicates[0].operand = scalar_operand(QueryScalar::String(".txt".to_owned()));
        changed.cursor = first.next_cursor.clone();
        assert_eq!(
            search_paths_at(&store, context(&store), &changed),
            Err(QueryError::CursorQueryMismatch)
        );

        let mut paths = Vec::new();
        loop {
            let page = search_paths_at(&store, context(&store), &request).unwrap();
            paths.extend(page.hits.into_iter().map(|hit| hit.path));
            let Some(cursor) = page.next_cursor else {
                break;
            };
            request.cursor = Some(cursor);
        }
        assert_eq!(
            paths
                .iter()
                .map(NormalizedRelativePath::as_str)
                .collect::<Vec<_>>(),
            vec![
                "outputs/alpha.log",
                "outputs/delta.log",
                "outputs/gamma.log"
            ]
        );
        assert_eq!(aggregate_count(&store, name.clone(), vec![predicate]), 3);

        let mut bytes_request = search_request(QueryScope::Workspace(name.clone()));
        bytes_request.predicates = vec![QueryPredicate {
            field_id: field("run.bytes"),
            operator: QueryOperator::Suffix,
            operand: scalar_operand(QueryScalar::Bytes(b"tail".to_vec())),
        }];
        assert_eq!(
            search_paths_at(&store, context(&store), &bytes_request)
                .unwrap()
                .hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/alpha.log", "outputs/beta.txt", "outputs/delta.log"]
        );

        bytes_request.predicates[0].operand =
            scalar_operand(QueryScalar::String("tail".to_owned()));
        assert_eq!(
            search_paths_at(&store, context(&store), &bytes_request),
            Err(QueryError::InvalidPredicate {
                field_id: field("run.bytes"),
                reason: "comparison value has a different scalar type",
            })
        );
        bytes_request.predicates[0] = QueryPredicate {
            field_id: field("run.score"),
            operator: QueryOperator::Suffix,
            operand: scalar_operand(QueryScalar::Unsigned(0)),
        };
        assert_eq!(
            search_paths_at(&store, context(&store), &bytes_request),
            Err(QueryError::InvalidPredicate {
                field_id: field("run.score"),
                reason: "operator is unsupported for this field type",
            })
        );
    }

    #[test]
    fn in_uses_one_bounded_canonical_set_with_empty_set_matching_nothing() {
        let (store, name) = predicate_store();
        let predicate = QueryPredicate {
            field_id: field("run.label"),
            operator: QueryOperator::In,
            operand: set_operand([
                QueryScalar::String("green".to_owned()),
                QueryScalar::String("blue".to_owned()),
                QueryScalar::String("blue".to_owned()),
            ]),
        };
        let mut request = search_request(QueryScope::Workspace(name.clone()));
        request.predicates = vec![predicate.clone()];
        assert_eq!(
            search_paths_at(&store, context(&store), &request)
                .unwrap()
                .hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/alpha.log", "outputs/delta.log"]
        );
        assert_eq!(aggregate_count(&store, name.clone(), vec![predicate]), 2);

        let empty = QueryPredicate {
            field_id: field("run.label"),
            operator: QueryOperator::In,
            operand: set_operand([]),
        };
        request.predicates = vec![empty.clone()];
        assert!(search_paths_at(&store, context(&store), &request)
            .unwrap()
            .hits
            .is_empty());
        assert_eq!(aggregate_count(&store, name.clone(), vec![empty]), 0);

        let too_many = (0..=MAX_QUERY_IN_VALUES)
            .map(|index| QueryScalar::String(format!("value-{index:03}")))
            .collect();
        request.predicates = vec![QueryPredicate {
            field_id: field("run.label"),
            operator: QueryOperator::In,
            operand: QueryOperand::Set(too_many),
        }];
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::BoundExceeded {
                field: "predicate_in_values",
                count: MAX_QUERY_IN_VALUES + 1,
                max: MAX_QUERY_IN_VALUES,
            })
        );

        request.predicates[0] = QueryPredicate {
            field_id: field("run.label"),
            operator: QueryOperator::In,
            operand: scalar_operand(QueryScalar::String("blue".to_owned())),
        };
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::InvalidPredicate {
                field_id: field("run.label"),
                reason: "in requires a canonical set operand",
            })
        );

        request.predicates[0].operand = set_operand([
            QueryScalar::String("blue".to_owned()),
            QueryScalar::Unsigned(1),
        ]);
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::InvalidPredicate {
                field_id: field("run.label"),
                reason: "comparison value has a different scalar type",
            })
        );

        request.predicates[0] = QueryPredicate {
            field_id: field("run.label"),
            operator: QueryOperator::Equal,
            operand: set_operand([QueryScalar::String("blue".to_owned())]),
        };
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::InvalidPredicate {
                field_id: field("run.label"),
                reason: "comparison operator requires one scalar operand",
            })
        );
    }

    #[test]
    fn not_exists_means_absent_while_null_remains_a_present_scalar() {
        let (store, name) = predicate_store();
        let mut request = search_request(QueryScope::Workspace(name.clone()));
        let missing_label = QueryPredicate {
            field_id: field("run.label"),
            operator: QueryOperator::NotExists,
            operand: QueryOperand::None,
        };
        request.predicates = vec![missing_label.clone()];
        assert_eq!(
            search_paths_at(&store, context(&store), &request)
                .unwrap()
                .hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/gamma.log"]
        );
        assert_eq!(
            aggregate_count(&store, name.clone(), vec![missing_label]),
            1
        );

        request.predicates = vec![QueryPredicate {
            field_id: field("run.null"),
            operator: QueryOperator::NotExists,
            operand: QueryOperand::None,
        }];
        assert_eq!(
            search_paths_at(&store, context(&store), &request)
                .unwrap()
                .hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/beta.txt", "outputs/delta.log", "outputs/gamma.log"]
        );

        request.predicates[0] = QueryPredicate {
            field_id: field("run.null"),
            operator: QueryOperator::Equal,
            operand: scalar_operand(QueryScalar::Null),
        };
        assert_eq!(
            search_paths_at(&store, context(&store), &request)
                .unwrap()
                .hits
                .iter()
                .map(|hit| hit.path.as_str())
                .collect::<Vec<_>>(),
            vec!["outputs/alpha.log"]
        );

        request.predicates[0] = QueryPredicate {
            field_id: field("producer"),
            operator: QueryOperator::NotExists,
            operand: QueryOperand::None,
        };
        assert!(search_paths_at(&store, context(&store), &request)
            .unwrap()
            .hits
            .is_empty());

        request.predicates[0].operand = scalar_operand(QueryScalar::Null);
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::InvalidPredicate {
                field_id: field("producer"),
                reason: "existence operators require the no-value operand",
            })
        );
    }

    #[test]
    fn catalog_advertises_only_executable_operator_sets_and_pages_stably() {
        let (store, name) = predicate_store();
        let mut request = CatalogRequest {
            profile: QueryProfile::ArtifactV1,
            scope: QueryScope::Workspace(name),
            path_prefix: Some(path("outputs")),
            path_match: CatalogPathMatch::Prefix,
            field_prefix: None,
            include_facets: false,
            cursor: None,
            limit: MAX_QUERY_PAGE_SIZE,
        };
        let full = catalog_fields_at(&store, context(&store), &request).unwrap();
        let operators = |field_id: &str| {
            full.fields
                .iter()
                .find(|candidate| candidate.field_id == field(field_id))
                .unwrap()
                .operators
                .clone()
        };
        let text_operators = vec![
            QueryOperator::Equal,
            QueryOperator::NotEqual,
            QueryOperator::In,
            QueryOperator::Less,
            QueryOperator::LessOrEqual,
            QueryOperator::Greater,
            QueryOperator::GreaterOrEqual,
            QueryOperator::Prefix,
            QueryOperator::Suffix,
            QueryOperator::Contains,
            QueryOperator::Exists,
            QueryOperator::NotExists,
        ];
        assert_eq!(operators("path"), text_operators);
        assert_eq!(operators("run.bytes"), text_operators);
        assert_eq!(
            operators("run.score"),
            vec![
                QueryOperator::Equal,
                QueryOperator::NotEqual,
                QueryOperator::In,
                QueryOperator::Less,
                QueryOperator::LessOrEqual,
                QueryOperator::Greater,
                QueryOperator::GreaterOrEqual,
                QueryOperator::Exists,
                QueryOperator::NotExists,
            ]
        );
        assert_eq!(
            operators("run.null"),
            vec![
                QueryOperator::Equal,
                QueryOperator::NotEqual,
                QueryOperator::In,
                QueryOperator::Less,
                QueryOperator::LessOrEqual,
                QueryOperator::Greater,
                QueryOperator::GreaterOrEqual,
                QueryOperator::Exists,
                QueryOperator::NotExists,
            ]
        );

        let expected = full
            .fields
            .iter()
            .map(|catalog| catalog.field_id.clone())
            .collect::<Vec<_>>();
        request.limit = 3;
        let mut paged = Vec::new();
        loop {
            let page = catalog_fields_at(&store, context(&store), &request).unwrap();
            paged.extend(page.fields.into_iter().map(|catalog| catalog.field_id));
            let Some(cursor) = page.next_cursor else {
                break;
            };
            request.cursor = Some(cursor);
        }
        assert_eq!(paged, expected);
        assert!(paged.windows(2).all(|pair| pair[0] < pair[1]));
    }

    #[test]
    fn catalog_exact_and_prefix_modes_are_distinct_and_exact_requires_a_path() {
        let store = ready_store();
        let name = workbench("catalog-path-match");
        let incarnation_id = incarnation(61);
        put_records(
            &store,
            3,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(
                    incarnation_id,
                    "outputs/collision",
                    &entry(
                        10,
                        1,
                        vec![("exact.only", QueryScalar::String("exact".to_owned()))],
                    ),
                ),
                path_row(
                    incarnation_id,
                    "outputs/collision/child.txt",
                    &entry(
                        20,
                        1,
                        vec![("child.only", QueryScalar::String("child".to_owned()))],
                    ),
                ),
            ],
        );
        let mut request = CatalogRequest {
            profile: QueryProfile::ArtifactV1,
            scope: QueryScope::Workspace(name),
            path_prefix: Some(path("outputs/collision")),
            path_match: CatalogPathMatch::Prefix,
            field_prefix: None,
            include_facets: false,
            cursor: None,
            limit: MAX_QUERY_PAGE_SIZE,
        };
        let prefix = catalog_fields_at(&store, context(&store), &request).unwrap();
        assert!(prefix
            .fields
            .iter()
            .any(|field_| field_.field_id == field("exact.only")));
        assert!(prefix
            .fields
            .iter()
            .any(|field_| field_.field_id == field("child.only")));

        request.path_match = CatalogPathMatch::Exact;
        let exact = catalog_fields_at(&store, context(&store), &request).unwrap();
        assert!(exact
            .fields
            .iter()
            .any(|field_| field_.field_id == field("exact.only")));
        assert!(!exact
            .fields
            .iter()
            .any(|field_| field_.field_id == field("child.only")));

        request.path_prefix = None;
        assert_eq!(
            catalog_fields_at(&store, context(&store), &request),
            Err(QueryError::ExactCatalogRequiresPath)
        );
    }

    #[test]
    fn catalog_cursor_commitment_binds_the_path_match_mode() {
        let (store, name) = predicate_store();
        let mut request = CatalogRequest {
            profile: QueryProfile::ArtifactV1,
            scope: QueryScope::Workspace(name),
            path_prefix: Some(path("outputs")),
            path_match: CatalogPathMatch::Prefix,
            field_prefix: None,
            include_facets: false,
            cursor: None,
            limit: 1,
        };
        let first = catalog_fields_at(&store, context(&store), &request).unwrap();
        request.cursor = first.next_cursor;
        request.path_match = CatalogPathMatch::Exact;
        assert_eq!(
            catalog_fields_at(&store, context(&store), &request),
            Err(QueryError::CursorQueryMismatch)
        );
    }

    #[test]
    fn historical_context_freezes_rows_and_rejects_cross_version_cursor() {
        let store = ready_store();
        let name = workbench("frozen");
        let incarnation_id = incarnation(7);
        let old_entry = entry(
            10,
            1,
            vec![("run.phase", QueryScalar::String("old".into()))],
        );
        let created = put_records(
            &store,
            3,
            vec![
                workspace_row(&name, incarnation_id, WorkspaceState::Visible),
                path_row(incarnation_id, "outputs/value", &old_entry),
            ],
        );
        let key = path_current_key(root(), incarnation_id, &path("outputs/value"));
        let old_payload = old_entry.encode().unwrap();
        let new_entry = entry(
            20,
            2,
            vec![("run.phase", QueryScalar::String("new".into()))],
        );
        execute(
            &store,
            4,
            vec![CommandPredicate::Value {
                family: MetadataFamily::PathCurrent,
                key: key.clone(),
                expected: Some(old_payload),
            }],
            vec![CommandMutation::Put {
                family: MetadataFamily::PathCurrent,
                key: key.clone(),
                value: new_entry.encode().unwrap(),
            }],
            vec![HistoryProjection {
                family: MetadataFamily::PathCurrent,
                key,
            }],
            Vec::new(),
        );

        let mut request = search_request(QueryScope::Workspace(name));
        request.projection = vec![field("run.phase")];
        request.limit = 1;
        let frozen = search_paths_at(&store, context_at(created), &request).unwrap();
        assert_eq!(frozen.hits[0].generation.get(), 1);
        assert_eq!(
            frozen.hits[0].projection.get(&field("run.phase")),
            Some(&QueryScalar::String("old".into()))
        );
        let live = search_paths_at(&store, context(&store), &request).unwrap();
        assert_eq!(live.hits[0].generation.get(), 2);
        assert_eq!(
            live.hits[0].projection.get(&field("run.phase")),
            Some(&QueryScalar::String("new".into()))
        );

        let forged = encode_cursor(
            CursorKind::Search,
            context_at(created).read_version,
            search_digest(root(), &request),
            b"missing",
        );
        request.cursor = Some(forged);
        assert_eq!(
            search_paths_at(&store, context(&store), &request),
            Err(QueryError::CursorReadVersionMismatch {
                cursor: created.get(),
                request: context(&store).read_version.get(),
            })
        );
    }

    #[test]
    fn committed_discovery_uses_visible_marker_and_head_scans() {
        let store = ready_store();
        let committed = workbench("committed");
        let open = workbench("open");
        let hidden = workbench("hidden");
        let committed_incarnation = incarnation(8);
        let open_incarnation = incarnation(9);
        let hidden_incarnation = incarnation(10);
        let commit_id = CommitId::from_bytes([0x55; SHA256_BYTES]);
        put_records(
            &store,
            3,
            vec![
                workspace_row(&committed, committed_incarnation, WorkspaceState::Visible),
                workspace_row(&open, open_incarnation, WorkspaceState::Visible),
                workspace_row(&hidden, hidden_incarnation, WorkspaceState::Staging),
                (
                    MetadataFamily::WorkbenchCommitHead,
                    workbench_commit_head_key(root(), committed_incarnation),
                    WorkbenchCommitHeadRecord {
                        commit_id,
                        head_generation: Generation::new(2).unwrap(),
                    }
                    .encode(),
                ),
                (
                    MetadataFamily::WorkbenchCommitHead,
                    workbench_commit_head_key(root(), hidden_incarnation),
                    WorkbenchCommitHeadRecord {
                        commit_id: CommitId::from_bytes([0x66; SHA256_BYTES]),
                        head_generation: Generation::new(1).unwrap(),
                    }
                    .encode(),
                ),
            ],
        );

        let committed_page = find_workspaces_at(
            &store,
            context(&store),
            &FindWorkspacesRequest {
                committed: CommittedFilter::Committed,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(
            committed_page.workspaces,
            vec![WorkspaceDiscovery {
                workbench_id: committed,
                workspace_incarnation_id: committed_incarnation,
                workspace_revision: WorkspaceRevision::new(1),
                commit_id: Some(commit_id),
                commit_head_generation: Some(Generation::new(2).unwrap()),
            }]
        );
        let open_page = find_workspaces_at(
            &store,
            context(&store),
            &FindWorkspacesRequest {
                committed: CommittedFilter::Uncommitted,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(open_page.workspaces.len(), 1);
        assert_eq!(open_page.workspaces[0].workbench_id, open);
    }

    #[test]
    fn find_workspace_cursor_rejects_a_different_storage_root() {
        let store = ready_store();
        let alternate_root = RootId::from_bytes([3; FIXED_ID_BYTES]);
        store
            .execute(&fence_command_at(
                &store,
                alternate_root,
                request(40),
                RootFenceAction::Install,
            ))
            .unwrap();
        store
            .execute(&fence_command_at(
                &store,
                alternate_root,
                request(41),
                RootFenceAction::Transition {
                    expected: RootActivationState::Installing,
                    next: RootActivationState::Active,
                },
            ))
            .unwrap();

        let names = [workbench("alpha"), workbench("beta")];
        put_records(
            &store,
            42,
            names
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    workspace_row(
                        name,
                        incarnation(u8::try_from(index + 50).unwrap()),
                        WorkspaceState::Visible,
                    )
                })
                .collect(),
        );
        let alternate_records = names
            .iter()
            .enumerate()
            .map(|(index, name)| {
                (
                    MetadataFamily::WorkspaceCurrent,
                    workspace_current_key(alternate_root, name),
                    workspace_record(
                        incarnation(u8::try_from(index + 60).unwrap()),
                        WorkspaceState::Visible,
                    )
                    .encode()
                    .unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let predicates = alternate_records
            .iter()
            .map(|(family, key, _)| CommandPredicate::Value {
                family: *family,
                key: key.clone(),
                expected: None,
            })
            .collect();
        let mutations = alternate_records
            .into_iter()
            .map(|(family, key, value)| CommandMutation::Put { family, key, value })
            .collect();
        execute_at(
            &store,
            alternate_root,
            43,
            predicates,
            mutations,
            Vec::new(),
            Vec::new(),
        );

        let mut request = FindWorkspacesRequest {
            committed: CommittedFilter::Any,
            cursor: None,
            limit: 1,
        };
        let first = find_workspaces_at(&store, context(&store), &request).unwrap();
        request.cursor = first.next_cursor;
        let alternate_context = RootReadContext {
            root_id: alternate_root,
            placement_generation: placement(),
            owner_epoch: owner(),
            read_version: context(&store).read_version,
        };
        assert_eq!(
            find_workspaces_at(&store, alternate_context, &request),
            Err(QueryError::CursorQueryMismatch)
        );
    }

    #[test]
    fn strict_change_pages_gate_at_event_version_and_scope() {
        let store = ready_store();
        let visible = workbench("events");
        let hidden = workbench("hidden-events");
        let visible_incarnation = incarnation(11);
        let hidden_incarnation = incarnation(12);
        put_records(
            &store,
            3,
            vec![
                workspace_row(&visible, visible_incarnation, WorkspaceState::Visible),
                workspace_row(&hidden, hidden_incarnation, WorkspaceState::Staging),
            ],
        );
        let visible_event = ChangeEventRecord {
            workbench_id: visible.clone(),
            workspace_incarnation_id: visible_incarnation,
            kind: super::super::query_records::ChangeEventKind::ArtifactPublished,
            artifact_revision_id: None,
            commit_id: None,
            operation_id: None,
            path: Some(path("outputs/result")),
            before: TypedProjection::empty(),
            after: TypedProjection::new(BTreeMap::from([(
                field("run.status"),
                QueryScalar::String("done".into()),
            )]))
            .unwrap(),
        };
        let hidden_event = ChangeEventRecord {
            workbench_id: hidden.clone(),
            workspace_incarnation_id: hidden_incarnation,
            kind: super::super::query_records::ChangeEventKind::WorkspaceRestored,
            artifact_revision_id: None,
            commit_id: None,
            operation_id: None,
            path: None,
            before: TypedProjection::empty(),
            after: TypedProjection::empty(),
        };
        let mismatched_name_event = ChangeEventRecord {
            workbench_id: workbench("forged-events"),
            workspace_incarnation_id: visible_incarnation,
            kind: super::super::query_records::ChangeEventKind::ArtifactPublished,
            artifact_revision_id: None,
            commit_id: None,
            operation_id: None,
            path: Some(path("outputs/forged")),
            before: TypedProjection::empty(),
            after: TypedProjection::empty(),
        };
        let event_version = execute(
            &store,
            4,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![visible_event.clone(), hidden_event, mismatched_name_event],
        );

        let page = read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                scope: QueryScope::Root,
                after_commit_version: None,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].workbench_id, visible);
        assert_eq!(page.events[0].commit_version, event_version);
        assert_eq!(page.events[0].sequence, 0);
        assert_eq!(page.events[0].event, visible_event);

        assert!(read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                scope: QueryScope::Workspace(workbench("other")),
                after_commit_version: None,
                cursor: None,
                limit: 10,
            },
        )
        .unwrap()
        .events
        .is_empty());
        assert!(read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                scope: QueryScope::Root,
                after_commit_version: Some(event_version),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap()
        .events
        .is_empty());

        let current = context(&store);
        let future = CommitVersion::new(current.read_version.get() + 1).unwrap();
        assert_eq!(
            read_changes_at(
                &store,
                current,
                &ChangePageRequest {
                    scope: QueryScope::Root,
                    after_commit_version: Some(future),
                    cursor: None,
                    limit: 10,
                },
            ),
            Err(QueryError::Meta(MetaError::ReadVersionInFuture {
                requested: future.get(),
                current: current.read_version.get(),
            }))
        );
    }

    #[test]
    fn change_cursor_resumes_mid_commit_after_the_root_advances() {
        let store = ready_store();
        let name = workbench("cursor-events");
        let incarnation_id = incarnation(14);
        put_records(
            &store,
            3,
            vec![workspace_row(
                &name,
                incarnation_id,
                WorkspaceState::Visible,
            )],
        );
        let event = |path_value: &str| ChangeEventRecord {
            workbench_id: name.clone(),
            workspace_incarnation_id: incarnation_id,
            kind: super::super::query_records::ChangeEventKind::ArtifactPublished,
            artifact_revision_id: None,
            commit_id: None,
            operation_id: None,
            path: Some(path(path_value)),
            before: TypedProjection::empty(),
            after: TypedProjection::empty(),
        };
        let first_commit = execute(
            &store,
            4,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![event("outputs/first"), event("outputs/second")],
        );
        let request = ChangePageRequest {
            scope: QueryScope::Root,
            after_commit_version: None,
            cursor: None,
            limit: 1,
        };
        let first = read_changes_at(&store, context(&store), &request).unwrap();
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.events[0].commit_version, first_commit);
        assert_eq!(first.events[0].sequence, 0);
        let first_cursor = first.next_cursor.expect("second event is lookahead");

        let later_commit = execute(
            &store,
            5,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![event("outputs/later")],
        );
        let second = read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                cursor: Some(first_cursor.clone()),
                ..request.clone()
            },
        )
        .unwrap();
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].commit_version, first_commit);
        assert_eq!(second.events[0].sequence, 1);
        let second_cursor = second.next_cursor.expect("later commit is lookahead");

        let third = read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                cursor: Some(second_cursor),
                ..request.clone()
            },
        )
        .unwrap();
        assert_eq!(third.events.len(), 1);
        assert_eq!(third.events[0].commit_version, later_commit);
        assert_eq!(third.events[0].sequence, 0);
        assert!(third.next_cursor.is_none());

        let after_first_commit = read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                scope: QueryScope::Root,
                after_commit_version: Some(first_commit),
                cursor: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(after_first_commit.events.len(), 1);
        assert_eq!(after_first_commit.events[0].commit_version, later_commit);

        let workspace_request = ChangePageRequest {
            scope: QueryScope::Workspace(name.clone()),
            cursor: Some(first_cursor.clone()),
            ..request.clone()
        };
        assert_eq!(
            read_changes_at(&store, context(&store), &workspace_request),
            Err(QueryError::CursorQueryMismatch)
        );

        let workspace_first = read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                scope: QueryScope::Workspace(name.clone()),
                cursor: None,
                ..request.clone()
            },
        )
        .unwrap();
        let workspace_cursor = workspace_first
            .next_cursor
            .expect("workspace scope has additional events");
        assert_eq!(
            read_changes_at(
                &store,
                context(&store),
                &ChangePageRequest {
                    scope: QueryScope::Workspace(workbench("other-workspace")),
                    cursor: Some(workspace_cursor),
                    ..request.clone()
                },
            ),
            Err(QueryError::CursorQueryMismatch)
        );

        let mut legacy_hasher = Sha256::new();
        legacy_hasher.update(b"nokv.query.changes.v1\0");
        hash_scope(&mut legacy_hasher, &request.scope);
        legacy_hasher.update([0]);
        let legacy_cursor = encode_cursor(
            CursorKind::Changes,
            first.read_version,
            legacy_hasher.finalize().into(),
            &change_event_identity(&first.events[0]),
        );
        assert_eq!(
            read_changes_at(
                &store,
                context(&store),
                &ChangePageRequest {
                    cursor: Some(legacy_cursor),
                    ..request.clone()
                },
            ),
            Err(QueryError::CursorQueryMismatch)
        );

        let foreign_context = RootReadContext {
            root_id: RootId::from_bytes([9; FIXED_ID_BYTES]),
            ..context(&store)
        };
        assert_eq!(
            decode_change_cursor(
                &first_cursor,
                foreign_context,
                change_digest(foreign_context.root_id, &request),
                None,
            ),
            Err(QueryError::CursorQueryMismatch)
        );
    }

    #[test]
    fn change_page_stops_after_visible_lookahead_before_decoding_later_events() {
        let store = ready_store();
        let name = workbench("bounded-events");
        let incarnation_id = incarnation(15);
        put_records(
            &store,
            3,
            vec![workspace_row(
                &name,
                incarnation_id,
                WorkspaceState::Visible,
            )],
        );
        let valid_event = ChangeEventRecord {
            workbench_id: name.clone(),
            workspace_incarnation_id: incarnation_id,
            kind: super::super::query_records::ChangeEventKind::ArtifactPublished,
            artifact_revision_id: None,
            commit_id: None,
            operation_id: None,
            path: Some(path("outputs/valid")),
            before: TypedProjection::empty(),
            after: TypedProjection::empty(),
        };
        execute(
            &store,
            4,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![valid_event.clone()],
        );
        execute(
            &store,
            5,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![valid_event],
        );

        let mut corrupt = MetadataCommand {
            schema_id: SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(nokv_types::ObjectNamespaceId::from_bytes(
                [10; FIXED_ID_BYTES],
            )),
            placement_generation: placement(),
            owner_epoch: owner(),
            request_id: request(6),
            command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: RootFenceAction::RequireActive,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: vec![EventProjection {
                payload: b"invalid-change-event-envelope".to_vec(),
            }],
            deterministic_result: Vec::new(),
        };
        corrupt = corrupt.seal();
        store.execute(&corrupt).unwrap();

        let page = read_changes_at(
            &store,
            context(&store),
            &ChangePageRequest {
                scope: QueryScope::Root,
                after_commit_version: None,
                cursor: None,
                limit: 1,
            },
        )
        .unwrap();
        assert_eq!(page.events.len(), 1);
        assert!(page.next_cursor.is_some());
    }

    #[test]
    fn more_than_meta_batch_pages_stably_after_file_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let store_path = directory.path().join("query-store");
        let store = crate::workspace::test_support::initialize_file(&store_path, shard()).unwrap();
        activate(&store);
        let name = workbench("large");
        let incarnation_id = incarnation(13);
        put_records(
            &store,
            3,
            vec![workspace_row(
                &name,
                incarnation_id,
                WorkspaceState::Visible,
            )],
        );
        for (batch, range) in [(4, 0..200), (5, 200..350)] {
            let records = range
                .map(|index| {
                    path_row(
                        incarnation_id,
                        &format!("outputs/item-{index:04}"),
                        &entry(
                            index + 1,
                            1,
                            vec![("row.number", QueryScalar::Unsigned(index))],
                        ),
                    )
                })
                .collect();
            put_records(&store, batch, records);
        }
        let frozen = context(&store);
        drop(store);

        let store = crate::workspace::test_support::open_file(&store_path, shard()).unwrap();
        let mut request = search_request(QueryScope::Workspace(name));
        request.projection = vec![field("row.number")];
        request.limit = 73;
        let mut paths = Vec::new();
        loop {
            let page = search_paths_at(&store, frozen, &request).unwrap();
            paths.extend(page.hits.into_iter().map(|hit| hit.path));
            let Some(cursor) = page.next_cursor else {
                break;
            };
            request.cursor = Some(cursor);
        }
        assert_eq!(paths.len(), 350);
        assert_eq!(paths.first().unwrap().as_str(), "outputs/item-0000");
        assert_eq!(paths.last().unwrap().as_str(), "outputs/item-0349");
        assert!(paths.windows(2).all(|pair| pair[0] < pair[1]));

        request.cursor = None;
        let first = search_paths_at(&store, frozen, &request).unwrap();
        let mut changed_request = request;
        changed_request.predicates = vec![QueryPredicate {
            field_id: field("row.number"),
            operator: QueryOperator::Greater,
            operand: QueryScalar::Unsigned(10).into(),
        }];
        changed_request.cursor = first.next_cursor;
        assert_eq!(
            search_paths_at(&store, frozen, &changed_request),
            Err(QueryError::CursorQueryMismatch)
        );
    }
}
