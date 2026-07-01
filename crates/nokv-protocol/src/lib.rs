//! Wire protocol types for NoKV metadata RPC.
//!
//! This crate owns storage-neutral request and response DTOs shared by the
//! server and service clients. It does not execute metadata semantics, know Holt
//! layout, own object-store behavior, or implement path resolution.

use std::fmt;

use nokv_types::{
    AdvisoryLock, AdvisoryLockKind, BlockDescriptor, BodyDescriptor, ChunkManifest, DentryName,
    DentryRecord, FileType, InodeAttr, InodeId, PathMetadata, ReadLease, SliceManifest,
    SnapshotPin,
};
use serde::{Deserialize, Serialize};

const BINARY_CODEC_LIMIT: u64 = 16 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireOpenPathReadPlanRequest {
    pub path: String,
    pub offset: u64,
    pub len: u64,
    pub expected_generation: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireOpenPathReadPlan {
    pub metadata: WirePathMetadata,
    pub lease: WireReadLease,
    pub plan: WireBodyReadPlan,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MetadataRpcRequest {
    Batch {
        requests: Vec<MetadataRpcRequest>,
    },
    BootstrapRoot {
        mode: u32,
        uid: u32,
        gid: u32,
    },
    GetAttr {
        inode: u64,
    },
    GetAttrAtSnapshot {
        snapshot_id: u64,
        inode: u64,
    },
    LookupPlus {
        parent: u64,
        name: String,
    },
    CurrentDentryVersion {
        parent: u64,
        name: String,
    },
    LookupPlusAtSnapshot {
        snapshot_id: u64,
        parent: u64,
        name: String,
    },
    LookupPath {
        path: String,
    },
    StatPath {
        path: String,
    },
    ReadDirPlus {
        parent: u64,
    },
    ReadDirPlusPage {
        parent: u64,
        after_name_hex: Option<String>,
        limit: usize,
    },
    ReadDirPlusAtSnapshot {
        snapshot_id: u64,
        parent: u64,
    },
    ReadDirPlusPath {
        path: String,
    },
    ReadDirPlusPathPage {
        path: String,
        after_name_hex: Option<String>,
        limit: usize,
    },
    ReadIndexedPathPage {
        path: String,
        after_name_hex: Option<String>,
        limit: usize,
    },
    StatCard {
        path: String,
    },
    ListPage {
        path: String,
        cursor: Option<String>,
        limit: u64,
    },
    FindPaths {
        request: Box<WireNamespaceFindRequest>,
    },
    AggregatePaths {
        request: Box<WireNamespaceAggregateRequest>,
    },
    GrepPaths {
        request: Box<WireNamespaceGrepRequest>,
    },
    ReadPage {
        path: String,
        options: Box<WireNamespaceReadOptions>,
    },
    CreateDir {
        parent: u64,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    /// Graft a foreign subtree directory into `parent`'s shard. `target_inode`
    /// is owned by another shard; the handler writes only the dentry projection
    /// (no Inode record). Routed by `parent` like the other inode-keyed creates.
    CreateGraft {
        parent: u64,
        name: String,
        target_inode: u64,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    /// Remove the parent-shard half of a cross-shard graft (the dentry only).
    /// Dedicated teardown path that bypasses the rmdir graft guard. Routed by
    /// `parent` like the other inode-keyed dentry ops. Idempotent: a
    /// `Dentry { entry: None }` result means the dentry was already gone.
    RemoveGraft {
        parent: u64,
        name: String,
    },
    CreateDirPath {
        path: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateFile {
        parent: u64,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateFilePrepared {
        parent: u64,
        name: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateSymlink {
        parent: u64,
        name: String,
        target: Vec<u8>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateSpecialNode {
        parent: u64,
        name: String,
        file_type: String,
        mode: u32,
        rdev: u32,
        uid: u32,
        gid: u32,
    },
    UpdateAttrs {
        parent: u64,
        name: String,
        changes: WireUpdateAttr,
    },
    UpdateRootAttrs {
        changes: WireUpdateAttr,
    },
    SetXattr {
        inode: u64,
        name_hex: String,
        value: Vec<u8>,
        mode: WireXattrSetMode,
    },
    GetXattr {
        inode: u64,
        name_hex: String,
    },
    ListXattr {
        inode: u64,
    },
    RemoveXattr {
        inode: u64,
        name_hex: String,
    },
    GetAdvisoryLock {
        inode: u64,
        owner: u64,
        start: u64,
        end: u64,
        kind: String,
        pid: u32,
    },
    SetAdvisoryLock {
        inode: u64,
        owner: u64,
        start: u64,
        end: u64,
        kind: String,
        pid: u32,
        wait: bool,
    },
    CreateFilePath {
        path: String,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    CreateFilesInDirPath {
        parent_path: String,
        names: Vec<String>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    RemoveFile {
        parent: u64,
        name: String,
    },
    RemoveFilePath {
        path: String,
    },
    RemoveEmptyDir {
        parent: u64,
        name: String,
    },
    RemoveEmptyDirPath {
        path: String,
    },
    Link {
        inode: u64,
        new_parent: u64,
        new_name: String,
    },
    Rename {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
    },
    RenamePath {
        source: String,
        destination: String,
    },
    RenameReplace {
        parent: u64,
        name: String,
        new_parent: u64,
        new_name: String,
    },
    RenameReplacePath {
        source: String,
        destination: String,
    },
    SnapshotSubtree {
        root: u64,
    },
    SnapshotPin {
        snapshot_id: u64,
    },
    SnapshotSubtreePath {
        path: String,
    },
    CloneSubtreePath {
        src_path: String,
        dst_path: String,
    },
    DiffSubtrees {
        a_path: String,
        b_path: String,
    },
    RollbackSubtreePath {
        target_path: String,
        snapshot_id: u64,
    },
    StatPathAtSnapshot {
        snapshot_id: u64,
        path: String,
    },
    ReadDirPlusPathAtSnapshot {
        snapshot_id: u64,
        path: String,
    },
    ReadFilePathAtSnapshot {
        snapshot_id: u64,
        path: String,
        offset: u64,
        len: u64,
    },
    ReadFileAtSnapshot {
        snapshot_id: u64,
        inode: u64,
        offset: u64,
        len: u64,
    },
    ReadSymlink {
        inode: u64,
    },
    ReadSymlinkAtSnapshot {
        snapshot_id: u64,
        inode: u64,
    },
    RetireSnapshot {
        snapshot_id: u64,
    },
    RenewSnapshot {
        snapshot_id: u64,
        lease_ms: u64,
    },
    OpenPathReadPlan {
        path: String,
        offset: u64,
        len: u64,
        expected_generation: Option<u64>,
    },
    OpenPathReadPlanBatch {
        requests: Vec<WireOpenPathReadPlanRequest>,
    },
    ReadBodyPlan {
        inode: u64,
        generation: u64,
        offset: u64,
        len: u64,
    },
    ReadArtifactPathAtSnapshot {
        snapshot_id: u64,
        path: String,
    },
    PrepareArtifact {
        parent: u64,
        name: String,
        replace: bool,
    },
    PrepareArtifactPath {
        path: String,
        replace: bool,
    },
    PublishPreparedArtifact {
        prepared: WirePreparedArtifact,
        body: Box<WireBodyDescriptor>,
        chunks: Vec<WireChunkManifest>,
        mode: u32,
        uid: u32,
        gid: u32,
    },
    PublishPreparedArtifactStagedSession {
        prepared: WirePreparedArtifact,
        producer: String,
        digest_uri: String,
        content_type: String,
        manifest_id: String,
        size: u64,
        chunks: Vec<WireChunkManifest>,
        staged: WireStagedObjectSet,
        mode: u32,
        uid: u32,
        gid: u32,
    },
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireUpdateAttr {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ctime_ms: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireXattrSetMode {
    Any,
    Create,
    Replace,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MetadataRpcEnvelope {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<MetadataRpcResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<WireMetadataError>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireMetadataError {
    NotFound,
    NotFile,
    NotDirectory,
    /// A directory removal/rename found the target non-empty. Carried as a typed
    /// variant so it survives the wire as `MetadError::DirectoryNotEmpty` (→
    /// `ENOTEMPTY` in FUSE), instead of collapsing into an opaque `Metadata`
    /// message that the client cannot classify.
    DirectoryNotEmpty,
    MissingBodyDescriptor,
    PredicateFailed,
    StaleBodyGeneration {
        expected: u64,
        current: u64,
    },
    StaleOwnerEpoch {
        owner_epoch: u64,
        required_epoch: u64,
    },
    LeaseExpired {
        now_ms: u64,
        deadline_ms: u64,
    },
    /// The addressed shard is not owned by this node. The client should
    /// re-resolve the shard owner (via the control plane / shard map) and retry
    /// against `endpoint` when present.
    NotOwner {
        shard_id: String,
        endpoint: Option<String>,
    },
    /// A rename/hardlink/clone crossed a shard boundary. The client maps this to
    /// `EXDEV`; it is terminal (not a handoff to retry).
    CrossShard {
        source_shard: u16,
        dest_shard: u16,
    },
    /// The target dentry is a cross-shard graft point; remove/rename of it must
    /// go through the graft lifecycle. The client maps this to `EBUSY`.
    GraftPoint,
    SyncLogArchiveFailed {
        committed: bool,
        message: String,
    },
    LockConflict {
        lock: WireAdvisoryLock,
    },
    InvalidPath {
        message: String,
    },
    InvalidQuery {
        message: String,
    },
    Metadata {
        message: String,
    },
    Object {
        message: String,
    },
    Io {
        message: String,
    },
    Protocol {
        message: String,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetadataRpcResult {
    Unit,
    Batch {
        results: Vec<MetadataRpcEnvelope>,
    },
    InodeAttr {
        attr: Option<WireInodeAttr>,
    },
    Dentry {
        entry: Option<Box<WireDentryWithAttr>>,
    },
    DentryVersion {
        version: Option<u64>,
    },
    Dentries {
        entries: Vec<WireDentryWithAttr>,
    },
    DentriesPage {
        entries: Vec<WireDentryWithAttr>,
        next_name_hex: Option<String>,
    },
    PathMetadata {
        metadata: Option<WirePathMetadata>,
    },
    NamespaceCard {
        card: Option<Box<WireNamespaceCard>>,
    },
    NamespaceListPage {
        page: Box<WireNamespaceListPage>,
    },
    NamespaceFindResult {
        result: Box<WireNamespaceFindResult>,
    },
    NamespaceAggregateResult {
        result: Box<WireNamespaceAggregateResult>,
    },
    NamespaceGrepResult {
        result: Box<WireNamespaceGrepResult>,
    },
    NamespaceReadPage {
        page: Box<WireNamespaceReadPage>,
    },
    RenameReplace {
        entry: Box<WireDentryWithAttr>,
        replaced: Option<Box<WireDentryWithAttr>>,
    },
    Snapshot {
        snapshot: WireSnapshotPin,
    },
    SnapshotPin {
        snapshot: Option<WireSnapshotPin>,
    },
    RetiredSnapshot {
        retired: bool,
    },
    RenewedSnapshot {
        renewed: bool,
    },
    OpenPathReadPlan {
        metadata: WirePathMetadata,
        lease: WireReadLease,
        plan: WireBodyReadPlan,
    },
    OpenPathReadPlanBatch {
        plans: Vec<WireOpenPathReadPlan>,
    },
    CloneSubtree {
        root: u64,
        snapshot_id: u64,
    },
    SubtreeDeltas {
        deltas: Vec<WireSubtreeDelta>,
    },
    BodyReadPlan {
        plan: WireBodyReadPlan,
    },
    FileBytes {
        bytes: Vec<u8>,
    },
    XattrValue {
        value: Option<Vec<u8>>,
    },
    XattrNames {
        names_hex: Vec<String>,
    },
    AdvisoryLock {
        lock: Option<WireAdvisoryLock>,
    },
    PreparedArtifact {
        prepared: WirePreparedArtifact,
    },
    CreatedPreparedArtifact {
        entry: Box<WireDentryWithAttr>,
        prepared: WirePreparedArtifact,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireDentryWithAttr {
    pub dentry: WireDentryRecord,
    pub attr: WireInodeAttr,
    pub body: Option<WireBodyDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireAdvisoryLock {
    pub inode: u64,
    pub owner: u64,
    pub start: u64,
    pub end: u64,
    pub kind: String,
    pub pid: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WirePathMetadata {
    pub attr: WireInodeAttr,
    pub body: Option<WireBodyDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceCardKind {
    File,
    Directory,
    Symlink,
    Special,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceRecordType {
    DirectoryEntries,
    JsonArray,
    JsonObject,
    YamlMapping,
    TextLines,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireRecordCountProvenance {
    LiveNamespace,
    StructuredBody,
    MaterializedIndex,
    Approximate,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceRecordCount {
    pub count: u64,
    pub provenance: WireRecordCountProvenance,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceSchema {
    pub record_type: WireNamespaceRecordType,
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceInclude {
    Body,
    Schema,
    Sample,
    Catalog,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFilterCapability {
    pub field: WireNamespaceFindField,
    pub operators: Vec<WireNamespacePredicateOp>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceQueryCatalog {
    pub filterable: Vec<WireNamespaceFilterCapability>,
    pub sortable: Vec<WireNamespaceSortField>,
    pub facetable: Vec<WireNamespaceFindField>,
    pub facets: Vec<WireNamespaceFacetSummary>,
    pub projections: Vec<WireNamespaceInclude>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFacetSummary {
    pub field: WireNamespaceFindField,
    pub values: Vec<WireNamespaceFacetValue>,
    pub distinct_count: u64,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFacetValue {
    pub value: WireNamespacePredicateValue,
    pub count: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceCard {
    pub path: String,
    pub name: String,
    pub kind: WireNamespaceCardKind,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub inode: u64,
    pub generation: u64,
    pub size_bytes: Option<u64>,
    pub entry_count: Option<u64>,
    pub record_count: Option<WireNamespaceRecordCount>,
    pub schema: Option<WireNamespaceSchema>,
    pub sample: Vec<String>,
    pub body: Option<WireBodyDescriptor>,
    pub catalog: WireNamespaceQueryCatalog,
    pub indexed_values: Vec<WireNamespaceIndexValue>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceListPage {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub entry_count: u64,
    pub entries: Vec<WireNamespaceCard>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct WireNamespaceFindField {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespacePredicateOp {
    Eq,
    NotEqual,
    In,
    Prefix,
    Suffix,
    Contains,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Exists,
    NotExists,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum WireNamespacePredicateValue {
    String(String),
    U64(u64),
    F64(f64),
    List(Vec<WireNamespacePredicateValue>),
}

impl PartialEq for WireNamespacePredicateValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::U64(left), Self::U64(right)) => left == right,
            (Self::F64(left), Self::F64(right)) => {
                left.total_cmp(right) == std::cmp::Ordering::Equal
            }
            (Self::List(left), Self::List(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for WireNamespacePredicateValue {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespacePredicate {
    pub field: WireNamespaceFindField,
    pub op: WireNamespacePredicateOp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<WireNamespacePredicateValue>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct WireNamespaceSortField {
    pub id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceIndexValue {
    pub field: WireNamespaceFindField,
    pub value: WireNamespacePredicateValue,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceSortDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceSort {
    pub field: WireNamespaceSortField,
    pub direction: WireNamespaceSortDirection,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFindRequest {
    pub path: String,
    pub predicates: Vec<WireNamespacePredicate>,
    pub sort: Vec<WireNamespaceSort>,
    pub include: Vec<WireNamespaceInclude>,
    #[serde(default)]
    pub facets: Vec<WireNamespaceFindField>,
    pub cursor: Option<String>,
    pub limit: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFindResult {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub match_count: u64,
    pub matches: Vec<WireNamespaceCard>,
    #[serde(default)]
    pub facets: Vec<WireNamespaceFacetSummary>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub scanned_entries: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceFieldSourceKind {
    Namespace,
    MaterializedIndex,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFieldSource {
    pub evidence: String,
    pub source_path: String,
    pub source_kind: WireNamespaceFieldSourceKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceFieldValue {
    pub field: WireNamespaceFindField,
    pub value: WireNamespacePredicateValue,
    pub source: WireNamespaceFieldSource,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceAggregateOp {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateMeasure {
    pub name: String,
    pub op: WireNamespaceAggregateOp,
    pub field: Option<WireNamespaceFindField>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateSort {
    pub field: String,
    pub direction: WireNamespaceSortDirection,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateRequest {
    pub path: String,
    pub predicates: Vec<WireNamespacePredicate>,
    pub group_by: Vec<WireNamespaceFindField>,
    pub measures: Vec<WireNamespaceAggregateMeasure>,
    pub sort: Vec<WireNamespaceAggregateSort>,
    pub limit: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum WireNamespaceAggregateValue {
    U64(u64),
    F64(f64),
    Null,
}

impl PartialEq for WireNamespaceAggregateValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::U64(left), Self::U64(right)) => left == right,
            (Self::F64(left), Self::F64(right)) => {
                left.total_cmp(right) == std::cmp::Ordering::Equal
            }
            (Self::Null, Self::Null) => true,
            _ => false,
        }
    }
}

impl Eq for WireNamespaceAggregateValue {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateOutputMeasure {
    pub name: String,
    pub op: WireNamespaceAggregateOp,
    pub field: Option<WireNamespaceFindField>,
    pub value: WireNamespaceAggregateValue,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateSample {
    pub path: String,
    pub evidence: String,
    pub generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateGroup {
    pub key: Vec<WireNamespaceFieldValue>,
    pub measures: Vec<WireNamespaceAggregateOutputMeasure>,
    pub evidence: String,
    pub sample_matches: Vec<WireNamespaceAggregateSample>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceAggregateResult {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub predicates: Vec<WireNamespacePredicate>,
    pub input_match_count: u64,
    pub row_count: u64,
    pub group_count: u64,
    pub groups: Vec<WireNamespaceAggregateGroup>,
    pub truncated: bool,
    pub scanned_entries: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceGrepRequest {
    pub path: String,
    pub pattern: String,
    pub recursive: bool,
    pub cursor: Option<String>,
    pub limit: u64,
    pub max_files: Option<u64>,
    pub max_bytes: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceGrepMatch {
    pub path: String,
    pub line_number: u64,
    pub snippet: String,
    pub evidence: String,
    pub generation: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceGrepResult {
    pub path: String,
    pub pattern: String,
    pub recursive: bool,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub matches: Vec<WireNamespaceGrepMatch>,
    pub files_scanned: u64,
    pub bytes_read: u64,
    pub next_cursor: Option<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireNamespaceReadFormat {
    Structured,
    Bytes,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceReadOptions {
    pub format: WireNamespaceReadFormat,
    pub cursor: Option<String>,
    pub offset: u64,
    pub limit: u64,
    pub expected_generation: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceReadItem {
    pub index: u64,
    pub value_json: String,
    pub evidence: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireNamespaceReadPage {
    pub path: String,
    pub evidence: String,
    pub snapshot_id: Option<u64>,
    pub generation: u64,
    pub total_size_bytes: u64,
    pub format: WireNamespaceReadFormat,
    pub record_type: Option<WireNamespaceRecordType>,
    pub record_count: Option<u64>,
    pub cursor: Option<String>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
    pub items: Vec<WireNamespaceReadItem>,
    pub bytes: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireDentryRecord {
    pub parent: u64,
    pub name_hex: String,
    pub child: u64,
    pub child_type: String,
    pub attr_generation: u64,
}

pub fn encode_name_cursor(name: &DentryName) -> String {
    hex_encode(name.as_bytes())
}

pub fn decode_name_cursor(raw: &str) -> Result<DentryName, MetadataProtocolError> {
    DentryName::new(hex_decode(raw)?).map_err(|err| MetadataProtocolError::new(err.to_string()))
}

pub fn encode_xattr_name(name: &[u8]) -> String {
    hex_encode(name)
}

pub fn decode_xattr_name(raw: &str) -> Result<Vec<u8>, MetadataProtocolError> {
    hex_decode(raw)
}

pub fn encode_file_type(file_type: FileType) -> &'static str {
    file_type_label(file_type)
}

pub fn decode_file_type(raw: &str) -> Result<FileType, MetadataProtocolError> {
    parse_file_type(raw)
}

pub fn encode_advisory_lock_kind(kind: AdvisoryLockKind) -> &'static str {
    advisory_lock_kind_label(kind)
}

pub fn decode_advisory_lock_kind(raw: &str) -> Result<AdvisoryLockKind, MetadataProtocolError> {
    parse_advisory_lock_kind(raw)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireInodeAttr {
    pub inode: u64,
    pub file_type: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub nlink: u32,
    pub size: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireBodyDescriptor {
    pub producer: String,
    pub digest_uri: String,
    pub size: u64,
    pub content_type: String,
    pub manifest_id: String,
    pub generation: u64,
    pub base_generation: u64,
    pub chunk_size: u64,
    pub block_size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WirePreparedArtifact {
    pub mount: u64,
    pub parent: u64,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    pub inode: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: Option<u64>,
    pub old_generation: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireChunkManifest {
    pub chunk_index: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub slices: Vec<WireSliceManifest>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireSliceManifest {
    pub slice_id: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub blocks: Vec<WireBlockDescriptor>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireBlockDescriptor {
    pub object_key: String,
    pub logical_offset: u64,
    pub object_offset: u64,
    pub len: u64,
    pub digest_uri: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireStagedObjectSet {
    pub objects: Vec<WireStagedObject>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireStagedObject {
    pub key: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireBodyReadPlan {
    pub output_len: u64,
    pub blocks: Vec<WireObjectReadBlock>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireObjectReadBlock {
    pub object_key: String,
    pub digest_uri: String,
    pub object_offset: u64,
    pub object_len: u64,
    pub len: u64,
    pub output_offset: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireSnapshotPin {
    pub snapshot_id: u64,
    pub root: u64,
    pub read_version: u64,
    pub created_version: u64,
    pub lease_expires_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireReadLease {
    pub inode: u64,
    pub generation: u64,
    pub read_version: u64,
    pub lease_expires_unix_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct WireSubtreeDelta {
    pub path: String,
    pub kind: WireSubtreeDeltaKind,
    pub digest: Option<String>,
    pub size_delta: i64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WireSubtreeDeltaKind {
    Added,
    Removed,
    Modified,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataProtocolError(String);

impl MetadataProtocolError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl WireDentryRecord {
    pub fn from_dentry_record(record: &DentryRecord) -> Self {
        Self {
            parent: record.parent.get(),
            name_hex: hex_encode(record.name.as_bytes()),
            child: record.child.get(),
            child_type: file_type_label(record.child_type).to_owned(),
            attr_generation: record.attr_generation,
        }
    }

    pub fn into_dentry_record(self) -> Result<DentryRecord, MetadataProtocolError> {
        Ok(DentryRecord {
            parent: inode_id(self.parent)?,
            name: DentryName::new(hex_decode(&self.name_hex)?)
                .map_err(|err| MetadataProtocolError::new(err.to_string()))?,
            child: inode_id(self.child)?,
            child_type: parse_file_type(&self.child_type)?,
            attr_generation: self.attr_generation,
        })
    }
}

impl WirePathMetadata {
    pub fn from_path_metadata(metadata: &PathMetadata) -> Self {
        Self {
            attr: WireInodeAttr::from_inode_attr(&metadata.attr),
            body: metadata
                .body
                .as_ref()
                .map(WireBodyDescriptor::from_body_descriptor),
        }
    }

    pub fn into_path_metadata(self) -> Result<PathMetadata, MetadataProtocolError> {
        Ok(PathMetadata {
            attr: self.attr.into_inode_attr()?,
            body: self.body.map(WireBodyDescriptor::into_body_descriptor),
        })
    }
}

impl WireInodeAttr {
    pub fn from_inode_attr(attr: &InodeAttr) -> Self {
        Self {
            inode: attr.inode.get(),
            file_type: file_type_label(attr.file_type).to_owned(),
            mode: attr.mode,
            uid: attr.uid,
            gid: attr.gid,
            rdev: attr.rdev,
            nlink: attr.nlink,
            size: attr.size,
            generation: attr.generation,
            mtime_ms: attr.mtime_ms,
            ctime_ms: attr.ctime_ms,
        }
    }

    pub fn into_inode_attr(self) -> Result<InodeAttr, MetadataProtocolError> {
        Ok(InodeAttr {
            inode: inode_id(self.inode)?,
            file_type: parse_file_type(&self.file_type)?,
            mode: self.mode,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            nlink: self.nlink,
            size: self.size,
            generation: self.generation,
            mtime_ms: self.mtime_ms,
            ctime_ms: self.ctime_ms,
        })
    }
}

impl WireAdvisoryLock {
    pub fn from_advisory_lock(lock: &AdvisoryLock) -> Self {
        Self {
            inode: lock.inode.get(),
            owner: lock.owner,
            start: lock.start,
            end: lock.end,
            kind: advisory_lock_kind_label(lock.kind).to_owned(),
            pid: lock.pid,
        }
    }

    pub fn into_advisory_lock(self) -> Result<AdvisoryLock, MetadataProtocolError> {
        Ok(AdvisoryLock {
            inode: inode_id(self.inode)?,
            owner: self.owner,
            start: self.start,
            end: self.end,
            kind: parse_advisory_lock_kind(&self.kind)?,
            pid: self.pid,
        })
    }
}

impl WireBodyDescriptor {
    pub fn from_body_descriptor(body: &BodyDescriptor) -> Self {
        Self {
            producer: body.producer.clone(),
            digest_uri: body.digest_uri.clone(),
            size: body.size,
            content_type: body.content_type.clone(),
            manifest_id: body.manifest_id.clone(),
            generation: body.generation,
            base_generation: body.base_generation,
            chunk_size: body.chunk_size,
            block_size: body.block_size,
        }
    }

    pub fn into_body_descriptor(self) -> BodyDescriptor {
        BodyDescriptor {
            producer: self.producer,
            digest_uri: self.digest_uri,
            size: self.size,
            content_type: self.content_type,
            manifest_id: self.manifest_id,
            generation: self.generation,
            base_generation: self.base_generation,
            chunk_size: self.chunk_size,
            block_size: self.block_size,
        }
    }
}

impl WireChunkManifest {
    pub fn from_chunk_manifest(chunk: &ChunkManifest) -> Self {
        Self {
            chunk_index: chunk.chunk_index,
            logical_offset: chunk.logical_offset,
            len: chunk.len,
            slices: chunk
                .slices
                .iter()
                .map(WireSliceManifest::from_slice_manifest)
                .collect(),
        }
    }

    pub fn into_chunk_manifest(self) -> Result<ChunkManifest, MetadataProtocolError> {
        Ok(ChunkManifest {
            chunk_index: self.chunk_index,
            logical_offset: self.logical_offset,
            len: self.len,
            slices: self
                .slices
                .into_iter()
                .map(WireSliceManifest::into_slice_manifest)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl WireSliceManifest {
    pub fn from_slice_manifest(slice: &SliceManifest) -> Self {
        Self {
            slice_id: slice.slice_id,
            logical_offset: slice.logical_offset,
            len: slice.len,
            blocks: slice
                .blocks
                .iter()
                .map(WireBlockDescriptor::from_block_descriptor)
                .collect(),
        }
    }

    pub fn into_slice_manifest(self) -> Result<SliceManifest, MetadataProtocolError> {
        Ok(SliceManifest {
            slice_id: self.slice_id,
            logical_offset: self.logical_offset,
            len: self.len,
            blocks: self
                .blocks
                .into_iter()
                .map(WireBlockDescriptor::into_block_descriptor)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl WireBlockDescriptor {
    pub fn from_block_descriptor(block: &BlockDescriptor) -> Self {
        Self {
            object_key: block.object_key.clone(),
            logical_offset: block.logical_offset,
            object_offset: block.object_offset,
            len: block.len,
            digest_uri: block.digest_uri.clone(),
        }
    }

    pub fn into_block_descriptor(self) -> Result<BlockDescriptor, MetadataProtocolError> {
        if self.object_key.is_empty() {
            return Err(MetadataProtocolError::new(
                "block descriptor object key is empty",
            ));
        }
        Ok(BlockDescriptor {
            object_key: self.object_key,
            logical_offset: self.logical_offset,
            object_offset: self.object_offset,
            len: self.len,
            digest_uri: self.digest_uri,
        })
    }
}

impl WireSnapshotPin {
    pub fn from_snapshot_pin(snapshot: &SnapshotPin) -> Self {
        Self {
            snapshot_id: snapshot.snapshot_id,
            root: snapshot.root.get(),
            read_version: snapshot.read_version,
            created_version: snapshot.created_version,
            lease_expires_unix_ms: snapshot.lease_expires_unix_ms,
        }
    }

    pub fn into_snapshot_pin(self) -> Result<SnapshotPin, MetadataProtocolError> {
        Ok(SnapshotPin {
            snapshot_id: self.snapshot_id,
            root: inode_id(self.root)?,
            read_version: self.read_version,
            created_version: self.created_version,
            lease_expires_unix_ms: self.lease_expires_unix_ms,
        })
    }
}

impl WireReadLease {
    pub fn from_read_lease(lease: &ReadLease) -> Self {
        Self {
            inode: lease.inode.get(),
            generation: lease.generation,
            read_version: lease.read_version,
            lease_expires_unix_ms: lease.lease_expires_unix_ms,
        }
    }

    pub fn into_read_lease(self) -> Result<ReadLease, MetadataProtocolError> {
        Ok(ReadLease {
            inode: inode_id(self.inode)?,
            generation: self.generation,
            read_version: self.read_version,
            lease_expires_unix_ms: self.lease_expires_unix_ms,
        })
    }
}

pub fn encode_request(request: &MetadataRpcRequest) -> Result<Vec<u8>, MetadataProtocolError> {
    serialize(request)
}

pub fn decode_request(body: &[u8]) -> Result<MetadataRpcRequest, MetadataProtocolError> {
    deserialize(body)
}

pub fn encode_envelope(envelope: &MetadataRpcEnvelope) -> Result<Vec<u8>, MetadataProtocolError> {
    serialize(envelope)
}

pub fn decode_envelope(body: &[u8]) -> Result<MetadataRpcEnvelope, MetadataProtocolError> {
    deserialize(body)
}

fn serialize<T: Serialize>(value: &T) -> Result<Vec<u8>, MetadataProtocolError> {
    let mut out = Vec::new();
    value
        .serialize(&mut rmp_serde::Serializer::new(&mut out).with_struct_map())
        .map_err(|err| MetadataProtocolError(err.to_string()))?;
    Ok(out)
}

fn deserialize<'a, T: Deserialize<'a>>(body: &'a [u8]) -> Result<T, MetadataProtocolError> {
    if body.len() as u64 > BINARY_CODEC_LIMIT {
        return Err(MetadataProtocolError(format!(
            "metadata binary rpc body exceeds {BINARY_CODEC_LIMIT} bytes"
        )));
    }
    rmp_serde::from_slice(body).map_err(|err| MetadataProtocolError(err.to_string()))
}

fn inode_id(raw: u64) -> Result<InodeId, MetadataProtocolError> {
    InodeId::new(raw).map_err(|err| MetadataProtocolError::new(err.to_string()))
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
        FileType::NamedPipe => "named_pipe",
        FileType::CharDevice => "char_device",
        FileType::BlockDevice => "block_device",
        FileType::Socket => "socket",
    }
}

fn parse_file_type(raw: &str) -> Result<FileType, MetadataProtocolError> {
    match raw {
        "file" => Ok(FileType::File),
        "directory" => Ok(FileType::Directory),
        "symlink" => Ok(FileType::Symlink),
        "named_pipe" | "fifo" => Ok(FileType::NamedPipe),
        "char_device" => Ok(FileType::CharDevice),
        "block_device" => Ok(FileType::BlockDevice),
        "socket" => Ok(FileType::Socket),
        other => Err(MetadataProtocolError::new(format!(
            "unknown file type {other}"
        ))),
    }
}

fn advisory_lock_kind_label(kind: AdvisoryLockKind) -> &'static str {
    match kind {
        AdvisoryLockKind::Read => "read",
        AdvisoryLockKind::Write => "write",
        AdvisoryLockKind::Unlock => "unlock",
    }
}

fn parse_advisory_lock_kind(raw: &str) -> Result<AdvisoryLockKind, MetadataProtocolError> {
    match raw {
        "read" => Ok(AdvisoryLockKind::Read),
        "write" => Ok(AdvisoryLockKind::Write),
        "unlock" => Ok(AdvisoryLockKind::Unlock),
        other => Err(MetadataProtocolError::new(format!(
            "unknown advisory lock kind {other}"
        ))),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(raw: &str) -> Result<Vec<u8>, MetadataProtocolError> {
    if !raw.len().is_multiple_of(2) {
        return Err(MetadataProtocolError::new("hex string has odd length"));
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.as_bytes().chunks_exact(2) {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_digit(byte: u8) -> Result<u8, MetadataProtocolError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(MetadataProtocolError::new("invalid hex digit")),
    }
}

impl fmt::Display for MetadataProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MetadataProtocolError {}

/// What a request routes on: an absolute path, a bare inode/parent, or nothing
/// addressable (bootstrap / snapshot-id-only / empty batch), which targets the
/// default shard. Borrows from the request so routing is allocation-free.
///
/// This lives in the protocol crate so the client and server route *identically*
/// off the same wire request — there is a single source of truth for the
/// partitioning key.
pub enum RoutingKey<'a> {
    Path(&'a str),
    Inode(u64),
    Default,
}

/// The routing dimension a request carries. For variants that bundle both a
/// `snapshot_id` and an inode/path, key on the inode/path (the snapshot id is
/// shard-local and not a routing key on its own).
pub fn request_routing_key(request: &MetadataRpcRequest) -> RoutingKey<'_> {
    match request {
        // Path-addressed operations route by longest-prefix match.
        MetadataRpcRequest::LookupPath { path }
        | MetadataRpcRequest::StatPath { path }
        | MetadataRpcRequest::ReadDirPlusPath { path }
        | MetadataRpcRequest::StatCard { path }
        | MetadataRpcRequest::CreateDirPath { path, .. }
        | MetadataRpcRequest::CreateFilePath { path, .. }
        | MetadataRpcRequest::RemoveFilePath { path }
        | MetadataRpcRequest::RemoveEmptyDirPath { path }
        | MetadataRpcRequest::SnapshotSubtreePath { path }
        | MetadataRpcRequest::ReadIndexedPathPage { path, .. }
        | MetadataRpcRequest::ReadDirPlusPathPage { path, .. }
        | MetadataRpcRequest::ReadPage { path, .. }
        | MetadataRpcRequest::ListPage { path, .. }
        | MetadataRpcRequest::StatPathAtSnapshot { path, .. }
        | MetadataRpcRequest::ReadDirPlusPathAtSnapshot { path, .. }
        | MetadataRpcRequest::ReadFilePathAtSnapshot { path, .. }
        | MetadataRpcRequest::ReadArtifactPathAtSnapshot { path, .. }
        | MetadataRpcRequest::OpenPathReadPlan { path, .. }
        | MetadataRpcRequest::PrepareArtifactPath { path, .. } => RoutingKey::Path(path),
        MetadataRpcRequest::CreateFilesInDirPath { parent_path, .. } => {
            RoutingKey::Path(parent_path)
        }
        MetadataRpcRequest::FindPaths { request } => RoutingKey::Path(&request.path),
        MetadataRpcRequest::AggregatePaths { request } => RoutingKey::Path(&request.path),
        MetadataRpcRequest::GrepPaths { request } => RoutingKey::Path(&request.path),
        // A batch open routes by its FIRST entry's path. The client guarantees
        // every batch it sends is single-shard (it groups requests by shard before
        // sending), so the first path's owner owns the whole batch; an empty batch
        // has no addressable key and targets the default shard.
        MetadataRpcRequest::OpenPathReadPlanBatch { requests } => match requests.first() {
            Some(first) => RoutingKey::Path(&first.path),
            None => RoutingKey::Default,
        },
        // Cross-path operations route on their source path; cross-shard pairs are
        // rejected downstream by the (single-shard) service.
        MetadataRpcRequest::RenamePath { source, .. } => RoutingKey::Path(source),
        MetadataRpcRequest::RenameReplacePath { source, .. } => RoutingKey::Path(source),
        MetadataRpcRequest::CloneSubtreePath { src_path, .. } => RoutingKey::Path(src_path),
        MetadataRpcRequest::DiffSubtrees { a_path, .. } => RoutingKey::Path(a_path),
        MetadataRpcRequest::RollbackSubtreePath { target_path, .. } => {
            RoutingKey::Path(target_path)
        }

        // Inode/parent-addressed operations route on the encoded shard index.
        MetadataRpcRequest::GetAttr { inode }
        | MetadataRpcRequest::GetAttrAtSnapshot { inode, .. }
        | MetadataRpcRequest::SetXattr { inode, .. }
        | MetadataRpcRequest::GetXattr { inode, .. }
        | MetadataRpcRequest::ListXattr { inode }
        | MetadataRpcRequest::RemoveXattr { inode, .. }
        | MetadataRpcRequest::GetAdvisoryLock { inode, .. }
        | MetadataRpcRequest::SetAdvisoryLock { inode, .. }
        | MetadataRpcRequest::ReadBodyPlan { inode, .. }
        | MetadataRpcRequest::ReadSymlink { inode }
        | MetadataRpcRequest::ReadSymlinkAtSnapshot { inode, .. }
        | MetadataRpcRequest::ReadFileAtSnapshot { inode, .. } => RoutingKey::Inode(*inode),
        MetadataRpcRequest::LookupPlus { parent, .. }
        | MetadataRpcRequest::CurrentDentryVersion { parent, .. }
        | MetadataRpcRequest::LookupPlusAtSnapshot { parent, .. }
        | MetadataRpcRequest::ReadDirPlus { parent }
        | MetadataRpcRequest::ReadDirPlusPage { parent, .. }
        | MetadataRpcRequest::ReadDirPlusAtSnapshot { parent, .. }
        | MetadataRpcRequest::CreateDir { parent, .. }
        | MetadataRpcRequest::CreateGraft { parent, .. }
        | MetadataRpcRequest::RemoveGraft { parent, .. }
        | MetadataRpcRequest::CreateFile { parent, .. }
        | MetadataRpcRequest::CreateFilePrepared { parent, .. }
        | MetadataRpcRequest::CreateSymlink { parent, .. }
        | MetadataRpcRequest::CreateSpecialNode { parent, .. }
        | MetadataRpcRequest::UpdateAttrs { parent, .. }
        | MetadataRpcRequest::RemoveFile { parent, .. }
        | MetadataRpcRequest::RemoveEmptyDir { parent, .. }
        | MetadataRpcRequest::PrepareArtifact { parent, .. } => RoutingKey::Inode(*parent),
        MetadataRpcRequest::Link { new_parent, .. } => RoutingKey::Inode(*new_parent),
        MetadataRpcRequest::Rename { parent, .. } => RoutingKey::Inode(*parent),
        MetadataRpcRequest::RenameReplace { parent, .. } => RoutingKey::Inode(*parent),
        MetadataRpcRequest::SnapshotSubtree { root } => RoutingKey::Inode(*root),
        MetadataRpcRequest::PublishPreparedArtifact { prepared, .. } => {
            RoutingKey::Inode(prepared.parent)
        }
        MetadataRpcRequest::PublishPreparedArtifactStagedSession { prepared, .. } => {
            RoutingKey::Inode(prepared.parent)
        }

        // No addressable key: target the default/root shard.
        MetadataRpcRequest::Batch { .. }
        | MetadataRpcRequest::BootstrapRoot { .. }
        | MetadataRpcRequest::UpdateRootAttrs { .. }
        | MetadataRpcRequest::SnapshotPin { .. }
        | MetadataRpcRequest::RetireSnapshot { .. }
        | MetadataRpcRequest::RenewSnapshot { .. } => RoutingKey::Default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_codec_round_trips_metadata_request() {
        let request = MetadataRpcRequest::GrepPaths {
            request: Box::new(WireNamespaceGrepRequest {
                path: "/runs".to_owned(),
                pattern: "needle".to_owned(),
                recursive: true,
                cursor: Some("1:0".to_owned()),
                limit: 10,
                max_files: Some(20),
                max_bytes: Some(4096),
            }),
        };
        let encoded = encode_request(&request).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }

    #[test]
    fn binary_codec_round_trips_open_path_read_plan() {
        let request = MetadataRpcRequest::OpenPathReadPlan {
            path: "/artifact.bin".to_owned(),
            offset: 1,
            len: 5,
            expected_generation: Some(7),
        };
        let encoded = encode_request(&request).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), request);

        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::OpenPathReadPlan {
                metadata: WirePathMetadata {
                    attr: WireInodeAttr {
                        inode: 42,
                        file_type: "file".to_owned(),
                        mode: 0o644,
                        uid: 1000,
                        gid: 1000,
                        rdev: 0,
                        nlink: 1,
                        size: 6,
                        generation: 7,
                        mtime_ms: 8,
                        ctime_ms: 8,
                    },
                    body: Some(WireBodyDescriptor {
                        producer: "unit-test".to_owned(),
                        digest_uri: "sha256:demo".to_owned(),
                        size: 6,
                        content_type: "application/octet-stream".to_owned(),
                        manifest_id: "artifact.bin".to_owned(),
                        generation: 7,
                        base_generation: 0,
                        chunk_size: 64 * 1024 * 1024,
                        block_size: 4 * 1024 * 1024,
                    }),
                },
                lease: WireReadLease {
                    inode: 42,
                    generation: 7,
                    read_version: 9,
                    lease_expires_unix_ms: 12_345,
                },
                plan: WireBodyReadPlan {
                    output_len: 3,
                    blocks: vec![WireObjectReadBlock {
                        object_key: "blocks/demo".to_owned(),
                        digest_uri: "sha256:test".to_owned(),
                        object_offset: 2,
                        object_len: 6,
                        len: 3,
                        output_offset: 0,
                    }],
                },
            }),
            error: None,
            error_kind: None,
        };
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
    }

    #[test]
    fn binary_codec_round_trips_open_path_read_plan_batch() {
        let request = MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: vec![WireOpenPathReadPlanRequest {
                path: "/dataset/sample-0.bin".to_owned(),
                offset: 4,
                len: 8,
                expected_generation: Some(3),
            }],
        };
        let encoded = encode_request(&request).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), request);

        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::OpenPathReadPlanBatch {
                plans: vec![WireOpenPathReadPlan {
                    metadata: WirePathMetadata {
                        attr: WireInodeAttr {
                            inode: 42,
                            file_type: "file".to_owned(),
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                            rdev: 0,
                            nlink: 1,
                            size: 16,
                            generation: 3,
                            mtime_ms: 8,
                            ctime_ms: 8,
                        },
                        body: None,
                    },
                    lease: WireReadLease {
                        inode: 42,
                        generation: 3,
                        read_version: 9,
                        lease_expires_unix_ms: 12_345,
                    },
                    plan: WireBodyReadPlan {
                        output_len: 8,
                        blocks: vec![WireObjectReadBlock {
                            object_key: "blocks/sample-0".to_owned(),
                            digest_uri: "sha256:test".to_owned(),
                            object_offset: 4,
                            object_len: 16,
                            len: 8,
                            output_offset: 0,
                        }],
                    },
                }],
            }),
            error: None,
            error_kind: None,
        };
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
    }

    #[test]
    fn open_path_read_plan_batch_routes_on_first_entry_path() {
        // A non-empty batch routes on its first entry's path so the whole
        // (client-guaranteed single-shard) batch reaches that path's owner.
        let batch = MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: vec![
                WireOpenPathReadPlanRequest {
                    path: "/dataset/sample-0.bin".to_owned(),
                    offset: 0,
                    len: 4,
                    expected_generation: None,
                },
                WireOpenPathReadPlanRequest {
                    path: "/dataset/sample-1.bin".to_owned(),
                    offset: 0,
                    len: 4,
                    expected_generation: None,
                },
            ],
        };
        assert!(matches!(
            request_routing_key(&batch),
            RoutingKey::Path("/dataset/sample-0.bin")
        ));

        // An empty batch has no addressable key and targets the default shard.
        let empty = MetadataRpcRequest::OpenPathReadPlanBatch {
            requests: Vec::new(),
        };
        assert!(matches!(request_routing_key(&empty), RoutingKey::Default));
    }

    #[test]
    fn binary_codec_round_trips_clone_and_diff_requests() {
        let clone = MetadataRpcRequest::CloneSubtreePath {
            src_path: "/base".to_owned(),
            dst_path: "/forks/agent-1".to_owned(),
        };
        let encoded = encode_request(&clone).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), clone);

        let diff = MetadataRpcRequest::DiffSubtrees {
            a_path: "/base".to_owned(),
            b_path: "/forks/agent-1".to_owned(),
        };
        let encoded = encode_request(&diff).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), diff);
    }

    #[test]
    fn binary_codec_round_trips_snapshot_and_rollback_requests() {
        let snapshot = MetadataRpcRequest::SnapshotSubtreePath {
            path: "/base".to_owned(),
        };
        let encoded = encode_request(&snapshot).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), snapshot);

        let rollback = MetadataRpcRequest::RollbackSubtreePath {
            target_path: "/base".to_owned(),
            snapshot_id: 7,
        };
        let encoded = encode_request(&rollback).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), rollback);
    }

    #[test]
    fn binary_codec_round_trips_subtree_deltas_envelope() {
        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::SubtreeDeltas {
                deltas: vec![
                    WireSubtreeDelta {
                        path: "/a".to_owned(),
                        kind: WireSubtreeDeltaKind::Modified,
                        digest: Some("sha256:abc".to_owned()),
                        size_delta: 12,
                    },
                    WireSubtreeDelta {
                        path: "/c".to_owned(),
                        kind: WireSubtreeDeltaKind::Added,
                        digest: None,
                        size_delta: -5,
                    },
                ],
            }),
            error: None,
            error_kind: None,
        };
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
    }

    #[test]
    fn wire_inode_attr_round_trips_special_file_type_and_rdev() {
        let attr = InodeAttr {
            inode: InodeId::new(42).unwrap(),
            file_type: FileType::CharDevice,
            mode: 0o660,
            uid: 0,
            gid: 44,
            rdev: 0x1234,
            nlink: 1,
            size: 0,
            generation: 9,
            mtime_ms: 10,
            ctime_ms: 11,
        };
        let wire = WireInodeAttr::from_inode_attr(&attr);
        assert_eq!(wire.file_type, "char_device");
        assert_eq!(wire.rdev, 0x1234);
        assert_eq!(wire.into_inode_attr().unwrap(), attr);
    }

    #[test]
    fn wire_advisory_lock_round_trips_kind_and_range() {
        let lock = AdvisoryLock {
            inode: InodeId::new(42).unwrap(),
            owner: 7,
            start: 10,
            end: 99,
            kind: AdvisoryLockKind::Write,
            pid: 1234,
        };
        let wire = WireAdvisoryLock::from_advisory_lock(&lock);
        assert_eq!(wire.kind, "write");
        assert_eq!(wire.into_advisory_lock().unwrap(), lock);

        let request = MetadataRpcRequest::SetAdvisoryLock {
            inode: 42,
            owner: 7,
            start: 10,
            end: 99,
            kind: "write".to_owned(),
            pid: 1234,
            wait: false,
        };
        let encoded = encode_request(&request).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }

    #[test]
    fn binary_codec_round_trips_metadata_envelope() {
        let envelope = MetadataRpcEnvelope {
            ok: true,
            result: Some(MetadataRpcResult::NamespaceGrepResult {
                result: Box::new(WireNamespaceGrepResult {
                    path: "/runs".to_owned(),
                    pattern: "needle".to_owned(),
                    recursive: true,
                    evidence: "nokv-native:///runs".to_owned(),
                    snapshot_id: Some(9),
                    matches: vec![WireNamespaceGrepMatch {
                        path: "/runs/a.txt".to_owned(),
                        line_number: 2,
                        snippet: "needle".to_owned(),
                        evidence: "nokv-native:///runs/a.txt@generation:7#L2".to_owned(),
                        generation: 7,
                    }],
                    files_scanned: 1,
                    bytes_read: 16,
                    next_cursor: None,
                    truncated: false,
                }),
            }),
            error: None,
            error_kind: None,
        };
        let encoded = encode_envelope(&envelope).unwrap();
        assert_eq!(decode_envelope(&encoded).unwrap(), envelope);
    }
}
