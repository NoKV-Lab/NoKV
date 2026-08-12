/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommitId, LogicalShardId, NormalizedRelativePath, OperationId, OwnerEpoch,
    PlacementGeneration, RequestId, RootId, SnapshotAliasName, SnapshotId, TagName, WorkbenchId,
    WorkspaceIncarnationId, FIXED_ID_BYTES, SHA256_BYTES,
};
use serde::{de, Deserialize, Deserializer, Serialize};

use crate::error::ProtocolError;

const BUILTIN_INDEX_FIELD_IDS: &[&str] = &[
    "body_digest_uri",
    "content_type",
    "generation",
    "logical_size",
    "manifest_id",
    "path",
    "producer",
    "workbench_id",
];

macro_rules! identity {
    ($(#[$meta:meta])* $name:ident, $domain:ty, $width:expr) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[serde(transparent)]
        pub struct $name(pub [u8; $width]);

        impl From<$domain> for $name {
            fn from(value: $domain) -> Self {
                Self(*value.as_bytes())
            }
        }

        impl From<$name> for $domain {
            fn from(value: $name) -> Self {
                <$domain>::from_bytes(value.0)
            }
        }
    };
}

identity!(
    /// Globally unique Agent-root routing identity.
    RootIdentity,
    RootId,
    FIXED_ID_BYTES
);
identity!(
    /// Persisted logical metadata-shard identity.
    LogicalShardIdentity,
    LogicalShardId,
    FIXED_ID_BYTES
);
identity!(
    /// Exact-replay request identity.
    RequestIdentity,
    RequestId,
    FIXED_ID_BYTES
);
identity!(
    /// Durable lifecycle-operation identity.
    OperationIdentity,
    OperationId,
    FIXED_ID_BYTES
);
identity!(
    /// Never-reused workspace incarnation identity.
    WorkspaceIdentity,
    WorkspaceIncarnationId,
    FIXED_ID_BYTES
);
identity!(
    /// Immutable artifact-revision identity.
    ArtifactRevisionIdentity,
    ArtifactRevisionId,
    FIXED_ID_BYTES
);
identity!(
    /// Immutable commit identity.
    CommitIdentity,
    CommitId,
    SHA256_BYTES
);

/// SHA-256 digest used for opaque state tokens and canonical row seals.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct Digest(pub [u8; SHA256_BYTES]);

/// One exact root placement and owner routing token.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RootRoute {
    pub root_id: RootIdentity,
    pub logical_shard_id: LogicalShardIdentity,
    pub placement_generation: u64,
    pub owner_epoch: u64,
}

impl RootRoute {
    pub fn validate(self) -> Result<(), ProtocolError> {
        PlacementGeneration::new(self.placement_generation).map_err(|error| {
            ProtocolError::invalid("route.placement_generation", error.to_string())
        })?;
        OwnerEpoch::new(self.owner_epoch)
            .map_err(|error| ProtocolError::invalid("route.owner_epoch", error.to_string()))?;
        Ok(())
    }
}

/// Versioned, storage-neutral behavior advertised by a workspace RPC owner.
///
/// Declaration order is the canonical wire order. Adding a capability is an
/// explicit protocol change; servers still advertise only the subset they
/// actually implement.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceCapability {
    ArtifactPublishV1,
    ArtifactRangeReadV1,
    ChangeFeedV1,
    CommitV1,
    QueryV1,
    RestoreV1,
    SnapshotLeaseV1,
    WorkspaceLifecycleV1,
    WorkspacePathV1,
}

impl WorkspaceCapability {
    pub const ALL: [Self; 9] = [
        Self::ArtifactPublishV1,
        Self::ArtifactRangeReadV1,
        Self::ChangeFeedV1,
        Self::CommitV1,
        Self::QueryV1,
        Self::RestoreV1,
        Self::SnapshotLeaseV1,
        Self::WorkspaceLifecycleV1,
        Self::WorkspacePathV1,
    ];
}

pub(crate) fn validate_capability_set(
    field: &'static str,
    capabilities: &[WorkspaceCapability],
) -> Result<(), ProtocolError> {
    if capabilities.len() > WorkspaceCapability::ALL.len() {
        return Err(ProtocolError::invalid(
            field,
            "contains more rows than the capability schema defines",
        ));
    }
    if capabilities.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ProtocolError::invalid(
            field,
            "must be strictly sorted in canonical order without duplicates",
        ));
    }
    Ok(())
}

macro_rules! validated_string {
    (
        $(#[$meta:meta])*
        $name:ident,
        $domain:ty
    ) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, ProtocolError> {
                let value = value.into();
                <$domain>::new(value.clone())
                    .map_err(|error| ProtocolError::invalid(stringify!($name), error.to_string()))?;
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

validated_string!(
    /// Validated external workbench name.
    WorkbenchName,
    WorkbenchId
);
validated_string!(
    /// Validated slash-joined relative workspace path.
    RelativePath,
    NormalizedRelativePath
);
validated_string!(
    /// Validated durable commit tag.
    Tag,
    TagName
);
validated_string!(
    /// Validated leased-snapshot alias.
    SnapshotAlias,
    SnapshotAliasName
);

/// Exact workspace path within one routed Agent root.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkspacePath {
    pub workbench: WorkbenchName,
    pub path: RelativePath,
}

/// Point-in-time selector for a leased snapshot.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotSelector {
    Id(u64),
    Alias(SnapshotAlias),
}

impl SnapshotSelector {
    pub(crate) fn validate(&self, field: &'static str) -> Result<(), ProtocolError> {
        if matches!(self, Self::Id(0)) {
            return Err(ProtocolError::invalid(
                field,
                "snapshot id must be greater than zero",
            ));
        }
        Ok(())
    }
}

impl From<SnapshotId> for SnapshotSelector {
    fn from(value: SnapshotId) -> Self {
        Self::Id(value.get())
    }
}

/// Read view shared by point reads, scans, and queries.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceReadView {
    Live,
    Snapshot(SnapshotSelector),
}

impl WorkspaceReadView {
    pub(crate) fn validate(&self, field: &'static str) -> Result<(), ProtocolError> {
        match self {
            Self::Live => Ok(()),
            Self::Snapshot(selector) => selector.validate(field),
        }
    }
}

/// Opaque page cursor with an explicit result bound.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    pub cursor: Option<Vec<u8>>,
    pub limit: u32,
}

impl PageRequest {
    pub const MAX_LIMIT: u32 = 1_000;
    pub const MAX_CURSOR_BYTES: usize = 4_096;

    pub fn validate(&self, field: &'static str) -> Result<(), ProtocolError> {
        if !(1..=Self::MAX_LIMIT).contains(&self.limit) {
            return Err(ProtocolError::invalid(
                field,
                format!("limit must be between 1 and {}", Self::MAX_LIMIT),
            ));
        }
        if self
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.len() > Self::MAX_CURSOR_BYTES)
        {
            return Err(ProtocolError::invalid(
                field,
                format!("cursor exceeds {} bytes", Self::MAX_CURSOR_BYTES),
            ));
        }
        Ok(())
    }
}

/// Requested logical byte interval.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ByteRange {
    pub offset: u64,
    pub length: u64,
}

impl ByteRange {
    pub fn validate(self) -> Result<(), ProtocolError> {
        if self.length == 0 {
            return Err(ProtocolError::invalid(
                "range.length",
                "must be greater than zero",
            ));
        }
        self.offset
            .checked_add(self.length)
            .ok_or_else(|| ProtocolError::invalid("range", "offset plus length overflows"))?;
        Ok(())
    }
}

/// Opaque provider-neutral immutable-object identity.
#[repr(transparent)]
#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct ObjectIdentity(String);

impl ObjectIdentity {
    pub const MAX_BYTES: usize = 1_024;

    pub fn new(value: impl Into<String>) -> Result<Self, ProtocolError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ProtocolError::invalid(
                "object_identity",
                "must not be empty",
            ));
        }
        if value.len() > Self::MAX_BYTES {
            return Err(ProtocolError::invalid(
                "object_identity",
                format!("exceeds {} bytes", Self::MAX_BYTES),
            ));
        }
        if value.contains('\0') {
            return Err(ProtocolError::invalid("object_identity", "contains NUL"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ObjectIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// Canonical digest URI retained in metadata.
#[repr(transparent)]
#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct DigestUri(String);

impl DigestUri {
    pub const MAX_BYTES: usize = 256;

    pub fn new(value: impl Into<String>) -> Result<Self, ProtocolError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ProtocolError::invalid("digest_uri", "must not be empty"));
        }
        if value.len() > Self::MAX_BYTES {
            return Err(ProtocolError::invalid(
                "digest_uri",
                format!("exceeds {} bytes", Self::MAX_BYTES),
            ));
        }
        if value.contains('\0') {
            return Err(ProtocolError::invalid("digest_uri", "contains NUL"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for DigestUri {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// Validated artifact content type.
#[repr(transparent)]
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct ContentType(String);

impl ContentType {
    pub const MAX_BYTES: usize = 255;

    pub fn new(value: impl Into<String>) -> Result<Self, ProtocolError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ProtocolError::invalid("content_type", "must not be empty"));
        }
        if value.len() > Self::MAX_BYTES {
            return Err(ProtocolError::invalid(
                "content_type",
                format!("exceeds {} bytes", Self::MAX_BYTES),
            ));
        }
        if value.contains('\0') {
            return Err(ProtocolError::invalid("content_type", "contains NUL"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for ContentType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

/// Conditional path-publication mode. There is deliberately no upsert mode.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PublishCondition {
    CreateOnly,
    ReplaceOnly { expected_generation: u64 },
    Append { expected_generation: Option<u64> },
}

/// Authority under which one artifact may become metadata-visible.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PublicationAuthority {
    Visible,
    /// Reserved run-manifest publication owned by one durable commit build.
    /// The artifact revision becomes available, but its canonical path remains
    /// invisible until that same build atomically advances the commit head.
    CommitStaging {
        commit_operation_id: OperationIdentity,
    },
    RestoreStaging {
        restore_operation_id: OperationIdentity,
    },
}

impl PublishCondition {
    pub fn validate(self) -> Result<(), ProtocolError> {
        match self {
            Self::CreateOnly => Ok(()),
            Self::ReplaceOnly {
                expected_generation,
            } => require_generation("publish.expected_generation", expected_generation),
            Self::Append {
                expected_generation: Some(expected_generation),
            } => require_generation("publish.expected_generation", expected_generation),
            Self::Append {
                expected_generation: None,
            } => Ok(()),
        }
    }
}

/// Provider-neutral staged object ledger entry.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StagedObject {
    pub sequence: u32,
    pub object_identity: ObjectIdentity,
    pub expected_length: u64,
    pub expected_digest: DigestUri,
    pub multipart_token: Option<Vec<u8>>,
}

impl StagedObject {
    pub const MAX_MULTIPART_TOKEN_BYTES: usize = 4_096;

    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.expected_length == 0 {
            return Err(ProtocolError::invalid(
                "staged_object.expected_length",
                "must be greater than zero",
            ));
        }
        if self
            .multipart_token
            .as_ref()
            .is_some_and(|token| token.len() > Self::MAX_MULTIPART_TOKEN_BYTES)
        {
            return Err(ProtocolError::invalid(
                "staged_object.multipart_token",
                format!("exceeds {} bytes", Self::MAX_MULTIPART_TOKEN_BYTES),
            ));
        }
        Ok(())
    }
}

/// One canonical block row in an immutable artifact manifest.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AppendSegment {
    pub segment_sequence: u32,
    pub segment_offset: u64,
}

/// One canonical block row in an immutable artifact manifest.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifestRow {
    /// Logical position in this revision's immutable manifest.
    pub object_index: u64,
    /// Owner-local index in `physical_owner_revision_id`'s object namespace.
    ///
    /// Appends preserve this coordinate for borrowed base rows and start new
    /// delta objects at zero even though their logical manifest positions are
    /// rebased after the base rows.
    pub physical_object_index: u64,
    pub logical_offset: u64,
    pub physical_owner_revision_id: ArtifactRevisionIdentity,
    pub object_identity: ObjectIdentity,
    pub object_offset: u64,
    pub length: u64,
    pub digest: DigestUri,
    pub append_segment: Option<AppendSegment>,
}

impl ArtifactManifestRow {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.length == 0 {
            return Err(ProtocolError::invalid(
                "manifest_row.length",
                "must be greater than zero",
            ));
        }
        self.logical_offset
            .checked_add(self.length)
            .ok_or_else(|| {
                ProtocolError::invalid("manifest_row", "logical offset plus length overflows")
            })?;
        self.object_offset.checked_add(self.length).ok_or_else(|| {
            ProtocolError::invalid("manifest_row", "object offset plus length overflows")
        })?;
        if let Some(segment) = self.append_segment {
            if segment.segment_sequence as usize >= crate::MAX_ARTIFACT_PUBLISH_OBJECTS {
                return Err(ProtocolError::invalid(
                    "manifest_row.append_segment.segment_sequence",
                    format!("exceeds {}", crate::MAX_ARTIFACT_PUBLISH_OBJECTS - 1),
                ));
            }
            segment
                .segment_offset
                .checked_add(self.length)
                .ok_or_else(|| {
                    ProtocolError::invalid(
                        "manifest_row.append_segment",
                        "segment offset plus length overflows",
                    )
                })?;
        }
        Ok(())
    }
}

/// Storage-neutral scalar shared by publication indexes and queries.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Decimal(String),
    Timestamp(i64),
    String(String),
    Bytes(Vec<u8>),
}

impl ScalarValue {
    pub const MAX_DECIMAL_BYTES: usize = 128;
    pub const MAX_TEXT_BYTES: usize = 64 * 1_024;

    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::Decimal(value) => {
                validate_required_text("scalar.decimal", value, Self::MAX_DECIMAL_BYTES)?;
                if !value.parse::<f64>().is_ok_and(f64::is_finite) {
                    return Err(ProtocolError::invalid(
                        "scalar.decimal",
                        "must be a finite decimal number",
                    ));
                }
            }
            Self::String(value) => {
                validate_required_text("scalar.string", value, Self::MAX_TEXT_BYTES)?;
            }
            Self::Bytes(value) if value.len() > Self::MAX_TEXT_BYTES => {
                return Err(ProtocolError::invalid(
                    "scalar.bytes",
                    format!("exceeds {} bytes", Self::MAX_TEXT_BYTES),
                ));
            }
            _ => {}
        }
        Ok(())
    }

    fn encoded_size_bound(&self) -> usize {
        1 + match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Signed(_) | Self::Unsigned(_) | Self::Timestamp(_) | Self::Decimal(_) => 8,
            Self::String(value) => 4 + value.len(),
            Self::Bytes(value) => 4 + value.len(),
        }
    }
}

/// One typed field value with a stable, provider-neutral identity.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FieldValue {
    pub field_id: String,
    pub value: ScalarValue,
}

impl FieldValue {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_field_id("field_value.field_id", &self.field_id)?;
        self.value.validate()
    }
}

/// Final immutable artifact metadata.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactDescriptor {
    pub logical_size: u64,
    pub body_digest: DigestUri,
    pub manifest_digest: DigestUri,
    pub content_type: ContentType,
    pub producer: Option<String>,
    pub manifest_identity: Option<String>,
    pub index_fields: Vec<FieldValue>,
}

impl ArtifactDescriptor {
    pub const MAX_PRODUCER_BYTES: usize = 512;
    pub const MAX_MANIFEST_IDENTITY_BYTES: usize = 1_024;
    pub const MAX_INDEX_FIELDS: usize = 64;
    pub const MAX_INDEX_FIELD_BYTES: usize = 64 * 1_024;

    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_optional_text(
            "artifact.producer",
            self.producer.as_deref(),
            Self::MAX_PRODUCER_BYTES,
        )?;
        validate_optional_text(
            "artifact.manifest_identity",
            self.manifest_identity.as_deref(),
            Self::MAX_MANIFEST_IDENTITY_BYTES,
        )?;
        if self.index_fields.len() > Self::MAX_INDEX_FIELDS {
            return Err(ProtocolError::invalid(
                "artifact.index_fields",
                format!("exceeds {} fields", Self::MAX_INDEX_FIELDS),
            ));
        }
        let mut encoded_size_bound = 3_usize;
        for field in &self.index_fields {
            field.validate()?;
            if BUILTIN_INDEX_FIELD_IDS
                .binary_search(&field.field_id.as_str())
                .is_ok()
            {
                return Err(ProtocolError::invalid(
                    "artifact.index_fields",
                    format!("{} is a reserved built-in field", field.field_id),
                ));
            }
            encoded_size_bound = encoded_size_bound
                .checked_add(2 + field.field_id.len() + field.value.encoded_size_bound())
                .ok_or_else(|| {
                    ProtocolError::invalid("artifact.index_fields", "size bound overflows")
                })?;
        }
        if !self
            .index_fields
            .windows(2)
            .all(|pair| pair[0].field_id < pair[1].field_id)
        {
            return Err(ProtocolError::invalid(
                "artifact.index_fields",
                "must be sorted by field_id and contain no duplicates",
            ));
        }
        if encoded_size_bound > Self::MAX_INDEX_FIELD_BYTES {
            return Err(ProtocolError::invalid(
                "artifact.index_fields",
                format!(
                    "canonical field payload exceeds {} bytes",
                    Self::MAX_INDEX_FIELD_BYTES
                ),
            ));
        }
        Ok(())
    }
}

/// Compact metadata returned without reading artifact bytes.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PathMetadata {
    pub path: WorkspacePath,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub workspace_revision: u64,
    pub generation: u64,
    pub artifact_revision_id: ArtifactRevisionIdentity,
    /// Number of distinct physical owner revisions retained by this revision.
    pub dependency_count: u32,
    /// Maximum sealed revision-dependency path ending at this revision.
    pub dependency_depth: u8,
    pub descriptor: ArtifactDescriptor,
}

impl PathMetadata {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        require_generation("path_metadata.generation", self.generation)?;
        if self.dependency_count > crate::MAX_ARTIFACT_DEPENDENCY_OWNERS {
            return Err(ProtocolError::invalid(
                "path_metadata.dependency_count",
                format!("exceeds {}", crate::MAX_ARTIFACT_DEPENDENCY_OWNERS),
            ));
        }
        if self.dependency_depth > crate::MAX_ARTIFACT_DEPENDENCY_DEPTH {
            return Err(ProtocolError::invalid(
                "path_metadata.dependency_depth",
                format!("exceeds {}", crate::MAX_ARTIFACT_DEPENDENCY_DEPTH),
            ));
        }
        if (self.dependency_count == 0) != (self.dependency_depth == 0) {
            return Err(ProtocolError::invalid(
                "path_metadata.dependencies",
                "count and depth must either both be zero or both be non-zero",
            ));
        }
        self.descriptor.validate()
    }
}

/// Opaque lifecycle state token used for exact compare-and-advance.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationToken {
    pub operation_id: OperationIdentity,
    pub state_digest: Digest,
}

/// Public lifecycle kind.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    ArtifactPublish,
    Commit,
    Restore,
}

pub(crate) fn require_generation(
    field: &'static str,
    generation: u64,
) -> Result<(), ProtocolError> {
    if generation == 0 {
        return Err(ProtocolError::invalid(field, "must be greater than zero"));
    }
    Ok(())
}

pub(crate) fn validate_optional_text(
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> Result<(), ProtocolError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.len() > max {
        return Err(ProtocolError::invalid(
            field,
            format!("exceeds {max} bytes"),
        ));
    }
    if value.contains('\0') {
        return Err(ProtocolError::invalid(field, "contains NUL"));
    }
    Ok(())
}

fn validate_required_text(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), ProtocolError> {
    if value.is_empty() {
        return Err(ProtocolError::invalid(field, "must not be empty"));
    }
    validate_optional_text(field, Some(value), max)
}

pub(crate) fn validate_field_id(field: &'static str, value: &str) -> Result<(), ProtocolError> {
    validate_required_text(field, value, 128)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(ProtocolError::invalid(
            field,
            "contains an unsupported character",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(index_fields: Vec<FieldValue>) -> ArtifactDescriptor {
        ArtifactDescriptor {
            logical_size: 4,
            body_digest: DigestUri::new(format!("sha256:{}", "11".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "22".repeat(32))).unwrap(),
            content_type: ContentType::new("application/octet-stream").unwrap(),
            producer: None,
            manifest_identity: None,
            index_fields,
        }
    }

    #[test]
    fn artifact_index_fields_are_typed_sorted_and_unique() {
        descriptor(vec![
            FieldValue {
                field_id: "run.id".to_owned(),
                value: ScalarValue::String("run-42".to_owned()),
            },
            FieldValue {
                field_id: "run.timestamp".to_owned(),
                value: ScalarValue::Timestamp(1_721_234_567),
            },
        ])
        .validate()
        .unwrap();

        assert!(descriptor(vec![
            FieldValue {
                field_id: "z".to_owned(),
                value: ScalarValue::Unsigned(1),
            },
            FieldValue {
                field_id: "a".to_owned(),
                value: ScalarValue::Unsigned(2),
            },
        ])
        .validate()
        .is_err());
        assert!(descriptor(vec![FieldValue {
            field_id: "producer".to_owned(),
            value: ScalarValue::String("shadow".to_owned()),
        }])
        .validate()
        .is_err());
        assert!(descriptor(vec![FieldValue {
            field_id: "run.value".to_owned(),
            value: ScalarValue::Decimal("NaN".to_owned()),
        }])
        .validate()
        .is_err());
    }

    #[test]
    fn path_metadata_dependency_shape_is_bounded_and_consistent() {
        let metadata = |dependency_count, dependency_depth| PathMetadata {
            path: WorkspacePath {
                workbench: WorkbenchName::new("run-42").unwrap(),
                path: RelativePath::new("outputs/model.bin").unwrap(),
            },
            workspace_incarnation_id: WorkspaceIdentity([1; 16]),
            workspace_revision: 7,
            generation: 3,
            artifact_revision_id: ArtifactRevisionIdentity([2; 16]),
            dependency_count,
            dependency_depth,
            descriptor: descriptor(Vec::new()),
        };

        metadata(0, 0).validate().unwrap();
        metadata(
            crate::MAX_ARTIFACT_DEPENDENCY_OWNERS,
            crate::MAX_ARTIFACT_DEPENDENCY_DEPTH,
        )
        .validate()
        .unwrap();
        assert!(metadata(1, 0).validate().is_err());
        assert!(metadata(0, 1).validate().is_err());
        assert!(metadata(crate::MAX_ARTIFACT_DEPENDENCY_OWNERS + 1, 1)
            .validate()
            .is_err());
        assert!(metadata(1, crate::MAX_ARTIFACT_DEPENDENCY_DEPTH + 1)
            .validate()
            .is_err());
    }
}
