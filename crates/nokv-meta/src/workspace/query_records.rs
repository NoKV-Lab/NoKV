/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Canonical typed projections and change-event payloads for workspace queries.
//!
//! A path record carries one opaque byte string so publication does not learn
//! query semantics. This module is the sole owner of those bytes. Decoding is
//! strict: field ids are sorted, unique, bounded, and every scalar has one
//! canonical representation.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommitId, Generation, NormalizedRelativePath, OperationId, RootId,
    WorkbenchId, WorkspaceIncarnationId, FIXED_ID_BYTES, SHA256_BYTES,
};

use super::codec::{push_ordered_path_components, PATH_EXACT_TERMINATOR};

/// Only supported typed-query payload format.
pub const QUERY_RECORD_VALUE_FORMAT_VERSION: u8 = 1;
/// Only supported durable change-event payload format.
pub const CHANGE_EVENT_VALUE_FORMAT_VERSION: u8 = 2;
/// Maximum number of fields in one path projection.
pub const MAX_TYPED_PROJECTION_FIELDS: usize = 60;
/// Maximum encoded path projection size.
pub const MAX_TYPED_PROJECTION_BYTES: usize = 64 * 1024;
/// Maximum field-id size.
pub const MAX_QUERY_FIELD_ID_BYTES: usize = 128;
/// Maximum string or byte scalar size.
pub const MAX_QUERY_SCALAR_BYTES: usize = 64 * 1024;

const BUILTIN_FIELD_IDS: &[&str] = &[
    "body_digest_uri",
    "content_type",
    "generation",
    "logical_size",
    "manifest_id",
    "path",
    "producer",
    "workbench_id",
];

/// Validated stable field identity used by predicates, projections, and groups.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueryFieldId(String);

impl QueryFieldId {
    pub fn new(value: impl Into<String>) -> Result<Self, QueryRecordError> {
        let value = value.into();
        validate_field_id(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    pub fn is_builtin(&self) -> bool {
        BUILTIN_FIELD_IDS.binary_search(&self.as_str()).is_ok()
    }
}

impl AsRef<str> for QueryFieldId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for QueryFieldId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A finite IEEE-754 value with canonical positive zero.
#[derive(Clone, Copy, Debug)]
pub struct FiniteFloat(u64);

impl FiniteFloat {
    pub fn new(value: f64) -> Result<Self, QueryRecordError> {
        if !value.is_finite() {
            return Err(QueryRecordError::NonFiniteFloat);
        }
        let canonical = if value == 0.0 { 0.0 } else { value };
        Ok(Self(canonical.to_bits()))
    }

    pub fn get(self) -> f64 {
        f64::from_bits(self.0)
    }

    fn from_canonical_bits(bits: u64) -> Result<Self, QueryRecordError> {
        let value = f64::from_bits(bits);
        let finite = Self::new(value)?;
        if finite.0 != bits {
            return Err(QueryRecordError::NonCanonicalFloatZero);
        }
        Ok(finite)
    }
}

impl PartialEq for FiniteFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for FiniteFloat {}

impl PartialOrd for FiniteFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FiniteFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.get().total_cmp(&other.get())
    }
}

/// Canonical scalar supported by metadata queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryScalar {
    Null,
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Float(FiniteFloat),
    Timestamp(i64),
    Bytes(Vec<u8>),
    String(String),
}

impl QueryScalar {
    pub const fn scalar_type(&self) -> QueryScalarType {
        match self {
            Self::Null => QueryScalarType::Null,
            Self::Boolean(_) => QueryScalarType::Boolean,
            Self::Signed(_) => QueryScalarType::Signed,
            Self::Unsigned(_) => QueryScalarType::Unsigned,
            Self::Float(_) => QueryScalarType::Float,
            Self::Timestamp(_) => QueryScalarType::Timestamp,
            Self::Bytes(_) => QueryScalarType::Bytes,
            Self::String(_) => QueryScalarType::String,
        }
    }

    fn discriminant(&self) -> u8 {
        self.scalar_type() as u8
    }
}

impl PartialOrd for QueryScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueryScalar {
    fn cmp(&self, other: &Self) -> Ordering {
        self.discriminant()
            .cmp(&other.discriminant())
            .then_with(|| match (self, other) {
                (Self::Null, Self::Null) => Ordering::Equal,
                (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
                (Self::Signed(left), Self::Signed(right)) => left.cmp(right),
                (Self::Unsigned(left), Self::Unsigned(right)) => left.cmp(right),
                (Self::Float(left), Self::Float(right)) => left.cmp(right),
                (Self::Timestamp(left), Self::Timestamp(right)) => left.cmp(right),
                (Self::Bytes(left), Self::Bytes(right)) => left.cmp(right),
                (Self::String(left), Self::String(right)) => left.cmp(right),
                _ => Ordering::Equal,
            })
    }
}

/// Stable scalar type advertised by the query catalog.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum QueryScalarType {
    Null = 1,
    Boolean = 2,
    Signed = 3,
    Unsigned = 4,
    Float = 5,
    Timestamp = 6,
    Bytes = 7,
    String = 8,
}

impl TryFrom<u8> for QueryScalarType {
    type Error = QueryRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Null),
            2 => Ok(Self::Boolean),
            3 => Ok(Self::Signed),
            4 => Ok(Self::Unsigned),
            5 => Ok(Self::Float),
            6 => Ok(Self::Timestamp),
            7 => Ok(Self::Bytes),
            8 => Ok(Self::String),
            value => Err(QueryRecordError::UnknownDiscriminant {
                type_name: "QueryScalarType",
                value,
            }),
        }
    }
}

/// Canonical field map stored in `PathEntry.typed_index_projection`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TypedProjection {
    fields: BTreeMap<QueryFieldId, QueryScalar>,
}

impl TypedProjection {
    pub fn new(fields: BTreeMap<QueryFieldId, QueryScalar>) -> Result<Self, QueryRecordError> {
        validate_projection_fields(&fields)?;
        let projection = Self { fields };
        let encoded = projection.encode_unchecked()?;
        if encoded.len() > MAX_TYPED_PROJECTION_BYTES {
            return Err(QueryRecordError::LengthLimit {
                field: "typed_projection",
                length: encoded.len(),
                max: MAX_TYPED_PROJECTION_BYTES,
            });
        }
        Ok(projection)
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn get(&self, field: &QueryFieldId) -> Option<&QueryScalar> {
        self.fields.get(field)
    }

    pub fn fields(&self) -> &BTreeMap<QueryFieldId, QueryScalar> {
        &self.fields
    }

    pub fn encode(&self) -> Result<Vec<u8>, QueryRecordError> {
        validate_projection_fields(&self.fields)?;
        let encoded = self.encode_unchecked()?;
        if encoded.len() > MAX_TYPED_PROJECTION_BYTES {
            return Err(QueryRecordError::LengthLimit {
                field: "typed_projection",
                length: encoded.len(),
                max: MAX_TYPED_PROJECTION_BYTES,
            });
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, QueryRecordError> {
        if encoded.len() > MAX_TYPED_PROJECTION_BYTES {
            return Err(QueryRecordError::LengthLimit {
                field: "typed_projection",
                length: encoded.len(),
                max: MAX_TYPED_PROJECTION_BYTES,
            });
        }
        let mut decoder = Decoder::new(encoded);
        decoder.require_version(QUERY_RECORD_VALUE_FORMAT_VERSION)?;
        let count = usize::from(decoder.u16("field_count")?);
        if count > MAX_TYPED_PROJECTION_FIELDS {
            return Err(QueryRecordError::FieldCountLimit {
                count,
                max: MAX_TYPED_PROJECTION_FIELDS,
            });
        }
        let mut fields = BTreeMap::new();
        let mut previous: Option<QueryFieldId> = None;
        for _ in 0..count {
            let field =
                QueryFieldId::new(decoder.short_string("field_id", MAX_QUERY_FIELD_ID_BYTES)?)?;
            if field.is_builtin() {
                return Err(QueryRecordError::ReservedFieldId {
                    field_id: field.to_string(),
                });
            }
            if previous.as_ref().is_some_and(|last| last >= &field) {
                return Err(QueryRecordError::NonCanonicalFieldOrder);
            }
            let scalar = decoder.scalar()?;
            previous = Some(field.clone());
            fields.insert(field, scalar);
        }
        decoder.finish()?;
        let projection = Self { fields };
        if projection.encode_unchecked()? != encoded {
            return Err(QueryRecordError::NonCanonicalEncoding);
        }
        Ok(projection)
    }

    fn encode_unchecked(&self) -> Result<Vec<u8>, QueryRecordError> {
        let mut encoded = Vec::new();
        encoded.push(QUERY_RECORD_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(
            &u16::try_from(self.fields.len())
                .map_err(|_| QueryRecordError::FieldCountLimit {
                    count: self.fields.len(),
                    max: MAX_TYPED_PROJECTION_FIELDS,
                })?
                .to_be_bytes(),
        );
        for (field, value) in &self.fields {
            put_short_bytes(&mut encoded, "field_id", field.as_bytes())?;
            put_scalar(&mut encoded, value)?;
        }
        Ok(encoded)
    }
}

/// Durable value stored beside a secondary-index key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexRecord {
    pub path_generation: Generation,
    pub compact_projection: TypedProjection,
}

impl SecondaryIndexRecord {
    pub fn encode(&self) -> Result<Vec<u8>, QueryRecordError> {
        let projection = self.compact_projection.encode()?;
        let mut encoded = Vec::with_capacity(1 + 8 + 4 + projection.len());
        encoded.push(QUERY_RECORD_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.path_generation.get().to_be_bytes());
        put_bytes(&mut encoded, "compact_projection", &projection)?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, QueryRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version(QUERY_RECORD_VALUE_FORMAT_VERSION)?;
        let generation = decoder.u64("path_generation")?;
        let path_generation =
            Generation::new(generation).map_err(|_| QueryRecordError::ZeroScalar {
                field: "path_generation",
            })?;
        let compact_projection = TypedProjection::decode(
            decoder.bytes("compact_projection", MAX_TYPED_PROJECTION_BYTES)?,
        )?;
        decoder.finish()?;
        let record = Self {
            path_generation,
            compact_projection,
        };
        if record.encode()? != encoded {
            return Err(QueryRecordError::NonCanonicalEncoding);
        }
        Ok(record)
    }
}

/// Typed user-visible metadata event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ChangeEventKind {
    WorkspaceCreated = 1,
    ArtifactPublished = 2,
    PathRemoved = 3,
    WorkspaceRestored = 4,
    SnapshotMinted = 5,
    SnapshotRenewed = 6,
    SnapshotRetired = 7,
    SnapshotReapClaimed = 8,
    SnapshotReaped = 9,
    SnapshotConsumerAttached = 10,
    SnapshotConsumerReleased = 11,
    CommitAdvanced = 12,
    CommitRetired = 13,
}

impl TryFrom<u8> for ChangeEventKind {
    type Error = QueryRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::WorkspaceCreated),
            2 => Ok(Self::ArtifactPublished),
            3 => Ok(Self::PathRemoved),
            4 => Ok(Self::WorkspaceRestored),
            5 => Ok(Self::SnapshotMinted),
            6 => Ok(Self::SnapshotRenewed),
            7 => Ok(Self::SnapshotRetired),
            8 => Ok(Self::SnapshotReapClaimed),
            9 => Ok(Self::SnapshotReaped),
            10 => Ok(Self::SnapshotConsumerAttached),
            11 => Ok(Self::SnapshotConsumerReleased),
            12 => Ok(Self::CommitAdvanced),
            13 => Ok(Self::CommitRetired),
            value => Err(QueryRecordError::UnknownDiscriminant {
                type_name: "ChangeEventKind",
                value,
            }),
        }
    }
}

/// Canonical `ChangeEvent` payload.
///
/// Both stable workbench id and incarnation are mandatory so readers can
/// point-read and re-evaluate the exact visibility marker at the event's
/// commit version without a root-wide marker scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangeEventRecord {
    pub workbench_id: WorkbenchId,
    pub workspace_incarnation_id: WorkspaceIncarnationId,
    pub kind: ChangeEventKind,
    pub artifact_revision_id: Option<ArtifactRevisionId>,
    pub commit_id: Option<CommitId>,
    pub operation_id: Option<OperationId>,
    pub path: Option<NormalizedRelativePath>,
    pub before: TypedProjection,
    pub after: TypedProjection,
}

impl ChangeEventRecord {
    pub fn encode(&self) -> Result<Vec<u8>, QueryRecordError> {
        let before = self.before.encode()?;
        let after = self.after.encode()?;
        let mut encoded = Vec::new();
        encoded.push(CHANGE_EVENT_VALUE_FORMAT_VERSION);
        put_bytes(&mut encoded, "workbench_id", self.workbench_id.as_bytes())?;
        encoded.extend_from_slice(self.workspace_incarnation_id.as_bytes());
        encoded.push(self.kind as u8);
        put_optional_fixed(
            &mut encoded,
            self.artifact_revision_id
                .as_ref()
                .map(ArtifactRevisionId::as_bytes),
        );
        put_optional_fixed(
            &mut encoded,
            self.commit_id.as_ref().map(CommitId::as_bytes),
        );
        put_optional_fixed(
            &mut encoded,
            self.operation_id.as_ref().map(OperationId::as_bytes),
        );
        match &self.path {
            None => encoded.push(0),
            Some(path) => {
                encoded.push(1);
                put_bytes(&mut encoded, "path", path.as_str().as_bytes())?;
            }
        }
        put_bytes(&mut encoded, "before", &before)?;
        put_bytes(&mut encoded, "after", &after)?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, QueryRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version(CHANGE_EVENT_VALUE_FORMAT_VERSION)?;
        let workbench_bytes = decoder.bytes("workbench_id", WorkbenchId::MAX_BYTES)?;
        let workbench_value =
            std::str::from_utf8(workbench_bytes).map_err(|_| QueryRecordError::InvalidUtf8 {
                field: "workbench_id",
            })?;
        let workbench_id = WorkbenchId::new(workbench_value).map_err(|error| {
            QueryRecordError::InvalidWorkbenchId {
                reason: error.to_string(),
            }
        })?;
        let workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes(decoder.fixed("workspace_incarnation_id")?);
        let kind = ChangeEventKind::try_from(decoder.u8("kind")?)?;
        let artifact_revision_id = decoder
            .optional_fixed::<FIXED_ID_BYTES>("artifact_revision_id")?
            .map(ArtifactRevisionId::from_bytes);
        let commit_id = decoder
            .optional_fixed::<SHA256_BYTES>("commit_id")?
            .map(CommitId::from_bytes);
        let operation_id = decoder
            .optional_fixed::<FIXED_ID_BYTES>("operation_id")?
            .map(OperationId::from_bytes);
        let path = match decoder.u8("path_tag")? {
            0 => None,
            1 => {
                let bytes = decoder.bytes("path", NormalizedRelativePath::MAX_BYTES)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| QueryRecordError::InvalidUtf8 { field: "path" })?;
                Some(
                    NormalizedRelativePath::new(value.to_owned()).map_err(|error| {
                        QueryRecordError::InvalidPath {
                            reason: error.to_string(),
                        }
                    })?,
                )
            }
            value => {
                return Err(QueryRecordError::InvalidOptionalTag {
                    field: "path",
                    value,
                })
            }
        };
        let before = TypedProjection::decode(decoder.bytes("before", MAX_TYPED_PROJECTION_BYTES)?)?;
        let after = TypedProjection::decode(decoder.bytes("after", MAX_TYPED_PROJECTION_BYTES)?)?;
        decoder.finish()?;
        let record = Self {
            workbench_id,
            workspace_incarnation_id,
            kind,
            artifact_revision_id,
            commit_id,
            operation_id,
            path,
            before,
            after,
        };
        if record.encode()? != encoded {
            return Err(QueryRecordError::NonCanonicalEncoding);
        }
        Ok(record)
    }
}

/// Encode one scalar for an order-preserving secondary-index key.
///
/// Variable bytes use a NUL escape and a two-byte terminator, so a value that
/// is a prefix of another value sorts first without colliding with the
/// following workspace/path suffix.
pub fn encode_ordered_index_scalar(value: &QueryScalar) -> Vec<u8> {
    let mut encoded = vec![value.discriminant()];
    match value {
        QueryScalar::Null => {}
        QueryScalar::Boolean(value) => encoded.push(u8::from(*value)),
        QueryScalar::Signed(value) | QueryScalar::Timestamp(value) => {
            encoded.extend_from_slice(&((*value as u64) ^ (1_u64 << 63)).to_be_bytes());
        }
        QueryScalar::Unsigned(value) => encoded.extend_from_slice(&value.to_be_bytes()),
        QueryScalar::Float(value) => {
            let bits = value.0;
            let ordered = if bits & (1_u64 << 63) == 0 {
                bits ^ (1_u64 << 63)
            } else {
                !bits
            };
            encoded.extend_from_slice(&ordered.to_be_bytes());
        }
        QueryScalar::Bytes(value) => put_ordered_bytes(&mut encoded, value),
        QueryScalar::String(value) => put_ordered_bytes(&mut encoded, value.as_bytes()),
    }
    encoded
}

/// Prefix for every secondary-index row of one root and field.
pub fn secondary_index_field_prefix(root: RootId, field: &QueryFieldId) -> Vec<u8> {
    let mut key = Vec::with_capacity(FIXED_ID_BYTES + 2 + field.as_bytes().len());
    key.extend_from_slice(root.as_bytes());
    key.extend_from_slice(
        &u16::try_from(field.as_bytes().len())
            .expect("validated query field id fits u16")
            .to_be_bytes(),
    );
    key.extend_from_slice(field.as_bytes());
    key
}

/// Canonical secondary-index key for one typed field value and path.
///
/// The ordered scalar is self-delimiting for variable-width values. Fixed
/// values have a type-defined width, so the workspace and component-encoded
/// path suffix is always unambiguous.
pub fn secondary_index_key(
    root: RootId,
    field: &QueryFieldId,
    value: &QueryScalar,
    workspace: WorkspaceIncarnationId,
    path: &NormalizedRelativePath,
) -> Vec<u8> {
    let mut key = secondary_index_field_prefix(root, field);
    key.extend_from_slice(&encode_ordered_index_scalar(value));
    key.extend_from_slice(workspace.as_bytes());
    push_ordered_path_components(&mut key, path);
    key.push(PATH_EXACT_TERMINATOR);
    key
}

/// Strict typed-query payload failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryRecordError {
    UnsupportedValueVersion {
        actual: u8,
        expected: u8,
    },
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
    },
    EmptyFieldId,
    InvalidFieldId {
        index: usize,
        byte: u8,
    },
    ReservedFieldId {
        field_id: String,
    },
    FieldCountLimit {
        count: usize,
        max: usize,
    },
    LengthLimit {
        field: &'static str,
        length: usize,
        max: usize,
    },
    InvalidUtf8 {
        field: &'static str,
    },
    InvalidBoolean {
        value: u8,
    },
    NonFiniteFloat,
    NonCanonicalFloatZero,
    ZeroScalar {
        field: &'static str,
    },
    InvalidOptionalTag {
        field: &'static str,
        value: u8,
    },
    InvalidPath {
        reason: String,
    },
    InvalidWorkbenchId {
        reason: String,
    },
    NonCanonicalFieldOrder,
    NonCanonicalEncoding,
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
}

impl fmt::Display for QueryRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported query value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::EmptyFieldId => formatter.write_str("query field id must not be empty"),
            Self::InvalidFieldId { index, byte } => write!(
                formatter,
                "query field id has unsupported byte 0x{byte:02x} at offset {index}"
            ),
            Self::ReservedFieldId { field_id } => {
                write!(
                    formatter,
                    "typed projection shadows built-in field {field_id}"
                )
            }
            Self::FieldCountLimit { count, max } => {
                write!(
                    formatter,
                    "typed projection has {count} fields, maximum is {max}"
                )
            }
            Self::LengthLimit { field, length, max } => {
                write!(formatter, "{field} length {length} exceeds maximum {max}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidBoolean { value } => {
                write!(formatter, "boolean discriminant {value} is invalid")
            }
            Self::NonFiniteFloat => formatter.write_str("query float must be finite"),
            Self::NonCanonicalFloatZero => {
                formatter.write_str("query float negative zero is not canonical")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::InvalidPath { reason } => write!(formatter, "invalid event path: {reason}"),
            Self::InvalidWorkbenchId { reason } => {
                write!(formatter, "invalid event workbench id: {reason}")
            }
            Self::NonCanonicalFieldOrder => {
                formatter.write_str("typed projection fields are not strictly ordered")
            }
            Self::NonCanonicalEncoding => {
                formatter.write_str("typed query payload is not canonically encoded")
            }
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(formatter, "typed query payload has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for QueryRecordError {}

fn validate_field_id(field: &str) -> Result<(), QueryRecordError> {
    if field.is_empty() {
        return Err(QueryRecordError::EmptyFieldId);
    }
    if field.len() > MAX_QUERY_FIELD_ID_BYTES {
        return Err(QueryRecordError::LengthLimit {
            field: "field_id",
            length: field.len(),
            max: MAX_QUERY_FIELD_ID_BYTES,
        });
    }
    if let Some((index, byte)) = field
        .bytes()
        .enumerate()
        .find(|(_, byte)| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'.' | b'-'))
    {
        return Err(QueryRecordError::InvalidFieldId { index, byte });
    }
    Ok(())
}

fn validate_projection_fields(
    fields: &BTreeMap<QueryFieldId, QueryScalar>,
) -> Result<(), QueryRecordError> {
    if fields.len() > MAX_TYPED_PROJECTION_FIELDS {
        return Err(QueryRecordError::FieldCountLimit {
            count: fields.len(),
            max: MAX_TYPED_PROJECTION_FIELDS,
        });
    }
    for (field, value) in fields {
        validate_field_id(field.as_str())?;
        if field.is_builtin() {
            return Err(QueryRecordError::ReservedFieldId {
                field_id: field.to_string(),
            });
        }
        match value {
            QueryScalar::Bytes(value) if value.len() > MAX_QUERY_SCALAR_BYTES => {
                return Err(QueryRecordError::LengthLimit {
                    field: "bytes_scalar",
                    length: value.len(),
                    max: MAX_QUERY_SCALAR_BYTES,
                });
            }
            QueryScalar::String(value) if value.len() > MAX_QUERY_SCALAR_BYTES => {
                return Err(QueryRecordError::LengthLimit {
                    field: "string_scalar",
                    length: value.len(),
                    max: MAX_QUERY_SCALAR_BYTES,
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn put_scalar(encoded: &mut Vec<u8>, value: &QueryScalar) -> Result<(), QueryRecordError> {
    encoded.push(value.discriminant());
    match value {
        QueryScalar::Null => {}
        QueryScalar::Boolean(value) => encoded.push(u8::from(*value)),
        QueryScalar::Signed(value) | QueryScalar::Timestamp(value) => {
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        QueryScalar::Unsigned(value) => encoded.extend_from_slice(&value.to_be_bytes()),
        QueryScalar::Float(value) => encoded.extend_from_slice(&value.0.to_be_bytes()),
        QueryScalar::Bytes(value) => put_bytes(encoded, "bytes_scalar", value)?,
        QueryScalar::String(value) => {
            put_bytes(encoded, "string_scalar", value.as_bytes())?;
        }
    }
    Ok(())
}

fn put_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
) -> Result<(), QueryRecordError> {
    if value.len() > MAX_QUERY_SCALAR_BYTES && field != "path" {
        return Err(QueryRecordError::LengthLimit {
            field,
            length: value.len(),
            max: MAX_QUERY_SCALAR_BYTES,
        });
    }
    let length = u32::try_from(value.len()).map_err(|_| QueryRecordError::LengthLimit {
        field,
        length: value.len(),
        max: u32::MAX as usize,
    })?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn put_short_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
) -> Result<(), QueryRecordError> {
    let length = u16::try_from(value.len()).map_err(|_| QueryRecordError::LengthLimit {
        field,
        length: value.len(),
        max: u16::MAX as usize,
    })?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn put_ordered_bytes(encoded: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            encoded.extend_from_slice(&[0, 0xff]);
        } else {
            encoded.push(*byte);
        }
    }
    encoded.extend_from_slice(&[0, 0]);
}

fn put_optional_fixed<const N: usize>(encoded: &mut Vec<u8>, value: Option<&[u8; N]>) {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(value);
        }
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { remaining: encoded }
    }

    fn require_version(&mut self, expected: u8) -> Result<(), QueryRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual != expected {
            return Err(QueryRecordError::UnsupportedValueVersion { actual, expected });
        }
        Ok(())
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, QueryRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, QueryRecordError> {
        Ok(u16::from_be_bytes(
            self.take(field, 2)?.try_into().expect("exact length"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, QueryRecordError> {
        Ok(u64::from_be_bytes(
            self.take(field, 8)?.try_into().expect("exact length"),
        ))
    }

    fn i64(&mut self, field: &'static str) -> Result<i64, QueryRecordError> {
        Ok(i64::from_be_bytes(
            self.take(field, 8)?.try_into().expect("exact length"),
        ))
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], QueryRecordError> {
        Ok(self.take(field, N)?.try_into().expect("exact length"))
    }

    fn optional_fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<Option<[u8; N]>, QueryRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.fixed(field).map(Some),
            value => Err(QueryRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn bytes(&mut self, field: &'static str, max: usize) -> Result<&'a [u8], QueryRecordError> {
        let length =
            u32::from_be_bytes(self.take(field, 4)?.try_into().expect("exact length")) as usize;
        if length > max {
            return Err(QueryRecordError::LengthLimit { field, length, max });
        }
        self.take(field, length)
    }

    fn short_string(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<String, QueryRecordError> {
        let length = usize::from(self.u16(field)?);
        if length > max {
            return Err(QueryRecordError::LengthLimit { field, length, max });
        }
        let bytes = self.take(field, length)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| QueryRecordError::InvalidUtf8 { field })
    }

    fn scalar(&mut self) -> Result<QueryScalar, QueryRecordError> {
        let scalar_type = QueryScalarType::try_from(self.u8("scalar_type")?)?;
        match scalar_type {
            QueryScalarType::Null => Ok(QueryScalar::Null),
            QueryScalarType::Boolean => match self.u8("boolean")? {
                0 => Ok(QueryScalar::Boolean(false)),
                1 => Ok(QueryScalar::Boolean(true)),
                value => Err(QueryRecordError::InvalidBoolean { value }),
            },
            QueryScalarType::Signed => Ok(QueryScalar::Signed(self.i64("signed")?)),
            QueryScalarType::Unsigned => Ok(QueryScalar::Unsigned(self.u64("unsigned")?)),
            QueryScalarType::Float => Ok(QueryScalar::Float(FiniteFloat::from_canonical_bits(
                self.u64("float")?,
            )?)),
            QueryScalarType::Timestamp => Ok(QueryScalar::Timestamp(self.i64("timestamp")?)),
            QueryScalarType::Bytes => Ok(QueryScalar::Bytes(
                self.bytes("bytes_scalar", MAX_QUERY_SCALAR_BYTES)?.to_vec(),
            )),
            QueryScalarType::String => {
                let bytes = self.bytes("string_scalar", MAX_QUERY_SCALAR_BYTES)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| QueryRecordError::InvalidUtf8 {
                        field: "string_scalar",
                    })?
                    .to_owned();
                Ok(QueryScalar::String(value))
            }
        }
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], QueryRecordError> {
        if self.remaining.len() < length {
            return Err(QueryRecordError::Truncated {
                field,
                needed: length,
                remaining: self.remaining.len(),
            });
        }
        let (head, tail) = self.remaining.split_at(length);
        self.remaining = tail;
        Ok(head)
    }

    fn finish(self) -> Result<(), QueryRecordError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(QueryRecordError::TrailingBytes {
                count: self.remaining.len(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use nokv_types::FIXED_ID_BYTES;

    use super::*;

    fn field(value: &str) -> QueryFieldId {
        QueryFieldId::new(value).unwrap()
    }

    #[test]
    fn typed_projection_has_one_canonical_order_and_golden_bytes() {
        let projection = TypedProjection::new(BTreeMap::from([
            (field("alpha"), QueryScalar::Boolean(true)),
            (field("zeta"), QueryScalar::Unsigned(9)),
        ]))
        .unwrap();
        let encoded = projection.encode().unwrap();
        assert_eq!(
            encoded,
            [
                &[1, 0, 2][..],
                &[0, 5][..],
                b"alpha",
                &[2, 1][..],
                &[0, 4][..],
                b"zeta",
                &[4][..],
                &9_u64.to_be_bytes(),
            ]
            .concat()
        );
        assert_eq!(TypedProjection::decode(&encoded).unwrap(), projection);
    }

    #[test]
    fn typed_projection_rejects_unknown_noncanonical_and_reserved_data() {
        let mut unknown = TypedProjection::empty().encode().unwrap();
        unknown[0] = 2;
        assert!(matches!(
            TypedProjection::decode(&unknown),
            Err(QueryRecordError::UnsupportedValueVersion { actual: 2, .. })
        ));

        let out_of_order = [
            &[1, 0, 2][..],
            &[0, 1][..],
            b"z",
            &[1][..],
            &[0, 1][..],
            b"a",
            &[1][..],
        ]
        .concat();
        assert_eq!(
            TypedProjection::decode(&out_of_order),
            Err(QueryRecordError::NonCanonicalFieldOrder)
        );
        assert!(matches!(
            TypedProjection::new(BTreeMap::from([(
                field("path"),
                QueryScalar::String("shadow".to_owned())
            )])),
            Err(QueryRecordError::ReservedFieldId { .. })
        ));
        assert_eq!(
            FiniteFloat::new(f64::NAN),
            Err(QueryRecordError::NonFiniteFloat)
        );
        assert_eq!(
            FiniteFloat::from_canonical_bits((-0.0_f64).to_bits()),
            Err(QueryRecordError::NonCanonicalFloatZero)
        );
    }

    #[test]
    fn ordered_scalar_encoding_preserves_numeric_and_prefix_order() {
        let signed =
            [-5, -1, 0, 1, 9].map(|value| encode_ordered_index_scalar(&QueryScalar::Signed(value)));
        assert!(signed.windows(2).all(|pair| pair[0] < pair[1]));

        let values = ["a", "a\0", "aa", "ab"]
            .map(|value| encode_ordered_index_scalar(&QueryScalar::String(value.to_owned())));
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
        assert_ne!(values[0], values[1]);

        let root = RootId::from_bytes([1; FIXED_ID_BYTES]);
        let workspace = WorkspaceIncarnationId::from_bytes([2; FIXED_ID_BYTES]);
        let field = field("run.label");
        let prefix = secondary_index_field_prefix(root, &field);
        let scalar = QueryScalar::String("same".to_owned());
        let key_a = secondary_index_key(
            root,
            &field,
            &scalar,
            workspace,
            &NormalizedRelativePath::new("a").unwrap(),
        );
        let key_a_control = secondary_index_key(
            root,
            &field,
            &scalar,
            workspace,
            &NormalizedRelativePath::new("a\u{1}").unwrap(),
        );
        let key_ab = secondary_index_key(
            root,
            &field,
            &scalar,
            workspace,
            &NormalizedRelativePath::new("ab").unwrap(),
        );
        assert!(key_a.starts_with(&prefix));
        assert!(key_a_control.starts_with(&prefix));
        assert!(key_ab.starts_with(&prefix));
        assert_ne!(key_a, key_ab);
        assert!(!key_a_control.starts_with(&key_a));
        assert!(!key_ab.starts_with(&key_a));
        assert!(key_a < key_a_control);
        assert!(key_a_control < key_ab);
        assert!(key_a < key_ab);
    }

    #[test]
    fn secondary_index_and_change_event_round_trip_strictly() {
        let projection = TypedProjection::new(BTreeMap::from([(
            field("run.step"),
            QueryScalar::Unsigned(42),
        )]))
        .unwrap();
        let index = SecondaryIndexRecord {
            path_generation: Generation::new(3).unwrap(),
            compact_projection: projection.clone(),
        };
        let index_bytes = index.encode().unwrap();
        assert_eq!(SecondaryIndexRecord::decode(&index_bytes).unwrap(), index);

        let golden_event = ChangeEventRecord {
            workbench_id: WorkbenchId::new("wb").unwrap(),
            workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([7; FIXED_ID_BYTES]),
            kind: ChangeEventKind::WorkspaceCreated,
            artifact_revision_id: None,
            commit_id: None,
            operation_id: None,
            path: None,
            before: TypedProjection::empty(),
            after: TypedProjection::empty(),
        };
        let golden = [
            &[CHANGE_EVENT_VALUE_FORMAT_VERSION][..],
            &2_u32.to_be_bytes(),
            b"wb",
            &[7; FIXED_ID_BYTES],
            &[1, 0, 0, 0, 0],
            &3_u32.to_be_bytes(),
            &[QUERY_RECORD_VALUE_FORMAT_VERSION, 0, 0],
            &3_u32.to_be_bytes(),
            &[QUERY_RECORD_VALUE_FORMAT_VERSION, 0, 0],
        ]
        .concat();
        assert_eq!(golden_event.encode().unwrap(), golden);
        assert_eq!(ChangeEventRecord::decode(&golden).unwrap(), golden_event);

        let mut invalid_utf8 = golden.clone();
        invalid_utf8[5] = 0xff;
        assert!(matches!(
            ChangeEventRecord::decode(&invalid_utf8),
            Err(QueryRecordError::InvalidUtf8 {
                field: "workbench_id"
            })
        ));
        let mut invalid_id = golden;
        invalid_id[5] = b'!';
        assert!(matches!(
            ChangeEventRecord::decode(&invalid_id),
            Err(QueryRecordError::InvalidWorkbenchId { .. })
        ));

        let event = ChangeEventRecord {
            workbench_id: WorkbenchId::new("agent-run").unwrap(),
            workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([7; FIXED_ID_BYTES]),
            kind: ChangeEventKind::ArtifactPublished,
            artifact_revision_id: Some(ArtifactRevisionId::from_bytes([8; FIXED_ID_BYTES])),
            commit_id: None,
            operation_id: Some(OperationId::from_bytes([9; FIXED_ID_BYTES])),
            path: Some(NormalizedRelativePath::new("a/file").unwrap()),
            before: TypedProjection::empty(),
            after: projection,
        };
        let event_bytes = event.encode().unwrap();
        assert_eq!(ChangeEventRecord::decode(&event_bytes).unwrap(), event);

        let mut legacy = event_bytes.clone();
        legacy[0] = QUERY_RECORD_VALUE_FORMAT_VERSION;
        assert_eq!(
            ChangeEventRecord::decode(&legacy),
            Err(QueryRecordError::UnsupportedValueVersion {
                actual: QUERY_RECORD_VALUE_FORMAT_VERSION,
                expected: CHANGE_EVENT_VALUE_FORMAT_VERSION,
            })
        );

        let mut trailing = event_bytes;
        trailing.push(0);
        assert!(matches!(
            ChangeEventRecord::decode(&trailing),
            Err(QueryRecordError::TrailingBytes { count: 1 })
        ));
    }
}
