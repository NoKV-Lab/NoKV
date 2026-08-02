/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable workspace payload records used by atomic artifact publication.
//!
//! These codecs encode only family-owned payload fields. `created_version` and
//! `modified_version` belong exclusively to the surrounding `CurrentValue`
//! envelope and must never be duplicated here.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommitVersion, GcClaimState, Generation, OperationId, ReferenceEpoch,
    RevisionState, WorkbenchId, WorkspaceIncarnationId, WorkspaceRevision, WorkspaceState,
    FIXED_ID_BYTES, SHA256_BYTES,
};

/// Only supported value format for publication payload records.
///
/// Version 2 makes `PathCurrent` a complete immutable `PathMetadata`
/// projection, so exact reads never have to follow the revision-lifetime row.
pub const PUBLICATION_VALUE_FORMAT_VERSION: u8 = 2;

/// Maximum encoded digest URI length.
pub const MAX_DIGEST_URI_BYTES: usize = 256;
/// Maximum encoded content type length.
pub const MAX_CONTENT_TYPE_BYTES: usize = 255;
/// Maximum encoded producer/provenance summary length.
pub const MAX_PRODUCER_BYTES: usize = 512;
/// Maximum encoded provider-neutral manifest id length.
pub const MAX_MANIFEST_ID_BYTES: usize = 1_024;
/// Maximum canonical typed secondary-index projection length.
pub const MAX_INDEX_PROJECTION_BYTES: usize = 64 * 1_024;
/// Maximum retained provider quarantine evidence length.
pub const MAX_QUARANTINE_EVIDENCE_BYTES: usize = 4 * 1_024;
/// Maximum number of distinct physical owner revisions retained by a child.
pub const MAX_DEPENDENCY_COUNT: u32 = 64;
/// Maximum sealed artifact-revision dependency depth.
pub const MAX_DEPENDENCY_DEPTH: u8 = 8;

/// Current durable marker for a workbench name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceRecord {
    pub incarnation_id: WorkspaceIncarnationId,
    pub workspace_revision: WorkspaceRevision,
    pub state: WorkspaceState,
    pub owning_operation_id: Option<OperationId>,
}

/// Permanent owner of one never-reused workspace incarnation identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkspaceIncarnationClaimRecord {
    pub workbench_id: WorkbenchId,
}

/// Current authoritative value for one normalized path.
///
/// Immutable revision fields needed by `PathMetadata` are copied here in the
/// same atomic publication command. `ArtifactRevisionRecord` remains the
/// reachability/GC lifetime row, not an exact-read dependency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathEntry {
    pub generation: Generation,
    pub artifact_revision_id: ArtifactRevisionId,
    /// Digest URI for the complete resulting body, not an append delta.
    pub body_digest_uri: String,
    /// Digest URI for the immutable manifest owned by this revision.
    pub manifest_digest_uri: String,
    pub logical_size: u64,
    /// Number of distinct physical owner revisions retained by this revision.
    pub dependency_count: u32,
    /// Maximum sealed dependency path ending at this revision.
    pub dependency_depth: u8,
    pub content_type: String,
    pub producer: Option<String>,
    pub manifest_id: Option<String>,
    /// Canonical bytes emitted by the registered typed index-projection codec.
    pub typed_index_projection: Vec<u8>,
}

/// Immutable revision descriptor and its strong-reference lifetime state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRevisionRecord {
    pub logical_size: u64,
    pub body_digest_uri: String,
    pub manifest_digest_uri: String,
    pub block_count: u64,
    pub dependency_count: u32,
    pub dependency_depth: u8,
    /// SHA-256 of the canonical, sorted dependency closure.
    pub dependency_digest: [u8; SHA256_BYTES],
    pub content_type: String,
    pub state: RevisionState,
    pub reference_epoch: ReferenceEpoch,
    pub strong_reference_count: u64,
    pub last_zero_ref_version: Option<CommitVersion>,
}

/// Strong reference row payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RevisionRefRecord {
    pub reference_epoch_at_add: ReferenceEpoch,
}

/// Epoch-keyed candidate for fenced artifact-revision garbage collection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcCandidateRecord {
    pub last_zero_ref_version: CommitVersion,
    pub claim_state: GcClaimState,
    pub retry_count: u32,
    pub quarantine_evidence: Option<Vec<u8>>,
}

/// Strict format-v1 publication-record encode or decode failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationRecordCodecError {
    UnsupportedValueVersion {
        actual: u8,
        expected: u8,
    },
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
    },
    ZeroScalar {
        field: &'static str,
    },
    InvalidOptionalTag {
        field: &'static str,
        value: u8,
    },
    LengthLimit {
        field: &'static str,
        length: usize,
        max: usize,
    },
    InvalidUtf8 {
        field: &'static str,
    },
    InvalidWorkbenchId {
        reason: String,
    },
    EmptyString {
        field: &'static str,
    },
    ContainsNul {
        field: &'static str,
        index: usize,
    },
    DependencyCountLimit {
        count: u32,
        max: u32,
    },
    DependencyDepthLimit {
        depth: u8,
        max: u8,
    },
    InvalidDependencyShape,
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
}

impl fmt::Display for PublicationRecordCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported publication value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::LengthLimit { field, length, max } => {
                write!(formatter, "{field} length {length} exceeds maximum {max}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidWorkbenchId { reason } => {
                write!(formatter, "invalid claimed workbench id: {reason}")
            }
            Self::EmptyString { field } => write!(formatter, "{field} must not be empty"),
            Self::ContainsNul { field, index } => {
                write!(formatter, "{field} contains NUL at byte offset {index}")
            }
            Self::DependencyCountLimit { count, max } => {
                write!(formatter, "dependency count {count} exceeds maximum {max}")
            }
            Self::DependencyDepthLimit { depth, max } => {
                write!(formatter, "dependency depth {depth} exceeds maximum {max}")
            }
            Self::InvalidDependencyShape => formatter.write_str(
                "dependency count and depth must either both be zero or both be non-zero",
            ),
            Self::Truncated {
                field,
                needed,
                remaining,
            } => write!(
                formatter,
                "truncated {field}: need {needed} bytes, have {remaining}"
            ),
            Self::TrailingBytes { count } => {
                write!(formatter, "publication value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for PublicationRecordCodecError {}

impl WorkspaceRecord {
    pub fn encode(&self) -> Result<Vec<u8>, PublicationRecordCodecError> {
        let mut encoded = Vec::with_capacity(1 + FIXED_ID_BYTES + 8 + 1 + 1 + FIXED_ID_BYTES);
        encoded.push(PUBLICATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.incarnation_id.as_bytes());
        encoded.extend_from_slice(&self.workspace_revision.get().to_be_bytes());
        encoded.push(self.state.into());
        put_optional_fixed(
            &mut encoded,
            self.owning_operation_id.as_ref().map(OperationId::as_bytes),
        );
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublicationRecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let incarnation_id = WorkspaceIncarnationId::from_bytes(decoder.fixed("incarnation_id")?);
        let workspace_revision = WorkspaceRevision::new(decoder.u64("workspace_revision")?);
        let state = decode_durable_enum(decoder.u8("state")?)?;
        let owning_operation_id = decoder
            .optional_fixed("owning_operation_id")?
            .map(OperationId::from_bytes);
        decoder.finish()?;
        Ok(Self {
            incarnation_id,
            workspace_revision,
            state,
            owning_operation_id,
        })
    }
}

impl WorkspaceIncarnationClaimRecord {
    pub fn encode(&self) -> Result<Vec<u8>, PublicationRecordCodecError> {
        let mut encoded = Vec::with_capacity(1 + 4 + self.workbench_id.as_bytes().len());
        encoded.push(PUBLICATION_VALUE_FORMAT_VERSION);
        put_bounded_bytes(
            &mut encoded,
            "workbench_id",
            self.workbench_id.as_bytes(),
            WorkbenchId::MAX_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublicationRecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let bytes = decoder.bounded_bytes("workbench_id", WorkbenchId::MAX_BYTES)?;
        let value =
            String::from_utf8(bytes).map_err(|_| PublicationRecordCodecError::InvalidUtf8 {
                field: "workbench_id",
            })?;
        let workbench_id = WorkbenchId::new(value).map_err(|error| {
            PublicationRecordCodecError::InvalidWorkbenchId {
                reason: error.to_string(),
            }
        })?;
        decoder.finish()?;
        Ok(Self { workbench_id })
    }
}

impl PathEntry {
    pub fn encode(&self) -> Result<Vec<u8>, PublicationRecordCodecError> {
        validate_required_string(
            "body_digest_uri",
            &self.body_digest_uri,
            MAX_DIGEST_URI_BYTES,
        )?;
        validate_required_string(
            "manifest_digest_uri",
            &self.manifest_digest_uri,
            MAX_DIGEST_URI_BYTES,
        )?;
        validate_dependency_bounds(self.dependency_count, self.dependency_depth)?;
        validate_required_string("content_type", &self.content_type, MAX_CONTENT_TYPE_BYTES)?;
        validate_optional_string("producer", self.producer.as_deref(), MAX_PRODUCER_BYTES)?;
        validate_optional_string(
            "manifest_id",
            self.manifest_id.as_deref(),
            MAX_MANIFEST_ID_BYTES,
        )?;
        validate_length(
            "typed_index_projection",
            self.typed_index_projection.len(),
            MAX_INDEX_PROJECTION_BYTES,
        )?;

        let mut encoded = Vec::new();
        encoded.push(PUBLICATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.generation.get().to_be_bytes());
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        put_bounded_bytes(
            &mut encoded,
            "body_digest_uri",
            self.body_digest_uri.as_bytes(),
            MAX_DIGEST_URI_BYTES,
        )?;
        put_bounded_bytes(
            &mut encoded,
            "manifest_digest_uri",
            self.manifest_digest_uri.as_bytes(),
            MAX_DIGEST_URI_BYTES,
        )?;
        encoded.extend_from_slice(&self.logical_size.to_be_bytes());
        encoded.extend_from_slice(&self.dependency_count.to_be_bytes());
        encoded.push(self.dependency_depth);
        put_bounded_bytes(
            &mut encoded,
            "content_type",
            self.content_type.as_bytes(),
            MAX_CONTENT_TYPE_BYTES,
        )?;
        put_optional_string(
            &mut encoded,
            "producer",
            self.producer.as_deref(),
            MAX_PRODUCER_BYTES,
        )?;
        put_optional_string(
            &mut encoded,
            "manifest_id",
            self.manifest_id.as_deref(),
            MAX_MANIFEST_ID_BYTES,
        )?;
        put_bounded_bytes(
            &mut encoded,
            "typed_index_projection",
            &self.typed_index_projection,
            MAX_INDEX_PROJECTION_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublicationRecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let generation = Generation::new(decoder.u64("generation")?).map_err(|_| {
            PublicationRecordCodecError::ZeroScalar {
                field: "generation",
            }
        })?;
        let artifact_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("artifact_revision_id")?);
        let body_digest_uri = decoder.required_string("body_digest_uri", MAX_DIGEST_URI_BYTES)?;
        let manifest_digest_uri =
            decoder.required_string("manifest_digest_uri", MAX_DIGEST_URI_BYTES)?;
        let logical_size = decoder.u64("logical_size")?;
        let dependency_count = decoder.u32("dependency_count")?;
        let dependency_depth = decoder.u8("dependency_depth")?;
        validate_dependency_bounds(dependency_count, dependency_depth)?;
        let content_type = decoder.required_string("content_type", MAX_CONTENT_TYPE_BYTES)?;
        let producer = decoder.optional_string("producer", MAX_PRODUCER_BYTES)?;
        let manifest_id = decoder.optional_string("manifest_id", MAX_MANIFEST_ID_BYTES)?;
        let typed_index_projection =
            decoder.bounded_bytes("typed_index_projection", MAX_INDEX_PROJECTION_BYTES)?;
        decoder.finish()?;
        Ok(Self {
            generation,
            artifact_revision_id,
            body_digest_uri,
            manifest_digest_uri,
            logical_size,
            dependency_count,
            dependency_depth,
            content_type,
            producer,
            manifest_id,
            typed_index_projection,
        })
    }
}

impl ArtifactRevisionRecord {
    pub fn encode(&self) -> Result<Vec<u8>, PublicationRecordCodecError> {
        validate_required_string(
            "body_digest_uri",
            &self.body_digest_uri,
            MAX_DIGEST_URI_BYTES,
        )?;
        validate_required_string(
            "manifest_digest_uri",
            &self.manifest_digest_uri,
            MAX_DIGEST_URI_BYTES,
        )?;
        validate_required_string("content_type", &self.content_type, MAX_CONTENT_TYPE_BYTES)?;
        validate_dependency_bounds(self.dependency_count, self.dependency_depth)?;

        let mut encoded = Vec::new();
        encoded.push(PUBLICATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.logical_size.to_be_bytes());
        put_bounded_bytes(
            &mut encoded,
            "body_digest_uri",
            self.body_digest_uri.as_bytes(),
            MAX_DIGEST_URI_BYTES,
        )?;
        put_bounded_bytes(
            &mut encoded,
            "manifest_digest_uri",
            self.manifest_digest_uri.as_bytes(),
            MAX_DIGEST_URI_BYTES,
        )?;
        encoded.extend_from_slice(&self.block_count.to_be_bytes());
        encoded.extend_from_slice(&self.dependency_count.to_be_bytes());
        encoded.push(self.dependency_depth);
        encoded.extend_from_slice(&self.dependency_digest);
        put_bounded_bytes(
            &mut encoded,
            "content_type",
            self.content_type.as_bytes(),
            MAX_CONTENT_TYPE_BYTES,
        )?;
        encoded.push(self.state.into());
        encoded.extend_from_slice(&self.reference_epoch.get().to_be_bytes());
        encoded.extend_from_slice(&self.strong_reference_count.to_be_bytes());
        put_optional_commit_version(&mut encoded, self.last_zero_ref_version);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublicationRecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let logical_size = decoder.u64("logical_size")?;
        let body_digest_uri = decoder.required_string("body_digest_uri", MAX_DIGEST_URI_BYTES)?;
        let manifest_digest_uri =
            decoder.required_string("manifest_digest_uri", MAX_DIGEST_URI_BYTES)?;
        let block_count = decoder.u64("block_count")?;
        let dependency_count = decoder.u32("dependency_count")?;
        let dependency_depth = decoder.u8("dependency_depth")?;
        validate_dependency_bounds(dependency_count, dependency_depth)?;
        let dependency_digest = decoder.fixed("dependency_digest")?;
        let content_type = decoder.required_string("content_type", MAX_CONTENT_TYPE_BYTES)?;
        let state = decode_durable_enum(decoder.u8("state")?)?;
        let reference_epoch = ReferenceEpoch::new(decoder.u64("reference_epoch")?);
        let strong_reference_count = decoder.u64("strong_reference_count")?;
        let last_zero_ref_version = decoder.optional_commit_version("last_zero_ref_version")?;
        decoder.finish()?;
        Ok(Self {
            logical_size,
            body_digest_uri,
            manifest_digest_uri,
            block_count,
            dependency_count,
            dependency_depth,
            dependency_digest,
            content_type,
            state,
            reference_epoch,
            strong_reference_count,
            last_zero_ref_version,
        })
    }
}

impl RevisionRefRecord {
    pub fn encode(&self) -> Result<Vec<u8>, PublicationRecordCodecError> {
        let mut encoded = Vec::with_capacity(1 + 8);
        encoded.push(PUBLICATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.reference_epoch_at_add.get().to_be_bytes());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublicationRecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let reference_epoch_at_add = ReferenceEpoch::new(decoder.u64("reference_epoch_at_add")?);
        decoder.finish()?;
        Ok(Self {
            reference_epoch_at_add,
        })
    }
}

impl GcCandidateRecord {
    pub fn encode(&self) -> Result<Vec<u8>, PublicationRecordCodecError> {
        if let Some(evidence) = &self.quarantine_evidence {
            validate_length(
                "quarantine_evidence",
                evidence.len(),
                MAX_QUARANTINE_EVIDENCE_BYTES,
            )?;
        }

        let evidence_capacity = self
            .quarantine_evidence
            .as_ref()
            .map_or(0, |evidence| 4 + evidence.len());
        let mut encoded = Vec::with_capacity(1 + 8 + 1 + 4 + 1 + evidence_capacity);
        encoded.push(PUBLICATION_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.last_zero_ref_version.get().to_be_bytes());
        encoded.push(self.claim_state.into());
        encoded.extend_from_slice(&self.retry_count.to_be_bytes());
        put_optional_bytes(
            &mut encoded,
            "quarantine_evidence",
            self.quarantine_evidence.as_deref(),
            MAX_QUARANTINE_EVIDENCE_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, PublicationRecordCodecError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let last_zero_ref_version = decoder.commit_version("last_zero_ref_version")?;
        let claim_state = decode_durable_enum(decoder.u8("claim_state")?)?;
        let retry_count = decoder.u32("retry_count")?;
        let quarantine_evidence =
            decoder.optional_bytes("quarantine_evidence", MAX_QUARANTINE_EVIDENCE_BYTES)?;
        decoder.finish()?;
        Ok(Self {
            last_zero_ref_version,
            claim_state,
            retry_count,
            quarantine_evidence,
        })
    }
}

fn decode_durable_enum<T>(value: u8) -> Result<T, PublicationRecordCodecError>
where
    T: TryFrom<u8, Error = nokv_types::UnknownDurableDiscriminant>,
{
    T::try_from(value).map_err(|error| PublicationRecordCodecError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

fn validate_dependency_bounds(
    dependency_count: u32,
    dependency_depth: u8,
) -> Result<(), PublicationRecordCodecError> {
    if dependency_count > MAX_DEPENDENCY_COUNT {
        return Err(PublicationRecordCodecError::DependencyCountLimit {
            count: dependency_count,
            max: MAX_DEPENDENCY_COUNT,
        });
    }
    if dependency_depth > MAX_DEPENDENCY_DEPTH {
        return Err(PublicationRecordCodecError::DependencyDepthLimit {
            depth: dependency_depth,
            max: MAX_DEPENDENCY_DEPTH,
        });
    }
    if (dependency_count == 0) != (dependency_depth == 0) {
        return Err(PublicationRecordCodecError::InvalidDependencyShape);
    }
    Ok(())
}

fn validate_length(
    field: &'static str,
    length: usize,
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    if length > max {
        Err(PublicationRecordCodecError::LengthLimit { field, length, max })
    } else {
        Ok(())
    }
}

fn validate_required_string(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    validate_string(field, value, max)?;
    if value.is_empty() {
        Err(PublicationRecordCodecError::EmptyString { field })
    } else {
        Ok(())
    }
}

fn validate_optional_string(
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    match value {
        Some(value) => validate_required_string(field, value, max),
        None => Ok(()),
    }
}

fn validate_string(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    validate_length(field, value.len(), max)?;
    if let Some(index) = value.as_bytes().iter().position(|byte| *byte == 0) {
        Err(PublicationRecordCodecError::ContainsNul { field, index })
    } else {
        Ok(())
    }
}

fn put_bounded_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    validate_length(field, value.len(), max)?;
    let length =
        u32::try_from(value.len()).expect("all publication byte limits fit in a u32 length");
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
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

fn put_optional_string(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    validate_optional_string(field, value, max)?;
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            put_bounded_bytes(encoded, field, value.as_bytes(), max)?;
        }
    }
    Ok(())
}

fn put_optional_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: Option<&[u8]>,
    max: usize,
) -> Result<(), PublicationRecordCodecError> {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            put_bounded_bytes(encoded, field, value, max)?;
        }
    }
    Ok(())
}

fn put_optional_commit_version(encoded: &mut Vec<u8>, value: Option<CommitVersion>) {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            encoded.extend_from_slice(&value.get().to_be_bytes());
        }
    }
}

struct Decoder<'a> {
    encoded: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { encoded, offset: 0 }
    }

    fn require_value_version(&mut self) -> Result<(), PublicationRecordCodecError> {
        let actual = self.u8("value_format_version")?;
        if actual == PUBLICATION_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(PublicationRecordCodecError::UnsupportedValueVersion {
                actual,
                expected: PUBLICATION_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, PublicationRecordCodecError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, PublicationRecordCodecError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, PublicationRecordCodecError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], PublicationRecordCodecError> {
        let mut value = [0; N];
        value.copy_from_slice(self.take(field, N)?);
        Ok(value)
    }

    fn commit_version(
        &mut self,
        field: &'static str,
    ) -> Result<CommitVersion, PublicationRecordCodecError> {
        CommitVersion::new(self.u64(field)?)
            .map_err(|_| PublicationRecordCodecError::ZeroScalar { field })
    }

    fn bounded_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Vec<u8>, PublicationRecordCodecError> {
        let length = self.u32(field)? as usize;
        validate_length(field, length, max)?;
        self.take(field, length).map(<[u8]>::to_vec)
    }

    fn required_string(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<String, PublicationRecordCodecError> {
        let bytes = self.bounded_bytes(field, max)?;
        let value = String::from_utf8(bytes)
            .map_err(|_| PublicationRecordCodecError::InvalidUtf8 { field })?;
        validate_required_string(field, &value, max)?;
        Ok(value)
    }

    fn optional_fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<Option<[u8; N]>, PublicationRecordCodecError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.fixed(field).map(Some),
            value => Err(PublicationRecordCodecError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_string(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Option<String>, PublicationRecordCodecError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.required_string(field, max).map(Some),
            value => Err(PublicationRecordCodecError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Option<Vec<u8>>, PublicationRecordCodecError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.bounded_bytes(field, max).map(Some),
            value => Err(PublicationRecordCodecError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_commit_version(
        &mut self,
        field: &'static str,
    ) -> Result<Option<CommitVersion>, PublicationRecordCodecError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.commit_version(field).map(Some),
            value => Err(PublicationRecordCodecError::InvalidOptionalTag { field, value }),
        }
    }

    fn take(
        &mut self,
        field: &'static str,
        length: usize,
    ) -> Result<&'a [u8], PublicationRecordCodecError> {
        let remaining = self.encoded.len().saturating_sub(self.offset);
        let Some(end) = self.offset.checked_add(length) else {
            return Err(PublicationRecordCodecError::Truncated {
                field,
                needed: length,
                remaining,
            });
        };
        if end > self.encoded.len() {
            return Err(PublicationRecordCodecError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let value = &self.encoded[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn finish(self) -> Result<(), PublicationRecordCodecError> {
        let count = self.encoded.len() - self.offset;
        if count == 0 {
            Ok(())
        } else {
            Err(PublicationRecordCodecError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generation(value: u64) -> Generation {
        Generation::new(value).unwrap()
    }

    fn commit_version(value: u64) -> CommitVersion {
        CommitVersion::new(value).unwrap()
    }

    fn workspace_record() -> WorkspaceRecord {
        WorkspaceRecord {
            incarnation_id: WorkspaceIncarnationId::from_bytes([0x11; FIXED_ID_BYTES]),
            workspace_revision: WorkspaceRevision::new(0x0102_0304_0506_0708),
            state: WorkspaceState::Visible,
            owning_operation_id: Some(OperationId::from_bytes([0x22; FIXED_ID_BYTES])),
        }
    }

    fn path_entry() -> PathEntry {
        PathEntry {
            generation: generation(0x0102_0304_0506_0708),
            artifact_revision_id: ArtifactRevisionId::from_bytes([0x33; FIXED_ID_BYTES]),
            body_digest_uri: "sha256:body".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            logical_size: 0x1112_1314_1516_1718,
            dependency_count: 2,
            dependency_depth: 2,
            content_type: "text/plain".to_owned(),
            producer: Some("agent".to_owned()),
            manifest_id: None,
            typed_index_projection: vec![1, 2, 3],
        }
    }

    fn artifact_revision_record() -> ArtifactRevisionRecord {
        ArtifactRevisionRecord {
            logical_size: 0x0102_0304_0506_0708,
            body_digest_uri: "sha256:body".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            block_count: 0x1112_1314_1516_1718,
            dependency_count: 2,
            dependency_depth: 2,
            dependency_digest: [0x44; SHA256_BYTES],
            content_type: "application/octet-stream".to_owned(),
            state: RevisionState::Available,
            reference_epoch: ReferenceEpoch::new(7),
            strong_reference_count: 3,
            last_zero_ref_version: Some(commit_version(9)),
        }
    }

    fn gc_candidate_record() -> GcCandidateRecord {
        GcCandidateRecord {
            last_zero_ref_version: commit_version(0x0102_0304_0506_0708),
            claim_state: GcClaimState::Quarantined,
            retry_count: 0x1112_1314,
            quarantine_evidence: Some(vec![0xaa, 0xbb]),
        }
    }

    fn assert_every_proper_prefix_is_truncated<T>(
        encoded: &[u8],
        decode: impl Fn(&[u8]) -> Result<T, PublicationRecordCodecError>,
    ) {
        for length in 0..encoded.len() {
            assert!(
                matches!(
                    decode(&encoded[..length]),
                    Err(PublicationRecordCodecError::Truncated { .. })
                ),
                "prefix length {length} was not rejected as truncated"
            );
        }
    }

    fn assert_trailing_byte_is_rejected<T: fmt::Debug>(
        mut encoded: Vec<u8>,
        decode: impl Fn(&[u8]) -> Result<T, PublicationRecordCodecError>,
    ) {
        encoded.push(0);
        assert_eq!(
            decode(&encoded).unwrap_err(),
            PublicationRecordCodecError::TrailingBytes { count: 1 }
        );
    }

    #[test]
    fn workspace_record_codec_has_frozen_golden_bytes() {
        let record = workspace_record();
        let expected = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &[0x11; FIXED_ID_BYTES],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &[2, 1],
            &[0x22; FIXED_ID_BYTES],
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(WorkspaceRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, WorkspaceRecord::decode);
        assert_trailing_byte_is_rejected(expected, WorkspaceRecord::decode);
    }

    #[test]
    fn workspace_incarnation_claim_has_frozen_golden_bytes() {
        let record = WorkspaceIncarnationClaimRecord {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
        };
        let expected = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &6_u32.to_be_bytes(),
            b"run-42",
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(
            WorkspaceIncarnationClaimRecord::decode(&expected).unwrap(),
            record
        );
        assert_every_proper_prefix_is_truncated(&expected, WorkspaceIncarnationClaimRecord::decode);
        assert_trailing_byte_is_rejected(expected, WorkspaceIncarnationClaimRecord::decode);

        let invalid = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &4_u32.to_be_bytes(),
            b"bad!",
        ]
        .concat();
        assert!(matches!(
            WorkspaceIncarnationClaimRecord::decode(&invalid),
            Err(PublicationRecordCodecError::InvalidWorkbenchId { .. })
        ));
    }

    #[test]
    fn path_entry_codec_has_frozen_golden_bytes() {
        let record = path_entry();
        let expected = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &[0x33; FIXED_ID_BYTES],
            &11_u32.to_be_bytes(),
            b"sha256:body",
            &15_u32.to_be_bytes(),
            b"sha256:manifest",
            &0x1112_1314_1516_1718_u64.to_be_bytes(),
            &2_u32.to_be_bytes(),
            &[2],
            &10_u32.to_be_bytes(),
            b"text/plain",
            &[1],
            &5_u32.to_be_bytes(),
            b"agent",
            &[0],
            &3_u32.to_be_bytes(),
            &[1, 2, 3],
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(PathEntry::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, PathEntry::decode);
        assert_trailing_byte_is_rejected(expected, PathEntry::decode);
    }

    #[test]
    fn artifact_revision_codec_has_frozen_golden_bytes() {
        let record = artifact_revision_record();
        let expected = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &11_u32.to_be_bytes(),
            b"sha256:body",
            &15_u32.to_be_bytes(),
            b"sha256:manifest",
            &0x1112_1314_1516_1718_u64.to_be_bytes(),
            &2_u32.to_be_bytes(),
            &[2],
            &[0x44; SHA256_BYTES],
            &24_u32.to_be_bytes(),
            b"application/octet-stream",
            &[1],
            &7_u64.to_be_bytes(),
            &3_u64.to_be_bytes(),
            &[1],
            &9_u64.to_be_bytes(),
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(ArtifactRevisionRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, ArtifactRevisionRecord::decode);
        assert_trailing_byte_is_rejected(expected, ArtifactRevisionRecord::decode);
    }

    #[test]
    fn revision_ref_codec_has_frozen_golden_bytes() {
        let record = RevisionRefRecord {
            reference_epoch_at_add: ReferenceEpoch::new(0x0102_0304_0506_0708),
        };
        let expected = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(RevisionRefRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, RevisionRefRecord::decode);
        assert_trailing_byte_is_rejected(expected, RevisionRefRecord::decode);
    }

    #[test]
    fn gc_candidate_codec_has_frozen_golden_bytes() {
        let record = gc_candidate_record();
        let expected = [
            &[PUBLICATION_VALUE_FORMAT_VERSION][..],
            &0x0102_0304_0506_0708_u64.to_be_bytes(),
            &[4],
            &0x1112_1314_u32.to_be_bytes(),
            &[1],
            &2_u32.to_be_bytes(),
            &[0xaa, 0xbb],
        ]
        .concat();

        assert_eq!(record.encode().unwrap(), expected);
        assert_eq!(GcCandidateRecord::decode(&expected).unwrap(), record);
        assert_every_proper_prefix_is_truncated(&expected, GcCandidateRecord::decode);
        assert_trailing_byte_is_rejected(expected, GcCandidateRecord::decode);
    }

    #[test]
    fn every_codec_rejects_unknown_value_version() {
        let revision_ref = RevisionRefRecord {
            reference_epoch_at_add: ReferenceEpoch::new(1),
        };
        let claim = WorkspaceIncarnationClaimRecord {
            workbench_id: WorkbenchId::new("claim").unwrap(),
        };
        let mut encoded = [
            workspace_record().encode().unwrap(),
            claim.encode().unwrap(),
            path_entry().encode().unwrap(),
            artifact_revision_record().encode().unwrap(),
            revision_ref.encode().unwrap(),
            gc_candidate_record().encode().unwrap(),
        ];
        for value in &mut encoded {
            value[0] = PUBLICATION_VALUE_FORMAT_VERSION + 1;
        }

        assert!(matches!(
            WorkspaceRecord::decode(&encoded[0]),
            Err(PublicationRecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            WorkspaceIncarnationClaimRecord::decode(&encoded[1]),
            Err(PublicationRecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            PathEntry::decode(&encoded[2]),
            Err(PublicationRecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            ArtifactRevisionRecord::decode(&encoded[3]),
            Err(PublicationRecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            RevisionRefRecord::decode(&encoded[4]),
            Err(PublicationRecordCodecError::UnsupportedValueVersion { .. })
        ));
        assert!(matches!(
            GcCandidateRecord::decode(&encoded[5]),
            Err(PublicationRecordCodecError::UnsupportedValueVersion { .. })
        ));

        let mut previous_path_layout = path_entry().encode().unwrap();
        previous_path_layout[0] = PUBLICATION_VALUE_FORMAT_VERSION - 1;
        assert_eq!(
            PathEntry::decode(&previous_path_layout),
            Err(PublicationRecordCodecError::UnsupportedValueVersion {
                actual: PUBLICATION_VALUE_FORMAT_VERSION - 1,
                expected: PUBLICATION_VALUE_FORMAT_VERSION,
            })
        );
    }

    #[test]
    fn durable_enum_decoding_fails_closed() {
        let mut workspace = workspace_record().encode().unwrap();
        workspace[1 + FIXED_ID_BYTES + 8] = 0xff;
        assert_eq!(
            WorkspaceRecord::decode(&workspace),
            Err(PublicationRecordCodecError::UnknownDiscriminant {
                type_name: "WorkspaceState",
                value: 0xff,
            })
        );

        let mut revision = artifact_revision_record().encode().unwrap();
        let revision_state_offset = revision.len() - (1 + 8 + 8 + 1 + 8);
        revision[revision_state_offset] = 0xff;
        assert_eq!(
            ArtifactRevisionRecord::decode(&revision),
            Err(PublicationRecordCodecError::UnknownDiscriminant {
                type_name: "RevisionState",
                value: 0xff,
            })
        );

        let mut candidate = gc_candidate_record().encode().unwrap();
        candidate[1 + 8] = 0xff;
        assert_eq!(
            GcCandidateRecord::decode(&candidate),
            Err(PublicationRecordCodecError::UnknownDiscriminant {
                type_name: "GcClaimState",
                value: 0xff,
            })
        );
    }

    #[test]
    fn optional_tags_and_nonzero_scalars_fail_closed() {
        let mut workspace = WorkspaceRecord {
            owning_operation_id: None,
            ..workspace_record()
        }
        .encode()
        .unwrap();
        *workspace.last_mut().unwrap() = 2;
        assert_eq!(
            WorkspaceRecord::decode(&workspace),
            Err(PublicationRecordCodecError::InvalidOptionalTag {
                field: "owning_operation_id",
                value: 2,
            })
        );

        let mut path = path_entry().encode().unwrap();
        path[1..9].fill(0);
        assert_eq!(
            PathEntry::decode(&path),
            Err(PublicationRecordCodecError::ZeroScalar {
                field: "generation",
            })
        );

        let mut revision = ArtifactRevisionRecord {
            last_zero_ref_version: None,
            ..artifact_revision_record()
        }
        .encode()
        .unwrap();
        *revision.last_mut().unwrap() = 2;
        assert_eq!(
            ArtifactRevisionRecord::decode(&revision),
            Err(PublicationRecordCodecError::InvalidOptionalTag {
                field: "last_zero_ref_version",
                value: 2,
            })
        );

        let mut candidate = gc_candidate_record().encode().unwrap();
        candidate[1..9].fill(0);
        assert_eq!(
            GcCandidateRecord::decode(&candidate),
            Err(PublicationRecordCodecError::ZeroScalar {
                field: "last_zero_ref_version",
            })
        );
    }

    #[test]
    fn exact_string_and_byte_limits_roundtrip() {
        let path = PathEntry {
            body_digest_uri: "d".repeat(MAX_DIGEST_URI_BYTES),
            content_type: "c".repeat(MAX_CONTENT_TYPE_BYTES),
            producer: Some("p".repeat(MAX_PRODUCER_BYTES)),
            manifest_id: Some("m".repeat(MAX_MANIFEST_ID_BYTES)),
            typed_index_projection: vec![0xaa; MAX_INDEX_PROJECTION_BYTES],
            ..path_entry()
        };
        let path_encoded = path.encode().unwrap();
        assert_eq!(PathEntry::decode(&path_encoded).unwrap(), path);

        let candidate = GcCandidateRecord {
            quarantine_evidence: Some(vec![0xbb; MAX_QUARANTINE_EVIDENCE_BYTES]),
            ..gc_candidate_record()
        };
        let candidate_encoded = candidate.encode().unwrap();
        assert_eq!(
            GcCandidateRecord::decode(&candidate_encoded).unwrap(),
            candidate
        );
    }

    #[test]
    fn oversized_strings_and_bytes_are_rejected() {
        let cases = [
            (
                PathEntry {
                    body_digest_uri: "d".repeat(MAX_DIGEST_URI_BYTES + 1),
                    ..path_entry()
                }
                .encode(),
                "body_digest_uri",
                MAX_DIGEST_URI_BYTES + 1,
                MAX_DIGEST_URI_BYTES,
            ),
            (
                PathEntry {
                    manifest_digest_uri: "d".repeat(MAX_DIGEST_URI_BYTES + 1),
                    ..path_entry()
                }
                .encode(),
                "manifest_digest_uri",
                MAX_DIGEST_URI_BYTES + 1,
                MAX_DIGEST_URI_BYTES,
            ),
            (
                PathEntry {
                    content_type: "c".repeat(MAX_CONTENT_TYPE_BYTES + 1),
                    ..path_entry()
                }
                .encode(),
                "content_type",
                MAX_CONTENT_TYPE_BYTES + 1,
                MAX_CONTENT_TYPE_BYTES,
            ),
            (
                PathEntry {
                    producer: Some("p".repeat(MAX_PRODUCER_BYTES + 1)),
                    ..path_entry()
                }
                .encode(),
                "producer",
                MAX_PRODUCER_BYTES + 1,
                MAX_PRODUCER_BYTES,
            ),
            (
                PathEntry {
                    manifest_id: Some("m".repeat(MAX_MANIFEST_ID_BYTES + 1)),
                    ..path_entry()
                }
                .encode(),
                "manifest_id",
                MAX_MANIFEST_ID_BYTES + 1,
                MAX_MANIFEST_ID_BYTES,
            ),
            (
                PathEntry {
                    typed_index_projection: vec![0; MAX_INDEX_PROJECTION_BYTES + 1],
                    ..path_entry()
                }
                .encode(),
                "typed_index_projection",
                MAX_INDEX_PROJECTION_BYTES + 1,
                MAX_INDEX_PROJECTION_BYTES,
            ),
        ];

        for (actual, field, length, max) in cases {
            assert_eq!(
                actual,
                Err(PublicationRecordCodecError::LengthLimit { field, length, max })
            );
        }

        let candidate = GcCandidateRecord {
            quarantine_evidence: Some(vec![0; MAX_QUARANTINE_EVIDENCE_BYTES + 1]),
            ..gc_candidate_record()
        };
        assert_eq!(
            candidate.encode(),
            Err(PublicationRecordCodecError::LengthLimit {
                field: "quarantine_evidence",
                length: MAX_QUARANTINE_EVIDENCE_BYTES + 1,
                max: MAX_QUARANTINE_EVIDENCE_BYTES,
            })
        );
    }

    #[test]
    fn decode_rejects_oversized_declared_length_before_allocation() {
        let mut encoded = path_entry().encode().unwrap();
        let body_length_offset = 1 + 8 + FIXED_ID_BYTES;
        encoded[body_length_offset..body_length_offset + 4]
            .copy_from_slice(&((MAX_DIGEST_URI_BYTES + 1) as u32).to_be_bytes());
        assert_eq!(
            PathEntry::decode(&encoded),
            Err(PublicationRecordCodecError::LengthLimit {
                field: "body_digest_uri",
                length: MAX_DIGEST_URI_BYTES + 1,
                max: MAX_DIGEST_URI_BYTES,
            })
        );
    }

    #[test]
    fn invalid_strings_fail_closed() {
        let mut invalid_utf8 = path_entry().encode().unwrap();
        let body_start = 1 + 8 + FIXED_ID_BYTES + 4;
        invalid_utf8[body_start] = 0xff;
        assert_eq!(
            PathEntry::decode(&invalid_utf8),
            Err(PublicationRecordCodecError::InvalidUtf8 {
                field: "body_digest_uri",
            })
        );

        let mut nul = path_entry();
        nul.content_type = "text/\0plain".to_owned();
        assert_eq!(
            nul.encode(),
            Err(PublicationRecordCodecError::ContainsNul {
                field: "content_type",
                index: 5,
            })
        );

        let mut empty_optional = path_entry();
        empty_optional.producer = Some(String::new());
        assert_eq!(
            empty_optional.encode(),
            Err(PublicationRecordCodecError::EmptyString { field: "producer" })
        );
    }

    #[test]
    fn dependency_bounds_are_enforced_on_encode_and_decode() {
        let over_count = ArtifactRevisionRecord {
            dependency_count: MAX_DEPENDENCY_COUNT + 1,
            ..artifact_revision_record()
        };
        assert_eq!(
            over_count.encode(),
            Err(PublicationRecordCodecError::DependencyCountLimit {
                count: MAX_DEPENDENCY_COUNT + 1,
                max: MAX_DEPENDENCY_COUNT,
            })
        );

        let over_depth = ArtifactRevisionRecord {
            dependency_depth: MAX_DEPENDENCY_DEPTH + 1,
            ..artifact_revision_record()
        };
        assert_eq!(
            over_depth.encode(),
            Err(PublicationRecordCodecError::DependencyDepthLimit {
                depth: MAX_DEPENDENCY_DEPTH + 1,
                max: MAX_DEPENDENCY_DEPTH,
            })
        );

        let mut encoded = artifact_revision_record().encode().unwrap();
        let dependency_count_offset =
            1 + 8 + 4 + "sha256:body".len() + 4 + "sha256:manifest".len() + 8;
        encoded[dependency_count_offset..dependency_count_offset + 4]
            .copy_from_slice(&(MAX_DEPENDENCY_COUNT + 1).to_be_bytes());
        assert_eq!(
            ArtifactRevisionRecord::decode(&encoded),
            Err(PublicationRecordCodecError::DependencyCountLimit {
                count: MAX_DEPENDENCY_COUNT + 1,
                max: MAX_DEPENDENCY_COUNT,
            })
        );
    }
}
