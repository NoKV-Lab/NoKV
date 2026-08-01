/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable payloads for immutable commits and their exact consumers.
//!
//! The key owns every identity field that selects a row. These payloads keep
//! only the sealed closure and mutable lifetime state. Holt's [`CurrentValue`]
//! envelope owns created and modified versions.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommitId, CommitState, CommitVersion, ConsumerEpoch, Generation,
    NormalizedRelativePath, WorkspaceIncarnationId, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::publication_records::{
    MAX_CONTENT_TYPE_BYTES, MAX_DEPENDENCY_COUNT, MAX_DEPENDENCY_DEPTH, MAX_MANIFEST_ID_BYTES,
    MAX_PRODUCER_BYTES,
};

/// Only supported value format for commit-owned payloads.
pub const COMMIT_VALUE_FORMAT_VERSION: u8 = 2;

/// A commit can name at most this many direct parent commits.
pub const MAX_PARENT_COMMITS: u32 = 64;
/// Maximum digest URI retained in a commit or member.
pub const MAX_COMMIT_DIGEST_URI_BYTES: usize = 256;
/// Maximum producer projection retained in a commit.
pub const MAX_COMMIT_PRODUCER_BYTES: usize = 512;
/// Maximum typed lineage projection retained in a commit.
pub const MAX_COMMIT_LINEAGE_BYTES: usize = 64 * 1024;
/// Maximum typed member projection retained beside one path.
pub const MAX_COMMIT_MEMBER_PROJECTION_BYTES: usize = 64 * 1024;

/// Immutable commit closure plus its exact mutable consumer lifetime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecord {
    pub source_workspace_incarnation_id: WorkspaceIncarnationId,
    /// Caller-supplied stable Workbench content digest.
    pub content_digest_uri: String,
    /// Canonical Workbench run-manifest digest.
    pub manifest_digest_uri: String,
    /// Server-derived immutable revision containing the canonical tree manifest.
    pub tree_manifest_revision_id: ArtifactRevisionId,
    /// `sha256:` URI of the canonical frozen `CommitMember` rolling digest.
    pub tree_digest_uri: String,
    pub member_count: u64,
    pub member_digest: [u8; SHA256_BYTES],
    pub unique_revision_count: u64,
    pub revision_digest: [u8; SHA256_BYTES],
    /// Strictly increasing root-global parent identities.
    pub parent_commits: Vec<CommitId>,
    pub parent_digest: [u8; SHA256_BYTES],
    pub producer: Option<String>,
    /// Canonical typed lineage projection.
    pub lineage_projection: Vec<u8>,
    pub consumer_count: u64,
    pub consumer_epoch: ConsumerEpoch,
    pub last_zero_consumer_version: Option<CommitVersion>,
    pub state: CommitState,
}

/// One canonical path member in an immutable commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitMemberRecord {
    pub artifact_revision_id: ArtifactRevisionId,
    pub path_generation: Generation,
    pub body_digest_uri: String,
    pub manifest_digest_uri: String,
    pub logical_size: u64,
    pub dependency_count: u32,
    pub dependency_depth: u8,
    pub content_type: String,
    pub producer: Option<String>,
    pub manifest_id: Option<String>,
    pub typed_projection: Vec<u8>,
}

/// Current immutable commit selected as one workspace's head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkbenchCommitHeadRecord {
    pub commit_id: CommitId,
    pub head_generation: Generation,
}

/// Current immutable commit selected by one durable workspace tag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TagRecord {
    pub commit_id: CommitId,
    pub tag_generation: Generation,
}

/// Exact owner row for one commit consumer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitConsumerRecord {
    pub consumer_epoch_at_add: ConsumerEpoch,
}

/// Strict commit payload encode, decode, or invariant failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitRecordError {
    UnsupportedValueVersion {
        actual: u8,
        expected: u8,
    },
    UnknownDiscriminant {
        type_name: &'static str,
        value: u8,
    },
    InvalidOptionalTag {
        field: &'static str,
        value: u8,
    },
    ZeroScalar {
        field: &'static str,
    },
    EmptyField {
        field: &'static str,
    },
    ContainsNul {
        field: &'static str,
        index: usize,
    },
    InvalidUtf8 {
        field: &'static str,
    },
    FieldTooLong {
        field: &'static str,
        length: usize,
        max: usize,
    },
    CountOutOfRange {
        field: &'static str,
        value: u64,
        max: u64,
    },
    ParentsNotCanonical,
    InvalidClosureSeal {
        closure: &'static str,
        reason: &'static str,
    },
    InvalidConsumerLifetime {
        reason: &'static str,
    },
    Truncated {
        field: &'static str,
        needed: usize,
        remaining: usize,
    },
    TrailingBytes {
        count: usize,
    },
}

impl fmt::Display for CommitRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported commit value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::EmptyField { field } => write!(formatter, "{field} must not be empty"),
            Self::ContainsNul { field, index } => {
                write!(formatter, "{field} contains NUL at byte offset {index}")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::FieldTooLong { field, length, max } => {
                write!(formatter, "{field} is {length} bytes, maximum is {max}")
            }
            Self::CountOutOfRange { field, value, max } => {
                write!(formatter, "{field} {value} exceeds maximum {max}")
            }
            Self::ParentsNotCanonical => {
                formatter.write_str("parent commits must be strictly increasing and unique")
            }
            Self::InvalidClosureSeal { closure, reason } => {
                write!(formatter, "invalid {closure} closure seal: {reason}")
            }
            Self::InvalidConsumerLifetime { reason } => {
                write!(formatter, "invalid commit consumer lifetime: {reason}")
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
                write!(formatter, "commit value has {count} trailing bytes")
            }
        }
    }
}

impl std::error::Error for CommitRecordError {}

impl CommitRecord {
    pub fn validate(&self) -> Result<(), CommitRecordError> {
        validate_required_string(
            "content_digest_uri",
            &self.content_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        validate_required_string(
            "manifest_digest_uri",
            &self.manifest_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        validate_required_string(
            "tree_digest_uri",
            &self.tree_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        validate_optional_string(
            "producer",
            self.producer.as_deref(),
            MAX_COMMIT_PRODUCER_BYTES,
        )?;
        validate_length(
            "lineage_projection",
            self.lineage_projection.len(),
            MAX_COMMIT_LINEAGE_BYTES,
        )?;
        validate_parent_commits(&self.parent_commits)?;
        validate_closure_seals(self)?;
        validate_consumer_lifetime(
            self.consumer_count,
            self.consumer_epoch,
            self.last_zero_consumer_version,
            self.state,
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>, CommitRecordError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.push(COMMIT_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.source_workspace_incarnation_id.as_bytes());
        put_bounded_string(
            &mut encoded,
            "content_digest_uri",
            &self.content_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        put_bounded_string(
            &mut encoded,
            "manifest_digest_uri",
            &self.manifest_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        encoded.extend_from_slice(self.tree_manifest_revision_id.as_bytes());
        put_bounded_string(
            &mut encoded,
            "tree_digest_uri",
            &self.tree_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        encoded.extend_from_slice(&self.member_count.to_be_bytes());
        encoded.extend_from_slice(&self.member_digest);
        encoded.extend_from_slice(&self.unique_revision_count.to_be_bytes());
        encoded.extend_from_slice(&self.revision_digest);
        encoded.extend_from_slice(
            &u32::try_from(self.parent_commits.len())
                .expect("validated parent count fits u32")
                .to_be_bytes(),
        );
        for parent in &self.parent_commits {
            encoded.extend_from_slice(parent.as_bytes());
        }
        encoded.extend_from_slice(&self.parent_digest);
        put_optional_string(
            &mut encoded,
            "producer",
            self.producer.as_deref(),
            MAX_COMMIT_PRODUCER_BYTES,
        )?;
        put_bounded_bytes(
            &mut encoded,
            "lineage_projection",
            &self.lineage_projection,
            MAX_COMMIT_LINEAGE_BYTES,
        )?;
        encoded.extend_from_slice(&self.consumer_count.to_be_bytes());
        encoded.extend_from_slice(&self.consumer_epoch.get().to_be_bytes());
        put_optional_commit_version(&mut encoded, self.last_zero_consumer_version);
        encoded.push(self.state.into());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let source_workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes(decoder.fixed("source_workspace_incarnation_id")?);
        let content_digest_uri =
            decoder.required_string("content_digest_uri", MAX_COMMIT_DIGEST_URI_BYTES)?;
        let manifest_digest_uri =
            decoder.required_string("manifest_digest_uri", MAX_COMMIT_DIGEST_URI_BYTES)?;
        let tree_manifest_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("tree_manifest_revision_id")?);
        let tree_digest_uri =
            decoder.required_string("tree_digest_uri", MAX_COMMIT_DIGEST_URI_BYTES)?;
        let member_count = decoder.u64("member_count")?;
        let member_digest = decoder.fixed("member_digest")?;
        let unique_revision_count = decoder.u64("unique_revision_count")?;
        let revision_digest = decoder.fixed("revision_digest")?;
        let parent_count = decoder.u32("parent_count")?;
        if parent_count > MAX_PARENT_COMMITS {
            return Err(CommitRecordError::CountOutOfRange {
                field: "parent_count",
                value: u64::from(parent_count),
                max: u64::from(MAX_PARENT_COMMITS),
            });
        }
        let mut parent_commits = Vec::with_capacity(parent_count as usize);
        for _ in 0..parent_count {
            parent_commits.push(CommitId::from_bytes(decoder.fixed("parent_commit_id")?));
        }
        let parent_digest = decoder.fixed("parent_digest")?;
        let producer = decoder.optional_string("producer", MAX_COMMIT_PRODUCER_BYTES)?;
        let lineage_projection =
            decoder.bounded_bytes("lineage_projection", MAX_COMMIT_LINEAGE_BYTES)?;
        let consumer_count = decoder.u64("consumer_count")?;
        let consumer_epoch = ConsumerEpoch::new(decoder.u64("consumer_epoch")?);
        let last_zero_consumer_version =
            decoder.optional_commit_version("last_zero_consumer_version")?;
        let state = decode_durable_enum(decoder.u8("state")?)?;
        decoder.finish()?;

        let record = Self {
            source_workspace_incarnation_id,
            content_digest_uri,
            manifest_digest_uri,
            tree_manifest_revision_id,
            tree_digest_uri,
            member_count,
            member_digest,
            unique_revision_count,
            revision_digest,
            parent_commits,
            parent_digest,
            producer,
            lineage_projection,
            consumer_count,
            consumer_epoch,
            last_zero_consumer_version,
            state,
        };
        record.validate()?;
        Ok(record)
    }
}

fn validate_closure_seals(record: &CommitRecord) -> Result<(), CommitRecordError> {
    if (record.member_count == 0) != (record.member_digest == [0; SHA256_BYTES]) {
        return Err(CommitRecordError::InvalidClosureSeal {
            closure: "member",
            reason: "the zero count and zero rolling digest must appear together",
        });
    }
    if record.unique_revision_count == 0 {
        return Err(CommitRecordError::InvalidClosureSeal {
            closure: "revision",
            reason: "the tree-manifest revision must be retained",
        });
    }
    if record.revision_digest == [0; SHA256_BYTES] {
        return Err(CommitRecordError::InvalidClosureSeal {
            closure: "revision",
            reason: "a non-empty closure cannot retain the initial digest",
        });
    }
    if record.parent_commits.is_empty() != (record.parent_digest == [0; SHA256_BYTES]) {
        return Err(CommitRecordError::InvalidClosureSeal {
            closure: "parent",
            reason: "the empty parent set and zero rolling digest must appear together",
        });
    }
    Ok(())
}

impl CommitMemberRecord {
    pub fn validate(&self) -> Result<(), CommitRecordError> {
        validate_required_string(
            "body_digest_uri",
            &self.body_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        validate_required_string(
            "manifest_digest_uri",
            &self.manifest_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        if self.dependency_count > MAX_DEPENDENCY_COUNT {
            return Err(CommitRecordError::CountOutOfRange {
                field: "dependency_count",
                value: u64::from(self.dependency_count),
                max: u64::from(MAX_DEPENDENCY_COUNT),
            });
        }
        if self.dependency_depth > MAX_DEPENDENCY_DEPTH {
            return Err(CommitRecordError::CountOutOfRange {
                field: "dependency_depth",
                value: u64::from(self.dependency_depth),
                max: u64::from(MAX_DEPENDENCY_DEPTH),
            });
        }
        if (self.dependency_count == 0) != (self.dependency_depth == 0) {
            return Err(CommitRecordError::InvalidClosureSeal {
                closure: "artifact dependency",
                reason: "count and depth must either both be zero or both be non-zero",
            });
        }
        validate_required_string("content_type", &self.content_type, MAX_CONTENT_TYPE_BYTES)?;
        validate_optional_string("producer", self.producer.as_deref(), MAX_PRODUCER_BYTES)?;
        validate_optional_string(
            "manifest_id",
            self.manifest_id.as_deref(),
            MAX_MANIFEST_ID_BYTES,
        )?;
        validate_length(
            "typed_projection",
            self.typed_projection.len(),
            MAX_COMMIT_MEMBER_PROJECTION_BYTES,
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>, CommitRecordError> {
        self.validate()?;
        let mut encoded = Vec::new();
        encoded.push(COMMIT_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.artifact_revision_id.as_bytes());
        encoded.extend_from_slice(&self.path_generation.get().to_be_bytes());
        put_bounded_string(
            &mut encoded,
            "body_digest_uri",
            &self.body_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        put_bounded_string(
            &mut encoded,
            "manifest_digest_uri",
            &self.manifest_digest_uri,
            MAX_COMMIT_DIGEST_URI_BYTES,
        )?;
        encoded.extend_from_slice(&self.logical_size.to_be_bytes());
        encoded.extend_from_slice(&self.dependency_count.to_be_bytes());
        encoded.push(self.dependency_depth);
        put_bounded_string(
            &mut encoded,
            "content_type",
            &self.content_type,
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
            "typed_projection",
            &self.typed_projection,
            MAX_COMMIT_MEMBER_PROJECTION_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let artifact_revision_id =
            ArtifactRevisionId::from_bytes(decoder.fixed("artifact_revision_id")?);
        let path_generation = decoder.generation("path_generation")?;
        let body_digest_uri =
            decoder.required_string("body_digest_uri", MAX_COMMIT_DIGEST_URI_BYTES)?;
        let manifest_digest_uri =
            decoder.required_string("manifest_digest_uri", MAX_COMMIT_DIGEST_URI_BYTES)?;
        let logical_size = decoder.u64("logical_size")?;
        let dependency_count = decoder.u32("dependency_count")?;
        let dependency_depth = decoder.u8("dependency_depth")?;
        let content_type = decoder.required_string("content_type", MAX_CONTENT_TYPE_BYTES)?;
        let producer = decoder.optional_string("producer", MAX_PRODUCER_BYTES)?;
        let manifest_id = decoder.optional_string("manifest_id", MAX_MANIFEST_ID_BYTES)?;
        let typed_projection =
            decoder.bounded_bytes("typed_projection", MAX_COMMIT_MEMBER_PROJECTION_BYTES)?;
        decoder.finish()?;
        let record = Self {
            artifact_revision_id,
            path_generation,
            body_digest_uri,
            manifest_digest_uri,
            logical_size,
            dependency_count,
            dependency_depth,
            content_type,
            producer,
            manifest_id,
            typed_projection,
        };
        record.validate()?;
        Ok(record)
    }
}

/// Digest one immutable member row together with its canonical full path.
pub fn commit_member_row_digest(
    path: &NormalizedRelativePath,
    member: &CommitMemberRecord,
) -> Result<[u8; SHA256_BYTES], CommitRecordError> {
    let encoded = member.encode()?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.commit.member-row.v2\0");
    hash_digest_bytes(&mut hasher, path.as_str().as_bytes());
    hash_digest_bytes(&mut hasher, &encoded);
    Ok(hasher.finalize().into())
}

/// Advance the ordered commit-member seal.
///
/// `sequence` is zero based and must equal the number of rows already folded
/// into `previous`. Callers persist that count beside the rolling digest.
pub fn advance_commit_member_rolling_digest(
    previous: [u8; SHA256_BYTES],
    sequence: u64,
    row_digest: [u8; SHA256_BYTES],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.commit.members.v1\0");
    hasher.update(previous);
    hasher.update(sequence.to_be_bytes());
    hasher.update(row_digest);
    hasher.finalize().into()
}

fn hash_digest_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u32).to_be_bytes());
    hasher.update(bytes);
}

impl WorkbenchCommitHeadRecord {
    pub fn encode(&self) -> Vec<u8> {
        encode_commit_pointer(self.commit_id, self.head_generation)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitRecordError> {
        let (commit_id, head_generation) = decode_commit_pointer(encoded, "head_generation")?;
        Ok(Self {
            commit_id,
            head_generation,
        })
    }
}

impl TagRecord {
    pub fn encode(&self) -> Vec<u8> {
        encode_commit_pointer(self.commit_id, self.tag_generation)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitRecordError> {
        let (commit_id, tag_generation) = decode_commit_pointer(encoded, "tag_generation")?;
        Ok(Self {
            commit_id,
            tag_generation,
        })
    }
}

impl CommitConsumerRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + 8);
        encoded.push(COMMIT_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.consumer_epoch_at_add.get().to_be_bytes());
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CommitRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_value_version()?;
        let consumer_epoch_at_add = ConsumerEpoch::new(decoder.u64("consumer_epoch_at_add")?);
        decoder.finish()?;
        Ok(Self {
            consumer_epoch_at_add,
        })
    }
}

fn encode_commit_pointer(commit_id: CommitId, generation: Generation) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(1 + CommitId::BYTE_WIDTH + 8);
    encoded.push(COMMIT_VALUE_FORMAT_VERSION);
    encoded.extend_from_slice(commit_id.as_bytes());
    encoded.extend_from_slice(&generation.get().to_be_bytes());
    encoded
}

fn decode_commit_pointer(
    encoded: &[u8],
    generation_field: &'static str,
) -> Result<(CommitId, Generation), CommitRecordError> {
    let mut decoder = Decoder::new(encoded);
    decoder.require_value_version()?;
    let commit_id = CommitId::from_bytes(decoder.fixed("commit_id")?);
    let generation = decoder.generation(generation_field)?;
    decoder.finish()?;
    Ok((commit_id, generation))
}

fn validate_parent_commits(parents: &[CommitId]) -> Result<(), CommitRecordError> {
    if parents.len() > MAX_PARENT_COMMITS as usize {
        return Err(CommitRecordError::CountOutOfRange {
            field: "parent_count",
            value: parents.len() as u64,
            max: u64::from(MAX_PARENT_COMMITS),
        });
    }
    if parents.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CommitRecordError::ParentsNotCanonical);
    }
    Ok(())
}

fn validate_consumer_lifetime(
    consumer_count: u64,
    consumer_epoch: ConsumerEpoch,
    last_zero_consumer_version: Option<CommitVersion>,
    state: CommitState,
) -> Result<(), CommitRecordError> {
    if consumer_count == 0 && last_zero_consumer_version.is_none() {
        return Err(CommitRecordError::InvalidConsumerLifetime {
            reason: "zero consumers require the exact last-zero version",
        });
    }
    if consumer_count > 0 && last_zero_consumer_version.is_some() {
        return Err(CommitRecordError::InvalidConsumerLifetime {
            reason: "a live consumer set cannot retain a last-zero version",
        });
    }
    if consumer_count > 0 && consumer_epoch == ConsumerEpoch::ZERO {
        return Err(CommitRecordError::InvalidConsumerLifetime {
            reason: "live consumers require a non-zero epoch",
        });
    }
    if state != CommitState::Sealed && consumer_count != 0 {
        return Err(CommitRecordError::InvalidConsumerLifetime {
            reason: "retiring or retired commits cannot have consumers",
        });
    }
    Ok(())
}

fn decode_durable_enum<T>(value: u8) -> Result<T, CommitRecordError>
where
    T: TryFrom<u8, Error = nokv_types::UnknownDurableDiscriminant>,
{
    T::try_from(value).map_err(|error| CommitRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

fn validate_required_string(
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), CommitRecordError> {
    if value.is_empty() {
        return Err(CommitRecordError::EmptyField { field });
    }
    validate_length(field, value.len(), max)?;
    if let Some(index) = value.as_bytes().iter().position(|byte| *byte == 0) {
        return Err(CommitRecordError::ContainsNul { field, index });
    }
    Ok(())
}

fn validate_optional_string(
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> Result<(), CommitRecordError> {
    match value {
        Some(value) => validate_required_string(field, value, max),
        None => Ok(()),
    }
}

fn validate_length(
    field: &'static str,
    length: usize,
    max: usize,
) -> Result<(), CommitRecordError> {
    if length > max {
        Err(CommitRecordError::FieldTooLong { field, length, max })
    } else {
        Ok(())
    }
}

fn put_bounded_string(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &str,
    max: usize,
) -> Result<(), CommitRecordError> {
    validate_required_string(field, value, max)?;
    put_bounded_bytes(encoded, field, value.as_bytes(), max)
}

fn put_bounded_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
    max: usize,
) -> Result<(), CommitRecordError> {
    validate_length(field, value.len(), max)?;
    encoded.extend_from_slice(
        &u32::try_from(value.len())
            .expect("validated commit byte length fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(value);
    Ok(())
}

fn put_optional_string(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> Result<(), CommitRecordError> {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            put_bounded_string(encoded, field, value, max)?;
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
    input: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn require_value_version(&mut self) -> Result<(), CommitRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == COMMIT_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(CommitRecordError::UnsupportedValueVersion {
                actual,
                expected: COMMIT_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn fixed<const N: usize>(&mut self, field: &'static str) -> Result<[u8; N], CommitRecordError> {
        let bytes = self.take(field, N)?;
        let mut value = [0; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, CommitRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, CommitRecordError> {
        Ok(u32::from_be_bytes(self.fixed(field)?))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, CommitRecordError> {
        Ok(u64::from_be_bytes(self.fixed(field)?))
    }

    fn generation(&mut self, field: &'static str) -> Result<Generation, CommitRecordError> {
        Generation::new(self.u64(field)?).map_err(|_| CommitRecordError::ZeroScalar { field })
    }

    fn required_string(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<String, CommitRecordError> {
        let bytes = self.bounded_bytes(field, max)?;
        let value = std::str::from_utf8(&bytes)
            .map_err(|_| CommitRecordError::InvalidUtf8 { field })?
            .to_owned();
        validate_required_string(field, &value, max)?;
        Ok(value)
    }

    fn optional_string(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Option<String>, CommitRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => self.required_string(field, max).map(Some),
            value => Err(CommitRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn bounded_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Vec<u8>, CommitRecordError> {
        let length = self.u32(field)? as usize;
        validate_length(field, length, max)?;
        Ok(self.take(field, length)?.to_vec())
    }

    fn optional_commit_version(
        &mut self,
        field: &'static str,
    ) -> Result<Option<CommitVersion>, CommitRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => CommitVersion::new(self.u64(field)?)
                .map(Some)
                .map_err(|_| CommitRecordError::ZeroScalar { field }),
            value => Err(CommitRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn take(&mut self, field: &'static str, length: usize) -> Result<&'a [u8], CommitRecordError> {
        let remaining = self.input.len().saturating_sub(self.offset);
        if remaining < length {
            return Err(CommitRecordError::Truncated {
                field,
                needed: length,
                remaining,
            });
        }
        let start = self.offset;
        self.offset += length;
        Ok(&self.input[start..self.offset])
    }

    fn finish(self) -> Result<(), CommitRecordError> {
        let count = self.input.len().saturating_sub(self.offset);
        if count == 0 {
            Ok(())
        } else {
            Err(CommitRecordError::TrailingBytes { count })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit_id(byte: u8) -> CommitId {
        CommitId::from_bytes([byte; SHA256_BYTES])
    }

    fn commit_record() -> CommitRecord {
        CommitRecord {
            source_workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([1; 16]),
            content_digest_uri: format!("sha256:{}", "11".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            tree_manifest_revision_id: ArtifactRevisionId::from_bytes([3; 16]),
            tree_digest_uri: format!("sha256:{}", "44".repeat(32)),
            member_count: 7,
            member_digest: [5; SHA256_BYTES],
            unique_revision_count: 3,
            revision_digest: [6; SHA256_BYTES],
            parent_commits: vec![commit_id(7), commit_id(8)],
            parent_digest: [9; SHA256_BYTES],
            producer: Some("agent-runtime".to_owned()),
            lineage_projection: vec![10, 11, 12],
            consumer_count: 2,
            consumer_epoch: ConsumerEpoch::new(4),
            last_zero_consumer_version: None,
            state: CommitState::Sealed,
        }
    }

    #[test]
    fn commit_round_trip_and_strict_envelope() {
        let record = commit_record();
        let encoded = record.encode().unwrap();
        assert_eq!(CommitRecord::decode(&encoded).unwrap(), record);
        for length in 0..encoded.len() {
            assert!(matches!(
                CommitRecord::decode(&encoded[..length]),
                Err(CommitRecordError::Truncated { .. })
                    | Err(CommitRecordError::UnsupportedValueVersion { .. })
                    | Err(CommitRecordError::FieldTooLong { .. })
                    | Err(CommitRecordError::EmptyField { .. })
            ));
        }
        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            CommitRecord::decode(&trailing),
            Err(CommitRecordError::TrailingBytes { count: 1 })
        );
    }

    #[test]
    fn commit_rejects_noncanonical_parents_and_invalid_lifetime() {
        let mut record = commit_record();
        record.parent_commits = vec![commit_id(8), commit_id(7)];
        assert_eq!(record.encode(), Err(CommitRecordError::ParentsNotCanonical));

        let mut record = commit_record();
        record.consumer_count = 0;
        assert_eq!(
            record.encode(),
            Err(CommitRecordError::InvalidConsumerLifetime {
                reason: "zero consumers require the exact last-zero version",
            })
        );

        let mut record = commit_record();
        record.state = CommitState::Retiring;
        assert_eq!(
            record.encode(),
            Err(CommitRecordError::InvalidConsumerLifetime {
                reason: "retiring or retired commits cannot have consumers",
            })
        );
    }

    #[test]
    fn commit_member_round_trip() {
        let record = CommitMemberRecord {
            artifact_revision_id: ArtifactRevisionId::from_bytes([13; 16]),
            path_generation: Generation::new(17).unwrap(),
            body_digest_uri: format!("sha256:{}", "ab".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "cd".repeat(32)),
            logical_size: 4096,
            dependency_count: 2,
            dependency_depth: 2,
            content_type: "application/octet-stream".to_owned(),
            producer: Some("agent".to_owned()),
            manifest_id: Some("manifest-17".to_owned()),
            typed_projection: vec![1, 2, 3],
        };
        let encoded = record.encode().unwrap();
        assert_eq!(CommitMemberRecord::decode(&encoded).unwrap(), record);
        let mut previous_layout = encoded;
        previous_layout[0] = COMMIT_VALUE_FORMAT_VERSION - 1;
        assert_eq!(
            CommitMemberRecord::decode(&previous_layout),
            Err(CommitRecordError::UnsupportedValueVersion {
                actual: COMMIT_VALUE_FORMAT_VERSION - 1,
                expected: COMMIT_VALUE_FORMAT_VERSION,
            })
        );

        let path = NormalizedRelativePath::new("outputs/result.bin").unwrap();
        let row_digest = commit_member_row_digest(&path, &record).unwrap();
        assert_ne!(row_digest, [0; SHA256_BYTES]);
        assert_eq!(
            advance_commit_member_rolling_digest([0; SHA256_BYTES], 0, row_digest),
            advance_commit_member_rolling_digest([0; SHA256_BYTES], 0, row_digest)
        );
    }

    #[test]
    fn pointer_and_consumer_golden_bytes() {
        let head = WorkbenchCommitHeadRecord {
            commit_id: commit_id(0x11),
            head_generation: Generation::new(0x0102_0304_0506_0708).unwrap(),
        };
        let mut expected = vec![COMMIT_VALUE_FORMAT_VERSION];
        expected.extend_from_slice(&[0x11; SHA256_BYTES]);
        expected.extend_from_slice(&0x0102_0304_0506_0708_u64.to_be_bytes());
        assert_eq!(head.encode(), expected);
        assert_eq!(WorkbenchCommitHeadRecord::decode(&expected).unwrap(), head);

        let tag = TagRecord {
            commit_id: commit_id(0x22),
            tag_generation: Generation::new(9).unwrap(),
        };
        assert_eq!(TagRecord::decode(&tag.encode()).unwrap(), tag);

        let consumer = CommitConsumerRecord {
            consumer_epoch_at_add: ConsumerEpoch::new(0x0102_0304_0506_0708),
        };
        assert_eq!(
            consumer.encode(),
            vec![
                COMMIT_VALUE_FORMAT_VERSION,
                0x01,
                0x02,
                0x03,
                0x04,
                0x05,
                0x06,
                0x07,
                0x08,
            ]
        );
        assert_eq!(
            CommitConsumerRecord::decode(&consumer.encode()).unwrap(),
            consumer
        );
    }

    #[test]
    fn unknown_state_and_generation_zero_fail_closed() {
        let mut commit = commit_record().encode().unwrap();
        *commit.last_mut().unwrap() = 0xff;
        assert_eq!(
            CommitRecord::decode(&commit),
            Err(CommitRecordError::UnknownDiscriminant {
                type_name: "CommitState",
                value: 0xff,
            })
        );

        let mut head = WorkbenchCommitHeadRecord {
            commit_id: commit_id(1),
            head_generation: Generation::new(1).unwrap(),
        }
        .encode();
        let generation_start = 1 + CommitId::BYTE_WIDTH;
        head[generation_start..].fill(0);
        assert_eq!(
            WorkbenchCommitHeadRecord::decode(&head),
            Err(CommitRecordError::ZeroScalar {
                field: "head_generation",
            })
        );
    }
}
