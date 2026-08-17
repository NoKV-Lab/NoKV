/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable Generic namespace-index records.

use std::fmt;

use nokv_types::{
    ArtifactRevisionId, CommandDigest, CommitId, CommitVersion, Generation,
    GenericIndexGenerationId, GenericIndexGenerationState, GenericIndexReferenceKind,
    GenericIndexRegistrationPhase, NormalizedRelativePath, OperationId, ReadVersion,
    ReferenceEpoch, WorkspaceIncarnationId, FIXED_ID_BYTES, SHA256_BYTES,
};
use sha2::{Digest, Sha256};

use super::query_records::{
    FiniteFloat, QueryFieldId, QueryScalar, MAX_QUERY_FIELD_ID_BYTES, MAX_QUERY_SCALAR_BYTES,
};

/// Only supported value format for Generic index records.
pub const GENERIC_INDEX_VALUE_FORMAT_VERSION: u8 = 1;
/// Maximum fields declared by one Generic index catalog.
pub const MAX_GENERIC_INDEX_FIELDS: usize = 60;
/// Maximum values retained for one field in one row.
pub const MAX_GENERIC_INDEX_VALUES_PER_FIELD: usize = 1_024;
/// Maximum field groups retained in one row.
pub const MAX_GENERIC_INDEX_ROW_FIELDS: usize = 60;
/// Maximum encoded durable row payload, leaving envelope headroom under the
/// workspace metadata command value limit.
pub const MAX_GENERIC_INDEX_ROW_BYTES: usize = 60 * 1_024;
/// Maximum rows admitted by one append command.
pub const MAX_GENERIC_INDEX_APPEND_ROWS: u32 = 4_096;
/// Maximum durable terminal error text.
pub const MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES: usize = 1_024;

const EMPTY_ROLLING_DIGEST: [u8; SHA256_BYTES] = [0; SHA256_BYTES];
const GENERIC_BUILTIN_FIELD_IDS: &[&str] = &[
    "body.content_type",
    "body.manifest_id",
    "body.producer",
    "kind",
    "name",
    "path",
    "size_bytes",
];

/// Predicate capability advertised by a Generic custom field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum GenericIndexOperator {
    Equal = 1,
    NotEqual = 2,
    In = 3,
    Prefix = 4,
    Suffix = 5,
    Contains = 6,
    Greater = 7,
    GreaterOrEqual = 8,
    Less = 9,
    LessOrEqual = 10,
    Exists = 11,
    NotExists = 12,
}

impl TryFrom<u8> for GenericIndexOperator {
    type Error = GenericIndexRecordError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Equal),
            2 => Ok(Self::NotEqual),
            3 => Ok(Self::In),
            4 => Ok(Self::Prefix),
            5 => Ok(Self::Suffix),
            6 => Ok(Self::Contains),
            7 => Ok(Self::Greater),
            8 => Ok(Self::GreaterOrEqual),
            9 => Ok(Self::Less),
            10 => Ok(Self::LessOrEqual),
            11 => Ok(Self::Exists),
            12 => Ok(Self::NotExists),
            value => Err(GenericIndexRecordError::UnknownDiscriminant {
                type_name: "GenericIndexOperator",
                value,
            }),
        }
    }
}

impl From<GenericIndexOperator> for u8 {
    fn from(value: GenericIndexOperator) -> Self {
        value as u8
    }
}

/// Capability declaration for one custom field.
///
/// No scalar type is declared: the pre-v10 Generic contract permits one field
/// to contain a mixture of strings, unsigned integers, and finite floats.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexFieldCapability {
    pub field: QueryFieldId,
    pub operators: Vec<GenericIndexOperator>,
    pub sortable: bool,
    pub facetable: bool,
}

/// Current pointer from one workspace-relative scope to an immutable index
/// generation. The duplicated seals prevent a pointer from changing the
/// advertised capabilities or row closure without changing generations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexCurrentRecord {
    pub generation_id: GenericIndexGenerationId,
    pub pointer_generation: Generation,
    pub capability_digest: [u8; SHA256_BYTES],
    pub row_count: u64,
    pub row_digest: [u8; SHA256_BYTES],
}

/// Mutable build/lifetime header for one never-reused index generation.
/// A `Retired` header is the permanent identity tombstone and must not be
/// deleted by generation payload GC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexGenerationRecord {
    pub capabilities: Vec<GenericIndexFieldCapability>,
    pub declared_row_count: u64,
    pub appended_row_count: u64,
    pub rolling_row_digest: [u8; SHA256_BYTES],
    pub reference_count: u64,
    pub reference_epoch: ReferenceEpoch,
    pub last_zero_reference_version: Option<CommitVersion>,
    pub state: GenericIndexGenerationState,
}

/// Artifact identity fence for one row that resolves to an exact artifact.
/// Implicit directory rows carry `None` instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenericIndexArtifactBinding {
    pub artifact_revision_id: ArtifactRevisionId,
    pub path_generation: Generation,
}

/// Durable path identity policy for one Generic index row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GenericIndexRowBinding {
    /// The path resolved to a derived or explicit directory at registration.
    Directory,
    /// The path was absent at registration and retains the historical
    /// path-keyed contract for a node that appears later.
    Unbound,
    /// The path resolved to one exact artifact revision and path generation.
    Artifact(GenericIndexArtifactBinding),
}

/// Ordered values for one custom field. Values preserve registration order and
/// may repeat, matching the historical Generic multivalue contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexFieldValues {
    pub field: QueryFieldId,
    pub values: Vec<QueryScalar>,
}

/// One generation-owned row. Paths are relative to the registration root so a
/// sealed generation can be retained by a commit and installed by restore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexRowRecord {
    /// `None` denotes the registration root itself.
    pub relative_path: Option<NormalizedRelativePath>,
    pub binding: GenericIndexRowBinding,
    pub values: Vec<GenericIndexFieldValues>,
}

/// Exact strong-owner row for one generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenericIndexGenerationRefRecord {
    pub kind: GenericIndexReferenceKind,
    pub owner_digest: [u8; SHA256_BYTES],
    pub reference_epoch_at_add: ReferenceEpoch,
}

/// Immutable receipt for one append batch. The receipt is keyed by
/// `first_sequence`, and its resulting seal makes response-loss replay exact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenericIndexAppendReceiptRecord {
    pub first_sequence: u64,
    pub row_count: u32,
    /// Exact metadata commit that installed this batch and receipt.
    pub commit_version: CommitVersion,
    /// Canonical digest of the caller's relative paths and ordered field
    /// values. Artifact bindings are intentionally excluded so the receipt
    /// remains sufficient for exact replay after abort cleanup removes rows.
    pub input_digest: [u8; SHA256_BYTES],
    pub resulting_row_count: u64,
    pub resulting_row_digest: [u8; SHA256_BYTES],
}

/// Recoverable registration operation. Its history hold retains
/// `source_read_version` until every row fence has been copied and sealed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexRegistrationOperationRecord {
    pub workspace_incarnation_id: WorkspaceIncarnationId,
    /// `None` denotes the workspace root.
    pub index_path: Option<NormalizedRelativePath>,
    pub generation_id: GenericIndexGenerationId,
    pub request_digest: CommandDigest,
    pub source_read_version: ReadVersion,
    /// Exact metadata commit that installed the current operation state.
    pub last_transition_version: CommitVersion,
    pub expected_current_generation: Option<Generation>,
    pub capability_digest: [u8; SHA256_BYTES],
    pub declared_row_count: u64,
    pub appended_row_count: u64,
    pub rolling_row_digest: [u8; SHA256_BYTES],
    pub phase: GenericIndexRegistrationPhase,
    pub published_pointer_generation: Option<Generation>,
    pub terminal_error: Option<String>,
}

/// Commit-owned strong reference to one exact Generic index generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitGenericIndexMemberRecord {
    pub generation_id: GenericIndexGenerationId,
    pub capability_digest: [u8; SHA256_BYTES],
    pub row_count: u64,
    pub row_digest: [u8; SHA256_BYTES],
}

/// Strict Generic index encode, decode, or invariant failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GenericIndexRecordError {
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
    InvalidBoolean {
        field: &'static str,
        value: u8,
    },
    ZeroScalar {
        field: &'static str,
    },
    LengthLimit {
        field: &'static str,
        length: usize,
        max: usize,
    },
    CountLimit {
        field: &'static str,
        count: usize,
        max: usize,
    },
    InvalidField {
        field: String,
        reason: String,
    },
    ReservedField {
        field: String,
    },
    CapabilitiesNotCanonical,
    OperatorsNotCanonical {
        field: String,
    },
    RowFieldsNotCanonical,
    EmptyFieldValues {
        field: String,
    },
    UnsupportedScalarType {
        field: String,
        scalar_type: &'static str,
    },
    NonFiniteFloat,
    NonCanonicalFloatZero,
    InvalidUtf8 {
        field: &'static str,
    },
    InvalidPath {
        reason: String,
    },
    InvalidRowBinding {
        reason: &'static str,
    },
    InvalidGenerationProgress,
    InvalidGenerationLifetime {
        reason: &'static str,
    },
    InvalidRegistrationState {
        reason: &'static str,
    },
    RangeOverflow {
        field: &'static str,
    },
    InvalidAppendReceipt,
    InvalidSeal {
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
    NonCanonicalEncoding,
}

impl fmt::Display for GenericIndexRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedValueVersion { actual, expected } => write!(
                formatter,
                "unsupported Generic index value format version {actual}, expected {expected}"
            ),
            Self::UnknownDiscriminant { type_name, value } => {
                write!(formatter, "unknown {type_name} discriminant {value}")
            }
            Self::InvalidOptionalTag { field, value } => {
                write!(formatter, "invalid optional tag {value} for {field}")
            }
            Self::InvalidBoolean { field, value } => {
                write!(formatter, "invalid boolean {value} for {field}")
            }
            Self::ZeroScalar { field } => write!(formatter, "{field} must be non-zero"),
            Self::LengthLimit { field, length, max } => {
                write!(formatter, "{field} length {length} exceeds maximum {max}")
            }
            Self::CountLimit { field, count, max } => {
                write!(formatter, "{field} count {count} exceeds maximum {max}")
            }
            Self::InvalidField { field, reason } => {
                write!(formatter, "invalid Generic index field {field}: {reason}")
            }
            Self::ReservedField { field } => {
                write!(
                    formatter,
                    "Generic index field {field} shadows a built-in field"
                )
            }
            Self::CapabilitiesNotCanonical => formatter
                .write_str("Generic index capabilities must be strictly ordered and unique"),
            Self::OperatorsNotCanonical { field } => write!(
                formatter,
                "Generic index operators for {field} must be strictly ordered and unique"
            ),
            Self::RowFieldsNotCanonical => {
                formatter.write_str("Generic index row fields must be strictly ordered and unique")
            }
            Self::EmptyFieldValues { field } => {
                write!(formatter, "Generic index row field {field} has no values")
            }
            Self::UnsupportedScalarType { field, scalar_type } => write!(
                formatter,
                "Generic index field {field} does not support {scalar_type} values"
            ),
            Self::NonFiniteFloat => formatter.write_str("Generic index float must be finite"),
            Self::NonCanonicalFloatZero => {
                formatter.write_str("Generic index float negative zero is not canonical")
            }
            Self::InvalidUtf8 { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidPath { reason } => {
                write!(formatter, "invalid Generic index path: {reason}")
            }
            Self::InvalidRowBinding { reason } => {
                write!(formatter, "invalid Generic index row binding: {reason}")
            }
            Self::InvalidGenerationProgress => formatter
                .write_str("Generic index appended row count exceeds its declared row count"),
            Self::InvalidGenerationLifetime { reason } => {
                write!(
                    formatter,
                    "invalid Generic index generation lifetime: {reason}"
                )
            }
            Self::InvalidRegistrationState { reason } => {
                write!(
                    formatter,
                    "invalid Generic index registration state: {reason}"
                )
            }
            Self::RangeOverflow { field } => write!(formatter, "{field} range overflows u64"),
            Self::InvalidAppendReceipt => formatter.write_str(
                "Generic index append receipt does not match its contiguous sequence range",
            ),
            Self::InvalidSeal { reason } => {
                write!(formatter, "invalid Generic index seal: {reason}")
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
                write!(formatter, "Generic index value has {count} trailing bytes")
            }
            Self::NonCanonicalEncoding => {
                formatter.write_str("Generic index value is not canonically encoded")
            }
        }
    }
}

impl std::error::Error for GenericIndexRecordError {}

impl GenericIndexCurrentRecord {
    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        validate_row_closure(self.row_count, self.row_digest)?;
        let mut encoded =
            Vec::with_capacity(1 + FIXED_ID_BYTES + 8 + SHA256_BYTES + 8 + SHA256_BYTES);
        encoded.push(GENERIC_INDEX_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.generation_id.as_bytes());
        encoded.extend_from_slice(&self.pointer_generation.get().to_be_bytes());
        encoded.extend_from_slice(&self.capability_digest);
        encoded.extend_from_slice(&self.row_count.to_be_bytes());
        encoded.extend_from_slice(&self.row_digest);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let generation_id = GenericIndexGenerationId::from_bytes(decoder.fixed("generation_id")?);
        let pointer_generation =
            nonzero_generation(decoder.u64("pointer_generation")?, "pointer_generation")?;
        let capability_digest = decoder.fixed("capability_digest")?;
        let row_count = decoder.u64("row_count")?;
        let row_digest = decoder.fixed("row_digest")?;
        decoder.finish()?;
        let record = Self {
            generation_id,
            pointer_generation,
            capability_digest,
            row_count,
            row_digest,
        };
        validate_row_closure(record.row_count, record.row_digest)?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

impl GenericIndexGenerationRecord {
    pub fn validate(&self) -> Result<(), GenericIndexRecordError> {
        validate_capabilities(&self.capabilities)?;
        if self.appended_row_count > self.declared_row_count {
            return Err(GenericIndexRecordError::InvalidGenerationProgress);
        }
        validate_row_closure(self.appended_row_count, self.rolling_row_digest)?;
        if self.reference_epoch == ReferenceEpoch::ZERO {
            return Err(GenericIndexRecordError::InvalidGenerationLifetime {
                reason: "a persisted generation must have a non-zero reference epoch",
            });
        }
        if self.state == GenericIndexGenerationState::Building && self.reference_count != 1 {
            return Err(GenericIndexRecordError::InvalidGenerationLifetime {
                reason: "a building generation must retain exactly one registration reference",
            });
        }
        if matches!(
            self.state,
            GenericIndexGenerationState::Sealed | GenericIndexGenerationState::Retiring
        ) && self.appended_row_count != self.declared_row_count
        {
            return Err(GenericIndexRecordError::InvalidSeal {
                reason: "a sealed or retiring generation must contain every declared row",
            });
        }
        if self.state == GenericIndexGenerationState::Retired && self.reference_count != 0 {
            return Err(GenericIndexRecordError::InvalidGenerationLifetime {
                reason: "a retired generation cannot retain strong references",
            });
        }
        if self.reference_count == 0
            && self.state != GenericIndexGenerationState::Building
            && self.last_zero_reference_version.is_none()
        {
            return Err(GenericIndexRecordError::InvalidGenerationLifetime {
                reason:
                    "a zero-reference non-building generation must record its last-zero version",
            });
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        self.validate()?;
        let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        put_capabilities(&mut encoded, &self.capabilities)?;
        encoded.extend_from_slice(&self.declared_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.appended_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.rolling_row_digest);
        encoded.extend_from_slice(&self.reference_count.to_be_bytes());
        encoded.extend_from_slice(&self.reference_epoch.get().to_be_bytes());
        put_optional_commit_version(&mut encoded, self.last_zero_reference_version);
        encoded.push(self.state.into());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let capabilities = decoder.capabilities()?;
        let declared_row_count = decoder.u64("declared_row_count")?;
        let appended_row_count = decoder.u64("appended_row_count")?;
        let rolling_row_digest = decoder.fixed("rolling_row_digest")?;
        let reference_count = decoder.u64("reference_count")?;
        let reference_epoch = ReferenceEpoch::new(decoder.u64("reference_epoch")?);
        let last_zero_reference_version =
            decoder.optional_commit_version("last_zero_reference_version")?;
        let state = durable_discriminant(
            decoder.u8("generation_state")?,
            GenericIndexGenerationState::try_from,
        )?;
        decoder.finish()?;
        let record = Self {
            capabilities,
            declared_row_count,
            appended_row_count,
            rolling_row_digest,
            reference_count,
            reference_epoch,
            last_zero_reference_version,
            state,
        };
        record.validate()?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

impl GenericIndexRowRecord {
    pub fn validate(&self) -> Result<(), GenericIndexRecordError> {
        if self.relative_path.is_none() && self.binding != GenericIndexRowBinding::Directory {
            return Err(GenericIndexRecordError::InvalidRowBinding {
                reason: "the registration-root row must be directory-bound",
            });
        }
        if self.values.len() > MAX_GENERIC_INDEX_ROW_FIELDS {
            return Err(GenericIndexRecordError::CountLimit {
                field: "row_fields",
                count: self.values.len(),
                max: MAX_GENERIC_INDEX_ROW_FIELDS,
            });
        }
        let mut previous: Option<&QueryFieldId> = None;
        for field_values in &self.values {
            validate_custom_field(&field_values.field)?;
            if previous.is_some_and(|field| field >= &field_values.field) {
                return Err(GenericIndexRecordError::RowFieldsNotCanonical);
            }
            if field_values.values.is_empty() {
                return Err(GenericIndexRecordError::EmptyFieldValues {
                    field: field_values.field.to_string(),
                });
            }
            if field_values.values.len() > MAX_GENERIC_INDEX_VALUES_PER_FIELD {
                return Err(GenericIndexRecordError::CountLimit {
                    field: "field_values",
                    count: field_values.values.len(),
                    max: MAX_GENERIC_INDEX_VALUES_PER_FIELD,
                });
            }
            for value in &field_values.values {
                validate_generic_scalar(&field_values.field, value)?;
            }
            previous = Some(&field_values.field);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        self.validate()?;
        let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        put_optional_path(&mut encoded, self.relative_path.as_ref())?;
        match self.binding {
            GenericIndexRowBinding::Directory => encoded.push(1),
            GenericIndexRowBinding::Unbound => encoded.push(2),
            GenericIndexRowBinding::Artifact(binding) => {
                encoded.push(3);
                encoded.extend_from_slice(binding.artifact_revision_id.as_bytes());
                encoded.extend_from_slice(&binding.path_generation.get().to_be_bytes());
            }
        }
        put_u16_count(
            &mut encoded,
            "row_fields",
            self.values.len(),
            MAX_GENERIC_INDEX_ROW_FIELDS,
        )?;
        for field_values in &self.values {
            put_short_bytes(
                &mut encoded,
                "field_id",
                field_values.field.as_bytes(),
                MAX_QUERY_FIELD_ID_BYTES,
            )?;
            put_u16_count(
                &mut encoded,
                "field_values",
                field_values.values.len(),
                MAX_GENERIC_INDEX_VALUES_PER_FIELD,
            )?;
            for value in &field_values.values {
                put_generic_scalar(&mut encoded, &field_values.field, value)?;
            }
        }
        if encoded.len() > MAX_GENERIC_INDEX_ROW_BYTES {
            return Err(GenericIndexRecordError::LengthLimit {
                field: "generic_index_row",
                length: encoded.len(),
                max: MAX_GENERIC_INDEX_ROW_BYTES,
            });
        }
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        if encoded.len() > MAX_GENERIC_INDEX_ROW_BYTES {
            return Err(GenericIndexRecordError::LengthLimit {
                field: "generic_index_row",
                length: encoded.len(),
                max: MAX_GENERIC_INDEX_ROW_BYTES,
            });
        }
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let relative_path = decoder.optional_path()?;
        let binding = match decoder.u8("row_binding")? {
            1 => GenericIndexRowBinding::Directory,
            2 => GenericIndexRowBinding::Unbound,
            3 => GenericIndexRowBinding::Artifact(GenericIndexArtifactBinding {
                artifact_revision_id: ArtifactRevisionId::from_bytes(
                    decoder.fixed("artifact_revision_id")?,
                ),
                path_generation: nonzero_generation(
                    decoder.u64("path_generation")?,
                    "path_generation",
                )?,
            }),
            value => {
                return Err(GenericIndexRecordError::UnknownDiscriminant {
                    type_name: "GenericIndexRowBinding",
                    value,
                })
            }
        };
        let field_count = decoder.bounded_u16_count("row_fields", MAX_GENERIC_INDEX_ROW_FIELDS)?;
        let mut values = Vec::with_capacity(field_count);
        for _ in 0..field_count {
            let field = decoder.query_field("field_id")?;
            let value_count =
                decoder.bounded_u16_count("field_values", MAX_GENERIC_INDEX_VALUES_PER_FIELD)?;
            let mut field_scalars = Vec::with_capacity(value_count);
            for _ in 0..value_count {
                field_scalars.push(decoder.generic_scalar(&field)?);
            }
            values.push(GenericIndexFieldValues {
                field,
                values: field_scalars,
            });
        }
        decoder.finish()?;
        let record = Self {
            relative_path,
            binding,
            values,
        };
        record.validate()?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

impl GenericIndexGenerationRefRecord {
    pub fn validate(&self) -> Result<(), GenericIndexRecordError> {
        if self.reference_epoch_at_add == ReferenceEpoch::ZERO {
            return Err(GenericIndexRecordError::ZeroScalar {
                field: "reference_epoch_at_add",
            });
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        self.validate()?;
        let mut encoded = Vec::with_capacity(1 + 1 + SHA256_BYTES + 8);
        encoded.push(GENERIC_INDEX_VALUE_FORMAT_VERSION);
        encoded.push(self.kind.into());
        encoded.extend_from_slice(&self.owner_digest);
        encoded.extend_from_slice(&self.reference_epoch_at_add.get().to_be_bytes());
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let kind = durable_discriminant(
            decoder.u8("reference_kind")?,
            GenericIndexReferenceKind::try_from,
        )?;
        let owner_digest = decoder.fixed("owner_digest")?;
        let reference_epoch_at_add = ReferenceEpoch::new(decoder.u64("reference_epoch_at_add")?);
        decoder.finish()?;
        let record = Self {
            kind,
            owner_digest,
            reference_epoch_at_add,
        };
        record.validate()?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

impl GenericIndexAppendReceiptRecord {
    pub fn validate(&self) -> Result<(), GenericIndexRecordError> {
        if self.row_count == 0 {
            return Err(GenericIndexRecordError::ZeroScalar { field: "row_count" });
        }
        if self.row_count > MAX_GENERIC_INDEX_APPEND_ROWS {
            return Err(GenericIndexRecordError::CountLimit {
                field: "row_count",
                count: self.row_count as usize,
                max: MAX_GENERIC_INDEX_APPEND_ROWS as usize,
            });
        }
        let expected = self
            .first_sequence
            .checked_add(u64::from(self.row_count))
            .ok_or(GenericIndexRecordError::RangeOverflow {
                field: "append_sequence",
            })?;
        if expected != self.resulting_row_count {
            return Err(GenericIndexRecordError::InvalidAppendReceipt);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        self.validate()?;
        let mut encoded = Vec::with_capacity(1 + 8 + 4 + 8 + SHA256_BYTES + 8 + SHA256_BYTES);
        encoded.push(GENERIC_INDEX_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(&self.first_sequence.to_be_bytes());
        encoded.extend_from_slice(&self.row_count.to_be_bytes());
        encoded.extend_from_slice(&self.commit_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.input_digest);
        encoded.extend_from_slice(&self.resulting_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.resulting_row_digest);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let record = Self {
            first_sequence: decoder.u64("first_sequence")?,
            row_count: decoder.u32("row_count")?,
            commit_version: CommitVersion::new(decoder.u64("commit_version")?).map_err(|_| {
                GenericIndexRecordError::ZeroScalar {
                    field: "commit_version",
                }
            })?,
            input_digest: decoder.fixed("input_digest")?,
            resulting_row_count: decoder.u64("resulting_row_count")?,
            resulting_row_digest: decoder.fixed("resulting_row_digest")?,
        };
        decoder.finish()?;
        record.validate()?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

impl GenericIndexRegistrationOperationRecord {
    pub fn validate(&self) -> Result<(), GenericIndexRecordError> {
        if self.appended_row_count > self.declared_row_count {
            return Err(GenericIndexRecordError::InvalidGenerationProgress);
        }
        validate_row_closure(self.appended_row_count, self.rolling_row_digest)?;
        if self.last_transition_version.get() <= self.source_read_version.get() {
            return Err(GenericIndexRecordError::InvalidRegistrationState {
                reason: "last transition must be newer than the frozen source read version",
            });
        }
        let next_pointer_generation = match self.expected_current_generation {
            None => 1,
            Some(current) => current.get().checked_add(1).ok_or(
                GenericIndexRecordError::InvalidRegistrationState {
                    reason: "the expected current pointer generation cannot advance",
                },
            )?,
        };
        if self.phase == GenericIndexRegistrationPhase::Preparing && self.appended_row_count != 0 {
            return Err(GenericIndexRecordError::InvalidRegistrationState {
                reason: "a preparing registration cannot contain appended rows",
            });
        }
        if matches!(
            self.phase,
            GenericIndexRegistrationPhase::Sealing
                | GenericIndexRegistrationPhase::Publishing
                | GenericIndexRegistrationPhase::Complete
        ) && self.appended_row_count != self.declared_row_count
        {
            return Err(GenericIndexRecordError::InvalidRegistrationState {
                reason: "a sealing, publishing, or complete registration must contain every declared row",
            });
        }
        match self.phase {
            GenericIndexRegistrationPhase::Complete => {
                let published =
                    self.published_pointer_generation
                        .ok_or(GenericIndexRecordError::InvalidRegistrationState {
                        reason:
                            "a complete registration must retain its published pointer generation",
                    })?;
                if published.get() != next_pointer_generation {
                    return Err(GenericIndexRecordError::InvalidRegistrationState {
                        reason: "published pointer generation is not the exact expected successor",
                    });
                }
                if self.terminal_error.is_some() {
                    return Err(GenericIndexRecordError::InvalidRegistrationState {
                        reason: "a complete registration cannot retain a terminal error",
                    });
                }
            }
            GenericIndexRegistrationPhase::Quarantined => {
                if self.published_pointer_generation.is_some() {
                    return Err(GenericIndexRecordError::InvalidRegistrationState {
                        reason: "a quarantined registration cannot claim publication",
                    });
                }
                validate_terminal_error(self.terminal_error.as_deref())?;
            }
            _ => {
                if self.published_pointer_generation.is_some() {
                    return Err(GenericIndexRecordError::InvalidRegistrationState {
                        reason: "only a complete registration can claim publication",
                    });
                }
                if self.terminal_error.is_some() {
                    return Err(GenericIndexRecordError::InvalidRegistrationState {
                        reason: "only a quarantined registration can retain a terminal error",
                    });
                }
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        self.validate()?;
        let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        encoded.extend_from_slice(self.workspace_incarnation_id.as_bytes());
        put_optional_path(&mut encoded, self.index_path.as_ref())?;
        encoded.extend_from_slice(self.generation_id.as_bytes());
        encoded.extend_from_slice(self.request_digest.as_bytes());
        encoded.extend_from_slice(&self.source_read_version.get().to_be_bytes());
        encoded.extend_from_slice(&self.last_transition_version.get().to_be_bytes());
        put_optional_generation(&mut encoded, self.expected_current_generation);
        encoded.extend_from_slice(&self.capability_digest);
        encoded.extend_from_slice(&self.declared_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.appended_row_count.to_be_bytes());
        encoded.extend_from_slice(&self.rolling_row_digest);
        encoded.push(self.phase.into());
        put_optional_generation(&mut encoded, self.published_pointer_generation);
        put_optional_string(
            &mut encoded,
            "terminal_error",
            self.terminal_error.as_deref(),
            MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES,
        )?;
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let workspace_incarnation_id =
            WorkspaceIncarnationId::from_bytes(decoder.fixed("workspace_incarnation_id")?);
        let index_path = decoder.optional_path()?;
        let generation_id = GenericIndexGenerationId::from_bytes(decoder.fixed("generation_id")?);
        let request_digest = CommandDigest::from_bytes(decoder.fixed("request_digest")?);
        let source_read_version =
            ReadVersion::new(decoder.u64("source_read_version")?).map_err(|_| {
                GenericIndexRecordError::ZeroScalar {
                    field: "source_read_version",
                }
            })?;
        let last_transition_version = CommitVersion::new(decoder.u64("last_transition_version")?)
            .map_err(|_| GenericIndexRecordError::ZeroScalar {
            field: "last_transition_version",
        })?;
        let expected_current_generation =
            decoder.optional_generation("expected_current_generation")?;
        let capability_digest = decoder.fixed("capability_digest")?;
        let declared_row_count = decoder.u64("declared_row_count")?;
        let appended_row_count = decoder.u64("appended_row_count")?;
        let rolling_row_digest = decoder.fixed("rolling_row_digest")?;
        let phase = durable_discriminant(
            decoder.u8("registration_phase")?,
            GenericIndexRegistrationPhase::try_from,
        )?;
        let published_pointer_generation =
            decoder.optional_generation("published_pointer_generation")?;
        let terminal_error =
            decoder.optional_string("terminal_error", MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES)?;
        decoder.finish()?;
        let record = Self {
            workspace_incarnation_id,
            index_path,
            generation_id,
            request_digest,
            source_read_version,
            last_transition_version,
            expected_current_generation,
            capability_digest,
            declared_row_count,
            appended_row_count,
            rolling_row_digest,
            phase,
            published_pointer_generation,
            terminal_error,
        };
        record.validate()?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

impl CommitGenericIndexMemberRecord {
    pub fn encode(&self) -> Result<Vec<u8>, GenericIndexRecordError> {
        validate_row_closure(self.row_count, self.row_digest)?;
        let mut encoded = Vec::with_capacity(1 + FIXED_ID_BYTES + SHA256_BYTES + 8 + SHA256_BYTES);
        encoded.push(GENERIC_INDEX_VALUE_FORMAT_VERSION);
        encoded.extend_from_slice(self.generation_id.as_bytes());
        encoded.extend_from_slice(&self.capability_digest);
        encoded.extend_from_slice(&self.row_count.to_be_bytes());
        encoded.extend_from_slice(&self.row_digest);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, GenericIndexRecordError> {
        let mut decoder = Decoder::new(encoded);
        decoder.require_version()?;
        let record = Self {
            generation_id: GenericIndexGenerationId::from_bytes(decoder.fixed("generation_id")?),
            capability_digest: decoder.fixed("capability_digest")?,
            row_count: decoder.u64("row_count")?,
            row_digest: decoder.fixed("row_digest")?,
        };
        decoder.finish()?;
        validate_row_closure(record.row_count, record.row_digest)?;
        ensure_canonical(encoded, record.encode()?)?;
        Ok(record)
    }
}

/// Canonical empty rolling digest used before the first appended row.
pub const fn empty_generic_index_row_digest() -> [u8; SHA256_BYTES] {
    EMPTY_ROLLING_DIGEST
}

/// Digest the exact ordered field capability list.
pub fn generic_index_capability_digest(
    capabilities: &[GenericIndexFieldCapability],
) -> Result<[u8; SHA256_BYTES], GenericIndexRecordError> {
    validate_capabilities(capabilities)?;
    let mut encoded = Vec::new();
    put_capabilities(&mut encoded, capabilities)?;
    Ok(domain_digest(
        b"nokv/generic-index/capabilities/v1",
        &encoded,
    ))
}

/// Digest one row together with its immutable sequence position.
pub fn generic_index_row_digest(
    sequence: u64,
    row: &GenericIndexRowRecord,
) -> Result<[u8; SHA256_BYTES], GenericIndexRecordError> {
    let encoded = row.encode()?;
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/generic-index/row/v1");
    hasher.update(sequence.to_be_bytes());
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    Ok(hasher.finalize().into())
}

/// Advance the canonical ordered row closure by one row digest.
pub fn advance_generic_index_row_rolling_digest(
    previous: [u8; SHA256_BYTES],
    row_digest: [u8; SHA256_BYTES],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/generic-index/row-chain/v1");
    hasher.update(previous);
    hasher.update(row_digest);
    hasher.finalize().into()
}

/// Digest an exact append batch for durable response-loss replay.
pub fn generic_index_append_batch_digest(
    first_sequence: u64,
    rows: &[GenericIndexRowRecord],
) -> Result<[u8; SHA256_BYTES], GenericIndexRecordError> {
    if rows.is_empty() {
        return Err(GenericIndexRecordError::ZeroScalar { field: "row_count" });
    }
    if rows.len() > MAX_GENERIC_INDEX_APPEND_ROWS as usize {
        return Err(GenericIndexRecordError::CountLimit {
            field: "row_count",
            count: rows.len(),
            max: MAX_GENERIC_INDEX_APPEND_ROWS as usize,
        });
    }
    first_sequence.checked_add(rows.len() as u64).ok_or(
        GenericIndexRecordError::RangeOverflow {
            field: "append_sequence",
        },
    )?;
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/generic-index/append-batch/v1");
    hasher.update(first_sequence.to_be_bytes());
    hasher.update((rows.len() as u64).to_be_bytes());
    for (offset, row) in rows.iter().enumerate() {
        hasher.update(generic_index_row_digest(
            first_sequence + offset as u64,
            row,
        )?);
    }
    Ok(hasher.finalize().into())
}

/// Digest the exact caller-visible append input for durable response-loss
/// replay. Artifact revision and path-generation fences are deliberately not
/// included: those are derived from the frozen source and sealed by the
/// resulting row digest, while this digest must remain verifiable after row
/// cleanup.
pub fn generic_index_append_input_digest(
    first_sequence: u64,
    rows: &[GenericIndexRowRecord],
) -> Result<[u8; SHA256_BYTES], GenericIndexRecordError> {
    if rows.is_empty() {
        return Err(GenericIndexRecordError::ZeroScalar { field: "row_count" });
    }
    if rows.len() > MAX_GENERIC_INDEX_APPEND_ROWS as usize {
        return Err(GenericIndexRecordError::CountLimit {
            field: "row_count",
            count: rows.len(),
            max: MAX_GENERIC_INDEX_APPEND_ROWS as usize,
        });
    }
    first_sequence.checked_add(rows.len() as u64).ok_or(
        GenericIndexRecordError::RangeOverflow {
            field: "append_sequence",
        },
    )?;

    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/generic-index/append-input/v1");
    hasher.update(first_sequence.to_be_bytes());
    hasher.update((rows.len() as u64).to_be_bytes());
    for row in rows {
        row.validate()?;
        let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        put_optional_path(&mut encoded, row.relative_path.as_ref())?;
        put_u16_count(
            &mut encoded,
            "row_fields",
            row.values.len(),
            MAX_GENERIC_INDEX_ROW_FIELDS,
        )?;
        for field_values in &row.values {
            put_short_bytes(
                &mut encoded,
                "field_id",
                field_values.field.as_bytes(),
                MAX_QUERY_FIELD_ID_BYTES,
            )?;
            put_u16_count(
                &mut encoded,
                "field_values",
                field_values.values.len(),
                MAX_GENERIC_INDEX_VALUES_PER_FIELD,
            )?;
            for value in &field_values.values {
                put_generic_scalar(&mut encoded, &field_values.field, value)?;
            }
        }
        hasher.update((encoded.len() as u64).to_be_bytes());
        hasher.update(encoded);
    }
    Ok(hasher.finalize().into())
}

/// Verify that a generation header is sealed to the exact advertised closure.
pub fn verify_generic_index_generation_seal(
    generation: &GenericIndexGenerationRecord,
    expected_capability_digest: [u8; SHA256_BYTES],
    expected_row_count: u64,
    expected_row_digest: [u8; SHA256_BYTES],
) -> Result<(), GenericIndexRecordError> {
    generation.validate()?;
    if generation.state != GenericIndexGenerationState::Sealed {
        return Err(GenericIndexRecordError::InvalidSeal {
            reason: "generation is not sealed",
        });
    }
    if generic_index_capability_digest(&generation.capabilities)? != expected_capability_digest {
        return Err(GenericIndexRecordError::InvalidSeal {
            reason: "capability digest does not match",
        });
    }
    if generation.appended_row_count != expected_row_count
        || generation.rolling_row_digest != expected_row_digest
    {
        return Err(GenericIndexRecordError::InvalidSeal {
            reason: "row closure does not match",
        });
    }
    Ok(())
}

/// Digest a commit member using its workspace-relative index scope.
pub fn commit_generic_index_member_row_digest(
    index_path: Option<&NormalizedRelativePath>,
    member: &CommitGenericIndexMemberRecord,
) -> Result<[u8; SHA256_BYTES], GenericIndexRecordError> {
    let encoded = member.encode()?;
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/commit-generic-index-member/v1");
    match index_path {
        None => hasher.update([0]),
        Some(path) => {
            hasher.update([1]);
            hasher.update((path.byte_len() as u64).to_be_bytes());
            hasher.update(path.as_str().as_bytes());
        }
    }
    hasher.update((encoded.len() as u64).to_be_bytes());
    hasher.update(encoded);
    Ok(hasher.finalize().into())
}

/// Advance the immutable commit Generic-index member closure.
pub fn advance_commit_generic_index_member_rolling_digest(
    previous: [u8; SHA256_BYTES],
    member_digest: [u8; SHA256_BYTES],
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/commit-generic-index-chain/v1");
    hasher.update(previous);
    hasher.update(member_digest);
    hasher.finalize().into()
}

/// Stable opaque owner digest for a current pointer reference.
pub fn generic_index_current_owner_digest(
    workspace: WorkspaceIncarnationId,
    index_path: Option<&NormalizedRelativePath>,
) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, b"nokv/generic-index/current-owner/v1");
    hasher.update(workspace.as_bytes());
    match index_path {
        None => hasher.update([0]),
        Some(path) => {
            hasher.update([1]);
            hasher.update((path.byte_len() as u64).to_be_bytes());
            hasher.update(path.as_str().as_bytes());
        }
    }
    hasher.finalize().into()
}

/// Stable opaque owner digest for one immutable commit reference.
pub fn generic_index_commit_owner_digest(commit: CommitId) -> [u8; SHA256_BYTES] {
    fixed_owner_digest(b"nokv/generic-index/commit-owner/v1", commit.as_bytes())
}

/// Stable opaque owner digest for a commit-build operation reference.
pub fn generic_index_build_commit_owner_digest(operation: OperationId) -> [u8; SHA256_BYTES] {
    fixed_owner_digest(
        b"nokv/generic-index/build-commit-owner/v1",
        operation.as_bytes(),
    )
}

/// Stable opaque owner digest for a restore operation reference.
pub fn generic_index_restore_owner_digest(operation: OperationId) -> [u8; SHA256_BYTES] {
    fixed_owner_digest(b"nokv/generic-index/restore-owner/v1", operation.as_bytes())
}

/// Stable opaque owner digest for a registration operation reference.
pub fn generic_index_registration_owner_digest(operation: OperationId) -> [u8; SHA256_BYTES] {
    fixed_owner_digest(
        b"nokv/generic-index/registration-owner/v1",
        operation.as_bytes(),
    )
}

fn fixed_owner_digest(domain: &[u8], owner: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, domain);
    hasher.update((owner.len() as u64).to_be_bytes());
    hasher.update(owner);
    hasher.finalize().into()
}

fn domain_digest(domain: &[u8], value: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hash_domain(&mut hasher, domain);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
    hasher.finalize().into()
}

fn hash_domain(hasher: &mut Sha256, domain: &[u8]) {
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
}

fn validate_capabilities(
    capabilities: &[GenericIndexFieldCapability],
) -> Result<(), GenericIndexRecordError> {
    if capabilities.len() > MAX_GENERIC_INDEX_FIELDS {
        return Err(GenericIndexRecordError::CountLimit {
            field: "capabilities",
            count: capabilities.len(),
            max: MAX_GENERIC_INDEX_FIELDS,
        });
    }
    let mut previous: Option<&QueryFieldId> = None;
    for capability in capabilities {
        validate_custom_field(&capability.field)?;
        if previous.is_some_and(|field| field >= &capability.field) {
            return Err(GenericIndexRecordError::CapabilitiesNotCanonical);
        }
        if capability
            .operators
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(GenericIndexRecordError::OperatorsNotCanonical {
                field: capability.field.to_string(),
            });
        }
        previous = Some(&capability.field);
    }
    Ok(())
}

fn validate_custom_field(field: &QueryFieldId) -> Result<(), GenericIndexRecordError> {
    let reparsed = QueryFieldId::new(field.to_string()).map_err(|error| {
        GenericIndexRecordError::InvalidField {
            field: field.to_string(),
            reason: error.to_string(),
        }
    })?;
    if GENERIC_BUILTIN_FIELD_IDS
        .binary_search(&reparsed.as_str())
        .is_ok()
    {
        return Err(GenericIndexRecordError::ReservedField {
            field: reparsed.to_string(),
        });
    }
    Ok(())
}

fn validate_row_closure(
    row_count: u64,
    row_digest: [u8; SHA256_BYTES],
) -> Result<(), GenericIndexRecordError> {
    if row_count == 0 && row_digest != empty_generic_index_row_digest() {
        Err(GenericIndexRecordError::InvalidSeal {
            reason: "a zero-row closure must use the canonical empty digest",
        })
    } else {
        Ok(())
    }
}

fn validate_generic_scalar(
    field: &QueryFieldId,
    value: &QueryScalar,
) -> Result<(), GenericIndexRecordError> {
    match value {
        QueryScalar::String(value) => {
            if value.len() > MAX_QUERY_SCALAR_BYTES {
                return Err(GenericIndexRecordError::LengthLimit {
                    field: "string_scalar",
                    length: value.len(),
                    max: MAX_QUERY_SCALAR_BYTES,
                });
            }
            Ok(())
        }
        QueryScalar::Unsigned(_) | QueryScalar::Float(_) => Ok(()),
        QueryScalar::Null => Err(unsupported_scalar(field, "null")),
        QueryScalar::Boolean(_) => Err(unsupported_scalar(field, "boolean")),
        QueryScalar::Signed(_) => Err(unsupported_scalar(field, "signed")),
        QueryScalar::Timestamp(_) => Err(unsupported_scalar(field, "timestamp")),
        QueryScalar::Bytes(_) => Err(unsupported_scalar(field, "bytes")),
    }
}

fn unsupported_scalar(field: &QueryFieldId, scalar_type: &'static str) -> GenericIndexRecordError {
    GenericIndexRecordError::UnsupportedScalarType {
        field: field.to_string(),
        scalar_type,
    }
}

fn put_capabilities(
    encoded: &mut Vec<u8>,
    capabilities: &[GenericIndexFieldCapability],
) -> Result<(), GenericIndexRecordError> {
    put_u16_count(
        encoded,
        "capabilities",
        capabilities.len(),
        MAX_GENERIC_INDEX_FIELDS,
    )?;
    for capability in capabilities {
        put_short_bytes(
            encoded,
            "field_id",
            capability.field.as_bytes(),
            MAX_QUERY_FIELD_ID_BYTES,
        )?;
        let operator_count = u8::try_from(capability.operators.len()).map_err(|_| {
            GenericIndexRecordError::CountLimit {
                field: "operators",
                count: capability.operators.len(),
                max: u8::MAX as usize,
            }
        })?;
        encoded.push(operator_count);
        encoded.extend(capability.operators.iter().copied().map(u8::from));
        encoded.push(u8::from(capability.sortable));
        encoded.push(u8::from(capability.facetable));
    }
    Ok(())
}

fn put_generic_scalar(
    encoded: &mut Vec<u8>,
    field: &QueryFieldId,
    value: &QueryScalar,
) -> Result<(), GenericIndexRecordError> {
    validate_generic_scalar(field, value)?;
    match value {
        QueryScalar::String(value) => {
            encoded.push(1);
            put_long_bytes(
                encoded,
                "string_scalar",
                value.as_bytes(),
                MAX_QUERY_SCALAR_BYTES,
            )?;
        }
        QueryScalar::Unsigned(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        QueryScalar::Float(value) => {
            encoded.push(3);
            encoded.extend_from_slice(&value.get().to_bits().to_be_bytes());
        }
        _ => return Err(unsupported_scalar(field, "unsupported")),
    }
    Ok(())
}

fn put_optional_path(
    encoded: &mut Vec<u8>,
    path: Option<&NormalizedRelativePath>,
) -> Result<(), GenericIndexRecordError> {
    match path {
        None => encoded.push(0),
        Some(path) => {
            encoded.push(1);
            put_long_bytes(
                encoded,
                "relative_path",
                path.as_str().as_bytes(),
                NormalizedRelativePath::MAX_BYTES,
            )?;
        }
    }
    Ok(())
}

fn put_optional_generation(encoded: &mut Vec<u8>, generation: Option<Generation>) {
    match generation {
        None => encoded.push(0),
        Some(generation) => {
            encoded.push(1);
            encoded.extend_from_slice(&generation.get().to_be_bytes());
        }
    }
}

fn put_optional_commit_version(encoded: &mut Vec<u8>, version: Option<CommitVersion>) {
    match version {
        None => encoded.push(0),
        Some(version) => {
            encoded.push(1);
            encoded.extend_from_slice(&version.get().to_be_bytes());
        }
    }
}

fn put_optional_string(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: Option<&str>,
    max: usize,
) -> Result<(), GenericIndexRecordError> {
    match value {
        None => encoded.push(0),
        Some(value) => {
            encoded.push(1);
            put_long_bytes(encoded, field, value.as_bytes(), max)?;
        }
    }
    Ok(())
}

fn put_u16_count(
    encoded: &mut Vec<u8>,
    field: &'static str,
    count: usize,
    max: usize,
) -> Result<(), GenericIndexRecordError> {
    if count > max {
        return Err(GenericIndexRecordError::CountLimit { field, count, max });
    }
    let count = u16::try_from(count).map_err(|_| GenericIndexRecordError::CountLimit {
        field,
        count,
        max,
    })?;
    encoded.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn put_short_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
    max: usize,
) -> Result<(), GenericIndexRecordError> {
    if value.len() > max {
        return Err(GenericIndexRecordError::LengthLimit {
            field,
            length: value.len(),
            max,
        });
    }
    let length = u16::try_from(value.len()).map_err(|_| GenericIndexRecordError::LengthLimit {
        field,
        length: value.len(),
        max,
    })?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn put_long_bytes(
    encoded: &mut Vec<u8>,
    field: &'static str,
    value: &[u8],
    max: usize,
) -> Result<(), GenericIndexRecordError> {
    if value.len() > max {
        return Err(GenericIndexRecordError::LengthLimit {
            field,
            length: value.len(),
            max,
        });
    }
    let length = u32::try_from(value.len()).map_err(|_| GenericIndexRecordError::LengthLimit {
        field,
        length: value.len(),
        max,
    })?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value);
    Ok(())
}

fn validate_terminal_error(value: Option<&str>) -> Result<(), GenericIndexRecordError> {
    let value = value.ok_or(GenericIndexRecordError::InvalidRegistrationState {
        reason: "a quarantined registration must retain a terminal error",
    })?;
    if value.is_empty() {
        return Err(GenericIndexRecordError::InvalidRegistrationState {
            reason: "terminal error must not be empty",
        });
    }
    if value.len() > MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES {
        return Err(GenericIndexRecordError::LengthLimit {
            field: "terminal_error",
            length: value.len(),
            max: MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES,
        });
    }
    if let Some((index, _)) = value.bytes().enumerate().find(|(_, byte)| *byte == 0) {
        return Err(GenericIndexRecordError::InvalidField {
            field: "terminal_error".to_owned(),
            reason: format!("contains NUL at byte offset {index}"),
        });
    }
    Ok(())
}

fn nonzero_generation(
    value: u64,
    field: &'static str,
) -> Result<Generation, GenericIndexRecordError> {
    Generation::new(value).map_err(|_| GenericIndexRecordError::ZeroScalar { field })
}

fn ensure_canonical(original: &[u8], canonical: Vec<u8>) -> Result<(), GenericIndexRecordError> {
    if original == canonical {
        Ok(())
    } else {
        Err(GenericIndexRecordError::NonCanonicalEncoding)
    }
}

fn durable_discriminant<T, E>(
    value: u8,
    decode: impl FnOnce(u8) -> Result<T, E>,
) -> Result<T, GenericIndexRecordError>
where
    E: DurableDiscriminantError,
{
    decode(value).map_err(|error| GenericIndexRecordError::UnknownDiscriminant {
        type_name: error.type_name(),
        value: error.value(),
    })
}

trait DurableDiscriminantError {
    fn type_name(&self) -> &'static str;
    fn value(&self) -> u8;
}

impl DurableDiscriminantError for nokv_types::UnknownDurableDiscriminant {
    fn type_name(&self) -> &'static str {
        (*self).type_name()
    }

    fn value(&self) -> u8 {
        (*self).value()
    }
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn new(encoded: &'a [u8]) -> Self {
        Self { remaining: encoded }
    }

    fn require_version(&mut self) -> Result<(), GenericIndexRecordError> {
        let actual = self.u8("value_format_version")?;
        if actual == GENERIC_INDEX_VALUE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(GenericIndexRecordError::UnsupportedValueVersion {
                actual,
                expected: GENERIC_INDEX_VALUE_FORMAT_VERSION,
            })
        }
    }

    fn take(
        &mut self,
        field: &'static str,
        length: usize,
    ) -> Result<&'a [u8], GenericIndexRecordError> {
        if self.remaining.len() < length {
            return Err(GenericIndexRecordError::Truncated {
                field,
                needed: length,
                remaining: self.remaining.len(),
            });
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, GenericIndexRecordError> {
        Ok(self.take(field, 1)?[0])
    }

    fn u16(&mut self, field: &'static str) -> Result<u16, GenericIndexRecordError> {
        Ok(u16::from_be_bytes(
            self.take(field, 2)?.try_into().expect("exact length"),
        ))
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, GenericIndexRecordError> {
        Ok(u32::from_be_bytes(
            self.take(field, 4)?.try_into().expect("exact length"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, GenericIndexRecordError> {
        Ok(u64::from_be_bytes(
            self.take(field, 8)?.try_into().expect("exact length"),
        ))
    }

    fn fixed<const N: usize>(
        &mut self,
        field: &'static str,
    ) -> Result<[u8; N], GenericIndexRecordError> {
        Ok(self.take(field, N)?.try_into().expect("exact length"))
    }

    fn bounded_u16_count(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<usize, GenericIndexRecordError> {
        let count = usize::from(self.u16(field)?);
        if count > max {
            Err(GenericIndexRecordError::CountLimit { field, count, max })
        } else {
            Ok(count)
        }
    }

    fn bool(&mut self, field: &'static str) -> Result<bool, GenericIndexRecordError> {
        match self.u8(field)? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(GenericIndexRecordError::InvalidBoolean { field, value }),
        }
    }

    fn short_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<&'a [u8], GenericIndexRecordError> {
        let length = usize::from(self.u16(field)?);
        if length > max {
            return Err(GenericIndexRecordError::LengthLimit { field, length, max });
        }
        self.take(field, length)
    }

    fn long_bytes(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<&'a [u8], GenericIndexRecordError> {
        let length = self.u32(field)? as usize;
        if length > max {
            return Err(GenericIndexRecordError::LengthLimit { field, length, max });
        }
        self.take(field, length)
    }

    fn query_field(
        &mut self,
        field_name: &'static str,
    ) -> Result<QueryFieldId, GenericIndexRecordError> {
        let bytes = self.short_bytes(field_name, MAX_QUERY_FIELD_ID_BYTES)?;
        let value = std::str::from_utf8(bytes)
            .map_err(|_| GenericIndexRecordError::InvalidUtf8 { field: field_name })?;
        QueryFieldId::new(value.to_owned()).map_err(|error| GenericIndexRecordError::InvalidField {
            field: value.to_owned(),
            reason: error.to_string(),
        })
    }

    fn capabilities(
        &mut self,
    ) -> Result<Vec<GenericIndexFieldCapability>, GenericIndexRecordError> {
        let count = self.bounded_u16_count("capabilities", MAX_GENERIC_INDEX_FIELDS)?;
        let mut capabilities = Vec::with_capacity(count);
        for _ in 0..count {
            let field = self.query_field("field_id")?;
            let operator_count = usize::from(self.u8("operator_count")?);
            if operator_count > 12 {
                return Err(GenericIndexRecordError::CountLimit {
                    field: "operators",
                    count: operator_count,
                    max: 12,
                });
            }
            let mut operators = Vec::with_capacity(operator_count);
            for _ in 0..operator_count {
                operators.push(GenericIndexOperator::try_from(self.u8("operator")?)?);
            }
            let sortable = self.bool("sortable")?;
            let facetable = self.bool("facetable")?;
            capabilities.push(GenericIndexFieldCapability {
                field,
                operators,
                sortable,
                facetable,
            });
        }
        Ok(capabilities)
    }

    fn generic_scalar(
        &mut self,
        field: &QueryFieldId,
    ) -> Result<QueryScalar, GenericIndexRecordError> {
        match self.u8("scalar_type")? {
            1 => {
                let bytes = self.long_bytes("string_scalar", MAX_QUERY_SCALAR_BYTES)?;
                let value = std::str::from_utf8(bytes).map_err(|_| {
                    GenericIndexRecordError::InvalidUtf8 {
                        field: "string_scalar",
                    }
                })?;
                Ok(QueryScalar::String(value.to_owned()))
            }
            2 => Ok(QueryScalar::Unsigned(self.u64("unsigned_scalar")?)),
            3 => {
                let bits = self.u64("float_scalar")?;
                let value = f64::from_bits(bits);
                let finite =
                    FiniteFloat::new(value).map_err(|_| GenericIndexRecordError::NonFiniteFloat)?;
                if finite.get().to_bits() != bits {
                    return Err(GenericIndexRecordError::NonCanonicalFloatZero);
                }
                Ok(QueryScalar::Float(finite))
            }
            value => Err(GenericIndexRecordError::UnknownDiscriminant {
                type_name: "GenericIndexScalar",
                value,
            }),
        }
        .and_then(|value| {
            validate_generic_scalar(field, &value)?;
            Ok(value)
        })
    }

    fn optional_path(&mut self) -> Result<Option<NormalizedRelativePath>, GenericIndexRecordError> {
        match self.u8("path_tag")? {
            0 => Ok(None),
            1 => {
                let bytes = self.long_bytes("relative_path", NormalizedRelativePath::MAX_BYTES)?;
                let value = std::str::from_utf8(bytes).map_err(|_| {
                    GenericIndexRecordError::InvalidUtf8 {
                        field: "relative_path",
                    }
                })?;
                NormalizedRelativePath::new(value.to_owned())
                    .map(Some)
                    .map_err(|error| GenericIndexRecordError::InvalidPath {
                        reason: error.to_string(),
                    })
            }
            value => Err(GenericIndexRecordError::InvalidOptionalTag {
                field: "relative_path",
                value,
            }),
        }
    }

    fn optional_generation(
        &mut self,
        field: &'static str,
    ) -> Result<Option<Generation>, GenericIndexRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => nonzero_generation(self.u64(field)?, field).map(Some),
            value => Err(GenericIndexRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_commit_version(
        &mut self,
        field: &'static str,
    ) -> Result<Option<CommitVersion>, GenericIndexRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => CommitVersion::new(self.u64(field)?)
                .map(Some)
                .map_err(|_| GenericIndexRecordError::ZeroScalar { field }),
            value => Err(GenericIndexRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn optional_string(
        &mut self,
        field: &'static str,
        max: usize,
    ) -> Result<Option<String>, GenericIndexRecordError> {
        match self.u8(field)? {
            0 => Ok(None),
            1 => {
                let bytes = self.long_bytes(field, max)?;
                let value = std::str::from_utf8(bytes)
                    .map_err(|_| GenericIndexRecordError::InvalidUtf8 { field })?;
                Ok(Some(value.to_owned()))
            }
            value => Err(GenericIndexRecordError::InvalidOptionalTag { field, value }),
        }
    }

    fn finish(self) -> Result<(), GenericIndexRecordError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(GenericIndexRecordError::TrailingBytes {
                count: self.remaining.len(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(value: &str) -> QueryFieldId {
        QueryFieldId::new(value).unwrap()
    }

    fn capability(value: &str) -> GenericIndexFieldCapability {
        GenericIndexFieldCapability {
            field: field(value),
            operators: vec![
                GenericIndexOperator::Equal,
                GenericIndexOperator::In,
                GenericIndexOperator::Exists,
            ],
            sortable: true,
            facetable: true,
        }
    }

    fn zero_row_generation() -> GenericIndexGenerationRecord {
        GenericIndexGenerationRecord {
            capabilities: vec![capability("run.status")],
            declared_row_count: 0,
            appended_row_count: 0,
            rolling_row_digest: empty_generic_index_row_digest(),
            reference_count: 1,
            reference_epoch: ReferenceEpoch::new(1),
            last_zero_reference_version: None,
            state: GenericIndexGenerationState::Sealed,
        }
    }

    fn multivalue_row() -> GenericIndexRowRecord {
        GenericIndexRowRecord {
            relative_path: Some(NormalizedRelativePath::new("a.json").unwrap()),
            binding: GenericIndexRowBinding::Artifact(GenericIndexArtifactBinding {
                artifact_revision_id: ArtifactRevisionId::from_bytes([3; FIXED_ID_BYTES]),
                path_generation: Generation::new(4).unwrap(),
            }),
            values: vec![GenericIndexFieldValues {
                field: field("run.score"),
                values: vec![
                    QueryScalar::String("7".to_owned()),
                    QueryScalar::Unsigned(7),
                    QueryScalar::Float(FiniteFloat::new(7.5).unwrap()),
                    QueryScalar::String("7".to_owned()),
                ],
            }],
        }
    }

    fn current() -> GenericIndexCurrentRecord {
        GenericIndexCurrentRecord {
            generation_id: GenericIndexGenerationId::from_bytes([1; FIXED_ID_BYTES]),
            pointer_generation: Generation::new(2).unwrap(),
            capability_digest: [3; SHA256_BYTES],
            row_count: 4,
            row_digest: [5; SHA256_BYTES],
        }
    }

    fn operation() -> GenericIndexRegistrationOperationRecord {
        GenericIndexRegistrationOperationRecord {
            workspace_incarnation_id: WorkspaceIncarnationId::from_bytes([1; FIXED_ID_BYTES]),
            index_path: Some(NormalizedRelativePath::new("outputs/runs").unwrap()),
            generation_id: GenericIndexGenerationId::from_bytes([2; FIXED_ID_BYTES]),
            request_digest: CommandDigest::from_bytes([3; SHA256_BYTES]),
            source_read_version: ReadVersion::new(4).unwrap(),
            last_transition_version: CommitVersion::new(5).unwrap(),
            expected_current_generation: Some(Generation::new(5).unwrap()),
            capability_digest: [6; SHA256_BYTES],
            declared_row_count: 2,
            appended_row_count: 2,
            rolling_row_digest: [7; SHA256_BYTES],
            phase: GenericIndexRegistrationPhase::Complete,
            published_pointer_generation: Some(Generation::new(6).unwrap()),
            terminal_error: None,
        }
    }

    fn assert_strict<T: fmt::Debug>(
        encoded: Vec<u8>,
        decode: fn(&[u8]) -> Result<T, GenericIndexRecordError>,
    ) {
        let mut future = encoded.clone();
        future[0] = GENERIC_INDEX_VALUE_FORMAT_VERSION + 1;
        assert!(matches!(
            decode(&future),
            Err(GenericIndexRecordError::UnsupportedValueVersion { .. })
        ));

        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(matches!(
            decode(&trailing),
            Err(GenericIndexRecordError::TrailingBytes { count: 1 })
        ));

        assert!(matches!(
            decode(&encoded[..encoded.len() - 1]),
            Err(GenericIndexRecordError::Truncated { .. })
        ));
    }

    #[test]
    fn zero_row_catalog_and_multivalue_row_are_strictly_encodable() {
        let catalog = zero_row_generation();
        assert_eq!(
            GenericIndexGenerationRecord::decode(&catalog.encode().unwrap()).unwrap(),
            catalog
        );

        let row = multivalue_row();
        assert_eq!(
            GenericIndexRowRecord::decode(&row.encode().unwrap()).unwrap(),
            row
        );

        let directory = GenericIndexRowRecord {
            relative_path: None,
            binding: GenericIndexRowBinding::Directory,
            values: Vec::new(),
        };
        assert_eq!(
            GenericIndexRowRecord::decode(&directory.encode().unwrap()).unwrap(),
            directory
        );
    }

    #[test]
    fn unbound_path_row_has_a_distinct_durable_binding() {
        let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        encoded.push(1);
        put_long_bytes(
            &mut encoded,
            "relative_path",
            b"future.json",
            NormalizedRelativePath::MAX_BYTES,
        )
        .unwrap();
        encoded.push(2);
        encoded.extend_from_slice(&0_u16.to_be_bytes());
        let row = GenericIndexRowRecord::decode(&encoded).unwrap();
        assert_eq!(row.binding, GenericIndexRowBinding::Unbound);
        assert_eq!(row.encode().unwrap(), encoded);

        let invalid_root = GenericIndexRowRecord {
            relative_path: None,
            binding: GenericIndexRowBinding::Unbound,
            values: Vec::new(),
        };
        assert!(matches!(
            invalid_root.encode(),
            Err(GenericIndexRecordError::InvalidRowBinding { .. })
        ));

        let raw_invalid_root = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION, 0, 2, 0, 0];
        assert!(matches!(
            GenericIndexRowRecord::decode(&raw_invalid_root),
            Err(GenericIndexRecordError::InvalidRowBinding { .. })
        ));
    }

    #[test]
    fn every_record_has_a_strict_versioned_envelope() {
        let current = current();
        assert_strict(current.encode().unwrap(), GenericIndexCurrentRecord::decode);

        let generation = zero_row_generation();
        assert_strict(
            generation.encode().unwrap(),
            GenericIndexGenerationRecord::decode,
        );

        let row = multivalue_row();
        assert_strict(row.encode().unwrap(), GenericIndexRowRecord::decode);

        let reference = GenericIndexGenerationRefRecord {
            kind: GenericIndexReferenceKind::Commit,
            owner_digest: [8; SHA256_BYTES],
            reference_epoch_at_add: ReferenceEpoch::new(3),
        };
        assert_strict(
            reference.encode().unwrap(),
            GenericIndexGenerationRefRecord::decode,
        );

        let receipt = GenericIndexAppendReceiptRecord {
            first_sequence: 2,
            row_count: 2,
            commit_version: CommitVersion::new(7).unwrap(),
            input_digest: [9; SHA256_BYTES],
            resulting_row_count: 4,
            resulting_row_digest: [10; SHA256_BYTES],
        };
        assert_strict(
            receipt.encode().unwrap(),
            GenericIndexAppendReceiptRecord::decode,
        );

        let operation = operation();
        assert_strict(
            operation.encode().unwrap(),
            GenericIndexRegistrationOperationRecord::decode,
        );

        let member = CommitGenericIndexMemberRecord {
            generation_id: GenericIndexGenerationId::from_bytes([11; FIXED_ID_BYTES]),
            capability_digest: [12; SHA256_BYTES],
            row_count: 13,
            row_digest: [14; SHA256_BYTES],
        };
        assert_strict(
            member.encode().unwrap(),
            CommitGenericIndexMemberRecord::decode,
        );
    }

    #[test]
    fn capabilities_and_row_fields_reject_out_of_order_and_duplicates() {
        let mut out_of_order = zero_row_generation();
        out_of_order.capabilities = vec![capability("z"), capability("a")];
        assert_eq!(
            out_of_order.encode(),
            Err(GenericIndexRecordError::CapabilitiesNotCanonical)
        );

        let mut duplicate = zero_row_generation();
        duplicate.capabilities = vec![capability("a"), capability("a")];
        assert_eq!(
            duplicate.encode(),
            Err(GenericIndexRecordError::CapabilitiesNotCanonical)
        );

        let mut operator_duplicate = zero_row_generation();
        operator_duplicate.capabilities[0].operators =
            vec![GenericIndexOperator::Equal, GenericIndexOperator::Equal];
        assert!(matches!(
            operator_duplicate.encode(),
            Err(GenericIndexRecordError::OperatorsNotCanonical { .. })
        ));

        let mut row = multivalue_row();
        row.values.push(row.values[0].clone());
        assert_eq!(
            row.encode(),
            Err(GenericIndexRecordError::RowFieldsNotCanonical)
        );

        fn raw_generation(capabilities: &[GenericIndexFieldCapability]) -> Vec<u8> {
            let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
            put_capabilities(&mut encoded, capabilities).unwrap();
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(&0_u64.to_be_bytes());
            encoded.extend_from_slice(&empty_generic_index_row_digest());
            encoded.extend_from_slice(&1_u64.to_be_bytes());
            encoded.extend_from_slice(&1_u64.to_be_bytes());
            encoded.push(0);
            encoded.push(u8::from(GenericIndexGenerationState::Sealed));
            encoded
        }
        assert_eq!(
            GenericIndexGenerationRecord::decode(&raw_generation(&[
                capability("z"),
                capability("a"),
            ])),
            Err(GenericIndexRecordError::CapabilitiesNotCanonical)
        );
        assert_eq!(
            GenericIndexGenerationRecord::decode(&raw_generation(&[
                capability("a"),
                capability("a"),
            ])),
            Err(GenericIndexRecordError::CapabilitiesNotCanonical)
        );

        fn raw_row(fields: &[&str]) -> Vec<u8> {
            let mut encoded = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION, 0, 1];
            put_u16_count(
                &mut encoded,
                "row_fields",
                fields.len(),
                MAX_GENERIC_INDEX_ROW_FIELDS,
            )
            .unwrap();
            for field_name in fields {
                let field = field(field_name);
                put_short_bytes(
                    &mut encoded,
                    "field_id",
                    field.as_bytes(),
                    MAX_QUERY_FIELD_ID_BYTES,
                )
                .unwrap();
                put_u16_count(
                    &mut encoded,
                    "field_values",
                    1,
                    MAX_GENERIC_INDEX_VALUES_PER_FIELD,
                )
                .unwrap();
                put_generic_scalar(&mut encoded, &field, &QueryScalar::String("x".to_owned()))
                    .unwrap();
            }
            encoded
        }
        assert_eq!(
            GenericIndexRowRecord::decode(&raw_row(&["z", "a"])),
            Err(GenericIndexRecordError::RowFieldsNotCanonical)
        );
        assert_eq!(
            GenericIndexRowRecord::decode(&raw_row(&["a", "a"])),
            Err(GenericIndexRecordError::RowFieldsNotCanonical)
        );
    }

    #[test]
    fn only_generic_builtin_fields_are_reserved() {
        for builtin in GENERIC_BUILTIN_FIELD_IDS {
            let mut generation = zero_row_generation();
            generation.capabilities = vec![capability(builtin)];
            assert!(matches!(
                generation.encode(),
                Err(GenericIndexRecordError::ReservedField { field }) if field == *builtin
            ));
        }
        for builtin in [
            "body_digest_uri",
            "content_type",
            "generation",
            "logical_size",
            "manifest_id",
            "producer",
            "workbench_id",
        ] {
            let generation = GenericIndexGenerationRecord {
                capabilities: vec![capability(builtin)],
                ..zero_row_generation()
            };
            assert_eq!(
                GenericIndexGenerationRecord::decode(&generation.encode().unwrap()).unwrap(),
                generation
            );
        }
    }

    #[test]
    fn zero_row_closures_require_the_canonical_empty_digest() {
        let mut invalid_current = current();
        invalid_current.row_count = 0;
        invalid_current.row_digest = [9; SHA256_BYTES];
        assert!(matches!(
            invalid_current.encode(),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let mut generation = zero_row_generation();
        generation.rolling_row_digest = [9; SHA256_BYTES];
        assert!(matches!(
            generation.encode(),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let mut registration = operation();
        registration.declared_row_count = 0;
        registration.appended_row_count = 0;
        registration.rolling_row_digest = [9; SHA256_BYTES];
        registration.phase = GenericIndexRegistrationPhase::Appending;
        registration.published_pointer_generation = None;
        assert!(matches!(
            registration.encode(),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let member = CommitGenericIndexMemberRecord {
            generation_id: GenericIndexGenerationId::from_bytes([11; FIXED_ID_BYTES]),
            capability_digest: [12; SHA256_BYTES],
            row_count: 0,
            row_digest: [9; SHA256_BYTES],
        };
        assert!(matches!(
            member.encode(),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let canonical_current = GenericIndexCurrentRecord {
            row_count: 0,
            row_digest: empty_generic_index_row_digest(),
            ..current()
        };
        let mut raw_current = canonical_current.encode().unwrap();
        let current_digest_offset = raw_current.len() - SHA256_BYTES;
        raw_current[current_digest_offset..].fill(9);
        assert!(matches!(
            GenericIndexCurrentRecord::decode(&raw_current),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let mut raw_generation = zero_row_generation().encode().unwrap();
        let generation_digest_offset = raw_generation.len() - (SHA256_BYTES + 8 + 8 + 1 + 1);
        raw_generation[generation_digest_offset..generation_digest_offset + SHA256_BYTES].fill(9);
        assert!(matches!(
            GenericIndexGenerationRecord::decode(&raw_generation),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let canonical_registration = GenericIndexRegistrationOperationRecord {
            declared_row_count: 0,
            appended_row_count: 0,
            rolling_row_digest: empty_generic_index_row_digest(),
            phase: GenericIndexRegistrationPhase::Appending,
            published_pointer_generation: None,
            ..operation()
        };
        let mut raw_registration = canonical_registration.encode().unwrap();
        let registration_digest_offset = raw_registration.len() - (SHA256_BYTES + 1 + 1 + 1);
        raw_registration[registration_digest_offset..registration_digest_offset + SHA256_BYTES]
            .fill(9);
        assert!(matches!(
            GenericIndexRegistrationOperationRecord::decode(&raw_registration),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));

        let canonical_member = CommitGenericIndexMemberRecord {
            row_count: 0,
            row_digest: empty_generic_index_row_digest(),
            ..member
        };
        let mut raw_member = canonical_member.encode().unwrap();
        let member_digest_offset = raw_member.len() - SHA256_BYTES;
        raw_member[member_digest_offset..].fill(9);
        assert!(matches!(
            CommitGenericIndexMemberRecord::decode(&raw_member),
            Err(GenericIndexRecordError::InvalidSeal { .. })
        ));
    }

    #[test]
    fn complete_registration_binds_the_exact_pointer_successor() {
        let mut create = operation();
        create.expected_current_generation = None;
        create.published_pointer_generation = Some(Generation::new(1).unwrap());
        create.encode().unwrap();
        for invalid in [2, 7] {
            create.published_pointer_generation = Some(Generation::new(invalid).unwrap());
            assert!(matches!(
                create.encode(),
                Err(GenericIndexRecordError::InvalidRegistrationState { .. })
            ));
        }

        let mut replace = operation();
        replace.expected_current_generation = Some(Generation::new(7).unwrap());
        replace.published_pointer_generation = Some(Generation::new(8).unwrap());
        replace.encode().unwrap();
        for invalid in [7, 9] {
            replace.published_pointer_generation = Some(Generation::new(invalid).unwrap());
            assert!(matches!(
                replace.encode(),
                Err(GenericIndexRecordError::InvalidRegistrationState { .. })
            ));
        }

        replace.expected_current_generation = Some(Generation::new(u64::MAX).unwrap());
        replace.published_pointer_generation = Some(Generation::new(u64::MAX).unwrap());
        assert!(matches!(
            replace.encode(),
            Err(GenericIndexRecordError::InvalidRegistrationState { .. })
        ));

        replace.phase = GenericIndexRegistrationPhase::Appending;
        replace.published_pointer_generation = None;
        assert!(matches!(
            replace.encode(),
            Err(GenericIndexRecordError::InvalidRegistrationState { .. })
        ));

        let mut invalid_progress = operation();
        invalid_progress.phase = GenericIndexRegistrationPhase::Sealing;
        invalid_progress.published_pointer_generation = None;
        invalid_progress.appended_row_count = 1;
        assert!(matches!(
            invalid_progress.encode(),
            Err(GenericIndexRecordError::InvalidRegistrationState { .. })
        ));
        invalid_progress.phase = GenericIndexRegistrationPhase::Preparing;
        assert!(matches!(
            invalid_progress.encode(),
            Err(GenericIndexRecordError::InvalidRegistrationState { .. })
        ));
    }

    #[test]
    fn unknown_discriminants_and_noncanonical_float_fail_closed() {
        let reference = GenericIndexGenerationRefRecord {
            kind: GenericIndexReferenceKind::Commit,
            owner_digest: [1; SHA256_BYTES],
            reference_epoch_at_add: ReferenceEpoch::new(1),
        };
        let mut unknown_reference = reference.encode().unwrap();
        unknown_reference[1] = 0xff;
        assert!(matches!(
            GenericIndexGenerationRefRecord::decode(&unknown_reference),
            Err(GenericIndexRecordError::UnknownDiscriminant { .. })
        ));

        let generation = zero_row_generation();
        let mut unknown_generation_state = generation.encode().unwrap();
        *unknown_generation_state.last_mut().unwrap() = 0xff;
        assert!(matches!(
            GenericIndexGenerationRecord::decode(&unknown_generation_state),
            Err(GenericIndexRecordError::UnknownDiscriminant {
                type_name: "GenericIndexGenerationState",
                ..
            })
        ));

        let mut unknown_operator = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        put_u16_count(
            &mut unknown_operator,
            "capabilities",
            1,
            MAX_GENERIC_INDEX_FIELDS,
        )
        .unwrap();
        put_short_bytes(
            &mut unknown_operator,
            "field_id",
            b"custom",
            MAX_QUERY_FIELD_ID_BYTES,
        )
        .unwrap();
        unknown_operator.extend_from_slice(&[1, 0xff, 0, 0]);
        unknown_operator.extend_from_slice(&0_u64.to_be_bytes());
        unknown_operator.extend_from_slice(&0_u64.to_be_bytes());
        unknown_operator.extend_from_slice(&empty_generic_index_row_digest());
        unknown_operator.extend_from_slice(&1_u64.to_be_bytes());
        unknown_operator.extend_from_slice(&1_u64.to_be_bytes());
        unknown_operator.push(0);
        unknown_operator.push(u8::from(GenericIndexGenerationState::Sealed));
        assert!(matches!(
            GenericIndexGenerationRecord::decode(&unknown_operator),
            Err(GenericIndexRecordError::UnknownDiscriminant {
                type_name: "GenericIndexOperator",
                ..
            })
        ));

        let mut appending = operation();
        appending.phase = GenericIndexRegistrationPhase::Appending;
        appending.published_pointer_generation = None;
        let mut unknown_registration_phase = appending.encode().unwrap();
        let phase_offset = unknown_registration_phase.len() - 3;
        unknown_registration_phase[phase_offset] = 0xff;
        assert!(matches!(
            GenericIndexRegistrationOperationRecord::decode(&unknown_registration_phase),
            Err(GenericIndexRecordError::UnknownDiscriminant {
                type_name: "GenericIndexRegistrationPhase",
                ..
            })
        ));

        let mut unknown_scalar = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION, 0, 1];
        put_u16_count(
            &mut unknown_scalar,
            "row_fields",
            1,
            MAX_GENERIC_INDEX_ROW_FIELDS,
        )
        .unwrap();
        put_short_bytes(
            &mut unknown_scalar,
            "field_id",
            b"custom",
            MAX_QUERY_FIELD_ID_BYTES,
        )
        .unwrap();
        put_u16_count(
            &mut unknown_scalar,
            "field_values",
            1,
            MAX_GENERIC_INDEX_VALUES_PER_FIELD,
        )
        .unwrap();
        unknown_scalar.push(0xff);
        assert!(matches!(
            GenericIndexRowRecord::decode(&unknown_scalar),
            Err(GenericIndexRecordError::UnknownDiscriminant {
                type_name: "GenericIndexScalar",
                ..
            })
        ));

        for unknown_binding in [0, 4, 0xff] {
            assert!(matches!(
                GenericIndexRowRecord::decode(&[
                    GENERIC_INDEX_VALUE_FORMAT_VERSION,
                    0,
                    unknown_binding,
                ]),
                Err(GenericIndexRecordError::UnknownDiscriminant {
                    type_name: "GenericIndexRowBinding",
                    ..
                })
            ));
        }

        let mut row = multivalue_row();
        row.values[0].values = vec![QueryScalar::Float(FiniteFloat::new(1.0).unwrap())];
        let mut noncanonical = row.encode().unwrap();
        let bits = (-0.0_f64).to_bits().to_be_bytes();
        let position = noncanonical
            .windows(8)
            .position(|window| window == 1.0_f64.to_bits().to_be_bytes())
            .unwrap();
        noncanonical[position..position + 8].copy_from_slice(&bits);
        assert_eq!(
            GenericIndexRowRecord::decode(&noncanonical),
            Err(GenericIndexRecordError::NonCanonicalFloatZero)
        );
    }

    #[test]
    fn append_receipt_and_batch_range_overflow_fail_closed() {
        let overflow = GenericIndexAppendReceiptRecord {
            first_sequence: u64::MAX,
            row_count: 1,
            commit_version: CommitVersion::new(1).unwrap(),
            input_digest: [1; SHA256_BYTES],
            resulting_row_count: 0,
            resulting_row_digest: [2; SHA256_BYTES],
        };
        assert_eq!(
            overflow.encode(),
            Err(GenericIndexRecordError::RangeOverflow {
                field: "append_sequence"
            })
        );
        assert_eq!(
            generic_index_append_batch_digest(u64::MAX, &[multivalue_row()]),
            Err(GenericIndexRecordError::RangeOverflow {
                field: "append_sequence"
            })
        );
        assert_eq!(
            generic_index_append_input_digest(u64::MAX, &[multivalue_row()]),
            Err(GenericIndexRecordError::RangeOverflow {
                field: "append_sequence"
            })
        );

        let mut raw = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        raw.extend_from_slice(&u64::MAX.to_be_bytes());
        raw.extend_from_slice(&1_u32.to_be_bytes());
        raw.extend_from_slice(&1_u64.to_be_bytes());
        raw.extend_from_slice(&[1; SHA256_BYTES]);
        raw.extend_from_slice(&0_u64.to_be_bytes());
        raw.extend_from_slice(&[2; SHA256_BYTES]);
        assert_eq!(
            GenericIndexAppendReceiptRecord::decode(&raw),
            Err(GenericIndexRecordError::RangeOverflow {
                field: "append_sequence"
            })
        );

        let mut stale_transition = operation();
        stale_transition.last_transition_version =
            CommitVersion::new(stale_transition.source_read_version.get()).unwrap();
        assert!(matches!(
            stale_transition.encode(),
            Err(GenericIndexRecordError::InvalidRegistrationState { .. })
        ));

        let oversized_row = GenericIndexRowRecord {
            relative_path: None,
            binding: GenericIndexRowBinding::Directory,
            values: vec![GenericIndexFieldValues {
                field: field("large"),
                values: vec![QueryScalar::String("x".repeat(MAX_GENERIC_INDEX_ROW_BYTES))],
            }],
        };
        assert!(matches!(
            oversized_row.encode(),
            Err(GenericIndexRecordError::LengthLimit {
                field: "generic_index_row",
                ..
            })
        ));
    }

    #[test]
    fn bounded_counts_fail_closed_before_allocation() {
        let mut raw_capability_count = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        raw_capability_count
            .extend_from_slice(&((MAX_GENERIC_INDEX_FIELDS + 1) as u16).to_be_bytes());
        assert!(matches!(
            GenericIndexGenerationRecord::decode(&raw_capability_count),
            Err(GenericIndexRecordError::CountLimit {
                field: "capabilities",
                ..
            })
        ));

        let mut raw_operator_count = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION];
        raw_operator_count.extend_from_slice(&1_u16.to_be_bytes());
        put_short_bytes(
            &mut raw_operator_count,
            "field_id",
            b"custom",
            MAX_QUERY_FIELD_ID_BYTES,
        )
        .unwrap();
        raw_operator_count.push(13);
        assert!(matches!(
            GenericIndexGenerationRecord::decode(&raw_operator_count),
            Err(GenericIndexRecordError::CountLimit {
                field: "operators",
                ..
            })
        ));

        let mut raw_row_field_count = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION, 0, 1];
        raw_row_field_count
            .extend_from_slice(&((MAX_GENERIC_INDEX_ROW_FIELDS + 1) as u16).to_be_bytes());
        assert!(matches!(
            GenericIndexRowRecord::decode(&raw_row_field_count),
            Err(GenericIndexRecordError::CountLimit {
                field: "row_fields",
                ..
            })
        ));

        let mut raw_value_count = vec![GENERIC_INDEX_VALUE_FORMAT_VERSION, 0, 1];
        raw_value_count.extend_from_slice(&1_u16.to_be_bytes());
        put_short_bytes(
            &mut raw_value_count,
            "field_id",
            b"custom",
            MAX_QUERY_FIELD_ID_BYTES,
        )
        .unwrap();
        raw_value_count
            .extend_from_slice(&((MAX_GENERIC_INDEX_VALUES_PER_FIELD + 1) as u16).to_be_bytes());
        assert!(matches!(
            GenericIndexRowRecord::decode(&raw_value_count),
            Err(GenericIndexRecordError::CountLimit {
                field: "field_values",
                ..
            })
        ));

        let excessive_capabilities = GenericIndexGenerationRecord {
            capabilities: (0..=MAX_GENERIC_INDEX_FIELDS)
                .map(|index| capability(&format!("custom.{index:03}")))
                .collect(),
            ..zero_row_generation()
        };
        assert!(matches!(
            excessive_capabilities.encode(),
            Err(GenericIndexRecordError::CountLimit {
                field: "capabilities",
                ..
            })
        ));

        let excessive_values = GenericIndexRowRecord {
            relative_path: None,
            binding: GenericIndexRowBinding::Directory,
            values: vec![GenericIndexFieldValues {
                field: field("custom"),
                values: vec![QueryScalar::Unsigned(0); MAX_GENERIC_INDEX_VALUES_PER_FIELD + 1],
            }],
        };
        assert!(matches!(
            excessive_values.encode(),
            Err(GenericIndexRecordError::CountLimit {
                field: "field_values",
                ..
            })
        ));

        let mut oversized_terminal = operation();
        oversized_terminal.phase = GenericIndexRegistrationPhase::Quarantined;
        oversized_terminal.published_pointer_generation = None;
        oversized_terminal.terminal_error =
            Some("x".repeat(MAX_GENERIC_INDEX_TERMINAL_ERROR_BYTES + 1));
        assert!(matches!(
            oversized_terminal.encode(),
            Err(GenericIndexRecordError::LengthLimit {
                field: "terminal_error",
                ..
            })
        ));
    }

    #[test]
    fn digest_and_generation_seal_bind_order_capabilities_and_rows() {
        let row = multivalue_row();
        let first = generic_index_row_digest(0, &row).unwrap();
        let shifted = generic_index_row_digest(1, &row).unwrap();
        assert_ne!(first, shifted);
        let input = generic_index_append_input_digest(0, std::slice::from_ref(&row)).unwrap();
        let mut rebound = row.clone();
        rebound.binding = GenericIndexRowBinding::Artifact(GenericIndexArtifactBinding {
            artifact_revision_id: ArtifactRevisionId::from_bytes([99; FIXED_ID_BYTES]),
            path_generation: Generation::new(99).unwrap(),
        });
        assert_eq!(
            generic_index_append_input_digest(0, &[rebound]).unwrap(),
            input
        );
        assert_ne!(
            generic_index_append_input_digest(1, std::slice::from_ref(&row)).unwrap(),
            input
        );
        let mut changed_input = row.clone();
        changed_input.values[0]
            .values
            .push(QueryScalar::String("extra".to_owned()));
        assert_ne!(
            generic_index_append_input_digest(0, &[changed_input]).unwrap(),
            input
        );
        let rolling =
            advance_generic_index_row_rolling_digest(empty_generic_index_row_digest(), first);
        let capabilities = vec![capability("run.score")];
        let capability_digest = generic_index_capability_digest(&capabilities).unwrap();
        let generation = GenericIndexGenerationRecord {
            capabilities,
            declared_row_count: 1,
            appended_row_count: 1,
            rolling_row_digest: rolling,
            reference_count: 1,
            reference_epoch: ReferenceEpoch::new(1),
            last_zero_reference_version: None,
            state: GenericIndexGenerationState::Sealed,
        };
        verify_generic_index_generation_seal(&generation, capability_digest, 1, rolling).unwrap();
        assert!(
            verify_generic_index_generation_seal(&generation, capability_digest, 1, shifted,)
                .is_err()
        );

        let mut retired = generation.clone();
        retired.state = GenericIndexGenerationState::Retired;
        retired.declared_row_count = 5;
        retired.appended_row_count = 1;
        retired.reference_count = 0;
        retired.last_zero_reference_version = Some(CommitVersion::new(9).unwrap());
        assert_eq!(
            GenericIndexGenerationRecord::decode(&retired.encode().unwrap()).unwrap(),
            retired
        );
        assert!(
            verify_generic_index_generation_seal(&retired, capability_digest, 1, rolling,).is_err()
        );

        let operation = OperationId::from_bytes([8; FIXED_ID_BYTES]);
        let commit = CommitId::from_bytes([8; SHA256_BYTES]);
        let owners = [
            generic_index_registration_owner_digest(operation),
            generic_index_build_commit_owner_digest(operation),
            generic_index_restore_owner_digest(operation),
            generic_index_commit_owner_digest(commit),
        ];
        assert_eq!(
            owners
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            owners.len()
        );
    }
}
