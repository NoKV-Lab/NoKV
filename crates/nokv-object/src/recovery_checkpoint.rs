/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use nokv_types::{LogicalShardId, ObjectNamespaceId};

use crate::chunked_immutable_blob::{
    derive_chunk_key, derive_manifest_key, derive_object_keys, plan_chunked_blob,
    read_chunked_blob, restore_chunked_blob_plan, validate_chunked_blob_receipt,
    write_chunked_blob_from_plan, ChunkedBlobBounds, ChunkedBlobError, ChunkedBlobKeyspace,
    ChunkedBlobPlan, ChunkedBlobReceipt,
};
use crate::{ArtifactObjectStore, ImmutableCreateOutcome, ObjectError, ObjectKey};

#[cfg(test)]
use crate::digest::{hex, sha256};

const MANIFEST_MAGIC: &[u8; 8] = b"NOKVRCPM";
const MANIFEST_VERSION: u16 = 1;
const RECEIPT_MAGIC: &[u8; 8] = b"NOKVRCPR";
const RECEIPT_VERSION: u16 = 1;
const PLAN_MAGIC: &[u8; 8] = b"NOKVRCPP";
const PLAN_VERSION: u16 = 1;
const MANIFEST_HEADER_BYTES: usize = 8 + 2 + 16 + 16 + 8 + 32 + 8 + 32;
const FIXED_MANIFEST_BYTES: usize = MANIFEST_HEADER_BYTES + 4 + 4;
const CHUNK_DESCRIPTOR_BYTES: usize = 4 + 4 + 32;
const RECEIPT_BYTES: usize = 8 + 2 + 16 + 16 + 8 + 32 + 8 + 32 + 8 + 32 + 4 + 4;
const PLAN_FIXED_BYTES: usize = 8 + 2 + 4 + RECEIPT_BYTES + 8;
const RECOVERY_CHECKPOINT_KEY_PREFIX: &str = "nokv/recovery/checkpoints/v1";

/// Default size of one recovery checkpoint single-PUT object.
pub const DEFAULT_RECOVERY_CHECKPOINT_CHUNK_SIZE: usize = 4 * 1024 * 1024;
/// Maximum number of chunks represented by one canonical manifest.
pub const MAX_RECOVERY_CHECKPOINT_CHUNKS: usize = 65_536;
/// Hard upper bound for one encoded storage checkpoint envelope.
pub const MAX_RECOVERY_CHECKPOINT_ENVELOPE_BYTES: usize = 513 * 1024 * 1024;

/// Stable recovery-chain position captured by a whole-store checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpointBoundary {
    recovery_lsn: u64,
    chain_digest: [u8; 32],
}

/// Durable identity a recovery blob must bind independently of provider coordinates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpointIdentity {
    object_namespace: ObjectNamespaceId,
    logical_shard: LogicalShardId,
    boundary: RecoveryCheckpointBoundary,
}

/// Content-addressed receipt for one immutable recovery checkpoint blob.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpointBlobReceipt {
    identity: RecoveryCheckpointIdentity,
    envelope_len: u64,
    envelope_digest: [u8; 32],
    manifest_len: u64,
    manifest_digest: [u8; 32],
    chunk_size: u32,
    chunk_count: u32,
}

/// Pure, durable plan exposing every immutable key before the first object write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpointBlobPlan {
    receipt: RecoveryCheckpointBlobReceipt,
    core: ChunkedBlobPlan,
}

/// Confirmed outcomes from writing every chunk and the final manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpointWrite {
    receipt: RecoveryCheckpointBlobReceipt,
    chunks_created: u32,
    chunks_replayed: u32,
    manifest_outcome: ImmutableCreateOutcome,
}

/// Fully verified encoded checkpoint envelope loaded from immutable objects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryCheckpointBlob {
    receipt: RecoveryCheckpointBlobReceipt,
    envelope: Vec<u8>,
}

/// Fail-closed planning, publication, or verification error for a recovery blob.
#[derive(PartialEq, Eq)]
pub enum RecoveryCheckpointError {
    EmptyEnvelope,
    EnvelopeTooLarge { actual: usize, maximum: usize },
    InvalidChunkSize,
    TooManyChunks { actual: usize, maximum: usize },
    EnvelopeMismatch,
    InvalidPlan(&'static str),
    ObjectNamespaceRequired,
    ForeignNamespace,
    ForeignShard,
    ForeignBoundary,
    ProviderAdmissionRequired,
    ProviderObjectTooLarge { requested: usize, admitted: usize },
    InvalidManifest(&'static str),
    InvalidReceipt(&'static str),
    CreateOutcomeUnknown { key: ObjectKey },
    Object(ObjectError),
}

impl RecoveryCheckpointBoundary {
    pub const fn new(recovery_lsn: u64, chain_digest: [u8; 32]) -> Self {
        Self {
            recovery_lsn,
            chain_digest,
        }
    }

    pub const fn recovery_lsn(self) -> u64 {
        self.recovery_lsn
    }

    pub const fn chain_digest(self) -> [u8; 32] {
        self.chain_digest
    }
}

impl RecoveryCheckpointIdentity {
    pub const fn new(
        object_namespace: ObjectNamespaceId,
        logical_shard: LogicalShardId,
        boundary: RecoveryCheckpointBoundary,
    ) -> Self {
        Self {
            object_namespace,
            logical_shard,
            boundary,
        }
    }

    pub const fn object_namespace(self) -> ObjectNamespaceId {
        self.object_namespace
    }

    pub const fn logical_shard(self) -> LogicalShardId {
        self.logical_shard
    }

    pub const fn boundary(self) -> RecoveryCheckpointBoundary {
        self.boundary
    }
}

impl RecoveryCheckpointWrite {
    pub fn receipt(&self) -> &RecoveryCheckpointBlobReceipt {
        &self.receipt
    }

    pub const fn chunks_created(&self) -> u32 {
        self.chunks_created
    }

    pub const fn chunks_replayed(&self) -> u32 {
        self.chunks_replayed
    }

    pub const fn manifest_outcome(&self) -> ImmutableCreateOutcome {
        self.manifest_outcome
    }
}

impl RecoveryCheckpointBlob {
    pub fn receipt(&self) -> &RecoveryCheckpointBlobReceipt {
        &self.receipt
    }

    pub fn envelope(&self) -> &[u8] {
        &self.envelope
    }
}

impl RecoveryCheckpointBlobReceipt {
    fn from_core(identity: RecoveryCheckpointIdentity, core: ChunkedBlobReceipt) -> Self {
        Self {
            identity,
            envelope_len: core.payload_len,
            envelope_digest: core.payload_digest,
            manifest_len: core.manifest_len,
            manifest_digest: core.manifest_digest,
            chunk_size: core.chunk_size,
            chunk_count: core.chunk_count,
        }
    }

    fn core_receipt(&self) -> ChunkedBlobReceipt {
        ChunkedBlobReceipt {
            payload_len: self.envelope_len,
            payload_digest: self.envelope_digest,
            manifest_len: self.manifest_len,
            manifest_digest: self.manifest_digest,
            chunk_size: self.chunk_size,
            chunk_count: self.chunk_count,
        }
    }

    pub const fn identity(&self) -> RecoveryCheckpointIdentity {
        self.identity
    }

    pub const fn envelope_len(&self) -> u64 {
        self.envelope_len
    }

    pub const fn envelope_digest(&self) -> [u8; 32] {
        self.envelope_digest
    }

    pub const fn manifest_len(&self) -> u64 {
        self.manifest_len
    }

    pub const fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }

    pub const fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    pub const fn chunk_count(&self) -> u32 {
        self.chunk_count
    }

    /// Encode the durable receipt without provider coordinates or process-local handles.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(RECEIPT_BYTES);
        bytes.extend_from_slice(RECEIPT_MAGIC);
        bytes.extend_from_slice(&RECEIPT_VERSION.to_be_bytes());
        bytes.extend_from_slice(self.identity.object_namespace.as_bytes());
        bytes.extend_from_slice(self.identity.logical_shard.as_bytes());
        bytes.extend_from_slice(&self.identity.boundary.recovery_lsn.to_be_bytes());
        bytes.extend_from_slice(&self.identity.boundary.chain_digest);
        bytes.extend_from_slice(&self.envelope_len.to_be_bytes());
        bytes.extend_from_slice(&self.envelope_digest);
        bytes.extend_from_slice(&self.manifest_len.to_be_bytes());
        bytes.extend_from_slice(&self.manifest_digest);
        bytes.extend_from_slice(&self.chunk_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_be_bytes());
        bytes
    }

    /// Decode and structurally validate one exact canonical receipt.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryCheckpointError> {
        if bytes.len() != RECEIPT_BYTES || !bytes.starts_with(RECEIPT_MAGIC) {
            return Err(RecoveryCheckpointError::InvalidReceipt(
                "wrong length or magic",
            ));
        }
        let mut offset = RECEIPT_MAGIC.len();
        if take_receipt_u16(bytes, &mut offset)? != RECEIPT_VERSION {
            return Err(RecoveryCheckpointError::InvalidReceipt(
                "unsupported receipt version",
            ));
        }
        let object_namespace =
            ObjectNamespaceId::from_bytes(take_receipt_array(bytes, &mut offset)?);
        let logical_shard = LogicalShardId::from_bytes(take_receipt_array(bytes, &mut offset)?);
        let recovery_lsn = take_receipt_u64(bytes, &mut offset)?;
        let chain_digest = take_receipt_array(bytes, &mut offset)?;
        let receipt = Self {
            identity: RecoveryCheckpointIdentity::new(
                object_namespace,
                logical_shard,
                RecoveryCheckpointBoundary::new(recovery_lsn, chain_digest),
            ),
            envelope_len: take_receipt_u64(bytes, &mut offset)?,
            envelope_digest: take_receipt_array(bytes, &mut offset)?,
            manifest_len: take_receipt_u64(bytes, &mut offset)?,
            manifest_digest: take_receipt_array(bytes, &mut offset)?,
            chunk_size: take_receipt_u32(bytes, &mut offset)?,
            chunk_count: take_receipt_u32(bytes, &mut offset)?,
        };
        if offset != bytes.len() {
            return Err(RecoveryCheckpointError::InvalidReceipt(
                "trailing receipt bytes",
            ));
        }
        validate_receipt(&receipt)?;
        Ok(receipt)
    }

    pub fn manifest_key(&self) -> Result<ObjectKey, RecoveryCheckpointError> {
        derive_manifest_key(checkpoint_keyspace(self.identity), self.manifest_digest)
            .map_err(map_core_error)
    }

    pub fn chunk_key(&self, index: u32) -> Result<ObjectKey, RecoveryCheckpointError> {
        if index >= self.chunk_count {
            return Err(RecoveryCheckpointError::InvalidReceipt(
                "chunk index is outside receipt",
            ));
        }
        derive_chunk_key(
            checkpoint_keyspace(self.identity),
            self.manifest_digest,
            index,
        )
        .map_err(map_core_error)
    }

    /// Rederive every chunks-first, manifest-last key from this receipt.
    pub fn object_keys(&self) -> Result<Vec<ObjectKey>, RecoveryCheckpointError> {
        derive_object_keys(
            checkpoint_keyspace(self.identity),
            self.core_receipt(),
            checkpoint_bounds(),
        )
        .map_err(map_core_error)
    }
}

impl RecoveryCheckpointBlobPlan {
    pub fn receipt(&self) -> &RecoveryCheckpointBlobReceipt {
        &self.receipt
    }

    /// Exact chunks-first, manifest-last object order for intent and cleanup.
    pub fn object_keys(&self) -> &[ObjectKey] {
        self.core.object_keys()
    }

    /// Every key that may exist after a failed execution of this plan.
    pub fn cleanup_keys(&self) -> &[ObjectKey] {
        self.object_keys()
    }

    /// Encode the plan, including its strict typed manifest and ordered descriptors.
    pub fn encode(&self) -> Vec<u8> {
        let receipt = self.receipt.encode();
        let manifest = self.core.manifest_bytes();
        let mut bytes = Vec::with_capacity(PLAN_FIXED_BYTES + manifest.len());
        bytes.extend_from_slice(PLAN_MAGIC);
        bytes.extend_from_slice(&PLAN_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(receipt.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&receipt);
        bytes.extend_from_slice(&(manifest.len() as u64).to_be_bytes());
        bytes.extend_from_slice(manifest);
        bytes
    }

    /// Decode one exact plan and rederive its complete object key set.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryCheckpointError> {
        if bytes.len() < PLAN_FIXED_BYTES || !bytes.starts_with(PLAN_MAGIC) {
            return Err(RecoveryCheckpointError::InvalidPlan(
                "wrong length or plan magic",
            ));
        }
        let mut offset = PLAN_MAGIC.len();
        if take_plan_u16(bytes, &mut offset)? != PLAN_VERSION {
            return Err(RecoveryCheckpointError::InvalidPlan(
                "unsupported plan version",
            ));
        }
        let receipt_len = take_plan_u32(bytes, &mut offset)? as usize;
        if receipt_len != RECEIPT_BYTES {
            return Err(RecoveryCheckpointError::InvalidPlan(
                "receipt length is not canonical",
            ));
        }
        let receipt_end =
            offset
                .checked_add(receipt_len)
                .ok_or(RecoveryCheckpointError::InvalidPlan(
                    "receipt length overflows",
                ))?;
        let receipt = RecoveryCheckpointBlobReceipt::decode(
            bytes
                .get(offset..receipt_end)
                .ok_or(RecoveryCheckpointError::InvalidPlan("receipt is truncated"))?,
        )?;
        offset = receipt_end;
        let manifest_len = usize::try_from(take_plan_u64(bytes, &mut offset)?)
            .map_err(|_| RecoveryCheckpointError::InvalidPlan("manifest length overflows"))?;
        let manifest_end =
            offset
                .checked_add(manifest_len)
                .ok_or(RecoveryCheckpointError::InvalidPlan(
                    "manifest length overflows",
                ))?;
        if manifest_end != bytes.len() || manifest_len as u64 != receipt.manifest_len {
            return Err(RecoveryCheckpointError::InvalidPlan(
                "manifest length does not match plan",
            ));
        }
        let header = encode_manifest_header(
            receipt.identity,
            receipt.envelope_len,
            receipt.envelope_digest,
        );
        let core = restore_chunked_blob_plan(
            checkpoint_keyspace(receipt.identity),
            checkpoint_bounds(),
            header,
            receipt.core_receipt(),
            bytes[offset..manifest_end].to_vec(),
        )
        .map_err(map_core_error)?;
        Ok(Self { receipt, core })
    }
}

/// Build a deterministic zero-I/O plan that exposes receipt and cleanup keys.
pub fn plan_recovery_checkpoint_blob(
    identity: RecoveryCheckpointIdentity,
    envelope: &[u8],
    chunk_size: usize,
) -> Result<RecoveryCheckpointBlobPlan, RecoveryCheckpointError> {
    let core = plan_chunked_blob(
        checkpoint_keyspace(identity),
        checkpoint_bounds(),
        envelope,
        chunk_size,
        |envelope_len, envelope_digest| {
            encode_manifest_header(identity, envelope_len, envelope_digest)
        },
    )
    .map_err(map_core_error)?;
    let receipt = RecoveryCheckpointBlobReceipt::from_core(identity, core.receipt());
    Ok(RecoveryCheckpointBlobPlan { receipt, core })
}

/// Execute a plan only after the coordinator durably persists its receipt or intent.
///
/// The manifest is always created last. Persisting the full plan is optional because the strict
/// receipt can rederive every possible object key needed for retry or cleanup.
pub fn write_recovery_checkpoint_blob_from_plan(
    store: &dyn ArtifactObjectStore,
    plan: &RecoveryCheckpointBlobPlan,
    envelope: &[u8],
) -> Result<RecoveryCheckpointWrite, RecoveryCheckpointError> {
    let written =
        write_chunked_blob_from_plan(store, &plan.core, envelope).map_err(map_core_error)?;
    Ok(RecoveryCheckpointWrite {
        receipt: plan.receipt.clone(),
        chunks_created: written.chunks_created,
        chunks_replayed: written.chunks_replayed,
        manifest_outcome: written.manifest_outcome,
    })
}

/// Read and verify a manifest and every ordered chunk using full object reads.
pub fn read_recovery_checkpoint_blob(
    store: &dyn ArtifactObjectStore,
    expected: RecoveryCheckpointIdentity,
    receipt: &RecoveryCheckpointBlobReceipt,
) -> Result<RecoveryCheckpointBlob, RecoveryCheckpointError> {
    validate_expected_receipt(expected, receipt)?;
    validate_receipt(receipt)?;
    let header = encode_manifest_header(
        receipt.identity,
        receipt.envelope_len,
        receipt.envelope_digest,
    );
    let envelope = read_chunked_blob(
        store,
        checkpoint_keyspace(expected),
        checkpoint_bounds(),
        header,
        receipt.core_receipt(),
    )
    .map_err(map_core_error)?;
    Ok(RecoveryCheckpointBlob {
        receipt: receipt.clone(),
        envelope,
    })
}

fn validate_expected_receipt(
    expected: RecoveryCheckpointIdentity,
    receipt: &RecoveryCheckpointBlobReceipt,
) -> Result<(), RecoveryCheckpointError> {
    if receipt.identity.object_namespace != expected.object_namespace {
        return Err(RecoveryCheckpointError::ForeignNamespace);
    }
    if receipt.identity.logical_shard != expected.logical_shard {
        return Err(RecoveryCheckpointError::ForeignShard);
    }
    if receipt.identity.boundary != expected.boundary {
        return Err(RecoveryCheckpointError::ForeignBoundary);
    }
    Ok(())
}

fn validate_receipt(
    receipt: &RecoveryCheckpointBlobReceipt,
) -> Result<(), RecoveryCheckpointError> {
    validate_chunked_blob_receipt(receipt.core_receipt(), checkpoint_bounds())
        .map_err(|_| RecoveryCheckpointError::InvalidReceipt("invalid chunk layout"))?;
    let expected_manifest_len = FIXED_MANIFEST_BYTES
        .checked_add(receipt.chunk_count as usize * CHUNK_DESCRIPTOR_BYTES)
        .ok_or(RecoveryCheckpointError::InvalidReceipt(
            "manifest length overflows",
        ))?;
    if receipt.manifest_len != expected_manifest_len as u64 {
        return Err(RecoveryCheckpointError::InvalidReceipt(
            "manifest length does not match chunk count",
        ));
    }
    Ok(())
}

fn checkpoint_bounds() -> ChunkedBlobBounds {
    ChunkedBlobBounds {
        max_payload_bytes: MAX_RECOVERY_CHECKPOINT_ENVELOPE_BYTES,
        max_chunks: MAX_RECOVERY_CHECKPOINT_CHUNKS,
    }
}

fn checkpoint_keyspace(identity: RecoveryCheckpointIdentity) -> ChunkedBlobKeyspace {
    ChunkedBlobKeyspace {
        prefix: RECOVERY_CHECKPOINT_KEY_PREFIX,
        object_namespace: identity.object_namespace,
        logical_shard: identity.logical_shard,
    }
}

fn encode_manifest_header(
    identity: RecoveryCheckpointIdentity,
    envelope_len: u64,
    envelope_digest: [u8; 32],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MANIFEST_HEADER_BYTES);
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_VERSION.to_be_bytes());
    bytes.extend_from_slice(identity.object_namespace.as_bytes());
    bytes.extend_from_slice(identity.logical_shard.as_bytes());
    bytes.extend_from_slice(&identity.boundary.recovery_lsn.to_be_bytes());
    bytes.extend_from_slice(&identity.boundary.chain_digest);
    bytes.extend_from_slice(&envelope_len.to_be_bytes());
    bytes.extend_from_slice(&envelope_digest);
    bytes
}

fn map_core_error(error: ChunkedBlobError) -> RecoveryCheckpointError {
    match error {
        ChunkedBlobError::EmptyPayload => RecoveryCheckpointError::EmptyEnvelope,
        ChunkedBlobError::PayloadTooLarge { actual, maximum } => {
            RecoveryCheckpointError::EnvelopeTooLarge { actual, maximum }
        }
        ChunkedBlobError::InvalidChunkSize => RecoveryCheckpointError::InvalidChunkSize,
        ChunkedBlobError::TooManyChunks { actual, maximum } => {
            RecoveryCheckpointError::TooManyChunks { actual, maximum }
        }
        ChunkedBlobError::PayloadMismatch => RecoveryCheckpointError::EnvelopeMismatch,
        ChunkedBlobError::InvalidPlan(reason) => RecoveryCheckpointError::InvalidPlan(reason),
        ChunkedBlobError::ObjectNamespaceRequired => {
            RecoveryCheckpointError::ObjectNamespaceRequired
        }
        ChunkedBlobError::ForeignNamespace => RecoveryCheckpointError::ForeignNamespace,
        ChunkedBlobError::ProviderAdmissionRequired => {
            RecoveryCheckpointError::ProviderAdmissionRequired
        }
        ChunkedBlobError::ProviderObjectTooLarge {
            requested,
            admitted,
        } => RecoveryCheckpointError::ProviderObjectTooLarge {
            requested,
            admitted,
        },
        ChunkedBlobError::InvalidManifest(reason) => {
            RecoveryCheckpointError::InvalidManifest(reason)
        }
        ChunkedBlobError::CreateOutcomeUnknown { key } => {
            RecoveryCheckpointError::CreateOutcomeUnknown { key }
        }
        ChunkedBlobError::Object(error) => RecoveryCheckpointError::Object(error),
    }
}

fn take_plan_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], RecoveryCheckpointError> {
    let end = offset
        .checked_add(N)
        .ok_or(RecoveryCheckpointError::InvalidPlan(
            "plan offset overflows",
        ))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or(RecoveryCheckpointError::InvalidPlan("plan is truncated"))?;
    *offset = end;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn take_plan_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, RecoveryCheckpointError> {
    Ok(u16::from_be_bytes(take_plan_array(bytes, offset)?))
}

fn take_plan_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, RecoveryCheckpointError> {
    Ok(u32::from_be_bytes(take_plan_array(bytes, offset)?))
}

fn take_plan_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, RecoveryCheckpointError> {
    Ok(u64::from_be_bytes(take_plan_array(bytes, offset)?))
}

fn take_receipt_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], RecoveryCheckpointError> {
    let end = offset
        .checked_add(N)
        .ok_or(RecoveryCheckpointError::InvalidReceipt(
            "receipt offset overflows",
        ))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or(RecoveryCheckpointError::InvalidReceipt(
            "receipt is truncated",
        ))?;
    *offset = end;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn take_receipt_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, RecoveryCheckpointError> {
    Ok(u16::from_be_bytes(take_receipt_array(bytes, offset)?))
}

fn take_receipt_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, RecoveryCheckpointError> {
    Ok(u32::from_be_bytes(take_receipt_array(bytes, offset)?))
}

fn take_receipt_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, RecoveryCheckpointError> {
    Ok(u64::from_be_bytes(take_receipt_array(bytes, offset)?))
}

impl From<ObjectError> for RecoveryCheckpointError {
    fn from(error: ObjectError) -> Self {
        Self::Object(error)
    }
}

impl fmt::Display for RecoveryCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyEnvelope => "recovery checkpoint envelope is empty",
            Self::EnvelopeTooLarge { .. } => "recovery checkpoint envelope is too large",
            Self::InvalidChunkSize => "invalid recovery checkpoint chunk size",
            Self::TooManyChunks { .. } => "recovery checkpoint has too many chunks",
            Self::EnvelopeMismatch => "recovery checkpoint envelope does not match its plan",
            Self::InvalidPlan(_) => "invalid recovery checkpoint plan",
            Self::ObjectNamespaceRequired => "recovery checkpoint requires a namespace-bound store",
            Self::ForeignNamespace => "recovery checkpoint belongs to another object namespace",
            Self::ForeignShard => "recovery checkpoint belongs to another logical shard",
            Self::ForeignBoundary => "recovery checkpoint belongs to another recovery boundary",
            Self::ProviderAdmissionRequired => "recovery checkpoint provider is not admitted",
            Self::ProviderObjectTooLarge { .. } => {
                "recovery checkpoint object exceeds provider admission"
            }
            Self::InvalidManifest(_) => "invalid recovery checkpoint manifest",
            Self::InvalidReceipt(_) => "invalid recovery checkpoint receipt",
            Self::CreateOutcomeUnknown { .. } => "recovery checkpoint create outcome is unknown",
            Self::Object(_) => "recovery checkpoint object operation failed",
        })
    }
}

impl fmt::Debug for RecoveryCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RecoveryCheckpointError")
            .field(&self.to_string())
            .finish()
    }
}

impl std::error::Error for RecoveryCheckpointError {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;
    use crate::{
        ArtifactStoreCapabilities, ObjectDeleteOutcome, ObjectInfo, ObjectRange,
        ProviderAdmissionReceipt, ProviderHandleIdentity,
    };

    #[derive(Clone, Copy)]
    enum AmbiguousCreate {
        Never,
        AfterApply,
        BeforeApply,
    }

    struct TestStore {
        namespace: Option<ObjectNamespaceId>,
        handle: ProviderHandleIdentity,
        admission: Option<ProviderAdmissionReceipt>,
        ambiguous_create: AmbiguousCreate,
        fail_before_apply_at: Option<usize>,
        state: Mutex<TestStoreState>,
    }

    #[derive(Default)]
    struct TestStoreState {
        objects: BTreeMap<ObjectKey, Vec<u8>>,
        creates: Vec<ObjectKey>,
        reads: Vec<ObjectKey>,
        head_calls: usize,
    }

    impl TestStore {
        fn admitted(namespace: ObjectNamespaceId, max_object_bytes: usize) -> Self {
            Self::with_ambiguity(namespace, max_object_bytes, AmbiguousCreate::Never)
        }

        fn with_ambiguity(
            namespace: ObjectNamespaceId,
            max_object_bytes: usize,
            ambiguous_create: AmbiguousCreate,
        ) -> Self {
            let handle = ProviderHandleIdentity::new();
            Self {
                namespace: Some(namespace),
                handle,
                admission: Some(ProviderAdmissionReceipt::trusted_in_process(
                    handle,
                    max_object_bytes,
                )),
                ambiguous_create,
                fail_before_apply_at: None,
                state: Mutex::new(TestStoreState::default()),
            }
        }

        fn fail_before_apply_at(
            namespace: ObjectNamespaceId,
            max_object_bytes: usize,
            create_number: usize,
        ) -> Self {
            let mut store = Self::admitted(namespace, max_object_bytes);
            store.fail_before_apply_at = Some(create_number);
            store
        }

        fn create_keys(&self) -> Vec<ObjectKey> {
            self.state.lock().unwrap().creates.clone()
        }

        fn read_count(&self) -> usize {
            self.state.lock().unwrap().reads.len()
        }

        fn head_calls(&self) -> usize {
            self.state.lock().unwrap().head_calls
        }

        fn resident_object_count(&self) -> usize {
            self.state.lock().unwrap().objects.len()
        }

        fn resident_keys(&self) -> Vec<ObjectKey> {
            self.state.lock().unwrap().objects.keys().cloned().collect()
        }

        fn replace(&self, key: &ObjectKey, bytes: Vec<u8>) {
            self.state
                .lock()
                .unwrap()
                .objects
                .insert(key.clone(), bytes);
        }

        fn remove(&self, key: &ObjectKey) {
            self.state.lock().unwrap().objects.remove(key);
        }

        fn bytes(&self, key: &ObjectKey) -> Vec<u8> {
            self.state.lock().unwrap().objects[key].clone()
        }
    }

    impl ArtifactObjectStore for TestStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.namespace
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            ArtifactStoreCapabilities {
                range_read: true,
                multipart_create: false,
                atomic_create_if_absent: true,
            }
        }

        fn provider_handle_identity(&self) -> ProviderHandleIdentity {
            self.handle
        }

        fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
            self.admission.as_ref()
        }

        fn create_immutable(
            &self,
            key: &ObjectKey,
            bytes: &[u8],
        ) -> Result<ImmutableCreateOutcome, ObjectError> {
            let mut state = self.state.lock().unwrap();
            state.creates.push(key.clone());
            if self.fail_before_apply_at == Some(state.creates.len()) {
                return Err(ObjectError::Backend {
                    detail: "injected definite create failure".to_owned(),
                    retryable: false,
                });
            }
            if matches!(self.ambiguous_create, AmbiguousCreate::BeforeApply) {
                return Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: "injected before apply".to_string(),
                });
            }
            if let Some(existing) = state.objects.get(key) {
                if existing == bytes {
                    return Ok(ImmutableCreateOutcome::Replayed);
                }
                return Err(ObjectError::ImmutableCollision {
                    key: key.clone(),
                    expected_sha256: hex(&sha256(bytes)),
                    actual_sha256: hex(&sha256(existing)),
                });
            }
            state.objects.insert(key.clone(), bytes.to_vec());
            if matches!(self.ambiguous_create, AmbiguousCreate::AfterApply) {
                return Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: "injected after apply".to_string(),
                });
            }
            Ok(ImmutableCreateOutcome::Created)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            assert!(range.is_none(), "recovery checkpoint must use full reads");
            let mut state = self.state.lock().unwrap();
            state.reads.push(key.clone());
            state
                .objects
                .get(key)
                .cloned()
                .ok_or_else(|| ObjectError::ObjectNotFound { key: key.clone() })
        }

        fn head(&self, _key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.state.lock().unwrap().head_calls += 1;
            Ok(None)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            Ok(
                if self.state.lock().unwrap().objects.remove(key).is_some() {
                    ObjectDeleteOutcome::Deleted
                } else {
                    ObjectDeleteOutcome::Absent
                },
            )
        }
    }

    fn identity() -> RecoveryCheckpointIdentity {
        RecoveryCheckpointIdentity::new(
            ObjectNamespaceId::from_bytes([1; 16]),
            LogicalShardId::from_bytes([2; 16]),
            RecoveryCheckpointBoundary::new(7, [3; 32]),
        )
    }

    fn plan_and_write(
        store: &TestStore,
        identity: RecoveryCheckpointIdentity,
        envelope: &[u8],
        chunk_size: usize,
    ) -> Result<RecoveryCheckpointWrite, RecoveryCheckpointError> {
        let calls_before = store.create_keys().len();
        let plan = plan_recovery_checkpoint_blob(identity, envelope, chunk_size)?;
        assert_eq!(
            store.create_keys().len(),
            calls_before,
            "planning must perform zero object calls"
        );
        write_recovery_checkpoint_blob_from_plan(store, &plan, envelope)
    }

    #[test]
    fn recovery_checkpoint_round_trips_through_manifest_last_blob() {
        let store = TestStore::admitted(identity().object_namespace(), 1024);
        let envelope = b"opaque whole-store checkpoint";

        let written = plan_and_write(&store, identity(), envelope, 8).unwrap();
        let loaded = read_recovery_checkpoint_blob(&store, identity(), written.receipt()).unwrap();

        assert_eq!(loaded.envelope(), envelope);
        assert_eq!(loaded.receipt(), written.receipt());
        let plan = plan_recovery_checkpoint_blob(identity(), envelope, 8).unwrap();
        let encoded_plan = plan.encode();
        assert_eq!(
            RecoveryCheckpointBlobPlan::decode(&encoded_plan).unwrap(),
            plan
        );
        let mut trailing_plan = encoded_plan;
        trailing_plan.push(0);
        assert!(matches!(
            RecoveryCheckpointBlobPlan::decode(&trailing_plan),
            Err(RecoveryCheckpointError::InvalidPlan(_))
        ));
        let encoded_receipt = written.receipt().encode();
        assert_eq!(
            RecoveryCheckpointBlobReceipt::decode(&encoded_receipt).unwrap(),
            *written.receipt()
        );
        let mut trailing_receipt = encoded_receipt;
        trailing_receipt.push(0);
        assert!(matches!(
            RecoveryCheckpointBlobReceipt::decode(&trailing_receipt),
            Err(RecoveryCheckpointError::InvalidReceipt(_))
        ));
        let keys = store.create_keys();
        assert_eq!(
            keys.last(),
            Some(&written.receipt().manifest_key().unwrap())
        );
        assert!(keys[..keys.len() - 1]
            .iter()
            .all(|key| key.as_str().contains("/chunks/")));
        assert_eq!(store.head_calls(), 0);
    }

    #[test]
    fn exact_retry_reuses_one_receipt_and_all_immutable_objects() {
        let store = TestStore::admitted(identity().object_namespace(), 1024);
        let envelope = b"three different chunks";
        let first = plan_and_write(&store, identity(), envelope, 8).unwrap();

        let replay = plan_and_write(&store, identity(), envelope, 8).unwrap();

        assert_eq!(replay.receipt(), first.receipt());
        assert_eq!(replay.chunks_created(), 0);
        assert_eq!(replay.chunks_replayed(), replay.receipt().chunk_count());
        assert_eq!(replay.manifest_outcome(), ImmutableCreateOutcome::Replayed);
    }

    #[test]
    fn namespace_handle_and_object_sizes_are_preflighted_before_any_create() {
        let foreign = TestStore::admitted(ObjectNamespaceId::from_bytes([9; 16]), 1024);
        assert_eq!(
            plan_and_write(&foreign, identity(), b"checkpoint", 8),
            Err(RecoveryCheckpointError::ForeignNamespace)
        );
        assert!(foreign.create_keys().is_empty());

        let chunk_too_small = TestStore::admitted(identity().object_namespace(), 7);
        assert!(matches!(
            plan_and_write(&chunk_too_small, identity(), b"checkpoint", 8),
            Err(RecoveryCheckpointError::ProviderObjectTooLarge { .. })
        ));
        assert!(chunk_too_small.create_keys().is_empty());

        let manifest_too_small = TestStore::admitted(identity().object_namespace(), 64);
        assert!(matches!(
            plan_and_write(&manifest_too_small, identity(), b"checkpoint", 8),
            Err(RecoveryCheckpointError::ProviderObjectTooLarge { .. })
        ));
        assert!(manifest_too_small.create_keys().is_empty());

        let mut wrong_handle = TestStore::admitted(identity().object_namespace(), 1024);
        wrong_handle.admission = Some(ProviderAdmissionReceipt::trusted_in_process(
            ProviderHandleIdentity::new(),
            1024,
        ));
        assert_eq!(
            plan_and_write(&wrong_handle, identity(), b"checkpoint", 8),
            Err(RecoveryCheckpointError::ProviderAdmissionRequired)
        );
        assert!(wrong_handle.create_keys().is_empty());
    }

    #[test]
    fn full_read_verification_rejects_missing_tampered_and_reordered_chunks() {
        let envelope = b"AAAABBBBCCCC";

        let missing = TestStore::admitted(identity().object_namespace(), 1024);
        let receipt = plan_and_write(&missing, identity(), envelope, 4)
            .unwrap()
            .receipt;
        missing.remove(&receipt.chunk_key(1).unwrap());
        assert!(matches!(
            read_recovery_checkpoint_blob(&missing, identity(), &receipt),
            Err(RecoveryCheckpointError::Object(
                ObjectError::ObjectNotFound { .. }
            ))
        ));

        let tampered = TestStore::admitted(identity().object_namespace(), 1024);
        let receipt = plan_and_write(&tampered, identity(), envelope, 4)
            .unwrap()
            .receipt;
        tampered.replace(&receipt.chunk_key(1).unwrap(), b"XXXX".to_vec());
        assert_eq!(
            read_recovery_checkpoint_blob(&tampered, identity(), &receipt),
            Err(RecoveryCheckpointError::InvalidManifest(
                "chunk digest does not match manifest"
            ))
        );

        let reordered = TestStore::admitted(identity().object_namespace(), 1024);
        let receipt = plan_and_write(&reordered, identity(), envelope, 4)
            .unwrap()
            .receipt;
        let zero = receipt.chunk_key(0).unwrap();
        let one = receipt.chunk_key(1).unwrap();
        let zero_bytes = reordered.bytes(&zero);
        let one_bytes = reordered.bytes(&one);
        reordered.replace(&zero, one_bytes);
        reordered.replace(&one, zero_bytes);
        assert_eq!(
            read_recovery_checkpoint_blob(&reordered, identity(), &receipt),
            Err(RecoveryCheckpointError::InvalidManifest(
                "chunk digest does not match manifest"
            ))
        );
    }

    #[test]
    fn strict_manifest_and_expected_identity_reject_duplicates_tamper_and_foreign_receipts() {
        let store = TestStore::admitted(identity().object_namespace(), 1024);
        let written = plan_and_write(&store, identity(), b"AAAABBBB", 4).unwrap();
        let receipt = written.receipt().clone();

        let mut foreign = receipt.clone();
        foreign.identity.logical_shard = LogicalShardId::from_bytes([8; 16]);
        let reads_before = store.read_count();
        assert_eq!(
            read_recovery_checkpoint_blob(&store, identity(), &foreign),
            Err(RecoveryCheckpointError::ForeignShard)
        );
        assert_eq!(store.read_count(), reads_before);

        let mut foreign_namespace = receipt.clone();
        foreign_namespace.identity.object_namespace = ObjectNamespaceId::from_bytes([7; 16]);
        assert_eq!(
            read_recovery_checkpoint_blob(&store, identity(), &foreign_namespace),
            Err(RecoveryCheckpointError::ForeignNamespace)
        );
        let mut foreign_boundary = receipt.clone();
        foreign_boundary.identity.boundary = RecoveryCheckpointBoundary::new(8, [3; 32]);
        assert_eq!(
            read_recovery_checkpoint_blob(&store, identity(), &foreign_boundary),
            Err(RecoveryCheckpointError::ForeignBoundary)
        );
        assert_eq!(store.read_count(), reads_before);

        let manifest_key = receipt.manifest_key().unwrap();
        let mut tampered_manifest = store.bytes(&manifest_key);
        tampered_manifest[MANIFEST_MAGIC.len()] ^= 1;
        store.replace(&manifest_key, tampered_manifest);
        assert_eq!(
            read_recovery_checkpoint_blob(&store, identity(), &receipt),
            Err(RecoveryCheckpointError::InvalidManifest(
                "manifest digest does not match receipt"
            ))
        );

        let duplicate_store = TestStore::admitted(identity().object_namespace(), 1024);
        let mut duplicate_receipt = plan_and_write(&duplicate_store, identity(), b"AAAABBBB", 4)
            .unwrap()
            .receipt;
        let old_key = duplicate_receipt.manifest_key().unwrap();
        let mut duplicate_manifest = duplicate_store.bytes(&old_key);
        let second_descriptor = FIXED_MANIFEST_BYTES + CHUNK_DESCRIPTOR_BYTES;
        duplicate_manifest[second_descriptor..second_descriptor + 4]
            .copy_from_slice(&0_u32.to_be_bytes());
        duplicate_receipt.manifest_digest = sha256(&duplicate_manifest);
        let duplicate_key = duplicate_receipt.manifest_key().unwrap();
        duplicate_store.replace(&duplicate_key, duplicate_manifest);
        assert_eq!(
            read_recovery_checkpoint_blob(&duplicate_store, identity(), &duplicate_receipt),
            Err(RecoveryCheckpointError::InvalidManifest(
                "chunk indexes are not contiguous and ordered"
            ))
        );

        let header_store = TestStore::admitted(identity().object_namespace(), 1024);
        let mut header_receipt = plan_and_write(&header_store, identity(), b"AAAABBBB", 4)
            .unwrap()
            .receipt;
        let original_key = header_receipt.manifest_key().unwrap();
        let mut bad_header = header_store.bytes(&original_key);
        bad_header[MANIFEST_MAGIC.len()..MANIFEST_MAGIC.len() + 2]
            .copy_from_slice(&2_u16.to_be_bytes());
        header_receipt.manifest_digest = sha256(&bad_header);
        let bad_header_key = header_receipt.manifest_key().unwrap();
        header_store.replace(&bad_header_key, bad_header);
        assert_eq!(
            read_recovery_checkpoint_blob(&header_store, identity(), &header_receipt),
            Err(RecoveryCheckpointError::InvalidManifest(
                "typed manifest header does not match receipt"
            ))
        );

        let missing_manifest_store = TestStore::admitted(identity().object_namespace(), 1024);
        let missing_manifest_receipt =
            plan_and_write(&missing_manifest_store, identity(), b"AAAABBBB", 4)
                .unwrap()
                .receipt;
        missing_manifest_store.remove(&missing_manifest_receipt.manifest_key().unwrap());
        assert!(matches!(
            read_recovery_checkpoint_blob(
                &missing_manifest_store,
                identity(),
                &missing_manifest_receipt,
            ),
            Err(RecoveryCheckpointError::Object(
                ObjectError::ObjectNotFound { .. }
            ))
        ));
    }

    #[test]
    fn ambiguous_create_is_only_confirmed_by_exact_full_readback() {
        let applied = TestStore::with_ambiguity(
            identity().object_namespace(),
            1024,
            AmbiguousCreate::AfterApply,
        );
        let write = plan_and_write(&applied, identity(), b"checkpoint", 8).unwrap();
        assert_eq!(write.chunks_created(), 0);
        assert_eq!(write.chunks_replayed(), 2);
        assert_eq!(write.manifest_outcome(), ImmutableCreateOutcome::Replayed);

        let absent = TestStore::with_ambiguity(
            identity().object_namespace(),
            1024,
            AmbiguousCreate::BeforeApply,
        );
        assert!(matches!(
            plan_and_write(&absent, identity(), b"checkpoint", 8),
            Err(RecoveryCheckpointError::CreateOutcomeUnknown { .. })
        ));
    }

    #[test]
    fn persisted_receipt_locates_every_key_after_second_create_fails() {
        let store = TestStore::fail_before_apply_at(identity().object_namespace(), 1024, 2);
        let plan = plan_recovery_checkpoint_blob(identity(), b"AAAABBBB", 4).unwrap();
        assert!(store.create_keys().is_empty());
        let durable_receipt = RecoveryCheckpointBlobReceipt::decode(&plan.receipt().encode())
            .expect("decode persisted receipt");

        assert!(write_recovery_checkpoint_blob_from_plan(&store, &plan, b"AAAABBBB").is_err());

        assert_eq!(store.resident_object_count(), 1);
        assert_eq!(durable_receipt.object_keys().unwrap(), plan.object_keys());
        assert_eq!(store.resident_keys(), vec![plan.cleanup_keys()[0].clone()]);
    }
}
