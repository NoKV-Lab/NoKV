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
use crate::digest::sha256;

const MANIFEST_MAGIC: &[u8; 8] = b"NOKVRLSM";
const MANIFEST_VERSION: u16 = 1;
const RECEIPT_MAGIC: &[u8; 8] = b"NOKVRLSR";
const RECEIPT_VERSION: u16 = 1;
const PLAN_MAGIC: &[u8; 8] = b"NOKVRLSP";
const PLAN_VERSION: u16 = 1;
const MANIFEST_HEADER_BYTES: usize = 8 + 2 + 16 + 16 + 8 + 8 + 32 + 32 + 32 + 8;
const FIXED_MANIFEST_BYTES: usize = MANIFEST_HEADER_BYTES + 4 + 4;
const CHUNK_DESCRIPTOR_BYTES: usize = 4 + 4 + 32;
const RECEIPT_BYTES: usize = 8 + 2 + 16 + 16 + 8 + 8 + 32 + 32 + 32 + 8 + 8 + 32 + 4 + 4;
const PLAN_FIXED_BYTES: usize = 8 + 2 + 4 + RECEIPT_BYTES + 8;
const RECOVERY_LOG_SEGMENT_KEY_PREFIX: &str = "nokv/recovery/log-segments/v1";

/// Default size of one recovery log segment single-PUT object.
pub const DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE: usize = 4 * 1024 * 1024;
/// Hard upper bound for one encoded recovery log segment.
pub const MAX_RECOVERY_LOG_SEGMENT_BYTES: usize = 64 * 1024 * 1024;
/// Maximum number of chunks represented by one recovery log manifest.
pub const MAX_RECOVERY_LOG_SEGMENT_CHUNKS: usize = 65_536;

/// Typed immutable identity of one contiguous recovery log segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryLogSegmentIdentity {
    object_namespace: ObjectNamespaceId,
    logical_shard: LogicalShardId,
    first_lsn: u64,
    last_lsn: u64,
    previous_chain_digest: [u8; 32],
    last_chain_digest: [u8; 32],
    segment_digest: [u8; 32],
}

/// Durable content-addressed receipt for one recovery log segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryLogSegmentReceipt {
    identity: RecoveryLogSegmentIdentity,
    segment_len: u64,
    manifest_len: u64,
    manifest_digest: [u8; 32],
    chunk_size: u32,
    chunk_count: u32,
}

/// Pure plan exposing the receipt and every possible cleanup key before writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryLogSegmentPlan {
    receipt: RecoveryLogSegmentReceipt,
    core: ChunkedBlobPlan,
}

/// Confirmed immutable-create outcomes for a recovery log segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryLogSegmentWrite {
    receipt: RecoveryLogSegmentReceipt,
    chunks_created: u32,
    chunks_replayed: u32,
    manifest_outcome: ImmutableCreateOutcome,
}

/// Fully verified recovery log segment bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryLogSegment {
    receipt: RecoveryLogSegmentReceipt,
    bytes: Vec<u8>,
}

/// Fail-closed planning, write, or read error for a recovery log segment.
#[derive(PartialEq, Eq)]
pub enum RecoveryLogSegmentError {
    InvalidLsnRange,
    EmptySegment,
    SegmentTooLarge { actual: usize, maximum: usize },
    InvalidChunkSize,
    TooManyChunks { actual: usize, maximum: usize },
    SegmentDigestMismatch,
    InvalidPlan(&'static str),
    ObjectNamespaceRequired,
    ForeignNamespace,
    ForeignShard,
    ForeignLsnRange,
    ForeignChainBoundary,
    ForeignSegmentDigest,
    ProviderAdmissionRequired,
    ProviderObjectTooLarge { requested: usize, admitted: usize },
    InvalidManifest(&'static str),
    InvalidReceipt(&'static str),
    CreateOutcomeUnknown { key: ObjectKey },
    Object(ObjectError),
}

impl RecoveryLogSegmentIdentity {
    pub const fn new(
        object_namespace: ObjectNamespaceId,
        logical_shard: LogicalShardId,
        first_lsn: u64,
        last_lsn: u64,
        previous_chain_digest: [u8; 32],
        last_chain_digest: [u8; 32],
        segment_digest: [u8; 32],
    ) -> Self {
        Self {
            object_namespace,
            logical_shard,
            first_lsn,
            last_lsn,
            previous_chain_digest,
            last_chain_digest,
            segment_digest,
        }
    }

    pub const fn object_namespace(self) -> ObjectNamespaceId {
        self.object_namespace
    }

    pub const fn logical_shard(self) -> LogicalShardId {
        self.logical_shard
    }

    pub const fn first_lsn(self) -> u64 {
        self.first_lsn
    }

    pub const fn last_lsn(self) -> u64 {
        self.last_lsn
    }

    pub const fn previous_chain_digest(self) -> [u8; 32] {
        self.previous_chain_digest
    }

    pub const fn last_chain_digest(self) -> [u8; 32] {
        self.last_chain_digest
    }

    pub const fn segment_digest(self) -> [u8; 32] {
        self.segment_digest
    }
}

impl RecoveryLogSegmentReceipt {
    fn from_core(identity: RecoveryLogSegmentIdentity, core: ChunkedBlobReceipt) -> Self {
        Self {
            identity,
            segment_len: core.payload_len,
            manifest_len: core.manifest_len,
            manifest_digest: core.manifest_digest,
            chunk_size: core.chunk_size,
            chunk_count: core.chunk_count,
        }
    }

    fn core_receipt(&self) -> ChunkedBlobReceipt {
        ChunkedBlobReceipt {
            payload_len: self.segment_len,
            payload_digest: self.identity.segment_digest,
            manifest_len: self.manifest_len,
            manifest_digest: self.manifest_digest,
            chunk_size: self.chunk_size,
            chunk_count: self.chunk_count,
        }
    }

    pub const fn identity(&self) -> RecoveryLogSegmentIdentity {
        self.identity
    }

    pub const fn segment_len(&self) -> u64 {
        self.segment_len
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

    pub fn manifest_key(&self) -> Result<ObjectKey, RecoveryLogSegmentError> {
        derive_manifest_key(log_keyspace(self.identity), self.manifest_digest)
            .map_err(map_core_error)
    }

    pub fn chunk_key(&self, index: u32) -> Result<ObjectKey, RecoveryLogSegmentError> {
        if index >= self.chunk_count {
            return Err(RecoveryLogSegmentError::InvalidReceipt(
                "chunk index is outside receipt",
            ));
        }
        derive_chunk_key(log_keyspace(self.identity), self.manifest_digest, index)
            .map_err(map_core_error)
    }

    pub fn object_keys(&self) -> Result<Vec<ObjectKey>, RecoveryLogSegmentError> {
        derive_object_keys(
            log_keyspace(self.identity),
            self.core_receipt(),
            log_bounds(),
        )
        .map_err(map_core_error)
    }

    /// Encode without provider coordinates or process-local handle identity.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(RECEIPT_BYTES);
        bytes.extend_from_slice(RECEIPT_MAGIC);
        bytes.extend_from_slice(&RECEIPT_VERSION.to_be_bytes());
        bytes.extend_from_slice(self.identity.object_namespace.as_bytes());
        bytes.extend_from_slice(self.identity.logical_shard.as_bytes());
        bytes.extend_from_slice(&self.identity.first_lsn.to_be_bytes());
        bytes.extend_from_slice(&self.identity.last_lsn.to_be_bytes());
        bytes.extend_from_slice(&self.identity.previous_chain_digest);
        bytes.extend_from_slice(&self.identity.last_chain_digest);
        bytes.extend_from_slice(&self.identity.segment_digest);
        bytes.extend_from_slice(&self.segment_len.to_be_bytes());
        bytes.extend_from_slice(&self.manifest_len.to_be_bytes());
        bytes.extend_from_slice(&self.manifest_digest);
        bytes.extend_from_slice(&self.chunk_size.to_be_bytes());
        bytes.extend_from_slice(&self.chunk_count.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryLogSegmentError> {
        if bytes.len() != RECEIPT_BYTES || !bytes.starts_with(RECEIPT_MAGIC) {
            return Err(RecoveryLogSegmentError::InvalidReceipt(
                "wrong length or magic",
            ));
        }
        let mut offset = RECEIPT_MAGIC.len();
        if take_receipt_u16(bytes, &mut offset)? != RECEIPT_VERSION {
            return Err(RecoveryLogSegmentError::InvalidReceipt(
                "unsupported receipt version",
            ));
        }
        let identity = RecoveryLogSegmentIdentity::new(
            ObjectNamespaceId::from_bytes(take_receipt_array(bytes, &mut offset)?),
            LogicalShardId::from_bytes(take_receipt_array(bytes, &mut offset)?),
            take_receipt_u64(bytes, &mut offset)?,
            take_receipt_u64(bytes, &mut offset)?,
            take_receipt_array(bytes, &mut offset)?,
            take_receipt_array(bytes, &mut offset)?,
            take_receipt_array(bytes, &mut offset)?,
        );
        let receipt = Self {
            identity,
            segment_len: take_receipt_u64(bytes, &mut offset)?,
            manifest_len: take_receipt_u64(bytes, &mut offset)?,
            manifest_digest: take_receipt_array(bytes, &mut offset)?,
            chunk_size: take_receipt_u32(bytes, &mut offset)?,
            chunk_count: take_receipt_u32(bytes, &mut offset)?,
        };
        if offset != bytes.len() {
            return Err(RecoveryLogSegmentError::InvalidReceipt(
                "trailing receipt bytes",
            ));
        }
        validate_receipt(&receipt)?;
        Ok(receipt)
    }
}

impl RecoveryLogSegmentPlan {
    pub fn receipt(&self) -> &RecoveryLogSegmentReceipt {
        &self.receipt
    }

    pub fn object_keys(&self) -> &[ObjectKey] {
        self.core.object_keys()
    }

    pub fn cleanup_keys(&self) -> &[ObjectKey] {
        self.object_keys()
    }

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

    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryLogSegmentError> {
        if bytes.len() < PLAN_FIXED_BYTES || !bytes.starts_with(PLAN_MAGIC) {
            return Err(RecoveryLogSegmentError::InvalidPlan(
                "wrong length or plan magic",
            ));
        }
        let mut offset = PLAN_MAGIC.len();
        if take_plan_u16(bytes, &mut offset)? != PLAN_VERSION {
            return Err(RecoveryLogSegmentError::InvalidPlan(
                "unsupported plan version",
            ));
        }
        let receipt_len = take_plan_u32(bytes, &mut offset)? as usize;
        if receipt_len != RECEIPT_BYTES {
            return Err(RecoveryLogSegmentError::InvalidPlan(
                "receipt length is not canonical",
            ));
        }
        let receipt_end =
            offset
                .checked_add(receipt_len)
                .ok_or(RecoveryLogSegmentError::InvalidPlan(
                    "receipt length overflows",
                ))?;
        let receipt = RecoveryLogSegmentReceipt::decode(
            bytes
                .get(offset..receipt_end)
                .ok_or(RecoveryLogSegmentError::InvalidPlan("receipt is truncated"))?,
        )?;
        offset = receipt_end;
        let manifest_len = usize::try_from(take_plan_u64(bytes, &mut offset)?)
            .map_err(|_| RecoveryLogSegmentError::InvalidPlan("manifest length overflows"))?;
        let manifest_end =
            offset
                .checked_add(manifest_len)
                .ok_or(RecoveryLogSegmentError::InvalidPlan(
                    "manifest length overflows",
                ))?;
        if manifest_end != bytes.len() || manifest_len as u64 != receipt.manifest_len {
            return Err(RecoveryLogSegmentError::InvalidPlan(
                "manifest length does not match plan",
            ));
        }
        let core = restore_chunked_blob_plan(
            log_keyspace(receipt.identity),
            log_bounds(),
            encode_manifest_header(receipt.identity, receipt.segment_len),
            receipt.core_receipt(),
            bytes[offset..manifest_end].to_vec(),
        )
        .map_err(map_core_error)?;
        Ok(Self { receipt, core })
    }
}

impl RecoveryLogSegmentWrite {
    pub fn receipt(&self) -> &RecoveryLogSegmentReceipt {
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

impl RecoveryLogSegment {
    pub fn receipt(&self) -> &RecoveryLogSegmentReceipt {
        &self.receipt
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Produce a deterministic plan without performing any object operation.
pub fn plan_recovery_log_segment(
    identity: RecoveryLogSegmentIdentity,
    segment: &[u8],
    chunk_size: usize,
) -> Result<RecoveryLogSegmentPlan, RecoveryLogSegmentError> {
    validate_identity(identity)?;
    let core = plan_chunked_blob(
        log_keyspace(identity),
        log_bounds(),
        segment,
        chunk_size,
        |segment_len, segment_digest| {
            encode_manifest_header(
                RecoveryLogSegmentIdentity {
                    segment_digest,
                    ..identity
                },
                segment_len,
            )
        },
    )
    .map_err(map_core_error)?;
    if core.receipt().payload_digest != identity.segment_digest {
        return Err(RecoveryLogSegmentError::SegmentDigestMismatch);
    }
    let receipt = RecoveryLogSegmentReceipt::from_core(identity, core.receipt());
    Ok(RecoveryLogSegmentPlan { receipt, core })
}

/// Execute only after the coordinator has durably persisted the plan or receipt intent.
pub fn write_recovery_log_segment_from_plan(
    store: &dyn ArtifactObjectStore,
    plan: &RecoveryLogSegmentPlan,
    segment: &[u8],
) -> Result<RecoveryLogSegmentWrite, RecoveryLogSegmentError> {
    let written =
        write_chunked_blob_from_plan(store, &plan.core, segment).map_err(map_core_error)?;
    Ok(RecoveryLogSegmentWrite {
        receipt: plan.receipt.clone(),
        chunks_created: written.chunks_created,
        chunks_replayed: written.chunks_replayed,
        manifest_outcome: written.manifest_outcome,
    })
}

/// Read and fully verify one typed segment without head, list, or range requests.
pub fn read_recovery_log_segment(
    store: &dyn ArtifactObjectStore,
    expected: RecoveryLogSegmentIdentity,
    receipt: &RecoveryLogSegmentReceipt,
) -> Result<RecoveryLogSegment, RecoveryLogSegmentError> {
    validate_expected_receipt(expected, receipt)?;
    validate_receipt(receipt)?;
    let bytes = read_chunked_blob(
        store,
        log_keyspace(expected),
        log_bounds(),
        encode_manifest_header(receipt.identity, receipt.segment_len),
        receipt.core_receipt(),
    )
    .map_err(map_core_error)?;
    Ok(RecoveryLogSegment {
        receipt: receipt.clone(),
        bytes,
    })
}

fn validate_identity(identity: RecoveryLogSegmentIdentity) -> Result<(), RecoveryLogSegmentError> {
    if identity.first_lsn == 0 || identity.last_lsn < identity.first_lsn {
        return Err(RecoveryLogSegmentError::InvalidLsnRange);
    }
    Ok(())
}

fn validate_expected_receipt(
    expected: RecoveryLogSegmentIdentity,
    receipt: &RecoveryLogSegmentReceipt,
) -> Result<(), RecoveryLogSegmentError> {
    if receipt.identity.object_namespace != expected.object_namespace {
        return Err(RecoveryLogSegmentError::ForeignNamespace);
    }
    if receipt.identity.logical_shard != expected.logical_shard {
        return Err(RecoveryLogSegmentError::ForeignShard);
    }
    if receipt.identity.first_lsn != expected.first_lsn
        || receipt.identity.last_lsn != expected.last_lsn
    {
        return Err(RecoveryLogSegmentError::ForeignLsnRange);
    }
    if receipt.identity.previous_chain_digest != expected.previous_chain_digest
        || receipt.identity.last_chain_digest != expected.last_chain_digest
    {
        return Err(RecoveryLogSegmentError::ForeignChainBoundary);
    }
    if receipt.identity.segment_digest != expected.segment_digest {
        return Err(RecoveryLogSegmentError::ForeignSegmentDigest);
    }
    Ok(())
}

fn validate_receipt(receipt: &RecoveryLogSegmentReceipt) -> Result<(), RecoveryLogSegmentError> {
    validate_identity(receipt.identity)
        .map_err(|_| RecoveryLogSegmentError::InvalidReceipt("invalid LSN range"))?;
    validate_chunked_blob_receipt(receipt.core_receipt(), log_bounds())
        .map_err(|_| RecoveryLogSegmentError::InvalidReceipt("invalid chunk layout"))?;
    let expected_manifest_len = FIXED_MANIFEST_BYTES
        .checked_add(receipt.chunk_count as usize * CHUNK_DESCRIPTOR_BYTES)
        .ok_or(RecoveryLogSegmentError::InvalidReceipt(
            "manifest length overflows",
        ))?;
    if receipt.manifest_len != expected_manifest_len as u64 {
        return Err(RecoveryLogSegmentError::InvalidReceipt(
            "manifest length does not match chunk count",
        ));
    }
    Ok(())
}

fn log_bounds() -> ChunkedBlobBounds {
    ChunkedBlobBounds {
        max_payload_bytes: MAX_RECOVERY_LOG_SEGMENT_BYTES,
        max_chunks: MAX_RECOVERY_LOG_SEGMENT_CHUNKS,
    }
}

fn log_keyspace(identity: RecoveryLogSegmentIdentity) -> ChunkedBlobKeyspace {
    ChunkedBlobKeyspace {
        prefix: RECOVERY_LOG_SEGMENT_KEY_PREFIX,
        object_namespace: identity.object_namespace,
        logical_shard: identity.logical_shard,
    }
}

fn encode_manifest_header(identity: RecoveryLogSegmentIdentity, segment_len: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MANIFEST_HEADER_BYTES);
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&MANIFEST_VERSION.to_be_bytes());
    bytes.extend_from_slice(identity.object_namespace.as_bytes());
    bytes.extend_from_slice(identity.logical_shard.as_bytes());
    bytes.extend_from_slice(&identity.first_lsn.to_be_bytes());
    bytes.extend_from_slice(&identity.last_lsn.to_be_bytes());
    bytes.extend_from_slice(&identity.previous_chain_digest);
    bytes.extend_from_slice(&identity.last_chain_digest);
    bytes.extend_from_slice(&identity.segment_digest);
    bytes.extend_from_slice(&segment_len.to_be_bytes());
    bytes
}

fn map_core_error(error: ChunkedBlobError) -> RecoveryLogSegmentError {
    match error {
        ChunkedBlobError::EmptyPayload => RecoveryLogSegmentError::EmptySegment,
        ChunkedBlobError::PayloadTooLarge { actual, maximum } => {
            RecoveryLogSegmentError::SegmentTooLarge { actual, maximum }
        }
        ChunkedBlobError::InvalidChunkSize => RecoveryLogSegmentError::InvalidChunkSize,
        ChunkedBlobError::TooManyChunks { actual, maximum } => {
            RecoveryLogSegmentError::TooManyChunks { actual, maximum }
        }
        ChunkedBlobError::PayloadMismatch => RecoveryLogSegmentError::SegmentDigestMismatch,
        ChunkedBlobError::InvalidPlan(reason) => RecoveryLogSegmentError::InvalidPlan(reason),
        ChunkedBlobError::ObjectNamespaceRequired => {
            RecoveryLogSegmentError::ObjectNamespaceRequired
        }
        ChunkedBlobError::ForeignNamespace => RecoveryLogSegmentError::ForeignNamespace,
        ChunkedBlobError::ProviderAdmissionRequired => {
            RecoveryLogSegmentError::ProviderAdmissionRequired
        }
        ChunkedBlobError::ProviderObjectTooLarge {
            requested,
            admitted,
        } => RecoveryLogSegmentError::ProviderObjectTooLarge {
            requested,
            admitted,
        },
        ChunkedBlobError::InvalidManifest(reason) => {
            RecoveryLogSegmentError::InvalidManifest(reason)
        }
        ChunkedBlobError::CreateOutcomeUnknown { key } => {
            RecoveryLogSegmentError::CreateOutcomeUnknown { key }
        }
        ChunkedBlobError::Object(error) => RecoveryLogSegmentError::Object(error),
    }
}

fn take_receipt_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], RecoveryLogSegmentError> {
    take_array(bytes, offset, RecoveryLogSegmentError::InvalidReceipt)
}

fn take_plan_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
) -> Result<[u8; N], RecoveryLogSegmentError> {
    take_array(bytes, offset, RecoveryLogSegmentError::InvalidPlan)
}

fn take_array<const N: usize>(
    bytes: &[u8],
    offset: &mut usize,
    error: fn(&'static str) -> RecoveryLogSegmentError,
) -> Result<[u8; N], RecoveryLogSegmentError> {
    let end = offset.checked_add(N).ok_or(error("offset overflows"))?;
    let slice = bytes
        .get(*offset..end)
        .ok_or(error("bytes are truncated"))?;
    *offset = end;
    let mut array = [0_u8; N];
    array.copy_from_slice(slice);
    Ok(array)
}

fn take_receipt_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, RecoveryLogSegmentError> {
    Ok(u16::from_be_bytes(take_receipt_array(bytes, offset)?))
}

fn take_receipt_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, RecoveryLogSegmentError> {
    Ok(u32::from_be_bytes(take_receipt_array(bytes, offset)?))
}

fn take_receipt_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, RecoveryLogSegmentError> {
    Ok(u64::from_be_bytes(take_receipt_array(bytes, offset)?))
}

fn take_plan_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, RecoveryLogSegmentError> {
    Ok(u16::from_be_bytes(take_plan_array(bytes, offset)?))
}

fn take_plan_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, RecoveryLogSegmentError> {
    Ok(u32::from_be_bytes(take_plan_array(bytes, offset)?))
}

fn take_plan_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, RecoveryLogSegmentError> {
    Ok(u64::from_be_bytes(take_plan_array(bytes, offset)?))
}

impl fmt::Display for RecoveryLogSegmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidLsnRange => "invalid recovery log segment LSN range",
            Self::EmptySegment => "recovery log segment is empty",
            Self::SegmentTooLarge { .. } => "recovery log segment is too large",
            Self::InvalidChunkSize => "invalid recovery log segment chunk size",
            Self::TooManyChunks { .. } => "recovery log segment has too many chunks",
            Self::SegmentDigestMismatch => "recovery log segment digest does not match its plan",
            Self::InvalidPlan(_) => "invalid recovery log segment plan",
            Self::ObjectNamespaceRequired => {
                "recovery log segment requires a namespace-bound store"
            }
            Self::ForeignNamespace => "recovery log segment belongs to another object namespace",
            Self::ForeignShard => "recovery log segment belongs to another logical shard",
            Self::ForeignLsnRange => "recovery log segment has another LSN range",
            Self::ForeignChainBoundary => "recovery log segment has another chain boundary",
            Self::ForeignSegmentDigest => "recovery log segment has another segment digest",
            Self::ProviderAdmissionRequired => "recovery log segment provider is not admitted",
            Self::ProviderObjectTooLarge { .. } => {
                "recovery log segment object exceeds provider admission"
            }
            Self::InvalidManifest(_) => "invalid recovery log segment manifest",
            Self::InvalidReceipt(_) => "invalid recovery log segment receipt",
            Self::CreateOutcomeUnknown { .. } => "recovery log segment create outcome is unknown",
            Self::Object(_) => "recovery log segment object operation failed",
        })
    }
}

impl fmt::Debug for RecoveryLogSegmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("RecoveryLogSegmentError")
            .field(&self.to_string())
            .finish()
    }
}

impl std::error::Error for RecoveryLogSegmentError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunked_immutable_blob::test_support::{CreateBehavior, TestStore};
    use crate::{
        plan_recovery_checkpoint_blob, RecoveryCheckpointBlobReceipt, RecoveryCheckpointBoundary,
        RecoveryCheckpointIdentity,
    };

    fn namespace() -> ObjectNamespaceId {
        ObjectNamespaceId::from_bytes([1; 16])
    }

    fn segment_identity(bytes: &[u8]) -> RecoveryLogSegmentIdentity {
        RecoveryLogSegmentIdentity::new(
            namespace(),
            LogicalShardId::from_bytes([2; 16]),
            11,
            13,
            [3; 32],
            [4; 32],
            sha256(bytes),
        )
    }

    #[test]
    fn plan_is_zero_io_and_roundtrip_is_manifest_last_with_strict_codecs() {
        let segment = b"AAAABBBBCCCC";
        let identity = segment_identity(segment);
        let store = TestStore::admitted(namespace(), 1024);

        let plan = plan_recovery_log_segment(identity, segment, 4).unwrap();
        assert!(store.create_keys().is_empty());
        let encoded_plan = plan.encode();
        assert_eq!(RecoveryLogSegmentPlan::decode(&encoded_plan).unwrap(), plan);
        let mut trailing_plan = encoded_plan;
        trailing_plan.push(0);
        assert!(matches!(
            RecoveryLogSegmentPlan::decode(&trailing_plan),
            Err(RecoveryLogSegmentError::InvalidPlan(_))
        ));
        assert_eq!(
            RecoveryLogSegmentReceipt::decode(&plan.receipt().encode()).unwrap(),
            *plan.receipt()
        );
        let mut trailing_receipt = plan.receipt().encode();
        trailing_receipt.push(0);
        assert!(matches!(
            RecoveryLogSegmentReceipt::decode(&trailing_receipt),
            Err(RecoveryLogSegmentError::InvalidReceipt(_))
        ));
        assert_eq!(plan.object_keys(), plan.receipt().object_keys().unwrap());

        let written = write_recovery_log_segment_from_plan(&store, &plan, segment).unwrap();
        let loaded = read_recovery_log_segment(&store, identity, written.receipt()).unwrap();
        assert_eq!(loaded.bytes(), segment);
        assert_eq!(loaded.receipt(), written.receipt());
        assert_eq!(store.create_keys().last(), plan.object_keys().last());
        assert!(store.create_keys()[..plan.receipt().chunk_count() as usize]
            .iter()
            .all(|key| key.as_str().contains("/chunks/")));
        assert_eq!(store.head_calls(), 0);

        let checkpoint = plan_recovery_checkpoint_blob(
            RecoveryCheckpointIdentity::new(
                namespace(),
                identity.logical_shard(),
                RecoveryCheckpointBoundary::new(identity.last_lsn(), identity.last_chain_digest()),
            ),
            segment,
            4,
        )
        .unwrap();
        assert_ne!(checkpoint.object_keys(), plan.object_keys());
        assert!(RecoveryLogSegmentReceipt::decode(&checkpoint.receipt().encode()).is_err());
        assert!(RecoveryCheckpointBlobReceipt::decode(&plan.receipt().encode()).is_err());
    }

    #[test]
    fn fresh_replay_collision_and_distinct_receipts_never_share_chunk_keys() {
        let segment = b"AAAABBBBCCCC";
        let identity = segment_identity(segment);
        let store = TestStore::admitted(namespace(), 1024);
        let plan = plan_recovery_log_segment(identity, segment, 4).unwrap();

        let fresh = write_recovery_log_segment_from_plan(&store, &plan, segment).unwrap();
        assert_eq!(fresh.chunks_created(), 3);
        assert_eq!(fresh.manifest_outcome(), ImmutableCreateOutcome::Created);
        let replay = write_recovery_log_segment_from_plan(&store, &plan, segment).unwrap();
        assert_eq!(replay.chunks_replayed(), 3);
        assert_eq!(replay.manifest_outcome(), ImmutableCreateOutcome::Replayed);

        let collision_store = TestStore::admitted(namespace(), 1024);
        collision_store.replace(&plan.object_keys()[0], b"XXXX".to_vec());
        assert!(matches!(
            write_recovery_log_segment_from_plan(&collision_store, &plan, segment),
            Err(RecoveryLogSegmentError::Object(
                ObjectError::ImmutableCollision { .. }
            ))
        ));

        let next_identity = RecoveryLogSegmentIdentity::new(
            namespace(),
            identity.logical_shard(),
            14,
            16,
            identity.last_chain_digest(),
            [5; 32],
            identity.segment_digest(),
        );
        let next = plan_recovery_log_segment(next_identity, segment, 4).unwrap();
        assert_ne!(
            next.receipt().manifest_digest(),
            plan.receipt().manifest_digest()
        );
        assert!(next
            .object_keys()
            .iter()
            .all(|key| !plan.object_keys().contains(key)));
    }

    #[test]
    fn ambiguous_create_requires_exact_readback() {
        let segment = b"log-segment";
        let identity = segment_identity(segment);
        let plan = plan_recovery_log_segment(identity, segment, 8).unwrap();
        let applied =
            TestStore::with_behavior(namespace(), 1024, CreateBehavior::AmbiguousAfterApply);
        let written = write_recovery_log_segment_from_plan(&applied, &plan, segment).unwrap();
        assert_eq!(written.chunks_created(), 0);
        assert_eq!(written.chunks_replayed(), 2);
        assert_eq!(written.manifest_outcome(), ImmutableCreateOutcome::Replayed);

        let absent =
            TestStore::with_behavior(namespace(), 1024, CreateBehavior::AmbiguousBeforeApply);
        assert!(matches!(
            write_recovery_log_segment_from_plan(&absent, &plan, segment),
            Err(RecoveryLogSegmentError::CreateOutcomeUnknown { .. })
        ));
    }

    #[test]
    fn partial_failure_uses_the_persisted_receipt_as_exact_cleanup_authority() {
        let segment = b"AAAABBBB";
        let identity = segment_identity(segment);
        let store = TestStore::fail_before_apply_at(namespace(), 1024, 2);
        let plan = plan_recovery_log_segment(identity, segment, 4).unwrap();
        let durable_receipt = RecoveryLogSegmentReceipt::decode(&plan.receipt().encode()).unwrap();
        assert!(store.create_keys().is_empty());

        assert!(write_recovery_log_segment_from_plan(&store, &plan, segment).is_err());

        assert_eq!(
            store.resident_keys(),
            vec![durable_receipt.object_keys().unwrap()[0].clone()]
        );
        assert_eq!(plan.cleanup_keys(), durable_receipt.object_keys().unwrap());
    }

    #[test]
    fn read_rejects_missing_tampered_reordered_and_duplicate_chunks() {
        let segment = b"AAAABBBBCCCC";
        let identity = segment_identity(segment);

        let missing = TestStore::admitted(namespace(), 1024);
        let plan = plan_recovery_log_segment(identity, segment, 4).unwrap();
        write_recovery_log_segment_from_plan(&missing, &plan, segment).unwrap();
        missing.remove(&plan.receipt().chunk_key(1).unwrap());
        assert!(matches!(
            read_recovery_log_segment(&missing, identity, plan.receipt()),
            Err(RecoveryLogSegmentError::Object(
                ObjectError::ObjectNotFound { .. }
            ))
        ));

        let tampered = TestStore::admitted(namespace(), 1024);
        write_recovery_log_segment_from_plan(&tampered, &plan, segment).unwrap();
        tampered.replace(&plan.receipt().chunk_key(1).unwrap(), b"XXXX".to_vec());
        assert_eq!(
            read_recovery_log_segment(&tampered, identity, plan.receipt()),
            Err(RecoveryLogSegmentError::InvalidManifest(
                "chunk digest does not match manifest"
            ))
        );

        let reordered = TestStore::admitted(namespace(), 1024);
        write_recovery_log_segment_from_plan(&reordered, &plan, segment).unwrap();
        let zero = plan.receipt().chunk_key(0).unwrap();
        let one = plan.receipt().chunk_key(1).unwrap();
        let zero_bytes = reordered.bytes(&zero);
        let one_bytes = reordered.bytes(&one);
        reordered.replace(&zero, one_bytes);
        reordered.replace(&one, zero_bytes);
        assert!(matches!(
            read_recovery_log_segment(&reordered, identity, plan.receipt()),
            Err(RecoveryLogSegmentError::InvalidManifest(_))
        ));

        let duplicate = TestStore::admitted(namespace(), 1024);
        write_recovery_log_segment_from_plan(&duplicate, &plan, segment).unwrap();
        let original_manifest_key = plan.receipt().manifest_key().unwrap();
        let mut manifest = duplicate.bytes(&original_manifest_key);
        let second_descriptor = FIXED_MANIFEST_BYTES + CHUNK_DESCRIPTOR_BYTES;
        manifest[second_descriptor..second_descriptor + 4].copy_from_slice(&0_u32.to_be_bytes());
        let mut receipt = plan.receipt().clone();
        receipt.manifest_digest = sha256(&manifest);
        let duplicate_key = receipt.manifest_key().unwrap();
        duplicate.replace(&duplicate_key, manifest);
        assert_eq!(
            read_recovery_log_segment(&duplicate, identity, &receipt),
            Err(RecoveryLogSegmentError::InvalidManifest(
                "chunk indexes are not contiguous and ordered"
            ))
        );

        let header_tamper = TestStore::admitted(namespace(), 1024);
        write_recovery_log_segment_from_plan(&header_tamper, &plan, segment).unwrap();
        let original_manifest_key = plan.receipt().manifest_key().unwrap();
        let mut manifest = header_tamper.bytes(&original_manifest_key);
        manifest[MANIFEST_MAGIC.len()..MANIFEST_MAGIC.len() + 2]
            .copy_from_slice(&2_u16.to_be_bytes());
        let mut receipt = plan.receipt().clone();
        receipt.manifest_digest = sha256(&manifest);
        header_tamper.replace(&receipt.manifest_key().unwrap(), manifest);
        assert_eq!(
            read_recovery_log_segment(&header_tamper, identity, &receipt),
            Err(RecoveryLogSegmentError::InvalidManifest(
                "typed manifest header does not match receipt"
            ))
        );

        let missing_manifest = TestStore::admitted(namespace(), 1024);
        write_recovery_log_segment_from_plan(&missing_manifest, &plan, segment).unwrap();
        missing_manifest.remove(&plan.receipt().manifest_key().unwrap());
        assert!(matches!(
            read_recovery_log_segment(&missing_manifest, identity, plan.receipt()),
            Err(RecoveryLogSegmentError::Object(
                ObjectError::ObjectNotFound { .. }
            ))
        ));
    }

    #[test]
    fn foreign_receipts_fail_before_object_reads() {
        let segment = b"segment";
        let identity = segment_identity(segment);
        let store = TestStore::admitted(namespace(), 1024);
        let receipt = plan_recovery_log_segment(identity, segment, 4)
            .unwrap()
            .receipt;

        let mut foreign = receipt.clone();
        foreign.identity.object_namespace = ObjectNamespaceId::from_bytes([9; 16]);
        assert_eq!(
            read_recovery_log_segment(&store, identity, &foreign),
            Err(RecoveryLogSegmentError::ForeignNamespace)
        );
        foreign = receipt.clone();
        foreign.identity.logical_shard = LogicalShardId::from_bytes([9; 16]);
        assert_eq!(
            read_recovery_log_segment(&store, identity, &foreign),
            Err(RecoveryLogSegmentError::ForeignShard)
        );
        foreign = receipt.clone();
        foreign.identity.first_lsn += 1;
        assert_eq!(
            read_recovery_log_segment(&store, identity, &foreign),
            Err(RecoveryLogSegmentError::ForeignLsnRange)
        );
        foreign = receipt.clone();
        foreign.identity.previous_chain_digest = [8; 32];
        assert_eq!(
            read_recovery_log_segment(&store, identity, &foreign),
            Err(RecoveryLogSegmentError::ForeignChainBoundary)
        );
        foreign = receipt;
        foreign.identity.segment_digest = [7; 32];
        assert_eq!(
            read_recovery_log_segment(&store, identity, &foreign),
            Err(RecoveryLogSegmentError::ForeignSegmentDigest)
        );
        assert_eq!(store.read_count(), 0);
    }

    #[test]
    fn provider_and_payload_preflight_perform_zero_writes() {
        let segment = b"segment";
        let identity = segment_identity(segment);
        let plan = plan_recovery_log_segment(identity, segment, 4).unwrap();

        let foreign = TestStore::admitted(ObjectNamespaceId::from_bytes([8; 16]), 1024);
        assert_eq!(
            write_recovery_log_segment_from_plan(&foreign, &plan, segment),
            Err(RecoveryLogSegmentError::ForeignNamespace)
        );
        assert!(foreign.create_keys().is_empty());

        let mut unbound = TestStore::admitted(namespace(), 1024);
        unbound.remove_namespace_binding();
        assert_eq!(
            write_recovery_log_segment_from_plan(&unbound, &plan, segment),
            Err(RecoveryLogSegmentError::ObjectNamespaceRequired)
        );
        assert!(unbound.create_keys().is_empty());

        let mut unadmitted = TestStore::admitted(namespace(), 1024);
        unadmitted.remove_admission();
        assert_eq!(
            write_recovery_log_segment_from_plan(&unadmitted, &plan, segment),
            Err(RecoveryLogSegmentError::ProviderAdmissionRequired)
        );
        assert!(unadmitted.create_keys().is_empty());

        let chunk_too_small = TestStore::admitted(namespace(), 3);
        assert!(matches!(
            write_recovery_log_segment_from_plan(&chunk_too_small, &plan, segment),
            Err(RecoveryLogSegmentError::ProviderObjectTooLarge { .. })
        ));
        assert!(chunk_too_small.create_keys().is_empty());

        let manifest_too_small = TestStore::admitted(namespace(), 64);
        assert!(matches!(
            write_recovery_log_segment_from_plan(&manifest_too_small, &plan, segment),
            Err(RecoveryLogSegmentError::ProviderObjectTooLarge { .. })
        ));
        assert!(manifest_too_small.create_keys().is_empty());

        let mut wrong_handle = TestStore::admitted(namespace(), 1024);
        wrong_handle.bind_admission_to_another_handle();
        assert_eq!(
            write_recovery_log_segment_from_plan(&wrong_handle, &plan, segment),
            Err(RecoveryLogSegmentError::ProviderAdmissionRequired)
        );
        assert!(wrong_handle.create_keys().is_empty());

        let payload_mismatch = TestStore::admitted(namespace(), 1024);
        assert_eq!(
            write_recovery_log_segment_from_plan(&payload_mismatch, &plan, b"different"),
            Err(RecoveryLogSegmentError::SegmentDigestMismatch)
        );
        assert!(payload_mismatch.create_keys().is_empty());
    }

    #[test]
    fn planner_rejects_invalid_range_digest_empty_and_chunk_count() {
        let segment = b"segment";
        let mut identity = segment_identity(segment);
        identity.first_lsn = 0;
        assert_eq!(
            plan_recovery_log_segment(identity, segment, 4),
            Err(RecoveryLogSegmentError::InvalidLsnRange)
        );
        identity = segment_identity(segment);
        identity.segment_digest = [0; 32];
        assert_eq!(
            plan_recovery_log_segment(identity, segment, 4),
            Err(RecoveryLogSegmentError::SegmentDigestMismatch)
        );
        assert_eq!(
            plan_recovery_log_segment(segment_identity(&[]), &[], 4),
            Err(RecoveryLogSegmentError::EmptySegment)
        );
        let too_many = vec![1; MAX_RECOVERY_LOG_SEGMENT_CHUNKS + 1];
        assert!(matches!(
            plan_recovery_log_segment(segment_identity(&too_many), &too_many, 1),
            Err(RecoveryLogSegmentError::TooManyChunks { .. })
        ));
        let too_large = vec![1; MAX_RECOVERY_LOG_SEGMENT_BYTES + 1];
        assert!(matches!(
            plan_recovery_log_segment(
                segment_identity(&too_large),
                &too_large,
                DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE,
            ),
            Err(RecoveryLogSegmentError::SegmentTooLarge { .. })
        ));
    }
}
