use std::fmt;

use nokv_types::{ArtifactRevisionId, LogicalShardId, RootId};

use crate::cache::ArtifactBlockCache;
use crate::digest::{hex, sha256};
use crate::store::{
    ArtifactObjectStore, ImmutableCreateOutcome, ObjectDeleteOutcome, ObjectError, ObjectKey,
    ObjectRange,
};

pub const DEFAULT_ARTIFACT_BLOCK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactKeyspace {
    pub logical_shard_id: LogicalShardId,
    pub root_id: RootId,
    pub artifact_revision_id: ArtifactRevisionId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactBlock {
    pub owner_revision_id: ArtifactRevisionId,
    pub object_index: u64,
    pub logical_offset: u64,
    pub len: u64,
    pub sha256: [u8; 32],
    pub key: ObjectKey,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactManifest {
    pub logical_shard_id: LogicalShardId,
    pub root_id: RootId,
    pub artifact_revision_id: ArtifactRevisionId,
    pub logical_len: u64,
    pub sha256: [u8; 32],
    pub blocks: Vec<ArtifactBlock>,
}

/// Bounded, contiguous manifest proof for one requested logical byte range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactReadWindow {
    pub logical_shard_id: LogicalShardId,
    pub root_id: RootId,
    pub artifact_revision_id: ArtifactRevisionId,
    pub artifact_logical_len: u64,
    pub blocks: Vec<ArtifactBlock>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactUploadOptions {
    pub logical_shard_id: LogicalShardId,
    pub root_id: RootId,
    pub artifact_revision_id: ArtifactRevisionId,
    pub block_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactUploadPlan {
    pub manifest: ArtifactManifest,
    pub block_size: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedArtifactObjects {
    logical_shard_id: LogicalShardId,
    root_id: RootId,
    artifact_revision_id: ArtifactRevisionId,
    keys: Vec<ObjectKey>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArtifactUploadStats {
    pub blocks: u64,
    pub bytes: u64,
    pub created: u64,
    pub replayed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactUpload {
    pub manifest: ArtifactManifest,
    pub staged: StagedArtifactObjects,
    pub stats: ArtifactUploadStats,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArtifactUploadFailure {
    pub source: Box<ObjectError>,
    pub staged: StagedArtifactObjects,
    pub stats: ArtifactUploadStats,
}

impl ArtifactUploadFailure {
    pub fn retryable(&self) -> bool {
        self.source.retryable()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactBlockRead {
    pub key: ObjectKey,
    pub expected_len: usize,
    pub expected_sha256: [u8; 32],
    pub source_offset: usize,
    pub copy_len: usize,
    pub output_offset: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactReadPlan {
    pub requested_offset: u64,
    pub output_len: usize,
    pub blocks: Vec<ArtifactBlockRead>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArtifactReadStats {
    pub planned_blocks: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub store_reads: u64,
    pub store_read_bytes: u64,
    pub verified_blocks: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactReadOutcome {
    pub bytes: Vec<u8>,
    pub stats: ArtifactReadStats,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArtifactCleanupOutcome {
    pub attempted: u64,
    pub deleted: u64,
    pub absent: u64,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ArtifactCleanupFailure {
    pub outcome: ArtifactCleanupOutcome,
    pub failed_key: ObjectKey,
    pub source: Box<ObjectError>,
}

impl ArtifactKeyspace {
    pub const fn new(
        logical_shard_id: LogicalShardId,
        root_id: RootId,
        artifact_revision_id: ArtifactRevisionId,
    ) -> Self {
        Self {
            logical_shard_id,
            root_id,
            artifact_revision_id,
        }
    }

    pub fn permanent_block_key(self, object_index: u64) -> ObjectKey {
        ObjectKey::new(format!(
            "nokv/artifacts/{}/{}/{}/blocks/{object_index:016x}",
            hex(self.logical_shard_id.as_bytes()),
            hex(self.root_id.as_bytes()),
            hex(self.artifact_revision_id.as_bytes())
        ))
        .expect("internally generated artifact object key is valid")
    }

    /// Recover the unique object index from one exact permanent key.
    pub fn object_index(self, key: &ObjectKey) -> Result<u64, ObjectError> {
        let prefix = self.revision_prefix();
        let Some(encoded) = key.as_str().strip_prefix(&prefix) else {
            return Err(ObjectError::InvalidManifest(format!(
                "object key {key} is outside the expected revision namespace"
            )));
        };
        if encoded.len() != 16
            || !encoded
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
        {
            return Err(ObjectError::InvalidManifest(format!(
                "object key {key} does not end in one canonical object index"
            )));
        }
        u64::from_str_radix(encoded, 16).map_err(|error| {
            ObjectError::InvalidManifest(format!(
                "object key {key} has an invalid object index: {error}"
            ))
        })
    }

    fn revision_prefix(self) -> String {
        format!(
            "nokv/artifacts/{}/{}/{}/blocks/",
            hex(self.logical_shard_id.as_bytes()),
            hex(self.root_id.as_bytes()),
            hex(self.artifact_revision_id.as_bytes())
        )
    }
}

impl ArtifactManifest {
    pub fn keyspace(&self) -> ArtifactKeyspace {
        ArtifactKeyspace {
            logical_shard_id: self.logical_shard_id,
            root_id: self.root_id,
            artifact_revision_id: self.artifact_revision_id,
        }
    }

    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.logical_len == 0 {
            if self.blocks.is_empty() && self.sha256 == sha256(&[]) {
                return Ok(());
            }
            return Err(ObjectError::InvalidManifest(
                "empty artifact must have no blocks and the empty digest".to_owned(),
            ));
        }
        if self.blocks.is_empty() {
            return Err(ObjectError::InvalidManifest(
                "non-empty artifact has no blocks".to_owned(),
            ));
        }

        let keyspace = self.keyspace();
        let mut next_offset = 0_u64;
        for block in &self.blocks {
            if block.len == 0 {
                return Err(ObjectError::InvalidManifest(
                    "artifact block length is zero".to_owned(),
                ));
            }
            if block.logical_offset != next_offset {
                return Err(ObjectError::InvalidManifest(format!(
                    "artifact blocks are not contiguous at logical offset {next_offset}"
                )));
            }
            let expected_key = ArtifactKeyspace {
                artifact_revision_id: block.owner_revision_id,
                ..keyspace
            }
            .permanent_block_key(block.object_index);
            if block.key != expected_key {
                return Err(ObjectError::InvalidManifest(format!(
                    "block key {} is not owned by logical shard, root, and revision",
                    block.key
                )));
            }
            next_offset = next_offset.checked_add(block.len).ok_or_else(|| {
                ObjectError::InvalidManifest("artifact logical length overflows".to_owned())
            })?;
        }
        if next_offset != self.logical_len {
            return Err(ObjectError::InvalidManifest(format!(
                "artifact blocks cover {next_offset} bytes, manifest declares {}",
                self.logical_len
            )));
        }
        Ok(())
    }
}

impl ArtifactReadWindow {
    fn validate_for_range(&self, offset: u64, len: usize) -> Result<(), ObjectError> {
        let requested = ObjectRange::new(offset, len)?;
        requested.validate_within(self.artifact_logical_len)?;
        let requested_end = requested.end()?;
        if self.blocks.is_empty() {
            return Err(ObjectError::InvalidManifest(
                "artifact read window has no blocks".to_owned(),
            ));
        }

        let mut previous_end = None;
        for block in &self.blocks {
            if block.len == 0 {
                return Err(ObjectError::InvalidManifest(
                    "artifact read-window block length is zero".to_owned(),
                ));
            }
            let block_end = block.logical_offset.checked_add(block.len).ok_or_else(|| {
                ObjectError::InvalidManifest(
                    "artifact read-window block range overflows".to_owned(),
                )
            })?;
            let expected_key = ArtifactKeyspace {
                logical_shard_id: self.logical_shard_id,
                root_id: self.root_id,
                artifact_revision_id: block.owner_revision_id,
            }
            .permanent_block_key(block.object_index);
            if block.key != expected_key {
                return Err(ObjectError::InvalidManifest(format!(
                    "read-window block key {} is outside its declared owner namespace",
                    block.key
                )));
            }
            if let Some(previous_end) = previous_end {
                if block.logical_offset != previous_end {
                    return Err(ObjectError::InvalidManifest(format!(
                        "artifact read window has a gap or overlap at logical offset {}",
                        block.logical_offset
                    )));
                }
            }
            if block.logical_offset >= requested_end || block_end <= offset {
                return Err(ObjectError::InvalidManifest(format!(
                    "artifact read-window block at {} does not intersect the requested range",
                    block.logical_offset
                )));
            }
            previous_end = Some(block_end);
        }

        let first = self
            .blocks
            .first()
            .expect("non-empty read window was validated");
        let first_end = first
            .logical_offset
            .checked_add(first.len)
            .expect("block range was validated");
        if first.logical_offset > offset || first_end <= offset {
            return Err(ObjectError::InvalidManifest(
                "artifact read window does not cover the requested start".to_owned(),
            ));
        }
        if previous_end.is_none_or(|end| end < requested_end) {
            return Err(ObjectError::InvalidManifest(
                "artifact read window does not cover the requested end".to_owned(),
            ));
        }
        Ok(())
    }
}

impl ArtifactUploadOptions {
    pub fn new(
        logical_shard_id: LogicalShardId,
        root_id: RootId,
        artifact_revision_id: ArtifactRevisionId,
    ) -> Self {
        Self {
            logical_shard_id,
            root_id,
            artifact_revision_id,
            block_size: DEFAULT_ARTIFACT_BLOCK_SIZE,
        }
    }

    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.block_size = block_size;
        self
    }

    fn validate(self) -> Result<(), ObjectError> {
        if self.block_size == 0 {
            return Err(ObjectError::InvalidManifest(
                "artifact upload block size is zero".to_owned(),
            ));
        }
        Ok(())
    }

    fn keyspace(self) -> ArtifactKeyspace {
        ArtifactKeyspace {
            logical_shard_id: self.logical_shard_id,
            root_id: self.root_id,
            artifact_revision_id: self.artifact_revision_id,
        }
    }
}

impl ArtifactUploadPlan {
    pub fn validate(&self) -> Result<(), ObjectError> {
        self.manifest.validate()?;
        if self.block_size == 0 {
            return Err(ObjectError::InvalidManifest(
                "artifact upload plan block size is zero".to_owned(),
            ));
        }
        for (expected_index, block) in (0_u64..).zip(&self.manifest.blocks) {
            if block.owner_revision_id != self.manifest.artifact_revision_id {
                return Err(ObjectError::InvalidManifest(
                    "artifact upload plan references a block owned by another revision".to_owned(),
                ));
            }
            if block.object_index != expected_index {
                return Err(ObjectError::InvalidManifest(format!(
                    "artifact upload plan expected object index {expected_index}, got {}",
                    block.object_index
                )));
            }
            let block_len = usize::try_from(block.len).map_err(|_| {
                ObjectError::InvalidManifest("artifact upload block is not addressable".to_owned())
            })?;
            let is_last = expected_index as usize + 1 == self.manifest.blocks.len();
            if block_len > self.block_size || (!is_last && block_len != self.block_size) {
                return Err(ObjectError::InvalidManifest(format!(
                    "artifact upload block {expected_index} violates planned block size {}",
                    self.block_size
                )));
            }
        }
        Ok(())
    }

    fn validate_payload(&self, bytes: &[u8]) -> Result<(), ObjectError> {
        self.validate()?;
        if bytes.len() as u64 != self.manifest.logical_len {
            return Err(ObjectError::InvalidManifest(format!(
                "artifact upload payload has {} bytes, plan declares {}",
                bytes.len(),
                self.manifest.logical_len
            )));
        }
        let actual_artifact_digest = sha256(bytes);
        if actual_artifact_digest != self.manifest.sha256 {
            return Err(ObjectError::DigestMismatch {
                key: artifact_identity_key(&self.manifest),
                expected_sha256: hex(&self.manifest.sha256),
                actual_sha256: hex(&actual_artifact_digest),
            });
        }
        for block in &self.manifest.blocks {
            let start = usize::try_from(block.logical_offset).map_err(|_| {
                ObjectError::InvalidManifest("artifact upload offset is not addressable".to_owned())
            })?;
            let block_len = usize::try_from(block.len).map_err(|_| {
                ObjectError::InvalidManifest("artifact upload block is not addressable".to_owned())
            })?;
            let end = start.checked_add(block_len).ok_or_else(|| {
                ObjectError::InvalidManifest("artifact upload range overflows".to_owned())
            })?;
            let actual_block_digest = sha256(&bytes[start..end]);
            if actual_block_digest != block.sha256 {
                return Err(ObjectError::DigestMismatch {
                    key: block.key.clone(),
                    expected_sha256: hex(&block.sha256),
                    actual_sha256: hex(&actual_block_digest),
                });
            }
        }
        Ok(())
    }
}

impl StagedArtifactObjects {
    fn empty_manifest(manifest: &ArtifactManifest) -> Self {
        Self {
            logical_shard_id: manifest.logical_shard_id,
            root_id: manifest.root_id,
            artifact_revision_id: manifest.artifact_revision_id,
            keys: Vec::new(),
        }
    }

    /// Rebuild a cleanup set from durable upload-operation state.
    pub fn from_keys(
        logical_shard_id: LogicalShardId,
        root_id: RootId,
        artifact_revision_id: ArtifactRevisionId,
        keys: Vec<ObjectKey>,
    ) -> Result<Self, ObjectError> {
        let staged = Self {
            logical_shard_id,
            root_id,
            artifact_revision_id,
            keys,
        };
        staged.validate()?;
        Ok(staged)
    }

    pub fn logical_shard_id(&self) -> LogicalShardId {
        self.logical_shard_id
    }

    pub fn root_id(&self) -> RootId {
        self.root_id
    }

    pub fn artifact_revision_id(&self) -> ArtifactRevisionId {
        self.artifact_revision_id
    }

    pub fn keys(&self) -> &[ObjectKey] {
        &self.keys
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    pub fn len(&self) -> usize {
        self.keys.len()
    }

    fn push(&mut self, key: ObjectKey) {
        self.keys.push(key);
    }

    fn validate(&self) -> Result<(), ObjectError> {
        let keyspace = ArtifactKeyspace {
            logical_shard_id: self.logical_shard_id,
            root_id: self.root_id,
            artifact_revision_id: self.artifact_revision_id,
        };
        let prefix = keyspace.revision_prefix();
        if self
            .keys
            .iter()
            .any(|key| !key.as_str().starts_with(&prefix))
        {
            return Err(ObjectError::InvalidManifest(
                "staged artifact contains an object outside its revision prefix".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Compute the complete immutable object layout without performing object I/O.
pub fn plan_artifact_upload(
    options: ArtifactUploadOptions,
    bytes: &[u8],
) -> Result<ArtifactUploadPlan, ObjectError> {
    options.validate()?;
    let keyspace = options.keyspace();
    let mut blocks = Vec::new();

    for (object_index, block_bytes) in (0_u64..).zip(bytes.chunks(options.block_size)) {
        let key = keyspace.permanent_block_key(object_index);
        let logical_offset = object_index
            .checked_mul(options.block_size as u64)
            .expect("artifact byte length is bounded by addressable memory");
        blocks.push(ArtifactBlock {
            owner_revision_id: options.artifact_revision_id,
            object_index,
            logical_offset,
            len: block_bytes.len() as u64,
            sha256: sha256(block_bytes),
            key,
        });
    }

    let manifest = ArtifactManifest {
        logical_shard_id: options.logical_shard_id,
        root_id: options.root_id,
        artifact_revision_id: options.artifact_revision_id,
        logical_len: bytes.len() as u64,
        sha256: sha256(bytes),
        blocks,
    };
    let plan = ArtifactUploadPlan {
        manifest,
        block_size: options.block_size,
    };
    plan.validate()?;
    Ok(plan)
}

/// Execute a previously computed upload plan.
///
/// The entire plan and payload are verified before the first object create, so
/// caller ordering can be `metadata begin -> stage planned rows -> object
/// upload -> mark uploaded -> stage manifest -> metadata complete` without a
/// hidden pre-begin write.
pub fn upload_artifact_from_plan(
    store: &dyn ArtifactObjectStore,
    plan: &ArtifactUploadPlan,
    bytes: &[u8],
) -> Result<ArtifactUpload, ArtifactUploadFailure> {
    if let Err(source) = plan.validate_payload(bytes) {
        return Err(ArtifactUploadFailure {
            source: Box::new(source),
            staged: StagedArtifactObjects::empty_manifest(&plan.manifest),
            stats: ArtifactUploadStats::default(),
        });
    }

    let mut staged = StagedArtifactObjects::empty_manifest(&plan.manifest);
    let mut stats = ArtifactUploadStats {
        blocks: plan.manifest.blocks.len() as u64,
        bytes: bytes.len() as u64,
        ..ArtifactUploadStats::default()
    };
    for block in &plan.manifest.blocks {
        let start = usize::try_from(block.logical_offset)
            .expect("validated artifact upload offset is addressable");
        let block_len =
            usize::try_from(block.len).expect("validated artifact upload block is addressable");
        let end = start
            .checked_add(block_len)
            .expect("validated artifact upload range does not overflow");
        match store.create_immutable(&block.key, &bytes[start..end]) {
            Ok(ImmutableCreateOutcome::Created) => {
                stats.created = stats.created.saturating_add(1);
            }
            Ok(ImmutableCreateOutcome::Replayed) => {
                stats.replayed = stats.replayed.saturating_add(1);
            }
            Err(source) => {
                return Err(ArtifactUploadFailure {
                    source: Box::new(source),
                    staged,
                    stats,
                });
            }
        }
        staged.push(block.key.clone());
    }

    Ok(ArtifactUpload {
        manifest: plan.manifest.clone(),
        staged,
        stats,
    })
}

pub fn plan_artifact_read(
    manifest: &ArtifactManifest,
    offset: u64,
    len: usize,
) -> Result<ArtifactReadPlan, ObjectError> {
    manifest.validate()?;
    plan_block_range(&manifest.blocks, manifest.logical_len, offset, len)
}

fn plan_block_range(
    manifest_blocks: &[ArtifactBlock],
    artifact_logical_len: u64,
    offset: u64,
    len: usize,
) -> Result<ArtifactReadPlan, ObjectError> {
    let requested = ObjectRange::new(offset, len)?;
    requested.validate_within(artifact_logical_len)?;
    let requested_end = requested.end()?;
    let mut blocks = Vec::new();

    for block in manifest_blocks {
        let block_end = block.logical_offset.checked_add(block.len).ok_or_else(|| {
            ObjectError::InvalidManifest("artifact block range overflows".to_owned())
        })?;
        let overlap_start = block.logical_offset.max(offset);
        let overlap_end = block_end.min(requested_end);
        if overlap_start >= overlap_end {
            continue;
        }
        let expected_len = usize::try_from(block.len).map_err(|_| {
            ObjectError::InvalidManifest("artifact block is not addressable".to_owned())
        })?;
        let source_offset =
            usize::try_from(overlap_start - block.logical_offset).map_err(|_| {
                ObjectError::InvalidManifest("read offset is not addressable".to_owned())
            })?;
        let copy_len = usize::try_from(overlap_end - overlap_start).map_err(|_| {
            ObjectError::InvalidManifest("read length is not addressable".to_owned())
        })?;
        let output_offset = usize::try_from(overlap_start - offset).map_err(|_| {
            ObjectError::InvalidManifest("read output offset is not addressable".to_owned())
        })?;
        blocks.push(ArtifactBlockRead {
            key: block.key.clone(),
            expected_len,
            expected_sha256: block.sha256,
            source_offset,
            copy_len,
            output_offset,
        });
    }
    Ok(ArtifactReadPlan {
        requested_offset: offset,
        output_len: len,
        blocks,
    })
}

pub fn read_artifact(
    store: &dyn ArtifactObjectStore,
    cache: Option<&dyn ArtifactBlockCache>,
    manifest: &ArtifactManifest,
) -> Result<ArtifactReadOutcome, ObjectError> {
    manifest.validate()?;
    if manifest.logical_len == 0 {
        return Ok(ArtifactReadOutcome {
            bytes: Vec::new(),
            stats: ArtifactReadStats::default(),
        });
    }
    let len = usize::try_from(manifest.logical_len).map_err(|_| {
        ObjectError::InvalidManifest("artifact length is not addressable".to_owned())
    })?;
    read_artifact_range(store, cache, manifest, 0, len)
}

pub fn read_artifact_range(
    store: &dyn ArtifactObjectStore,
    cache: Option<&dyn ArtifactBlockCache>,
    manifest: &ArtifactManifest,
    offset: u64,
    len: usize,
) -> Result<ArtifactReadOutcome, ObjectError> {
    let plan = plan_artifact_read(manifest, offset, len)?;
    let outcome = execute_read_plan(store, cache, &plan, manifest.logical_len)?;
    if offset == 0 && len as u64 == manifest.logical_len {
        verify_artifact_bytes(manifest, &outcome.bytes)?;
    }
    Ok(outcome)
}

fn execute_read_plan(
    store: &dyn ArtifactObjectStore,
    cache: Option<&dyn ArtifactBlockCache>,
    plan: &ArtifactReadPlan,
    artifact_logical_len: u64,
) -> Result<ArtifactReadOutcome, ObjectError> {
    let mut output = vec![0_u8; plan.output_len];
    let mut stats = ArtifactReadStats {
        planned_blocks: plan.blocks.len() as u64,
        ..ArtifactReadStats::default()
    };

    for block in &plan.blocks {
        let bytes = read_verified_block(store, cache, block, &mut stats)?;
        let source_end =
            block
                .source_offset
                .checked_add(block.copy_len)
                .ok_or(ObjectError::InvalidRange {
                    offset: plan.requested_offset,
                    len: plan.output_len,
                    object_size: Some(artifact_logical_len),
                })?;
        let output_end =
            block
                .output_offset
                .checked_add(block.copy_len)
                .ok_or(ObjectError::InvalidRange {
                    offset: plan.requested_offset,
                    len: plan.output_len,
                    object_size: Some(artifact_logical_len),
                })?;
        output[block.output_offset..output_end]
            .copy_from_slice(&bytes[block.source_offset..source_end]);
    }

    Ok(ArtifactReadOutcome {
        bytes: output,
        stats,
    })
}

/// Read one exact logical range from a bounded, already-authorized manifest
/// window. Every supplied row must intersect the request and the rows must
/// prove contiguous coverage; missing bytes are never synthesized.
pub fn read_artifact_window(
    store: &dyn ArtifactObjectStore,
    cache: Option<&dyn ArtifactBlockCache>,
    window: &ArtifactReadWindow,
    offset: u64,
    len: usize,
) -> Result<ArtifactReadOutcome, ObjectError> {
    window.validate_for_range(offset, len)?;
    let plan = plan_block_range(&window.blocks, window.artifact_logical_len, offset, len)?;
    execute_read_plan(store, cache, &plan, window.artifact_logical_len)
}

/// Verify a complete logical body against its canonical manifest without I/O.
pub fn verify_artifact_bytes(manifest: &ArtifactManifest, bytes: &[u8]) -> Result<(), ObjectError> {
    manifest.validate()?;
    if bytes.len() as u64 != manifest.logical_len {
        return Err(ObjectError::InvalidManifest(format!(
            "artifact body has {} bytes, manifest declares {}",
            bytes.len(),
            manifest.logical_len
        )));
    }
    let actual = sha256(bytes);
    if actual != manifest.sha256 {
        return Err(ObjectError::DigestMismatch {
            key: artifact_identity_key(manifest),
            expected_sha256: hex(&manifest.sha256),
            actual_sha256: hex(&actual),
        });
    }
    Ok(())
}

fn artifact_identity_key(manifest: &ArtifactManifest) -> ObjectKey {
    ObjectKey::new(format!(
        "nokv/artifacts/{}/{}/{}",
        hex(manifest.logical_shard_id.as_bytes()),
        hex(manifest.root_id.as_bytes()),
        hex(manifest.artifact_revision_id.as_bytes())
    ))
    .expect("internally generated artifact identity is valid")
}

fn read_verified_block(
    store: &dyn ArtifactObjectStore,
    cache: Option<&dyn ArtifactBlockCache>,
    block: &ArtifactBlockRead,
    stats: &mut ArtifactReadStats,
) -> Result<Vec<u8>, ObjectError> {
    if let Some(cache) = cache {
        match cache.get(&block.key)? {
            Some(bytes)
                if bytes.len() == block.expected_len && sha256(&bytes) == block.expected_sha256 =>
            {
                stats.cache_hits = stats.cache_hits.saturating_add(1);
                stats.verified_blocks = stats.verified_blocks.saturating_add(1);
                return Ok(bytes);
            }
            Some(_) => {
                cache.remove(&block.key)?;
                stats.cache_misses = stats.cache_misses.saturating_add(1);
            }
            None => {
                stats.cache_misses = stats.cache_misses.saturating_add(1);
            }
        }
    }

    let range = ObjectRange::new(0, block.expected_len)?;
    let bytes = store.read(&block.key, Some(range))?;
    stats.store_reads = stats.store_reads.saturating_add(1);
    stats.store_read_bytes = stats.store_read_bytes.saturating_add(bytes.len() as u64);
    let actual = sha256(&bytes);
    if actual != block.expected_sha256 {
        return Err(ObjectError::DigestMismatch {
            key: block.key.clone(),
            expected_sha256: hex(&block.expected_sha256),
            actual_sha256: hex(&actual),
        });
    }
    stats.verified_blocks = stats.verified_blocks.saturating_add(1);
    if let Some(cache) = cache {
        cache.insert(block.key.clone(), bytes.clone())?;
    }
    Ok(bytes)
}

pub fn cleanup_staged_artifact(
    store: &dyn ArtifactObjectStore,
    staged: &StagedArtifactObjects,
) -> Result<ArtifactCleanupOutcome, ArtifactCleanupFailure> {
    if let Err(source) = staged.validate() {
        let failed_key = staged.keys.first().cloned().unwrap_or_else(|| {
            ArtifactKeyspace {
                logical_shard_id: staged.logical_shard_id,
                root_id: staged.root_id,
                artifact_revision_id: staged.artifact_revision_id,
            }
            .permanent_block_key(0)
        });
        return Err(ArtifactCleanupFailure {
            outcome: ArtifactCleanupOutcome::default(),
            failed_key,
            source: Box::new(source),
        });
    }

    let mut outcome = ArtifactCleanupOutcome::default();
    for key in &staged.keys {
        outcome.attempted = outcome.attempted.saturating_add(1);
        match store.delete(key) {
            Ok(ObjectDeleteOutcome::Deleted) => {
                outcome.deleted = outcome.deleted.saturating_add(1);
            }
            Ok(ObjectDeleteOutcome::Absent) => {
                outcome.absent = outcome.absent.saturating_add(1);
            }
            Err(source) => {
                return Err(ArtifactCleanupFailure {
                    outcome,
                    failed_key: key.clone(),
                    source: Box::new(source),
                });
            }
        }
    }
    Ok(outcome)
}

impl fmt::Display for ArtifactUploadFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "artifact upload failed after {} staged objects: {}",
            self.staged.len(),
            self.source
        )
    }
}

impl fmt::Debug for ArtifactUploadFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactUploadFailure")
            .field("staged_objects", &self.staged.len())
            .field("stats", &self.stats)
            .field("source", &self.source)
            .finish()
    }
}

impl std::error::Error for ArtifactUploadFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl fmt::Display for ArtifactCleanupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "artifact cleanup stopped after {} attempts: {}",
            self.outcome.attempted, self.source
        )
    }
}

impl fmt::Debug for ArtifactCleanupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ArtifactCleanupFailure")
            .field("outcome", &self.outcome)
            .field("source", &self.source)
            .finish()
    }
}

impl std::error::Error for ArtifactCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
