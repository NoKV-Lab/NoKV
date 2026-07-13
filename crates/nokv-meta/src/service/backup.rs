//! Off-node disaster recovery for the metadata namespace.
//!
//! File *bodies* already live durably in the object store, but the *namespace*
//! (inodes, dentries, versions, CoW relationships) lives in the local Holt
//! engine. Losing that node loses the namespace even though every object
//! survives in S3. This module periodically exports a Holt checkpoint image and
//! publishes it to the same object store, so a fresh node can reconstruct the
//! namespace from the archive.
//!
//! Durability discipline mirrors the data path: **object-first, pointer-second**.
//! A backup PUTs the checkpoint image, then atomically swaps a single `CURRENT`
//! manifest object to point at it. A crash between the two leaves an orphan
//! checkpoint object (reclaimed by retention on a later backup), never a manifest
//! that points at a missing checkpoint.

use super::*;
use crate::command::MetadataCheckpointStore;

const ARCHIVE_MAGIC: &str = "nokv-metadata-archive";
const ARCHIVE_FORMAT: u32 = 2;
const CHECKPOINT_PROOF_MAGIC: &str = "nokv-metadata-checkpoint-proof";
const CHECKPOINT_PROOF_FORMAT: u32 = 1;

/// Where (and how many) metadata checkpoints to keep in the object store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataArchiveConfig {
    /// Object-key prefix under which checkpoints and the manifest are stored.
    pub prefix: String,
    /// Number of most-recent checkpoints to retain; older ones are deleted.
    pub keep_last: usize,
}

impl MetadataArchiveConfig {
    pub fn new(prefix: impl Into<String>, keep_last: usize) -> Self {
        Self {
            prefix: prefix.into(),
            keep_last: keep_last.max(1),
        }
    }

    /// Validate a control-plane checkpoint identity without touching the object
    /// store. Startup uses this before acquiring a failover lease so a malformed
    /// or cross-shard reference cannot create local state or perform object I/O.
    pub fn validate_controlled_checkpoint_identity(
        &self,
        identity: &MetadataCheckpointIdentity,
    ) -> Result<(), MetadError> {
        validate_controlled_checkpoint_identity(self, identity)
    }
}

/// Result of publishing a metadata checkpoint to the archive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataBackupOutcome {
    pub checkpoint_key: String,
    pub image_bytes: u64,
    /// SHA-256 identity of the exact checkpoint image.
    pub image_digest: String,
    pub commit_version: u64,
    pub pruned: usize,
    /// Shared-log pointers retired after this checkpoint won the control CAS.
    /// Standalone backups leave this at zero because they have no control-plane
    /// publication boundary authorizing shared-log object deletion.
    pub log_segments_pruned: usize,
    /// Covered shared-log objects physically deleted after publication.
    pub log_segment_objects_deleted: usize,
    /// Covered shared-log objects that were already absent during cleanup.
    pub log_segment_objects_missing: usize,
    /// Covered shared-log objects left as safe leaks because cleanup failed.
    pub log_segment_delete_failures: usize,
    /// Logical-log position the image is consistent with, captured atomically
    /// with the image so a published `CheckpointRef` never claims an LSN beyond
    /// the image content (which would silently drop an acknowledged write on
    /// restore). `log_lsn` is a safe lower bound: the image always contains at
    /// least every command up to `log_lsn`, so replaying segments above it is at
    /// worst redundant (idempotent via command dedupe), never lossy.
    pub log_lsn: u64,
    pub log_digest: [u8; 32],
}

/// Exact immutable checkpoint identity carried by a control-plane reference.
/// Controlled restore never follows the mutable standalone `CURRENT` pointer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataCheckpointIdentity {
    pub checkpoint_key: String,
    pub image_bytes: u64,
    pub image_digest: String,
}

/// Result of restoring the namespace from the archive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataRestoreOutcome {
    pub checkpoint_key: String,
    pub image_bytes: u64,
    pub commit_version: u64,
}

/// The single `CURRENT` pointer object: which checkpoint is live, plus the
/// retained-checkpoint window so retention works without an object `list`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ArchiveManifest {
    seq: u64,
    current: String,
    version: u64,
    size: u64,
    /// True only when the checkpoint image itself contains the durable fence
    /// that prevents object GC from outrunning off-node metadata recovery.
    object_gc_failover_fenced: bool,
    recent: Vec<String>,
}

/// Immutable proof stored beside a controlled checkpoint image. Its key is
/// derived only after validating `checkpoint_key` against the shard archive
/// prefix and content digest.
#[derive(Clone, Debug, PartialEq, Eq)]
struct CheckpointProof {
    checkpoint_key: String,
    image_bytes: u64,
    image_digest: String,
    object_gc_failover_fenced: bool,
}

struct ExportedMetadataCheckpoint {
    image: Vec<u8>,
    image_bytes: u64,
    image_digest: String,
    commit_version: u64,
    log_lsn: u64,
    log_digest: [u8; 32],
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore + MetadataCheckpointStore,
    O: ObjectStore,
{
    /// Export a metadata checkpoint and publish it to the object store under
    /// `config.prefix`, retaining the most-recent `config.keep_last` checkpoints.
    pub fn backup_metadata(
        &self,
        config: &MetadataArchiveConfig,
    ) -> Result<MetadataBackupOutcome, MetadError> {
        // Every published recovery image must contain the durable fence. Doing
        // this before checkpoint export also upgrades a live pre-fence store
        // safely: current namespace references are intact, then future object
        // deletion remains disabled before CURRENT can name the new image.
        self.require_failover_durability()?;
        let keep_last = config.keep_last.max(1);
        // The manifest is a read-modify-write of the `recent` window plus a
        // sequence-derived checkpoint key, so two backups must not interleave.
        let _guard = self
            .backup_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());

        let exported = self.export_metadata_checkpoint()?;

        let manifest_key = archive_manifest_key(&config.prefix);
        let prior = self.read_archive_manifest(&manifest_key)?;
        let next_seq = prior.as_ref().map(|m| m.seq.saturating_add(1)).unwrap_or(1);
        let checkpoint_key = archive_checkpoint_key(&config.prefix, next_seq);

        // Object-first: write the checkpoint image before anything references it.
        let object_key = ObjectKey::new(checkpoint_key.clone())?;
        self.objects.put(&object_key, exported.image)?;

        // Compute the retained window; everything older than keep_last is pruned.
        let mut recent = prior.map(|m| m.recent).unwrap_or_default();
        recent.push(checkpoint_key.clone());
        let mut to_delete = Vec::new();
        while recent.len() > keep_last {
            to_delete.push(recent.remove(0));
        }

        // Pointer-second: atomically swap CURRENT to the new checkpoint.
        let manifest = ArchiveManifest {
            seq: next_seq,
            current: checkpoint_key.clone(),
            version: exported.commit_version,
            size: exported.image_bytes,
            object_gc_failover_fenced: true,
            recent,
        };
        let manifest_object = ObjectKey::new(manifest_key)?;
        self.objects.put(
            &manifest_object,
            serialize_archive_manifest(&manifest).into_bytes(),
        )?;

        // Retention deletes happen only after the manifest stops referencing
        // them; a crash here leaks orphans (reclaimable), never a live pointer.
        let mut pruned = 0;
        for stale in &to_delete {
            if let Ok(key) = ObjectKey::new(stale.clone()) {
                if self.objects.delete(&key)? {
                    pruned += 1;
                }
            }
        }

        Ok(MetadataBackupOutcome {
            checkpoint_key,
            image_bytes: exported.image_bytes,
            image_digest: exported.image_digest,
            commit_version: exported.commit_version,
            pruned,
            log_segments_pruned: 0,
            log_segment_objects_deleted: 0,
            log_segment_objects_missing: 0,
            log_segment_delete_failures: 0,
            log_lsn: exported.log_lsn,
            log_digest: exported.log_digest,
        })
    }

    /// Export one immutable, content-addressed checkpoint plus an exact proof,
    /// without reading or mutating the standalone `CURRENT` pointer. The caller
    /// must publish the returned identity through its own durable authority
    /// (the control-plane lease CAS) before pruning any prior checkpoint.
    pub fn prepare_immutable_metadata_backup(
        &self,
        config: &MetadataArchiveConfig,
    ) -> Result<MetadataBackupOutcome, MetadError> {
        self.require_failover_durability()?;
        self.ensure_owner_epoch_current()?;
        let _guard = self
            .backup_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let exported = self.export_metadata_checkpoint()?;
        self.ensure_owner_epoch_current()?;

        let checkpoint_key = controlled_checkpoint_key(&config.prefix, &exported.image_digest)?;
        let identity = MetadataCheckpointIdentity {
            checkpoint_key: checkpoint_key.clone(),
            image_bytes: exported.image_bytes,
            image_digest: exported.image_digest.clone(),
        };
        validate_controlled_checkpoint_identity(config, &identity)?;
        let image_key = ObjectKey::new(checkpoint_key.clone())?;
        put_immutable_object(&self.objects, &image_key, &exported.image)?;

        // If the local owner fence expired during the image PUT, stop before
        // publishing a proof. The content-addressed image is only an orphan.
        self.ensure_owner_epoch_current()?;
        let proof = CheckpointProof {
            checkpoint_key: checkpoint_key.clone(),
            image_bytes: exported.image_bytes,
            image_digest: exported.image_digest.clone(),
            object_gc_failover_fenced: true,
        };
        let proof_key = ObjectKey::new(checkpoint_proof_key(&identity))?;
        let encoded_proof = serialize_checkpoint_proof(&proof).into_bytes();
        put_immutable_object(&self.objects, &proof_key, &encoded_proof)?;

        Ok(MetadataBackupOutcome {
            checkpoint_key,
            image_bytes: exported.image_bytes,
            image_digest: exported.image_digest,
            commit_version: exported.commit_version,
            pruned: 0,
            log_segments_pruned: 0,
            log_segment_objects_deleted: 0,
            log_segment_objects_missing: 0,
            log_segment_delete_failures: 0,
            log_lsn: exported.log_lsn,
            log_digest: exported.log_digest,
        })
    }

    /// Delete one no-longer-authoritative immutable controlled checkpoint.
    /// Callers must first replace its exact control-plane reference by lease CAS.
    pub fn prune_immutable_metadata_backup(
        &self,
        config: &MetadataArchiveConfig,
        identity: &MetadataCheckpointIdentity,
    ) -> Result<usize, MetadError> {
        validate_controlled_checkpoint_identity(config, identity)?;
        let proof_key = ObjectKey::new(checkpoint_proof_key(identity))?;
        let image_key = ObjectKey::new(identity.checkpoint_key.clone())?;
        let _ = self.objects.delete(&proof_key)?;
        Ok(usize::from(self.objects.delete(&image_key)?))
    }

    /// Restore one exact immutable controlled checkpoint. Proof is loaded and
    /// matched to the control identity before the image object is fetched, so a
    /// legacy/misdirected ref cannot mutate the target metadata store.
    pub fn restore_metadata_checkpoint(
        &self,
        config: &MetadataArchiveConfig,
        identity: &MetadataCheckpointIdentity,
    ) -> Result<MetadataRestoreOutcome, MetadError> {
        validate_controlled_checkpoint_identity(config, identity)?;
        let proof_key = ObjectKey::new(checkpoint_proof_key(identity))?;
        if self.objects.head(&proof_key)?.is_none() {
            return Err(MetadError::MetadataArchiveMissingObjectGcFence {
                checkpoint_key: identity.checkpoint_key.clone(),
            });
        }
        let proof_bytes = self.objects.get(&proof_key, None)?;
        let proof_text = String::from_utf8(proof_bytes)
            .map_err(|_| MetadError::Codec("checkpoint proof is not valid UTF-8".to_owned()))?;
        let proof = parse_checkpoint_proof(&proof_text)?;
        if !proof.object_gc_failover_fenced {
            return Err(MetadError::MetadataArchiveMissingObjectGcFence {
                checkpoint_key: identity.checkpoint_key.clone(),
            });
        }
        if proof.checkpoint_key != identity.checkpoint_key
            || proof.image_bytes != identity.image_bytes
            || proof.image_digest != identity.image_digest
        {
            return Err(MetadError::Codec(format!(
                "checkpoint proof does not match control identity for {}",
                identity.checkpoint_key
            )));
        }

        let image_key = ObjectKey::new(identity.checkpoint_key.clone())?;
        let image = self.objects.get(&image_key, None)?;
        if image.len() as u64 != identity.image_bytes {
            return Err(MetadError::Codec(format!(
                "checkpoint image size mismatch for {}",
                identity.checkpoint_key
            )));
        }
        if checkpoint_image_digest(&image) != identity.image_digest {
            return Err(MetadError::Codec(format!(
                "checkpoint image digest mismatch for {}",
                identity.checkpoint_key
            )));
        }

        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.mark_materialization_orphan_possible_under_gc_gate();
        self.metadata.install_checkpoint_image(&image)?;
        self.purge_path_caches_after_write();
        self.refresh_allocator_state()?;
        self.verify_failover_durability_required()?;
        self.reconcile_materialization_orphan_state_under_gc_gate()?;
        Ok(MetadataRestoreOutcome {
            checkpoint_key: identity.checkpoint_key.clone(),
            image_bytes: identity.image_bytes,
            commit_version: self.clock.load(Ordering::SeqCst),
        })
    }

    fn export_metadata_checkpoint(&self) -> Result<ExportedMetadataCheckpoint, MetadError> {
        let _log_enable_fence = self
            .metadata_log_enable_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        let _commit_log_guard = self
            .metadata_commit_log_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // An image may not pass an ambiguous apply or an unarchived committed
        // segment. Resolve and flush while holding the same total-order gate
        // used to capture both its logical-log boundary and engine image.
        self.resolve_unresolved_metadata_commit_group_locked()?;
        self.flush_pending_metadata_log_segment_locked()
            .map_err(|err| MetadError::SyncLogArchiveFailed {
                committed: true,
                message: err.to_string(),
            })?;
        // Capture the logical-log boundary BEFORE exporting the image while the
        // commit/log gate is still held. Commits apply before archive, making
        // this a safe lower bound for replay.
        let (log_lsn, log_digest) = self
            .sync_metadata_log_snapshot()
            .map(|snapshot| (snapshot.durable_lsn, snapshot.last_digest))
            .unwrap_or((0, crate::METADATA_LOG_ZERO_DIGEST));
        self.metadata.checkpoint()?;
        let image = self.metadata.export_checkpoint_image()?;
        let image_bytes = image.len() as u64;
        let image_digest = checkpoint_image_digest(&image);
        Ok(ExportedMetadataCheckpoint {
            image,
            image_bytes,
            image_digest,
            commit_version: self.clock.load(Ordering::SeqCst),
            log_lsn,
            log_digest,
        })
    }

    /// Restore the namespace from the latest archived checkpoint, if any.
    ///
    /// Installs the checkpoint image into this service's metadata engine and
    /// refreshes in-memory allocator state. Intended to run on a freshly opened
    /// (empty) store while no server is serving. Returns `Ok(None)` when the
    /// archive prefix holds no manifest yet (nothing to restore).
    pub fn restore_metadata(
        &self,
        config: &MetadataArchiveConfig,
    ) -> Result<Option<MetadataRestoreOutcome>, MetadError> {
        let manifest_key = archive_manifest_key(&config.prefix);
        let Some(manifest) = self.read_archive_manifest(&manifest_key)? else {
            return Ok(None);
        };
        if !manifest.object_gc_failover_fenced {
            return Err(MetadError::MetadataArchiveMissingObjectGcFence {
                checkpoint_key: manifest.current,
            });
        }
        let object_key = ObjectKey::new(manifest.current.clone())?;
        let image = self.objects.get(&object_key, None)?;
        // install_checkpoint_image validates the image and rejects truncation or
        // corruption, so a torn archive surfaces as a metadata error here.
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.mark_materialization_orphan_possible_under_gc_gate();
        self.metadata.install_checkpoint_image(&image)?;
        // The image replaced the engine state wholesale, bypassing the commit
        // funnel; entries cached before the install may not exist in it at all.
        self.purge_path_caches_after_write();
        self.refresh_allocator_state()?;
        // The manifest proof is written only after exporting a fenced image;
        // verify the installed state as defense against a mismatched/corrupt
        // archive assembled outside the supported backup path.
        self.verify_failover_durability_required()?;
        self.reconcile_materialization_orphan_state_under_gc_gate()?;
        Ok(Some(MetadataRestoreOutcome {
            checkpoint_key: manifest.current,
            image_bytes: image.len() as u64,
            commit_version: manifest.version,
        }))
    }

    fn read_archive_manifest(
        &self,
        manifest_key: &str,
    ) -> Result<Option<ArchiveManifest>, MetadError> {
        let object_key = ObjectKey::new(manifest_key.to_owned())?;
        if self.objects.head(&object_key)?.is_none() {
            return Ok(None);
        }
        let bytes = self.objects.get(&object_key, None)?;
        let text = String::from_utf8(bytes)
            .map_err(|_| MetadError::Codec("archive manifest is not valid UTF-8".to_owned()))?;
        parse_archive_manifest(&text).map(Some)
    }
}

fn checkpoint_image_digest(image: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(image))
}

fn controlled_checkpoint_key(prefix: &str, image_digest: &str) -> Result<String, MetadError> {
    let digest = canonical_sha256_hex(image_digest)?;
    Ok(format!(
        "{}/controlled/sha256/{digest}.image",
        normalize_prefix(prefix)
    ))
}

fn validate_controlled_checkpoint_identity(
    config: &MetadataArchiveConfig,
    identity: &MetadataCheckpointIdentity,
) -> Result<(), MetadError> {
    let expected = controlled_checkpoint_key(&config.prefix, &identity.image_digest)?;
    if identity.checkpoint_key != expected {
        return Err(MetadError::Codec(format!(
            "controlled checkpoint key {} does not match archive prefix and image digest",
            identity.checkpoint_key
        )));
    }
    if identity.image_bytes == 0 {
        return Err(MetadError::Codec(
            "controlled checkpoint image size must be non-zero".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_sha256_hex(image_digest: &str) -> Result<&str, MetadError> {
    let Some(hex) = image_digest.strip_prefix("sha256:") else {
        return Err(MetadError::Codec(
            "checkpoint image digest must use sha256".to_owned(),
        ));
    };
    if hex.len() != 64
        || !hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(MetadError::Codec(
            "checkpoint image digest is not canonical SHA-256 hex".to_owned(),
        ));
    }
    Ok(hex)
}

fn checkpoint_proof_key(identity: &MetadataCheckpointIdentity) -> String {
    format!("{}.proof", identity.checkpoint_key)
}

fn put_immutable_object<O: ObjectStore>(
    objects: &O,
    key: &ObjectKey,
    expected: &[u8],
) -> Result<(), MetadError> {
    if objects.head(key)?.is_some() {
        let current = objects.get(key, None)?;
        if current != expected {
            return Err(MetadError::Codec(format!(
                "immutable metadata archive object {} already has different bytes",
                key.as_str()
            )));
        }
        return Ok(());
    }
    objects.put(key, expected.to_vec())?;
    if objects.get(key, None)? != expected {
        return Err(MetadError::Codec(format!(
            "immutable metadata archive object {} failed read-after-write verification",
            key.as_str()
        )));
    }
    Ok(())
}

fn serialize_checkpoint_proof(proof: &CheckpointProof) -> String {
    format!(
        "{CHECKPOINT_PROOF_MAGIC}\t{CHECKPOINT_PROOF_FORMAT}\ncheckpoint_key\t{}\nimage_bytes\t{}\nimage_digest\t{}\nobject_gc_failover_fenced\t{}\n",
        proof.checkpoint_key,
        proof.image_bytes,
        proof.image_digest,
        u8::from(proof.object_gc_failover_fenced),
    )
}

fn parse_checkpoint_proof(text: &str) -> Result<CheckpointProof, MetadError> {
    let mut lines = text.lines();
    let header = lines
        .next()
        .ok_or_else(|| MetadError::Codec("checkpoint proof is empty".to_owned()))?;
    let expected_header = format!("{CHECKPOINT_PROOF_MAGIC}\t{CHECKPOINT_PROOF_FORMAT}");
    if header != expected_header {
        return Err(MetadError::Codec(
            "checkpoint proof header is unsupported".to_owned(),
        ));
    }
    let mut checkpoint_key = None;
    let mut image_bytes = None;
    let mut image_digest = None;
    let mut object_gc_failover_fenced = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (tag, value) = line
            .split_once('\t')
            .ok_or_else(|| MetadError::Codec("checkpoint proof line is malformed".to_owned()))?;
        let duplicate = |name: &str| MetadError::Codec(format!("checkpoint proof repeats {name}"));
        match tag {
            "checkpoint_key" => {
                if checkpoint_key.replace(value.to_owned()).is_some() {
                    return Err(duplicate("checkpoint key"));
                }
            }
            "image_bytes" => {
                if image_bytes.replace(parse_u64(value)?).is_some() {
                    return Err(duplicate("image bytes"));
                }
            }
            "image_digest" => {
                canonical_sha256_hex(value)?;
                if image_digest.replace(value.to_owned()).is_some() {
                    return Err(duplicate("image digest"));
                }
            }
            "object_gc_failover_fenced" => {
                let fenced = match value {
                    "0" => false,
                    "1" => true,
                    _ => {
                        return Err(MetadError::Codec(
                            "checkpoint proof failover fence must be 0 or 1".to_owned(),
                        ));
                    }
                };
                if object_gc_failover_fenced.replace(fenced).is_some() {
                    return Err(duplicate("failover fence"));
                }
            }
            _ => {
                return Err(MetadError::Codec(format!(
                    "checkpoint proof has unknown tag: {tag}"
                )));
            }
        }
    }
    Ok(CheckpointProof {
        checkpoint_key: checkpoint_key
            .filter(|value| !value.is_empty())
            .ok_or_else(|| MetadError::Codec("checkpoint proof has no key".to_owned()))?,
        image_bytes: image_bytes
            .filter(|value| *value > 0)
            .ok_or_else(|| MetadError::Codec("checkpoint proof has no image bytes".to_owned()))?,
        image_digest: image_digest
            .ok_or_else(|| MetadError::Codec("checkpoint proof has no image digest".to_owned()))?,
        object_gc_failover_fenced: object_gc_failover_fenced.ok_or_else(|| {
            MetadError::Codec("checkpoint proof has no failover fence".to_owned())
        })?,
    })
}

fn normalize_prefix(prefix: &str) -> &str {
    prefix.trim_end_matches('/')
}

fn archive_manifest_key(prefix: &str) -> String {
    format!("{}/CURRENT", normalize_prefix(prefix))
}

fn archive_checkpoint_key(prefix: &str, seq: u64) -> String {
    format!("{}/ckpt/{:020}.image", normalize_prefix(prefix), seq)
}

fn serialize_archive_manifest(manifest: &ArchiveManifest) -> String {
    let mut out = String::new();
    out.push_str(&format!("{ARCHIVE_MAGIC}\t{ARCHIVE_FORMAT}\n"));
    out.push_str(&format!("seq\t{}\n", manifest.seq));
    out.push_str(&format!("current\t{}\n", manifest.current));
    out.push_str(&format!("version\t{}\n", manifest.version));
    out.push_str(&format!("size\t{}\n", manifest.size));
    out.push_str(&format!(
        "object_gc_failover_fenced\t{}\n",
        u8::from(manifest.object_gc_failover_fenced)
    ));
    for key in &manifest.recent {
        out.push_str(&format!("recent\t{key}\n"));
    }
    out
}

fn parse_archive_manifest(text: &str) -> Result<ArchiveManifest, MetadError> {
    let mut lines = text.lines();
    let header = lines
        .next()
        .ok_or_else(|| MetadError::Codec("archive manifest is empty".to_owned()))?;
    let (magic, format) = header
        .split_once('\t')
        .ok_or_else(|| MetadError::Codec("archive manifest header is malformed".to_owned()))?;
    if magic != ARCHIVE_MAGIC {
        return Err(MetadError::Codec(format!(
            "unexpected archive manifest magic: {magic}"
        )));
    }
    let format = format
        .parse::<u32>()
        .map_err(|_| MetadError::Codec("archive manifest format is not a number".to_owned()))?;
    if !(1..=ARCHIVE_FORMAT).contains(&format) {
        return Err(MetadError::Codec(format!(
            "unsupported archive manifest format: {format}"
        )));
    }
    let mut manifest = ArchiveManifest::default();
    let mut saw_current = false;
    let mut saw_object_gc_fence = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (tag, value) = line
            .split_once('\t')
            .ok_or_else(|| MetadError::Codec("archive manifest line is malformed".to_owned()))?;
        match tag {
            "seq" => manifest.seq = parse_u64(value)?,
            "current" => {
                manifest.current = value.to_owned();
                saw_current = true;
            }
            "version" => manifest.version = parse_u64(value)?,
            "size" => manifest.size = parse_u64(value)?,
            "object_gc_failover_fenced" => {
                if saw_object_gc_fence {
                    return Err(MetadError::Codec(
                        "archive manifest repeats object-GC failover fence".to_owned(),
                    ));
                }
                manifest.object_gc_failover_fenced = match value {
                    "0" => false,
                    "1" => true,
                    _ => {
                        return Err(MetadError::Codec(
                            "archive manifest object-GC failover fence must be 0 or 1".to_owned(),
                        ));
                    }
                };
                saw_object_gc_fence = true;
            }
            "recent" => manifest.recent.push(value.to_owned()),
            _ => {
                return Err(MetadError::Codec(format!(
                    "archive manifest has unknown tag: {tag}"
                )));
            }
        }
    }
    if !saw_current || manifest.current.is_empty() {
        return Err(MetadError::Codec(
            "archive manifest has no current checkpoint".to_owned(),
        ));
    }
    if format >= 2 && !saw_object_gc_fence {
        return Err(MetadError::Codec(
            "archive manifest has no object-GC failover fence proof".to_owned(),
        ));
    }
    Ok(manifest)
}

fn parse_u64(value: &str) -> Result<u64, MetadError> {
    value
        .parse::<u64>()
        .map_err(|_| MetadError::Codec(format!("archive manifest has invalid number: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_object::MemoryObjectStore;

    #[test]
    fn archive_manifest_v2_round_trips_failover_fence_proof() {
        let manifest = ArchiveManifest {
            seq: 7,
            current: "meta/ckpt/7.image".to_owned(),
            version: 11,
            size: 13,
            object_gc_failover_fenced: true,
            recent: vec!["meta/ckpt/7.image".to_owned()],
        };

        assert_eq!(
            parse_archive_manifest(&serialize_archive_manifest(&manifest)).unwrap(),
            manifest
        );
    }

    #[test]
    fn archive_manifest_v1_has_no_failover_fence_proof() {
        let manifest = parse_archive_manifest(
            "nokv-metadata-archive\t1\nseq\t1\ncurrent\tmeta/ckpt/1.image\nversion\t2\nsize\t3\nrecent\tmeta/ckpt/1.image\n",
        )
        .unwrap();

        assert!(!manifest.object_gc_failover_fenced);
    }

    #[test]
    fn archive_manifest_v2_requires_failover_fence_proof() {
        let err = parse_archive_manifest(
            "nokv-metadata-archive\t2\nseq\t1\ncurrent\tmeta/ckpt/1.image\nversion\t2\nsize\t3\n",
        )
        .unwrap_err();

        assert!(matches!(
            err,
            MetadError::Codec(message)
                if message == "archive manifest has no object-GC failover fence proof"
        ));
    }

    #[test]
    fn archive_manifest_rejects_unknown_tags() {
        let err = parse_archive_manifest(
            "nokv-metadata-archive\t1\nseq\t1\ncurrent\tmeta/ckpt/1.image\nversion\t2\nsize\t3\nextra\t4\n",
        )
        .unwrap_err();

        assert!(matches!(
            err,
            MetadError::Codec(message) if message == "archive manifest has unknown tag: extra"
        ));
    }

    #[test]
    fn checkpoint_proof_round_trips_exact_image_identity() {
        let proof = CheckpointProof {
            checkpoint_key: format!("meta/controlled/sha256/{}.image", "a".repeat(64)),
            image_bytes: 4096,
            image_digest: format!("sha256:{}", "a".repeat(64)),
            object_gc_failover_fenced: true,
        };

        assert_eq!(
            parse_checkpoint_proof(&serialize_checkpoint_proof(&proof)).unwrap(),
            proof
        );
    }

    #[test]
    fn immutable_archive_object_rejects_different_bytes_at_same_key() {
        let objects = MemoryObjectStore::new();
        let key = ObjectKey::new("meta/controlled/proof").unwrap();
        put_immutable_object(&objects, &key, b"first").unwrap();
        put_immutable_object(&objects, &key, b"first").unwrap();

        let err = put_immutable_object(&objects, &key, b"second").unwrap_err();
        assert!(matches!(
            err,
            MetadError::Codec(message) if message.contains("already has different bytes")
        ));
        assert_eq!(objects.get(&key, None).unwrap(), b"first");
    }

    #[test]
    fn controlled_identity_binds_prefix_key_and_digest() {
        let config = MetadataArchiveConfig::new("meta/shard", 4);
        let digest = format!("sha256:{}", "b".repeat(64));
        let identity = MetadataCheckpointIdentity {
            checkpoint_key: controlled_checkpoint_key(&config.prefix, &digest).unwrap(),
            image_bytes: 1024,
            image_digest: digest,
        };
        validate_controlled_checkpoint_identity(&config, &identity).unwrap();

        let cross_prefix = MetadataArchiveConfig::new("meta/other", 4);
        assert!(validate_controlled_checkpoint_identity(&cross_prefix, &identity).is_err());
    }
}
