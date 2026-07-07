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
const ARCHIVE_FORMAT: u32 = 1;

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
}

/// Result of publishing a metadata checkpoint to the archive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataBackupOutcome {
    pub checkpoint_key: String,
    pub image_bytes: u64,
    pub commit_version: u64,
    pub pruned: usize,
    /// Logical-log position the image is consistent with, captured atomically
    /// with the image so a published `CheckpointRef` never claims an LSN beyond
    /// the image content (which would silently drop an acknowledged write on
    /// restore). `log_lsn` is a safe lower bound: the image always contains at
    /// least every command up to `log_lsn`, so replaying segments above it is at
    /// worst redundant (idempotent via command dedupe), never lossy.
    pub log_lsn: u64,
    pub log_digest: [u8; 32],
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
    recent: Vec<String>,
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
        let keep_last = config.keep_last.max(1);
        // The manifest is a read-modify-write of the `recent` window plus a
        // sequence-derived checkpoint key, so two backups must not interleave.
        let _guard = self
            .backup_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());

        // Capture the logical-log boundary BEFORE exporting the image. Commits
        // apply to the engine before they archive (so durable_lsn <= applied
        // set), which makes this a safe lower bound: the image is guaranteed to
        // contain at least everything up to (log_lsn, log_digest). Reading it
        // after the export (as the server used to) could capture an LSN beyond
        // the image and lose that write on restore.
        let (log_lsn, log_digest) = self
            .sync_metadata_log_snapshot()
            .map(|snapshot| (snapshot.durable_lsn, snapshot.last_digest))
            .unwrap_or((0, crate::METADATA_LOG_ZERO_DIGEST));

        // Fold the WAL into a durable checkpoint, then export that image.
        self.metadata.checkpoint()?;
        let image = self.metadata.export_checkpoint_image()?;
        let commit_version = self.clock.load(Ordering::SeqCst);

        let manifest_key = archive_manifest_key(&config.prefix);
        let prior = self.read_archive_manifest(&manifest_key)?;
        let next_seq = prior.as_ref().map(|m| m.seq.saturating_add(1)).unwrap_or(1);
        let checkpoint_key = archive_checkpoint_key(&config.prefix, next_seq);

        // Object-first: write the checkpoint image before anything references it.
        let object_key = ObjectKey::new(checkpoint_key.clone())?;
        let image_size = image.len() as u64;
        self.objects.put(&object_key, image)?;

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
            version: commit_version,
            size: image_size,
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
            image_bytes: image_size,
            commit_version,
            pruned,
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
        let object_key = ObjectKey::new(manifest.current.clone())?;
        let image = self.objects.get(&object_key, None)?;
        // install_checkpoint_image validates the image and rejects truncation or
        // corruption, so a torn archive surfaces as a metadata error here.
        self.metadata.install_checkpoint_image(&image)?;
        // The image replaced the engine state wholesale, bypassing the commit
        // funnel; entries cached before the install may not exist in it at all.
        self.purge_path_caches_after_write();
        self.refresh_allocator_state()?;
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
    let (magic, _format) = header
        .split_once('\t')
        .ok_or_else(|| MetadError::Codec("archive manifest header is malformed".to_owned()))?;
    if magic != ARCHIVE_MAGIC {
        return Err(MetadError::Codec(format!(
            "unexpected archive manifest magic: {magic}"
        )));
    }
    let mut manifest = ArchiveManifest::default();
    let mut saw_current = false;
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
}
