//! Durable, append-only journal of pending write-back publishes.
//!
//! In opt-in write-back mode, `flush`/`release` acknowledges once the write
//! blocks are durably staged in the writeback cache AND a `PendingPublishRecord`
//! has been fsync'd here. A background worker later uploads the blocks and
//! commits the metadata manifest, then appends a tombstone. On mount,
//! [`PublishJournal::replay`] returns the still-pending records so they can be
//! re-driven — this is the crash-recovery source of truth.
//!
//! Frame layout (append-only): `[u8 kind][u32 payload_len][32-byte sha256][payload]`.
//! A crash mid-append leaves a truncated tail; `replay` stops at the first frame
//! whose header or payload is incomplete, or whose sha256 does not match — so a
//! torn or corrupt tail is silently dropped without losing earlier records.
//! A complete record with an unsupported schema version instead fails closed.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use sha2::{Digest, Sha256};

const KIND_PUBLISH: u8 = 1;
const KIND_TOMBSTONE: u8 = 2;
// Version 2 adds the durable object-GC claim required to reconstruct a
// prepared artifact after restart. Older records cannot be replayed safely
// once metadata publish requires this claim, so replay rejects them instead of
// silently treating them as a torn tail.
const RECORD_VERSION: u8 = 2;
const FRAME_HEADER_LEN: usize = 1 + 4 + 32;
const TOMBSTONE_PAYLOAD_LEN: usize = 16;
pub(crate) const JOURNAL_FILE_NAME: &str = "publish.journal";

/// The on-disk writeback-cache block backing one staged range, recorded so
/// recovery can `WritebackCache::reinsert` it (after a restart wipes the
/// in-memory ticket index) and re-stage it at `logical_offset`. `cache_key` is
/// the cache's content key (needed to decode the file); `len` is its byte count.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheFileRef {
    pub logical_offset: u64,
    pub cache_key: String,
    pub file_name: String,
    pub len: u64,
}

/// Everything needed to re-drive one generation's publish without the in-memory
/// `WriteHandle`. Recovery re-stages the `cache_files` (reconstructing the chunk
/// manifest from the upload), so no chunk tree is stored here. The prepared
/// artifact is rebuilt from these scalars; `dentry_version`/`old_generation` of
/// 0 mean "none" (a create vs a replace).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PendingPublishRecord {
    pub inode: u64,
    pub parent: u64,
    pub name: Vec<u8>,
    pub mount: u64,
    pub generation: u64,
    pub mtime_ms: u64,
    pub ctime_ms: u64,
    pub replace: bool,
    pub dentry_version: u64,
    pub old_generation: u64,
    pub object_gc_claim_version: u64,
    pub size: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub manifest_id: String,
    pub cache_files: Vec<CacheFileRef>,
}

pub(crate) struct PublishJournal {
    path: PathBuf,
    file: Mutex<File>,
}

impl PublishJournal {
    pub fn open(root: &Path) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let path = root.join(JOURNAL_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
        })
    }

    /// Append + fsync a pending-publish record. This is the durability anchor:
    /// the caller may ack the write only after this returns.
    pub fn append_publish(&self, record: &PendingPublishRecord) -> io::Result<()> {
        if record.object_gc_claim_version == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "publish journal record has no object GC claim version",
            ));
        }
        self.append_frame(KIND_PUBLISH, &encode_record(record))
    }

    /// Append + fsync a tombstone retiring a published generation.
    pub fn append_tombstone(&self, inode: u64, generation: u64) -> io::Result<()> {
        let mut payload = Vec::with_capacity(16);
        put_u64(&mut payload, inode);
        put_u64(&mut payload, generation);
        self.append_frame(KIND_TOMBSTONE, &payload)
    }

    fn append_frame(&self, kind: u8, payload: &[u8]) -> io::Result<()> {
        let frame = encode_frame(kind, payload)?;
        let mut file = self.file.lock().unwrap_or_else(|err| err.into_inner());
        file.write_all(&frame)?;
        file.sync_all()
    }

    /// The records still pending: every published generation that has not been
    /// tombstoned, in append order. Stops at the first torn/corrupt frame.
    pub fn replay(&self) -> io::Result<Vec<PendingPublishRecord>> {
        let bytes = fs::read(&self.path)?;
        let mut reader = FrameReader::new(&bytes);
        let mut records: Vec<PendingPublishRecord> = Vec::new();
        let mut tombstones: HashSet<(u64, u64)> = HashSet::new();
        while let Some((kind, payload)) = reader.next_frame() {
            match kind {
                KIND_PUBLISH => {
                    if let Some(version) = payload.first().copied() {
                        if version != RECORD_VERSION {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "unsupported publish journal record version {version}; expected {RECORD_VERSION}"
                                ),
                            ));
                        }
                    }
                    let Some(record) = decode_record(payload) else {
                        break; // unparsable record => treat the rest as torn
                    };
                    if record.object_gc_claim_version == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "publish journal record has no object GC claim version",
                        ));
                    }
                    records.push(record);
                }
                KIND_TOMBSTONE => {
                    if payload.len() != TOMBSTONE_PAYLOAD_LEN {
                        break;
                    }
                    let mut r = Reader::new(payload);
                    match (r.u64(), r.u64()) {
                        (Some(inode), Some(generation)) => {
                            if r.finish().is_some() {
                                tombstones.insert((inode, generation));
                            } else {
                                break;
                            }
                        }
                        _ => break,
                    }
                }
                _ => break,
            }
        }
        records.retain(|record| !tombstones.contains(&(record.inode, record.generation)));
        Ok(records)
    }

    /// Rewrite the journal containing only `live` records (dropping tombstones
    /// and tombstoned entries). Single-threaded use only (mount-time), via a
    /// temp file + atomic rename + dir fsync.
    pub fn compact(&self, root: &Path, live: &[PendingPublishRecord]) -> io::Result<()> {
        let tmp = root.join(format!("{JOURNAL_FILE_NAME}.tmp"));
        {
            let mut out = File::create(&tmp)?;
            for record in live {
                let payload = encode_record(record);
                let frame = encode_frame(KIND_PUBLISH, &payload)?;
                out.write_all(&frame)?;
            }
            out.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        File::open(root)?.sync_all()?;
        let mut file = self.file.lock().unwrap_or_else(|err| err.into_inner());
        *file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        Ok(())
    }
}

// --------------------------------------------------------------------------- //
// Serialization (local codec — keeps the nokv-meta codec crate-private).
// --------------------------------------------------------------------------- //
fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

fn put_str(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn encode_frame(kind: u8, payload: &[u8]) -> io::Result<Vec<u8>> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "publish journal frame payload exceeds u32",
        )
    })?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    frame.push(kind);
    put_u32(&mut frame, payload_len);
    frame.extend_from_slice(frame_digest(kind, payload_len, payload).as_slice());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn frame_digest(kind: u8, payload_len: u32, payload: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([kind]);
    hasher.update(payload_len.to_be_bytes());
    hasher.update(payload);
    hasher.finalize().into()
}

fn encode_record(record: &PendingPublishRecord) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(RECORD_VERSION);
    put_u64(&mut out, record.inode);
    put_u64(&mut out, record.parent);
    put_bytes(&mut out, &record.name);
    put_u64(&mut out, record.mount);
    put_u64(&mut out, record.generation);
    put_u64(&mut out, record.mtime_ms);
    put_u64(&mut out, record.ctime_ms);
    out.push(u8::from(record.replace));
    put_u64(&mut out, record.dentry_version);
    put_u64(&mut out, record.old_generation);
    put_u64(&mut out, record.object_gc_claim_version);
    put_u64(&mut out, record.size);
    put_u32(&mut out, record.mode);
    put_u32(&mut out, record.uid);
    put_u32(&mut out, record.gid);
    put_str(&mut out, &record.manifest_id);
    put_u32(&mut out, record.cache_files.len() as u32);
    for cache_file in &record.cache_files {
        put_u64(&mut out, cache_file.logical_offset);
        put_str(&mut out, &cache_file.cache_key);
        put_str(&mut out, &cache_file.file_name);
        put_u64(&mut out, cache_file.len);
    }
    out
}

fn decode_record(payload: &[u8]) -> Option<PendingPublishRecord> {
    let mut r = Reader::new(payload);
    if r.u8()? != RECORD_VERSION {
        return None;
    }
    let inode = r.u64()?;
    let parent = r.u64()?;
    let name = r.bytes()?.to_vec();
    let mount = r.u64()?;
    let generation = r.u64()?;
    let mtime_ms = r.u64()?;
    let ctime_ms = r.u64()?;
    let replace = r.u8()? != 0;
    let dentry_version = r.u64()?;
    let old_generation = r.u64()?;
    let object_gc_claim_version = r.u64()?;
    let size = r.u64()?;
    let mode = r.u32()?;
    let uid = r.u32()?;
    let gid = r.u32()?;
    let manifest_id = r.string()?;
    let cache_count = r.u32()? as usize;
    let mut cache_files = Vec::with_capacity(cache_count);
    for _ in 0..cache_count {
        let logical_offset = r.u64()?;
        let cache_key = r.string()?;
        let file_name = r.string()?;
        let len = r.u64()?;
        cache_files.push(CacheFileRef {
            logical_offset,
            cache_key,
            file_name,
            len,
        });
    }
    r.finish()?;
    Some(PendingPublishRecord {
        inode,
        parent,
        name,
        mount,
        generation,
        mtime_ms,
        ctime_ms,
        replace,
        dentry_version,
        old_generation,
        object_gc_claim_version,
        size,
        mode,
        uid,
        gid,
        manifest_id,
        cache_files,
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.offset.checked_add(len)?;
        let out = self.bytes.get(self.offset..end)?;
        self.offset = end;
        Some(out)
    }

    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn string(&mut self) -> Option<String> {
        String::from_utf8(self.bytes()?.to_vec()).ok()
    }

    fn finish(&self) -> Option<()> {
        (self.offset == self.bytes.len()).then_some(())
    }
}

struct FrameReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FrameReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Next `(kind, payload)`, or `None` at end / first torn-or-corrupt frame.
    fn next_frame(&mut self) -> Option<(u8, &'a [u8])> {
        if self.offset + FRAME_HEADER_LEN > self.bytes.len() {
            return None; // truncated header (torn tail)
        }
        let kind = self.bytes[self.offset];
        let len = u32::from_be_bytes(
            self.bytes[self.offset + 1..self.offset + 5]
                .try_into()
                .ok()?,
        ) as usize;
        let digest = &self.bytes[self.offset + 5..self.offset + FRAME_HEADER_LEN];
        let payload_start = self.offset + FRAME_HEADER_LEN;
        let payload_end = payload_start.checked_add(len)?;
        let payload = self.bytes.get(payload_start..payload_end)?; // truncated payload
        if frame_digest(kind, len as u32, payload).as_slice() != digest {
            return None; // corrupt frame => stop
        }
        self.offset = payload_end;
        Some((kind, payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_record(inode: u64, generation: u64) -> PendingPublishRecord {
        PendingPublishRecord {
            inode,
            parent: 1,
            name: b"file.bin".to_vec(),
            mount: 1,
            generation,
            mtime_ms: 1_700_000_000_000,
            ctime_ms: 1_700_000_000_000,
            replace: generation > 1,
            dentry_version: 7,
            old_generation: generation.saturating_sub(1),
            object_gc_claim_version: 17,
            size: 4096,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            manifest_id: format!("fuse/1/{inode}"),
            cache_files: vec![CacheFileRef {
                logical_offset: 0,
                cache_key: format!("fuse/1/{inode}:{generation}:0:4096"),
                file_name: "abc-0000000000000001.writeback".to_owned(),
                len: 4096,
            }],
        }
    }

    #[test]
    fn record_round_trips() {
        let record = sample_record(42, 10);
        let decoded = decode_record(&encode_record(&record)).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn replay_fails_closed_for_legacy_record_without_object_gc_claim() {
        let dir = tempfile::tempdir().unwrap();
        let journal = PublishJournal::open(dir.path()).unwrap();
        let mut payload = encode_record(&sample_record(42, 10));
        payload[0] = 1;
        journal.append_frame(KIND_PUBLISH, &payload).unwrap();

        let error = journal.replay().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .to_string()
            .contains("unsupported publish journal record version 1"));
    }

    #[test]
    fn append_rejects_zero_object_gc_claim() {
        let dir = tempfile::tempdir().unwrap();
        let journal = PublishJournal::open(dir.path()).unwrap();
        let mut record = sample_record(42, 10);
        record.object_gc_claim_version = 0;

        let error = journal.append_publish(&record).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error
            .to_string()
            .contains("publish journal record has no object GC claim version"));
    }

    #[test]
    fn replay_rejects_zero_object_gc_claim() {
        let dir = tempfile::tempdir().unwrap();
        let journal = PublishJournal::open(dir.path()).unwrap();
        let mut record = sample_record(42, 10);
        record.object_gc_claim_version = 0;
        journal
            .append_frame(KIND_PUBLISH, &encode_record(&record))
            .unwrap();

        let error = journal.replay().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .to_string()
            .contains("publish journal record has no object GC claim version"));
    }

    #[test]
    fn replay_returns_untombstoned_records() {
        let dir = tempfile::tempdir().unwrap();
        let journal = PublishJournal::open(dir.path()).unwrap();
        let a = sample_record(1, 10);
        let b = sample_record(2, 20);
        journal.append_publish(&a).unwrap();
        journal.append_publish(&b).unwrap();
        journal.append_tombstone(a.inode, a.generation).unwrap();
        let live = journal.replay().unwrap();
        assert_eq!(live, vec![b]);
    }

    #[test]
    fn replay_drops_torn_tail_keeping_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(JOURNAL_FILE_NAME);
        let a = sample_record(1, 10);
        {
            let journal = PublishJournal::open(dir.path()).unwrap();
            journal.append_publish(&a).unwrap();
        }
        // Simulate a crash mid-append: a truncated second frame.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[KIND_PUBLISH, 0, 0, 0, 200]); // header claims 200B, none follow
        std::fs::write(&path, &bytes).unwrap();
        let journal = PublishJournal::open(dir.path()).unwrap();
        let live = journal.replay().unwrap();
        assert_eq!(live, vec![a]); // prefix survives, torn tail dropped
    }

    #[test]
    fn frame_digest_covers_kind() {
        let mut frame = encode_frame(KIND_PUBLISH, b"payload").unwrap();
        frame[0] = KIND_TOMBSTONE;
        let mut reader = FrameReader::new(&frame);
        assert!(
            reader.next_frame().is_none(),
            "changing the frame kind must invalidate the frame digest"
        );
    }

    #[test]
    fn replay_rejects_tombstone_with_trailing_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let journal = PublishJournal::open(dir.path()).unwrap();
        let record = sample_record(9, 30);
        journal.append_publish(&record).unwrap();

        let mut payload = Vec::new();
        put_u64(&mut payload, record.inode);
        put_u64(&mut payload, record.generation);
        payload.extend_from_slice(b"trailing-corruption");
        journal.append_frame(KIND_TOMBSTONE, &payload).unwrap();

        let live = journal.replay().unwrap();
        assert_eq!(
            live,
            vec![record],
            "a tombstone with extra bytes is corrupt and must not retire a publish record"
        );
    }
}
