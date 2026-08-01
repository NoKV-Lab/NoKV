use std::sync::atomic::{AtomicBool, Ordering};

use nokv_types::{ArtifactRevisionId, LogicalShardId, RootId};

use super::*;

fn logical_shard(byte: u8) -> LogicalShardId {
    LogicalShardId::from_bytes([byte; 16])
}

fn root(byte: u8) -> RootId {
    RootId::from_bytes([byte; 16])
}

fn revision(byte: u8) -> ArtifactRevisionId {
    ArtifactRevisionId::from_bytes([byte; 16])
}

fn options(revision_byte: u8) -> ArtifactUploadOptions {
    ArtifactUploadOptions::new(logical_shard(1), root(2), revision(revision_byte))
        .with_block_size(4)
}

fn execute_upload(
    store: &dyn ArtifactObjectStore,
    upload_options: ArtifactUploadOptions,
    bytes: &[u8],
) -> ArtifactUpload {
    let plan = plan_artifact_upload(upload_options, bytes).unwrap();
    upload_artifact_from_plan(store, &plan, bytes).unwrap()
}

#[test]
fn permanent_keys_are_owned_by_logical_shard_root_and_revision() {
    let keyspace = ArtifactKeyspace {
        logical_shard_id: logical_shard(1),
        root_id: root(2),
        artifact_revision_id: revision(3),
    };
    let key = keyspace.permanent_block_key(7);
    assert_eq!(
        key.as_str(),
        concat!(
            "nokv/artifacts/01010101010101010101010101010101/",
            "02020202020202020202020202020202/",
            "03030303030303030303030303030303/blocks/0000000000000007"
        )
    );
    assert_ne!(
        key,
        ArtifactKeyspace {
            logical_shard_id: logical_shard(9),
            ..keyspace
        }
        .permanent_block_key(7)
    );
    assert_ne!(
        key,
        ArtifactKeyspace {
            root_id: root(9),
            ..keyspace
        }
        .permanent_block_key(7)
    );
    assert_ne!(
        key,
        ArtifactKeyspace {
            artifact_revision_id: revision(9),
            ..keyspace
        }
        .permanent_block_key(7)
    );
    assert!(!key.as_str().contains("http"));
}

#[test]
fn provider_neutral_keys_reject_ambiguous_components() {
    assert_eq!(ObjectKey::new(""), Err(ObjectError::EmptyKey));
    assert_eq!(ObjectKey::new("/absolute"), Err(ObjectError::AbsoluteKey));
    assert_eq!(ObjectKey::new("a//b"), Err(ObjectError::EmptyKeyComponent));
    assert_eq!(ObjectKey::new("a/../b"), Err(ObjectError::ParentTraversal));
    assert_eq!(ObjectKey::new("a/./b"), Err(ObjectError::CurrentDirectory));
    assert_eq!(ObjectKey::new(r"a\b"), Err(ObjectError::BackslashInKey));
}

#[test]
fn immutable_create_accepts_only_exact_replay() {
    let store = MemoryArtifactStore::new();
    let key = ObjectKey::new("nokv/artifacts/a").unwrap();
    assert_eq!(
        store.create_immutable(&key, b"alpha").unwrap(),
        ImmutableCreateOutcome::Created
    );
    assert_eq!(
        store.create_immutable(&key, b"alpha").unwrap(),
        ImmutableCreateOutcome::Replayed
    );
    assert!(matches!(
        store.create_immutable(&key, b"beta"),
        Err(ObjectError::ImmutableCollision { .. })
    ));
    assert_eq!(store.read(&key, None).unwrap(), b"alpha");
    let stats = store.stats().unwrap();
    assert_eq!(stats.creates, 1);
    assert_eq!(stats.replays, 1);
    assert_eq!(stats.collisions, 1);
}

#[test]
fn upload_range_read_and_exact_replay_share_one_manifest() {
    let store = MemoryArtifactStore::new();
    let uploaded = execute_upload(&store, options(3), b"abcdefghij");
    assert_eq!(uploaded.manifest.blocks.len(), 3);
    assert_eq!(uploaded.stats.created, 3);
    assert_eq!(uploaded.staged.len(), 3);

    let replay = execute_upload(&store, options(3), b"abcdefghij");
    assert_eq!(replay.manifest, uploaded.manifest);
    assert_eq!(replay.stats.replayed, 3);

    let range =
        read_artifact_range(&store, None, &uploaded.manifest, 2, 7).expect("valid strict range");
    assert_eq!(range.bytes, b"cdefghi");
    assert_eq!(range.stats.planned_blocks, 3);
    assert_eq!(range.stats.verified_blocks, 3);

    let whole = read_artifact(&store, None, &uploaded.manifest).unwrap();
    assert_eq!(whole.bytes, b"abcdefghij");
}

#[test]
fn upload_plan_is_pure_deterministic_and_precedes_object_writes() {
    let store = MemoryArtifactStore::new();
    let first = plan_artifact_upload(options(9), b"abcdefgh").unwrap();
    let second = plan_artifact_upload(options(9), b"abcdefgh").unwrap();
    assert_eq!(first, second);
    assert_eq!(first.manifest.blocks.len(), 2);
    assert_eq!(store.stats().unwrap().resident_objects, 0);

    let uploaded = upload_artifact_from_plan(&store, &first, b"abcdefgh").unwrap();
    assert_eq!(uploaded.manifest, first.manifest);
    assert_eq!(store.stats().unwrap().resident_objects, 2);
}

#[test]
fn upload_from_plan_rejects_payload_mismatch_before_first_write() {
    let store = MemoryArtifactStore::new();
    let plan = plan_artifact_upload(options(10), b"abcdefgh").unwrap();
    let failure = upload_artifact_from_plan(&store, &plan, b"abcdefgx").unwrap_err();
    assert!(matches!(
        *failure.source,
        ObjectError::DigestMismatch { .. }
    ));
    assert!(failure.staged.is_empty());
    assert_eq!(failure.stats, ArtifactUploadStats::default());
    assert_eq!(store.stats().unwrap().resident_objects, 0);
}

#[test]
fn upload_failure_returns_revision_owned_cleanup_set() {
    let store = MemoryArtifactStore::new();
    let keyspace = ArtifactKeyspace {
        logical_shard_id: logical_shard(1),
        root_id: root(2),
        artifact_revision_id: revision(4),
    };
    let conflicting = keyspace.permanent_block_key(1);
    store.create_immutable(&conflicting, b"xxxx").unwrap();

    let plan = plan_artifact_upload(options(4), b"abcdefgh").unwrap();
    let failure = upload_artifact_from_plan(&store, &plan, b"abcdefgh").unwrap_err();
    assert!(matches!(
        *failure.source,
        ObjectError::ImmutableCollision { .. }
    ));
    assert_eq!(failure.staged.len(), 1);
    assert_eq!(failure.stats.created, 1);

    let cleanup = cleanup_staged_artifact(&store, &failure.staged).unwrap();
    assert_eq!(cleanup.deleted, 1);
    assert_eq!(store.read(&conflicting, None).unwrap(), b"xxxx");
}

#[test]
fn staged_cleanup_state_rejects_cross_revision_keys() {
    let keyspace = ArtifactKeyspace {
        logical_shard_id: logical_shard(1),
        root_id: root(2),
        artifact_revision_id: revision(4),
    };
    let wrong_key = ArtifactKeyspace {
        artifact_revision_id: revision(9),
        ..keyspace
    }
    .permanent_block_key(0);
    assert!(matches!(
        StagedArtifactObjects::from_keys(logical_shard(1), root(2), revision(4), vec![wrong_key]),
        Err(ObjectError::InvalidManifest(_))
    ));
}

#[test]
fn strict_ranges_and_block_digests_are_verified() {
    let store = MemoryArtifactStore::new();
    let uploaded = execute_upload(&store, options(5), b"abcdefgh");
    assert!(matches!(
        plan_artifact_read(&uploaded.manifest, 7, 2),
        Err(ObjectError::InvalidRange { .. })
    ));
    assert!(matches!(
        plan_artifact_read(&uploaded.manifest, 0, 0),
        Err(ObjectError::InvalidRange { .. })
    ));

    let mut corrupt_manifest = uploaded.manifest.clone();
    corrupt_manifest.blocks[0].sha256 = [0xff; 32];
    assert!(matches!(
        read_artifact(&store, None, &corrupt_manifest),
        Err(ObjectError::DigestMismatch { .. })
    ));

    let mut corrupt_artifact_digest = uploaded.manifest.clone();
    corrupt_artifact_digest.sha256 = [0xee; 32];
    assert!(matches!(
        read_artifact(&store, None, &corrupt_artifact_digest),
        Err(ObjectError::DigestMismatch { .. })
    ));
}

#[test]
fn bounded_read_windows_reject_gaps_and_overlaps_without_synthesizing_bytes() {
    let store = MemoryArtifactStore::new();
    let uploaded = execute_upload(&store, options(11), b"abcdefghijkl");
    let mut window = ArtifactReadWindow {
        logical_shard_id: uploaded.manifest.logical_shard_id,
        root_id: uploaded.manifest.root_id,
        artifact_revision_id: uploaded.manifest.artifact_revision_id,
        artifact_logical_len: uploaded.manifest.logical_len,
        blocks: vec![
            uploaded.manifest.blocks[0].clone(),
            uploaded.manifest.blocks[2].clone(),
        ],
    };
    assert!(matches!(
        read_artifact_window(&store, None, &window, 0, 12),
        Err(ObjectError::InvalidManifest(_))
    ));

    window.blocks[1] = uploaded.manifest.blocks[1].clone();
    window.blocks[1].logical_offset = 3;
    assert!(matches!(
        read_artifact_window(&store, None, &window, 0, 8),
        Err(ObjectError::InvalidManifest(_))
    ));
}

#[test]
fn empty_artifact_has_a_canonical_manifest() {
    let store = MemoryArtifactStore::new();
    let uploaded = execute_upload(&store, options(6), b"");
    assert!(uploaded.manifest.blocks.is_empty());
    assert!(uploaded.staged.is_empty());
    assert_eq!(
        read_artifact(&store, None, &uploaded.manifest)
            .unwrap()
            .bytes,
        Vec::<u8>::new()
    );
}

#[test]
fn memory_cache_evicts_and_recovers_from_corruption() {
    let store = MemoryArtifactStore::new();
    let uploaded = execute_upload(&store, options(7), b"abcdefgh");
    let cache = MemoryArtifactCache::new(MemoryArtifactCacheOptions {
        max_bytes: 4,
        max_entries: 1,
    });

    let first = read_artifact_range(&store, Some(&cache), &uploaded.manifest, 0, 4).unwrap();
    assert_eq!(first.stats.store_reads, 1);
    let second = read_artifact_range(&store, Some(&cache), &uploaded.manifest, 0, 4).unwrap();
    assert_eq!(second.stats.cache_hits, 1);
    assert_eq!(second.stats.store_reads, 0);

    let key = uploaded.manifest.blocks[0].key.clone();
    cache.insert(key, b"zzzz".to_vec()).unwrap();
    let recovered = read_artifact_range(&store, Some(&cache), &uploaded.manifest, 0, 4).unwrap();
    assert_eq!(recovered.bytes, b"abcd");
    assert_eq!(recovered.stats.cache_misses, 1);
    assert_eq!(recovered.stats.store_reads, 1);
}

#[test]
fn local_hot_tier_enforces_capacity_without_overwriting() {
    let temp = tempfile::tempdir().unwrap();
    let hot = LocalHotTier::new(LocalHotTierOptions::new(temp.path(), 4)).unwrap();
    let first = ObjectKey::new("nokv/artifacts/first").unwrap();
    let second = ObjectKey::new("nokv/artifacts/second").unwrap();

    assert_eq!(
        hot.create_immutable(&first, b"aaaa").unwrap(),
        ImmutableCreateOutcome::Created
    );
    assert_eq!(
        hot.create_immutable(&second, b"bbbb").unwrap(),
        ImmutableCreateOutcome::Created
    );
    assert!(hot.head(&first).unwrap().is_none());
    assert!(hot.head(&second).unwrap().is_some());
    assert!(matches!(
        hot.create_immutable(&second, b"cccc"),
        Err(ObjectError::ImmutableCollision { .. })
    ));
    let stats = hot.stats().unwrap();
    assert_eq!(stats.evictions, 1);
    assert_eq!(stats.resident_bytes, 4);

    let reopened = LocalHotTier::new(LocalHotTierOptions::new(temp.path(), 3)).unwrap();
    assert!(reopened.head(&second).unwrap().is_none());
    assert_eq!(reopened.stats().unwrap().resident_bytes, 0);
}

#[test]
fn tiered_store_reads_durable_then_uses_hot_tier() {
    let hot = MemoryArtifactStore::new();
    let durable = MemoryArtifactStore::new();
    let key = ObjectKey::new("nokv/artifacts/tiered").unwrap();
    durable.create_immutable(&key, b"payload").unwrap();
    let tiered = TieredArtifactStore::new(
        hot.clone(),
        durable.clone(),
        TieredArtifactStoreOptions::default(),
    );

    assert_eq!(tiered.read(&key, None).unwrap(), b"payload");
    assert_eq!(hot.read(&key, None).unwrap(), b"payload");
    assert_eq!(tiered.read(&key, None).unwrap(), b"payload");
    let stats = tiered.stats().unwrap();
    assert_eq!(stats.hot_misses, 1);
    assert_eq!(stats.hot_hits, 1);
    assert_eq!(stats.durable_reads, 1);
    assert_eq!(stats.hot_populates, 1);
}

#[derive(Debug)]
struct AmbiguousDeleteStore {
    inner: MemoryArtifactStore,
    fail_next_delete: AtomicBool,
}

impl AmbiguousDeleteStore {
    fn new() -> Self {
        Self {
            inner: MemoryArtifactStore::new(),
            fail_next_delete: AtomicBool::new(false),
        }
    }
}

impl ArtifactObjectStore for AmbiguousDeleteStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        self.inner.capabilities()
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        self.inner.create_immutable(key, bytes)
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.read(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        if self.fail_next_delete.swap(false, Ordering::SeqCst) {
            return Err(ObjectError::DeleteAmbiguous {
                key: key.clone(),
                detail: "injected response loss".to_owned(),
            });
        }
        self.inner.delete(key)
    }
}

#[test]
fn cleanup_surfaces_ambiguous_delete_with_progress() {
    let store = AmbiguousDeleteStore::new();
    let uploaded = execute_upload(&store, options(8), b"abcdefgh");
    store.fail_next_delete.store(true, Ordering::SeqCst);
    let failure = cleanup_staged_artifact(&store, &uploaded.staged).unwrap_err();
    assert_eq!(failure.outcome.attempted, 1);
    assert_eq!(failure.outcome.deleted, 0);
    assert!(matches!(
        *failure.source,
        ObjectError::DeleteAmbiguous { .. }
    ));
    assert_eq!(store.inner.stats().unwrap().resident_objects, 2);
}

#[test]
fn object_ranges_are_strict_not_truncated() {
    let store = MemoryArtifactStore::new();
    let key = ObjectKey::new("nokv/artifacts/range").unwrap();
    store.create_immutable(&key, b"abcd").unwrap();
    assert_eq!(
        store
            .read(&key, Some(ObjectRange::new(1, 2).unwrap()))
            .unwrap(),
        b"bc"
    );
    assert!(matches!(
        store.read(&key, Some(ObjectRange::new(3, 2).unwrap())),
        Err(ObjectError::InvalidRange { .. })
    ));
}

#[test]
fn s3_options_require_durable_coordinates() {
    let mut options = S3ArtifactStoreOptions::new("");
    assert_eq!(options.validate(), Err(ObjectError::MissingBucket));
    options.bucket = "artifacts".to_owned();
    options.region.clear();
    assert_eq!(options.validate(), Err(ObjectError::MissingRegion));
}
