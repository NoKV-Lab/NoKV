use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn new(mount: MountId, metadata: M, objects: O) -> Self {
        Self {
            mount,
            shard_index: 0,
            metadata,
            objects,
            allocator_gate: Mutex::new(()),
            backup_gate: Mutex::new(()),
            epoch_fence: RwLock::new(()),
            path_resolution_cache: new_path_resolution_cache_shards(),
            path_index_lookup_cache: new_path_index_lookup_cache_shards(),
            path_index_validation_cache: new_path_index_validation_cache_shards(),
            path_cache_epoch: AtomicU64::new(0),
            advisory_locks: Mutex::new(AdvisoryLockTable::default()),
            clock: AtomicU64::new(1),
            reserved_version: AtomicU64::new(1),
            next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            reserved_next_inode: AtomicU64::new(InodeId::ROOT_RAW + 1),
            epoch: AtomicU64::new(1),
            required_owner_epoch: AtomicU64::new(1),
            lease_deadline_ms: AtomicU64::new(0),
            clock_override_ms: AtomicU64::new(0),
            metadata_log_sync: Mutex::new(None),
            metadata_log_segments_archived_total: AtomicU64::new(0),
            metadata_log_entries_archived_total: AtomicU64::new(0),
            metadata_log_archive_bytes_total: AtomicU64::new(0),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            watch_logging_enabled: AtomicBool::new(false),
            object_puts: AtomicU64::new(0),
            object_put_bytes: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            object_get_bytes: AtomicU64::new(0),
            coalesced_gets: AtomicU64::new(0),
            coalesced_get_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_hit_bytes: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
            path_index_lookup_total: AtomicU64::new(0),
            path_index_hit_total: AtomicU64::new(0),
            path_index_miss_total: AtomicU64::new(0),
            path_index_stale_total: AtomicU64::new(0),
            path_index_scan_stale_total: AtomicU64::new(0),
            path_index_fallback_total: AtomicU64::new(0),
            create_files_batch_total: AtomicU64::new(0),
            create_files_entry_total: AtomicU64::new(0),
            create_dirs_batch_total: AtomicU64::new(0),
            create_dirs_entry_total: AtomicU64::new(0),
            read_dir_plus_total: AtomicU64::new(0),
            read_dir_plus_entry_total: AtomicU64::new(0),
            read_dir_plus_projection_hit_total: AtomicU64::new(0),
        }
    }

    /// Set the shard index this service owns and seed its inode allocator into
    /// the shard's high-bit subspace. Builder applied at construction, before the
    /// service is shared. Idempotent and safe for recovered stores: it only
    /// raises the allocator floor (`fetch_max`), so a recovered high-water (which
    /// already carries the shard's high bits) is never regressed, and shard 0 is
    /// a no-op (`compose(0, 2) == 2`).
    pub fn with_shard_index(self, shard_index: u16) -> Self {
        let base = InodeId::compose(shard_index, InodeId::ROOT_RAW + 1)
            .expect("composed allocator base inode is valid")
            .get();
        self.next_inode.fetch_max(base, Ordering::Relaxed);
        self.reserved_next_inode.fetch_max(base, Ordering::Relaxed);
        Self {
            shard_index,
            ..self
        }
    }

    pub fn shard_index(&self) -> u16 {
        self.shard_index
    }

    /// Reopen a persisted shard. Recovery is shard-scoped: only inodes minted by
    /// `shard_index` raise the local allocator high-water (foreign ids embedded
    /// in this shard's records — e.g. a cross-shard graft target — are excluded),
    /// and the allocator floor is then seeded into this shard's high-bit subspace
    /// exactly like [`Self::with_shard_index`]. So callers must NOT chain
    /// `.with_shard_index(...)` after `open_existing`; pass the index here.
    pub fn open_existing(
        mount: MountId,
        metadata: M,
        objects: O,
        shard_index: u16,
    ) -> Result<Self, MetadError> {
        let allocator = recover_allocator_state(&metadata, mount, shard_index)?;
        // Raise the allocator floor into this shard's subspace. `fetch_max`
        // semantics: a recovered high-water (which already carries the shard's
        // high bits) is never regressed, and shard 0 is a no-op
        // (`compose(0, ROOT_RAW + 1) == ROOT_RAW + 1`).
        let allocator_floor = InodeId::compose(shard_index, InodeId::ROOT_RAW + 1)
            .expect("composed allocator base inode is valid")
            .get();
        let next_inode = allocator.next_inode.max(allocator_floor);
        Ok(Self {
            mount,
            shard_index,
            metadata,
            objects,
            allocator_gate: Mutex::new(()),
            backup_gate: Mutex::new(()),
            epoch_fence: RwLock::new(()),
            path_resolution_cache: new_path_resolution_cache_shards(),
            path_index_lookup_cache: new_path_index_lookup_cache_shards(),
            path_index_validation_cache: new_path_index_validation_cache_shards(),
            path_cache_epoch: AtomicU64::new(0),
            advisory_locks: Mutex::new(AdvisoryLockTable::default()),
            clock: AtomicU64::new(allocator.last_commit_version),
            reserved_version: AtomicU64::new(allocator.last_commit_version),
            next_inode: AtomicU64::new(next_inode),
            reserved_next_inode: AtomicU64::new(next_inode),
            epoch: AtomicU64::new(allocator.epoch),
            required_owner_epoch: AtomicU64::new(allocator.epoch),
            lease_deadline_ms: AtomicU64::new(0),
            clock_override_ms: AtomicU64::new(0),
            metadata_log_sync: Mutex::new(None),
            metadata_log_segments_archived_total: AtomicU64::new(0),
            metadata_log_entries_archived_total: AtomicU64::new(0),
            metadata_log_archive_bytes_total: AtomicU64::new(0),
            block_cache: MemoryBlockCache::default(),
            block_cache_enabled: AtomicBool::new(true),
            watch_logging_enabled: AtomicBool::new(false),
            object_puts: AtomicU64::new(0),
            object_put_bytes: AtomicU64::new(0),
            object_gets: AtomicU64::new(0),
            object_get_bytes: AtomicU64::new(0),
            coalesced_gets: AtomicU64::new(0),
            coalesced_get_bytes: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_hit_bytes: AtomicU64::new(0),
            manifest_chunks: AtomicU64::new(0),
            manifest_blocks: AtomicU64::new(0),
            path_index_lookup_total: AtomicU64::new(0),
            path_index_hit_total: AtomicU64::new(0),
            path_index_miss_total: AtomicU64::new(0),
            path_index_stale_total: AtomicU64::new(0),
            path_index_scan_stale_total: AtomicU64::new(0),
            path_index_fallback_total: AtomicU64::new(0),
            create_files_batch_total: AtomicU64::new(0),
            create_files_entry_total: AtomicU64::new(0),
            create_dirs_batch_total: AtomicU64::new(0),
            create_dirs_entry_total: AtomicU64::new(0),
            read_dir_plus_total: AtomicU64::new(0),
            read_dir_plus_entry_total: AtomicU64::new(0),
            read_dir_plus_projection_hit_total: AtomicU64::new(0),
        })
    }

    pub fn object_stats(&self) -> ObjectTransferStats {
        ObjectTransferStats {
            object_puts: self.object_puts.load(Ordering::Relaxed),
            object_put_bytes: self.object_put_bytes.load(Ordering::Relaxed),
            object_gets: self.object_gets.load(Ordering::Relaxed),
            object_get_bytes: self.object_get_bytes.load(Ordering::Relaxed),
            coalesced_gets: self.coalesced_gets.load(Ordering::Relaxed),
            coalesced_get_bytes: self.coalesced_get_bytes.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_hit_bytes: self.cache_hit_bytes.load(Ordering::Relaxed),
            prefetch_enqueued: 0,
            prefetch_dropped: 0,
            prefetch_completed: 0,
            prefetch_failed: 0,
            prefetch_object_gets: 0,
            prefetch_object_get_bytes: 0,
            prefetch_cache_hits: 0,
            prefetch_cache_hit_bytes: 0,
            read_plan_cache_hits: 0,
            read_plan_cache_misses: 0,
            object_writeback_enqueued: 0,
            object_writeback_inline: 0,
            object_writeback_completed: 0,
            object_writeback_failed: 0,
            object_writeback_staged_bytes: 0,
            object_writeback_uploaded_bytes: 0,
            object_writeback_queue_wait_ns: 0,
            object_writeback_queue_max_wait_ns: 0,
            object_writeback_upload_ns: 0,
            object_writeback_upload_max_ns: 0,
            object_writeback_collect_ns: 0,
            object_writeback_digest_ns: 0,
            object_writeback_store_put_ns: 0,
            object_writeback_cache_put_ns: 0,
            manifest_chunks: self.manifest_chunks.load(Ordering::Relaxed),
            manifest_blocks: self.manifest_blocks.load(Ordering::Relaxed),
        }
    }

    pub fn metadata_service_stats(&self) -> MetadataServiceStats {
        MetadataServiceStats {
            path_index_lookup_total: self.path_index_lookup_total.load(Ordering::Relaxed),
            path_index_hit_total: self.path_index_hit_total.load(Ordering::Relaxed),
            path_index_miss_total: self.path_index_miss_total.load(Ordering::Relaxed),
            path_index_stale_total: self.path_index_stale_total.load(Ordering::Relaxed),
            path_index_scan_stale_total: self.path_index_scan_stale_total.load(Ordering::Relaxed),
            path_index_fallback_total: self.path_index_fallback_total.load(Ordering::Relaxed),
            create_files_batch_total: self.create_files_batch_total.load(Ordering::Relaxed),
            create_files_entry_total: self.create_files_entry_total.load(Ordering::Relaxed),
            create_dirs_batch_total: self.create_dirs_batch_total.load(Ordering::Relaxed),
            create_dirs_entry_total: self.create_dirs_entry_total.load(Ordering::Relaxed),
            read_dir_plus_total: self.read_dir_plus_total.load(Ordering::Relaxed),
            read_dir_plus_entry_total: self.read_dir_plus_entry_total.load(Ordering::Relaxed),
            read_dir_plus_projection_hit_total: self
                .read_dir_plus_projection_hit_total
                .load(Ordering::Relaxed),
            metadata_log_segments_archived_total: self
                .metadata_log_segments_archived_total
                .load(Ordering::Relaxed),
            metadata_log_entries_archived_total: self
                .metadata_log_entries_archived_total
                .load(Ordering::Relaxed),
            metadata_log_archive_bytes_total: self
                .metadata_log_archive_bytes_total
                .load(Ordering::Relaxed),
        }
    }

    pub fn mount_id(&self) -> MountId {
        self.mount
    }

    pub fn metadata_store(&self) -> &M {
        &self.metadata
    }

    pub fn set_block_cache_enabled(&self, enabled: bool) {
        self.block_cache_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn block_cache_enabled(&self) -> bool {
        self.block_cache_enabled.load(Ordering::Relaxed)
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore + MetadataStoreStatsProvider,
    O: ObjectStore,
{
    pub fn metadata_store_stats(&self) -> MetadataStoreStats {
        self.metadata.metadata_store_stats()
    }
}
