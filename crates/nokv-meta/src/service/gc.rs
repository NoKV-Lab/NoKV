use super::*;
use std::time::Duration;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn cleanup_staged_objects(
        &self,
        staged: &StagedObjectSet,
    ) -> Result<ObjectCleanupOutcome, MetadError> {
        self.objects.delete_staged(staged).map_err(Into::into)
    }

    pub fn cleanup_pending_objects(
        &self,
        limit: usize,
    ) -> Result<PendingObjectCleanupOutcome, MetadError> {
        self.cleanup_pending_objects_with_grace(limit, Duration::ZERO)
    }

    pub fn cleanup_pending_objects_with_grace(
        &self,
        limit: usize,
        read_lease_grace: Duration,
    ) -> Result<PendingObjectCleanupOutcome, MetadError> {
        // Reap expired snapshot pins first so the retention floor reflects only
        // live snapshots before deciding what is reclaimable.
        let snapshot_reap = self.reclaim_expired_snapshot_pins(limit)?;
        let now_ms = self.now_ms();
        let grace_ms = duration_millis_u64(read_lease_grace);
        let version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(self.mount),
            start_after: None,
            version,
            limit,
            purpose: ReadPurpose::UserStrong,
        })?;
        if rows.is_empty() {
            return Ok(PendingObjectCleanupOutcome {
                snapshot_reap,
                ..PendingObjectCleanupOutcome::default()
            });
        }
        let retention_floor = self.history_retention_floor()?;

        let mut outcome = PendingObjectCleanupOutcome {
            snapshot_reap,
            scanned: rows.len(),
            blocked_by_snapshots: 0,
            blocked_by_read_leases: 0,
            attempted: 0,
            deleted: 0,
            missing: 0,
            records_removed: 0,
        };
        let mut cleaned_keys = Vec::with_capacity(rows.len());
        for row in rows {
            let record = decode_object_gc_record(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if retention_floor.is_some_and(|floor| floor.get() < record.enqueue_version) {
                outcome.blocked_by_snapshots += 1;
                continue;
            }
            if now_ms < record.enqueue_unix_ms.saturating_add(grace_ms) {
                outcome.blocked_by_read_leases += 1;
                continue;
            }
            let key = ObjectKey::new(record.object_key)?;
            outcome.attempted += 1;
            if self.objects.delete(&key)? {
                outcome.deleted += 1;
            } else {
                outcome.missing += 1;
            }
            cleaned_keys.push(row.key);
        }

        if cleaned_keys.is_empty() {
            return Ok(outcome);
        }

        let commit_version = self.next_version()?;
        let records_removed = cleaned_keys.len();
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"cleanup-objects",
                self.mount,
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Gc,
            primary_key: gc_queue_prefix(self.mount),
            predicates: Vec::new(),
            mutations: cleaned_keys
                .into_iter()
                .map(|key| delete_mutation(RecordFamily::Gc, key))
                .collect(),
            watch: Vec::new(),
        })?;
        outcome.records_removed = records_removed;
        Ok(outcome)
    }

    pub fn cleanup_history(&self, limit: usize) -> Result<HistoryPruneOutcome, MetadError> {
        const RETENTION_RETRIES: usize = 8;
        for attempt in 0..RETENTION_RETRIES {
            let before = self.metadata.history_retention_epoch()?;
            let retain_from = self.history_retention_floor()?;
            let after = self.metadata.history_retention_epoch()?;
            if before != after {
                continue;
            }
            match self.metadata.prune_history(HistoryPruneRequest {
                retain_from,
                retention_epoch: after,
                limit,
            }) {
                Err(MetadataError::PredicateFailed) if attempt + 1 < RETENTION_RETRIES => continue,
                result => return result.map_err(Into::into),
            }
        }
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    }

    /// Whether `object_key` was minted by this `(inode, generation)` and is thus
    /// safe for this namespace to reclaim. Block keys are
    /// `blocks/{mount}/{inode}/{generation}/{chunk}/{block}`, so an owned key
    /// starts with `blocks/{mount}/{inode}/{generation}/`. A clone shares the
    /// source's blocks by copying chunk manifests that still reference the
    /// source's keys; those borrowed keys fail this check, so a divergent write
    /// in the fork never enqueues the source's live blocks for deletion.
    pub(super) fn owns_block_object_key(
        &self,
        inode: InodeId,
        generation: u64,
        object_key: &str,
    ) -> bool {
        let owner_prefix = format!(
            "blocks/{}/{}/{}/",
            self.mount.get(),
            inode.get(),
            generation
        );
        object_key.starts_with(&owner_prefix)
    }

    pub(super) fn history_retention_floor(&self) -> Result<Option<Version>, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })?;
        let now_ms = self.now_ms();
        let mut floor: Option<Version> = None;
        for row in rows {
            let pin = decode_snapshot_pin(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if now_ms >= pin.lease_expires_unix_ms {
                // Expired lease: this pin no longer protects its snapshot, so it
                // must not hold the retention floor down (a crashed holder can
                // never block GC forever).
                continue;
            }
            let version = Version::new(pin.read_version)?;
            floor = Some(floor.map_or(version, |floor| floor.min(version)));
        }
        Ok(floor)
    }

    /// Delete pin records whose lease has expired. Every delete is fenced by the
    /// record version observed by the scan, so an old reaper pass cannot remove
    /// a pin that changed after it was selected.
    /// Expired pins already stop holding the retention floor (see
    /// [`Self::history_retention_floor`]); this removes their records so they do
    /// not accumulate.
    pub fn reclaim_expired_snapshot_pins(
        &self,
        limit: usize,
    ) -> Result<SnapshotReapOutcome, MetadError> {
        let now_ms = self.now_ms();
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit,
            purpose: ReadPurpose::UserStrong,
        })?;
        let scanned = rows.len();
        let mut expired = Vec::new();
        for row in rows {
            let pin = decode_snapshot_pin(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if now_ms >= pin.lease_expires_unix_ms {
                expired.push((row.key, row.version, pin.snapshot_id));
            }
        }
        let mut outcome = SnapshotReapOutcome {
            scanned,
            expired_candidates: expired.len(),
            reaped: 0,
            conflicted: 0,
        };
        if expired.is_empty() {
            return Ok(outcome);
        }
        for (_, _, snapshot_id) in &expired {
            live_test_barrier::snapshot(*snapshot_id, "reaper-scan")?;
        }

        // Common case: one atomic command removes the whole expired page. If a
        // single candidate changed after the scan, the command is all-or-none;
        // fall back to the same version-fenced delete per candidate so one hot
        // pin cannot block unrelated expired pins.
        let commit_version = self.next_version()?;
        let batch = MetadataCommand {
            request_id: request_id(
                b"reclaim-expired-pins",
                self.mount,
                InodeId::root(),
                commit_version,
            ),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Snapshot,
            primary_key: snapshot_pin_prefix(self.mount),
            predicates: expired
                .iter()
                .map(|(key, version, _)| PredicateRef {
                    family: RecordFamily::Snapshot,
                    key: key.clone(),
                    predicate: Predicate::VersionEquals(*version),
                })
                .collect(),
            mutations: expired
                .iter()
                .map(|(key, _, _)| delete_mutation(RecordFamily::Snapshot, key.clone()))
                .collect(),
            watch: Vec::new(),
        };
        match self.commit_metadata(batch) {
            Ok(_) => {
                outcome.reaped = expired.len();
                return Ok(outcome);
            }
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {}
            Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => {
                // Atomic apply completed. Reconcile rather than issuing a
                // second delete under a different request id.
                for (key, _, _) in &expired {
                    if self
                        .metadata
                        .get(
                            RecordFamily::Snapshot,
                            key,
                            self.read_version()?,
                            ReadPurpose::UserStrong,
                        )?
                        .is_none()
                    {
                        outcome.reaped += 1;
                    } else {
                        outcome.conflicted += 1;
                    }
                }
                return Ok(outcome);
            }
            Err(err) => return Err(err),
        }

        for (key, scanned_version, _) in expired {
            let commit_version = self.next_version()?;
            let command = MetadataCommand {
                request_id: request_id(
                    b"reclaim-expired-pin",
                    self.mount,
                    InodeId::root(),
                    commit_version,
                ),
                kind: CommandKind::RetireSnapshot,
                read_version: predecessor(commit_version)?,
                commit_version,
                primary_family: RecordFamily::Snapshot,
                primary_key: key.clone(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::Snapshot,
                    key: key.clone(),
                    predicate: Predicate::VersionEquals(scanned_version),
                }],
                mutations: vec![delete_mutation(RecordFamily::Snapshot, key.clone())],
                watch: Vec::new(),
            };
            match self.commit_metadata(command) {
                Ok(_) => outcome.reaped += 1,
                Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                    outcome.conflicted += 1;
                }
                Err(MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                }) => {
                    if self
                        .metadata
                        .get(
                            RecordFamily::Snapshot,
                            &key,
                            self.read_version()?,
                            ReadPurpose::UserStrong,
                        )?
                        .is_none()
                    {
                        outcome.reaped += 1;
                    } else {
                        outcome.conflicted += 1;
                    }
                }
                Err(err) => return Err(err),
            }
        }
        debug_assert_eq!(
            outcome.reaped + outcome.conflicted,
            outcome.expired_candidates
        );
        Ok(outcome)
    }

    pub(super) fn chunk_manifest_delete_and_gc_mutations(
        &self,
        inode: InodeId,
        generation: u64,
        enqueue_version: Version,
        retained_object_keys: &HashSet<String>,
    ) -> Result<Vec<Mutation>, MetadError> {
        let enqueue_unix_ms = current_time_ms();
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::ChunkManifest,
            prefix: chunk_manifest_prefix(self.mount, inode, generation),
            start_after: None,
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut mutations = Vec::new();
        for row in rows {
            if chunk_index_from_manifest_key(&row.key)? != BODY_SUMMARY_CHUNK_INDEX {
                let manifest = decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                for (block_index, block) in manifest
                    .slices
                    .iter()
                    .flat_map(|slice| slice.blocks.iter())
                    .enumerate()
                {
                    if retained_object_keys.contains(&block.object_key) {
                        continue;
                    }
                    if !self.owns_block_object_key(inode, generation, &block.object_key) {
                        // Borrowed (clone-shared) block: its key is owned by the
                        // inode/generation that minted it, not this one. A borrower
                        // must never enqueue another namespace's blocks for GC.
                        continue;
                    }
                    let record = ObjectGcRecord {
                        inode,
                        generation,
                        object_key: block.object_key.clone(),
                        size: block.len,
                        digest_uri: block.digest_uri.clone(),
                        enqueue_version: enqueue_version.get(),
                        enqueue_unix_ms,
                    };
                    mutations.push(Mutation {
                        family: RecordFamily::Gc,
                        key: gc_object_key(
                            self.mount,
                            enqueue_version.get(),
                            inode,
                            generation,
                            manifest.chunk_index,
                            block_index as u64,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_object_gc_record(&record))),
                    });
                }
            }
            mutations.push(delete_mutation(RecordFamily::ChunkManifest, row.key));
        }
        Ok(mutations)
    }

    /// Reclaim an entire superseded generation chain at a chain collapse. When a
    /// self-contained generation supersedes a delta chain, every generation in
    /// the old chain becomes unreachable, so each one must enqueue the blocks it
    /// OWNS that the new generation no longer references — `retained_object_keys`
    /// (the new generation's complete key set) and `owns_block_object_key`
    /// together keep borrowed and still-referenced blocks alive. The
    /// version-stamped enqueue keeps any block a live snapshot can still reach
    /// protected by the GC retention floor.
    pub(super) fn collapse_chain_gc_mutations(
        &self,
        inode: InodeId,
        old_top_generation: u64,
        old_chunks: &[ChunkManifest],
        enqueue_version: Version,
        retained_object_keys: &HashSet<String>,
    ) -> Result<Vec<Mutation>, MetadError> {
        let read_version = self.read_version()?;
        let chain = match self.body_descriptor_at_version_for_purpose(
            inode,
            old_top_generation,
            read_version,
            ReadPurpose::WritePlanLocal,
        ) {
            Ok(Some(body)) => self.resolve_generation_chain(
                inode,
                &body,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?,
            // The old summary is already gone; best-effort reclaim the top only.
            Ok(None) | Err(MetadError::MissingBodyDescriptor) => vec![old_top_generation],
            Err(err) => return Err(err),
        };
        // Fast path: a self-contained old generation whose manifests the caller
        // already resolved is reclaimed without a metadata scan.
        if chain.len() == 1 && !old_chunks.is_empty() {
            return Ok(self.chunk_manifest_delete_and_gc_mutations_from_manifests(
                inode,
                old_top_generation,
                old_chunks,
                enqueue_version,
                retained_object_keys,
            ));
        }
        let mut mutations = Vec::new();
        for generation in chain {
            mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                inode,
                generation,
                enqueue_version,
                retained_object_keys,
            )?);
        }
        Ok(mutations)
    }

    pub(super) fn chunk_manifest_delete_and_gc_mutations_from_manifests(
        &self,
        inode: InodeId,
        generation: u64,
        manifests: &[ChunkManifest],
        enqueue_version: Version,
        retained_object_keys: &HashSet<String>,
    ) -> Vec<Mutation> {
        let enqueue_unix_ms = current_time_ms();
        let mut mutations = vec![delete_mutation(
            RecordFamily::ChunkManifest,
            chunk_manifest_key(self.mount, inode, generation, BODY_SUMMARY_CHUNK_INDEX),
        )];
        for manifest in manifests {
            for (block_index, block) in manifest
                .slices
                .iter()
                .flat_map(|slice| slice.blocks.iter())
                .enumerate()
            {
                if retained_object_keys.contains(&block.object_key) {
                    continue;
                }
                if !self.owns_block_object_key(inode, generation, &block.object_key) {
                    // Borrowed (clone-shared) block: owned by the inode/generation
                    // that minted it, so this borrower must not enqueue it for GC.
                    continue;
                }
                let record = ObjectGcRecord {
                    inode,
                    generation,
                    object_key: block.object_key.clone(),
                    size: block.len,
                    digest_uri: block.digest_uri.clone(),
                    enqueue_version: enqueue_version.get(),
                    enqueue_unix_ms,
                };
                mutations.push(Mutation {
                    family: RecordFamily::Gc,
                    key: gc_object_key(
                        self.mount,
                        enqueue_version.get(),
                        inode,
                        generation,
                        manifest.chunk_index,
                        block_index as u64,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_record(&record))),
                });
            }
            mutations.push(delete_mutation(
                RecordFamily::ChunkManifest,
                chunk_manifest_key(self.mount, inode, generation, manifest.chunk_index),
            ));
        }
        mutations
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
