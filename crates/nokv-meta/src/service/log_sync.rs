//! Synchronous logical metadata log archiving for commit ACK durability.

use super::log_archive::archive_metadata_log_segment_to_store;
use super::*;
use crate::{MetadataLogEntry, MetadataLogSegment};

/// A pointer to one archived logical-log segment in the live chain.
///
/// The sync state keeps the ordered chain of every segment archived above the
/// latest checkpoint so the control-plane `LogRef` can enumerate all of them.
/// A single latest pointer would lose every segment but the newest on failover.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSegmentPointer {
    pub segment_key: String,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub last_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSyncConfig {
    pub archive: MetadataLogArchiveConfig,
    pub shard_id: String,
    pub epoch: u64,
    pub durable_lsn: u64,
    pub last_digest: [u8; 32],
    /// Segment chain inherited from the control record (e.g. after failover),
    /// so the new owner's future `LogRef` publishes keep the full chain.
    pub segments: Vec<MetadataLogSegmentPointer>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSyncSnapshot {
    pub shard_id: String,
    pub epoch: u64,
    pub durable_lsn: u64,
    pub last_digest: [u8; 32],
    /// Ordered (oldest first) segment chain above the latest checkpoint.
    pub segments: Vec<MetadataLogSegmentPointer>,
}

/// Atomic local state used to decide whether a control-published recovery tail
/// still exactly covers every metadata mutation visible on this server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogPublicationState {
    pub snapshot: MetadataLogSyncSnapshot,
    pub has_pending_segment: bool,
    pub has_unresolved_commit_group: bool,
}

/// Result of retiring shared-log segment objects covered by an authoritative
/// checkpoint. Object deletion is best-effort because the checkpoint remains
/// published even when cleanup fails.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataLogPruneOutcome {
    /// Exact pointers removed from the live chain because their tail is at or
    /// before the checkpoint LSN.
    pub pointers_pruned: usize,
    /// Covered segment objects physically deleted from the object store.
    pub objects_deleted: usize,
    /// Covered segment objects that were already absent.
    pub objects_missing: usize,
    /// Covered segment objects whose key validation or deletion failed and
    /// therefore remain as safe, unreferenced leaks.
    pub delete_failures: usize,
}

pub(super) struct MetadataLogSyncState {
    config: MetadataLogArchiveConfig,
    shard_id: String,
    epoch: u64,
    next_lsn: u64,
    prev_digest: [u8; 32],
    segments: Vec<MetadataLogSegmentPointer>,
    /// A locally committed segment whose object-store archive has not yet
    /// completed. No later metadata command may apply until this exact segment
    /// is durably retried, otherwise its LSN could be reused and recovery would
    /// permanently omit the earlier commit.
    pending_segment: Option<MetadataLogSegment>,
    /// Exact input and engine results for a commit group containing at least
    /// one `Backend` outcome whose durable apply status could not be read back.
    /// No later command may apply, and no checkpoint may be exported, until the
    /// whole group is resolved and its actually committed subset is archived in
    /// original input order.
    unresolved_commit_group: Option<UnresolvedMetadataCommitGroup>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct UnresolvedMetadataCommitGroup {
    commands: Vec<MetadataCommand>,
    results: Vec<Result<CommitResult, MetadataError>>,
}

impl UnresolvedMetadataCommitGroup {
    pub(super) fn new(
        commands: Vec<MetadataCommand>,
        results: Vec<Result<CommitResult, MetadataError>>,
    ) -> Result<Self, MetadError> {
        if commands.len() != results.len() {
            return Err(MetadError::Codec(
                "metadata commit resolution group length mismatch".to_owned(),
            ));
        }
        if !results
            .iter()
            .any(|result| matches!(result, Err(MetadataError::Backend(_))))
        {
            return Err(MetadError::Codec(
                "metadata commit resolution group has no backend outcome".to_owned(),
            ));
        }
        Ok(Self { commands, results })
    }
}

impl MetadataLogSyncState {
    fn snapshot(&self) -> MetadataLogSyncSnapshot {
        MetadataLogSyncSnapshot {
            shard_id: self.shard_id.clone(),
            epoch: self.epoch,
            // `segments` contains only the live chain above the latest
            // checkpoint and can therefore be empty immediately after pruning.
            // The allocator tail remains the authoritative durable boundary.
            durable_lsn: self.next_lsn - 1,
            last_digest: self.prev_digest,
            segments: self.segments.clone(),
        }
    }
}

impl MetadataLogSyncConfig {
    pub fn new(
        archive_prefix: impl Into<String>,
        shard_id: impl Into<String>,
        epoch: u64,
        durable_lsn: u64,
        last_digest: [u8; 32],
    ) -> Self {
        Self {
            archive: MetadataLogArchiveConfig::new(archive_prefix),
            shard_id: shard_id.into(),
            epoch,
            durable_lsn,
            last_digest,
            segments: Vec::new(),
        }
    }

    /// Inherit a previously-archived segment chain (above the latest checkpoint)
    /// so future `LogRef` publishes keep the full chain after a failover restore.
    pub fn with_segments(mut self, segments: Vec<MetadataLogSegmentPointer>) -> Self {
        self.segments = segments;
        self
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Verify that an installed/restored metadata image already carries the
    /// failover fence. This never creates or repairs the marker: callers use it
    /// to reject checkpoints produced before failover-safe object GC existed.
    pub fn verify_failover_durability_required(&self) -> Result<(), MetadError> {
        if self.failover_durability_is_required()? {
            return Ok(());
        }
        Err(MetadError::Codec(
            "failover durability requirement marker is missing; the checkpoint predates failover-safe object GC"
                .to_owned(),
        ))
    }

    /// Report whether the persisted metadata image carries the failover
    /// durability fence. A present marker is decoded strictly so corrupt or
    /// unknown marker values are never reported as a safe deployment.
    pub fn failover_durability_is_required(&self) -> Result<bool, MetadError> {
        let key = failover_durability_required_key(self.mount);
        let Some(value) = self.metadata.get(
            RecordFamily::System,
            &key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(false);
        };
        decode_failover_durability_required_marker(&value.0)?;
        Ok(true)
    }

    pub fn enable_sync_metadata_log(
        &self,
        config: MetadataLogSyncConfig,
    ) -> Result<(), MetadError> {
        if config.shard_id.is_empty() {
            return Err(MetadError::Codec(
                "metadata log shard id is empty".to_owned(),
            ));
        }
        if config.epoch == 0 {
            return Err(MetadError::InvalidOwnerEpoch);
        }
        let next_lsn = config
            .durable_lsn
            .checked_add(1)
            .ok_or_else(|| MetadError::Codec("metadata log LSN is exhausted".to_owned()))?;
        self.require_failover_durability()?;
        let _log_enable_fence = self
            .metadata_log_enable_fence
            .write()
            .unwrap_or_else(|err| err.into_inner());
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        if guard.is_some() {
            return Err(MetadError::Codec(
                "synchronous metadata log is already enabled".to_owned(),
            ));
        }
        let state = MetadataLogSyncState {
            config: config.archive,
            shard_id: config.shard_id,
            epoch: config.epoch,
            next_lsn,
            prev_digest: config.last_digest,
            segments: config.segments,
            pending_segment: None,
            unresolved_commit_group: None,
        };
        *guard = Some(state);
        Ok(())
    }

    /// Persist the policy fence required whenever this metadata state can be
    /// restored on another server. Object deletion stays fail-closed until a
    /// future control-published durability watermark can prove completion.
    ///
    /// This is intentionally independent of synchronous logical-log archiving:
    /// checkpoint-only and control-owned deployments also cross server failure
    /// boundaries and must install the same fence before recovery or serving.
    pub fn require_failover_durability(&self) -> Result<(), MetadError> {
        const MAX_ATTEMPTS: usize = 8;
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let key = failover_durability_required_key(self.mount);
        for attempt in 0..MAX_ATTEMPTS {
            if let Some(value) = self.metadata.get(
                RecordFamily::System,
                &key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )? {
                decode_failover_durability_required_marker(&value.0)?;
                return Ok(());
            }

            self.ensure_object_gc_claim_record()?;
            let claim_key = object_gc_claim_key(self.mount);
            let claim = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &claim_key,
                    self.read_version()?,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or_else(|| {
                    MetadError::Codec("durable object GC claim is not initialized".to_owned())
                })?;
            let claim_state = decode_object_gc_claim(self.mount, &claim.value.0)?;
            if let ObjectGcClaim::Deleting { owner_epoch, .. } = claim_state {
                let current_epoch = self.epoch.load(Ordering::Relaxed);
                if owner_epoch > current_epoch {
                    return Err(MetadError::StaleOwnerEpoch {
                        owner_epoch: current_epoch,
                        required_epoch: owner_epoch,
                    });
                }
            }
            let version = self.next_version()?;
            let command = MetadataCommand {
                request_id: request_id(
                    b"require-failover-durability",
                    self.mount,
                    InodeId::root(),
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: key.clone(),
                predicates: vec![
                    PredicateRef {
                        family: RecordFamily::System,
                        key: claim_key,
                        predicate: Predicate::VersionEquals(claim.version),
                    },
                    PredicateRef {
                        family: RecordFamily::System,
                        key: key.clone(),
                        predicate: Predicate::NotExists,
                    },
                ],
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(FAILOVER_DURABILITY_REQUIRED_MARKER.to_vec())),
                }],
                watch: Vec::new(),
            };
            match self.commit_metadata(command) {
                Ok(_)
                | Err(MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                }) => {
                    let value = self
                        .metadata
                        .get(
                            RecordFamily::System,
                            &key,
                            self.read_version()?,
                            ReadPurpose::WritePlanLocal,
                        )?
                        .ok_or_else(|| {
                            MetadError::Codec(
                                "failover durability requirement marker was not durable".to_owned(),
                            )
                        })?;
                    decode_failover_durability_required_marker(&value.0)?;
                    return Ok(());
                }
                Err(MetadError::Metadata(MetadataError::PredicateFailed))
                    if attempt + 1 < MAX_ATTEMPTS =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
        Err(MetadError::Metadata(MetadataError::PredicateFailed))
    }

    /// Retire archived segments fully covered by an authoritative checkpoint at
    /// `checkpoint_lsn` (their effects now live in the checkpoint image), while
    /// preserving every tail segment above the checkpoint boundary.
    ///
    /// Callers must invoke this only after the checkpoint publication CAS wins.
    /// Deletion uses the exact retired pointers rather than listing an object
    /// prefix. Failures are reported as safe leaks and never roll back the
    /// already-published checkpoint.
    pub fn prune_sync_metadata_log_segments(&self, checkpoint_lsn: u64) -> MetadataLogPruneOutcome {
        let (covered, archive) = {
            let mut guard = self
                .metadata_log_sync
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            let Some(state) = guard.as_mut() else {
                return MetadataLogPruneOutcome::default();
            };
            let (covered, tail): (Vec<_>, Vec<_>) = state
                .segments
                .drain(..)
                .partition(|segment| segment.last_lsn <= checkpoint_lsn);
            state.segments = tail;
            (covered, state.config.clone())
        };

        let mut outcome = MetadataLogPruneOutcome {
            pointers_pruned: covered.len(),
            ..MetadataLogPruneOutcome::default()
        };
        for segment in covered {
            if archive.validate_segment_key(&segment.segment_key).is_err() {
                outcome.delete_failures += 1;
                continue;
            }
            let Ok(key) = ObjectKey::new(segment.segment_key) else {
                outcome.delete_failures += 1;
                continue;
            };
            match self.objects.delete(&key) {
                Ok(true) => outcome.objects_deleted += 1,
                Ok(false) => outcome.objects_missing += 1,
                Err(_) => outcome.delete_failures += 1,
            }
        }
        outcome
    }

    pub fn disable_sync_metadata_log(&self) -> Result<(), MetadError> {
        let _log_enable_fence = self
            .metadata_log_enable_fence
            .write()
            .unwrap_or_else(|err| err.into_inner());
        let _commit_log_guard = self
            .metadata_commit_log_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.resolve_unresolved_metadata_commit_group_locked()?;
        self.flush_pending_metadata_log_segment_locked()
            .map_err(|err| MetadError::SyncLogArchiveFailed {
                committed: true,
                message: err.to_string(),
            })?;
        *self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner()) = None;
        Ok(())
    }

    pub fn sync_metadata_log_snapshot(&self) -> Option<MetadataLogSyncSnapshot> {
        self.metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(MetadataLogSyncState::snapshot)
    }

    /// Read the logical tail and its unpublishable local states under one lock.
    /// A caller may treat a cached control tail as clean only when both flags
    /// are false and the snapshot identity matches exactly.
    pub fn sync_metadata_log_publication_state(&self) -> Option<MetadataLogPublicationState> {
        self.metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(|state| MetadataLogPublicationState {
                snapshot: state.snapshot(),
                has_pending_segment: state.pending_segment.is_some(),
                has_unresolved_commit_group: state.unresolved_commit_group.is_some(),
            })
    }

    /// Resolve every ambiguous apply and flush the exact committed segment
    /// before a control-plane recovery reference is selected. This uses the
    /// same lock order as metadata commit and checkpoint export.
    pub fn flush_sync_metadata_log_for_publication(&self) -> Result<(), MetadError> {
        fn committed_failure(err: MetadError) -> MetadError {
            match err {
                MetadError::SyncLogArchiveFailed { message, .. } => {
                    MetadError::SyncLogArchiveFailed {
                        committed: true,
                        message,
                    }
                }
                err => MetadError::SyncLogArchiveFailed {
                    committed: true,
                    message: err.to_string(),
                },
            }
        }

        let _log_enable_fence = self
            .metadata_log_enable_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        let enabled = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_some();
        if !enabled {
            return Ok(());
        }
        let _commit_log_guard = self
            .metadata_commit_log_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.resolve_unresolved_metadata_commit_group_locked()
            .map_err(committed_failure)?;
        self.flush_pending_metadata_log_segment_locked()
            .map_err(committed_failure)
    }

    /// Retry a segment left pending by a prior committed metadata command.
    /// The caller must hold `metadata_commit_log_gate`, keeping the retry and
    /// any following metadata apply in one total order.
    pub(super) fn flush_pending_metadata_log_segment_locked(&self) -> Result<(), MetadError> {
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let Some(state) = guard.as_mut() else {
            return Ok(());
        };
        let Some(segment) = state.pending_segment.clone() else {
            return Ok(());
        };
        self.archive_and_advance_metadata_log_state(state, &segment)?;
        state.pending_segment = None;
        Ok(())
    }

    /// Resolve one exact commit group using the metadata engine's authoritative
    /// request-id lookup. The caller must hold `metadata_commit_log_gate`.
    ///
    /// Only `Backend` outcomes are ambiguous. `Some` is accepted solely when it
    /// equals the result derived from the original command; `None` is the
    /// authoritative proof that the command did not apply. Any lookup error or
    /// mismatch leaves the whole group unresolved.
    pub(super) fn reconcile_metadata_commit_group_locked(
        &self,
        group: &UnresolvedMetadataCommitGroup,
    ) -> Result<Vec<Result<CommitResult, MetadataError>>, MetadError> {
        let mut resolved = group.results.clone();
        for (index, (command, original_result)) in
            group.commands.iter().zip(&group.results).enumerate()
        {
            if !matches!(original_result, Err(MetadataError::Backend(_))) {
                continue;
            }
            let expected = command.expected_commit_result();
            match self.metadata.committed_request_result(&command.request_id) {
                Ok(Some(actual)) if actual == expected => resolved[index] = Ok(actual),
                Ok(Some(actual)) => {
                    return Err(MetadError::Codec(format!(
                        "metadata commit readback result mismatch: expected {expected:?}, got {actual:?}"
                    )));
                }
                Ok(None) => {
                    // Keep the original Backend error: authoritative lookup
                    // proved that this command did not apply.
                }
                Err(err) => {
                    return Err(MetadError::Codec(format!(
                        "metadata commit readback failed: {err}"
                    )));
                }
            }
        }
        Ok(resolved)
    }

    /// Retain an unresolved group before returning its uncertainty to the
    /// caller. The caller must hold `metadata_commit_log_gate`.
    pub(super) fn defer_unresolved_metadata_commit_group_locked(
        &self,
        group: UnresolvedMetadataCommitGroup,
    ) -> Result<(), MetadError> {
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let state = guard.as_mut().ok_or_else(|| {
            MetadError::Codec(
                "synchronous metadata log was disabled during commit resolution".to_owned(),
            )
        })?;
        if state.pending_segment.is_some() {
            return Err(MetadError::Codec(
                "metadata log has a pending segment before unresolved commit state".to_owned(),
            ));
        }
        if state.unresolved_commit_group.is_some() {
            return Err(MetadError::Codec(
                "metadata log already has an unresolved commit group".to_owned(),
            ));
        }
        state.unresolved_commit_group = Some(group);
        Ok(())
    }

    /// Resolve and archive the exact group left by a prior ambiguous backend
    /// outcome. No later apply or checkpoint capture may pass this method while
    /// the authoritative lookup still fails or disagrees. The caller must hold
    /// `metadata_commit_log_gate`.
    pub(super) fn resolve_unresolved_metadata_commit_group_locked(&self) -> Result<(), MetadError> {
        let group = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .and_then(|state| state.unresolved_commit_group.clone());
        let Some(group) = group else {
            return Ok(());
        };

        let resolved = self.reconcile_metadata_commit_group_locked(&group)?;
        let committed = group
            .commands
            .iter()
            .zip(&resolved)
            .filter_map(|(command, result)| {
                result
                    .as_ref()
                    .ok()
                    .map(|result| (command.clone(), result.clone()))
            })
            .collect::<Vec<_>>();

        {
            let mut guard = self
                .metadata_log_sync
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            let state = guard.as_mut().ok_or_else(|| {
                MetadError::Codec(
                    "synchronous metadata log was disabled during commit resolution".to_owned(),
                )
            })?;
            if state.unresolved_commit_group.as_ref() != Some(&group) {
                return Err(MetadError::Codec(
                    "metadata commit resolution group changed unexpectedly".to_owned(),
                ));
            }
            state.unresolved_commit_group = None;
        }

        let log_commands = committed
            .iter()
            .map(|(command, result)| (command, result))
            .collect::<Vec<_>>();
        if let Err(err) = self.record_committed_metadata_commands(&log_commands) {
            let mut guard = self
                .metadata_log_sync
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let pending = guard
                .as_ref()
                .is_some_and(|state| state.pending_segment.is_some());
            if !pending {
                if let Some(state) = guard.as_mut() {
                    state.unresolved_commit_group = Some(group);
                }
                return Err(err);
            }
            return Err(MetadError::SyncLogArchiveFailed {
                committed: true,
                message: err.to_string(),
            });
        }
        Ok(())
    }

    /// Prove before metadata apply that every command which can succeed has a
    /// representable LSN and can be sealed into the logical log. Predicate
    /// failures may reduce the actual successful subset, so reserving for all
    /// structurally valid commands is conservative and fail-closed.
    /// The caller must hold `metadata_commit_log_gate`.
    pub(super) fn preflight_sync_metadata_log_locked(
        &self,
        commands: &[MetadataCommand],
    ) -> Result<(), MetadError> {
        let guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let Some(state) = guard.as_ref() else {
            return Ok(());
        };
        if state.unresolved_commit_group.is_some() {
            return Err(MetadError::Codec(
                "metadata log has an unresolved commit group".to_owned(),
            ));
        }
        if state.pending_segment.is_some() {
            return Err(MetadError::Codec(
                "metadata log has an unflushed committed segment".to_owned(),
            ));
        }

        let mut entries = Vec::new();
        let mut next_lsn = state.next_lsn;
        let mut prev_digest = state.prev_digest;
        for command in commands {
            // Structurally invalid commands are rejected independently by the
            // metadata engine and therefore consume no logical-log LSN.
            if command.validate().is_err() {
                continue;
            }
            let result = command.expected_commit_result();
            let entry = MetadataLogEntry::seal(
                state.shard_id.clone(),
                state.epoch,
                next_lsn,
                command.clone(),
                result,
                prev_digest,
            )
            .map_err(|err| MetadError::Codec(format!("metadata log preflight failed: {err}")))?;
            next_lsn = next_lsn.checked_add(1).ok_or_else(|| {
                MetadError::Codec("metadata log LSN is exhausted before commit".to_owned())
            })?;
            prev_digest = entry.digest;
            entries.push(entry);
        }
        if !entries.is_empty() {
            MetadataLogSegment::seal(entries).map_err(|err| {
                MetadError::Codec(format!("metadata log segment preflight failed: {err}"))
            })?;
        }
        Ok(())
    }

    fn archive_and_advance_metadata_log_state(
        &self,
        state: &mut MetadataLogSyncState,
        segment: &MetadataLogSegment,
    ) -> Result<(), MetadError> {
        if segment.first_lsn != state.next_lsn || segment.prev_digest != state.prev_digest {
            return Err(MetadError::Codec(
                "pending metadata log segment does not follow the durable sync-log tail".to_owned(),
            ));
        }
        let next_lsn = segment
            .last_lsn
            .checked_add(1)
            .ok_or_else(|| MetadError::Codec("metadata log LSN is exhausted".to_owned()))?;
        let archived =
            archive_metadata_log_segment_to_store(&self.objects, &state.config, segment)?;
        self.metadata_log_segments_archived_total
            .fetch_add(1, Ordering::Relaxed);
        self.metadata_log_entries_archived_total
            .fetch_add(segment.entries.len() as u64, Ordering::Relaxed);
        self.metadata_log_archive_bytes_total
            .fetch_add(archived.encoded_bytes, Ordering::Relaxed);
        state.next_lsn = next_lsn;
        state.prev_digest = segment.last_digest;
        state.segments.push(MetadataLogSegmentPointer {
            segment_key: archived.segment_key,
            first_lsn: segment.first_lsn,
            last_lsn: segment.last_lsn,
            last_digest: segment.last_digest,
        });
        Ok(())
    }

    pub(super) fn record_committed_metadata_command(
        &self,
        command: &MetadataCommand,
        result: &CommitResult,
    ) -> Result<Option<MetadataLogSyncSnapshot>, MetadError> {
        self.record_committed_metadata_commands(&[(command, result)])
    }

    pub(super) fn record_committed_metadata_commands(
        &self,
        commands: &[(&MetadataCommand, &CommitResult)],
    ) -> Result<Option<MetadataLogSyncSnapshot>, MetadError> {
        if commands.is_empty() {
            return Ok(None);
        }
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let Some(state) = guard.as_mut() else {
            return Ok(None);
        };
        if state.unresolved_commit_group.is_some() {
            return Err(MetadError::Codec(
                "metadata log has an unresolved commit group".to_owned(),
            ));
        }
        if state.pending_segment.is_some() {
            return Err(MetadError::Codec(
                "metadata log has an unflushed committed segment".to_owned(),
            ));
        }

        let mut entries = Vec::with_capacity(commands.len());
        let mut next_lsn = state.next_lsn;
        let mut prev_digest = state.prev_digest;
        for (command, result) in commands {
            let entry = MetadataLogEntry::seal(
                state.shard_id.clone(),
                state.epoch,
                next_lsn,
                (*command).clone(),
                (*result).clone(),
                prev_digest,
            )
            .map_err(|err| MetadError::Codec(format!("metadata log entry seal failed: {err}")))?;
            next_lsn = next_lsn.checked_add(1).ok_or_else(|| {
                MetadError::Codec("metadata log LSN is exhausted before archive".to_owned())
            })?;
            prev_digest = entry.digest;
            entries.push(entry);
        }

        let segment = MetadataLogSegment::seal(entries)
            .map_err(|err| MetadError::Codec(format!("metadata log segment seal failed: {err}")))?;
        // Retain the exact deterministic segment before the first archive
        // attempt. Failure leaves it pending and prevents every subsequent
        // command from applying until this same content-addressed object is
        // durably retried.
        state.pending_segment = Some(segment.clone());
        self.archive_and_advance_metadata_log_state(state, &segment)?;
        state.pending_segment = None;
        Ok(Some(state.snapshot()))
    }
}
