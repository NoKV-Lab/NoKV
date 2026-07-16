//! Synchronous logical metadata log archiving for commit ACK durability.

use super::log_archive::archive_metadata_log_segment_to_store;
use super::*;
use crate::{MetadataLogEntry, MetadataLogSegment};

const PREPARED_TERMINAL_PROOF_CACHE_LIMIT: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ArchivedPreparedRequestResult {
    result: CommitResult,
    lsn: u64,
}

#[derive(Clone, Debug)]
struct PreparedArchiveProofSnapshot {
    archive: MetadataLogArchiveConfig,
    shard_id: String,
    segments: Vec<MetadataLogSegmentPointer>,
    baseline_lsn: u64,
    baseline_digest: [u8; 32],
    durable_tail_lsn: u64,
    durable_tail_digest: [u8; 32],
}

fn trim_prepared_terminal_proof_cache(
    proofs: &mut BTreeMap<Vec<u8>, ArchivedPreparedRequestResult>,
) {
    while proofs.len() > PREPARED_TERMINAL_PROOF_CACHE_LIMIT {
        let Some(oldest_request_id) = proofs
            .iter()
            .min_by(|(left_id, left), (right_id, right)| {
                left.lsn.cmp(&right.lsn).then_with(|| left_id.cmp(right_id))
            })
            .map(|(request_id, _)| request_id.clone())
        else {
            break;
        };
        proofs.remove(&oldest_request_id);
    }
}

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
    /// Exact checkpoint boundary below the inherited active segment chain.
    /// This differs from `durable_lsn` after failover, where `durable_lsn`
    /// names the tail and the baseline names the checkpoint before replay.
    pub durable_recovery_baseline_lsn: u64,
    pub durable_recovery_baseline_digest: [u8; 32],
    /// Segment chain inherited from the control record (e.g. after failover),
    /// so the new owner's future `LogRef` publishes keep the full chain.
    pub segments: Vec<MetadataLogSegmentPointer>,
    /// Whether `durable_recovery_baseline_*` names an externally authoritative
    /// checkpoint. Prepared terminal durability never relies on metadata
    /// versions; it uses this baseline plus the exact active LSN chain.
    pub has_authoritative_recovery_baseline: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSyncSnapshot {
    pub shard_id: String,
    pub epoch: u64,
    pub durable_lsn: u64,
    pub last_digest: [u8; 32],
    /// Ordered (oldest first) segment chain above the latest checkpoint.
    pub segments: Vec<MetadataLogSegmentPointer>,
    pub has_authoritative_recovery_baseline: bool,
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
    has_authoritative_recovery_baseline: bool,
    durable_recovery_covered_lsn: u64,
    durable_recovery_covered_digest: [u8; 32],
    /// Prepared-publish terminal results whose exact commands are present in
    /// an archived segment above the latest authoritative checkpoint.
    archived_prepared_request_results: BTreeMap<Vec<u8>, ArchivedPreparedRequestResult>,
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
            has_authoritative_recovery_baseline: self.has_authoritative_recovery_baseline,
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
            durable_recovery_baseline_lsn: durable_lsn,
            durable_recovery_baseline_digest: last_digest,
            segments: Vec::new(),
            has_authoritative_recovery_baseline: false,
        }
    }

    /// Inherit a previously-archived segment chain (above the latest checkpoint)
    /// so future `LogRef` publishes keep the full chain after a failover restore.
    pub fn with_segments(mut self, segments: Vec<MetadataLogSegmentPointer>) -> Self {
        self.segments = segments;
        self
    }

    /// Set the authoritative checkpoint boundary below an inherited log tail.
    /// The active segment chain must connect this digest to `durable_lsn`.
    pub fn with_durable_recovery_baseline(mut self, lsn: u64, digest: [u8; 32]) -> Self {
        self.durable_recovery_baseline_lsn = lsn;
        self.durable_recovery_baseline_digest = digest;
        self
    }

    /// Mark the configured recovery baseline as externally authoritative.
    pub fn with_authoritative_recovery_baseline(mut self) -> Self {
        self.has_authoritative_recovery_baseline = true;
        self
    }
}

struct ImmediateMetadataLogPublication<'a> {
    depth: &'a AtomicUsize,
}

impl Drop for ImmediateMetadataLogPublication<'_> {
    fn drop(&mut self) {
        let previous = self.depth.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "metadata log publication scope underflow");
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Install the owner-fenced callback used to publish an exact archived log
    /// tail to the external control plane. A controlled server installs this
    /// once, after enabling synchronous logging and before accepting requests.
    pub fn install_sync_metadata_log_publication_hook<F>(&self, hook: F) -> Result<(), MetadError>
    where
        F: Fn(&MetadataLogSyncSnapshot) -> Result<(), String> + Send + Sync + 'static,
    {
        if self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_none()
        {
            return Err(MetadError::Codec(
                "cannot install metadata log publication hook before enabling the log".to_owned(),
            ));
        }
        let mut installed = self
            .metadata_log_publication_hook
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if installed.is_some() {
            return Err(MetadError::Codec(
                "metadata log publication hook is already installed".to_owned(),
            ));
        }
        *installed = Some(Arc::new(hook));
        Ok(())
    }

    /// Run a multi-command operation with a control publication barrier after
    /// every command whose shared-log segment archived successfully. The
    /// callback is invoked before the metadata API reports that command as
    /// applied, so an applied-phase crash point is always reachable from the
    /// control record rather than only from an unreferenced object.
    pub fn with_immediate_sync_metadata_log_publication<T>(
        &self,
        execute: impl FnOnce() -> Result<T, MetadError>,
    ) -> Result<T, MetadError> {
        self.metadata_log_immediate_publication_depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
                depth.checked_add(1)
            })
            .map_err(|_| {
                MetadError::Codec("metadata log publication scope depth is exhausted".to_owned())
            })?;
        let _publication = ImmediateMetadataLogPublication {
            depth: &self.metadata_log_immediate_publication_depth,
        };
        execute()
    }

    fn publish_archived_metadata_log_tail_immediately(
        &self,
        snapshot: &MetadataLogSyncSnapshot,
    ) -> Result<(), MetadError> {
        if self
            .metadata_log_immediate_publication_depth
            .load(Ordering::Acquire)
            == 0
        {
            return Ok(());
        }
        let hook = self
            .metadata_log_publication_hook
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or_else(|| {
                MetadError::Codec(
                    "immediate metadata log publication has no control-plane hook".to_owned(),
                )
            })?;
        hook(snapshot).map_err(|message| {
            MetadError::Codec(format!(
                "immediate metadata log control publication failed: {message}"
            ))
        })
    }

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
        if config.durable_recovery_baseline_lsn > config.durable_lsn {
            return Err(MetadError::Codec(
                "metadata recovery baseline is above the durable log tail".to_owned(),
            ));
        }
        if config.durable_recovery_baseline_lsn == config.durable_lsn
            && config.durable_recovery_baseline_digest != config.last_digest
        {
            return Err(MetadError::Codec(
                "metadata recovery baseline digest differs from the durable log tail".to_owned(),
            ));
        }
        if config.durable_recovery_baseline_lsn < config.durable_lsn {
            let mut expected_lsn = config
                .durable_recovery_baseline_lsn
                .checked_add(1)
                .ok_or_else(|| MetadError::Codec("metadata log LSN is exhausted".to_owned()))?;
            let mut observed_tail_digest = None;
            for pointer in config
                .segments
                .iter()
                .filter(|pointer| pointer.last_lsn > config.durable_recovery_baseline_lsn)
            {
                if pointer.first_lsn <= config.durable_recovery_baseline_lsn
                    || pointer.first_lsn > pointer.last_lsn
                    || pointer.first_lsn != expected_lsn
                {
                    return Err(MetadError::Codec(
                        "metadata recovery segment pointers do not form a continuous LSN chain"
                            .to_owned(),
                    ));
                }
                expected_lsn = pointer
                    .last_lsn
                    .checked_add(1)
                    .ok_or_else(|| MetadError::Codec("metadata log LSN is exhausted".to_owned()))?;
                observed_tail_digest = Some(pointer.last_digest);
            }
            if expected_lsn - 1 != config.durable_lsn
                || observed_tail_digest != Some(config.last_digest)
            {
                return Err(MetadError::Codec(
                    "metadata recovery segment pointers do not reach the durable log tail"
                        .to_owned(),
                ));
            }
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
            has_authoritative_recovery_baseline: config.has_authoritative_recovery_baseline,
            durable_recovery_covered_lsn: config.durable_recovery_baseline_lsn,
            durable_recovery_covered_digest: config.durable_recovery_baseline_digest,
            archived_prepared_request_results: BTreeMap::new(),
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
        *self
            .metadata_log_publication_hook
            .write()
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

    #[cfg(test)]
    pub(super) fn prepared_terminal_proof_cache_len(&self) -> Option<usize> {
        self.metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .map(|state| state.archived_prepared_request_results.len())
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

    /// Return an already-committed prepared publish only when its local Holt
    /// apply marker also has a cross-machine durability proof. This is kept out
    /// of the generic commit funnel: ordinary request ids do not bind the full
    /// prepared payload and a raw local marker is not a shared-log proof.
    pub(super) fn prepared_terminal_commit_result(
        &self,
        request_id: &[u8],
        expected_commit_version: Version,
    ) -> Result<Option<CommitResult>, MetadError> {
        let _log_enable_fence = self
            .metadata_log_enable_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        let log_enabled = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_some();
        let _commit_log_guard = log_enabled.then(|| {
            self.metadata_commit_log_gate
                .lock()
                .unwrap_or_else(|err| err.into_inner())
        });
        if log_enabled {
            self.resolve_unresolved_metadata_commit_group_locked()?;
            self.flush_pending_metadata_log_segment_locked()
                .map_err(|err| MetadError::SyncLogArchiveFailed {
                    committed: true,
                    message: err.to_string(),
                })?;
        }

        {
            let _epoch_fence = self
                .epoch_fence
                .read()
                .unwrap_or_else(|err| err.into_inner());
            self.ensure_owner_epoch_current()?;
        }
        let Some(result) = self.metadata.committed_request_result(request_id)? else {
            return Ok(None);
        };
        if result.commit_version != expected_commit_version {
            return Err(MetadError::Codec(format!(
                "prepared publish terminal version mismatch: expected {}, got {}",
                expected_commit_version.get(),
                result.commit_version.get()
            )));
        }
        if log_enabled {
            let guard = self
                .metadata_log_sync
                .lock()
                .unwrap_or_else(|err| err.into_inner());
            let state = guard.as_ref().ok_or_else(|| {
                MetadError::Codec(
                    "synchronous metadata log was disabled during prepared terminal lookup"
                        .to_owned(),
                )
            })?;
            let cached_lsn = state
                .archived_prepared_request_results
                .get(request_id)
                .map(|archived| {
                    if archived.result != result {
                        return Err(MetadError::Codec(
                            "prepared publish cached archive result changed identity".to_owned(),
                        ));
                    }
                    Ok(archived.lsn)
                })
                .transpose()?;
            let proof = PreparedArchiveProofSnapshot {
                archive: state.config.clone(),
                shard_id: state.shard_id.clone(),
                segments: state.segments.clone(),
                baseline_lsn: state.durable_recovery_covered_lsn,
                baseline_digest: state.durable_recovery_covered_digest,
                durable_tail_lsn: state.next_lsn - 1,
                durable_tail_digest: state.prev_digest,
            };
            let has_authoritative_baseline = state.has_authoritative_recovery_baseline;
            drop(guard);
            if !self
                .prepared_request_is_in_archived_segments(request_id, &result, cached_lsn, &proof)?
                && !has_authoritative_baseline
            {
                return Ok(None);
            }
        }
        // Object-store proof can be slow, so it deliberately runs without the
        // owner-epoch read fence. Reacquire the fence afterwards, re-read the
        // immutable local terminal marker, and perform the lease/epoch check
        // last so failover or lease expiry during proof cannot return success
        // from a stale owner.
        let _epoch_fence = self
            .epoch_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        let final_result = self.metadata.committed_request_result(request_id)?;
        if final_result.as_ref() != Some(&result) {
            return Err(MetadError::Codec(
                "prepared publish terminal marker changed during archive proof".to_owned(),
            ));
        }
        self.ensure_owner_epoch_current()?;
        Ok(Some(result))
    }

    /// Record that a checkpoint became authoritative in the external control
    /// plane. The exact logical-log boundary is validated before archived
    /// per-request proofs covered by the checkpoint are compacted away.
    pub fn acknowledge_published_metadata_checkpoint(
        &self,
        log_lsn: u64,
        log_digest: [u8; 32],
    ) -> Result<(), MetadError> {
        let _log_enable_fence = self
            .metadata_log_enable_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        let log_enabled = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .is_some();
        if !log_enabled {
            return Ok(());
        }
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
        self.ensure_owner_epoch_current()?;
        let mut guard = self
            .metadata_log_sync
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let state = guard.as_mut().ok_or_else(|| {
            MetadError::Codec(
                "synchronous metadata log was disabled during checkpoint acknowledgement"
                    .to_owned(),
            )
        })?;
        if log_lsn < state.durable_recovery_covered_lsn {
            return Err(MetadError::Codec(
                "published checkpoint regresses durable recovery coverage".to_owned(),
            ));
        }
        let current_lsn = state.next_lsn - 1;
        let boundary_matches = if log_lsn == state.durable_recovery_covered_lsn {
            log_digest == state.durable_recovery_covered_digest
        } else if log_lsn == current_lsn {
            log_digest == state.prev_digest
        } else {
            state
                .segments
                .iter()
                .any(|segment| segment.last_lsn == log_lsn && segment.last_digest == log_digest)
        };
        if !boundary_matches {
            return Err(MetadError::Codec(format!(
                "published checkpoint log boundary {log_lsn} does not match the durable local chain"
            )));
        }

        state.has_authoritative_recovery_baseline = true;
        state.durable_recovery_covered_lsn = log_lsn;
        state.durable_recovery_covered_digest = log_digest;
        state
            .archived_prepared_request_results
            .retain(|_, proof| proof.lsn > log_lsn);
        Ok(())
    }

    fn prepared_request_is_in_archived_segments(
        &self,
        request_id: &[u8],
        expected: &CommitResult,
        cached_lsn: Option<u64>,
        proof: &PreparedArchiveProofSnapshot,
    ) -> Result<bool, MetadError> {
        if cached_lsn.is_some_and(|lsn| lsn <= proof.baseline_lsn) {
            return Err(MetadError::Codec(
                "prepared publish cache retained a checkpoint-covered LSN".to_owned(),
            ));
        }
        let mut expected_first_lsn = proof.baseline_lsn.checked_add(1);
        let mut expected_prev_digest = proof.baseline_digest;
        let mut found = false;
        for pointer in &proof.segments {
            if pointer.last_lsn <= proof.baseline_lsn {
                continue;
            }
            let Some(first_lsn) = expected_first_lsn else {
                return Err(MetadError::Codec(
                    "prepared publish archive LSN is exhausted".to_owned(),
                ));
            };
            if pointer.first_lsn <= proof.baseline_lsn
                || pointer.first_lsn > pointer.last_lsn
                || pointer.first_lsn != first_lsn
            {
                return Err(MetadError::Codec(
                    "prepared publish archive chain has an LSN gap".to_owned(),
                ));
            }
            proof.archive.validate_segment_key(&pointer.segment_key)?;
            let segment = self.load_metadata_log_segment(&pointer.segment_key)?;
            if segment.first_lsn != pointer.first_lsn
                || segment.last_lsn != pointer.last_lsn
                || segment.last_digest != pointer.last_digest
                || segment.prev_digest != expected_prev_digest
                || segment.shard_id != proof.shard_id
            {
                return Err(MetadError::Codec(
                    "prepared publish archive pointer changed segment identity".to_owned(),
                ));
            }
            for entry in &segment.entries {
                if entry.request_id != request_id {
                    continue;
                }
                if !is_prepared_artifact_request_id(entry.command.kind, &entry.request_id)
                    || entry.result != *expected
                    || cached_lsn.is_some_and(|lsn| entry.lsn != lsn)
                    || found
                {
                    return Err(MetadError::Codec(
                        "prepared publish archived result changed identity".to_owned(),
                    ));
                }
                found = true;
            }
            if !found && cached_lsn.is_some_and(|lsn| lsn <= pointer.last_lsn) {
                return Err(MetadError::Codec(
                    "prepared publish cached archive locator is missing".to_owned(),
                ));
            }
            expected_first_lsn = pointer.last_lsn.checked_add(1);
            expected_prev_digest = pointer.last_digest;
        }
        if cached_lsn.is_some() && !found {
            return Err(MetadError::Codec(
                "prepared publish cached archive segment is missing".to_owned(),
            ));
        }
        let observed_tail_lsn = expected_first_lsn
            .map(|next_lsn| next_lsn - 1)
            .unwrap_or(u64::MAX);
        if observed_tail_lsn != proof.durable_tail_lsn
            || expected_prev_digest != proof.durable_tail_digest
        {
            return Err(MetadError::Codec(
                "prepared publish archive chain does not reach the durable log tail".to_owned(),
            ));
        }
        Ok(found)
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
        let mut prepared_results: BTreeMap<Vec<u8>, ArchivedPreparedRequestResult> =
            BTreeMap::new();
        for entry in &segment.entries {
            if !is_prepared_artifact_request_id(entry.command.kind, &entry.request_id) {
                continue;
            }
            if let Some(existing) = state
                .archived_prepared_request_results
                .get(&entry.request_id)
                .or_else(|| prepared_results.get(&entry.request_id))
            {
                if existing.result != entry.result {
                    return Err(MetadError::Codec(
                        "prepared publish request id has conflicting archived results".to_owned(),
                    ));
                }
            } else {
                prepared_results.insert(
                    entry.request_id.clone(),
                    ArchivedPreparedRequestResult {
                        result: entry.result.clone(),
                        lsn: entry.lsn,
                    },
                );
            }
        }
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
        state
            .archived_prepared_request_results
            .extend(prepared_results);
        trim_prepared_terminal_proof_cache(&mut state.archived_prepared_request_results);
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
        let snapshot = state.snapshot();
        drop(guard);
        self.publish_archived_metadata_log_tail_immediately(&snapshot)?;
        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod prepared_terminal_proof_cache_tests {
    use super::*;

    #[test]
    fn proof_cache_is_bounded_and_evicts_oldest_lsn_first() {
        let mut proofs = BTreeMap::new();
        for index in 0..(PREPARED_TERMINAL_PROOF_CACHE_LIMIT + 2) {
            proofs.insert(
                (index as u64).to_be_bytes().to_vec(),
                ArchivedPreparedRequestResult {
                    result: CommitResult {
                        commit_version: Version::new(1).unwrap(),
                        applied_mutations: 1,
                        watch_events: 0,
                    },
                    lsn: index as u64 + 1,
                },
            );
        }

        trim_prepared_terminal_proof_cache(&mut proofs);

        assert_eq!(proofs.len(), PREPARED_TERMINAL_PROOF_CACHE_LIMIT);
        assert!(!proofs.contains_key(0_u64.to_be_bytes().as_slice()));
        assert!(!proofs.contains_key(1_u64.to_be_bytes().as_slice()));
        assert_eq!(proofs.values().map(|proof| proof.lsn).min(), Some(3));
    }
}
