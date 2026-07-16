use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

/// A reason the owner is not allowed to commit right now. `Copy` so the batch
/// path can replicate one fault across every command without cloning
/// [`MetadError`] (which is not `Clone`), keeping the check logic single-sourced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OwnerLeaseFault {
    CheckpointInstallUncertain,
    StaleEpoch {
        owner_epoch: u64,
        required_epoch: u64,
    },
    LeaseExpired {
        now_ms: u64,
        deadline_ms: u64,
    },
}

impl OwnerLeaseFault {
    fn into_error(self) -> MetadError {
        match self {
            OwnerLeaseFault::CheckpointInstallUncertain => {
                MetadError::MetadataCheckpointInstallUncertain
            }
            OwnerLeaseFault::StaleEpoch {
                owner_epoch,
                required_epoch,
            } => MetadError::StaleOwnerEpoch {
                owner_epoch,
                required_epoch,
            },
            OwnerLeaseFault::LeaseExpired {
                now_ms,
                deadline_ms,
            } => MetadError::LeaseExpired {
                now_ms,
                deadline_ms,
            },
        }
    }
}

/// Brackets one authoritative restore-graph metadata apply. The paired
/// sequence increments make an overlapping reader reject its mixed System
/// view; the active count closes the windows on either side of those bumps.
pub(super) struct RestoreGraphWriteGuard<'a> {
    sequence: &'a AtomicU64,
    writers: &'a AtomicUsize,
}

impl<'a> RestoreGraphWriteGuard<'a> {
    fn new(sequence: &'a AtomicU64, writers: &'a AtomicUsize) -> Self {
        writers.fetch_add(1, Ordering::SeqCst);
        sequence.fetch_add(1, Ordering::SeqCst);
        Self { sequence, writers }
    }
}

impl Drop for RestoreGraphWriteGuard<'_> {
    fn drop(&mut self) {
        self.sequence.fetch_add(1, Ordering::SeqCst);
        self.writers.fetch_sub(1, Ordering::SeqCst);
    }
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub(super) fn begin_restore_graph_write(&self) -> RestoreGraphWriteGuard<'_> {
        RestoreGraphWriteGuard::new(&self.restore_graph_sequence, &self.restore_graph_writers)
    }

    fn restore_graph_write_for_command(
        &self,
        command: &MetadataCommand,
    ) -> Option<RestoreGraphWriteGuard<'_>> {
        super::restore::command_mutates_restore_graph(self.mount, command)
            .then(|| self.begin_restore_graph_write())
    }

    fn restore_graph_write_for_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Option<RestoreGraphWriteGuard<'_>> {
        commands
            .iter()
            .any(|command| super::restore::command_mutates_restore_graph(self.mount, command))
            .then(|| self.begin_restore_graph_write())
    }

    /// Capture a quiescent restore-graph sequence. Writers register before
    /// their durable apply, so observing either an active writer or a later
    /// sequence change invalidates a current-only System scan.
    pub(super) fn restore_graph_read_sequence(&self) -> Option<u64> {
        if self.restore_graph_writers.load(Ordering::SeqCst) != 0 {
            return None;
        }
        let sequence = self.restore_graph_sequence.load(Ordering::SeqCst);
        (self.restore_graph_writers.load(Ordering::SeqCst) == 0).then_some(sequence)
    }

    pub(super) fn restore_graph_read_is_stable(&self, sequence: u64) -> bool {
        self.restore_graph_writers.load(Ordering::SeqCst) == 0
            && self.restore_graph_sequence.load(Ordering::SeqCst) == sequence
            && self.restore_graph_writers.load(Ordering::SeqCst) == 0
    }

    pub fn install_owner_epoch(&self, epoch: u64) -> Result<(), MetadError> {
        validate_owner_epoch(epoch)?;
        let _restore_graph_write = self.begin_restore_graph_write();
        // Fail closed while ownership changes. The startup path rebuilds the
        // optimization after bootstrap or checkpoint/log recovery.
        self.mark_restore_staging_possible();
        // Write guard: wait for in-flight commits to finish, then raise both
        // epochs together so no commit ever observes a torn (epoch, required)
        // pair or applies under a superseded epoch.
        {
            let _fence = self
                .epoch_fence
                .write()
                .unwrap_or_else(|err| err.into_inner());
            self.epoch.fetch_max(epoch, Ordering::Relaxed);
            self.required_owner_epoch
                .fetch_max(epoch, Ordering::Relaxed);
        }
        // Controlled failover installs the owner epoch before installing a
        // checkpoint into a pristine Holt store. Do not scan in that state: a
        // read would create trees and make wholesale image install fail. An
        // already-open/bootstrap-complete shard, however, must reconstruct the
        // fast-path hint after this fail-closed epoch boundary.
        if self
            .restore_visibility_recovery_ready
            .load(Ordering::Acquire)
        {
            self.recover_restore_staging_visibility()?;
        }
        Ok(())
    }

    pub fn observe_required_owner_epoch(&self, epoch: u64) -> Result<(), MetadError> {
        validate_owner_epoch(epoch)?;
        let _restore_graph_write = self.begin_restore_graph_write();
        self.mark_restore_staging_possible();
        self.fence_required_owner_epoch(epoch)?;
        self.recover_restore_staging_visibility()
    }

    /// Raise only the commit fence after observing a newer control-plane epoch.
    /// Publication callbacks use this form because they can run while the
    /// current restore command still owns the restore-visibility fence; trying
    /// to rebuild that process-local hint on the same thread would deadlock.
    /// The stale owner cannot serve through the recovery publication gate, and
    /// the successor reconstructs restore visibility from durable state.
    pub fn fence_required_owner_epoch(&self, epoch: u64) -> Result<(), MetadError> {
        validate_owner_epoch(epoch)?;
        // Write guard: a failover bump waits for in-flight commits, then raises
        // the required epoch so every subsequent commit is fenced.
        let _fence = self
            .epoch_fence
            .write()
            .unwrap_or_else(|err| err.into_inner());
        self.required_owner_epoch
            .fetch_max(epoch, Ordering::Relaxed);
        Ok(())
    }

    pub fn required_owner_epoch(&self) -> u64 {
        self.required_owner_epoch.load(Ordering::Relaxed)
    }

    /// Current wall-clock time in ms since the Unix epoch, honoring the test/
    /// simulation clock override when set.
    pub fn now_ms(&self) -> u64 {
        let override_ms = self.clock_override_ms.load(Ordering::Relaxed);
        if override_ms != 0 {
            return override_ms;
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Override the clock used for lease-deadline fencing (`0` restores the
    /// system clock). For deterministic tests and partition simulations.
    pub fn set_clock_override_ms(&self, now_ms: u64) {
        self.clock_override_ms.store(now_ms, Ordering::Relaxed);
    }

    /// Arm the owner's self-fence: refuse commits once `now_ms()` passes
    /// `deadline_ms`. `0` disables it. Owners pass `basis + lease_ttl` where
    /// `basis` is captured *before* the control-plane renew, so the local
    /// deadline never outlives the control plane's own lease expiry.
    pub fn set_lease_deadline(&self, deadline_ms: u64) {
        self.lease_deadline_ms.store(deadline_ms, Ordering::Relaxed);
    }

    pub fn disable_lease_deadline(&self) {
        self.lease_deadline_ms.store(0, Ordering::Relaxed);
    }

    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms.load(Ordering::Relaxed)
    }

    /// Verify that this service still holds a current, unexpired owner fence
    /// before performing an external side effect such as publishing recovery
    /// metadata. The control-plane CAS remains the final authority.
    pub fn verify_owner_lease(&self) -> Result<(), MetadError> {
        self.ensure_owner_epoch_current()
    }

    /// Single source of truth for "may this owner commit?": epoch fence first,
    /// then the wall-clock lease deadline (the partition-safe self-fence).
    pub(super) fn check_owner_lease(&self) -> Result<(), OwnerLeaseFault> {
        if self
            .metadata_checkpoint_install_uncertain
            .load(Ordering::Acquire)
        {
            return Err(OwnerLeaseFault::CheckpointInstallUncertain);
        }
        let owner_epoch = self.epoch.load(Ordering::Relaxed);
        let required_epoch = self.required_owner_epoch.load(Ordering::Relaxed);
        if owner_epoch < required_epoch {
            return Err(OwnerLeaseFault::StaleEpoch {
                owner_epoch,
                required_epoch,
            });
        }
        let deadline_ms = self.lease_deadline_ms.load(Ordering::Relaxed);
        if deadline_ms != 0 {
            let now_ms = self.now_ms();
            // `>=`: a commit at exactly the deadline is rejected. The control
            // plane treats the lease as expired at `deadline_ms`, so the owner
            // must stop one tick earlier to stay strictly inside the window.
            if now_ms >= deadline_ms {
                return Err(OwnerLeaseFault::LeaseExpired {
                    now_ms,
                    deadline_ms,
                });
            }
        }
        Ok(())
    }

    pub(super) fn ensure_owner_epoch_current(&self) -> Result<(), MetadError> {
        self.check_owner_lease()
            .map_err(OwnerLeaseFault::into_error)
    }

    pub(super) fn commit_metadata(
        &self,
        command: MetadataCommand,
    ) -> Result<CommitResult, MetadError> {
        self.commit_metadata_from_factory(|| Ok(command))
    }

    /// Build and commit one command while holding the same epoch fence used by
    /// the apply. Allocator reservations use this entry point because their
    /// persisted owner epoch must be read inside that fence, and because every
    /// reservation must join the synchronous recovery log when it is enabled.
    pub(super) fn commit_metadata_from_factory<F>(
        &self,
        build: F,
    ) -> Result<CommitResult, MetadError>
    where
        F: FnOnce() -> Result<MetadataCommand, MetadError>,
    {
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
            self.resolve_unresolved_metadata_commit_group_locked()
                .map_err(blocked_before_apply)?;
            self.flush_pending_metadata_log_segment_locked()
                .map_err(|err| MetadError::SyncLogArchiveFailed {
                    committed: false,
                    message: err.to_string(),
                })?;
        }

        let (command, result) = {
            // The command factory, owner check, and apply share one epoch read
            // fence. In particular, a reservation cannot encode an old owner
            // epoch and then apply after a concurrent failover bump. Release the
            // fence before object-store archive I/O so lease renewal can still
            // install the current epoch while an upload is slow.
            let _epoch_fence = self
                .epoch_fence
                .read()
                .unwrap_or_else(|err| err.into_inner());
            self.ensure_owner_epoch_current()?;
            let command = build()?;
            if log_enabled {
                self.preflight_sync_metadata_log_locked(std::slice::from_ref(&command))
                    .map_err(|err| MetadError::SyncLogArchiveFailed {
                        committed: false,
                        message: err.to_string(),
                    })?;
            }
            let _restore_graph_write = self.restore_graph_write_for_command(&command);
            match self.metadata.commit_metadata(command.clone()) {
                Ok(result) => {
                    self.purge_path_caches_after_write();
                    (command, result)
                }
                Err(backend @ MetadataError::Backend(_)) if log_enabled => {
                    // A backend error may be an acknowledgement lost after the
                    // atomic apply became visible. Invalidate conservatively
                    // before readback so blocked writers cannot leave readers
                    // observing a stale pre-apply path cache.
                    self.purge_path_caches_after_write();
                    let group = log_sync::UnresolvedMetadataCommitGroup::new(
                        vec![command.clone()],
                        vec![Err(backend)],
                    )?;
                    match self.reconcile_metadata_commit_group_locked(&group) {
                        Ok(mut resolved) => {
                            let result = resolved.pop().expect("single commit result")?;
                            (command, result)
                        }
                        Err(err) => {
                            self.defer_unresolved_metadata_commit_group_locked(group)?;
                            return Err(err);
                        }
                    }
                }
                Err(err) => return Err(err.into()),
            }
        };
        // The command is already durably applied; if the sync-log segment fails
        // to archive we report committed=true so the caller reconciles rather
        // than blindly retrying data that actually landed.
        self.record_committed_metadata_command(&command, &result)
            .map_err(|err| MetadError::SyncLogArchiveFailed {
                committed: true,
                message: err.to_string(),
            })?;
        Ok(result)
    }

    pub(super) fn commit_metadata_without_sync_log(
        &self,
        command: MetadataCommand,
    ) -> Result<CommitResult, MetadError> {
        // Read guard held across check + apply: an epoch bump (write guard)
        // cannot land between them, so a commit that passes the fence always
        // applies under a still-current epoch.
        let _fence = self
            .epoch_fence
            .read()
            .unwrap_or_else(|err| err.into_inner());
        self.ensure_owner_epoch_current()?;
        let _restore_graph_write = self.restore_graph_write_for_command(&command);
        let result = self.metadata.commit_metadata(command)?;
        self.purge_path_caches_after_write();
        Ok(result)
    }

    pub(super) fn commit_independent_metadata_batch(
        &self,
        commands: &[MetadataCommand],
    ) -> Vec<Result<CommitResult, MetadError>> {
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
            if let Err(err) = self.resolve_unresolved_metadata_commit_group_locked() {
                return blocked_batch_before_apply(commands.len(), err);
            }
            if let Err(err) = self
                .flush_pending_metadata_log_segment_locked()
                .and_then(|()| self.preflight_sync_metadata_log_locked(commands))
            {
                let message = err.to_string();
                return commands
                    .iter()
                    .map(|_| {
                        Err(MetadError::SyncLogArchiveFailed {
                            committed: false,
                            message: message.clone(),
                        })
                    })
                    .collect();
            }
        }
        let raw_results = {
            // Read guard held across the fence check and the whole batch apply,
            // so a failover epoch bump cannot interleave with an accepted batch.
            let _fence = self
                .epoch_fence
                .read()
                .unwrap_or_else(|err| err.into_inner());
            if let Err(fault) = self.check_owner_lease() {
                return commands.iter().map(|_| Err(fault.into_error())).collect();
            }
            let _restore_graph_write = self.restore_graph_write_for_batch(commands);
            self.metadata.commit_independent_batch(commands)
        };

        let may_have_committed = raw_results
            .iter()
            .any(|result| matches!(result, Ok(_) | Err(MetadataError::Backend(_))));
        if may_have_committed {
            // Backend outcomes are ambiguous and successful subgroup outcomes
            // are definitely visible. Purge once for the whole batch before
            // any possible unresolved state blocks subsequent writes.
            self.purge_path_caches_after_write();
        }

        let metadata_results = if log_enabled
            && raw_results
                .iter()
                .any(|result| matches!(result, Err(MetadataError::Backend(_))))
        {
            let group = match log_sync::UnresolvedMetadataCommitGroup::new(
                commands.to_vec(),
                raw_results,
            ) {
                Ok(group) => group,
                Err(err) => {
                    let message = err.to_string();
                    return commands
                        .iter()
                        .map(|_| Err(MetadError::Codec(message.clone())))
                        .collect();
                }
            };
            match self.reconcile_metadata_commit_group_locked(&group) {
                Ok(resolved) => resolved,
                Err(err) => {
                    let message = err.to_string();
                    if let Err(store_err) =
                        self.defer_unresolved_metadata_commit_group_locked(group)
                    {
                        let message = store_err.to_string();
                        return commands
                            .iter()
                            .map(|_| Err(MetadError::Codec(message.clone())))
                            .collect();
                    }
                    // Do not acknowledge even later successful Holt subgroups:
                    // the exact whole batch remains frozen until every earlier
                    // ambiguous outcome can be resolved and archived in order.
                    return commands
                        .iter()
                        .map(|_| Err(MetadError::Codec(message.clone())))
                        .collect();
                }
            }
        } else {
            raw_results
        };

        let mut successful = Vec::new();
        let mut results = metadata_results
            .into_iter()
            .zip(commands)
            .enumerate()
            .map(|(index, (result, command))| {
                result
                    .inspect(|result| {
                        successful.push((index, command, result.clone()));
                    })
                    .map_err(MetadError::from)
            })
            .collect::<Vec<_>>();
        let log_commands = successful
            .iter()
            .map(|(_, command, result)| (*command, result))
            .collect::<Vec<_>>();
        if let Err(err) = self.record_committed_metadata_commands(&log_commands) {
            // These commands are durably applied; the grouped segment archive
            // failed. Report committed=true (not a generic Codec error) so the
            // caller reconciles instead of re-creating data that already landed.
            let message = err.to_string();
            for (index, _, _) in successful {
                results[index] = Err(MetadError::SyncLogArchiveFailed {
                    committed: true,
                    message: message.clone(),
                });
            }
        }
        results
    }
}

fn blocked_before_apply(err: MetadError) -> MetadError {
    match err {
        MetadError::SyncLogArchiveFailed { message, .. } => MetadError::SyncLogArchiveFailed {
            committed: false,
            message,
        },
        err => err,
    }
}

fn blocked_batch_before_apply(
    command_count: usize,
    err: MetadError,
) -> Vec<Result<CommitResult, MetadError>> {
    match err {
        MetadError::SyncLogArchiveFailed { message, .. } => (0..command_count)
            .map(|_| {
                Err(MetadError::SyncLogArchiveFailed {
                    committed: false,
                    message: message.clone(),
                })
            })
            .collect(),
        err => {
            let message = err.to_string();
            (0..command_count)
                .map(|_| Err(MetadError::Codec(message.clone())))
                .collect()
        }
    }
}

fn validate_owner_epoch(epoch: u64) -> Result<(), MetadError> {
    if epoch == 0 {
        return Err(MetadError::InvalidOwnerEpoch);
    }
    Ok(())
}
