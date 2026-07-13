use super::*;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectGcClaimProgress {
    Open,
    Pending,
    BlockedByFailoverDurability,
    UncertainDelete {
        owner_epoch: u64,
        operation_token: u64,
    },
}

fn encode_object_gc_quarantine_record(row: &crate::command::ScanItem, reason: &str) -> Vec<u8> {
    let mut value = Vec::new();
    let reason = reason.as_bytes();
    value.extend_from_slice(&(row.key.len() as u32).to_be_bytes());
    value.extend_from_slice(&row.key);
    value.extend_from_slice(&(row.value.0.len() as u32).to_be_bytes());
    value.extend_from_slice(&row.value.0);
    let reason_len = reason.len().min(4096);
    value.extend_from_slice(&(reason_len as u32).to_be_bytes());
    value.extend_from_slice(&reason[..reason_len]);
    value
}

fn decode_validated_object_gc_row(
    mount: MountId,
    row: &crate::command::ScanItem,
) -> Result<ObjectGcRecord, MetadError> {
    let key = decode_object_gc_record_key(mount, &row.key)?;
    let record =
        decode_object_gc_record(&row.value.0).map_err(|err| MetadError::Codec(err.to_string()))?;
    if record.enqueue_version != key.enqueue_version {
        return Err(MetadError::Codec(
            "object GC row enqueue version does not match its key".to_owned(),
        ));
    }
    if record.inode != key.inode {
        return Err(MetadError::Codec(
            "object GC row inode does not match its key".to_owned(),
        ));
    }
    if record.generation != key.generation {
        return Err(MetadError::Codec(
            "object GC row generation does not match its key".to_owned(),
        ));
    }
    let (object_mount, object_inode, object_generation, object_chunk_index, object_block_index) =
        decode_canonical_block_object_owner(&record.object_key)?;
    if object_mount != mount.get()
        || object_inode != key.inode.get()
        || object_generation != key.generation
        || object_chunk_index != key.chunk_index
        || object_block_index != key.block_index
    {
        return Err(MetadError::Codec(
            "object GC row object identity does not match its key".to_owned(),
        ));
    }
    Ok(record)
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub(super) fn ensure_object_gc_claim_record(&self) -> Result<(), MetadError> {
        let key = object_gc_claim_key(self.mount);
        if let Some(value) = self.metadata.get(
            RecordFamily::System,
            &key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )? {
            decode_object_gc_claim(self.mount, &value.0)?;
            return Ok(());
        }
        let version = self.next_version()?;
        let result = self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"initialize-object-gc-claim",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: key.clone(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_object_gc_claim(&ObjectGcClaim::Open)?)),
            }],
            watch: Vec::new(),
        });
        match result {
            Ok(_)
            | Err(MetadError::Metadata(MetadataError::PredicateFailed))
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
                            "object GC claim initialization was not durable".to_owned(),
                        )
                    })?;
                decode_object_gc_claim(self.mount, &value.0)?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Capture the durable Open epoch before object upload or historical
    /// planning. The exact record version must join the metadata commit that
    /// makes the reference durable.
    pub(super) fn begin_object_reference_mutation(
        &self,
    ) -> Result<ObjectReferenceMutation, MetadError> {
        self.ensure_object_gc_claim_record()?;
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &object_gc_claim_key(self.mount),
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("durable object GC claim is not initialized".to_owned())
            })?;
        let claim = decode_object_gc_claim(self.mount, &item.value.0)?;
        let current_epoch = self.epoch.load(Ordering::Relaxed);
        let claim_owner = match &claim {
            ObjectGcClaim::Open => None,
            ObjectGcClaim::Deleting { owner_epoch, .. } => Some(*owner_epoch),
        };
        if claim_owner.is_some_and(|owner| owner > current_epoch) {
            return Err(MetadError::StaleOwnerEpoch {
                owner_epoch: current_epoch,
                required_epoch: claim_owner.expect("future claim owner exists"),
            });
        }
        match claim {
            ObjectGcClaim::Open => Ok(ObjectReferenceMutation::from_version(item.version)),
            ObjectGcClaim::Deleting { .. } => {
                Err(MetadError::Metadata(MetadataError::PredicateFailed))
            }
        }
    }

    fn failover_durability_required(&self) -> Result<bool, MetadError> {
        let Some(value) = self.metadata.get(
            RecordFamily::System,
            &failover_durability_required_key(self.mount),
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(false);
        };
        decode_failover_durability_required_marker(&value.0)?;
        Ok(true)
    }

    fn update_object_gc_scan_cursor(
        &self,
        cursor: Option<&crate::command::ReadItem>,
        next: Option<Vec<u8>>,
    ) -> Result<bool, MetadError> {
        if cursor.is_none() && next.is_none() {
            return Ok(true);
        }
        let key = object_gc_scan_cursor_key(self.mount);
        let version = self.next_version()?;
        let cursor_predicate = cursor.map_or(Predicate::NotExists, |item| {
            Predicate::VersionEquals(item.version)
        });
        let mutation = match next {
            Some(next) => Mutation {
                family: RecordFamily::System,
                key: key.clone(),
                op: MutationOp::Put,
                value: Some(Value(next)),
            },
            None => delete_mutation(RecordFamily::System, key.clone()),
        };
        let command = MetadataCommand {
            request_id: request_id(
                b"advance-object-gc-scan-cursor",
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
                    key,
                    predicate: cursor_predicate,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: failover_durability_required_key(self.mount),
                    predicate: Predicate::PrefixEmpty,
                },
            ],
            mutations: vec![mutation],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(_) => Ok(true),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn recover_object_gc_claim_locked(
        &self,
        outcome: &mut PendingObjectCleanupOutcome,
    ) -> Result<ObjectGcClaimProgress, MetadError> {
        self.ensure_object_gc_claim_record()?;
        let key = object_gc_claim_key(self.mount);
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("durable object GC claim is not initialized".to_owned())
            })?;
        match decode_object_gc_claim(self.mount, &item.value.0)? {
            ObjectGcClaim::Open => Ok(ObjectGcClaimProgress::Open),
            ObjectGcClaim::Deleting {
                owner_epoch,
                operation_token,
                gc_record_key,
                gc_record_version,
            } => {
                let current_epoch = self.epoch.load(Ordering::Relaxed);
                if owner_epoch > current_epoch {
                    return Err(MetadError::StaleOwnerEpoch {
                        owner_epoch: current_epoch,
                        required_epoch: owner_epoch,
                    });
                }
                if self.failover_durability_required()? {
                    // The durable claim cannot tell whether the external DELETE
                    // completed before the old owner crashed, or is still in
                    // flight. Keep the namespace closed: reopening would admit
                    // references that a late DELETE could invalidate. Startup
                    // treats this outcome as requiring controlled operator
                    // recovery; the worker preserves both claim and queue row.
                    outcome.blocked_by_failover_durability += 1;
                    return Ok(ObjectGcClaimProgress::UncertainDelete {
                        owner_epoch,
                        operation_token,
                    });
                }
                let Some(row) = self.metadata.get_versioned(
                    RecordFamily::Gc,
                    &gc_record_key,
                    self.read_version()?,
                    ReadPurpose::WritePlanLocal,
                )?
                else {
                    self.release_object_gc_claim(item.version, None)?;
                    return Ok(ObjectGcClaimProgress::Open);
                };
                if row.version.get() != gc_record_version {
                    self.release_object_gc_claim(item.version, None)?;
                    return Ok(ObjectGcClaimProgress::Open);
                }
                let row = crate::command::ScanItem {
                    key: gc_record_key,
                    value: row.value,
                    version: row.version,
                };
                let record = match decode_validated_object_gc_row(self.mount, &row) {
                    Ok(record) => record,
                    Err(err) => {
                        self.quarantine_claimed_gc_row(
                            item.version,
                            &row,
                            &err.to_string(),
                            outcome,
                        )?;
                        return Ok(ObjectGcClaimProgress::Open);
                    }
                };
                self.finish_claimed_gc_row(item.version, &row, &record, outcome)
            }
        }
    }

    fn acquire_object_gc_claim(
        &self,
        row: &crate::command::ScanItem,
    ) -> Result<Option<Version>, MetadError> {
        if self.failover_durability_required()? {
            return Ok(None);
        }
        let open = self.begin_object_reference_mutation()?;
        match self.transition_object_gc_claim(
            open.version(),
            &ObjectGcClaim::Deleting {
                owner_epoch: self.epoch.load(Ordering::Relaxed),
                operation_token: open.version().get(),
                gc_record_key: row.key.clone(),
                gc_record_version: row.version.get(),
            },
            Some((&row.key, row.version)),
            b"claim-object-delete",
        ) {
            Ok(version) => Ok(Some(version)),
            Err(MetadError::Metadata(MetadataError::PredicateFailed))
                if self.failover_durability_required()? =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn transition_object_gc_claim(
        &self,
        expected_claim_version: Version,
        next: &ObjectGcClaim,
        gc_record: Option<(&[u8], Version)>,
        request_domain: &[u8],
    ) -> Result<Version, MetadError> {
        let key = object_gc_claim_key(self.mount);
        let version = self.next_version()?;
        let encoded = encode_object_gc_claim(next)?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: key.clone(),
            predicate: Predicate::VersionEquals(expected_claim_version),
        }];
        predicates.push(PredicateRef {
            family: RecordFamily::System,
            key: failover_durability_required_key(self.mount),
            predicate: Predicate::PrefixEmpty,
        });
        if let Some((gc_key, gc_version)) = gc_record {
            predicates.push(PredicateRef {
                family: RecordFamily::Gc,
                key: gc_key.to_vec(),
                predicate: Predicate::VersionEquals(gc_version),
            });
        }
        let command = MetadataCommand {
            request_id: request_id(request_domain, self.mount, InodeId::root(), version),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates,
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encoded.clone())),
            }],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            result @ (Ok(_)
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            })) => {
                let item = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &key,
                        self.read_version()?,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec("object GC claim transition was lost".to_owned())
                    })?;
                if item.version != version || item.value.0 != encoded {
                    return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                }
                match result {
                    Ok(_) => Ok(item.version),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    fn release_object_gc_claim(
        &self,
        expected_claim_version: Version,
        delete_gc_record: Option<(&[u8], Version)>,
    ) -> Result<(), MetadError> {
        let key = object_gc_claim_key(self.mount);
        let version = self.next_version()?;
        let encoded = encode_object_gc_claim(&ObjectGcClaim::Open)?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: key.clone(),
            predicate: Predicate::VersionEquals(expected_claim_version),
        }];
        let mut mutations = vec![Mutation {
            family: RecordFamily::System,
            key: key.clone(),
            op: MutationOp::Put,
            value: Some(Value(encoded.clone())),
        }];
        if let Some((gc_key, gc_version)) = delete_gc_record {
            predicates.push(PredicateRef {
                family: RecordFamily::Gc,
                key: gc_key.to_vec(),
                predicate: Predicate::VersionEquals(gc_version),
            });
            mutations.push(delete_mutation(RecordFamily::Gc, gc_key.to_vec()));
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"release-object-gc-claim",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            result @ (Ok(_)
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            })) => {
                let item = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &key,
                        self.read_version()?,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| MetadError::Codec("object GC claim was lost".to_owned()))?;
                if item.version != version
                    || decode_object_gc_claim(self.mount, &item.value.0)? != ObjectGcClaim::Open
                {
                    return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                }
                match result {
                    Ok(_) => Ok(()),
                    Err(err) => Err(err),
                }
            }
            Err(err) => Err(err),
        }
    }

    fn delete_gc_row_under_durable_claim(
        &self,
        row: &crate::command::ScanItem,
        record: &ObjectGcRecord,
        outcome: &mut PendingObjectCleanupOutcome,
    ) -> Result<ObjectGcClaimProgress, MetadError> {
        let claim_version = match self.acquire_object_gc_claim(row) {
            Ok(Some(version)) => version,
            Ok(None) => {
                outcome.blocked_by_failover_durability += 1;
                return Ok(ObjectGcClaimProgress::BlockedByFailoverDurability);
            }
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                return Ok(ObjectGcClaimProgress::Pending);
            }
            Err(err) => return Err(err),
        };
        self.finish_claimed_gc_row(claim_version, row, record, outcome)
    }

    fn finish_claimed_gc_row(
        &self,
        claim_version: Version,
        row: &crate::command::ScanItem,
        record: &ObjectGcRecord,
        outcome: &mut PendingObjectCleanupOutcome,
    ) -> Result<ObjectGcClaimProgress, MetadError> {
        let protected = match self.object_delete_is_protected(record) {
            Ok(protected) => protected,
            Err(err) => {
                self.quarantine_claimed_gc_row(claim_version, row, &err.to_string(), outcome)?;
                return Ok(ObjectGcClaimProgress::Open);
            }
        };
        if protected {
            outcome.blocked_by_snapshots += 1;
            self.release_object_gc_claim(claim_version, None)?;
            return Ok(ObjectGcClaimProgress::Open);
        }
        self.ensure_owner_epoch_current()?;
        let object_key = match ObjectKey::new(record.object_key.clone()) {
            Ok(key) => key,
            Err(err) => {
                self.quarantine_claimed_gc_row(claim_version, row, &err.to_string(), outcome)?;
                return Ok(ObjectGcClaimProgress::Open);
            }
        };
        outcome.attempted += 1;
        match self.objects.delete(&object_key) {
            Ok(true) => outcome.deleted += 1,
            Ok(false) => outcome.missing += 1,
            // An error does not prove the remote DELETE failed: the object may
            // already be gone, its acknowledgement may have been lost, or the
            // request may still be in flight. Keep the durable Deleting claim
            // closed so no writer can restage this generation until recovery
            // retries the same idempotent delete under the same claim.
            Err(err) => return Err(err.into()),
        }
        self.release_object_gc_claim(claim_version, Some((&row.key, row.version)))?;
        outcome.records_removed += 1;
        Ok(ObjectGcClaimProgress::Open)
    }

    fn quarantine_claimed_gc_row(
        &self,
        claim_version: Version,
        row: &crate::command::ScanItem,
        reason: &str,
        outcome: &mut PendingObjectCleanupOutcome,
    ) -> Result<(), MetadError> {
        let claim_key = object_gc_claim_key(self.mount);
        let digest: [u8; 32] = Sha256::digest(&row.key).into();
        let quarantine_key = object_gc_quarantine_key(self.mount, &digest);
        let version = self.next_version()?;
        let value = encode_object_gc_quarantine_record(row, reason);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"quarantine-object-gc-row",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: claim_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: claim_key.clone(),
                    predicate: Predicate::VersionEquals(claim_version),
                },
                PredicateRef {
                    family: RecordFamily::Gc,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: quarantine_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![
                Mutation {
                    family: RecordFamily::System,
                    key: claim_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_claim(&ObjectGcClaim::Open)?)),
                },
                delete_mutation(RecordFamily::Gc, row.key.clone()),
                Mutation {
                    family: RecordFamily::System,
                    key: quarantine_key,
                    op: MutationOp::Put,
                    value: Some(Value(value)),
                },
            ],
            watch: Vec::new(),
        })?;
        outcome.records_removed += 1;
        Ok(())
    }

    /// Quarantine a row whose key/value cannot name a valid object deletion.
    /// No external DELETE is possible, so an exact-version metadata move is
    /// sufficient and deliberately avoids persisting an invalid Deleting claim.
    fn quarantine_unclaimed_gc_row(
        &self,
        row: &crate::command::ScanItem,
        reason: &str,
        outcome: &mut PendingObjectCleanupOutcome,
    ) -> Result<(), MetadError> {
        let digest: [u8; 32] = Sha256::digest(&row.key).into();
        let quarantine_key = object_gc_quarantine_key(self.mount, &digest);
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"quarantine-invalid-object-gc-row",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Gc,
            primary_key: row.key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::Gc,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: quarantine_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![
                delete_mutation(RecordFamily::Gc, row.key.clone()),
                Mutation {
                    family: RecordFamily::System,
                    key: quarantine_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_quarantine_record(row, reason))),
                },
            ],
            watch: Vec::new(),
        })?;
        outcome.records_removed += 1;
        Ok(())
    }

    fn object_delete_is_protected(&self, record: &ObjectGcRecord) -> Result<bool, MetadError> {
        if self
            .history_retention_floor()?
            .is_some_and(|floor| floor.get() < record.enqueue_version)
        {
            return Ok(true);
        }
        let Some(body) = self.body_descriptor(record.inode)? else {
            return Ok(false);
        };
        let manifests = self.chunk_manifests_for_body_at_version(
            record.inode,
            &body,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?;
        Ok(manifests
            .iter()
            .flat_map(|manifest| &manifest.slices)
            .flat_map(|slice| &slice.blocks)
            .any(|block| block.object_key == record.object_key))
    }

    pub fn cleanup_staged_objects(
        &self,
        staged: &StagedObjectSet,
    ) -> Result<ObjectCleanupOutcome, MetadError> {
        self.objects.delete_staged(staged).map_err(Into::into)
    }

    /// Resume only a crash-left durable object-GC claim. This does not scan or
    /// start new GC work, so a server can call it after installing durability
    /// policy and before admitting writers or starting the background worker.
    pub fn recover_object_gc_claim(&self) -> Result<PendingObjectCleanupOutcome, MetadError> {
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let mut outcome = PendingObjectCleanupOutcome::default();
        match self.recover_object_gc_claim_locked(&mut outcome)? {
            ObjectGcClaimProgress::UncertainDelete {
                owner_epoch,
                operation_token,
            } => Err(MetadError::ObjectGcRecoveryRequiresIntervention {
                owner_epoch,
                operation_token,
            }),
            _ => Ok(outcome),
        }
    }

    /// Invalidate every prepared object reference minted by a previous owner.
    ///
    /// Failover calls this only after crash-left claim recovery has proved the
    /// durable claim is Open. Rewriting Open at a fresh metadata version makes
    /// the claim version an incarnation fence: a prepared upload from the old
    /// owner can no longer publish a manifest after takeover.
    pub fn rotate_object_gc_claim_for_failover(&self) -> Result<(), MetadError> {
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        self.ensure_object_gc_claim_record()?;
        let key = object_gc_claim_key(self.mount);
        let old = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("durable object GC claim is not initialized".to_owned())
            })?;
        if !matches!(
            decode_object_gc_claim(self.mount, &old.value.0)?,
            ObjectGcClaim::Open
        ) {
            // Recovery must never reopen or overwrite a deletion claim whose
            // external DELETE outcome is not known.
            return Err(MetadError::Metadata(MetadataError::PredicateFailed));
        }

        let version = self.next_version()?;
        if version <= old.version {
            return Err(MetadError::Codec(
                "object GC claim rotation did not advance its record version".to_owned(),
            ));
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"rotate-failover-object-gc-claim",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: key.clone(),
                predicate: Predicate::VersionEquals(old.version),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: key.clone(),
                op: MutationOp::Put,
                value: Some(Value(encode_object_gc_claim(&ObjectGcClaim::Open)?)),
            }],
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(_) => Ok(()),
            Err(err)
                if matches!(
                    &err,
                    MetadError::SyncLogArchiveFailed {
                        committed: true,
                        ..
                    } | MetadError::Metadata(MetadataError::Backend(_))
                ) =>
            {
                // A backend may apply durably and lose only its acknowledgement.
                // Accept that uncertain result solely when an exact read-back
                // proves this command's new Open version is present.
                let current = self.metadata.get_versioned(
                    RecordFamily::System,
                    &key,
                    self.read_version()?,
                    ReadPurpose::WritePlanLocal,
                )?;
                if current.as_ref().is_some_and(|item| {
                    item.version == version
                        && matches!(
                            decode_object_gc_claim(self.mount, &item.value.0),
                            Ok(ObjectGcClaim::Open)
                        )
                }) {
                    Ok(())
                } else {
                    Err(err)
                }
            }
            Err(err) => Err(err),
        }
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
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let mut outcome = PendingObjectCleanupOutcome::default();
        if !matches!(
            self.recover_object_gc_claim_locked(&mut outcome)?,
            ObjectGcClaimProgress::Open
        ) {
            return Ok(outcome);
        }
        let page_size = limit.max(1);
        let cursor_key = object_gc_scan_cursor_key(self.mount);
        let read_version = self.read_version()?;
        let cursor = self.metadata.get_versioned(
            RecordFamily::System,
            &cursor_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        let queue_prefix = gc_queue_prefix(self.mount);
        if cursor.as_ref().is_some_and(|item| {
            !item.value.0.starts_with(&queue_prefix) || item.value.0.len() <= queue_prefix.len()
        }) {
            return Err(MetadError::Codec(
                "durable object GC scan cursor is outside the GC queue".to_owned(),
            ));
        }
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: queue_prefix,
            start_after: cursor.as_ref().map(|item| item.value.0.clone()),
            version: read_version,
            limit: page_size,
            purpose: ReadPurpose::UserStrong,
        })?;
        outcome.scanned += rows.len();
        if self.failover_durability_required()? {
            outcome.blocked_by_failover_durability =
                outcome.blocked_by_failover_durability.max(rows.len());
            return Ok(outcome);
        }
        // Reap expired snapshot pins only after the failover marker check, so
        // HA fail-closed mode performs no unrelated cleanup mutations.
        outcome.snapshot_reap = self.reclaim_expired_snapshot_pins(page_size)?;
        if rows.is_empty() {
            self.update_object_gc_scan_cursor(cursor.as_ref(), None)?;
            return Ok(outcome);
        }

        let now_ms = self.now_ms();
        let grace_ms = duration_millis_u64(read_lease_grace);
        let reached_tail = rows.len() < page_size;
        let last_scanned_key = rows.last().expect("non-empty GC page").key.clone();
        let mut page_completed = true;
        for row in rows {
            let record = match decode_validated_object_gc_row(self.mount, &row) {
                Ok(record) => record,
                Err(err) => {
                    self.quarantine_unclaimed_gc_row(&row, &err.to_string(), &mut outcome)?;
                    continue;
                }
            };
            if now_ms < record.enqueue_unix_ms.saturating_add(grace_ms) {
                outcome.blocked_by_read_leases += 1;
                continue;
            }
            // Avoid rotating the global reference epoch for a candidate that is
            // already known to be protected. This first check is advisory: a
            // snapshot/reference can race it, so every actual delete still
            // acquires the durable claim and repeats the protection check.
            if matches!(self.object_delete_is_protected(&record), Ok(true)) {
                outcome.blocked_by_snapshots += 1;
                continue;
            }
            match self.delete_gc_row_under_durable_claim(&row, &record, &mut outcome)? {
                ObjectGcClaimProgress::Open => {}
                ObjectGcClaimProgress::Pending
                | ObjectGcClaimProgress::BlockedByFailoverDurability
                | ObjectGcClaimProgress::UncertainDelete { .. } => {
                    page_completed = false;
                    break;
                }
            }
        }
        if page_completed {
            let next = (!reached_tail).then_some(last_scanned_key);
            self.update_object_gc_scan_cursor(cursor.as_ref(), next)?;
        }
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

    /// Whether a canonical block key was minted by `inode`, irrespective of
    /// generation. Fork retirement uses this across the effective generation
    /// chain: blocks owned by the fork inode are self-contained, while a block
    /// owned by any other inode is still borrowed and keeps the durable fork
    /// retention binding live.
    pub(super) fn block_object_is_owned_by_inode(
        &self,
        inode: InodeId,
        object_key: &str,
    ) -> Result<bool, MetadError> {
        let (mount, owner, _, _, _) = decode_canonical_block_object_owner(object_key)?;
        Ok(mount == self.mount.get() && owner == inode.get())
    }

    pub(super) fn canonical_block_object_identity(
        &self,
        object_key: &str,
    ) -> Result<(InodeId, u64, u64, u64), MetadError> {
        let (mount, inode, generation, chunk_index, block_index) =
            decode_canonical_block_object_owner(object_key)?;
        if mount != self.mount.get() {
            return Err(MetadError::Codec(
                "block object belongs to another mount".to_owned(),
            ));
        }
        let inode = InodeId::new(inode)
            .map_err(|err| MetadError::Codec(format!("invalid block owner inode: {err}")))?;
        Ok((inode, generation, chunk_index, block_index))
    }

    pub(super) fn history_retention_floor(&self) -> Result<Option<Version>, MetadError> {
        let read_version = self.read_version()?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(self.mount),
            start_after: None,
            version: read_version,
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
        // A clone's snapshot lease is only the construction-time fence. Once
        // the eager fork is exposed, its durable ForkBinding is the retention
        // root for borrowed source blocks and deliberately has no wall-clock
        // expiry. Only explicit snapshot retirement may delete the binding.
        for versioned in self.versioned_fork_bindings_at(read_version, ReadPurpose::UserStrong)? {
            let version = Version::new(versioned.binding.pinned_read_version)?;
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
        let mut queued_object_keys = HashSet::new();
        for row in rows {
            if chunk_index_from_manifest_key(&row.key)? != BODY_SUMMARY_CHUNK_INDEX {
                let manifest = decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                for block in manifest.slices.iter().flat_map(|slice| slice.blocks.iter()) {
                    if retained_object_keys.contains(&block.object_key) {
                        continue;
                    }
                    if !self.owns_block_object_key(inode, generation, &block.object_key) {
                        // Borrowed (clone-shared) block: its key is owned by the
                        // inode/generation that minted it, not this one. A borrower
                        // must never enqueue another namespace's blocks for GC.
                        continue;
                    }
                    let (owner, object_generation, chunk_index, block_index) =
                        self.canonical_block_object_identity(&block.object_key)?;
                    if owner != inode
                        || object_generation != generation
                        || chunk_index != manifest.chunk_index
                    {
                        return Err(MetadError::Codec(
                            "owned block object identity does not match its manifest".to_owned(),
                        ));
                    }
                    if !queued_object_keys.insert(block.object_key.clone()) {
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
                            chunk_index,
                            block_index,
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
            return self.chunk_manifest_delete_and_gc_mutations_from_manifests(
                inode,
                old_top_generation,
                old_chunks,
                enqueue_version,
                retained_object_keys,
            );
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
    ) -> Result<Vec<Mutation>, MetadError> {
        let enqueue_unix_ms = current_time_ms();
        let mut mutations = vec![delete_mutation(
            RecordFamily::ChunkManifest,
            chunk_manifest_key(self.mount, inode, generation, BODY_SUMMARY_CHUNK_INDEX),
        )];
        let mut queued_object_keys = HashSet::new();
        for manifest in manifests {
            for block in manifest.slices.iter().flat_map(|slice| slice.blocks.iter()) {
                if retained_object_keys.contains(&block.object_key) {
                    continue;
                }
                if !self.owns_block_object_key(inode, generation, &block.object_key) {
                    // Borrowed (clone-shared) block: owned by the inode/generation
                    // that minted it, so this borrower must not enqueue it for GC.
                    continue;
                }
                let (owner, object_generation, chunk_index, block_index) =
                    self.canonical_block_object_identity(&block.object_key)?;
                if owner != inode
                    || object_generation != generation
                    || chunk_index != manifest.chunk_index
                {
                    return Err(MetadError::Codec(
                        "owned block object identity does not match its manifest".to_owned(),
                    ));
                }
                if !queued_object_keys.insert(block.object_key.clone()) {
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
                        chunk_index,
                        block_index,
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
        Ok(mutations)
    }
}

fn duration_millis_u64(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
