use super::clone::validate_restore_command_bounds;
use super::*;
use std::collections::HashMap;
use std::time::Duration;

struct ObjectRetention {
    version_floor: Option<Version>,
}

struct BaseRefReleaseGuard {
    family: RecordFamily,
    key: Vec<u8>,
    version: Version,
    binding: Option<(Vec<u8>, Version)>,
    operation: Option<(Vec<u8>, Version)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObjectGcClaimProgress {
    Open,
    Pending,
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
            decode_object_gc_claim(&value.0)?;
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
                decode_object_gc_claim(&value.0)?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    /// Capture the durable Open epoch before any upload or historical planning.
    /// Callers must carry this exact version into the metadata commit that makes
    /// an object reference durable; reading a newer Open epoch at commit time
    /// would permit an intervening GC delete cycle to go unnoticed.
    pub(super) fn begin_object_reference_mutation(
        &self,
    ) -> Result<ObjectReferenceMutation, MetadError> {
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
        let claim = decode_object_gc_claim(&item.value.0)?;
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
            ObjectGcClaim::Open => Ok(ObjectReferenceMutation {
                claim_version: item.version,
            }),
            ObjectGcClaim::Deleting { .. } => {
                Err(MetadError::Metadata(MetadataError::PredicateFailed))
            }
        }
    }

    fn recover_object_gc_claim_locked(
        &self,
        outcome: &mut PendingObjectCleanupOutcome,
    ) -> Result<ObjectGcClaimProgress, MetadError> {
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
        match decode_object_gc_claim(&item.value.0)? {
            ObjectGcClaim::Open => Ok(ObjectGcClaimProgress::Open),
            ObjectGcClaim::Deleting {
                owner_epoch,
                gc_record_key,
                gc_record_version,
                ..
            } => {
                let current_epoch = self.epoch.load(Ordering::Relaxed);
                if owner_epoch > current_epoch {
                    return Err(MetadError::StaleOwnerEpoch {
                        owner_epoch: current_epoch,
                        required_epoch: owner_epoch,
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
                let record = match decode_object_gc_record(&row.value.0) {
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
    ) -> Result<Version, MetadError> {
        let open = self.begin_object_reference_mutation()?;
        self.transition_object_gc_claim(
            open.claim_version,
            &ObjectGcClaim::Deleting {
                owner_epoch: self.epoch.load(Ordering::Relaxed),
                operation_token: open.claim_version.get(),
                gc_record_key: row.key.clone(),
                gc_record_version: row.version.get(),
            },
            Some((&row.key, row.version)),
            b"claim-object-delete",
        )
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
                    || decode_object_gc_claim(&item.value.0)? != ObjectGcClaim::Open
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
            Ok(version) => version,
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
        let deletion = self.objects.delete(&object_key);
        match deletion {
            Ok(true) => outcome.deleted += 1,
            Ok(false) => outcome.missing += 1,
            Err(err) => {
                let _ = self.release_object_gc_claim(claim_version, None);
                return Err(err.into());
            }
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
        let mut value = Vec::new();
        let reason = reason.as_bytes();
        value.extend_from_slice(&(row.key.len() as u32).to_be_bytes());
        value.extend_from_slice(&row.key);
        value.extend_from_slice(&(row.value.0.len() as u32).to_be_bytes());
        value.extend_from_slice(&row.value.0);
        let reason_len = reason.len().min(4096);
        value.extend_from_slice(&(reason_len as u32).to_be_bytes());
        value.extend_from_slice(&reason[..reason_len]);
        let command = MetadataCommand {
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
                    key: claim_key.clone(),
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
        };
        match self.commit_metadata(command) {
            Ok(_) => {
                outcome.records_removed += 1;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn object_delete_is_protected(&self, record: &ObjectGcRecord) -> Result<bool, MetadError> {
        if self
            .object_retention()?
            .version_floor
            .is_some_and(|floor| floor.get() < record.enqueue_version)
        {
            return Ok(true);
        }
        let digest = object_key_digest(&record.object_key);
        if !self
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: fork_base_ref_inverse_prefix(self.mount, &digest),
                start_after: None,
                version: self.read_version()?,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })?
            .is_empty()
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

    fn release_releasing_fork_base_refs(&self, limit: usize) -> Result<(), MetadError> {
        let mut remaining = limit.max(1);
        for row in self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: fork_base_ref_release_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit: remaining,
            purpose: ReadPurpose::WritePlanLocal,
        })? {
            let binding = match decode_fork_binding(&row.value.0) {
                Ok(binding) => binding,
                Err(err) => {
                    self.quarantine_base_ref_release_job(&row, &err.to_string())?;
                    continue;
                }
            };
            let binding_key = fork_binding_key(self.mount, binding.fork_root);
            let binding_item = self.metadata.get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?;
            let live_binding = binding_item
                .as_ref()
                .map(|item| {
                    decode_fork_binding(&item.value.0)
                        .map_err(|err| MetadError::Codec(err.to_string()))
                })
                .transpose()?
                .filter(|live| {
                    live.state == ForkBindingState::Releasing
                        && live.base_ref_set_id == binding.base_ref_set_id
                });
            let operation_key = restore_operation_key(self.mount, &binding.operation_digest);
            let operation = if live_binding.is_some() {
                self.metadata.get_versioned(
                    RecordFamily::System,
                    &operation_key,
                    self.read_version()?,
                    ReadPurpose::WritePlanLocal,
                )?
            } else {
                None
            };
            let guard = BaseRefReleaseGuard {
                family: RecordFamily::System,
                key: row.key,
                version: row.version,
                binding: live_binding
                    .and_then(|_| binding_item.map(|item| (binding_key, item.version))),
                operation: operation.map(|item| (operation_key, item.version)),
            };
            let released = match self.release_base_ref_page(&binding, &guard, remaining) {
                Ok(released) => released,
                Err(MetadError::Metadata(MetadataError::PredicateFailed)) => continue,
                Err(MetadError::Codec(reason)) => {
                    self.quarantine_base_ref_release_job(
                        &crate::command::ScanItem {
                            key: guard.key.clone(),
                            value: Value(encode_fork_binding(&binding)),
                            version: guard.version,
                        },
                        &reason,
                    )?;
                    continue;
                }
                Err(err) => return Err(err),
            };
            remaining = remaining.saturating_sub(released);
            if remaining == 0 {
                return Ok(());
            }
        }
        Ok(())
    }

    fn quarantine_base_ref_release_job(
        &self,
        row: &crate::command::ScanItem,
        reason: &str,
    ) -> Result<(), MetadError> {
        let digest: [u8; 32] = Sha256::digest(&row.key).into();
        let quarantine_key = fork_base_release_quarantine_key(self.mount, &digest);
        let version = self.next_version()?;
        let reason = reason.as_bytes();
        let reason_len = reason.len().min(4096);
        let mut value = Vec::new();
        value.extend_from_slice(&(row.key.len() as u32).to_be_bytes());
        value.extend_from_slice(&row.key);
        value.extend_from_slice(&(row.value.0.len() as u32).to_be_bytes());
        value.extend_from_slice(&row.value.0);
        value.extend_from_slice(&(reason_len as u32).to_be_bytes());
        value.extend_from_slice(&reason[..reason_len]);
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"quarantine-fork-base-release",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: row.key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
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
                delete_mutation(RecordFamily::System, row.key.clone()),
                Mutation {
                    family: RecordFamily::System,
                    key: quarantine_key,
                    op: MutationOp::Put,
                    value: Some(Value(value)),
                },
            ],
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn release_base_ref_page(
        &self,
        binding: &ForkBinding,
        guard: &BaseRefReleaseGuard,
        limit: usize,
    ) -> Result<usize, MetadError> {
        let read_version = self.read_version()?;
        let owner_prefix = fork_base_ref_set_prefix(self.mount, binding.base_ref_set_id);
        let cursor_key = fork_base_ref_release_cursor_key(self.mount, binding.base_ref_set_id);
        let cursor = self.metadata.get_versioned(
            RecordFamily::System,
            &cursor_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        let start_after = cursor.as_ref().map(|item| item.value.0.clone());
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: owner_prefix.clone(),
            start_after,
            version: read_version,
            limit: limit.max(1),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            if let Some(cursor) = cursor {
                self.reset_base_ref_release_cursor(guard, &cursor_key, cursor.version)?;
                return Ok(1);
            }
            let released = self.release_restore_path_index_page(binding, guard, limit)?;
            if released != 0 {
                return Ok(released);
            }
            self.finalize_base_ref_release(guard)?;
            return Ok(0);
        }

        // A base-ref belongs to the restored borrower inode, not to the fork
        // root's pathname. Renames and hardlinks can therefore outlive removal
        // of the root binding. Keep every owner/inverse row while the borrower's
        // current manifest still names that object; final unlink or a
        // self-contained body replacement removes the manifest and makes the
        // row releasable on a later pass.
        let references = rows
            .iter()
            .map(|row| decode_fork_base_ref(&row.value.0))
            .collect::<Result<Vec<_>, _>>()?;
        let mut required_by_owner = HashMap::<(InodeId, u64), HashSet<String>>::new();
        for reference in &references {
            required_by_owner
                .entry((reference.owner_inode, reference.owner_generation))
                .or_default()
                .insert(reference.object_key.clone());
        }
        let mut retained = HashSet::new();
        for ((owner_inode, owner_generation), required) in required_by_owner {
            retained.extend(self.current_manifest_borrowed_object_keys(
                owner_inode,
                owner_generation,
                read_version,
                &required,
            )?);
        }

        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: guard.family,
            key: guard.key.clone(),
            predicate: Predicate::VersionEquals(guard.version),
        }];
        predicates.push(PredicateRef {
            family: RecordFamily::System,
            key: cursor_key.clone(),
            predicate: cursor.as_ref().map_or(Predicate::NotExists, |item| {
                Predicate::VersionEquals(item.version)
            }),
        });
        let mut mutations = Vec::with_capacity(rows.len() * 3 + 1);
        mutations.push(Mutation {
            family: RecordFamily::System,
            key: cursor_key,
            op: MutationOp::Put,
            value: Some(Value(
                rows.last()
                    .expect("non-empty base-ref release page")
                    .key
                    .clone(),
            )),
        });
        for (row, reference) in rows.iter().zip(references) {
            if retained.contains(&reference.object_key) {
                continue;
            }
            let digest =
                fork_base_ref_digest_from_owner_key(self.mount, binding.base_ref_set_id, &row.key)
                    .ok_or_else(|| {
                        MetadError::Codec("invalid fork base-ref owner key".to_owned())
                    })?;
            let inverse_key =
                fork_base_ref_inverse_key(self.mount, &digest, binding.base_ref_set_id);
            let inverse = match self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )? {
                Some(inverse) => inverse,
                None => {
                    self.repair_missing_fork_base_inverse(binding, guard, row, &inverse_key)?;
                    return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                }
            };
            let gc_key = gc_released_base_ref_key(
                self.mount,
                version.get(),
                binding.base_ref_set_id,
                &digest,
            );
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::System,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::VersionEquals(inverse.version),
                },
                PredicateRef {
                    family: RecordFamily::Gc,
                    key: gc_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ]);
            mutations.extend([
                delete_mutation(RecordFamily::System, row.key.clone()),
                delete_mutation(RecordFamily::System, inverse_key),
                Mutation {
                    family: RecordFamily::Gc,
                    key: gc_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_record(&ObjectGcRecord {
                        inode: reference.owner_inode,
                        generation: reference.owner_generation,
                        object_key: reference.object_key,
                        size: reference.size,
                        digest_uri: reference.digest_uri,
                        enqueue_version: version.get(),
                        enqueue_unix_ms: self.now_ms(),
                    }))),
                },
            ]);
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"release-fork-base-ref-page",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: rows[0].key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        // The cursor is advanced even when the whole page is retained. This is
        // bounded useful work and prevents an escaped borrower at the beginning
        // of the key range from starving releasable rows later in the set. A
        // subsequent empty tail pass resets the cursor and starts another round.
        Ok(rows.len())
    }

    fn reset_base_ref_release_cursor(
        &self,
        guard: &BaseRefReleaseGuard,
        cursor_key: &[u8],
        cursor_version: Version,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"reset-fork-base-ref-release-cursor",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.to_vec(),
            predicates: vec![
                PredicateRef {
                    family: guard.family,
                    key: guard.key.clone(),
                    predicate: Predicate::VersionEquals(guard.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: cursor_key.to_vec(),
                    predicate: Predicate::VersionEquals(cursor_version),
                },
            ],
            mutations: vec![delete_mutation(RecordFamily::System, cursor_key.to_vec())],
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn current_manifest_borrowed_object_keys(
        &self,
        owner_inode: InodeId,
        owner_generation: u64,
        version: Version,
        required: &HashSet<String>,
    ) -> Result<HashSet<String>, MetadError> {
        const MANIFEST_SCAN_PAGE: usize = 128;
        let prefix = chunk_manifest_prefix(self.mount, owner_inode, owner_generation);
        let mut start_after = None;
        let mut retained = HashSet::new();
        loop {
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::ChunkManifest,
                prefix: prefix.clone(),
                start_after: start_after.clone(),
                version,
                limit: MANIFEST_SCAN_PAGE,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if rows.is_empty() {
                break;
            }
            let exhausted = rows.len() < MANIFEST_SCAN_PAGE;
            start_after = rows.last().map(|row| row.key.clone());
            for row in rows {
                if chunk_index_from_manifest_key(&row.key)? == BODY_SUMMARY_CHUNK_INDEX {
                    continue;
                }
                let manifest = decode_chunk_manifest(&row.value.0)
                    .map_err(|err| MetadataError::Backend(err.to_string()))?;
                for block in manifest.slices.iter().flat_map(|slice| slice.blocks.iter()) {
                    if required.contains(&block.object_key) {
                        retained.insert(block.object_key.clone());
                    }
                }
            }
            if retained.len() == required.len() || exhausted {
                break;
            }
        }
        Ok(retained)
    }

    fn release_restore_path_index_page(
        &self,
        binding: &ForkBinding,
        guard: &BaseRefReleaseGuard,
        limit: usize,
    ) -> Result<usize, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore_path_index_set_prefix(self.mount, binding.base_ref_set_id),
            start_after: None,
            version: self.read_version()?,
            limit: limit.max(1),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            return Ok(0);
        }
        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: guard.family,
            key: guard.key.clone(),
            predicate: Predicate::VersionEquals(guard.version),
        }];
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        for row in &rows {
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
            let marker = decode_dentry_projection(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            let inverse_key =
                fork_shadow_key(self.mount, marker.dentry.parent, &marker.dentry.name);
            let inverse = self.metadata.get_versioned(
                RecordFamily::ForkShadow,
                &inverse_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?;
            if let Some(inverse) = inverse {
                let (inverse_set, _) = decode_restore_shadow_inverse(&inverse.value.0)?;
                if inverse_set == binding.base_ref_set_id {
                    predicates.push(PredicateRef {
                        family: RecordFamily::ForkShadow,
                        key: inverse_key.clone(),
                        predicate: Predicate::VersionEquals(inverse.version),
                    });
                    mutations.push(delete_mutation(RecordFamily::ForkShadow, inverse_key));
                }
            }
        }
        let released = rows.len();
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"release-restore-path-index-page",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: rows[0].key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(released)
    }

    fn repair_missing_fork_base_inverse(
        &self,
        binding: &ForkBinding,
        guard: &BaseRefReleaseGuard,
        owner: &crate::command::ScanItem,
        inverse_key: &[u8],
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"repair-fork-base-ref-inverse",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: inverse_key.to_vec(),
            predicates: vec![
                PredicateRef {
                    family: guard.family,
                    key: guard.key.clone(),
                    predicate: Predicate::VersionEquals(guard.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: owner.key.clone(),
                    predicate: Predicate::VersionEquals(owner.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.to_vec(),
                    predicate: Predicate::NotExists,
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: inverse_key.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(Vec::new())),
            }],
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn finalize_base_ref_release(&self, guard: &BaseRefReleaseGuard) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: guard.family,
            key: guard.key.clone(),
            predicate: Predicate::VersionEquals(guard.version),
        }];
        let mut mutations = vec![delete_mutation(guard.family, guard.key.clone())];
        if let Some((binding_key, binding_version)) = &guard.binding {
            predicates.push(PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.clone(),
                predicate: Predicate::VersionEquals(*binding_version),
            });
            mutations.push(delete_mutation(
                RecordFamily::ForkBinding,
                binding_key.clone(),
            ));
        }
        if let Some((operation_key, operation_version)) = &guard.operation {
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(*operation_version),
            });
            mutations.push(delete_mutation(RecordFamily::System, operation_key.clone()));
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"complete-fork-base-ref-release",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: guard.family,
            primary_key: guard.key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(())
    }

    pub fn cleanup_staged_objects(
        &self,
        staged: &StagedObjectSet,
    ) -> Result<ObjectCleanupOutcome, MetadError> {
        self.objects.delete_staged(staged).map_err(Into::into)
    }

    fn advance_pending_restore_staging_cleanup(&self, limit: usize) -> Result<usize, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore_staging_cleanup_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit: limit.clamp(1, 128),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mut advanced = 0;
        for state in rows {
            match self.advance_restore_staging_cleanup_state(&state, limit) {
                Ok(()) | Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {}
                Err(err) => return Err(err),
            }
            advanced += 1;
        }
        Ok(advanced)
    }

    fn advance_restore_staging_cleanup_state(
        &self,
        state: &ScanItem,
        limit: usize,
    ) -> Result<(), MetadError> {
        let (disposition, binding) = decode_restore_staging_cleanup(&state.value.0)?;
        let member_prefix = restore_staging_member_prefix(self.mount, binding.base_ref_set_id);
        let member = self
            .metadata
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: member_prefix,
                start_after: None,
                version: self.read_version()?,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })?
            .into_iter()
            .next();
        if let Some(member) = member {
            if disposition == RestoreStagingCleanupDisposition::ForgetMembers {
                return self.forget_restore_staging_member(state, &member);
            }
            return self.cleanup_restore_staging_member(state, &binding, &member);
        }
        if disposition != RestoreStagingCleanupDisposition::ForgetMembers {
            if self.delete_restore_staging_base_ref_page(state, &binding, limit)? != 0 {
                return Ok(());
            }
            if self.delete_restore_staging_path_index_page(state, &binding, limit)? != 0 {
                return Ok(());
            }
        }
        self.finalize_restore_staging_cleanup(state, disposition, &binding)
    }

    fn forget_restore_staging_member(
        &self,
        state: &ScanItem,
        member: &ScanItem,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"forget-restore-staging-member",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: member.key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: state.key.clone(),
                    predicate: Predicate::VersionEquals(state.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: member.key.clone(),
                    predicate: Predicate::VersionEquals(member.version),
                },
            ],
            mutations: vec![delete_mutation(RecordFamily::System, member.key.clone())],
            watch: Vec::new(),
        })?;
        Ok(())
    }

    fn cleanup_restore_staging_member(
        &self,
        state: &ScanItem,
        binding: &ForkBinding,
        member: &ScanItem,
    ) -> Result<(), MetadError> {
        let inode = restore_staging_member_inode(self.mount, binding.base_ref_set_id, &member.key)
            .ok_or_else(|| MetadError::Codec("invalid restore staging member key".to_owned()))?;
        if self.cleanup_restore_staging_family_page(
            state,
            inode,
            RecordFamily::Dentry,
            dentry_prefix(self.mount, inode),
        )? {
            return Ok(());
        }
        if self.cleanup_restore_staging_family_page(
            state,
            inode,
            RecordFamily::Xattr,
            xattr_prefix(self.mount, inode),
        )? {
            return Ok(());
        }
        let inode_key = inode_key(self.mount, inode);
        let attr = self.metadata.get_versioned(
            RecordFamily::Inode,
            &inode_key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )?;
        if let Some(attr) = &attr {
            let attr_value = decode_inode_attr(&attr.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            let manifests = self.metadata.scan(ScanRequest {
                family: RecordFamily::ChunkManifest,
                prefix: chunk_manifest_prefix(self.mount, inode, attr_value.generation),
                start_after: None,
                version: self.read_version()?,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            if let Some(manifest) = manifests.first() {
                let version = self.next_version()?;
                let command = self.restore_staging_manifest_cleanup_command(
                    &state.key,
                    state.version,
                    inode,
                    attr_value.generation,
                    manifest,
                    version,
                    self.now_ms(),
                )?;
                validate_restore_command_bounds(&command, "restore staging manifest cleanup")?;
                self.commit_metadata(command)?;
                return Ok(());
            }
        }
        self.finish_restore_staging_member(state, binding, member, inode, attr.as_ref())
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_staging_manifest_cleanup_command(
        &self,
        cleanup_key: &[u8],
        cleanup_version: Version,
        inode: InodeId,
        generation: u64,
        row: &ScanItem,
        version: Version,
        enqueue_unix_ms: u64,
    ) -> Result<MetadataCommand, MetadError> {
        let chunk_index = chunk_index_from_manifest_key(&row.key)?;
        let mut mutations = Vec::new();
        if chunk_index != BODY_SUMMARY_CHUNK_INDEX {
            let manifest = decode_chunk_manifest(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            for (block_index, block) in manifest
                .slices
                .iter()
                .flat_map(|slice| slice.blocks.iter())
                .enumerate()
            {
                if !self.owns_block_object_key(inode, generation, &block.object_key) {
                    continue;
                }
                mutations.push(Mutation {
                    family: RecordFamily::Gc,
                    key: gc_object_key(
                        self.mount,
                        version.get(),
                        inode,
                        generation,
                        chunk_index,
                        block_index as u64,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_object_gc_record(&ObjectGcRecord {
                        inode,
                        generation,
                        object_key: block.object_key.clone(),
                        size: block.len,
                        digest_uri: block.digest_uri.clone(),
                        enqueue_version: version.get(),
                        enqueue_unix_ms,
                    }))),
                });
            }
        }
        mutations.push(delete_mutation(
            RecordFamily::ChunkManifest,
            row.key.clone(),
        ));
        Ok(MetadataCommand {
            request_id: request_id(
                b"restore-staging-cleanup-manifest",
                self.mount,
                inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::ChunkManifest,
            primary_key: row.key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::System,
                    key: cleanup_key.to_vec(),
                    predicate: Predicate::VersionEquals(cleanup_version),
                },
                PredicateRef {
                    family: RecordFamily::ChunkManifest,
                    key: row.key.clone(),
                    predicate: Predicate::VersionEquals(row.version),
                },
            ],
            mutations,
            watch: Vec::new(),
        })
    }

    fn cleanup_restore_staging_family_page(
        &self,
        state: &ScanItem,
        inode: InodeId,
        family: RecordFamily,
        prefix: Vec<u8>,
    ) -> Result<bool, MetadError> {
        const PAGE: usize = 64;
        let rows = self.metadata.scan(ScanRequest {
            family,
            prefix,
            start_after: None,
            version: self.read_version()?,
            limit: PAGE,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            return Ok(false);
        }
        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: state.key.clone(),
            predicate: Predicate::VersionEquals(state.version),
        }];
        predicates.extend(rows.iter().map(|row| PredicateRef {
            family,
            key: row.key.clone(),
            predicate: Predicate::VersionEquals(row.version),
        }));
        let command = MetadataCommand {
            request_id: request_id(
                b"cleanup-restore-staging-family",
                self.mount,
                inode,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: family,
            primary_key: rows[0].key.clone(),
            predicates,
            mutations: rows
                .iter()
                .map(|row| delete_mutation(family, row.key.clone()))
                .collect(),
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore staging family cleanup")?;
        self.commit_metadata(command)?;
        Ok(true)
    }

    fn finish_restore_staging_member(
        &self,
        state: &ScanItem,
        binding: &ForkBinding,
        member: &ScanItem,
        inode: InodeId,
        attr: Option<&crate::command::ReadItem>,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let inode_key = inode_key(self.mount, inode);
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: state.key.clone(),
                predicate: Predicate::VersionEquals(state.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: member.key.clone(),
                predicate: Predicate::VersionEquals(member.version),
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_prefix(self.mount, inode),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::Xattr,
                key: xattr_prefix(self.mount, inode),
                predicate: Predicate::PrefixEmpty,
            },
        ];
        if let Some(attr) = attr {
            predicates.push(PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key.clone(),
                predicate: Predicate::VersionEquals(attr.version),
            });
            let decoded = decode_inode_attr(&attr.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            predicates.push(PredicateRef {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_prefix(self.mount, inode, decoded.generation),
                predicate: Predicate::PrefixEmpty,
            });
        }
        let mut mutations = vec![delete_mutation(RecordFamily::System, member.key.clone())];
        if attr.is_some() {
            mutations.push(delete_mutation(RecordFamily::Inode, inode_key));
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"finish-restore-staging-member",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: member.key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore staging member finish")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn delete_restore_staging_base_ref_page(
        &self,
        state: &ScanItem,
        binding: &ForkBinding,
        limit: usize,
    ) -> Result<usize, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: fork_base_ref_set_prefix(self.mount, binding.base_ref_set_id),
            start_after: None,
            version: self.read_version()?,
            limit: limit.clamp(1, 128),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            return Ok(0);
        }
        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: state.key.clone(),
            predicate: Predicate::VersionEquals(state.version),
        }];
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        for row in &rows {
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
            let digest =
                fork_base_ref_digest_from_owner_key(self.mount, binding.base_ref_set_id, &row.key)
                    .ok_or_else(|| MetadError::Codec("invalid staging base-ref key".to_owned()))?;
            let inverse_key =
                fork_base_ref_inverse_key(self.mount, &digest, binding.base_ref_set_id);
            if let Some(inverse) = self.metadata.get_versioned(
                RecordFamily::System,
                &inverse_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )? {
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::VersionEquals(inverse.version),
                });
                mutations.push(delete_mutation(RecordFamily::System, inverse_key));
            }
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"delete-restore-staging-base-refs",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: rows[0].key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore staging base-ref cleanup")?;
        self.commit_metadata(command)?;
        Ok(rows.len())
    }

    fn delete_restore_staging_path_index_page(
        &self,
        state: &ScanItem,
        binding: &ForkBinding,
        limit: usize,
    ) -> Result<usize, MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore_path_index_set_prefix(self.mount, binding.base_ref_set_id),
            start_after: None,
            version: self.read_version()?,
            limit: limit.clamp(1, 128),
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            return Ok(0);
        }
        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: state.key.clone(),
            predicate: Predicate::VersionEquals(state.version),
        }];
        let mut mutations = Vec::with_capacity(rows.len() * 2);
        for row in &rows {
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            mutations.push(delete_mutation(RecordFamily::System, row.key.clone()));
            let marker = decode_dentry_projection(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            let inverse_key =
                fork_shadow_key(self.mount, marker.dentry.parent, &marker.dentry.name);
            if let Some(inverse) = self.metadata.get_versioned(
                RecordFamily::ForkShadow,
                &inverse_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )? {
                let (set_id, _) = decode_restore_shadow_inverse(&inverse.value.0)?;
                if set_id == binding.base_ref_set_id {
                    predicates.push(PredicateRef {
                        family: RecordFamily::ForkShadow,
                        key: inverse_key.clone(),
                        predicate: Predicate::VersionEquals(inverse.version),
                    });
                    mutations.push(delete_mutation(RecordFamily::ForkShadow, inverse_key));
                }
            }
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"delete-restore-staging-path-index",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: rows[0].key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore staging path-index cleanup")?;
        self.commit_metadata(command)?;
        Ok(rows.len())
    }

    fn finalize_restore_staging_cleanup(
        &self,
        state: &ScanItem,
        disposition: RestoreStagingCleanupDisposition,
        binding: &ForkBinding,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        let mut predicates = vec![
            PredicateRef {
                family: RecordFamily::System,
                key: state.key.clone(),
                predicate: Predicate::VersionEquals(state.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: restore_staging_member_prefix(self.mount, binding.base_ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
        ];
        let mut mutations = vec![delete_mutation(RecordFamily::System, state.key.clone())];
        if disposition != RestoreStagingCleanupDisposition::ForgetMembers {
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::System,
                    key: fork_base_ref_set_prefix(self.mount, binding.base_ref_set_id),
                    predicate: Predicate::PrefixEmpty,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_path_index_set_prefix(self.mount, binding.base_ref_set_id),
                    predicate: Predicate::PrefixEmpty,
                },
            ]);
            let binding_key = fork_binding_key(self.mount, binding.fork_root);
            let operation_key = restore_operation_key(self.mount, &binding.operation_digest);
            let read_version = self.read_version()?;
            let binding_item = self
                .metadata
                .get_versioned(
                    RecordFamily::ForkBinding,
                    &binding_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::NotFound)?;
            let operation = self
                .metadata
                .get_versioned(
                    RecordFamily::System,
                    &operation_key,
                    read_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::NotFound)?;
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: binding_key.clone(),
                    predicate: Predicate::VersionEquals(binding_item.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: operation_key.clone(),
                    predicate: Predicate::VersionEquals(operation.version),
                },
            ]);
            match disposition {
                RestoreStagingCleanupDisposition::ResetForRetry => {
                    let mut prepared = binding.clone();
                    prepared.state = ForkBindingState::Preparing;
                    let clean_key = restore_staging_clean_key(self.mount, binding.base_ref_set_id);
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: clean_key.clone(),
                        predicate: Predicate::NotExists,
                    });
                    mutations.extend([
                        Mutation {
                            family: RecordFamily::ForkBinding,
                            key: binding_key,
                            op: MutationOp::Put,
                            value: Some(Value(encode_fork_binding(&prepared))),
                        },
                        Mutation {
                            family: RecordFamily::System,
                            key: operation_key,
                            op: MutationOp::Put,
                            value: Some(Value(encode_fork_binding(&prepared))),
                        },
                        Mutation {
                            family: RecordFamily::System,
                            key: clean_key,
                            op: MutationOp::Put,
                            value: Some(Value(Vec::new())),
                        },
                    ]);
                }
                RestoreStagingCleanupDisposition::Discard => {
                    let hold_key = fork_base_hold_key(
                        self.mount,
                        binding.pinned_read_version,
                        binding.base_ref_set_id,
                    );
                    if let Some(hold) = self.metadata.get_versioned(
                        RecordFamily::System,
                        &hold_key,
                        read_version,
                        ReadPurpose::WritePlanLocal,
                    )? {
                        predicates.push(PredicateRef {
                            family: RecordFamily::System,
                            key: hold_key.clone(),
                            predicate: Predicate::VersionEquals(hold.version),
                        });
                        mutations.push(delete_mutation(RecordFamily::System, hold_key));
                    }
                    mutations.push(delete_mutation(RecordFamily::ForkBinding, binding_key));
                    mutations.push(delete_mutation(RecordFamily::System, operation_key));
                }
                RestoreStagingCleanupDisposition::ForgetMembers => unreachable!(),
            }
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"finalize-restore-staging-cleanup",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: state.key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore staging cleanup finalization")?;
        self.commit_metadata(command)?;
        Ok(())
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
        if self.recover_object_gc_claim_locked(&mut outcome)? == ObjectGcClaimProgress::Pending {
            return Ok(outcome);
        }
        self.advance_pending_restore_staging_cleanup(limit)?;
        outcome.snapshot_reap = self.reclaim_expired_snapshot_pins(limit)?;
        self.release_releasing_fork_base_refs(limit)?;
        self.cleanup_stale_path_index_page(limit)?;
        let now_ms = self.now_ms();
        let grace_ms = duration_millis_u64(read_lease_grace);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Gc,
            prefix: gc_queue_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit,
            purpose: ReadPurpose::UserStrong,
        })?;
        outcome.scanned += rows.len();
        for row in rows {
            let record = match decode_object_gc_record(&row.value.0) {
                Ok(record) => record,
                Err(err) => {
                    let claim_version = self.acquire_object_gc_claim(&row)?;
                    self.quarantine_claimed_gc_row(
                        claim_version,
                        &row,
                        &err.to_string(),
                        &mut outcome,
                    )?;
                    continue;
                }
            };
            if now_ms < record.enqueue_unix_ms.saturating_add(grace_ms) {
                outcome.blocked_by_read_leases += 1;
                continue;
            }
            if self.delete_gc_row_under_durable_claim(&row, &record, &mut outcome)?
                == ObjectGcClaimProgress::Pending
            {
                break;
            }
        }
        Ok(outcome)
    }

    fn cleanup_stale_path_index_page(&self, limit: usize) -> Result<usize, MetadError> {
        const MAX_PAGE: usize = 128;
        let page_size = limit.clamp(1, MAX_PAGE);
        let cursor_key = path_index_gc_cursor_key(self.mount);
        let read_version = self.read_version()?;
        let cursor = self.metadata.get_versioned(
            RecordFamily::System,
            &cursor_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::PathIndex,
            prefix: path_index_prefix(self.mount, &[]),
            start_after: cursor.as_ref().map(|item| item.value.0.clone()),
            version: read_version,
            limit: page_size,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if rows.is_empty() {
            if let Some(cursor) = cursor {
                self.reset_path_index_gc_cursor(&cursor_key, cursor.version)?;
                return Ok(1);
            }
            return Ok(0);
        }
        let stale = rows
            .iter()
            .map(|row| {
                self.path_index_row_is_stale(row, read_version)
                    .map(|stale| (row, stale))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter_map(|(row, stale)| stale.then_some(row))
            .collect::<Vec<_>>();
        let last_key = rows
            .last()
            .expect("non-empty path-index GC page")
            .key
            .clone();
        let version = self.next_version()?;
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::System,
            key: cursor_key.clone(),
            predicate: cursor.as_ref().map_or(Predicate::NotExists, |item| {
                Predicate::VersionEquals(item.version)
            }),
        }];
        predicates.extend(stale.iter().map(|row| PredicateRef {
            family: RecordFamily::PathIndex,
            key: row.key.clone(),
            predicate: Predicate::VersionEquals(row.version),
        }));
        let mut mutations = vec![Mutation {
            family: RecordFamily::System,
            key: cursor_key.clone(),
            op: MutationOp::Put,
            value: Some(Value(last_key.clone())),
        }];
        mutations.extend(
            stale
                .iter()
                .map(|row| delete_mutation(RecordFamily::PathIndex, row.key.clone())),
        );
        let command = MetadataCommand {
            request_id: request_id(
                b"cleanup-stale-path-index-page",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        match self.commit_metadata(command) {
            Ok(_)
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => Ok(rows.len()),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                // One concurrent PathIndex update must not block unrelated stale
                // rows in this page. Delete each candidate with its captured
                // version, then advance the durable cursor independently.
                for row in stale {
                    self.delete_stale_path_index_row(row)?;
                }
                self.advance_path_index_gc_cursor(&cursor_key, cursor.as_ref(), &last_key)?;
                Ok(rows.len())
            }
            Err(err) => Err(err),
        }
    }

    fn path_index_row_is_stale(
        &self,
        row: &crate::command::ScanItem,
        version: Version,
    ) -> Result<bool, MetadError> {
        let prefix = path_index_prefix(self.mount, &[]);
        let Some(suffix) = row.key.strip_prefix(prefix.as_slice()) else {
            return Ok(true);
        };
        let components = suffix
            .split(|byte| *byte == PATH_INDEX_DELIMITER)
            .map(|component| {
                DentryName::new(component.to_vec())
                    .map_err(|err| MetadError::InvalidPath(err.to_string()))
            })
            .collect::<Result<Vec<_>, _>>();
        let components = match components {
            Ok(components) if !components.is_empty() => components,
            Ok(_) | Err(_) => return Ok(true),
        };
        let (name, parents) = components
            .split_last()
            .expect("non-empty PathIndex components");
        let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
            InodeId::root(),
            parents,
            version,
            ReadPurpose::WritePlanLocal,
        ) {
            Ok(parent) => parent,
            Err(MetadError::NotFound | MetadError::NotDirectory) => return Ok(true),
            Err(err) => return Err(err),
        };
        let Some((canonical, dentry_version)) = self.lookup_plus_at_version_for_purpose(
            parent,
            name,
            version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(true);
        };
        let indexed = decode_dentry_projection(&row.value.0)
            .map(DentryWithAttr::from)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        Ok(row.version != dentry_version || indexed != canonical)
    }

    fn delete_stale_path_index_row(
        &self,
        row: &crate::command::ScanItem,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"cleanup-stale-path-index-row",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::PathIndex,
            primary_key: row.key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::PathIndex,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            }],
            mutations: vec![delete_mutation(RecordFamily::PathIndex, row.key.clone())],
            watch: Vec::new(),
        }) {
            Ok(_)
            | Err(MetadError::Metadata(MetadataError::PredicateFailed))
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn advance_path_index_gc_cursor(
        &self,
        cursor_key: &[u8],
        cursor: Option<&crate::command::ReadItem>,
        last_key: &[u8],
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"advance-path-index-gc-cursor",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: cursor_key.to_vec(),
                predicate: cursor.map_or(Predicate::NotExists, |item| {
                    Predicate::VersionEquals(item.version)
                }),
            }],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: cursor_key.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(last_key.to_vec())),
            }],
            watch: Vec::new(),
        }) {
            Ok(_)
            | Err(MetadError::Metadata(MetadataError::PredicateFailed))
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn reset_path_index_gc_cursor(
        &self,
        cursor_key: &[u8],
        cursor_version: Version,
    ) -> Result<(), MetadError> {
        let version = self.next_version()?;
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"reset-path-index-gc-cursor",
                self.mount,
                InodeId::root(),
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: cursor_key.to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::System,
                key: cursor_key.to_vec(),
                predicate: Predicate::VersionEquals(cursor_version),
            }],
            mutations: vec![delete_mutation(RecordFamily::System, cursor_key.to_vec())],
            watch: Vec::new(),
        }) {
            Ok(_)
            | Err(MetadError::Metadata(MetadataError::PredicateFailed))
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn cleanup_history(&self, limit: usize) -> Result<HistoryPruneOutcome, MetadError> {
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let mut recovery_outcome = PendingObjectCleanupOutcome::default();
        if self.recover_object_gc_claim_locked(&mut recovery_outcome)?
            == ObjectGcClaimProgress::Pending
        {
            return Ok(HistoryPruneOutcome::default());
        }
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
                Err(MetadataError::PredicateFailed) if attempt + 1 < RETENTION_RETRIES => {
                    continue;
                }
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
        if let Some(version) = self.preparing_fork_hold_floor()? {
            floor = Some(floor.map_or(version, |floor| floor.min(version)));
        }
        Ok(floor)
    }

    /// Object GC can use exact completed fork references. Preparing forks still
    /// conservatively hold their whole read version because materialization has
    /// not yet produced the final object-key set.
    fn object_retention(&self) -> Result<ObjectRetention, MetadError> {
        let now_ms = self.now_ms();
        let mut version_floor: Option<Version> = None;
        for row in self.metadata.scan(ScanRequest {
            family: RecordFamily::Snapshot,
            prefix: snapshot_pin_prefix(self.mount),
            start_after: None,
            version: self.read_version()?,
            limit: 0,
            purpose: ReadPurpose::UserStrong,
        })? {
            let pin = decode_snapshot_pin(&row.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            if now_ms < pin.lease_expires_unix_ms {
                let version = Version::new(pin.read_version)?;
                version_floor = Some(version_floor.map_or(version, |floor| floor.min(version)));
            }
        }
        if let Some(version) = self.preparing_fork_hold_floor()? {
            version_floor = Some(version_floor.map_or(version, |floor| floor.min(version)));
        }
        Ok(ObjectRetention { version_floor })
    }

    fn preparing_fork_hold_floor(&self) -> Result<Option<Version>, MetadError> {
        self.metadata
            .scan(ScanRequest {
                family: RecordFamily::System,
                prefix: fork_base_hold_prefix(self.mount),
                start_after: None,
                version: self.read_version()?,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })?
            .first()
            .map(|row| {
                let read_version =
                    fork_base_hold_read_version(self.mount, &row.key).ok_or_else(|| {
                        MetadError::Codec("invalid preparing fork hold key".to_owned())
                    })?;
                Version::new(read_version).map_err(Into::into)
            })
            .transpose()
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
        let enqueue_unix_ms = self.now_ms();
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
        let enqueue_unix_ms = self.now_ms();
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
