use super::*;

const RESTORE_CLONE_BATCH_ENTRIES: usize = 64;
const MAX_RESTORE_COMMAND_ITEMS: usize = 4096;
const MAX_RESTORE_COMMAND_BYTES: usize = 8 * 1024 * 1024;
pub(super) const MAX_RESTORE_SUBTREE_ENTRIES: usize = 1_000_000;

/// One node of the source subtree paired with the fork inode it copies into.
struct CloneFrame {
    src_inode: InodeId,
    dst_inode: InodeId,
    relative_components: Vec<DentryName>,
    ancestor_markers: Vec<DentryProjection>,
}

pub(super) struct RestorePathIndexContext {
    pub(super) source_root_components: Vec<DentryName>,
    pub(super) base_ref_set_id: u64,
    pub(super) track_staging_members: bool,
}

struct CloneChildrenContext<'a> {
    src_parent: InodeId,
    dst_parent: InodeId,
    relative_parent: &'a [DentryName],
    ancestor_markers: &'a [DentryProjection],
    read_version: Version,
    path_index: Option<&'a RestorePathIndexContext>,
}

pub(super) struct DirectoryPathProof {
    pub(super) inode: InodeId,
    pub(super) predicates: Vec<PredicateRef>,
}

struct RestoreHoldRequest<'a> {
    restored_root: InodeId,
    source_root: InodeId,
    pin: &'a SnapshotPin,
    pin_version: Version,
    operation_digest: [u8; 32],
    destination_path: String,
    source_path_predicates: &'a [PredicateRef],
    initialization_digest: [u8; 32],
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    /// Restore a retained snapshot into a new writable copy-on-write subtree.
    ///
    /// The source remains unchanged. The destination is attached exactly once;
    /// retrying the same `(source, snapshot, destination)` returns the existing
    /// fork, while an unrelated existing destination fails loudly. The source
    /// checkpoint stabilizes construction; once attached, the [`ForkBinding`]
    /// lifecycle and its exact durable base references protect shared objects
    /// independently of the caller-owned snapshot lease.
    pub fn restore_subtree_path_to_fork(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
    ) -> Result<RestoreOutcome, MetadError> {
        self.restore_subtree_path_to_fork_initialized(
            source_path,
            snapshot_id,
            destination_path,
            RestoreInitialization::default(),
        )
    }

    pub fn restore_subtree_path_to_fork_initialized(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
        initialization: RestoreInitialization,
    ) -> Result<RestoreOutcome, MetadError> {
        self.restore_subtree_path_to_fork_impl(
            source_path,
            snapshot_id,
            destination_path,
            initialization,
            false,
        )
    }

    #[cfg(test)]
    pub(super) fn restore_subtree_path_to_fork_leave_preparing(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
        initialization: RestoreInitialization,
    ) -> Result<(), MetadError> {
        self.restore_subtree_path_to_fork_impl(
            source_path,
            snapshot_id,
            destination_path,
            initialization,
            true,
        )
        .map(|_| ())
    }

    fn restore_subtree_path_to_fork_impl(
        &self,
        source_path: &str,
        snapshot_id: u64,
        destination_path: &str,
        initialization: RestoreInitialization,
        stop_before_attach: bool,
    ) -> Result<RestoreOutcome, MetadError> {
        let _restore = self
            .restore_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let source_components = parse_absolute_path(source_path)?;
        let source_path = canonical_path(&source_components)?;
        let destination_components = parse_absolute_path(destination_path)?;
        let destination_path = canonical_path(&destination_components)?;
        const MAX_RESTORE_PATH_BYTES: usize = 4096;
        if source_path.len().saturating_add(destination_path.len()) > MAX_RESTORE_PATH_BYTES {
            return Err(MetadError::RestoreResourceLimit {
                resource: "restore source and destination path bytes".to_owned(),
                limit: MAX_RESTORE_PATH_BYTES as u64,
                actual: source_path.len().saturating_add(destination_path.len()) as u64,
            });
        }
        let (initialization, initialization_digest) =
            canonical_restore_initialization(initialization)?;
        let operation_digest =
            restore_operation_digest(&source_path, snapshot_id, &destination_path);
        let operation_id = restore_operation_id(&source_path, snapshot_id, &destination_path)?;

        // Terminal lookup precedes source/pin validation. The durable completed
        // binding is sufficient to answer an identical retry even after the
        // source or caller-owned snapshot pin has been retired.
        if let Some(outcome) = self.existing_restore_outcome(
            &destination_path,
            &operation_id,
            operation_digest,
            initialization_digest,
        )? {
            return Ok(outcome);
        }
        let Some((destination_name, destination_parent_components)) =
            destination_components.split_last()
        else {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path,
            });
        };
        self.resolve_directory_path_proof(destination_parent_components)?;
        if self.lookup_path(&destination_path)?.is_some() {
            if let Some(stale) = self.restore_binding_by_operation(operation_digest)? {
                if stale.state == ForkBindingState::Preparing
                    && !self.binding_root_is_visible(&stale)?
                {
                    self.schedule_restore_staging_cleanup(
                        &stale,
                        RestoreStagingCleanupDisposition::Discard,
                    )?;
                }
            }
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path,
            });
        }
        // A crash-left Preparing hold is the recovery capability: it retains the
        // historical source version even after the caller pin/source path is
        // removed. Reset partial staging while preserving that hold, then rebuild
        // from its durable source root/read version.
        let resume = if let Some(stale) = self.restore_binding_by_operation(operation_digest)? {
            if stale.state == ForkBindingState::Releasing {
                return Err(MetadError::RestoreInProgress);
            }
            if stale.initialization_digest != initialization_digest
                || stale.state != ForkBindingState::Preparing
                || self.binding_root_is_visible(&stale)?
            {
                return Err(MetadError::RestoreDestinationConflict {
                    destination: destination_path,
                });
            }
            if !self.claim_clean_restore_staging(&stale)? {
                self.schedule_restore_staging_cleanup(
                    &stale,
                    RestoreStagingCleanupDisposition::ResetForRetry,
                )?;
                return Err(MetadError::RestoreInProgress);
            }
            if let Err(err) = self.preflight_snapshot_subtree_restore(
                stale.source_root,
                Version::new(stale.pinned_read_version)?,
                &initialization,
            ) {
                if is_deterministic_restore_preflight_error(&err) {
                    self.schedule_restore_staging_cleanup(
                        &stale,
                        RestoreStagingCleanupDisposition::Discard,
                    )?;
                }
                return Err(err);
            }
            Some(stale)
        } else {
            None
        };

        let (source_root, read_version, restored_root, binding) = if let Some(binding) = resume {
            (
                binding.source_root,
                binding.pinned_read_version,
                binding.fork_root,
                binding,
            )
        } else {
            let mut prepared = None;
            for attempt in 0..8 {
                let source_proof = self.resolve_directory_path_proof(&source_components)?;
                let source_root = source_proof.inode;
                self.ensure_snapshot_id_shard(snapshot_id, source_root)?;
                let (pin, pin_version) = self
                    .versioned_snapshot_pin(snapshot_id)?
                    .ok_or(MetadError::NotFound)?;
                if pin.root != source_root {
                    return Err(MetadError::SnapshotRootMismatch {
                        snapshot_id,
                        expected_root: source_root,
                        actual_root: Some(pin.root),
                        actual_shard: self.shard_index(),
                    });
                }
                self.ensure_snapshot_pin_live(&pin)?;
                self.preflight_snapshot_subtree_restore(
                    source_root,
                    Version::new(pin.read_version)?,
                    &initialization,
                )?;
                let restored_root = self.next_inode()?;
                match self.create_restore_hold(RestoreHoldRequest {
                    restored_root,
                    source_root,
                    pin: &pin,
                    pin_version,
                    operation_digest,
                    destination_path: destination_path.clone(),
                    source_path_predicates: &source_proof.predicates,
                    initialization_digest,
                }) {
                    Ok(binding) => {
                        prepared = Some((source_root, pin.read_version, restored_root, binding));
                        break;
                    }
                    Err(MetadError::Metadata(MetadataError::PredicateFailed)) if attempt < 7 => {
                        continue;
                    }
                    Err(err) => return Err(err),
                }
            }
            prepared.ok_or(MetadError::Metadata(MetadataError::PredicateFailed))?
        };
        let path_index = RestorePathIndexContext {
            source_root_components: source_components.clone(),
            base_ref_set_id: binding.base_ref_set_id,
            track_staging_members: true,
        };
        if let Err(err) = self.materialize_subtree_at_root(
            source_root,
            restored_root,
            Version::new(read_version)?,
            Some(&path_index),
        ) {
            self.schedule_restore_staging_cleanup(
                &binding,
                RestoreStagingCleanupDisposition::ResetForRetry,
            )?;
            return Err(err);
        }
        if let Err(err) = self.apply_restore_initialization(
            restored_root,
            binding.base_ref_set_id,
            &initialization,
        ) {
            self.schedule_restore_staging_cleanup(
                &binding,
                RestoreStagingCleanupDisposition::ResetForRetry,
            )?;
            return Err(err);
        }
        let base_refs = match self.subtree_base_refs(restored_root, read_version) {
            Ok(references) => references,
            Err(err) => {
                self.schedule_restore_staging_cleanup(
                    &binding,
                    RestoreStagingCleanupDisposition::ResetForRetry,
                )?;
                return Err(err);
            }
        };
        if let Err(err) = self.persist_fork_base_refs(&binding, &base_refs) {
            self.schedule_restore_staging_cleanup(
                &binding,
                RestoreStagingCleanupDisposition::ResetForRetry,
            )?;
            return Err(err);
        }
        if stop_before_attach {
            return Err(MetadError::SyncLogArchiveFailed {
                committed: false,
                message: "test stop before restore attach".to_owned(),
            });
        }
        let mut attach = Err(MetadError::Metadata(MetadataError::PredicateFailed));
        for _ in 0..8 {
            let destination_proof =
                match self.resolve_directory_path_proof(destination_parent_components) {
                    Ok(proof) => proof,
                    Err(err) => {
                        attach = Err(err);
                        break;
                    }
                };
            attach = self.attach_restored_root(
                restored_root,
                &binding,
                destination_proof.inode,
                destination_name.clone(),
                &destination_proof.predicates,
            );
            if !matches!(
                attach,
                Err(MetadError::Metadata(MetadataError::PredicateFailed))
            ) {
                break;
            }
        }
        if let Err(err) = attach {
            // A racing identical request may have attached first. Reconcile the
            // durable destination before returning the predicate failure.
            if let Some(outcome) = self.existing_restore_outcome(
                &destination_path,
                &operation_id,
                operation_digest,
                initialization_digest,
            )? {
                return Ok(outcome);
            }
            let err = if self.lookup_path(&destination_path)?.is_some() {
                MetadError::RestoreDestinationConflict {
                    destination: destination_path.clone(),
                }
            } else {
                err
            };
            self.schedule_restore_staging_cleanup(
                &binding,
                RestoreStagingCleanupDisposition::Discard,
            )?;
            return Err(err);
        }

        Ok(RestoreOutcome {
            operation_id,
            state: RestoreState::Complete,
            source_root,
            destination_root: restored_root,
            snapshot_id,
            read_version,
            cleanup_pending: false,
        })
    }

    fn existing_restore_outcome(
        &self,
        destination_path: &str,
        operation_id: &str,
        operation_digest: [u8; 32],
        initialization_digest: [u8; 32],
    ) -> Result<Option<RestoreOutcome>, MetadError> {
        let Some(binding) = self.restore_binding_by_operation(operation_digest)? else {
            return Ok(None);
        };
        if binding.initialization_digest != initialization_digest {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path.to_owned(),
            });
        }
        if binding.state == ForkBindingState::Preparing {
            return Ok(None);
        }
        if binding.state == ForkBindingState::Releasing {
            return Err(MetadError::RestoreInProgress);
        }
        let Some(destination) = self.lookup_path(destination_path)? else {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path.to_owned(),
            });
        };
        if destination.attr.file_type != FileType::Directory {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path.to_owned(),
            });
        }
        if destination.attr.inode != binding.fork_root
            || binding.destination_path != destination_path
            || binding.operation_digest != operation_digest
            || binding.state != ForkBindingState::Complete
        {
            return Err(MetadError::RestoreDestinationConflict {
                destination: destination_path.to_owned(),
            });
        }
        Ok(Some(RestoreOutcome {
            operation_id: operation_id.to_owned(),
            state: RestoreState::Complete,
            source_root: binding.source_root,
            destination_root: binding.fork_root,
            snapshot_id: binding.snapshot_id,
            read_version: binding.pinned_read_version,
            cleanup_pending: false,
        }))
    }

    pub(super) fn versioned_snapshot_pin(
        &self,
        snapshot_id: u64,
    ) -> Result<Option<(SnapshotPin, Version)>, MetadError> {
        self.metadata
            .get_versioned(
                RecordFamily::Snapshot,
                &snapshot_pin_key(self.mount, snapshot_id),
                self.read_version()?,
                ReadPurpose::UserStrong,
            )?
            .map(|item| {
                decode_snapshot_pin(&item.value.0)
                    .map(|pin| (pin, item.version))
                    .map_err(|err| MetadError::Codec(err.to_string()))
            })
            .transpose()
    }

    fn restore_binding_by_operation(
        &self,
        operation_digest: [u8; 32],
    ) -> Result<Option<ForkBinding>, MetadError> {
        self.metadata
            .get(
                RecordFamily::System,
                &restore_operation_key(self.mount, &operation_digest),
                self.read_version()?,
                ReadPurpose::UserStrong,
            )?
            .map(|value| {
                decode_fork_binding(&value.0).map_err(|err| MetadError::Codec(err.to_string()))
            })
            .transpose()
    }

    pub(super) fn schedule_restore_staging_cleanup(
        &self,
        binding: &ForkBinding,
        disposition: RestoreStagingCleanupDisposition,
    ) -> Result<(), MetadError> {
        let cleanup_key = restore_staging_cleanup_key(self.mount, binding.base_ref_set_id);
        if let Some(existing) = self.metadata.get_versioned(
            RecordFamily::System,
            &cleanup_key,
            self.read_version()?,
            ReadPurpose::WritePlanLocal,
        )? {
            let (current_disposition, current_binding) =
                decode_restore_staging_cleanup(&existing.value.0)?;
            if current_binding.operation_digest != binding.operation_digest {
                return Err(MetadError::RestoreDestinationConflict {
                    destination: binding.destination_path.clone(),
                });
            }
            if current_disposition == disposition
                || current_disposition == RestoreStagingCleanupDisposition::Discard
                || disposition != RestoreStagingCleanupDisposition::Discard
            {
                return Ok(());
            }
            let version = self.next_version()?;
            self.commit_metadata(MetadataCommand {
                request_id: request_id(
                    b"upgrade-restore-staging-cleanup",
                    self.mount,
                    binding.fork_root,
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: cleanup_key.clone(),
                predicates: vec![PredicateRef {
                    family: RecordFamily::System,
                    key: cleanup_key.clone(),
                    predicate: Predicate::VersionEquals(existing.version),
                }],
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key: cleanup_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_cleanup(
                        RestoreStagingCleanupDisposition::Discard,
                        &current_binding,
                    ))),
                }],
                watch: Vec::new(),
            })?;
            return Ok(());
        }

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
        let mut releasing = decode_fork_binding(&binding_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        if releasing.operation_digest != binding.operation_digest {
            return Err(MetadError::RestoreDestinationConflict {
                destination: binding.destination_path.clone(),
            });
        }
        let operation = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        if releasing.state == ForkBindingState::Releasing {
            return Err(MetadError::RestoreInProgress);
        }
        if releasing.state != ForkBindingState::Preparing {
            return Err(MetadError::RestoreDestinationConflict {
                destination: binding.destination_path.clone(),
            });
        }
        releasing.state = ForkBindingState::Releasing;
        let clean_key = restore_staging_clean_key(self.mount, binding.base_ref_set_id);
        let clean = self.metadata.get_versioned(
            RecordFamily::System,
            &clean_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?;
        let version = self.next_version()?;
        let mut predicates = vec![
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
            PredicateRef {
                family: RecordFamily::System,
                key: cleanup_key.clone(),
                predicate: Predicate::NotExists,
            },
        ];
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::ForkBinding,
                key: binding_key,
                op: MutationOp::Put,
                value: Some(Value(encode_fork_binding(&releasing))),
            },
            Mutation {
                family: RecordFamily::System,
                key: operation_key,
                op: MutationOp::Put,
                value: Some(Value(encode_fork_binding(&releasing))),
            },
            Mutation {
                family: RecordFamily::System,
                key: cleanup_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_staging_cleanup(
                    disposition,
                    &releasing,
                ))),
            },
        ];
        if let Some(clean) = clean {
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: clean_key.clone(),
                predicate: Predicate::VersionEquals(clean.version),
            });
            mutations.push(delete_mutation(RecordFamily::System, clean_key));
        }
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"schedule-restore-staging-cleanup",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: restore_staging_cleanup_key(self.mount, binding.base_ref_set_id),
            predicates,
            mutations,
            watch: Vec::new(),
        }) {
            Ok(_)
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => Ok(()),
            Err(MetadError::Metadata(MetadataError::PredicateFailed))
                if self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &restore_staging_cleanup_key(self.mount, binding.base_ref_set_id),
                        self.read_version()?,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some() =>
            {
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    fn claim_clean_restore_staging(&self, binding: &ForkBinding) -> Result<bool, MetadError> {
        let clean_key = restore_staging_clean_key(self.mount, binding.base_ref_set_id);
        let read_version = self.read_version()?;
        let Some(clean) = self.metadata.get_versioned(
            RecordFamily::System,
            &clean_key,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Ok(false);
        };
        let binding_key = fork_binding_key(self.mount, binding.fork_root);
        let binding_item = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"claim-clean-restore-staging",
                self.mount,
                binding.fork_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: clean_key.clone(),
            predicates: vec![
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: binding_key,
                    predicate: Predicate::VersionEquals(binding_item.version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: clean_key.clone(),
                    predicate: Predicate::VersionEquals(clean.version),
                },
            ],
            mutations: vec![delete_mutation(RecordFamily::System, clean_key)],
            watch: Vec::new(),
        })?;
        Ok(true)
    }

    fn binding_root_is_visible(&self, binding: &ForkBinding) -> Result<bool, MetadError> {
        Ok(self
            .lookup_path(&binding.destination_path)?
            .is_some_and(|entry| entry.attr.inode == binding.fork_root))
    }

    pub(super) fn resolve_directory_path_proof(
        &self,
        components: &[DentryName],
    ) -> Result<DirectoryPathProof, MetadError> {
        let version = self.read_version()?;
        let mut current = InodeId::root();
        let mut predicates = vec![PredicateRef {
            family: RecordFamily::Inode,
            key: inode_key(self.mount, current),
            predicate: Predicate::Exists,
        }];
        for name in components {
            let (entry, dentry_version) = self
                .lookup_plus_at_version_for_purpose(
                    current,
                    name,
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::NotFound)?;
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_key(self.mount, current, name),
                predicate: Predicate::VersionEquals(dentry_version),
            });
            current = entry.attr.inode;
        }
        Ok(DirectoryPathProof {
            inode: current,
            predicates,
        })
    }

    fn create_restore_hold(
        &self,
        request: RestoreHoldRequest<'_>,
    ) -> Result<ForkBinding, MetadError> {
        let RestoreHoldRequest {
            restored_root,
            source_root,
            pin,
            pin_version,
            operation_digest,
            destination_path,
            source_path_predicates,
            initialization_digest,
        } = request;
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let key = fork_binding_key(self.mount, restored_root);
        let operation_key = restore_operation_key(self.mount, &operation_digest);
        let binding = ForkBinding {
            fork_root: restored_root,
            source_root,
            pinned_read_version: pin.read_version,
            snapshot_id: pin.snapshot_id,
            created_version: version.get(),
            base_ref_set_id: version.get(),
            operation_digest,
            initialization_digest,
            state: ForkBindingState::Preparing,
            destination_path,
        };
        let hold_key = fork_base_hold_key(
            self.mount,
            binding.pinned_read_version,
            binding.base_ref_set_id,
        );
        let mut predicates = source_path_predicates.to_vec();
        predicates.push(object_reference.predicate(self.mount));
        predicates.extend([
            PredicateRef {
                family: RecordFamily::Snapshot,
                key: snapshot_pin_key(self.mount, pin.snapshot_id),
                predicate: Predicate::VersionEquals(pin_version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: fork_base_ref_set_prefix(self.mount, binding.base_ref_set_id),
                predicate: Predicate::PrefixEmpty,
            },
            PredicateRef {
                family: RecordFamily::System,
                key: hold_key.clone(),
                predicate: Predicate::NotExists,
            },
        ]);
        let committed = self.commit_metadata(MetadataCommand {
            request_id: request_id(b"restore-to-fork-hold", self.mount, restored_root, version),
            kind: CommandKind::SnapshotSubtree,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::ForkBinding,
            primary_key: key.clone(),
            predicates,
            mutations: vec![
                Mutation {
                    family: RecordFamily::ForkBinding,
                    key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_fork_binding(&binding))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: operation_key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(encode_fork_binding(&binding))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: hold_key,
                    op: MutationOp::Put,
                    value: Some(Value(Vec::new())),
                },
            ],
            watch: Vec::new(),
        });
        match committed {
            Ok(_)
            | Err(MetadError::SyncLogArchiveFailed {
                committed: true, ..
            }) => {
                let durable = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        &operation_key,
                        self.read_version()?,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .ok_or_else(|| {
                        MetadError::Codec(
                            "restore hold committed without operation index".to_owned(),
                        )
                    })?;
                if decode_fork_binding(&durable.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?
                    != binding
                {
                    return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                }
                Ok(binding)
            }
            Err(err) => Err(err),
        }
    }

    fn attach_restored_root(
        &self,
        restored_root: InodeId,
        binding: &ForkBinding,
        destination_parent: InodeId,
        destination_name: DentryName,
        destination_path_predicates: &[PredicateRef],
    ) -> Result<(), MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let Some(mut attr) = self.get_attr_at_version_for_purpose(
            restored_root,
            read_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::NotFound);
        };
        attr.ctime_ms = self.now_ms();
        let projection = projection(destination_parent, destination_name.clone(), attr, None);
        let dentry = dentry_key(self.mount, destination_parent, &destination_name);
        let binding_key = fork_binding_key(self.mount, restored_root);
        let operation_key = restore_operation_key(self.mount, &binding.operation_digest);
        let binding_item = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        let durable_binding = decode_fork_binding(&binding_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?;
        if &durable_binding != binding {
            return Err(MetadError::RestoreDestinationConflict {
                destination: String::from_utf8_lossy(destination_name.as_bytes()).into_owned(),
            });
        }
        let binding_version = binding_item.version;
        let operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        if decode_fork_binding(&operation_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?
            != *binding
        {
            return Err(MetadError::RestoreDestinationConflict {
                destination: binding.destination_path.clone(),
            });
        }
        let hold_key = fork_base_hold_key(
            self.mount,
            binding.pinned_read_version,
            binding.base_ref_set_id,
        );
        let hold = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &hold_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore hold index is missing".to_owned()))?;
        let mut completed_binding = durable_binding;
        completed_binding.state = ForkBindingState::Complete;
        let staging_cleanup_key = restore_staging_cleanup_key(self.mount, binding.base_ref_set_id);
        let mut predicates = destination_path_predicates.to_vec();
        predicates.push(object_reference.predicate(self.mount));
        predicates.extend([
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, destination_parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.clone(),
                predicate: Predicate::VersionEquals(binding_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(operation_item.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: hold_key.clone(),
                predicate: Predicate::VersionEquals(hold.version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: staging_cleanup_key.clone(),
                predicate: Predicate::NotExists,
            },
        ]);
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-to-fork-attach",
                self.mount,
                restored_root,
                version,
            ),
            kind: CommandKind::CreateDir,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry.clone(),
            predicates,
            mutations: vec![
                put_projection_mutation(RecordFamily::Dentry, dentry, &projection),
                Mutation {
                    family: RecordFamily::ForkBinding,
                    key: binding_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_fork_binding(&completed_binding))),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: operation_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_fork_binding(&completed_binding))),
                },
                delete_mutation(RecordFamily::System, hold_key),
                Mutation {
                    family: RecordFamily::System,
                    key: staging_cleanup_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_staging_cleanup(
                        RestoreStagingCleanupDisposition::ForgetMembers,
                        &completed_binding,
                    ))),
                },
            ],
            watch: self
                .watch_projection(
                    destination_parent,
                    WatchEvent {
                        kind: WatchEventKind::Create,
                        parent: Some(destination_parent),
                        name: Some(destination_name),
                        inode: restored_root,
                        version: version.get(),
                    },
                )
                .into_iter()
                .collect(),
        };
        self.commit_metadata(command)?;
        Ok(())
    }

    /// Create a writable copy-on-write fork of the directory subtree rooted at
    /// `src_root`.
    ///
    /// The returned [`CloneHandle::root`] is a new namespace root that sees every
    /// file and directory the source had at clone time. File bodies are **shared,
    /// not copied**: the fork's chunk manifests reference the same object blocks
    /// (same `blocks/{mount}/{inode}/{generation}/...` keys) as the source. The
    /// data is therefore zero-copy and the clone is **independent of data size**;
    /// the metadata work is **O(entries)** — one inode + one commit per descendant,
    /// the same complexity class as any per-entry namespace copy (batching these
    /// commits, or a lazy CoW namespace fork, is future work). Each fork node gets a
    /// fresh inode, so the fork's namespace is fully isolated: writing or deleting
    /// in the fork does not affect the source, and vice versa.
    ///
    /// Divergence is copy-on-write at the object layer. The first time a fork file
    /// is rewritten, the normal publish path mints a fresh generation under the
    /// fork's own inode, producing new object keys; the borrowed source blocks are
    /// left untouched (a borrower never GCs another namespace's blocks).
    ///
    /// This low-level detached constructor keeps its construction pin until an
    /// internal caller attaches or discards the tree. Public path-based clones use
    /// the durable restore-to-fork lifecycle instead.
    #[cfg(test)]
    pub(super) fn clone_subtree(&self, src_root: InodeId) -> Result<CloneHandle, MetadError> {
        // Pin the source first so the read version we copy from is stable and the
        // shared base objects are GC-protected from the moment the fork exists.
        let pin = self.snapshot_subtree(src_root)?;
        let version = Version::new(pin.read_version)?;
        let dst_root = self.materialize_subtree_at(src_root, version)?;
        Ok(CloneHandle {
            root: dst_root,
            snapshot_id: pin.snapshot_id,
        })
    }

    /// Materialize the directory subtree rooted at `src_root`, **as seen at
    /// `read_version`**, into a brand-new detached namespace root and return that
    /// root inode.
    ///
    /// This convenience wrapper reserves a destination root and delegates
    /// to the same copy-on-write walk used by restore and rollback. Every node is reproduced
    /// under a fresh inode while keeping each file body's `generation`, so the new
    /// tree's chunk manifests reference the same `blocks/{mount}/{inode}/{generation}/...`
    /// object keys as the source captured at `read_version` — the bodies are shared,
    /// not copied, so this is zero-copy in data and O(entries) in metadata (one
    /// commit per descendant). The returned root is detached: no
    /// dentry names it yet, so the caller must link it (clone) or graft it over an
    /// existing root (rollback).
    ///
    /// `read_version` must be a stable, GC-protected read version (a snapshot pin's
    /// `read_version`), otherwise the source blocks the new manifests borrow could be
    /// reclaimed out from under it.
    ///
    /// Snapshot-aware metadata scans merge current keys with the derived retained-
    /// history key index, so deleted entries are enumerated without a whole-history
    /// scan or a second point-read reconstruction pass.
    pub(super) fn materialize_subtree_at(
        &self,
        src_root: InodeId,
        read_version: Version,
    ) -> Result<InodeId, MetadError> {
        let dst_root = self.next_inode()?;
        self.materialize_subtree_at_root(src_root, dst_root, read_version, None)?;
        Ok(dst_root)
    }

    /// Materialize into a pre-reserved root. Restore-to-fork reserves this inode
    /// before installing its durable operation hold, so history/object retention
    /// is protected for the entire materialization window.
    pub(super) fn materialize_subtree_at_root(
        &self,
        src_root: InodeId,
        dst_root: InodeId,
        read_version: Version,
        path_index: Option<&RestorePathIndexContext>,
    ) -> Result<(), MetadError> {
        let Some(src_attr) =
            self.get_attr_at_version_for_purpose(src_root, read_version, ReadPurpose::Snapshot)?
        else {
            return Err(MetadError::NotFound);
        };
        if src_attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }

        let mut root_attr = src_attr;
        root_attr.inode = dst_root;
        self.commit_root_inode(
            &root_attr,
            path_index
                .filter(|context| context.track_staging_members)
                .map(|context| context.base_ref_set_id),
        )?;
        self.copy_inode_xattrs(src_root, dst_root, read_version)?;

        // Breadth-first so a fork parent always exists before its children (the
        // create path predicates on the parent inode existing). Each directory's
        // children are materialized in a single batched commit, so a clone costs
        // one commit per source directory rather than one per entry.
        let mut queue = vec![CloneFrame {
            src_inode: src_root,
            dst_inode: dst_root,
            relative_components: Vec::new(),
            ancestor_markers: Vec::new(),
        }];
        while let Some(frame) = queue.pop() {
            let children = self.list_dir_at_version(frame.src_inode, read_version)?;
            self.validate_restore_entries(&children)?;
            if !children.is_empty() {
                let context = CloneChildrenContext {
                    src_parent: frame.src_inode,
                    dst_parent: frame.dst_inode,
                    relative_parent: &frame.relative_components,
                    ancestor_markers: &frame.ancestor_markers,
                    read_version,
                    path_index,
                };
                let mut sub_frames = self.clone_children_into(&context, &children)?;
                queue.append(&mut sub_frames);
            }
        }

        Ok(())
    }

    /// Validate every deterministic restore constraint before creating a durable
    /// hold or detached inode. This keeps invalid hardlinks, cross-shard grafts,
    /// initialization paths, and oversized metadata commands from leaking a
    /// resumable Preparing operation that can never succeed.
    pub(super) fn preflight_snapshot_subtree_restore(
        &self,
        root: InodeId,
        version: Version,
        initialization: &RestoreInitialization,
    ) -> Result<(), MetadError> {
        let attr = self
            .get_attr_at_version_for_purpose(root, version, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
        if attr.file_type != FileType::Directory {
            return Err(MetadError::NotDirectory);
        }
        self.preflight_snapshot_xattrs(root, version)?;
        let mut queue = vec![root];
        let mut entries_seen = 0usize;
        while let Some(directory) = queue.pop() {
            let children = self.list_dir_at_version(directory, version)?;
            self.validate_restore_entries(&children)?;
            entries_seen = entries_seen.saturating_add(children.len());
            check_restore_resource(
                "snapshot subtree entries",
                MAX_RESTORE_SUBTREE_ENTRIES,
                entries_seen,
            )?;
            for batch in children.chunks(RESTORE_CLONE_BATCH_ENTRIES) {
                let mut items = 2usize.saturating_add(batch.len());
                let mut bytes = 256usize;
                for child in batch {
                    items = items.saturating_add(2);
                    bytes = bytes
                        .saturating_add(child.dentry.name.as_bytes().len())
                        .saturating_add(encode_inode_attr(&child.attr).len())
                        .saturating_add(
                            encode_dentry_projection(&projection(
                                directory,
                                child.dentry.name.clone(),
                                child.attr.clone(),
                                child.body.clone(),
                            ))
                            .len(),
                        );
                    if let Some(body) = &child.body {
                        let chunks = self.chunk_manifests_for_body_at_version(
                            child.attr.inode,
                            body,
                            version,
                            ReadPurpose::Snapshot,
                        )?;
                        items = items.saturating_add(1).saturating_add(chunks.len());
                        bytes = bytes.saturating_add(encode_body_descriptor(body).len());
                        for chunk in &chunks {
                            bytes = bytes.saturating_add(encode_chunk_manifest(chunk).len());
                        }
                    }
                    self.preflight_snapshot_xattrs(child.attr.inode, version)?;
                    if child.attr.file_type == FileType::Directory {
                        queue.push(child.attr.inode);
                    }
                }
                check_restore_resource(
                    "detached clone batch items",
                    MAX_RESTORE_COMMAND_ITEMS,
                    items,
                )?;
                check_restore_resource(
                    "detached clone batch encoded bytes",
                    MAX_RESTORE_COMMAND_BYTES,
                    bytes,
                )?;
            }
        }
        self.preflight_restore_initialization(root, version, initialization)
    }

    fn preflight_snapshot_xattrs(
        &self,
        inode: InodeId,
        version: Version,
    ) -> Result<(), MetadError> {
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Xattr,
            prefix: xattr_prefix(self.mount, inode),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::Snapshot,
        })?;
        for batch in rows.chunks(128) {
            let items = 2usize.saturating_add(batch.len().saturating_mul(2));
            let bytes = batch.iter().fold(256usize, |total, row| {
                total
                    .saturating_add(row.key.len().saturating_mul(2))
                    .saturating_add(row.value.0.len())
            });
            check_restore_resource(
                "detached xattr batch items",
                MAX_RESTORE_COMMAND_ITEMS,
                items,
            )?;
            check_restore_resource(
                "detached xattr batch encoded bytes",
                MAX_RESTORE_COMMAND_BYTES,
                bytes,
            )?;
        }
        Ok(())
    }

    fn preflight_restore_initialization(
        &self,
        root: InodeId,
        version: Version,
        initialization: &RestoreInitialization,
    ) -> Result<(), MetadError> {
        for relative_path in &initialization.remove_relative_paths {
            let components = restore_relative_components(relative_path)?;
            let Some((name, parents)) = components.split_last() else {
                return Err(MetadError::InvalidPath(
                    "restore initialization cannot remove its root".to_owned(),
                ));
            };
            let parent = match self.resolve_components_as_directory_from_at_version_for_purpose(
                root,
                parents,
                version,
                ReadPurpose::Snapshot,
            ) {
                Ok(parent) => parent,
                Err(MetadError::NotFound) => continue,
                Err(err) => return Err(err),
            };
            if self
                .lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::Snapshot)?
                .is_some_and(|(entry, _)| entry.attr.file_type == FileType::Directory)
            {
                return Err(MetadError::InvalidPath(format!(
                    "restore initialization remove path is a directory: {relative_path}"
                )));
            }
        }
        for file in &initialization.files {
            let components = restore_relative_components(&file.relative_path)?;
            let Some((name, parents)) = components.split_last() else {
                return Err(MetadError::InvalidPath(
                    "restore initialization cannot write its root".to_owned(),
                ));
            };
            let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
                root,
                parents,
                version,
                ReadPurpose::Snapshot,
            )?;
            if self
                .lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::Snapshot)?
                .is_some_and(|(entry, _)| entry.attr.file_type != FileType::File)
            {
                return Err(MetadError::InvalidPath(format!(
                    "restore initialization write path is not a file: {}",
                    file.relative_path
                )));
            }
        }
        Ok(())
    }

    pub(super) fn validate_restore_entries(
        &self,
        entries: &[DentryWithAttr],
    ) -> Result<(), MetadError> {
        for entry in entries {
            if entry.attr.inode.shard_index() != self.shard_index {
                return Err(MetadError::RestoreCrossShardUnsupported {
                    inode: entry.attr.inode,
                });
            }
            if entry.attr.file_type != FileType::Directory && entry.attr.nlink > 1 {
                return Err(MetadError::RestoreHardlinkUnsupported {
                    inode: entry.attr.inode,
                });
            }
        }
        Ok(())
    }

    pub(super) fn subtree_base_refs(
        &self,
        root: InodeId,
        read_version: u64,
    ) -> Result<Vec<ForkBaseRef>, MetadError> {
        let version = self.read_version()?;
        let mut references = BTreeMap::new();
        let mut queue = vec![root];
        while let Some(dir) = queue.pop() {
            for child in
                self.read_dir_plus_at_version_for_purpose(dir, version, ReadPurpose::Snapshot)?
            {
                if child.attr.file_type == FileType::Directory {
                    queue.push(child.attr.inode);
                } else if let Some(body) = &child.body {
                    let manifests = self.chunk_manifests_for_body_at_version(
                        child.attr.inode,
                        body,
                        version,
                        ReadPurpose::Snapshot,
                    )?;
                    for block in manifests
                        .iter()
                        .flat_map(|manifest| &manifest.slices)
                        .flat_map(|slice| &slice.blocks)
                    {
                        if self.owns_block_object_key(
                            child.attr.inode,
                            body.generation,
                            &block.object_key,
                        ) {
                            continue;
                        }
                        references
                            .entry(block.object_key.clone())
                            .or_insert_with(|| ForkBaseRef {
                                owner_inode: child.attr.inode,
                                owner_generation: body.generation,
                                read_version,
                                size: block.len,
                                object_key: block.object_key.clone(),
                                digest_uri: block.digest_uri.clone(),
                            });
                    }
                }
            }
        }
        Ok(references.into_values().collect())
    }

    pub(super) fn persist_fork_base_refs(
        &self,
        binding: &ForkBinding,
        references: &[ForkBaseRef],
    ) -> Result<(), MetadError> {
        const BATCH_SIZE: usize = 128;
        if references.is_empty() {
            return Ok(());
        }
        let binding_key = fork_binding_key(self.mount, binding.fork_root);
        let binding_item = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        if decode_fork_binding(&binding_item.value.0)
            .map_err(|err| MetadError::Codec(err.to_string()))?
            != *binding
        {
            return Err(MetadError::Metadata(MetadataError::PredicateFailed));
        }
        for chunk in references.chunks(BATCH_SIZE) {
            let object_reference = self.begin_object_reference_mutation()?;
            let version = self.next_version()?;
            let mut predicates = vec![
                object_reference.predicate(self.mount),
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: binding_key.clone(),
                    predicate: Predicate::VersionEquals(binding_item.version),
                },
            ];
            let mut mutations = Vec::with_capacity(chunk.len() * 2);
            for reference in chunk {
                let digest = object_key_digest(&reference.object_key);
                let owner_key =
                    fork_base_ref_owner_key(self.mount, binding.base_ref_set_id, &digest);
                let inverse_key =
                    fork_base_ref_inverse_key(self.mount, &digest, binding.base_ref_set_id);
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: owner_key.clone(),
                    predicate: Predicate::NotExists,
                });
                predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::NotExists,
                });
                mutations.push(Mutation {
                    family: RecordFamily::System,
                    key: owner_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_fork_base_ref(reference)?)),
                });
                mutations.push(Mutation {
                    family: RecordFamily::System,
                    key: inverse_key,
                    op: MutationOp::Put,
                    value: Some(Value(Vec::new())),
                });
            }
            let command = MetadataCommand {
                request_id: request_id(
                    b"persist-fork-base-refs",
                    self.mount,
                    binding.fork_root,
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: fork_base_ref_set_prefix(self.mount, binding.base_ref_set_id),
                predicates,
                mutations,
                watch: Vec::new(),
            };
            match self.commit_metadata(command) {
                Ok(_)
                | Err(MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                }) => {
                    for reference in chunk {
                        let digest = object_key_digest(&reference.object_key);
                        let owner_key =
                            fork_base_ref_owner_key(self.mount, binding.base_ref_set_id, &digest);
                        let value = self
                            .metadata
                            .get(
                                RecordFamily::System,
                                &owner_key,
                                self.read_version()?,
                                ReadPurpose::WritePlanLocal,
                            )?
                            .ok_or_else(|| {
                                MetadError::Codec("fork base-ref commit lost a row".to_owned())
                            })?;
                        if decode_fork_base_ref(&value.0)? != *reference {
                            return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                        }
                        if self
                            .metadata
                            .get(
                                RecordFamily::System,
                                &fork_base_ref_inverse_key(
                                    self.mount,
                                    &digest,
                                    binding.base_ref_set_id,
                                ),
                                self.read_version()?,
                                ReadPurpose::WritePlanLocal,
                            )?
                            .is_none()
                        {
                            return Err(MetadError::Codec(
                                "fork base-ref commit lost its inverse row".to_owned(),
                            ));
                        }
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    fn apply_restore_initialization(
        &self,
        root: InodeId,
        base_ref_set_id: u64,
        initialization: &RestoreInitialization,
    ) -> Result<(), MetadError> {
        for relative_path in &initialization.remove_relative_paths {
            let components = restore_relative_components(relative_path)?;
            let Some((name, parents)) = components.split_last() else {
                return Err(MetadError::InvalidPath(
                    "restore initialization cannot remove its root".to_owned(),
                ));
            };
            let parent = match self.resolve_components_as_directory_from_at_version(
                root,
                parents,
                self.read_version()?,
            ) {
                Ok(parent) => parent,
                Err(MetadError::NotFound) => continue,
                Err(err) => return Err(err),
            };
            let Some(entry) = self.lookup_plus(parent, name)? else {
                continue;
            };
            if entry.attr.file_type == FileType::Directory {
                return Err(MetadError::InvalidPath(format!(
                    "restore initialization remove path is a directory: {relative_path}"
                )));
            }
            self.remove_detached_restore_file(parent, name)?;
        }
        for file in &initialization.files {
            let components = restore_relative_components(&file.relative_path)?;
            let Some((name, parents)) = components.split_last() else {
                return Err(MetadError::InvalidPath(
                    "restore initialization cannot write its root".to_owned(),
                ));
            };
            let parent = self.resolve_components_as_directory_from_at_version(
                root,
                parents,
                self.read_version()?,
            )?;
            let request = PublishArtifact {
                parent,
                name: name.clone(),
                producer: "nokv-restore-initialization".to_owned(),
                digest_uri: body_digest_uri(&file.bytes),
                content_type: file.content_type.clone(),
                manifest_id: format!("restore-init/{}", file.relative_path),
                bytes: file.bytes.clone(),
                mode: file.mode,
                uid: file.uid,
                gid: file.gid,
            };
            self.publish_detached_restore_file(request, base_ref_set_id)?;
        }
        Ok(())
    }

    fn remove_detached_restore_file(
        &self,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<(), MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let read_version = predecessor(version)?;
        let (entry, dentry_version) = self
            .lookup_plus_at_version_for_purpose(
                parent,
                name,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type == FileType::Directory {
            return Err(MetadError::NotFile);
        }
        let inode_key = inode_key(self.mount, entry.attr.inode);
        let inode = self
            .metadata
            .get_versioned(
                RecordFamily::Inode,
                &inode_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::NotFound)?;
        let dentry_key = dentry_key(self.mount, parent, name);
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_key.clone(),
                predicate: Predicate::VersionEquals(dentry_version),
            },
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key.clone(),
                predicate: Predicate::VersionEquals(inode.version),
            },
        ];
        let mut mutations = vec![
            delete_mutation(RecordFamily::Dentry, dentry_key.clone()),
            delete_mutation(RecordFamily::Inode, inode_key),
        ];
        for row in self.metadata.scan(ScanRequest {
            family: RecordFamily::Xattr,
            prefix: xattr_prefix(self.mount, entry.attr.inode),
            start_after: None,
            version: read_version,
            limit: 0,
            purpose: ReadPurpose::WritePlanLocal,
        })? {
            predicates.push(PredicateRef {
                family: RecordFamily::Xattr,
                key: row.key.clone(),
                predicate: Predicate::VersionEquals(row.version),
            });
            mutations.push(delete_mutation(RecordFamily::Xattr, row.key));
        }
        if let Some(body) = entry.body {
            mutations.extend(self.chunk_manifest_delete_and_gc_mutations(
                entry.attr.inode,
                body.generation,
                version,
                &HashSet::new(),
            )?);
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-initialization-remove",
                self.mount,
                entry.attr.inode,
                version,
            ),
            kind: CommandKind::StageDetachedRestore,
            read_version,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_key,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "restore initialization remove")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    fn publish_detached_restore_file(
        &self,
        request: PublishArtifact,
        base_ref_set_id: u64,
    ) -> Result<(), MetadError> {
        let existing = self.lookup_plus_for_write_plan(request.parent, &request.name)?;
        if existing
            .as_ref()
            .is_some_and(|(entry, _)| entry.attr.file_type != FileType::File)
        {
            return Err(MetadError::NotFile);
        }
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let inode = match &existing {
            Some((entry, _)) => entry.attr.inode,
            None => self.next_inode()?,
        };
        let StagedArtifactBody {
            body,
            chunks,
            old_chunks: _,
            staged,
        } = self.stage_artifact_body(&request, inode, version)?;
        let attr = InodeAttr {
            inode,
            file_type: FileType::File,
            mode: request.mode,
            uid: request.uid,
            gid: request.gid,
            rdev: 0,
            nlink: existing
                .as_ref()
                .map_or(FileType::File.initial_link_count(), |(entry, _)| {
                    entry.attr.nlink
                }),
            size: body.size,
            generation: version.get(),
            mtime_ms: self.now_ms(),
            ctime_ms: self.now_ms(),
        };
        let projection = projection(request.parent, request.name, attr, Some(body));
        let dentry = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, projection.dentry.parent),
                predicate: Predicate::Exists,
            },
            PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: existing
                    .as_ref()
                    .map_or(Predicate::NotExists, |(_, version)| {
                        Predicate::VersionEquals(*version)
                    }),
            },
        ];
        let mut mutations = vec![
            Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&projection.attr))),
            },
            put_projection_mutation(RecordFamily::Dentry, dentry.clone(), &projection),
        ];
        if let Some((old, _)) = &existing {
            let old_inode_key = inode_key(self.mount, inode);
            let old_inode = self
                .metadata
                .get_versioned(
                    RecordFamily::Inode,
                    &old_inode_key,
                    predecessor(version)?,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::NotFound)?;
            predicates.push(PredicateRef {
                family: RecordFamily::Inode,
                key: old_inode_key,
                predicate: Predicate::VersionEquals(old_inode.version),
            });
            if let Some(old_body) = &old.body {
                mutations.extend(self.collapse_chain_gc_mutations(
                    inode,
                    old_body.generation,
                    &[],
                    version,
                    &chunk_object_keys(&chunks),
                )?);
            }
        } else {
            predicates.push(PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, inode),
                predicate: Predicate::NotExists,
            });
            let member = restore_staging_member_key(self.mount, base_ref_set_id, inode);
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: member.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::System,
                key: member,
                op: MutationOp::Put,
                value: Some(Value(Vec::new())),
            });
        }
        if let Some(body) = &projection.body {
            mutations.push(Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(
                    self.mount,
                    inode,
                    body.generation,
                    BODY_SUMMARY_CHUNK_INDEX,
                ),
                op: MutationOp::Put,
                value: Some(Value(encode_body_descriptor(body))),
            });
            mutations.extend(chunks.iter().map(|chunk| Mutation {
                family: RecordFamily::ChunkManifest,
                key: chunk_manifest_key(self.mount, inode, body.generation, chunk.chunk_index),
                op: MutationOp::Put,
                value: Some(Value(encode_chunk_manifest(chunk))),
            }));
        }
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-initialization-publish",
                self.mount,
                inode,
                version,
            ),
            kind: CommandKind::StageDetachedRestore,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry,
            predicates,
            mutations,
            watch: Vec::new(),
        };
        if let Err(err) =
            validate_restore_command_bounds(&command, "restore initialization publish")
        {
            self.cleanup_staged_objects(&staged)?;
            return Err(err);
        }
        let committed = self.commit_metadata(command);
        if let Err(source) = committed {
            self.cleanup_staged_objects(&staged)?;
            return Err(MetadError::PublishArtifactFailed {
                source: Box::new(source),
                staged,
            });
        }
        Ok(())
    }

    /// Enumerate the children of `dir` at `version` through snapshot-aware scan.
    pub(super) fn list_dir_at_version(
        &self,
        dir: InodeId,
        version: Version,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        self.read_dir_plus_at_version_for_purpose(dir, version, ReadPurpose::Snapshot)
    }

    /// Path variant of [`NoKvFs::clone_subtree`].
    #[cfg(test)]
    pub(super) fn clone_subtree_path(&self, path: &str) -> Result<CloneHandle, MetadError> {
        let root = self.resolve_directory_path(path)?;
        self.clone_subtree(root)
    }

    /// Clone the subtree at `src_path` and link the resulting fork root into the
    /// namespace at `dst_path`, so the fork is a usable, navigable directory.
    ///
    /// The construction checkpoint stabilizes the source read only. The attached
    /// destination receives durable fork-base references before it becomes visible,
    /// so source checkpoint expiry/retirement does not affect its lifetime.
    /// `dst_path`'s parent must already exist and `dst_path` itself must be free.
    pub fn clone_subtree_path_into(
        &self,
        src_path: &str,
        dst_path: &str,
    ) -> Result<CloneHandle, MetadError> {
        let pin = self.snapshot_subtree_path(src_path)?;
        let restored = match self.restore_subtree_path_to_fork(src_path, pin.snapshot_id, dst_path)
        {
            Ok(restored) => restored,
            Err(MetadError::RestoreDestinationConflict { .. }) => {
                return Err(MetadError::Metadata(MetadataError::PredicateFailed));
            }
            Err(err) => return Err(err),
        };
        Ok(CloneHandle {
            root: restored.destination_root,
            snapshot_id: pin.snapshot_id,
        })
    }

    /// Report the path-level differences between two subtrees as a flat list of
    /// [`SubtreeDelta`]s. Paths are relative to the subtree roots. An entry that
    /// exists only under `b_root` is `Added`, only under `a_root` is `Removed`, and
    /// present under both but with a different type or content is `Modified`.
    ///
    /// Two files are considered identical when they share the same content
    /// generation (the copy-on-write sharing signal a clone establishes) along with
    /// the same size and content digest; a divergent write bumps the generation, so
    /// rewritten files surface as `Modified` while still-shared files do not.
    pub fn diff_subtrees(
        &self,
        a_root: InodeId,
        b_root: InodeId,
    ) -> Result<Vec<SubtreeDelta>, MetadError> {
        let version = self.read_version()?;
        if self
            .get_attr_at_version(a_root, version)?
            .is_none_or(|attr| attr.file_type != FileType::Directory)
        {
            return Err(MetadError::NotDirectory);
        }
        if self
            .get_attr_at_version(b_root, version)?
            .is_none_or(|attr| attr.file_type != FileType::Directory)
        {
            return Err(MetadError::NotDirectory);
        }
        let mut deltas = Vec::new();
        self.diff_dirs(a_root, b_root, "", version, &mut deltas)?;
        Ok(deltas)
    }

    /// Path variant of [`NoKvFs::diff_subtrees`]. Resolves both subtree roots from
    /// their paths and reports the deltas with `a_path` as the base direction.
    pub fn diff_subtrees_path(
        &self,
        a_path: &str,
        b_path: &str,
    ) -> Result<Vec<SubtreeDelta>, MetadError> {
        let a_root = self.resolve_directory_path(a_path)?;
        let b_root = self.resolve_directory_path(b_path)?;
        self.diff_subtrees(a_root, b_root)
    }

    fn diff_dirs(
        &self,
        a_dir: InodeId,
        b_dir: InodeId,
        prefix: &str,
        version: Version,
        deltas: &mut Vec<SubtreeDelta>,
    ) -> Result<(), MetadError> {
        let a_entries = self.entries_by_name(a_dir, version)?;
        let b_entries = self.entries_by_name(b_dir, version)?;
        for (name, a_entry) in &a_entries {
            let path = child_path(prefix, name)?;
            match b_entries.get(name) {
                None => deltas.push(SubtreeDelta {
                    path,
                    kind: SubtreeDeltaKind::Removed,
                    digest: entry_digest(a_entry),
                    size_delta: -(a_entry.attr.size as i64),
                }),
                Some(b_entry) => {
                    let both_dirs = a_entry.attr.file_type == FileType::Directory
                        && b_entry.attr.file_type == FileType::Directory;
                    if both_dirs {
                        self.diff_dirs(
                            a_entry.attr.inode,
                            b_entry.attr.inode,
                            &path,
                            version,
                            deltas,
                        )?;
                    } else if !entries_equivalent(a_entry, b_entry) {
                        deltas.push(SubtreeDelta {
                            path,
                            kind: SubtreeDeltaKind::Modified,
                            digest: entry_digest(b_entry),
                            size_delta: b_entry.attr.size as i64 - a_entry.attr.size as i64,
                        });
                    }
                }
            }
        }
        for (name, b_entry) in &b_entries {
            if !a_entries.contains_key(name) {
                deltas.push(SubtreeDelta {
                    path: child_path(prefix, name)?,
                    kind: SubtreeDeltaKind::Added,
                    digest: entry_digest(b_entry),
                    size_delta: b_entry.attr.size as i64,
                });
            }
        }
        Ok(())
    }

    fn entries_by_name(
        &self,
        dir: InodeId,
        version: Version,
    ) -> Result<BTreeMap<Vec<u8>, DentryWithAttr>, MetadError> {
        let entries =
            self.read_dir_plus_at_version_for_purpose(dir, version, ReadPurpose::UserStrong)?;
        Ok(entries
            .into_iter()
            .map(|entry| (entry.dentry.name.as_bytes().to_vec(), entry))
            .collect())
    }

    fn commit_root_inode(
        &self,
        attr: &InodeAttr,
        staging_set_id: Option<u64>,
    ) -> Result<(), MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let key = inode_key(self.mount, attr.inode);
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::Inode,
                key: key.clone(),
                predicate: Predicate::NotExists,
            },
        ];
        let mut mutations = vec![Mutation {
            family: RecordFamily::Inode,
            key: key.clone(),
            op: MutationOp::Put,
            value: Some(Value(encode_inode_attr(attr))),
        }];
        if let Some(base_ref_set_id) = staging_set_id {
            let member = restore_staging_member_key(self.mount, base_ref_set_id, attr.inode);
            predicates.push(PredicateRef {
                family: RecordFamily::System,
                key: member.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::System,
                key: member,
                op: MutationOp::Put,
                value: Some(Value(Vec::new())),
            });
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"clone-subtree-root", self.mount, attr.inode, version),
            kind: CommandKind::StageDetachedRestore,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Inode,
            primary_key: key.clone(),
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        Ok(())
    }

    /// Materialize one source directory in fixed-size metadata batches. A wide
    /// directory therefore cannot create an unbounded atomic command, while each
    /// child remains unreachable until the final restore attach.
    fn clone_children_into(
        &self,
        context: &CloneChildrenContext<'_>,
        children: &[DentryWithAttr],
    ) -> Result<Vec<CloneFrame>, MetadError> {
        let mut sub_frames = Vec::new();
        for batch in children.chunks(RESTORE_CLONE_BATCH_ENTRIES) {
            sub_frames.extend(self.clone_children_batch(context, batch)?);
        }
        Ok(sub_frames)
    }

    fn clone_children_batch(
        &self,
        context: &CloneChildrenContext<'_>,
        children: &[DentryWithAttr],
    ) -> Result<Vec<CloneFrame>, MetadError> {
        let CloneChildrenContext {
            src_parent,
            dst_parent,
            relative_parent,
            ancestor_markers,
            read_version,
            path_index,
        } = *context;
        let object_reference = self.begin_object_reference_mutation()?;
        let commit_version = self.next_version()?;
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, dst_parent),
                predicate: Predicate::Exists,
            },
        ];
        let mut mutations = Vec::new();
        let mut sub_frames = Vec::new();
        // Xattrs are copied after the batch commit (so the dst inodes exist); most
        // entries have none, so this is usually empty.
        let mut xattr_copies = Vec::new();
        let mut planned_shadow_keys = HashSet::new();

        for child in children {
            let mut relative_components = relative_parent.to_vec();
            relative_components.push(child.dentry.name.clone());
            let dst_inode = self.next_inode()?;
            let mut attr = child.attr.clone();
            attr.inode = dst_inode;
            attr.nlink = attr.file_type.initial_link_count();

            let (body, chunks) = match &child.body {
                Some(body) => {
                    // Carry the body descriptor verbatim, including its generation:
                    // the fork's chunk manifests land under (dst_inode, generation)
                    // but still point at the source's object blocks.
                    attr.generation = body.generation;
                    let chunks = self.chunk_manifests_for_body_at_version(
                        child.attr.inode,
                        body,
                        read_version,
                        ReadPurpose::Snapshot,
                    )?;
                    (Some(body.clone()), chunks)
                }
                None => {
                    attr.generation = commit_version.get();
                    (None, Vec::new())
                }
            };

            let proj = projection(dst_parent, child.dentry.name.clone(), attr, body);
            let dentry = dentry_key(self.mount, dst_parent, &proj.dentry.name);
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry.clone(),
                predicate: Predicate::NotExists,
            });
            mutations.push(Mutation {
                family: RecordFamily::Inode,
                key: inode_key(self.mount, dst_inode),
                op: MutationOp::Put,
                value: Some(Value(encode_inode_attr(&proj.attr))),
            });
            mutations.push(put_projection_mutation(RecordFamily::Dentry, dentry, &proj));
            if let Some(context) = path_index {
                if context.track_staging_members {
                    let member =
                        restore_staging_member_key(self.mount, context.base_ref_set_id, dst_inode);
                    predicates.push(PredicateRef {
                        family: RecordFamily::System,
                        key: member.clone(),
                        predicate: Predicate::NotExists,
                    });
                    mutations.push(Mutation {
                        family: RecordFamily::System,
                        key: member,
                        op: MutationOp::Put,
                        value: Some(Value(Vec::new())),
                    });
                }
                let mut source_components = context.source_root_components.clone();
                source_components.extend(relative_components.iter().cloned());
                let source_index_key = path_index_key(self.mount, &source_components);
                let source_index = self.metadata.get_versioned(
                    RecordFamily::PathIndex,
                    &source_index_key,
                    read_version,
                    ReadPurpose::Snapshot,
                )?;
                let source_dentry = self.lookup_plus_at_version_for_purpose(
                    src_parent,
                    &child.dentry.name,
                    read_version,
                    ReadPurpose::Snapshot,
                )?;
                let source_shadow = self.metadata.get_versioned(
                    RecordFamily::ForkShadow,
                    &fork_shadow_key(self.mount, src_parent, &child.dentry.name),
                    read_version,
                    ReadPurpose::Snapshot,
                )?;
                let source_is_indexed = match source_dentry.as_ref() {
                    Some((canonical, dentry_version)) => {
                        let live_indexed = source_index
                            .as_ref()
                            .map(|index| {
                                let indexed = decode_dentry_projection(&index.value.0)
                                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                                Ok::<_, MetadError>(
                                    index.version == *dentry_version
                                        && DentryWithAttr::from(indexed) == *canonical
                                        && canonical == child,
                                )
                            })
                            .transpose()?
                            .unwrap_or(false);
                        let shadow_indexed = source_shadow
                            .as_ref()
                            .map(|shadow| {
                                let (_, indexed) = decode_restore_shadow_inverse(&shadow.value.0)?;
                                Ok::<_, MetadError>(
                                    DentryWithAttr::from(indexed) == *canonical
                                        && canonical == child,
                                )
                            })
                            .transpose()?
                            .unwrap_or(false);
                        live_indexed || shadow_indexed
                    }
                    None => false,
                };
                if source_is_indexed {
                    for marker in ancestor_markers.iter().chain(std::iter::once(&proj)) {
                        let shadow_key = restore_path_index_key(
                            self.mount,
                            context.base_ref_set_id,
                            marker.dentry.parent,
                            &marker.dentry.name,
                        );
                        if !planned_shadow_keys.insert(shadow_key.clone()) {
                            continue;
                        }
                        match self.metadata.get(
                            RecordFamily::System,
                            &shadow_key,
                            predecessor(commit_version)?,
                            ReadPurpose::WritePlanLocal,
                        )? {
                            Some(value) => {
                                let existing = decode_dentry_projection(&value.0)
                                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                                if existing.attr.inode != marker.attr.inode
                                    || existing.dentry.parent != marker.dentry.parent
                                    || existing.dentry.name != marker.dentry.name
                                {
                                    return Err(MetadError::Metadata(
                                        MetadataError::PredicateFailed,
                                    ));
                                }
                            }
                            None => {
                                predicates.push(PredicateRef {
                                    family: RecordFamily::System,
                                    key: shadow_key.clone(),
                                    predicate: Predicate::NotExists,
                                });
                                mutations.push(put_projection_mutation(
                                    RecordFamily::System,
                                    shadow_key,
                                    marker,
                                ));
                            }
                        }
                        let inverse_key =
                            fork_shadow_key(self.mount, marker.dentry.parent, &marker.dentry.name);
                        let inverse_value =
                            encode_restore_shadow_inverse(context.base_ref_set_id, marker);
                        match self.metadata.get_versioned(
                            RecordFamily::ForkShadow,
                            &inverse_key,
                            predecessor(commit_version)?,
                            ReadPurpose::WritePlanLocal,
                        )? {
                            Some(item) => {
                                if item.value.0 != inverse_value {
                                    return Err(MetadError::Metadata(
                                        MetadataError::PredicateFailed,
                                    ));
                                }
                            }
                            None => {
                                predicates.push(PredicateRef {
                                    family: RecordFamily::ForkShadow,
                                    key: inverse_key.clone(),
                                    predicate: Predicate::NotExists,
                                });
                                mutations.push(Mutation {
                                    family: RecordFamily::ForkShadow,
                                    key: inverse_key,
                                    op: MutationOp::Put,
                                    value: Some(Value(inverse_value)),
                                });
                            }
                        }
                    }
                }
            }
            if let Some(body) = &proj.body {
                mutations.push(Mutation {
                    family: RecordFamily::ChunkManifest,
                    key: chunk_manifest_key(
                        self.mount,
                        dst_inode,
                        body.generation,
                        BODY_SUMMARY_CHUNK_INDEX,
                    ),
                    op: MutationOp::Put,
                    value: Some(Value(encode_body_descriptor(body))),
                });
                for chunk in &chunks {
                    mutations.push(Mutation {
                        family: RecordFamily::ChunkManifest,
                        key: chunk_manifest_key(
                            self.mount,
                            dst_inode,
                            body.generation,
                            chunk.chunk_index,
                        ),
                        op: MutationOp::Put,
                        value: Some(Value(encode_chunk_manifest(chunk))),
                    });
                }
            }
            xattr_copies.push((child.attr.inode, dst_inode));
            if child.attr.file_type == FileType::Directory {
                let mut child_ancestors = ancestor_markers.to_vec();
                child_ancestors.push(proj.clone());
                sub_frames.push(CloneFrame {
                    src_inode: child.attr.inode,
                    dst_inode,
                    relative_components,
                    ancestor_markers: child_ancestors,
                });
            }
        }

        let command = MetadataCommand {
            request_id: request_id(
                b"clone-subtree-batch",
                self.mount,
                dst_parent,
                commit_version,
            ),
            kind: CommandKind::StageDetachedRestore,
            read_version: predecessor(commit_version)?,
            commit_version,
            primary_family: RecordFamily::Dentry,
            primary_key: dentry_prefix(self.mount, dst_parent),
            predicates,
            mutations,
            watch: Vec::new(),
        };
        validate_restore_command_bounds(&command, "detached clone batch")?;
        self.commit_metadata(command)?;

        for (src, dst) in xattr_copies {
            self.copy_inode_xattrs(src, dst, read_version)?;
        }

        Ok(sub_frames)
    }

    fn copy_inode_xattrs(
        &self,
        src_inode: InodeId,
        dst_inode: InodeId,
        version: Version,
    ) -> Result<(), MetadError> {
        let prefix = xattr_prefix(self.mount, src_inode);
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::Xattr,
            prefix: prefix.clone(),
            start_after: None,
            version,
            limit: 0,
            purpose: ReadPurpose::Snapshot,
        })?;
        for chunk in rows.chunks(128) {
            let object_reference = self.begin_object_reference_mutation()?;
            let commit_version = self.next_version()?;
            let mut predicates = vec![
                object_reference.predicate(self.mount),
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, dst_inode),
                    predicate: Predicate::Exists,
                },
            ];
            let mut mutations = Vec::with_capacity(chunk.len());
            for row in chunk {
                let name = row.key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                    MetadError::Codec("source xattr escaped its inode prefix".to_owned())
                })?;
                let key = xattr_key(self.mount, dst_inode, name);
                predicates.push(PredicateRef {
                    family: RecordFamily::Xattr,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                });
                mutations.push(Mutation {
                    family: RecordFamily::Xattr,
                    key,
                    op: MutationOp::Put,
                    value: Some(row.value.clone()),
                });
            }
            let command = MetadataCommand {
                request_id: request_id(
                    b"clone-detached-xattrs",
                    self.mount,
                    dst_inode,
                    commit_version,
                ),
                kind: CommandKind::StageDetachedRestore,
                read_version: predecessor(commit_version)?,
                commit_version,
                primary_family: RecordFamily::Xattr,
                primary_key: xattr_prefix(self.mount, dst_inode),
                predicates,
                mutations,
                watch: Vec::new(),
            };
            validate_restore_command_bounds(&command, "detached xattr batch")?;
            self.commit_metadata(command)?;
        }
        Ok(())
    }
}

/// Two non-directory entries are equivalent when they have the same type and, for
/// content-bearing nodes, the same size, content generation, and digest. A
/// divergent write bumps the generation, so a rewritten file is never equivalent
/// to its shared origin.
/// The content digest of an entry's body, if it has one.
fn entry_digest(entry: &DentryWithAttr) -> Option<String> {
    entry.body.as_ref().map(|body| body.digest_uri.clone())
}

fn entries_equivalent(a: &DentryWithAttr, b: &DentryWithAttr) -> bool {
    if a.attr.file_type != b.attr.file_type {
        return false;
    }
    if a.attr.size != b.attr.size || a.attr.generation != b.attr.generation {
        return false;
    }
    match (&a.body, &b.body) {
        (Some(a_body), Some(b_body)) => {
            a_body.generation == b_body.generation && a_body.digest_uri == b_body.digest_uri
        }
        (None, None) => true,
        _ => false,
    }
}

fn child_path(prefix: &str, name: &[u8]) -> Result<String, MetadError> {
    let name = std::str::from_utf8(name)
        .map_err(|_| MetadError::InvalidPath("subtree diff requires utf-8 names".to_owned()))?;
    Ok(format!("{prefix}/{name}"))
}

fn restore_relative_components(path: &str) -> Result<Vec<DentryName>, MetadError> {
    if path.is_empty() || path.starts_with('/') {
        return Err(MetadError::InvalidPath(format!(
            "restore initialization path must be non-empty and relative: {path}"
        )));
    }
    parse_absolute_path(&format!("/{path}")).map_err(Into::into)
}

fn canonical_restore_relative_path(path: &str) -> Result<String, MetadError> {
    let components = restore_relative_components(path)?;
    let mut canonical = String::new();
    for (index, component) in components.iter().enumerate() {
        if index != 0 {
            canonical.push('/');
        }
        canonical.push_str(std::str::from_utf8(component.as_bytes()).map_err(|_| {
            MetadError::InvalidPath("restore initialization path is not utf-8".to_owned())
        })?);
    }
    Ok(canonical)
}

fn canonical_restore_initialization(
    mut initialization: RestoreInitialization,
) -> Result<(RestoreInitialization, [u8; 32]), MetadError> {
    const MAX_INITIALIZATION_ENTRIES: usize = 1024;
    const MAX_INITIALIZATION_BYTES: usize = 8 * 1024 * 1024;
    const MAX_RELATIVE_PATH_BYTES: usize = 4096;
    if initialization
        .remove_relative_paths
        .len()
        .saturating_add(initialization.files.len())
        > MAX_INITIALIZATION_ENTRIES
    {
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore initialization entries".to_owned(),
            limit: MAX_INITIALIZATION_ENTRIES as u64,
            actual: initialization
                .remove_relative_paths
                .len()
                .saturating_add(initialization.files.len()) as u64,
        });
    }
    for path in &mut initialization.remove_relative_paths {
        *path = canonical_restore_relative_path(path)?;
    }
    for file in &mut initialization.files {
        file.relative_path = canonical_restore_relative_path(&file.relative_path)?;
    }
    initialization.remove_relative_paths.sort();
    initialization
        .files
        .sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    let total_bytes = initialization
        .remove_relative_paths
        .iter()
        .map(String::len)
        .chain(initialization.files.iter().map(|file| {
            file.relative_path
                .len()
                .saturating_add(file.bytes.len())
                .saturating_add(file.content_type.len())
        }))
        .fold(0usize, usize::saturating_add);
    if total_bytes > MAX_INITIALIZATION_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore initialization bytes".to_owned(),
            limit: MAX_INITIALIZATION_BYTES as u64,
            actual: total_bytes as u64,
        });
    }
    if initialization
        .remove_relative_paths
        .iter()
        .chain(initialization.files.iter().map(|file| &file.relative_path))
        .any(|path| path.len() > MAX_RELATIVE_PATH_BYTES)
    {
        let actual = initialization
            .remove_relative_paths
            .iter()
            .chain(initialization.files.iter().map(|file| &file.relative_path))
            .map(String::len)
            .max()
            .unwrap_or(0);
        return Err(MetadError::RestoreResourceLimit {
            resource: "restore initialization relative path bytes".to_owned(),
            limit: MAX_RELATIVE_PATH_BYTES as u64,
            actual: actual as u64,
        });
    }
    if initialization
        .remove_relative_paths
        .windows(2)
        .any(|paths| paths[0] == paths[1])
        || initialization
            .files
            .windows(2)
            .any(|files| files[0].relative_path == files[1].relative_path)
    {
        return Err(MetadError::InvalidPath(
            "restore initialization contains duplicate paths".to_owned(),
        ));
    }
    let mut all_paths = initialization
        .remove_relative_paths
        .iter()
        .chain(initialization.files.iter().map(|file| &file.relative_path))
        .collect::<Vec<_>>();
    all_paths.sort();
    if all_paths.windows(2).any(|paths| {
        paths[1]
            .as_bytes()
            .strip_prefix(paths[0].as_bytes())
            .is_some_and(|suffix| suffix.first() == Some(&b'/'))
    }) {
        return Err(MetadError::InvalidPath(
            "restore initialization paths cannot overlap as ancestor and descendant".to_owned(),
        ));
    }
    let removals = initialization
        .remove_relative_paths
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if initialization
        .files
        .iter()
        .any(|file| removals.contains(file.relative_path.as_str()))
    {
        return Err(MetadError::InvalidPath(
            "restore initialization cannot remove and write the same path".to_owned(),
        ));
    }

    fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-restore-initialization-v1\0");
    hasher.update((initialization.remove_relative_paths.len() as u64).to_be_bytes());
    for path in &initialization.remove_relative_paths {
        hash_bytes(&mut hasher, path.as_bytes());
    }
    hasher.update((initialization.files.len() as u64).to_be_bytes());
    for file in &initialization.files {
        hash_bytes(&mut hasher, file.relative_path.as_bytes());
        hash_bytes(&mut hasher, &file.bytes);
        hash_bytes(&mut hasher, file.content_type.as_bytes());
        hasher.update(file.mode.to_be_bytes());
        hasher.update(file.uid.to_be_bytes());
        hasher.update(file.gid.to_be_bytes());
    }
    Ok((initialization, hasher.finalize().into()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RestoreCommandShape {
    pub(super) item_count: usize,
    pub(super) encoded_bytes: usize,
}

pub(super) fn restore_command_shape(command: &MetadataCommand) -> RestoreCommandShape {
    let item_count = command
        .predicates
        .len()
        .saturating_add(command.mutations.len())
        .saturating_add(command.watch.len());
    let mut encoded_bytes = command
        .request_id
        .len()
        .saturating_add(command.primary_key.len());
    for predicate in &command.predicates {
        encoded_bytes = encoded_bytes
            .saturating_add(predicate.key.len())
            .saturating_add(24);
    }
    for mutation in &command.mutations {
        encoded_bytes = encoded_bytes
            .saturating_add(mutation.key.len())
            .saturating_add(mutation.value.as_ref().map_or(0, |value| value.0.len()))
            .saturating_add(24);
    }
    for projection in &command.watch {
        encoded_bytes = encoded_bytes
            .saturating_add(projection.key.len())
            .saturating_add(projection.event.len())
            .saturating_add(16);
    }
    RestoreCommandShape {
        item_count,
        encoded_bytes,
    }
}

pub(super) fn validate_restore_command_bounds(
    command: &MetadataCommand,
    resource: &str,
) -> Result<(), MetadError> {
    let shape = restore_command_shape(command);
    if shape.item_count > MAX_RESTORE_COMMAND_ITEMS {
        return Err(MetadError::RestoreResourceLimit {
            resource: format!("{resource} items"),
            limit: MAX_RESTORE_COMMAND_ITEMS as u64,
            actual: shape.item_count as u64,
        });
    }
    if shape.encoded_bytes > MAX_RESTORE_COMMAND_BYTES {
        return Err(MetadError::RestoreResourceLimit {
            resource: format!("{resource} encoded bytes"),
            limit: MAX_RESTORE_COMMAND_BYTES as u64,
            actual: shape.encoded_bytes as u64,
        });
    }
    Ok(())
}

pub(super) fn check_restore_resource(
    resource: &str,
    limit: usize,
    actual: usize,
) -> Result<(), MetadError> {
    if actual <= limit {
        return Ok(());
    }
    Err(MetadError::RestoreResourceLimit {
        resource: resource.to_owned(),
        limit: limit as u64,
        actual: actual as u64,
    })
}

fn is_deterministic_restore_preflight_error(error: &MetadError) -> bool {
    matches!(
        error,
        MetadError::InvalidPath(_)
            | MetadError::NotFound
            | MetadError::NotDirectory
            | MetadError::NotFile
            | MetadError::RestoreHardlinkUnsupported { .. }
            | MetadError::RestoreCrossShardUnsupported { .. }
            | MetadError::RestoreResourceLimit { .. }
    )
}
