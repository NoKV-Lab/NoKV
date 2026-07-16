//! Exact borrowed-object references and bounded restore release.

use super::restore::{
    decode_restore_operation, encode_restore_operation, restore_barrier_operation_id,
    restore_base_inverse_key, restore_base_inverse_prefix, restore_base_owner_key,
    restore_base_seal_key, restore_operation_key, RestoreOperation, RestoreOperationState,
};
use super::*;

const BASE_REFERENCE_FORMAT_VERSION: u8 = 1;
const BASE_REFERENCE_BUILD_FORMAT_VERSION: u8 = 3;
const BASE_REFERENCE_BATCH: usize = 64;
const BASE_MEMBER_SCAN_BATCH: usize = 64;
const BASE_CHUNK_SCAN_BATCH: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreBaseReference {
    pub(super) operation_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) borrower_inode: InodeId,
    pub(super) borrower_generation: u64,
    pub(super) object_key: String,
    pub(super) digest_uri: String,
    pub(super) size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreBaseSeal {
    pub(super) operation_digest: [u8; 32],
    pub(super) initialization_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) incarnation: u64,
    pub(super) reference_count: u64,
    pub(super) reference_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreBaseBuild {
    pub(super) operation_digest: [u8; 32],
    pub(super) initialization_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) incarnation: u64,
    pub(super) completed_member: Option<InodeId>,
    pub(super) active_member: Option<InodeId>,
    pub(super) chunk_cursor: Option<u64>,
    pub(super) object_cursor: Option<[u8; 32]>,
    pub(super) reference_count: u64,
    pub(super) reference_digest: [u8; 32],
    pub(super) batch_index: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum RestoreBaseSealRecord {
    Building(RestoreBaseBuild),
    Sealed(RestoreBaseSeal),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RestoreBaseInverse {
    pub(super) operation_digest: [u8; 32],
    pub(super) ref_set_id: u64,
    pub(super) object_digest: [u8; 32],
    pub(super) borrower_inode: InodeId,
    pub(super) borrower_generation: u64,
}

#[derive(Debug)]
struct RestoreBaseReferencePage {
    next: RestoreBaseBuild,
    references: Vec<RestoreBaseReference>,
    member_predicates: Vec<PredicateRef>,
    complete: bool,
}

#[derive(Debug)]
pub(super) struct RestoreMemberBaseReferenceLayout {
    borrower_generation: u64,
    generation_chain: Vec<u64>,
    pub(super) end_chunk: u64,
}

#[derive(Debug)]
struct RestoreChunkReferenceCache {
    inode: InodeId,
    chunk_index: u64,
    references: Vec<([u8; 32], RestoreBaseReference)>,
}

pub(super) struct RestoreBaseReferencePreflight {
    build: RestoreBaseBuild,
    pending: Vec<RestoreBaseReference>,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub(super) fn begin_restore_base_reference_preflight(
        &self,
        operation: &RestoreOperation,
    ) -> RestoreBaseReferencePreflight {
        RestoreBaseReferencePreflight {
            build: RestoreBaseBuild {
                operation_digest: operation.operation_digest,
                initialization_digest: operation.initialization_digest,
                ref_set_id: operation.ref_set_id,
                incarnation: operation.created_version,
                completed_member: None,
                active_member: None,
                chunk_cursor: None,
                object_cursor: None,
                reference_count: 0,
                reference_digest: initial_restore_base_reference_digest(),
                batch_index: 0,
            },
            pending: Vec::with_capacity(BASE_REFERENCE_BATCH),
        }
    }

    pub(super) fn preflight_restore_base_references_for_entry(
        &self,
        operation: &RestoreOperation,
        borrower_inode: InodeId,
        borrower_generation: u64,
        owned_object_inode: Option<InodeId>,
        chunks: &[ChunkManifest],
        preflight: &mut RestoreBaseReferencePreflight,
    ) -> Result<(), MetadError> {
        for manifest in chunks {
            let manifest_len =
                usize::try_from(manifest.len).map_err(|_| ObjectError::InvalidRange)?;
            let plan = plan_chunk_manifest_reads(
                std::slice::from_ref(manifest),
                manifest.logical_offset,
                manifest_len,
            )?;
            let mut selected = BTreeMap::<[u8; 32], RestoreBaseReference>::new();
            for block in plan.blocks {
                if owned_object_inode.is_some_and(|inode| {
                    self.owns_block_object_key(inode, borrower_generation, &block.object_key)
                }) {
                    continue;
                }
                let (_, _, object_chunk, _) =
                    self.canonical_block_object_identity(&block.object_key)?;
                if object_chunk != manifest.chunk_index {
                    return Err(MetadError::Codec(
                        "restore preflight object changed chunk identity".to_owned(),
                    ));
                }
                let digest: [u8; 32] = Sha256::digest(block.object_key.as_bytes()).into();
                let reference = RestoreBaseReference {
                    operation_digest: operation.operation_digest,
                    ref_set_id: operation.ref_set_id,
                    borrower_inode,
                    borrower_generation,
                    object_key: block.object_key,
                    digest_uri: block.digest_uri,
                    size: block.object_len,
                };
                if let Some(existing) = selected.get(&digest) {
                    if existing != &reference {
                        return Err(MetadError::Codec(
                            "restore preflight found an inconsistent object identity".to_owned(),
                        ));
                    }
                    continue;
                }
                selected.insert(digest, reference);
            }
            for reference in selected.into_values() {
                preflight.pending.push(reference);
                while preflight.pending.len() >= BASE_REFERENCE_BATCH {
                    self.flush_restore_base_reference_preflight(operation, preflight)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn finish_restore_base_reference_preflight(
        &self,
        operation: &RestoreOperation,
        preflight: &mut RestoreBaseReferencePreflight,
    ) -> Result<(), MetadError> {
        while !preflight.pending.is_empty() {
            self.flush_restore_base_reference_preflight(operation, preflight)?;
        }
        Ok(())
    }

    fn flush_restore_base_reference_preflight(
        &self,
        operation: &RestoreOperation,
        preflight: &mut RestoreBaseReferencePreflight,
    ) -> Result<(), MetadError> {
        if preflight.pending.is_empty() {
            return Ok(());
        }
        if preflight.build.operation_digest != operation.operation_digest
            || preflight.build.initialization_digest != operation.initialization_digest
            || preflight.build.ref_set_id != operation.ref_set_id
            || preflight.build.incarnation != operation.created_version
        {
            return Err(MetadError::Codec(
                "restore base-reference preflight changed identity".to_owned(),
            ));
        }
        let take = preflight.pending.len().min(BASE_REFERENCE_BATCH);
        let references = preflight.pending[..take].to_vec();
        let last = references.last().ok_or_else(|| {
            MetadError::Codec("restore base-reference preflight made no progress".to_owned())
        })?;
        let (_, _, chunk_index, _) = self.canonical_block_object_identity(&last.object_key)?;
        let mut next = preflight.build.clone();
        next.active_member = Some(last.borrower_inode);
        next.chunk_cursor = Some(chunk_index);
        next.object_cursor = Some(Sha256::digest(last.object_key.as_bytes()).into());
        // Runtime may carry proof for up to 64 scanned staging members,
        // including empty members before the first reference. Pad preflight to
        // that exact worst-case predicate cardinality so the hold cannot land
        // on a source whose first real reference page is a few KiB larger.
        let mut member_predicates = Vec::with_capacity(BASE_MEMBER_SCAN_BATCH);
        for index in 0..BASE_MEMBER_SCAN_BATCH {
            let inode = InodeId::new(u64::MAX - index as u64)?;
            let key =
                super::restore::restore_staging_member_key(self.mount, operation.ref_set_id, inode);
            member_predicates.push(PredicateRef {
                family: RecordFamily::System,
                key,
                predicate: Predicate::VersionEquals(Version::new(2)?),
            });
        }
        let page = RestoreBaseReferencePage {
            next,
            references,
            member_predicates,
            complete: false,
        };
        let bounds_version = Version::new(2)?;
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let seal_key = restore_base_seal_key(self.mount, operation.ref_set_id);
        let fitted = self.fit_restore_base_reference_page(
            operation,
            bounds_version,
            &binding_key,
            &seal_key,
            Version::new(1)?,
            &preflight.build,
            page,
            true,
        )?;
        let accepted = fitted.references.len();
        let (command, next, _) = self.build_restore_base_reference_page_command(
            operation,
            bounds_version,
            &binding_key,
            &seal_key,
            Version::new(1)?,
            ObjectReferenceMutation::from_version(Version::new(1)?),
            bounds_version,
            &fitted,
        )?;
        super::restore::validate_restore_command_bounds(&command, "restore base-reference batch")?;
        preflight.build = next;
        preflight.pending.drain(..accepted);
        Ok(())
    }

    pub(super) fn seal_restore_base_references(
        &self,
        operation: &RestoreOperation,
    ) -> Result<(), MetadError> {
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let seal_key = restore_base_seal_key(self.mount, operation.ref_set_id);
        let operation_version = Version::new(operation.created_version)?;
        let mut chunk_cache = None;
        loop {
            let read_version = self.read_version()?;
            let record = self.metadata.get_versioned(
                RecordFamily::System,
                &seal_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?;
            let Some(record) = record else {
                let build = RestoreBaseBuild {
                    operation_digest: operation.operation_digest,
                    initialization_digest: operation.initialization_digest,
                    ref_set_id: operation.ref_set_id,
                    incarnation: operation.created_version,
                    completed_member: None,
                    active_member: None,
                    chunk_cursor: None,
                    object_cursor: None,
                    reference_count: 0,
                    reference_digest: initial_restore_base_reference_digest(),
                    batch_index: 0,
                };
                let object_reference = self.begin_object_reference_mutation()?;
                let version = self.next_version()?;
                let command = MetadataCommand {
                    request_id: request_id(
                        b"restore-start-base-refs",
                        self.mount,
                        operation.destination_root,
                        version,
                    ),
                    kind: CommandKind::CleanupObjects,
                    read_version: predecessor(version)?,
                    commit_version: version,
                    primary_family: RecordFamily::System,
                    primary_key: seal_key.clone(),
                    predicates: vec![
                        object_reference.predicate(self.mount),
                        PredicateRef {
                            family: RecordFamily::System,
                            key: operation_key.clone(),
                            predicate: Predicate::VersionEquals(operation_version),
                        },
                        PredicateRef {
                            family: RecordFamily::ForkBinding,
                            key: binding_key.clone(),
                            predicate: Predicate::VersionEquals(operation_version),
                        },
                        PredicateRef {
                            family: RecordFamily::System,
                            key: seal_key.clone(),
                            predicate: Predicate::NotExists,
                        },
                    ],
                    mutations: vec![Mutation {
                        family: RecordFamily::System,
                        key: seal_key.clone(),
                        op: MutationOp::Put,
                        value: Some(Value(encode_restore_base_build(&build)?)),
                    }],
                    watch: Vec::new(),
                };
                super::restore::validate_restore_command_bounds(
                    &command,
                    "restore base-reference build start",
                )?;
                self.commit_metadata(command)?;
                continue;
            };
            match decode_restore_base_seal_record(&record.value.0)? {
                RestoreBaseSealRecord::Sealed(seal) => {
                    validate_restore_base_seal(operation, &seal)?;
                    return Ok(());
                }
                RestoreBaseSealRecord::Building(build) => {
                    validate_restore_base_build(operation, &build)?;
                    let page = self.plan_restore_base_reference_page(
                        operation,
                        &build,
                        read_version,
                        &mut chunk_cache,
                    )?;
                    if page.complete && page.next == build {
                        self.finish_restore_base_reference_seal(
                            operation,
                            operation_version,
                            &binding_key,
                            &seal_key,
                            record.version,
                            &build,
                        )?;
                        return Ok(());
                    }
                    let page = self.fit_restore_base_reference_page(
                        operation,
                        operation_version,
                        &binding_key,
                        &seal_key,
                        record.version,
                        &build,
                        page,
                        false,
                    )?;
                    self.persist_restore_base_reference_page(
                        operation,
                        operation_version,
                        &binding_key,
                        &seal_key,
                        record.version,
                        page,
                    )?;
                }
            }
        }
    }

    pub(super) fn restore_base_seal_predicate(
        &self,
        operation: &RestoreOperation,
        version: Version,
    ) -> Result<PredicateRef, MetadError> {
        let seal_key = restore_base_seal_key(self.mount, operation.ref_set_id);
        let item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &seal_key,
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore base-reference seal is missing".to_owned())
            })?;
        let seal = decode_restore_base_seal(&item.value.0)?;
        validate_restore_base_seal(operation, &seal)?;
        Ok(PredicateRef {
            family: RecordFamily::System,
            key: seal_key,
            predicate: Predicate::VersionEquals(item.version),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_restore_base_reference_seal(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        binding_key: &[u8],
        seal_key: &[u8],
        build_version: Version,
        build: &RestoreBaseBuild,
    ) -> Result<(), MetadError> {
        let seal = RestoreBaseSeal {
            operation_digest: operation.operation_digest,
            initialization_digest: operation.initialization_digest,
            ref_set_id: operation.ref_set_id,
            incarnation: operation.created_version,
            reference_count: build.reference_count,
            reference_digest: finalize_restore_base_reference_digest(
                build.reference_count,
                build.reference_digest,
            ),
        };
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-seal-base-refs",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: seal_key.to_vec(),
            predicates: vec![
                object_reference.predicate(self.mount),
                PredicateRef {
                    family: RecordFamily::System,
                    key: restore_operation_key(self.mount, &operation.operation_digest),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::ForkBinding,
                    key: binding_key.to_vec(),
                    predicate: Predicate::VersionEquals(operation_version),
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: seal_key.to_vec(),
                    predicate: Predicate::VersionEquals(build_version),
                },
            ],
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: seal_key.to_vec(),
                op: MutationOp::Put,
                value: Some(Value(encode_restore_base_seal(&seal))),
            }],
            watch: Vec::new(),
        };
        super::restore::validate_restore_command_bounds(&command, "restore base seal")?;
        self.commit_metadata(command)?;
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(operation),
            live_test_barrier::RestoreAppliedPhase::ReferencesSealed,
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_restore_base_reference_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        binding_key: &[u8],
        seal_key: &[u8],
        build_version: Version,
        page: RestoreBaseReferencePage,
    ) -> Result<(), MetadError> {
        // Another restore's release removes its owner/inverse projections
        // atomically while holding object_gc_gate. Keep the cross-ref-set
        // identity read and this bounded page commit under the same gate so a
        // prefix scan cannot observe the old inverse immediately before its
        // canonical owner disappears. The GC-claim predicate remains the
        // durable failover fence; this mutex only supplies local multi-key
        // read consistency.
        let _gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let read_version = self.read_version()?;
        self.validate_restore_base_reference_identities(operation, &page.references, read_version)?;
        let object_reference = self.begin_object_reference_mutation()?;
        let version = self.next_version()?;
        let (command, next, batch_index) = self.build_restore_base_reference_page_command(
            operation,
            operation_version,
            binding_key,
            seal_key,
            build_version,
            object_reference,
            version,
            &page,
        )?;
        super::restore::validate_restore_command_bounds(&command, "restore base-reference batch")?;
        let encoded_next = encode_restore_base_build(&next)?;
        match self.commit_metadata(command) {
            Ok(_) => {}
            Err(error @ MetadError::Metadata(MetadataError::Backend(_))) => {
                let applied = self
                    .metadata
                    .get(
                        RecordFamily::System,
                        seal_key,
                        self.read_version()?,
                        ReadPurpose::WritePlanLocal,
                    )?
                    .is_some_and(|value| value.0 == encoded_next);
                if !applied {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
        live_test_barrier::restore_applied(
            &restore_barrier_operation_id(operation),
            live_test_barrier::RestoreAppliedPhase::ReferenceBatch(batch_index),
        )?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_restore_base_reference_page_command(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        binding_key: &[u8],
        seal_key: &[u8],
        build_version: Version,
        object_reference: ObjectReferenceMutation,
        version: Version,
        page: &RestoreBaseReferencePage,
    ) -> Result<(MetadataCommand, RestoreBaseBuild, u64), MetadError> {
        let mut next = page.next.clone();
        for reference in &page.references {
            next.reference_digest =
                extend_restore_base_reference_digest(next.reference_digest, reference)?;
            next.reference_count = next.reference_count.checked_add(1).ok_or_else(|| {
                MetadError::Codec("restore base-reference count overflow".to_owned())
            })?;
        }
        let batch_index = next.batch_index;
        next.batch_index = next
            .batch_index
            .checked_add(1)
            .ok_or_else(|| MetadError::Codec("restore base-reference batch overflow".to_owned()))?;
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: restore_operation_key(self.mount, &operation.operation_digest),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key.to_vec(),
                predicate: Predicate::VersionEquals(operation_version),
            },
            PredicateRef {
                family: RecordFamily::System,
                key: seal_key.to_vec(),
                predicate: Predicate::VersionEquals(build_version),
            },
        ];
        predicates.extend(page.member_predicates.iter().cloned());
        let mut mutations = Vec::with_capacity(page.references.len() * 3 + 1);
        for reference in &page.references {
            let object_digest: [u8; 32] = Sha256::digest(reference.object_key.as_bytes()).into();
            let owner_key = restore_base_owner_key(
                self.mount,
                operation.ref_set_id,
                &object_digest,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            let inverse_key = restore_base_inverse_key(
                self.mount,
                &object_digest,
                operation.ref_set_id,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            let inverse_owner_key = super::restore::restore_base_inverse_owner_key(
                self.mount,
                operation.ref_set_id,
                &object_digest,
                reference.borrower_inode,
                reference.borrower_generation,
            );
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::System,
                    key: owner_key.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_key.clone(),
                    predicate: Predicate::NotExists,
                },
                PredicateRef {
                    family: RecordFamily::System,
                    key: inverse_owner_key.clone(),
                    predicate: Predicate::NotExists,
                },
            ]);
            let inverse_value = encode_restore_base_inverse(&RestoreBaseInverse {
                operation_digest: operation.operation_digest,
                ref_set_id: operation.ref_set_id,
                object_digest,
                borrower_inode: reference.borrower_inode,
                borrower_generation: reference.borrower_generation,
            });
            mutations.extend([
                Mutation {
                    family: RecordFamily::System,
                    key: owner_key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_restore_base_reference(reference)?)),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: inverse_key,
                    op: MutationOp::Put,
                    value: Some(Value(inverse_value.clone())),
                },
                Mutation {
                    family: RecordFamily::System,
                    key: inverse_owner_key,
                    op: MutationOp::Put,
                    value: Some(Value(inverse_value)),
                },
            ]);
        }
        mutations.push(Mutation {
            family: RecordFamily::System,
            key: seal_key.to_vec(),
            op: MutationOp::Put,
            value: Some(Value(encode_restore_base_build(&next)?)),
        });
        Ok((
            MetadataCommand {
                request_id: request_id(
                    b"restore-persist-base-refs",
                    self.mount,
                    operation.destination_root,
                    version,
                ),
                kind: CommandKind::CleanupObjects,
                read_version: predecessor(version)?,
                commit_version: version,
                primary_family: RecordFamily::System,
                primary_key: super::restore::restore_base_owner_prefix(
                    self.mount,
                    operation.ref_set_id,
                ),
                predicates,
                mutations,
                watch: Vec::new(),
            },
            next,
            batch_index,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn fit_restore_base_reference_page(
        &self,
        operation: &RestoreOperation,
        operation_version: Version,
        binding_key: &[u8],
        seal_key: &[u8],
        build_version: Version,
        build: &RestoreBaseBuild,
        page: RestoreBaseReferencePage,
        retain_all_member_predicates: bool,
    ) -> Result<RestoreBaseReferencePage, MetadError> {
        let bounds_object_reference = ObjectReferenceMutation::from_version(Version::new(1)?);
        let bounds_version = Version::new(2)?;
        let full = self.build_restore_base_reference_page_command(
            operation,
            operation_version,
            binding_key,
            seal_key,
            build_version,
            bounds_object_reference,
            bounds_version,
            &page,
        )?;
        match super::restore::validate_restore_command_bounds(
            &full.0,
            "restore base-reference batch",
        ) {
            Ok(()) => return Ok(page),
            Err(error) if page.references.len() <= 1 => return Err(error),
            Err(_) => {}
        }

        for accepted in (1..page.references.len()).rev() {
            let last = &page.references[accepted - 1];
            let (_, _, chunk_index, _) = self.canonical_block_object_identity(&last.object_key)?;
            let mut next = build.clone();
            next.active_member = Some(last.borrower_inode);
            next.chunk_cursor = Some(chunk_index);
            next.object_cursor = Some(Sha256::digest(last.object_key.as_bytes()).into());
            let member_predicates = if retain_all_member_predicates {
                page.member_predicates.clone()
            } else {
                let member_cursor = super::restore::restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    last.borrower_inode,
                );
                page.member_predicates
                    .iter()
                    .filter(|predicate| predicate.key.as_slice() <= member_cursor.as_slice())
                    .cloned()
                    .collect()
            };
            let candidate = RestoreBaseReferencePage {
                next,
                references: page.references[..accepted].to_vec(),
                member_predicates,
                complete: false,
            };
            let command = self.build_restore_base_reference_page_command(
                operation,
                operation_version,
                binding_key,
                seal_key,
                build_version,
                bounds_object_reference,
                bounds_version,
                &candidate,
            )?;
            match super::restore::validate_restore_command_bounds(
                &command.0,
                "restore base-reference batch",
            ) {
                Ok(()) => return Ok(candidate),
                Err(error) if accepted == 1 => return Err(error),
                Err(_) => {}
            }
        }
        Err(MetadError::Codec(
            "restore base-reference packer made no progress".to_owned(),
        ))
    }

    fn plan_restore_base_reference_page(
        &self,
        operation: &RestoreOperation,
        build: &RestoreBaseBuild,
        version: Version,
        chunk_cache: &mut Option<RestoreChunkReferenceCache>,
    ) -> Result<RestoreBaseReferencePage, MetadError> {
        let mut next = build.clone();
        let mut references = Vec::with_capacity(BASE_REFERENCE_BATCH);
        let mut member_predicates = Vec::with_capacity(BASE_MEMBER_SCAN_BATCH);
        let mut members_examined = 0_usize;
        let mut chunks_examined = 0_usize;
        loop {
            if references.len() == BASE_REFERENCE_BATCH
                || members_examined == BASE_MEMBER_SCAN_BATCH
                || chunks_examined == BASE_CHUNK_SCAN_BATCH
            {
                return Ok(RestoreBaseReferencePage {
                    next,
                    references,
                    member_predicates,
                    complete: false,
                });
            }
            let active = next.active_member;
            let rows = if let Some(active) = active {
                let key = super::restore::restore_staging_member_key(
                    self.mount,
                    operation.ref_set_id,
                    active,
                );
                let item = self
                    .metadata
                    .get_versioned(
                        RecordFamily::System,
                        &key,
                        version,
                        ReadPurpose::RestoreStaging,
                    )?
                    .ok_or(MetadError::RestoreRootChanged { root: active })?;
                vec![crate::command::ScanItem {
                    key,
                    value: item.value,
                    version: item.version,
                }]
            } else {
                let prefix =
                    super::restore::restore_staging_member_prefix(self.mount, operation.ref_set_id);
                self.metadata.scan(ScanRequest {
                    family: RecordFamily::System,
                    prefix,
                    start_after: next.completed_member.map(|inode| {
                        super::restore::restore_staging_member_key(
                            self.mount,
                            operation.ref_set_id,
                            inode,
                        )
                    }),
                    version,
                    limit: BASE_MEMBER_SCAN_BATCH - members_examined,
                    purpose: ReadPurpose::RestoreStaging,
                })?
            };
            if rows.is_empty() {
                return Ok(RestoreBaseReferencePage {
                    next,
                    references,
                    member_predicates,
                    complete: true,
                });
            }
            let requested = BASE_MEMBER_SCAN_BATCH - members_examined;
            let reached_tail = active.is_none() && rows.len() < requested;
            for row in rows {
                let member = super::restore::decode_restore_staging_member(&row.value.0)?;
                if member.operation_digest != operation.operation_digest
                    || row.key
                        != super::restore::restore_staging_member_key(
                            self.mount,
                            operation.ref_set_id,
                            member.destination_inode,
                        )
                    || next
                        .completed_member
                        .is_some_and(|completed| member.destination_inode <= completed)
                    || active.is_some_and(|active| active != member.destination_inode)
                {
                    return Err(MetadError::RestoreRootChanged {
                        root: operation.destination_root,
                    });
                }
                member_predicates.push(PredicateRef {
                    family: RecordFamily::System,
                    key: row.key,
                    predicate: Predicate::VersionEquals(row.version),
                });
                members_examined += 1;

                let Some(layout) = self.restore_member_base_reference_layout(&member, version)?
                else {
                    next.completed_member = Some(member.destination_inode);
                    next.active_member = None;
                    next.chunk_cursor = None;
                    next.object_cursor = None;
                    *chunk_cache = None;
                    continue;
                };
                let mut chunk_index = if active == Some(member.destination_inode) {
                    next.chunk_cursor.ok_or_else(|| {
                        MetadError::Codec(
                            "restore base-reference build lost its chunk cursor".to_owned(),
                        )
                    })?
                } else {
                    0
                };
                if chunk_index > layout.end_chunk {
                    return Err(MetadError::Codec(
                        "restore base-reference chunk cursor exceeds borrower size".to_owned(),
                    ));
                }
                let mut after = if active == Some(member.destination_inode) {
                    next.object_cursor
                } else {
                    None
                };
                loop {
                    if references.len() == BASE_REFERENCE_BATCH
                        || chunks_examined == BASE_CHUNK_SCAN_BATCH
                    {
                        next.active_member = Some(member.destination_inode);
                        next.chunk_cursor = Some(chunk_index);
                        next.object_cursor = after;
                        return Ok(RestoreBaseReferencePage {
                            next,
                            references,
                            member_predicates,
                            complete: false,
                        });
                    }
                    if chunk_cache.as_ref().is_none_or(|cache| {
                        cache.inode != member.destination_inode || cache.chunk_index != chunk_index
                    }) {
                        *chunk_cache = Some(RestoreChunkReferenceCache {
                            inode: member.destination_inode,
                            chunk_index,
                            references: self.restore_member_base_references_for_chunk(
                                operation,
                                &member,
                                &layout,
                                chunk_index,
                                version,
                            )?,
                        });
                    }
                    let cached = &chunk_cache
                        .as_ref()
                        .expect("restore chunk cache installed")
                        .references;
                    let start = cached.partition_point(|(digest, _)| {
                        after.is_some_and(|cursor| *digest <= cursor)
                    });
                    let remaining = BASE_REFERENCE_BATCH - references.len();
                    let end = start.saturating_add(remaining).min(cached.len());
                    references.extend(
                        cached[start..end]
                            .iter()
                            .map(|(_, reference)| reference.clone()),
                    );
                    if end < cached.len() {
                        let digest =
                            cached
                                .get(end - 1)
                                .map(|(digest, _)| *digest)
                                .ok_or_else(|| {
                                    MetadError::Codec(
                                        "restore member reference page made no cursor progress"
                                            .to_owned(),
                                    )
                                })?;
                        next.active_member = Some(member.destination_inode);
                        next.chunk_cursor = Some(chunk_index);
                        next.object_cursor = Some(digest);
                        return Ok(RestoreBaseReferencePage {
                            next,
                            references,
                            member_predicates,
                            complete: false,
                        });
                    }

                    chunks_examined += 1;
                    after = None;
                    *chunk_cache = None;
                    if chunk_index == layout.end_chunk {
                        next.completed_member = Some(member.destination_inode);
                        next.active_member = None;
                        next.chunk_cursor = None;
                        next.object_cursor = None;
                        break;
                    }
                    chunk_index = chunk_index.checked_add(1).ok_or_else(|| {
                        MetadError::Codec("restore base-reference chunk cursor overflow".to_owned())
                    })?;
                    next.active_member = Some(member.destination_inode);
                    next.chunk_cursor = Some(chunk_index);
                    next.object_cursor = None;
                }
                if references.len() == BASE_REFERENCE_BATCH
                    || members_examined == BASE_MEMBER_SCAN_BATCH
                    || chunks_examined == BASE_CHUNK_SCAN_BATCH
                {
                    return Ok(RestoreBaseReferencePage {
                        next,
                        references,
                        member_predicates,
                        complete: false,
                    });
                }
            }
            if reached_tail {
                return Ok(RestoreBaseReferencePage {
                    next,
                    references,
                    member_predicates,
                    complete: true,
                });
            }
        }
    }

    pub(super) fn restore_member_base_reference_layout(
        &self,
        member: &super::restore::RestoreStagingMember,
        version: Version,
    ) -> Result<Option<RestoreMemberBaseReferenceLayout>, MetadError> {
        let attr = self
            .get_attr_at_version_for_purpose(
                member.destination_inode,
                version,
                ReadPurpose::RestoreStaging,
            )?
            .ok_or(MetadError::RestoreRootChanged {
                root: member.destination_inode,
            })?;
        let summary_key = chunk_manifest_key(
            self.mount,
            member.destination_inode,
            attr.generation,
            BODY_SUMMARY_CHUNK_INDEX,
        );
        let Some(body) = self.metadata.get(
            RecordFamily::ChunkManifest,
            &summary_key,
            version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(None);
        };
        let body = decode_body_descriptor(&body.0)
            .map_err(|error| MetadError::Codec(error.to_string()))?;
        if body.generation != attr.generation {
            return Err(MetadError::Codec(
                "restore borrower body generation changed identity".to_owned(),
            ));
        }
        if body.chunk_size == 0 || body.block_size == 0 {
            return Err(ObjectError::InvalidChunkLayout.into());
        }
        if body.size == 0 {
            return Ok(None);
        }
        Ok(Some(RestoreMemberBaseReferenceLayout {
            borrower_generation: body.generation,
            generation_chain: self.resolve_generation_chain(
                member.destination_inode,
                &body,
                version,
                ReadPurpose::RestoreStaging,
            )?,
            end_chunk: (body.size - 1) / body.chunk_size,
        }))
    }

    pub(super) fn restore_member_base_references_for_chunk(
        &self,
        operation: &RestoreOperation,
        member: &super::restore::RestoreStagingMember,
        layout: &RestoreMemberBaseReferenceLayout,
        chunk_index: u64,
        version: Version,
    ) -> Result<Vec<([u8; 32], RestoreBaseReference)>, MetadError> {
        if chunk_index > layout.end_chunk {
            return Err(MetadError::Codec(
                "restore borrower chunk exceeds effective body".to_owned(),
            ));
        }
        let Some(manifest) = self.chain_chunk_manifest(
            member.destination_inode,
            &layout.generation_chain,
            chunk_index,
            version,
            ReadPurpose::RestoreStaging,
        )?
        else {
            return Ok(Vec::new());
        };
        if manifest.chunk_index != chunk_index {
            return Err(MetadError::Codec(
                "restore borrower manifest changed chunk identity".to_owned(),
            ));
        }
        let manifest_len = usize::try_from(manifest.len).map_err(|_| ObjectError::InvalidRange)?;
        let plan = plan_chunk_manifest_reads(
            std::slice::from_ref(&manifest),
            manifest.logical_offset,
            manifest_len,
        )?;
        let mut selected = BTreeMap::<[u8; 32], RestoreBaseReference>::new();
        for block in plan.blocks {
            if self.owns_block_object_key(
                member.destination_inode,
                layout.borrower_generation,
                &block.object_key,
            ) {
                continue;
            }
            let (_, _, object_chunk, _) =
                self.canonical_block_object_identity(&block.object_key)?;
            if object_chunk != chunk_index {
                return Err(MetadError::Codec(
                    "restore borrower object changed chunk identity".to_owned(),
                ));
            }
            let digest: [u8; 32] = Sha256::digest(block.object_key.as_bytes()).into();
            let reference = RestoreBaseReference {
                operation_digest: operation.operation_digest,
                ref_set_id: operation.ref_set_id,
                borrower_inode: member.destination_inode,
                borrower_generation: layout.borrower_generation,
                object_key: block.object_key,
                digest_uri: block.digest_uri,
                size: block.object_len,
            };
            if let Some(existing) = selected.get(&digest) {
                if existing != &reference {
                    return Err(MetadError::Codec(
                        "restore object-key digest collision or inconsistent borrower".to_owned(),
                    ));
                }
                continue;
            }
            selected.insert(digest, reference);
        }
        Ok(selected.into_iter().collect())
    }

    fn validate_restore_base_reference_identities(
        &self,
        operation: &RestoreOperation,
        references: &[RestoreBaseReference],
        version: Version,
    ) -> Result<(), MetadError> {
        let mut batch = BTreeMap::new();
        for reference in references {
            let object_digest: [u8; 32] = Sha256::digest(reference.object_key.as_bytes()).into();
            let identity = (
                reference.object_key.clone(),
                self.canonical_block_object_identity(&reference.object_key)?,
                reference.digest_uri.clone(),
                reference.size,
            );
            if let Some(existing) = batch.insert(object_digest, identity.clone()) {
                if existing != identity {
                    return Err(MetadError::Codec(
                        "restore object-key digest collision or inconsistent object identity"
                            .to_owned(),
                    ));
                }
            }
            let inverse_rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::System,
                prefix: restore_base_inverse_prefix(self.mount, &object_digest),
                start_after: None,
                version,
                limit: 1,
                purpose: ReadPurpose::WritePlanLocal,
            })?;
            let Some(inverse_row) = inverse_rows.first() else {
                continue;
            };
            let inverse = decode_restore_base_inverse(&inverse_row.value.0)?;
            if inverse.object_digest != object_digest {
                return Err(MetadError::Codec(
                    "restore base inverse changed object identity".to_owned(),
                ));
            }
            let owner = self
                .metadata
                .get(
                    RecordFamily::System,
                    &restore_base_owner_key(
                        self.mount,
                        inverse.ref_set_id,
                        &object_digest,
                        inverse.borrower_inode,
                        inverse.borrower_generation,
                    ),
                    version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or_else(|| {
                    MetadError::Codec("restore base inverse has no canonical owner".to_owned())
                })?;
            let owner = decode_restore_base_reference(&owner.0)?;
            let owner_identity = (
                owner.object_key.clone(),
                self.canonical_block_object_identity(&owner.object_key)?,
                owner.digest_uri.clone(),
                owner.size,
            );
            if owner_identity != identity {
                return Err(MetadError::Codec(
                    "restore object-key digest collision or inconsistent object identity"
                        .to_owned(),
                ));
            }
            if owner.operation_digest == operation.operation_digest
                && owner.ref_set_id == operation.ref_set_id
                && owner.borrower_inode == reference.borrower_inode
                && owner.borrower_generation == reference.borrower_generation
            {
                return Err(MetadError::Codec(
                    "restore base-reference builder revisited a durable owner".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn mark_restore_ready_to_attach(
        &self,
        operation: &RestoreOperation,
    ) -> Result<(), MetadError> {
        let object_reference = self.begin_object_reference_mutation()?;
        let read_version = self.read_version()?;
        let operation_key = restore_operation_key(self.mount, &operation.operation_digest);
        let operation_item = self
            .metadata
            .get_versioned(
                RecordFamily::System,
                &operation_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreInProgress)?;
        let durable = decode_restore_operation(&operation_item.value.0)?;
        if durable != *operation || durable.state != RestoreOperationState::Preparing {
            return Err(MetadError::RestoreInProgress);
        }
        let binding_key = fork_binding_key(self.mount, operation.destination_root);
        let binding = self
            .metadata
            .get_versioned(
                RecordFamily::ForkBinding,
                &binding_key,
                read_version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            })?;
        let decoded_binding =
            crate::layout::decode_fork_binding(&binding.value.0).map_err(|_| {
                MetadError::RestoreBindingChanged {
                    root: operation.destination_root,
                }
            })?;
        if binding.version != Version::new(operation.created_version)?
            || decoded_binding.fork_root != operation.destination_root
            || decoded_binding.source_root != operation.source_root
            || decoded_binding.pinned_read_version != operation.read_version
            || decoded_binding.snapshot_id != operation.snapshot_id
            || decoded_binding.created_version != operation.created_version
        {
            return Err(MetadError::RestoreBindingChanged {
                root: operation.destination_root,
            });
        }
        let mut predicates = vec![
            object_reference.predicate(self.mount),
            PredicateRef {
                family: RecordFamily::System,
                key: operation_key.clone(),
                predicate: Predicate::VersionEquals(operation_item.version),
            },
            PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding_key,
                predicate: Predicate::VersionEquals(binding.version),
            },
            self.restore_base_seal_predicate(operation, read_version)?,
            self.restore_index_seal_predicate(operation, read_version)?,
        ];
        let mut ready = durable;
        ready.state = RestoreOperationState::ReadyToAttach;
        let version = self.next_version()?;
        let command = MetadataCommand {
            request_id: request_id(
                b"restore-ready-to-attach",
                self.mount,
                operation.destination_root,
                version,
            ),
            kind: CommandKind::CleanupObjects,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::System,
            primary_key: operation_key.clone(),
            predicates: std::mem::take(&mut predicates),
            mutations: vec![Mutation {
                family: RecordFamily::System,
                key: operation_key,
                op: MutationOp::Put,
                value: Some(Value(encode_restore_operation(&ready)?)),
            }],
            watch: Vec::new(),
        };
        super::restore::validate_restore_command_bounds(&command, "restore ready-to-attach")?;
        self.commit_metadata(command)?;
        Ok(())
    }

    /// Fail closed while any exact inverse row exists and is consistent with its
    /// owner and operation records. Corruption is surfaced to the generic GC
    /// worker, which quarantines the candidate instead of issuing DELETE.
    pub(super) fn restore_object_is_borrowed(&self, object_key: &str) -> Result<bool, MetadError> {
        let object_digest: [u8; 32] = Sha256::digest(object_key.as_bytes()).into();
        let version = self.read_version()?;
        let object_quarantine = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: super::restore::restore_release_object_quarantine_prefix(
                self.mount,
                &object_digest,
            ),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let mount_wide_quarantine = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: super::restore::restore_release_mount_wide_quarantine_prefix(self.mount),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        if !object_quarantine.is_empty() || !mount_wide_quarantine.is_empty() {
            // An object-scoped malformed owner row can hide this candidate's
            // only durable borrower identity. Mount-wide quarantine is reserved
            // for the rarer case where even the canonical owner-key digest is
            // unrecoverable. Diagnostic member/job quarantines do not block
            // unrelated object GC.
            return Ok(true);
        }
        let rows = self.metadata.scan(ScanRequest {
            family: RecordFamily::System,
            prefix: restore_base_inverse_prefix(self.mount, &object_digest),
            start_after: None,
            version,
            limit: 1,
            purpose: ReadPurpose::WritePlanLocal,
        })?;
        let Some(row) = rows.first() else {
            return Ok(false);
        };
        let inverse = decode_restore_base_inverse(&row.value.0)?;
        if inverse.object_digest != object_digest {
            return Err(MetadError::Codec(
                "restore inverse row object digest mismatch".to_owned(),
            ));
        }
        let owner_key = restore_base_owner_key(
            self.mount,
            inverse.ref_set_id,
            &inverse.object_digest,
            inverse.borrower_inode,
            inverse.borrower_generation,
        );
        let owner = self
            .metadata
            .get(
                RecordFamily::System,
                &owner_key,
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| MetadError::Codec("restore inverse row has no owner row".to_owned()))?;
        let owner = decode_restore_base_reference(&owner.0)?;
        if owner.operation_digest != inverse.operation_digest
            || owner.ref_set_id != inverse.ref_set_id
            || owner.borrower_inode != inverse.borrower_inode
            || owner.borrower_generation != inverse.borrower_generation
            || owner.object_key != object_key
        {
            return Err(MetadError::Codec(
                "restore inverse/owner row mismatch".to_owned(),
            ));
        }
        let inverse_owner_key = super::restore::restore_base_inverse_owner_key(
            self.mount,
            inverse.ref_set_id,
            &inverse.object_digest,
            inverse.borrower_inode,
            inverse.borrower_generation,
        );
        let inverse_owner = self
            .metadata
            .get(
                RecordFamily::System,
                &inverse_owner_key,
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore inverse has no ref-set owner row".to_owned())
            })?;
        if decode_restore_base_inverse(&inverse_owner.0)? != inverse {
            return Err(MetadError::Codec(
                "restore inverse/ref-set owner row mismatch".to_owned(),
            ));
        }
        let operation = self
            .metadata
            .get(
                RecordFamily::System,
                &restore_operation_key(self.mount, &inverse.operation_digest),
                version,
                ReadPurpose::WritePlanLocal,
            )?
            .ok_or_else(|| {
                MetadError::Codec("restore base reference has no operation".to_owned())
            })?;
        let operation = decode_restore_operation(&operation.0)?;
        if operation.ref_set_id != inverse.ref_set_id {
            return Err(MetadError::Codec(
                "restore base reference set does not match operation".to_owned(),
            ));
        }
        Ok(true)
    }
}

pub(super) fn initial_restore_base_reference_digest() -> [u8; 32] {
    [0; 32]
}

pub(super) fn extend_restore_base_reference_digest(
    previous: [u8; 32],
    reference: &RestoreBaseReference,
) -> Result<[u8; 32], MetadError> {
    let encoded = encode_restore_base_reference(reference)?;
    let mut leaf = Sha256::new();
    leaf.update(b"nokv-restore-base-reference-set-leaf-v2\0");
    leaf.update((encoded.len() as u64).to_be_bytes());
    leaf.update(encoded);
    let leaf: [u8; 32] = leaf.finalize().into();
    Ok(std::array::from_fn(|index| previous[index] ^ leaf[index]))
}

pub(super) fn finalize_restore_base_reference_digest(
    reference_count: u64,
    accumulator: [u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-restore-base-reference-set-seal-v2\0");
    hasher.update(reference_count.to_be_bytes());
    hasher.update(accumulator);
    hasher.finalize().into()
}

fn validate_restore_base_build(
    operation: &RestoreOperation,
    build: &RestoreBaseBuild,
) -> Result<(), MetadError> {
    if build.operation_digest != operation.operation_digest
        || build.initialization_digest != operation.initialization_digest
        || build.ref_set_id != operation.ref_set_id
        || build.incarnation != operation.created_version
    {
        return Err(MetadError::Codec(
            "restore base-reference build changed identity".to_owned(),
        ));
    }
    if build.active_member.is_some() != build.chunk_cursor.is_some()
        || (build.object_cursor.is_some() && build.active_member.is_none())
        || build
            .completed_member
            .zip(build.active_member)
            .is_some_and(|(completed, active)| active <= completed)
        || (build.reference_count == 0
            && build.reference_digest != initial_restore_base_reference_digest())
    {
        return Err(MetadError::Codec(
            "restore base-reference build cursor is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_base_seal(
    operation: &RestoreOperation,
    seal: &RestoreBaseSeal,
) -> Result<(), MetadError> {
    if seal.operation_digest != operation.operation_digest
        || seal.initialization_digest != operation.initialization_digest
        || seal.ref_set_id != operation.ref_set_id
        || seal.incarnation != operation.created_version
        || (seal.reference_count == 0
            && seal.reference_digest
                != finalize_restore_base_reference_digest(
                    0,
                    initial_restore_base_reference_digest(),
                ))
    {
        return Err(MetadError::Codec(
            "restore base-reference seal changed identity".to_owned(),
        ));
    }
    Ok(())
}

pub(super) fn encode_restore_base_build(build: &RestoreBaseBuild) -> Result<Vec<u8>, MetadError> {
    if build.ref_set_id == 0 || build.incarnation == 0 {
        return Err(MetadError::Codec(
            "restore base-reference build contains a zero identity".to_owned(),
        ));
    }
    if build.active_member.is_some() != build.chunk_cursor.is_some()
        || (build.object_cursor.is_some() && build.active_member.is_none())
        || build
            .completed_member
            .zip(build.active_member)
            .is_some_and(|(completed, active)| active <= completed)
    {
        return Err(MetadError::Codec(
            "restore base-reference build cursor is invalid".to_owned(),
        ));
    }
    let mut out = Vec::with_capacity(189);
    out.push(BASE_REFERENCE_BUILD_FORMAT_VERSION);
    out.extend_from_slice(&build.operation_digest);
    out.extend_from_slice(&build.initialization_digest);
    out.extend_from_slice(&build.ref_set_id.to_be_bytes());
    out.extend_from_slice(&build.incarnation.to_be_bytes());
    for inode in [build.completed_member, build.active_member] {
        out.push(u8::from(inode.is_some()));
        out.extend_from_slice(&inode.map_or(0, InodeId::get).to_be_bytes());
    }
    out.push(u8::from(build.chunk_cursor.is_some()));
    out.extend_from_slice(&build.chunk_cursor.unwrap_or(0).to_be_bytes());
    out.push(u8::from(build.object_cursor.is_some()));
    out.extend_from_slice(&build.object_cursor.unwrap_or([0; 32]));
    out.extend_from_slice(&build.reference_count.to_be_bytes());
    out.extend_from_slice(&build.reference_digest);
    out.extend_from_slice(&build.batch_index.to_be_bytes());
    Ok(out)
}

pub(super) fn decode_restore_base_build(bytes: &[u8]) -> Result<RestoreBaseBuild, MetadError> {
    let mut decoder = BaseDecoder::new(bytes);
    if decoder.u8()? != BASE_REFERENCE_BUILD_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "unsupported restore base-reference build version".to_owned(),
        ));
    }
    let operation_digest = decoder.array_32()?;
    let initialization_digest = decoder.array_32()?;
    let ref_set_id = decoder.u64()?;
    let incarnation = decoder.u64()?;
    let decode_inode = |decoder: &mut BaseDecoder<'_>| -> Result<Option<InodeId>, MetadError> {
        match (decoder.u8()?, decoder.u64()?) {
            (0, 0) => Ok(None),
            (1, raw) => Ok(Some(InodeId::new(raw)?)),
            _ => Err(MetadError::Codec(
                "restore base-reference build has an invalid inode cursor".to_owned(),
            )),
        }
    };
    let completed_member = decode_inode(&mut decoder)?;
    let active_member = decode_inode(&mut decoder)?;
    let chunk_cursor = match (decoder.u8()?, decoder.u64()?) {
        (0, 0) => None,
        (1, raw) => Some(raw),
        _ => {
            return Err(MetadError::Codec(
                "restore base-reference build has an invalid chunk cursor".to_owned(),
            ))
        }
    };
    let object_tag = decoder.u8()?;
    let object_raw = decoder.array_32()?;
    let object_cursor = match object_tag {
        0 if object_raw == [0; 32] => None,
        1 => Some(object_raw),
        _ => {
            return Err(MetadError::Codec(
                "restore base-reference build has an invalid object cursor".to_owned(),
            ))
        }
    };
    let build = RestoreBaseBuild {
        operation_digest,
        initialization_digest,
        ref_set_id,
        incarnation,
        completed_member,
        active_member,
        chunk_cursor,
        object_cursor,
        reference_count: decoder.u64()?,
        reference_digest: decoder.array_32()?,
        batch_index: decoder.u64()?,
    };
    decoder.finish()?;
    if build.ref_set_id == 0
        || build.incarnation == 0
        || build.active_member.is_some() != build.chunk_cursor.is_some()
        || (build.object_cursor.is_some() && build.active_member.is_none())
        || build
            .completed_member
            .zip(build.active_member)
            .is_some_and(|(completed, active)| active <= completed)
    {
        return Err(MetadError::Codec(
            "restore base-reference build contains an invalid identity".to_owned(),
        ));
    }
    Ok(build)
}

pub(super) fn decode_restore_base_seal_record(
    bytes: &[u8],
) -> Result<RestoreBaseSealRecord, MetadError> {
    match bytes.first().copied() {
        Some(BASE_REFERENCE_FORMAT_VERSION) => {
            decode_restore_base_seal(bytes).map(RestoreBaseSealRecord::Sealed)
        }
        Some(BASE_REFERENCE_BUILD_FORMAT_VERSION) => {
            decode_restore_base_build(bytes).map(RestoreBaseSealRecord::Building)
        }
        _ => Err(MetadError::Codec(
            "unsupported restore base seal/progress version".to_owned(),
        )),
    }
}

fn encode_restore_base_reference(reference: &RestoreBaseReference) -> Result<Vec<u8>, MetadError> {
    if reference.ref_set_id == 0 || reference.borrower_generation == 0 {
        return Err(MetadError::Codec(
            "restore base reference contains a zero identity".to_owned(),
        ));
    }
    let object_key = reference.object_key.as_bytes();
    let digest_uri = reference.digest_uri.as_bytes();
    let object_len = u32::try_from(object_key.len())
        .map_err(|_| MetadError::Codec("restore object key is too long".to_owned()))?;
    let digest_len = u32::try_from(digest_uri.len())
        .map_err(|_| MetadError::Codec("restore digest URI is too long".to_owned()))?;
    let mut out = Vec::with_capacity(102 + object_key.len() + digest_uri.len());
    out.push(BASE_REFERENCE_FORMAT_VERSION);
    out.extend_from_slice(&reference.operation_digest);
    out.extend_from_slice(&reference.ref_set_id.to_be_bytes());
    out.extend_from_slice(&reference.borrower_inode.get().to_be_bytes());
    out.extend_from_slice(&reference.borrower_generation.to_be_bytes());
    out.extend_from_slice(&reference.size.to_be_bytes());
    out.extend_from_slice(&object_len.to_be_bytes());
    out.extend_from_slice(object_key);
    out.extend_from_slice(&digest_len.to_be_bytes());
    out.extend_from_slice(digest_uri);
    Ok(out)
}

pub(super) fn decode_restore_base_reference(
    bytes: &[u8],
) -> Result<RestoreBaseReference, MetadError> {
    let mut decoder = BaseDecoder::new(bytes);
    if decoder.u8()? != BASE_REFERENCE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "unsupported restore base-reference version".to_owned(),
        ));
    }
    let reference = RestoreBaseReference {
        operation_digest: decoder.array_32()?,
        ref_set_id: decoder.u64()?,
        borrower_inode: InodeId::new(decoder.u64()?)?,
        borrower_generation: decoder.u64()?,
        size: decoder.u64()?,
        object_key: decoder.string()?,
        digest_uri: decoder.string()?,
    };
    decoder.finish()?;
    if reference.ref_set_id == 0 || reference.borrower_generation == 0 {
        return Err(MetadError::Codec(
            "restore base reference contains a zero identity".to_owned(),
        ));
    }
    Ok(reference)
}

fn encode_restore_base_inverse(inverse: &RestoreBaseInverse) -> Vec<u8> {
    let mut out = Vec::with_capacity(89);
    out.push(BASE_REFERENCE_FORMAT_VERSION);
    out.extend_from_slice(&inverse.operation_digest);
    out.extend_from_slice(&inverse.ref_set_id.to_be_bytes());
    out.extend_from_slice(&inverse.object_digest);
    out.extend_from_slice(&inverse.borrower_inode.get().to_be_bytes());
    out.extend_from_slice(&inverse.borrower_generation.to_be_bytes());
    out
}

pub(super) fn decode_restore_base_inverse(bytes: &[u8]) -> Result<RestoreBaseInverse, MetadError> {
    let mut decoder = BaseDecoder::new(bytes);
    if decoder.u8()? != BASE_REFERENCE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "unsupported restore base-inverse version".to_owned(),
        ));
    }
    let inverse = RestoreBaseInverse {
        operation_digest: decoder.array_32()?,
        ref_set_id: decoder.u64()?,
        object_digest: decoder.array_32()?,
        borrower_inode: InodeId::new(decoder.u64()?)?,
        borrower_generation: decoder.u64()?,
    };
    decoder.finish()?;
    if inverse.ref_set_id == 0 || inverse.borrower_generation == 0 {
        return Err(MetadError::Codec(
            "restore base inverse contains a zero identity".to_owned(),
        ));
    }
    Ok(inverse)
}

pub(super) fn encode_restore_base_seal(seal: &RestoreBaseSeal) -> Vec<u8> {
    let mut out = Vec::with_capacity(121);
    out.push(BASE_REFERENCE_FORMAT_VERSION);
    out.extend_from_slice(&seal.operation_digest);
    out.extend_from_slice(&seal.initialization_digest);
    out.extend_from_slice(&seal.ref_set_id.to_be_bytes());
    out.extend_from_slice(&seal.incarnation.to_be_bytes());
    out.extend_from_slice(&seal.reference_count.to_be_bytes());
    out.extend_from_slice(&seal.reference_digest);
    out
}

pub(super) fn decode_restore_base_seal(bytes: &[u8]) -> Result<RestoreBaseSeal, MetadError> {
    let mut decoder = BaseDecoder::new(bytes);
    if decoder.u8()? != BASE_REFERENCE_FORMAT_VERSION {
        return Err(MetadError::Codec(
            "unsupported restore base-seal version".to_owned(),
        ));
    }
    let seal = RestoreBaseSeal {
        operation_digest: decoder.array_32()?,
        initialization_digest: decoder.array_32()?,
        ref_set_id: decoder.u64()?,
        incarnation: decoder.u64()?,
        reference_count: decoder.u64()?,
        reference_digest: decoder.array_32()?,
    };
    decoder.finish()?;
    if seal.ref_set_id == 0 || seal.incarnation == 0 {
        return Err(MetadError::Codec(
            "restore base seal contains a zero identity".to_owned(),
        ));
    }
    Ok(seal)
}

struct BaseDecoder<'a> {
    input: &'a [u8],
}

impl<'a> BaseDecoder<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadError> {
        let Some((head, tail)) = self.input.split_at_checked(len) else {
            return Err(MetadError::Codec(
                "restore base-reference record is truncated".to_owned(),
            ));
        };
        self.input = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, MetadError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, MetadError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, MetadError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().expect("u64 width"),
        ))
    }

    fn array_32(&mut self) -> Result<[u8; 32], MetadError> {
        Ok(self.take(32)?.try_into().expect("digest width"))
    }

    fn string(&mut self) -> Result<String, MetadError> {
        let len = self.u32()? as usize;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| MetadError::Codec("restore base-reference string is not utf-8".to_owned()))
    }

    fn finish(self) -> Result<(), MetadError> {
        if self.input.is_empty() {
            Ok(())
        } else {
            Err(MetadError::Codec(
                "restore base-reference record has trailing bytes".to_owned(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::holtstore::HoltMetadataStore;
    use nokv_object::MemoryObjectStore;

    #[test]
    fn base_reference_codecs_fail_closed() {
        let reference = RestoreBaseReference {
            operation_digest: [1; 32],
            ref_set_id: 9,
            borrower_inode: InodeId::new(7).unwrap(),
            borrower_generation: 11,
            object_key: "blocks/1/2/3/4/5".to_owned(),
            digest_uri: "sha256:abc".to_owned(),
            size: 12,
        };
        let encoded = encode_restore_base_reference(&reference).unwrap();
        assert_eq!(decode_restore_base_reference(&encoded).unwrap(), reference);
        assert!(decode_restore_base_reference(&encoded[..encoded.len() - 1]).is_err());

        let seal = RestoreBaseSeal {
            operation_digest: [2; 32],
            initialization_digest: [3; 32],
            ref_set_id: 9,
            incarnation: 10,
            reference_count: 4,
            reference_digest: [5; 32],
        };
        assert_eq!(
            decode_restore_base_seal(&encode_restore_base_seal(&seal)).unwrap(),
            seal
        );

        let build = RestoreBaseBuild {
            operation_digest: [6; 32],
            initialization_digest: [7; 32],
            ref_set_id: 12,
            incarnation: 13,
            completed_member: Some(InodeId::new(20).unwrap()),
            active_member: Some(InodeId::new(21).unwrap()),
            chunk_cursor: Some(4),
            object_cursor: Some([8; 32]),
            reference_count: 65,
            reference_digest: [9; 32],
            batch_index: 2,
        };
        let encoded = encode_restore_base_build(&build).unwrap();
        assert_eq!(decode_restore_base_build(&encoded).unwrap(), build);
        assert!(decode_restore_base_build(&encoded[..encoded.len() - 1]).is_err());

        let mut invalid = build;
        invalid.active_member = None;
        assert!(encode_restore_base_build(&invalid).is_err());
    }

    #[test]
    fn base_reference_packer_rejects_one_reference_larger_than_the_command_budget() {
        let service = NoKvFs::new(
            MountId::new(1).unwrap(),
            HoltMetadataStore::open_memory().unwrap(),
            MemoryObjectStore::new(),
        );
        let operation = RestoreOperation {
            operation_digest: [7; 32],
            initialization_digest: [8; 32],
            state: RestoreOperationState::Preparing,
            source_root: InodeId::new(2).unwrap(),
            destination_root: InodeId::new(3).unwrap(),
            snapshot_id: 4,
            read_version: 5,
            created_version: 6,
            ref_set_id: 6,
            source_path: "/source".to_owned(),
            destination_path: "/destination".to_owned(),
        };
        let build = service
            .begin_restore_base_reference_preflight(&operation)
            .build;
        let page = RestoreBaseReferencePage {
            next: build.clone(),
            references: vec![RestoreBaseReference {
                operation_digest: operation.operation_digest,
                ref_set_id: operation.ref_set_id,
                borrower_inode: InodeId::new(9).unwrap(),
                borrower_generation: 10,
                object_key: "blocks/1/2/10/0/0".to_owned(),
                digest_uri: "d".repeat(8 * 1024 * 1024),
                size: 1,
            }],
            member_predicates: Vec::new(),
            complete: false,
        };
        let binding_key = fork_binding_key(service.mount, operation.destination_root);
        let seal_key = restore_base_seal_key(service.mount, operation.ref_set_id);

        // A public restore carrying this descriptor is normally rejected even
        // earlier by the larger materialization-command preflight. Exercise the
        // shared exact-reference packer directly so its own one-item no-progress
        // boundary remains independently fail-closed.
        let error = service
            .fit_restore_base_reference_page(
                &operation,
                Version::new(operation.created_version).unwrap(),
                &binding_key,
                &seal_key,
                Version::new(1).unwrap(),
                &build,
                page,
                false,
            )
            .unwrap_err();
        assert!(matches!(
            error,
            MetadError::RestoreResourceLimit {
                resource,
                limit,
                actual,
            } if resource == "restore base-reference batch bytes"
                && limit == 8 * 1024 * 1024
                && actual > limit
        ));
    }
}
