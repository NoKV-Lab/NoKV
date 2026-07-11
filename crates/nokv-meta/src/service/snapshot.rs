use super::*;

/// Default lease for a new snapshot pin: holders renew to keep it alive; an
/// abandoned pin expires after this so a crashed client never blocks GC forever.
pub const DEFAULT_SNAPSHOT_LEASE_MS: u64 = 3_600_000;
const SNAPSHOT_MINT_MAX_ATTEMPTS: usize = 8;
const SNAPSHOT_RENEW_MAX_ATTEMPTS: usize = 8;
const SNAPSHOT_ID_SHARD_BITS: u32 = 16;
const SNAPSHOT_ID_LOCAL_BITS: u32 = u64::BITS - SNAPSHOT_ID_SHARD_BITS;
const SNAPSHOT_ID_LOCAL_MASK: u64 = (1_u64 << SNAPSHOT_ID_LOCAL_BITS) - 1;

#[derive(Clone, Debug)]
struct VersionedSnapshotPin {
    pin: SnapshotPin,
    version: Version,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    #[cfg(test)]
    pub(crate) fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, MetadError> {
        self.snapshot_subtree_with_lease(root, DEFAULT_SNAPSHOT_LEASE_MS)
    }

    #[cfg(test)]
    pub(crate) fn snapshot_subtree_with_lease(
        &self,
        root: InodeId,
        lease_ms: u64,
    ) -> Result<SnapshotPin, MetadError> {
        for attempt in 0..SNAPSHOT_MINT_MAX_ATTEMPTS {
            let Some(attr) = self.get_attr_at_version_for_purpose(
                root,
                self.read_version()?,
                ReadPurpose::WritePlanLocal,
            )?
            else {
                return Err(MetadError::NotFound);
            };
            if attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            let created_version = self.next_version()?;
            let read_version = predecessor(created_version)?;
            let pin = SnapshotPin {
                snapshot_id: self.snapshot_id_for_version(created_version)?,
                root,
                read_version: read_version.get(),
                created_version: created_version.get(),
                lease_expires_unix_ms: self.now_ms().saturating_add(lease_ms),
            };
            let key = snapshot_pin_key(self.mount, pin.snapshot_id);
            match self.commit_metadata(MetadataCommand {
                request_id: request_id(b"snapshot-subtree", self.mount, root, created_version),
                kind: CommandKind::SnapshotSubtree,
                read_version,
                commit_version: created_version,
                primary_family: RecordFamily::Snapshot,
                primary_key: key.clone(),
                predicates: vec![
                    PredicateRef {
                        family: RecordFamily::Inode,
                        key: inode_key(self.mount, root),
                        predicate: Predicate::Exists,
                    },
                    PredicateRef {
                        family: RecordFamily::Snapshot,
                        key: key.clone(),
                        predicate: Predicate::NotExists,
                    },
                ],
                mutations: vec![Mutation {
                    family: RecordFamily::Snapshot,
                    key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_snapshot_pin(&pin))),
                }],
                watch: Vec::new(),
            }) {
                Ok(_) => return Ok(pin),
                Err(MetadError::Metadata(MetadataError::PredicateFailed))
                    if attempt + 1 < SNAPSHOT_MINT_MAX_ATTEMPTS =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("snapshot mint retry loop always returns")
    }

    pub fn snapshot_subtree_path(&self, path: &str) -> Result<SnapshotPin, MetadError> {
        self.snapshot_subtree_path_with_lease(path, DEFAULT_SNAPSHOT_LEASE_MS)
    }

    pub fn snapshot_subtree_path_with_lease(
        &self,
        path: &str,
        lease_ms: u64,
    ) -> Result<SnapshotPin, MetadError> {
        for attempt in 0..SNAPSHOT_MINT_MAX_ATTEMPTS {
            let binding_version = self.read_version()?;
            let (root, mut predicates) =
                self.resolve_snapshot_root_binding(path, binding_version)?;
            let binding_predicates = predicates.clone();
            let created_version = self.next_version()?;
            let read_version = predecessor(created_version)?;
            let pin = SnapshotPin {
                snapshot_id: self.snapshot_id_for_version(created_version)?,
                root,
                read_version: read_version.get(),
                created_version: created_version.get(),
                lease_expires_unix_ms: self.now_ms().saturating_add(lease_ms),
            };
            let key = snapshot_pin_key(self.mount, pin.snapshot_id);
            predicates.extend([
                PredicateRef {
                    family: RecordFamily::Inode,
                    key: inode_key(self.mount, root),
                    predicate: Predicate::Exists,
                },
                PredicateRef {
                    family: RecordFamily::Snapshot,
                    key: key.clone(),
                    predicate: Predicate::NotExists,
                },
            ]);
            match self.commit_metadata(MetadataCommand {
                request_id: request_id(b"snapshot-subtree-path", self.mount, root, created_version),
                kind: CommandKind::SnapshotSubtree,
                read_version,
                commit_version: created_version,
                primary_family: RecordFamily::Snapshot,
                primary_key: key.clone(),
                predicates,
                mutations: vec![Mutation {
                    family: RecordFamily::Snapshot,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(encode_snapshot_pin(&pin))),
                }],
                watch: Vec::new(),
            }) {
                Ok(_) => return Ok(pin),
                Err(MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                }) => {
                    let durable = self.snapshot_pin(pin.snapshot_id)?;
                    if durable.as_ref() == Some(&pin) {
                        return Ok(pin);
                    }
                    return Err(MetadError::Codec(format!(
                        "snapshot {} reported committed without its durable pin",
                        pin.snapshot_id
                    )));
                }
                Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                    if !self.snapshot_binding_predicates_match(&binding_predicates)? {
                        return Err(MetadError::SnapshotBindingChanged {
                            root_path: path.to_owned(),
                        });
                    }
                    if attempt + 1 == SNAPSHOT_MINT_MAX_ATTEMPTS {
                        return Err(MetadError::Metadata(MetadataError::PredicateFailed));
                    }
                }
                Err(err) => return Err(err),
            }
        }
        unreachable!("snapshot path mint retry loop always returns")
    }

    #[cfg(test)]
    pub(crate) fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, MetadError> {
        let key = snapshot_pin_key(self.mount, snapshot_id);
        if self.snapshot_pin(snapshot_id)?.is_none() {
            return Ok(false);
        }
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"retire-snapshot", self.mount, InodeId::root(), version),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![delete_mutation(RecordFamily::Snapshot, key)],
            watch: Vec::new(),
        })?;
        Ok(true)
    }

    pub fn retire_snapshot_path(
        &self,
        root_path: &str,
        snapshot_id: u64,
    ) -> Result<bool, MetadError> {
        let read_version = self.read_version()?;
        let (expected_root, mut predicates) =
            self.resolve_snapshot_root_binding(root_path, read_version)?;
        self.ensure_snapshot_id_shard(snapshot_id, expected_root)?;
        let Some(versioned) =
            self.versioned_snapshot_pin_at(snapshot_id, read_version, ReadPurpose::UserStrong)?
        else {
            return Ok(false);
        };
        if versioned.pin.root != expected_root {
            return Err(MetadError::SnapshotRootMismatch {
                snapshot_id,
                expected_root,
                actual_root: Some(versioned.pin.root),
                actual_shard: self.shard_index(),
            });
        }
        let key = snapshot_pin_key(self.mount, snapshot_id);
        predicates.push(PredicateRef {
            family: RecordFamily::Snapshot,
            key: key.clone(),
            predicate: Predicate::VersionEquals(versioned.version),
        });
        let version = self.next_version()?;
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(
                b"retire-snapshot-path",
                self.mount,
                versioned.pin.root,
                version,
            ),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates,
            mutations: vec![delete_mutation(RecordFamily::Snapshot, key)],
            watch: Vec::new(),
        }) {
            Ok(_) => Ok(true),
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                Err(MetadError::SnapshotBindingChanged {
                    root_path: root_path.to_owned(),
                })
            }
            Err(err) => Err(err),
        }
    }

    /// Extend a live pin without ever shortening a lease promised by another
    /// successful renew. The pin record version read by this attempt is the CAS
    /// fence; a conflict is retried from an authoritative read.
    #[cfg(test)]
    pub(crate) fn renew_snapshot(
        &self,
        snapshot_id: u64,
        lease_ms: u64,
    ) -> Result<SnapshotRenewOutcome, MetadError> {
        self.renew_snapshot_bound(None, snapshot_id, lease_ms)
    }

    /// Root-bound form used by path clients. The absolute root path is resolved
    /// on every retry and every component's dentry version joins the pin CAS, so
    /// a concurrent rename/rebind cannot renew a snapshot through a stale name.
    pub fn renew_snapshot_path(
        &self,
        root_path: &str,
        snapshot_id: u64,
        lease_ms: u64,
    ) -> Result<SnapshotRenewOutcome, MetadError> {
        self.renew_snapshot_bound(Some(root_path), snapshot_id, lease_ms)
    }

    fn renew_snapshot_bound(
        &self,
        root_path: Option<&str>,
        snapshot_id: u64,
        lease_ms: u64,
    ) -> Result<SnapshotRenewOutcome, MetadError> {
        let requested_expiry = self.now_ms().saturating_add(lease_ms);
        let key = snapshot_pin_key(self.mount, snapshot_id);
        for _attempt in 0..SNAPSHOT_RENEW_MAX_ATTEMPTS {
            let read_version = self.read_version()?;
            let (bound_root, mut binding_predicates) = match root_path {
                Some(root_path) => {
                    let (root, predicates) =
                        self.resolve_snapshot_root_binding(root_path, read_version)?;
                    self.ensure_snapshot_id_shard(snapshot_id, root)?;
                    (Some(root), predicates)
                }
                None => (None, Vec::new()),
            };
            let Some(versioned) =
                self.versioned_snapshot_pin_at(snapshot_id, read_version, ReadPurpose::UserStrong)?
            else {
                return Ok(SnapshotRenewOutcome::Missing { snapshot_id });
            };
            if let Some(actual_root) = bound_root {
                if actual_root != versioned.pin.root {
                    return Err(MetadError::SnapshotRootMismatch {
                        snapshot_id,
                        expected_root: actual_root,
                        actual_root: Some(versioned.pin.root),
                        actual_shard: self.shard_index(),
                    });
                }
            }
            let now_ms = self.now_ms();
            if now_ms >= versioned.pin.lease_expires_unix_ms {
                return Err(MetadError::SnapshotLeaseExpired {
                    snapshot_id,
                    lease_expires_unix_ms: versioned.pin.lease_expires_unix_ms,
                    now_ms,
                });
            }
            if versioned.pin.lease_expires_unix_ms >= requested_expiry {
                return Ok(SnapshotRenewOutcome::Renewed {
                    pin: versioned.pin,
                    extended: false,
                });
            }

            live_test_barrier::snapshot(snapshot_id, "renew-read")?;

            let mut renewed = versioned.pin;
            renewed.lease_expires_unix_ms = requested_expiry;
            let binding_fence = binding_predicates.clone();
            binding_predicates.push(PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::VersionEquals(versioned.version),
            });
            let commit_version = self.next_version()?;
            let command = MetadataCommand {
                request_id: request_id(b"renew-snapshot", self.mount, renewed.root, commit_version),
                kind: CommandKind::RenewSnapshot,
                read_version: predecessor(commit_version)?,
                commit_version,
                primary_family: RecordFamily::Snapshot,
                primary_key: key.clone(),
                predicates: binding_predicates,
                mutations: vec![Mutation {
                    family: RecordFamily::Snapshot,
                    key: key.clone(),
                    op: MutationOp::Put,
                    value: Some(Value(encode_snapshot_pin(&renewed))),
                }],
                watch: Vec::new(),
            };
            match self.commit_metadata(command) {
                Ok(_) => {
                    return Ok(SnapshotRenewOutcome::Renewed {
                        pin: renewed,
                        extended: true,
                    });
                }
                Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                    if let Some(root_path) = root_path {
                        if !self.snapshot_binding_predicates_match(&binding_fence)? {
                            return Err(MetadError::SnapshotBindingChanged {
                                root_path: root_path.to_owned(),
                            });
                        }
                    }
                    continue;
                }
                Err(MetadError::SyncLogArchiveFailed {
                    committed: true,
                    message,
                }) => {
                    let reconciled = self.snapshot_pin(snapshot_id)?;
                    if let Some(pin) = reconciled {
                        if pin.root == renewed.root && pin.lease_expires_unix_ms >= requested_expiry
                        {
                            return Ok(SnapshotRenewOutcome::Renewed {
                                pin,
                                extended: true,
                            });
                        }
                    }
                    return Err(MetadError::SyncLogArchiveFailed {
                        committed: true,
                        message,
                    });
                }
                Err(err) => return Err(err),
            }
        }
        Err(MetadError::SnapshotRenewContended {
            snapshot_id,
            attempts: SNAPSHOT_RENEW_MAX_ATTEMPTS,
        })
    }

    pub(crate) fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, MetadError> {
        self.snapshot_pin_for_purpose(snapshot_id, ReadPurpose::UserStrong)
    }

    pub fn snapshot_pin_path(
        &self,
        root_path: &str,
        snapshot_id: u64,
    ) -> Result<Option<SnapshotPin>, MetadError> {
        let binding_version = self.read_version()?;
        let (actual_root, _) = self.resolve_snapshot_root_binding(root_path, binding_version)?;
        self.ensure_snapshot_id_shard(snapshot_id, actual_root)?;
        let Some(pin) = self
            .versioned_snapshot_pin_at(snapshot_id, binding_version, ReadPurpose::UserStrong)?
            .map(|versioned| versioned.pin)
        else {
            return Ok(None);
        };
        if actual_root != pin.root {
            return Err(MetadError::SnapshotRootMismatch {
                snapshot_id,
                expected_root: actual_root,
                actual_root: Some(pin.root),
                actual_shard: self.shard_index(),
            });
        }
        Ok(Some(pin))
    }

    fn snapshot_pin_for_purpose(
        &self,
        snapshot_id: u64,
        purpose: ReadPurpose,
    ) -> Result<Option<SnapshotPin>, MetadError> {
        Ok(self
            .versioned_snapshot_pin_at(snapshot_id, self.read_version()?, purpose)?
            .map(|versioned| versioned.pin))
    }

    fn versioned_snapshot_pin_at(
        &self,
        snapshot_id: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Option<VersionedSnapshotPin>, MetadError> {
        let item = self.metadata.get_versioned(
            RecordFamily::Snapshot,
            &snapshot_pin_key(self.mount, snapshot_id),
            version,
            purpose,
        )?;
        item.map(|item| {
            let pin = decode_snapshot_pin(&item.value.0)
                .map_err(|err| MetadError::Codec(err.to_string()))?;
            Ok(VersionedSnapshotPin {
                pin,
                version: item.version,
            })
        })
        .transpose()
    }

    fn resolve_snapshot_root_binding(
        &self,
        root_path: &str,
        version: Version,
    ) -> Result<(InodeId, Vec<PredicateRef>), MetadError> {
        let components = parse_absolute_path(root_path)?;
        let mut current = InodeId::root();
        let mut predicates = Vec::with_capacity(components.len());
        for name in components {
            let Some((entry, dentry_version)) = self.lookup_plus_at_version_for_purpose(
                current,
                &name,
                version,
                ReadPurpose::UserStrong,
            )?
            else {
                return Err(MetadError::NotFound);
            };
            if entry.attr.file_type != FileType::Directory {
                return Err(MetadError::NotDirectory);
            }
            predicates.push(PredicateRef {
                family: RecordFamily::Dentry,
                key: dentry_key(self.mount, current, &name),
                predicate: Predicate::VersionEquals(dentry_version),
            });
            current = entry.attr.inode;
        }
        Ok((current, predicates))
    }

    fn snapshot_binding_predicates_match(
        &self,
        predicates: &[PredicateRef],
    ) -> Result<bool, MetadError> {
        let version = self.read_version()?;
        for predicate in predicates {
            let Predicate::VersionEquals(expected) = predicate.predicate else {
                continue;
            };
            let current = self.metadata.get_versioned(
                predicate.family,
                &predicate.key,
                version,
                ReadPurpose::UserStrong,
            )?;
            if current.as_ref().map(|item| item.version) != Some(expected) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn live_snapshot_pin_bound_to_path(
        &self,
        root_path: &str,
        snapshot_id: u64,
        purpose: ReadPurpose,
    ) -> Result<SnapshotPin, MetadError> {
        let binding_version = self.read_version()?;
        let (actual_root, _) = self.resolve_snapshot_root_binding(root_path, binding_version)?;
        self.ensure_snapshot_id_shard(snapshot_id, actual_root)?;
        let pin = self
            .versioned_snapshot_pin_at(snapshot_id, binding_version, purpose)?
            .map(|versioned| versioned.pin)
            .ok_or(MetadError::NotFound)?;
        if actual_root != pin.root {
            return Err(MetadError::SnapshotRootMismatch {
                snapshot_id,
                expected_root: actual_root,
                actual_root: Some(pin.root),
                actual_shard: self.shard_index(),
            });
        }
        self.ensure_snapshot_pin_live(&pin)?;
        Ok(pin)
    }

    fn snapshot_id_for_version(&self, version: Version) -> Result<u64, MetadError> {
        let local = version.get();
        if local > SNAPSHOT_ID_LOCAL_MASK {
            return Err(MetadError::Codec(
                "snapshot id local sequence exhausted its 48-bit shard namespace".to_owned(),
            ));
        }
        Ok((u64::from(self.shard_index()) << SNAPSHOT_ID_LOCAL_BITS) | local)
    }

    pub(super) fn ensure_snapshot_id_shard(
        &self,
        snapshot_id: u64,
        expected_root: InodeId,
    ) -> Result<(), MetadError> {
        let actual_shard = (snapshot_id >> SNAPSHOT_ID_LOCAL_BITS) as u16;
        if actual_shard != self.shard_index() {
            return Err(MetadError::SnapshotRootMismatch {
                snapshot_id,
                expected_root,
                actual_root: None,
                actual_shard,
            });
        }
        Ok(())
    }

    pub(super) fn ensure_snapshot_pin_live(&self, pin: &SnapshotPin) -> Result<(), MetadError> {
        let now_ms = self.now_ms();
        if now_ms >= pin.lease_expires_unix_ms {
            return Err(MetadError::SnapshotLeaseExpired {
                snapshot_id: pin.snapshot_id,
                lease_expires_unix_ms: pin.lease_expires_unix_ms,
                now_ms,
            });
        }
        Ok(())
    }

    pub fn get_attr_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        components: &[DentryName],
    ) -> Result<Option<InodeAttr>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        if components.is_empty() {
            return self.get_attr_at_version_for_purpose(pin.root, version, ReadPurpose::Snapshot);
        }
        self.snapshot_entry_from_components(pin.root, components, version)
            .map(|entry| entry.map(|entry| entry.attr))
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        parent_components: &[DentryName],
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            pin.root,
            parent_components,
            version,
            ReadPurpose::Snapshot,
        )?;
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::Snapshot)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        components: &[DentryName],
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            pin.root,
            components,
            version,
            ReadPurpose::Snapshot,
        )?;
        self.read_dir_plus_at_version_for_purpose(parent, version, ReadPurpose::Snapshot)
    }

    pub fn stat_path_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Option<PathMetadata>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        self.stat_path_from_at_version_for_purpose(pin.root, path, version, ReadPurpose::Snapshot)
    }

    pub fn read_dir_plus_path_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            pin.root,
            &parse_absolute_path(path)?,
            version,
            ReadPurpose::Snapshot,
        )?;
        self.read_dir_plus_at_version_for_purpose(parent, version, ReadPurpose::Snapshot)
    }

    pub fn read_dir_plus_path_at_snapshot_page(
        &self,
        root_path: &str,
        snapshot_id: u64,
        path: &str,
        after: Option<&DentryName>,
        limit: usize,
    ) -> Result<ReadDirPlusPage, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            pin.root,
            &parse_absolute_path(path)?,
            version,
            ReadPurpose::Snapshot,
        )?;
        self.read_dir_plus_page_at_version_for_purpose(
            parent,
            after,
            limit,
            version,
            ReadPurpose::Snapshot,
        )
    }

    pub fn read_file_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        components: &[DentryName],
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let version = Version::new(pin.read_version)?;
        let entry = self
            .snapshot_entry_from_components(pin.root, components, version)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= entry.attr.size {
            return Ok(Vec::new());
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version_for_purpose(
            entry.attr.inode,
            &body,
            offset,
            len,
            version,
            ReadPurpose::Snapshot,
        )
    }

    pub fn read_symlink_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        components: &[DentryName],
    ) -> Result<Vec<u8>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        let entry = self
            .snapshot_entry_from_components(pin.root, components, version)?
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::Symlink {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version_for_purpose(
            entry.attr.inode,
            &body,
            0,
            body.size as usize,
            version,
            ReadPurpose::Snapshot,
        )
    }

    fn snapshot_entry_from_components(
        &self,
        root: InodeId,
        components: &[DentryName],
        version: Version,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let Some((name, parents)) = components.split_last() else {
            return Ok(None);
        };
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            root,
            parents,
            version,
            ReadPurpose::Snapshot,
        )?;
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::Snapshot)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub fn read_file_path_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        if len == 0 {
            return Ok(Vec::new());
        }
        let version = Version::new(pin.read_version)?;
        let entry = self
            .lookup_path_from_at_version_for_purpose(
                pin.root,
                path,
                version,
                ReadPurpose::Snapshot,
            )?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= entry.attr.size {
            return Ok(Vec::new());
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version_for_purpose(
            entry.attr.inode,
            &body,
            offset,
            len,
            version,
            ReadPurpose::Snapshot,
        )
    }

    pub fn read_artifact_path_at_snapshot(
        &self,
        root_path: &str,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<u8>, MetadError> {
        let pin =
            self.live_snapshot_pin_bound_to_path(root_path, snapshot_id, ReadPurpose::Snapshot)?;
        let version = Version::new(pin.read_version)?;
        let entry = self
            .lookup_path_from_at_version_for_purpose(
                pin.root,
                path,
                version,
                ReadPurpose::Snapshot,
            )?
            .map(|(entry, _)| entry)
            .ok_or(MetadError::NotFound)?;
        if entry.attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        let body = entry.body.ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version_for_purpose(
            entry.attr.inode,
            &body,
            0,
            body.size as usize,
            version,
            ReadPurpose::Snapshot,
        )
    }
}
