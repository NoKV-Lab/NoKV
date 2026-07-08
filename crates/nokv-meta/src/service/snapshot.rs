use super::*;

/// Default lease for a new snapshot pin: holders renew to keep it alive; an
/// abandoned pin expires after this so a crashed client never blocks GC forever.
pub const DEFAULT_SNAPSHOT_LEASE_MS: u64 = 3_600_000;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, MetadError> {
        self.snapshot_subtree_with_lease(root, DEFAULT_SNAPSHOT_LEASE_MS)
    }

    pub fn snapshot_subtree_with_lease(
        &self,
        root: InodeId,
        lease_ms: u64,
    ) -> Result<SnapshotPin, MetadError> {
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
            snapshot_id: created_version.get(),
            root,
            read_version: read_version.get(),
            created_version: created_version.get(),
            lease_expires_unix_ms: current_time_ms().saturating_add(lease_ms),
        };
        let key = snapshot_pin_key(self.mount, pin.snapshot_id);
        self.commit_metadata(MetadataCommand {
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
        })?;
        Ok(pin)
    }

    pub fn snapshot_subtree_path(&self, path: &str) -> Result<SnapshotPin, MetadError> {
        let root = self.resolve_directory_path(path)?;
        self.snapshot_subtree(root)
    }

    pub fn retire_snapshot(&self, snapshot_id: u64) -> Result<bool, MetadError> {
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

    /// Extend a pin's lease so it keeps protecting its snapshot. Extend-only: a
    /// renew never shortens an existing lease (Iceberg / S3 Object Lock
    /// semantics), so a short renew can't silently drop protection below what a
    /// prior renew already granted. Shrinking protection is expressed by
    /// `retire`, not by renew. Returns false if the pin no longer exists
    /// (already retired, or reaped after expiry).
    pub fn renew_snapshot(&self, snapshot_id: u64, lease_ms: u64) -> Result<bool, MetadError> {
        let Some(mut pin) = self.snapshot_pin(snapshot_id)? else {
            return Ok(false);
        };
        let new_expiry = current_time_ms().saturating_add(lease_ms);
        pin.lease_expires_unix_ms = pin.lease_expires_unix_ms.max(new_expiry);
        let key = snapshot_pin_key(self.mount, snapshot_id);
        let version = self.next_version()?;
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"renew-snapshot", self.mount, pin.root, version),
            kind: CommandKind::RenewSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family: RecordFamily::Snapshot,
            primary_key: key.clone(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Snapshot,
                key: key.clone(),
                predicate: Predicate::Exists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Snapshot,
                key,
                op: MutationOp::Put,
                value: Some(Value(encode_snapshot_pin(&pin))),
            }],
            watch: Vec::new(),
        })?;
        Ok(true)
    }

    pub fn snapshot_pin(&self, snapshot_id: u64) -> Result<Option<SnapshotPin>, MetadError> {
        self.snapshot_pin_for_purpose(snapshot_id, ReadPurpose::UserStrong)
    }

    fn snapshot_pin_for_purpose(
        &self,
        snapshot_id: u64,
        purpose: ReadPurpose,
    ) -> Result<Option<SnapshotPin>, MetadError> {
        let value = self.metadata.get(
            RecordFamily::Snapshot,
            &snapshot_pin_key(self.mount, snapshot_id),
            self.read_version()?,
            purpose,
        )?;
        value
            .map(|value| {
                decode_snapshot_pin(&value.0).map_err(|err| MetadError::Codec(err.to_string()))
            })
            .transpose()
    }

    pub fn get_attr_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Option<InodeAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::Snapshot)
    }

    pub fn lookup_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Option<DentryWithAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::Snapshot)
            .map(|entry| entry.map(|(entry, _)| entry))
    }

    pub fn read_dir_plus_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        self.read_dir_plus_at_version_for_purpose(parent, version, ReadPurpose::Snapshot)
    }

    pub fn stat_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Option<PathMetadata>, MetadError> {
        let pin = self
            .snapshot_pin_for_purpose(snapshot_id, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
        let version = Version::new(pin.read_version)?;
        self.stat_path_from_at_version_for_purpose(pin.root, path, version, ReadPurpose::Snapshot)
    }

    pub fn read_dir_plus_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<DentryWithAttr>, MetadError> {
        let pin = self
            .snapshot_pin_for_purpose(snapshot_id, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
        let version = Version::new(pin.read_version)?;
        let parent = self.resolve_components_as_directory_from_at_version_for_purpose(
            pin.root,
            &parse_absolute_path(path)?,
            version,
            ReadPurpose::Snapshot,
        )?;
        self.read_dir_plus_at_version_for_purpose(parent, version, ReadPurpose::Snapshot)
    }

    pub fn read_file_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let version = self.snapshot_read_version(snapshot_id)?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::Snapshot)?
        else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::File {
            return Err(MetadError::NotFile);
        }
        if offset >= attr.size {
            return Ok(Vec::new());
        }
        let body = self
            .body_descriptor_at_version_for_purpose(
                inode,
                attr.generation,
                version,
                ReadPurpose::Snapshot,
            )?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version_for_purpose(
            inode,
            &body,
            offset,
            len,
            version,
            ReadPurpose::Snapshot,
        )
    }

    pub fn read_symlink_at_snapshot(
        &self,
        snapshot_id: u64,
        inode: InodeId,
    ) -> Result<Vec<u8>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        let Some(attr) =
            self.get_attr_at_version_for_purpose(inode, version, ReadPurpose::Snapshot)?
        else {
            return Err(MetadError::NotFound);
        };
        if attr.file_type != FileType::Symlink {
            return Err(MetadError::NotFile);
        }
        let body = self
            .body_descriptor_at_version_for_purpose(
                inode,
                attr.generation,
                version,
                ReadPurpose::Snapshot,
            )?
            .ok_or(MetadError::MissingBodyDescriptor)?;
        self.read_file_at_version_for_purpose(
            inode,
            &body,
            0,
            body.size as usize,
            version,
            ReadPurpose::Snapshot,
        )
    }

    pub fn read_file_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, MetadError> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let pin = self
            .snapshot_pin_for_purpose(snapshot_id, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
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

    pub fn read_artifact_at_snapshot(
        &self,
        snapshot_id: u64,
        parent: InodeId,
        name: &DentryName,
    ) -> Result<Vec<u8>, MetadError> {
        let version = self.snapshot_read_version(snapshot_id)?;
        let entry = self
            .lookup_plus_at_version_for_purpose(parent, name, version, ReadPurpose::Snapshot)?
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

    pub fn read_artifact_path_at_snapshot(
        &self,
        snapshot_id: u64,
        path: &str,
    ) -> Result<Vec<u8>, MetadError> {
        let pin = self
            .snapshot_pin_for_purpose(snapshot_id, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
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

    pub(super) fn snapshot_read_version(&self, snapshot_id: u64) -> Result<Version, MetadError> {
        let pin = self
            .snapshot_pin_for_purpose(snapshot_id, ReadPurpose::Snapshot)?
            .ok_or(MetadError::NotFound)?;
        Version::new(pin.read_version).map_err(Into::into)
    }
}
