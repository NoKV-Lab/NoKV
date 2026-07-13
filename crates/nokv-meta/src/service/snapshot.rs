use super::*;
use crate::layout::{decode_fork_binding, fork_binding_prefix};

/// Default lease for a new snapshot pin: holders renew to keep it alive; an
/// abandoned pin expires after this so a crashed client never blocks GC forever.
pub const DEFAULT_SNAPSHOT_LEASE_MS: u64 = 3_600_000;
const SNAPSHOT_MINT_MAX_ATTEMPTS: usize = 8;
const SNAPSHOT_RENEW_MAX_ATTEMPTS: usize = 8;
const SNAPSHOT_ID_SHARD_BITS: u32 = 16;
const SNAPSHOT_ID_LOCAL_BITS: u32 = u64::BITS - SNAPSHOT_ID_SHARD_BITS;
const SNAPSHOT_ID_LOCAL_MASK: u64 = (1_u64 << SNAPSHOT_ID_LOCAL_BITS) - 1;

#[derive(Clone, Debug)]
pub(super) struct VersionedSnapshotPin {
    pub(super) pin: SnapshotPin,
    pub(super) version: Version,
}

#[derive(Clone, Debug)]
pub(super) struct VersionedForkBinding {
    pub(super) binding: ForkBinding,
    pub(super) key: Vec<u8>,
    pub(super) version: Version,
}

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub(crate) fn snapshot_subtree(&self, root: InodeId) -> Result<SnapshotPin, MetadError> {
        self.snapshot_subtree_with_lease(root, DEFAULT_SNAPSHOT_LEASE_MS)
    }

    pub(crate) fn snapshot_subtree_with_lease(
        &self,
        root: InodeId,
        lease_ms: u64,
    ) -> Result<SnapshotPin, MetadError> {
        for attempt in 0..SNAPSHOT_MINT_MAX_ATTEMPTS {
            let object_reference = self.begin_object_reference_mutation()?;
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
                    object_reference.predicate(self.mount),
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
            let object_reference = self.begin_object_reference_mutation()?;
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
                object_reference.predicate(self.mount),
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
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let read_version = self.read_version()?;
        let pin =
            self.versioned_snapshot_pin_at(snapshot_id, read_version, ReadPurpose::UserStrong)?;
        let bindings =
            self.fork_bindings_for_snapshot_at(snapshot_id, read_version, ReadPurpose::UserStrong)?;
        if pin.is_none() && bindings.is_empty() {
            return Ok(false);
        }
        self.validate_snapshot_retention_roots(snapshot_id, pin.as_ref(), &bindings, None)?;
        self.ensure_fork_bindings_releasable(&bindings, read_version)?;

        let pin_key = snapshot_pin_key(self.mount, snapshot_id);
        let mut predicates = Vec::with_capacity(usize::from(pin.is_some()) + bindings.len());
        let mut mutations = Vec::with_capacity(predicates.capacity());
        if let Some(versioned) = &pin {
            predicates.push(PredicateRef {
                family: RecordFamily::Snapshot,
                key: pin_key.clone(),
                predicate: Predicate::VersionEquals(versioned.version),
            });
            mutations.push(delete_mutation(RecordFamily::Snapshot, pin_key.clone()));
        }
        for binding in &bindings {
            predicates.push(PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding.key.clone(),
                predicate: Predicate::VersionEquals(binding.version),
            });
            mutations.push(delete_mutation(
                RecordFamily::ForkBinding,
                binding.key.clone(),
            ));
        }

        let (primary_family, primary_key, request_inode) = if let Some(versioned) = &pin {
            (RecordFamily::Snapshot, pin_key, versioned.pin.root)
        } else {
            let binding = bindings
                .first()
                .expect("retire has either a snapshot pin or fork binding");
            (
                RecordFamily::ForkBinding,
                binding.key.clone(),
                binding.binding.source_root,
            )
        };
        let version = self.next_version()?;
        let retires_binding = !bindings.is_empty();
        if retires_binding {
            // A detached root becomes unbound at this CAS. Enter fail-closed
            // mode before the commit so a committed-but-unacknowledged result
            // cannot leave the healthy fast path enabled.
            self.mark_materialization_orphan_possible_under_gc_gate();
        }
        self.commit_metadata(MetadataCommand {
            request_id: request_id(b"retire-snapshot", self.mount, request_inode, version),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family,
            primary_key,
            predicates,
            mutations,
            watch: Vec::new(),
        })?;
        if retires_binding {
            let _ = self.reconcile_materialization_orphan_state_under_gc_gate();
        }
        Ok(true)
    }

    /// Retire a source snapshot and every durable fork binding derived from it.
    ///
    /// Clone bindings deliberately outlive the snapshot lease. Because the
    /// retention floor is mount-global, retirement fails closed while any
    /// reachable effective manifest under the mount root or a live detached
    /// fork root still borrows blocks from another inode (including hardlinks,
    /// renames, and rollback propagation).
    /// `root_path` is resolved at retirement time, so a renamed source is
    /// retired through its new path; if the source path has been deleted this
    /// path-bound API leaves the binding retained.
    pub fn retire_snapshot_path(
        &self,
        root_path: &str,
        snapshot_id: u64,
    ) -> Result<bool, MetadError> {
        let _object_gc_gate = self
            .object_gc_gate
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        let read_version = self.read_version()?;
        let (expected_root, mut predicates) =
            self.resolve_snapshot_root_binding(root_path, read_version)?;
        let binding_predicates = predicates.clone();
        self.ensure_snapshot_id_shard(snapshot_id, expected_root)?;
        let pin =
            self.versioned_snapshot_pin_at(snapshot_id, read_version, ReadPurpose::UserStrong)?;
        let bindings =
            self.fork_bindings_for_snapshot_at(snapshot_id, read_version, ReadPurpose::UserStrong)?;
        if pin.is_none() && bindings.is_empty() {
            return Ok(false);
        }
        self.validate_snapshot_retention_roots(
            snapshot_id,
            pin.as_ref(),
            &bindings,
            Some(expected_root),
        )?;
        self.ensure_fork_bindings_releasable(&bindings, read_version)?;

        let pin_key = snapshot_pin_key(self.mount, snapshot_id);
        let mut mutations = Vec::with_capacity(usize::from(pin.is_some()) + bindings.len());
        if let Some(versioned) = &pin {
            predicates.push(PredicateRef {
                family: RecordFamily::Snapshot,
                key: pin_key.clone(),
                predicate: Predicate::VersionEquals(versioned.version),
            });
            mutations.push(delete_mutation(RecordFamily::Snapshot, pin_key.clone()));
        }
        for binding in &bindings {
            predicates.push(PredicateRef {
                family: RecordFamily::ForkBinding,
                key: binding.key.clone(),
                predicate: Predicate::VersionEquals(binding.version),
            });
            mutations.push(delete_mutation(
                RecordFamily::ForkBinding,
                binding.key.clone(),
            ));
        }

        let (primary_family, primary_key) = if pin.is_some() {
            (RecordFamily::Snapshot, pin_key)
        } else {
            (
                RecordFamily::ForkBinding,
                bindings
                    .first()
                    .expect("retire has either a snapshot pin or fork binding")
                    .key
                    .clone(),
            )
        };
        let version = self.next_version()?;
        let retires_binding = !bindings.is_empty();
        if retires_binding {
            self.mark_materialization_orphan_possible_under_gc_gate();
        }
        match self.commit_metadata(MetadataCommand {
            request_id: request_id(b"retire-snapshot-path", self.mount, expected_root, version),
            kind: CommandKind::RetireSnapshot,
            read_version: predecessor(version)?,
            commit_version: version,
            primary_family,
            primary_key,
            predicates,
            mutations,
            watch: Vec::new(),
        }) {
            Ok(_) => {
                if retires_binding {
                    let _ = self.reconcile_materialization_orphan_state_under_gc_gate();
                }
                Ok(true)
            }
            Err(MetadError::Metadata(MetadataError::PredicateFailed)) => {
                if !self.snapshot_binding_predicates_match(&binding_predicates)? {
                    Err(MetadError::SnapshotBindingChanged {
                        root_path: root_path.to_owned(),
                    })
                } else {
                    Err(MetadError::Metadata(MetadataError::PredicateFailed))
                }
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
            // Capture the durable Open epoch before this attempt reads the pin.
            // If GC starts after the live check, the renew commit conflicts and
            // the next attempt re-reads the pin and its current lease state.
            let object_reference = self.begin_object_reference_mutation()?;
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
            binding_predicates.push(object_reference.predicate(self.mount));
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
                Err(err) => return Err(err),
            }
        }
        Err(MetadError::SnapshotRenewContended {
            snapshot_id,
            attempts: SNAPSHOT_RENEW_MAX_ATTEMPTS,
        })
    }

    #[cfg(test)]
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

    #[cfg(test)]
    fn snapshot_pin_for_purpose(
        &self,
        snapshot_id: u64,
        purpose: ReadPurpose,
    ) -> Result<Option<SnapshotPin>, MetadError> {
        Ok(self
            .versioned_snapshot_pin_at(snapshot_id, self.read_version()?, purpose)?
            .map(|versioned| versioned.pin))
    }

    pub(super) fn versioned_snapshot_pin_at(
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

    /// Prove that raising the mount-global history floor cannot orphan any
    /// reachable borrowed object reference. A binding may be the oldest hold for
    /// an object borrowed by a different fork or by rollback, so source-local
    /// lineage is insufficient: retirement fails closed while any effective
    /// manifest reachable from the mount root or a live detached fork root names
    /// a block owned by another inode.
    ///
    /// Callers hold `object_gc_gate` across this scan and the binding CAS. Clone
    /// and rollback hold the same gate while publishing new borrowed references,
    /// so the proof cannot race either producer.
    pub(super) fn validate_current_dentry_projection(
        &self,
        row_key: &[u8],
        projection: &DentryProjection,
        current_version: Version,
    ) -> Result<(), MetadError> {
        let expected_key = dentry_key(
            self.mount,
            projection.dentry.parent,
            &projection.dentry.name,
        );
        if row_key != expected_key {
            return Err(MetadError::Codec(format!(
                "dentry row key does not match borrower inode {}",
                projection.attr.inode.get()
            )));
        }
        if projection.dentry.child != projection.attr.inode
            || projection.dentry.child_type != projection.attr.file_type
            || projection.dentry.attr_generation != projection.attr.generation
        {
            return Err(MetadError::Codec(format!(
                "dentry projection identity does not match borrower inode {}",
                projection.attr.inode.get()
            )));
        }
        if projection.attr.inode.shard_index() != self.shard_index() {
            if projection.attr.file_type != FileType::Directory
                || projection.attr.size != 0
                || projection.attr.rdev != 0
                || projection.body.is_some()
            {
                return Err(MetadError::Codec(format!(
                    "foreign dentry projection is not a valid graft for inode {}",
                    projection.attr.inode.get()
                )));
            }
            return Ok(());
        }

        let Some(inode_value) = self.metadata.get(
            RecordFamily::Inode,
            &inode_key(self.mount, projection.attr.inode),
            current_version,
            ReadPurpose::WritePlanLocal,
        )?
        else {
            return Err(MetadError::Codec(format!(
                "dentry projection names missing borrower inode {}",
                projection.attr.inode.get()
            )));
        };
        let canonical_attr =
            decode_inode_attr(&inode_value.0).map_err(|err| MetadError::Codec(err.to_string()))?;
        if canonical_attr.inode != projection.attr.inode
            || canonical_attr.file_type != projection.attr.file_type
            || canonical_attr.size != projection.attr.size
            || canonical_attr.generation != projection.attr.generation
        {
            return Err(MetadError::Codec(format!(
                "dentry projection object identity does not match borrower inode {}",
                projection.attr.inode.get()
            )));
        }

        if let Some(body) = projection.body.as_ref() {
            if body.size != projection.attr.size
                || body.generation == 0
                || body.generation > projection.attr.generation
            {
                return Err(MetadError::Codec(format!(
                    "dentry projection body does not match borrower inode {}",
                    projection.attr.inode.get()
                )));
            }
            let canonical_body = self
                .body_descriptor_at_version_for_purpose(
                    projection.attr.inode,
                    body.generation,
                    current_version,
                    ReadPurpose::WritePlanLocal,
                )?
                .ok_or(MetadError::MissingBodyDescriptor)?;
            if canonical_body != *body {
                return Err(MetadError::Codec(format!(
                    "dentry projection body descriptor does not match borrower inode {}",
                    projection.attr.inode.get()
                )));
            }
        }
        Ok(())
    }

    fn ensure_fork_bindings_releasable(
        &self,
        bindings: &[VersionedForkBinding],
        current_version: Version,
    ) -> Result<(), MetadError> {
        let Some(versioned) = bindings.first() else {
            return Ok(());
        };
        let binding = versioned.binding;
        let purpose = ReadPurpose::WritePlanLocal;
        let mut roots = vec![InodeId::root()];

        // A clone may deliberately remain detached and is then reachable through
        // its durable binding rather than the mount root. A rollback binding is
        // also durable, but its materialization root is deleted by the graft; in
        // that case the restored children are reached through the mount root.
        // Conversely, an interrupted materialization has neither a namespace link
        // nor a binding and must not become an immortal retention root merely
        // because its partial dentry rows survived the crash/failure.
        for versioned in self.versioned_fork_bindings_at(current_version, purpose)? {
            let root = versioned.binding.fork_root;
            let Some(attr) =
                self.get_attr_at_version_for_purpose(root, current_version, purpose)?
            else {
                continue;
            };
            if attr.file_type != FileType::Directory {
                return Err(MetadError::Codec(format!(
                    "fork binding root {} is not a directory",
                    root.get()
                )));
            }
            roots.push(root);
        }

        let Some(root_attr) =
            self.get_attr_at_version_for_purpose(InodeId::root(), current_version, purpose)?
        else {
            return Err(MetadError::Codec(
                "mount root is missing during fork retention proof".to_owned(),
            ));
        };
        if root_attr.file_type != FileType::Directory {
            return Err(MetadError::Codec(
                "mount root is not a directory during fork retention proof".to_owned(),
            ));
        }

        let mut visited_directories = HashSet::new();
        let mut checked_bodies = HashSet::new();
        while let Some(parent) = roots.pop() {
            if !visited_directories.insert(parent) {
                continue;
            }
            let rows = self.metadata.scan(ScanRequest {
                family: RecordFamily::Dentry,
                prefix: dentry_prefix(self.mount, parent),
                start_after: None,
                version: current_version,
                limit: 0,
                purpose,
            })?;
            for row in rows {
                let projection = decode_dentry_projection(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                self.validate_current_dentry_projection(&row.key, &projection, current_version)?;

                if projection.attr.inode.shard_index() == self.shard_index()
                    && projection.attr.file_type == FileType::Directory
                {
                    roots.push(projection.attr.inode);
                }

                let Some(body) = projection.body.as_ref() else {
                    if projection.attr.size == 0 {
                        continue;
                    }
                    return Err(MetadError::MissingBodyDescriptor);
                };
                if !checked_bodies.insert((projection.attr.inode, body.generation)) {
                    continue;
                }
                let manifests = self.chunk_manifests_for_body_at_version(
                    projection.attr.inode,
                    body,
                    current_version,
                    purpose,
                )?;
                for block in manifests
                    .iter()
                    .flat_map(|manifest| &manifest.slices)
                    .flat_map(|slice| &slice.blocks)
                {
                    if !self
                        .block_object_is_owned_by_inode(projection.attr.inode, &block.object_key)?
                    {
                        return Err(MetadError::ForkRetentionActive {
                            snapshot_id: binding.snapshot_id,
                            fork_root: binding.fork_root,
                            borrower: projection.attr.inode,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn versioned_fork_bindings_at(
        &self,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<VersionedForkBinding>, MetadError> {
        self.metadata
            .scan(ScanRequest {
                family: RecordFamily::ForkBinding,
                prefix: fork_binding_prefix(self.mount),
                start_after: None,
                version,
                limit: 0,
                purpose,
            })?
            .into_iter()
            .map(|row| {
                let binding = decode_fork_binding(&row.value.0)
                    .map_err(|err| MetadError::Codec(err.to_string()))?;
                let expected_key = fork_binding_key(self.mount, binding.fork_root);
                if row.key != expected_key {
                    return Err(MetadError::Codec(format!(
                        "fork binding key does not match fork root {}",
                        binding.fork_root.get()
                    )));
                }
                if binding.fork_root.shard_index() != self.shard_index()
                    || binding.source_root.shard_index() != self.shard_index()
                {
                    return Err(MetadError::Codec(format!(
                        "fork binding {} crosses shard boundary",
                        binding.snapshot_id
                    )));
                }
                self.ensure_snapshot_id_shard(binding.snapshot_id, binding.source_root)?;
                let snapshot_created_version = binding.snapshot_id & SNAPSHOT_ID_LOCAL_MASK;
                if snapshot_created_version == 0
                    || binding.pinned_read_version.checked_add(1) != Some(snapshot_created_version)
                    || snapshot_created_version >= binding.created_version
                    || binding.created_version != row.version.get()
                {
                    return Err(MetadError::Codec(format!(
                        "fork binding {} has invalid version identity",
                        binding.snapshot_id
                    )));
                }
                Ok(VersionedForkBinding {
                    binding,
                    key: row.key,
                    version: row.version,
                })
            })
            .collect()
    }

    fn fork_bindings_for_snapshot_at(
        &self,
        snapshot_id: u64,
        version: Version,
        purpose: ReadPurpose,
    ) -> Result<Vec<VersionedForkBinding>, MetadError> {
        Ok(self
            .versioned_fork_bindings_at(version, purpose)?
            .into_iter()
            .filter(|binding| binding.binding.snapshot_id == snapshot_id)
            .collect())
    }

    fn validate_snapshot_retention_roots(
        &self,
        snapshot_id: u64,
        pin: Option<&VersionedSnapshotPin>,
        bindings: &[VersionedForkBinding],
        expected_root: Option<InodeId>,
    ) -> Result<(), MetadError> {
        let actual_root = pin
            .map(|versioned| versioned.pin.root)
            .or_else(|| bindings.first().map(|binding| binding.binding.source_root));
        if let Some(root) = actual_root {
            if pin.is_some_and(|versioned| versioned.pin.snapshot_id != snapshot_id)
                || bindings.iter().any(|binding| {
                    binding.binding.snapshot_id != snapshot_id
                        || binding.binding.source_root != root
                        || pin.is_some_and(|versioned| {
                            binding.binding.pinned_read_version != versioned.pin.read_version
                        })
                })
            {
                return Err(MetadError::Codec(format!(
                    "snapshot {snapshot_id} retention records disagree on identity"
                )));
            }
            if expected_root.is_some_and(|expected| expected != root) {
                return Err(MetadError::SnapshotRootMismatch {
                    snapshot_id,
                    expected_root: expected_root.expect("checked as some"),
                    actual_root: Some(root),
                    actual_shard: self.shard_index(),
                });
            }
        }
        Ok(())
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

    fn ensure_snapshot_id_shard(
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
