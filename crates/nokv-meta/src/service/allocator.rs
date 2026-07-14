use super::*;

impl<M, O> NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    pub fn refresh_allocator_state(&self) -> Result<(), MetadError> {
        let allocator = recover_allocator_state(&self.metadata, self.mount, self.shard_index)?;
        self.clock
            .fetch_max(allocator.last_commit_version, Ordering::Relaxed);
        self.reserved_version
            .fetch_max(allocator.last_commit_version, Ordering::Relaxed);
        self.next_inode
            .fetch_max(allocator.next_inode, Ordering::Relaxed);
        self.reserved_next_inode
            .fetch_max(allocator.next_inode, Ordering::Relaxed);
        // Epoch is monotonic: a concurrent refresh that reads an older record
        // must never lower it, or an in-flight reservation could re-persist a
        // stale epoch. fetch_max preserves restart/refresh monotonicity.
        self.epoch.fetch_max(allocator.epoch, Ordering::Relaxed);
        Ok(())
    }

    /// The identity of this node's allocation authority. `1` for a single owner;
    /// a control plane bumps it on inode-range ownership transfer so a stale
    /// owner can be fenced (see [`AllocatorState::epoch`]).
    pub fn allocator_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    pub(super) fn ensure_allocator_reservation(
        &self,
        required_version: u64,
        required_next_inode: u64,
    ) -> Result<(), MetadError> {
        if required_version <= self.reserved_version.load(Ordering::Relaxed)
            && required_next_inode <= self.reserved_next_inode.load(Ordering::Relaxed)
        {
            return Ok(());
        }

        let _guard = self.allocator_gate.lock().map_err(|err| {
            MetadataError::Backend(format!("metadata allocator gate poisoned: {err}"))
        })?;
        let current_reserved_version = self.reserved_version.load(Ordering::Relaxed);
        let current_reserved_next_inode = self.reserved_next_inode.load(Ordering::Relaxed);
        if required_version <= current_reserved_version
            && required_next_inode <= current_reserved_next_inode
        {
            return Ok(());
        }
        let reserved_version = current_reserved_version.max(reservation_upper_bound(
            required_version,
            ALLOCATOR_VERSION_RESERVATION,
        ));
        let reserved_next_inode = current_reserved_next_inode.max(reservation_upper_bound(
            required_next_inode,
            ALLOCATOR_INODE_RESERVATION,
        ));
        InodeId::new(reserved_next_inode)?;

        let commit_version = Version::new(
            required_version
                .max(self.clock.load(Ordering::Relaxed))
                .max(2),
        )?;
        let key = allocator_key(self.mount);
        let reservation = self.commit_metadata_from_factory(|| {
            Ok(MetadataCommand {
                request_id: allocator_reservation_request_id(
                    self.mount,
                    commit_version,
                    reserved_version,
                    reserved_next_inode,
                ),
                kind: CommandKind::ReserveAllocator,
                read_version: predecessor(commit_version)?,
                commit_version,
                primary_family: RecordFamily::System,
                primary_key: key.clone(),
                predicates: Vec::new(),
                mutations: vec![Mutation {
                    family: RecordFamily::System,
                    key,
                    op: MutationOp::Put,
                    value: Some(Value(encode_allocator_state(
                        reserved_version,
                        reserved_next_inode,
                        // The factory executes under the epoch read fence held
                        // through metadata apply.
                        self.epoch.load(Ordering::Relaxed),
                    ))),
                }],
                watch: Vec::new(),
            })
        });
        match reservation {
            Ok(_) => {
                // The reservation is durable in the metadata engine. Preserve
                // that knowledge locally.
                self.reserved_version
                    .store(reserved_version, Ordering::Relaxed);
                self.reserved_next_inode
                    .store(reserved_next_inode, Ordering::Relaxed);
                Ok(())
            }
            Err(
                err @ MetadError::SyncLogArchiveFailed {
                    committed: true, ..
                },
            ) => {
                // Keep the old process-local watermark even though Holt applied
                // the reservation. The next prepare must re-enter this slow path
                // so the pending segment is flushed before it can acknowledge a
                // newly allocated generation or inode.
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    pub(super) fn next_version(&self) -> Result<Version, MetadError> {
        let raw = self.clock.fetch_add(1, Ordering::Relaxed) + 1;
        self.ensure_allocator_reservation(raw, self.next_inode.load(Ordering::Relaxed))?;
        Version::new(raw).map_err(Into::into)
    }

    pub(super) fn read_version(&self) -> Result<Version, MetadError> {
        Version::new(self.clock.load(Ordering::Relaxed)).map_err(Into::into)
    }

    pub(super) fn next_inode(&self) -> Result<InodeId, MetadError> {
        let raw = self.next_inode.fetch_add(1, Ordering::Relaxed);
        let required_next_inode = raw.checked_add(1).ok_or(MetadError::AllocatorExhausted)?;
        self.ensure_allocator_reservation(self.clock.load(Ordering::Relaxed), required_next_inode)?;
        InodeId::new(raw).map_err(Into::into)
    }

    pub(super) fn next_inodes(&self, count: usize) -> Result<Vec<InodeId>, MetadError> {
        let count = u64::try_from(count).map_err(|_| MetadError::AllocatorExhausted)?;
        let start = self.next_inode.fetch_add(count, Ordering::Relaxed);
        let end = start
            .checked_add(count)
            .ok_or(MetadError::AllocatorExhausted)?;
        self.ensure_allocator_reservation(self.clock.load(Ordering::Relaxed), end)?;
        (start..end)
            .map(|raw| InodeId::new(raw).map_err(Into::into))
            .collect()
    }
}
