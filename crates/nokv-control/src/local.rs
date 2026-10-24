//! Single-host control store.
//!
//! Records live in one file that every mutation replaces atomically under an
//! exclusive store lock. An owner session is an exclusive `flock` on a
//! per-shard lock file. The kernel releases it when the owner process exits
//! for any reason, so a crash ends the session the way lease expiry ends an
//! etcd session, without a TTL, a clock, or a renewal round trip. While this
//! store holds that lock no other process can install a successor, so proving
//! ownership is a check of in-process state plus one `stat` showing that the
//! lock file was not replaced.
//!
//! Every transition runs the in-memory store's state machine over the durable
//! records, with the touched shard's session taken from its lock, so the two
//! stores cannot diverge on owner, epoch, or recovery rules.
//!
//! Tenant identity records live in a second file under the same store lock,
//! so readers that predate them never parse it.
//!
//! The directory must be on a local filesystem of the one host that runs the
//! owner: network filesystems do not give `flock` these semantics. Owners or
//! clients that span hosts use the etcd store.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::os::unix::io::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use crate::codec::{
    decode_local_control_state, decode_local_identity_state, encode_fixed_id,
    encode_local_control_state, encode_local_identity_state,
};
use crate::identity::IdentityState;
use crate::store::{validate_record_lease, InMemoryState};
use crate::{
    ControlError, ControlStore, CredentialBinding, CredentialFingerprint, IdentityLimits,
    IdentityStore, InMemoryControlStore, LogicalShardId, LogicalShardLease, LogicalShardRecord,
    NodeId, OwnerEpoch, PrincipalRecord, RecoveryPublication, RecoveryUploadIntent, RoleBinding,
    RootAgentBinding, RootId, RootObjectNamespaceBinding, RootPlacement, RootPresentationBinding,
    RootTenantBinding, TenantId, TenantPolicy, TenantRecord,
};

const STATE_FILE: &str = "control-state.json";
const STATE_TEMP_FILE: &str = "control-state.json.tmp";
const IDENTITY_FILE: &str = "identity-state.json";
const IDENTITY_TEMP_FILE: &str = "identity-state.json.tmp";
const STORE_LOCK_FILE: &str = "control.lock";
const SESSION_DIRECTORY: &str = "sessions";
/// How often a locker reopens a lock file that was replaced while it waited.
const MAX_LOCK_FILE_REOPENS: usize = 8;

/// Control store for owners and clients on one host.
pub struct LocalControlStore {
    directory: PathBuf,
    sessions: RwLock<BTreeMap<LogicalShardId, OwnerSession>>,
}

/// One live owner session held by this store.
struct OwnerSession {
    lease: LogicalShardLease,
    /// The durable record as of this session's last mutation. No other
    /// process can change it while the session lock is held.
    record: LogicalShardRecord,
    lock: FileLock,
}

/// Where a mutation found the session of the shard it touches.
enum SessionProbe {
    /// This store holds the live session.
    Held(LogicalShardLease),
    /// No session is live. This store holds the lock until the mutation
    /// either installs a session with it or ends.
    Free(FileLock),
    /// Another process holds the live session.
    Foreign,
}

enum SessionChange {
    Unchanged,
    Began(LogicalShardLease, FileLock),
    Ended,
}

impl LocalControlStore {
    /// Use `directory` as the control store. It is created by the first
    /// mutation; until then every record reads as absent.
    pub fn open(directory: impl Into<PathBuf>) -> Result<Self, ControlError> {
        let directory = directory.into();
        if directory.as_os_str().is_empty() {
            return Err(ControlError::InvalidOptions(
                "local control directory must not be empty".to_owned(),
            ));
        }
        Ok(Self {
            directory,
            sessions: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    fn read<T>(
        &self,
        operation: impl FnOnce(&InMemoryControlStore) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let (_, state) = self.read_state()?;
        operation(&InMemoryControlStore::with_state(state))
    }

    /// Run one transition over the durable records under the store lock.
    ///
    /// The records reach disk before a session begins or ends, so a crash
    /// never leaves a session lock without the record that names it, and a
    /// released record is durable before its lock is dropped.
    fn mutate<T>(
        &self,
        shard: Option<&LogicalShardId>,
        operation: impl FnOnce(&InMemoryControlStore) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let _store_lock = self.lock_store()?;
        let (loaded, mut state) = self.read_state()?;
        // A shard without a record has no session to probe; every session
        // transition on it fails in the state machine.
        let probe = shard
            .filter(|shard| state.logical_shards.contains_key(shard))
            .map(|shard| self.probe_session(shard))
            .transpose()?;
        let live = match (shard, &probe) {
            (Some(shard), Some(probe)) => live_session(probe, state.logical_shards.get(shard)),
            _ => None,
        };
        if let (Some(shard), Some(lease)) = (shard, &live) {
            state.owner_sessions.insert(*shard, lease.clone());
        }

        let store = InMemoryControlStore::with_state(state);
        let result = operation(&store);
        let state = store.into_state();
        let value = result?;

        let change = match (shard, probe) {
            (Some(shard), Some(probe)) => session_change(shard, probe, live, &state)?,
            (Some(shard), None) if state.owner_sessions.contains_key(shard) => {
                return Err(ControlError::Backend(
                    "a local owner session began without its lock".to_owned(),
                ));
            }
            _ => SessionChange::Unchanged,
        };
        let encoded = encode_local_control_state(&state)?;
        if encoded != loaded {
            self.write_state(&encoded)?;
        }
        if let Some(shard) = shard {
            self.apply_session_change(shard, change, &state)?;
        }
        Ok(value)
    }

    fn probe_session(&self, shard: &LogicalShardId) -> Result<SessionProbe, ControlError> {
        {
            let mut sessions = self.sessions_mut()?;
            if let Some(session) = sessions.get(shard) {
                if session
                    .lock
                    .is_at_path()
                    .map_err(io_error("check session lock"))?
                {
                    return Ok(SessionProbe::Held(session.lease.clone()));
                }
                // The lock file was replaced under this session: another
                // process may lock the new file, so this session is over.
                sessions.remove(shard);
            }
        }
        Ok(
            match FileLock::try_exclusive(&self.session_lock_path(shard))
                .map_err(io_error("lock owner session"))?
            {
                Some(lock) => SessionProbe::Free(lock),
                None => SessionProbe::Foreign,
            },
        )
    }

    fn apply_session_change(
        &self,
        shard: &LogicalShardId,
        change: SessionChange,
        state: &InMemoryState,
    ) -> Result<(), ControlError> {
        let mut sessions = self.sessions_mut()?;
        match change {
            SessionChange::Unchanged => {
                if let (Some(session), Some(record)) =
                    (sessions.get_mut(shard), state.logical_shards.get(shard))
                {
                    session.record = record.clone();
                }
            }
            SessionChange::Began(lease, lock) => {
                let record = state
                    .logical_shards
                    .get(shard)
                    .cloned()
                    .ok_or(ControlError::LogicalShardNotFound(*shard))?;
                sessions.insert(
                    *shard,
                    OwnerSession {
                        lease,
                        record,
                        lock,
                    },
                );
            }
            SessionChange::Ended => {
                sessions.remove(shard);
            }
        }
        Ok(())
    }

    /// The record of the live session `lease`, if this store holds it.
    fn held_record(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        let sessions = self.sessions.read().map_err(|_| poisoned_session_table())?;
        let session = sessions
            .get(&lease.logical_shard_id)
            .filter(|session| session.lease == *lease)
            .ok_or_else(|| ControlError::StaleLease(lease.clone()))?;
        if !session
            .lock
            .is_at_path()
            .map_err(io_error("check session lock"))?
        {
            return Err(ControlError::StaleLease(lease.clone()));
        }
        validate_record_lease(&session.record, lease)?;
        Ok(session.record.clone())
    }

    fn sessions_mut(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, BTreeMap<LogicalShardId, OwnerSession>>, ControlError>
    {
        self.sessions.write().map_err(|_| poisoned_session_table())
    }

    fn lock_store(&self) -> Result<FileLock, ControlError> {
        let sessions = self.directory.join(SESSION_DIRECTORY);
        if !sessions.is_dir() {
            fs::create_dir_all(&sessions).map_err(io_error("create control directory"))?;
            sync_directory(&self.directory).map_err(io_error("sync control directory"))?;
            if let Some(parent) = self
                .directory
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                sync_directory(parent).map_err(io_error("sync control directory parent"))?;
            }
        }
        FileLock::exclusive(&self.directory.join(STORE_LOCK_FILE))
            .map_err(io_error("lock control store"))
    }

    fn read_state(&self) -> Result<(Vec<u8>, InMemoryState), ControlError> {
        match fs::read(self.directory.join(STATE_FILE)) {
            Ok(bytes) => {
                let state = decode_local_control_state(&bytes)?;
                Ok((bytes, state))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok((Vec::new(), InMemoryState::default()))
            }
            Err(error) => Err(io_error("read control state")(error)),
        }
    }

    fn write_state(&self, encoded: &[u8]) -> Result<(), ControlError> {
        self.replace_file(
            STATE_TEMP_FILE,
            STATE_FILE,
            encoded,
            ("write control state", "replace control state"),
        )
    }

    /// Durably replace `target` with `encoded`: write and sync a temporary
    /// file, rename it over the target, then sync the directory.
    fn replace_file(
        &self,
        temporary: &str,
        target: &str,
        encoded: &[u8],
        (write, replace): (&'static str, &'static str),
    ) -> Result<(), ControlError> {
        let temporary = self.directory.join(temporary);
        let mut file = File::create(&temporary).map_err(io_error(write))?;
        file.write_all(encoded)
            .and_then(|()| file.sync_all())
            .map_err(io_error(write))?;
        drop(file);
        fs::rename(&temporary, self.directory.join(target)).map_err(io_error(replace))?;
        sync_directory(&self.directory).map_err(io_error("sync control directory"))
    }

    fn read_identity(&self) -> Result<(Vec<u8>, IdentityState), ControlError> {
        match fs::read(self.directory.join(IDENTITY_FILE)) {
            Ok(bytes) => {
                let state = decode_local_identity_state(&bytes)?;
                Ok((bytes, state))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Ok((Vec::new(), IdentityState::default()))
            }
            Err(error) => Err(io_error("read identity state")(error)),
        }
    }

    /// Run one identity transition under the store lock and persist its
    /// result if it changed anything.
    fn mutate_identity<T>(
        &self,
        operation: impl FnOnce(&mut IdentityState) -> Result<T, ControlError>,
    ) -> Result<T, ControlError> {
        let _store_lock = self.lock_store()?;
        let (loaded, mut state) = self.read_identity()?;
        let value = operation(&mut state)?;
        let encoded = encode_local_identity_state(&state)?;
        if encoded != loaded {
            self.replace_file(
                IDENTITY_TEMP_FILE,
                IDENTITY_FILE,
                &encoded,
                ("write identity state", "replace identity state"),
            )?;
        }
        Ok(value)
    }

    fn session_lock_path(&self, shard: &LogicalShardId) -> PathBuf {
        self.directory
            .join(SESSION_DIRECTORY)
            .join(format!("{}.lock", encode_fixed_id(shard.as_bytes())))
    }
}

/// The session the shared transitions see for a probed shard.
///
/// A foreign session is live but grants this store no authority: its lease
/// keeps the record's owner and epoch with lease id zero, which no owned
/// record carries, so no caller lease validates against it.
fn live_session(
    probe: &SessionProbe,
    record: Option<&LogicalShardRecord>,
) -> Option<LogicalShardLease> {
    match probe {
        SessionProbe::Held(lease) => Some(lease.clone()),
        SessionProbe::Free(_) => None,
        SessionProbe::Foreign => {
            let record = record?;
            Some(LogicalShardLease {
                logical_shard_id: record.logical_shard_id,
                owner: record.owner.clone()?,
                owner_epoch: record.owner_epoch?,
                lease_id: 0,
            })
        }
    }
}

fn session_change(
    shard: &LogicalShardId,
    probe: SessionProbe,
    live: Option<LogicalShardLease>,
    state: &InMemoryState,
) -> Result<SessionChange, ControlError> {
    let after = state.owner_sessions.get(shard).cloned();
    match probe {
        SessionProbe::Held(lease) => match after {
            Some(after) if after == lease => Ok(SessionChange::Unchanged),
            None => Ok(SessionChange::Ended),
            Some(_) => Err(ControlError::Backend(
                "a held local owner session was replaced without ending".to_owned(),
            )),
        },
        SessionProbe::Free(lock) => Ok(match after {
            Some(lease) => SessionChange::Began(lease, lock),
            None => SessionChange::Unchanged,
        }),
        SessionProbe::Foreign if after == live => Ok(SessionChange::Unchanged),
        // Another process still holds the lock, for example between a
        // durable release and its unlock.
        SessionProbe::Foreign => Err(
            match state
                .logical_shards
                .get(shard)
                .and_then(|record| record.owner_epoch)
            {
                Some(owner_epoch) => ControlError::PreviousOwnerSessionLive {
                    logical_shard_id: *shard,
                    owner_epoch,
                },
                None => ControlError::Backend(
                    "another process holds the session lock of a never-owned logical shard"
                        .to_owned(),
                ),
            },
        ),
    }
}

/// An exclusive `flock` on the file at `path` when it was taken.
struct FileLock {
    _file: File,
    path: PathBuf,
    device: u64,
    inode: u64,
}

impl FileLock {
    fn exclusive(path: &Path) -> io::Result<Self> {
        Self::lock(path, true)?.ok_or_else(|| io::Error::other("blocking lock did not block"))
    }

    /// `None` when another holder has the lock.
    fn try_exclusive(path: &Path) -> io::Result<Option<Self>> {
        Self::lock(path, false)
    }

    fn lock(path: &Path, wait: bool) -> io::Result<Option<Self>> {
        let operation = if wait {
            libc::LOCK_EX
        } else {
            libc::LOCK_EX | libc::LOCK_NB
        };
        for _ in 0..MAX_LOCK_FILE_REOPENS {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?;
            loop {
                // SAFETY: `file` owns the descriptor for the whole call.
                if unsafe { libc::flock(file.as_raw_fd(), operation) } == 0 {
                    break;
                }
                let error = io::Error::last_os_error();
                match error.kind() {
                    io::ErrorKind::Interrupted => {}
                    io::ErrorKind::WouldBlock if !wait => return Ok(None),
                    _ => return Err(error),
                }
            }
            let metadata = file.metadata()?;
            let lock = Self {
                _file: file,
                path: path.to_owned(),
                device: metadata.dev(),
                inode: metadata.ino(),
            };
            // A locker that waited may hold a file that was unlinked and
            // replaced meanwhile; only the file at `path` excludes others.
            if lock.is_at_path()? {
                return Ok(Some(lock));
            }
        }
        Err(io::Error::other(format!(
            "lock file {} keeps being replaced",
            path.display()
        )))
    }

    fn is_at_path(&self) -> io::Result<bool> {
        match fs::metadata(&self.path) {
            Ok(current) => Ok(current.dev() == self.device && current.ino() == self.inode),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn io_error(context: &'static str) -> impl Fn(io::Error) -> ControlError {
    move |error| ControlError::Backend(format!("local control store cannot {context}: {error}"))
}

fn poisoned_session_table() -> ControlError {
    ControlError::Backend("local control session table is poisoned".to_owned())
}

impl IdentityStore for LocalControlStore {
    fn create_tenant(&self, tenant: TenantRecord) -> Result<TenantRecord, ControlError> {
        self.mutate_identity(|state| Ok(state.create_tenant(tenant)))
    }

    fn create_principal(
        &self,
        principal: PrincipalRecord,
    ) -> Result<PrincipalRecord, ControlError> {
        self.mutate_identity(|state| state.create_principal(principal, IdentityLimits::DEFAULT))
    }

    fn add_credential(
        &self,
        credential: CredentialBinding,
    ) -> Result<CredentialBinding, ControlError> {
        self.mutate_identity(|state| state.add_credential(credential, IdentityLimits::DEFAULT))
    }

    fn revoke_credential(
        &self,
        tenant_id: &TenantId,
        fingerprint: &CredentialFingerprint,
    ) -> Result<bool, ControlError> {
        self.mutate_identity(|state| state.revoke_credential(tenant_id, fingerprint))
    }

    fn grant_role(&self, binding: RoleBinding) -> Result<RoleBinding, ControlError> {
        self.mutate_identity(|state| state.grant_role(binding, IdentityLimits::DEFAULT))
    }

    fn revoke_role(&self, binding: &RoleBinding) -> Result<bool, ControlError> {
        self.mutate_identity(|state| state.revoke_role(binding))
    }

    fn bind_root_tenant(
        &self,
        binding: RootTenantBinding,
    ) -> Result<RootTenantBinding, ControlError> {
        self.mutate_identity(|state| state.bind_root_tenant(binding))
    }

    fn get_root_tenant(&self, root_id: &RootId) -> Result<Option<RootTenantBinding>, ControlError> {
        Ok(self.read_identity()?.1.root_tenant(root_id))
    }

    fn bind_root_presentation(
        &self,
        binding: RootPresentationBinding,
    ) -> Result<RootPresentationBinding, ControlError> {
        self.mutate_identity(|state| state.bind_root_presentation(binding))
    }

    fn get_root_presentation(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootPresentationBinding>, ControlError> {
        Ok(self.read_identity()?.1.root_presentation(root_id))
    }

    fn load_tenant_policy(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantPolicy>, ControlError> {
        Ok(self.read_identity()?.1.policy(tenant_id))
    }

    fn tenant_policy_revision(&self, tenant_id: &TenantId) -> Result<Option<u64>, ControlError> {
        Ok(self.read_identity()?.1.revision(tenant_id))
    }
}

impl ControlStore for LocalControlStore {
    /// Sessions end with their process, not with a lease, so nothing renews.
    fn owner_lease_renew_interval(&self) -> Option<std::time::Duration> {
        None
    }

    fn create_root_agent_binding(
        &self,
        binding: RootAgentBinding,
    ) -> Result<RootAgentBinding, ControlError> {
        self.mutate(None, |store| store.create_root_agent_binding(binding))
    }

    fn get_root_agent_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootAgentBinding>, ControlError> {
        self.read(|store| store.get_root_agent_binding(root_id))
    }

    fn create_root_object_namespace_binding(
        &self,
        binding: RootObjectNamespaceBinding,
    ) -> Result<RootObjectNamespaceBinding, ControlError> {
        self.mutate(None, |store| {
            store.create_root_object_namespace_binding(binding)
        })
    }

    fn get_root_object_namespace_binding(
        &self,
        root_id: &RootId,
    ) -> Result<Option<RootObjectNamespaceBinding>, ControlError> {
        self.read(|store| store.get_root_object_namespace_binding(root_id))
    }

    fn create_root_placement(
        &self,
        placement: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        self.mutate(None, |store| store.create_root_placement(placement))
    }

    fn get_root_placement(&self, root_id: &RootId) -> Result<Option<RootPlacement>, ControlError> {
        self.read(|store| store.get_root_placement(root_id))
    }

    fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
        self.read(|store| store.list_root_placements())
    }

    fn compare_and_set_root_placement(
        &self,
        expected: &RootPlacement,
        next: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        self.mutate(None, |store| {
            store.compare_and_set_root_placement(expected, next)
        })
    }

    fn create_logical_shard(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(None, |store| store.create_logical_shard(logical_shard_id))
    }

    fn get_logical_shard(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<LogicalShardRecord>, ControlError> {
        self.read(|store| store.get_logical_shard(logical_shard_id))
    }

    fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
        self.read(|store| store.list_logical_shards())
    }

    fn acquire_owner(
        &self,
        logical_shard_id: &LogicalShardId,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        self.mutate(Some(logical_shard_id), |store| {
            store.acquire_owner(logical_shard_id, owner, endpoint)
        })
    }

    fn acquire_successor(
        &self,
        logical_shard_id: &LogicalShardId,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        self.mutate(Some(logical_shard_id), |store| {
            store.acquire_successor(logical_shard_id, expected_owner_epoch, owner, endpoint)
        })
    }

    fn reacquire_recovery(
        &self,
        logical_shard_id: &LogicalShardId,
        recovery_epoch: OwnerEpoch,
        owner: NodeId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        self.mutate(Some(logical_shard_id), |store| {
            store.reacquire_recovery(logical_shard_id, recovery_epoch, owner, endpoint)
        })
    }

    /// Confirm the held session and that the durable record still names it
    /// exactly as this session last wrote it.
    fn renew_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        let held = self.held_record(lease)?;
        let durable = self
            .get_logical_shard(&lease.logical_shard_id)?
            .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
        validate_record_lease(&durable, lease)?;
        if durable != held {
            return Err(ControlError::Backend(
                "the durable logical shard record changed outside its owner session".to_owned(),
            ));
        }
        Ok(durable)
    }

    /// The held lock already excludes every successor, so the proof reads no
    /// file: it checks this store's session and that its lock file is the one
    /// a successor would have to lock.
    fn prove_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        self.held_record(lease)
    }

    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(Some(&lease.logical_shard_id), |store| {
            store.mark_serving(lease, publication)
        })
    }

    fn prepare_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        intent: RecoveryUploadIntent,
    ) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(Some(&lease.logical_shard_id), |store| {
            store.prepare_recovery_upload(lease, intent)
        })
    }

    fn finalize_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        expected_intent: &RecoveryUploadIntent,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(Some(&lease.logical_shard_id), |store| {
            store.finalize_recovery_upload(lease, expected_intent, publication)
        })
    }

    fn abort_recovery_upload(
        &self,
        lease: &LogicalShardLease,
        expected_intent: &RecoveryUploadIntent,
    ) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(Some(&lease.logical_shard_id), |store| {
            store.abort_recovery_upload(lease, expected_intent)
        })
    }

    fn suspend_recovery(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(Some(&lease.logical_shard_id), |store| {
            store.suspend_recovery(lease)
        })
    }

    fn release_owner(&self, lease: &LogicalShardLease) -> Result<LogicalShardRecord, ControlError> {
        self.mutate(Some(&lease.logical_shard_id), |store| {
            store.release_owner(lease)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LogicalShardState, PlacementGeneration, RootPlacementLifecycle};

    struct LocalHarness {
        store: LocalControlStore,
        _directory: tempfile::TempDir,
    }

    impl crate::conformance::ConformanceHarness for LocalHarness {
        fn store(&self) -> &dyn ControlStore {
            &self.store
        }

        fn lose_session(&self, lease: &LogicalShardLease) {
            // Process death: the kernel drops the lock and the record stays.
            let removed = self
                .store
                .sessions
                .write()
                .unwrap()
                .remove(&lease.logical_shard_id);
            assert_eq!(removed.map(|session| session.lease).as_ref(), Some(lease));
        }

        fn fresh_shard(&self, scenario: u8) -> (RootId, LogicalShardId) {
            (
                RootId::from_bytes([scenario; 16]),
                LogicalShardId::from_bytes([scenario; 16]),
            )
        }
    }

    fn node(name: &str) -> NodeId {
        NodeId::new(name).unwrap()
    }

    fn empty_publication() -> RecoveryPublication {
        RecoveryPublication {
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        }
    }

    fn placed_shard(store: &LocalControlStore, value: u8) -> LogicalShardId {
        let shard = LogicalShardId::from_bytes([value; 16]);
        store.create_logical_shard(shard).unwrap();
        store
            .create_root_placement(RootPlacement {
                root_id: RootId::from_bytes([value; 16]),
                logical_shard_id: shard,
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        shard
    }

    #[test]
    fn local_store_passes_identity_conformance() {
        let directory = tempfile::tempdir().unwrap();
        let next = std::cell::Cell::new(0_u32);
        crate::identity_conformance::run_all(&|scenario| {
            next.set(next.get() + 1);
            scenario(
                &LocalControlStore::open(directory.path().join(format!("control-{}", next.get())))
                    .unwrap(),
            );
        });
    }

    #[test]
    fn identity_records_leave_the_control_state_file_unchanged() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalControlStore::open(directory.path().join("control")).unwrap();
        placed_shard(&store, 3);
        let control_state = fs::read(store.directory().join(STATE_FILE)).unwrap();
        let tenant_id = TenantId::from_bytes([5; 16]);
        store.create_tenant(TenantRecord { tenant_id }).unwrap();
        store
            .bind_root_tenant(RootTenantBinding {
                root_id: RootId::from_bytes([3; 16]),
                tenant_id,
            })
            .unwrap();
        // Readers that predate identity records parse only the control file.
        assert_eq!(
            fs::read(store.directory().join(STATE_FILE)).unwrap(),
            control_state
        );
        let reopened = LocalControlStore::open(store.directory()).unwrap();
        assert_eq!(
            reopened.tenant_policy_revision(&tenant_id).unwrap(),
            Some(1)
        );
        assert_eq!(reopened.list_root_placements().unwrap().len(), 1);
    }

    #[test]
    fn local_store_passes_provider_conformance() {
        let directory = tempfile::tempdir().unwrap();
        crate::conformance::run_all(&LocalHarness {
            store: LocalControlStore::open(directory.path().join("control")).unwrap(),
            _directory: directory,
        });
    }

    #[test]
    fn another_store_sees_a_held_session_as_live_and_cannot_use_its_lease() {
        let directory = tempfile::tempdir().unwrap();
        let first = LocalControlStore::open(directory.path()).unwrap();
        let shard = placed_shard(&first, 1);
        let lease = first
            .acquire_owner(&shard, node("node-a"), "127.0.0.1:7001".to_owned())
            .unwrap();
        first.mark_serving(&lease, empty_publication()).unwrap();

        // A second store takes its own lock descriptions, like a process.
        let second = LocalControlStore::open(directory.path()).unwrap();
        assert!(matches!(
            second.acquire_successor(
                &shard,
                lease.owner_epoch,
                node("node-b"),
                "127.0.0.1:7002".to_owned()
            ),
            Err(ControlError::PreviousOwnerSessionLive { .. })
        ));
        // Knowing the lease grants nothing without holding its lock.
        assert!(matches!(
            second.prove_owner(&lease),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            second.mark_serving(&lease, empty_publication()),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            second.release_owner(&lease),
            Err(ControlError::StaleLease(_))
        ));
        assert_eq!(
            second.get_logical_shard(&shard).unwrap().unwrap().state,
            LogicalShardState::Serving
        );
        first.prove_owner(&lease).unwrap();

        // Dropping the holder releases its lock, as process exit does.
        drop(first);
        let successor = second
            .acquire_successor(
                &shard,
                lease.owner_epoch,
                node("node-b"),
                "127.0.0.1:7002".to_owned(),
            )
            .unwrap();
        assert_eq!(successor.owner_epoch.get(), 2);
        assert!(matches!(
            second.prove_owner(&lease),
            Err(ControlError::StaleLease(_))
        ));
        second
            .mark_serving(&successor, empty_publication())
            .unwrap();
        assert_eq!(
            second.prove_owner(&successor).unwrap().state,
            LogicalShardState::Serving
        );
    }

    #[test]
    fn records_and_the_lease_allocator_survive_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let (shard, first) = {
            let store = LocalControlStore::open(directory.path()).unwrap();
            let shard = placed_shard(&store, 2);
            let lease = store
                .acquire_owner(&shard, node("node-a"), "127.0.0.1:7001".to_owned())
                .unwrap();
            store.mark_serving(&lease, empty_publication()).unwrap();
            store.release_owner(&lease).unwrap();
            (shard, lease)
        };

        let store = LocalControlStore::open(directory.path()).unwrap();
        let record = store.get_logical_shard(&shard).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, Some(first.owner_epoch));
        assert_eq!(store.list_root_placements().unwrap().len(), 1);
        let second = store
            .acquire_successor(
                &shard,
                first.owner_epoch,
                node("node-b"),
                "127.0.0.1:7002".to_owned(),
            )
            .unwrap();
        assert!(second.lease_id > first.lease_id);
    }

    #[test]
    fn a_replaced_session_lock_file_ends_the_session() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalControlStore::open(directory.path()).unwrap();
        let shard = placed_shard(&store, 3);
        let lease = store
            .acquire_owner(&shard, node("node-a"), "127.0.0.1:7001".to_owned())
            .unwrap();
        store.prove_owner(&lease).unwrap();

        // Another process could lock a new file at the same path.
        let path = store.session_lock_path(&shard);
        fs::remove_file(&path).unwrap();
        fs::write(&path, b"").unwrap();

        assert!(matches!(
            store.prove_owner(&lease),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            store.renew_owner(&lease),
            Err(ControlError::StaleLease(_))
        ));
        assert!(matches!(
            store.mark_serving(&lease, empty_publication()),
            Err(ControlError::StaleLease(_))
        ));
    }

    #[test]
    fn renewal_refuses_a_record_changed_outside_the_session() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalControlStore::open(directory.path()).unwrap();
        let shard = placed_shard(&store, 4);
        let lease = store
            .acquire_owner(&shard, node("node-a"), "127.0.0.1:7001".to_owned())
            .unwrap();
        store.renew_owner(&lease).unwrap();

        let (_, mut state) = store.read_state().unwrap();
        state.logical_shards.get_mut(&shard).unwrap().endpoint = Some("127.0.0.1:7999".to_owned());
        store
            .write_state(&encode_local_control_state(&state).unwrap())
            .unwrap();

        assert!(matches!(
            store.renew_owner(&lease),
            Err(ControlError::Backend(_))
        ));
    }

    #[test]
    fn local_state_decoding_accepts_only_its_canonical_encoding() {
        let directory = tempfile::tempdir().unwrap();
        let store = LocalControlStore::open(directory.path()).unwrap();
        let shard = placed_shard(&store, 5);
        store
            .acquire_owner(&shard, node("node-a"), "127.0.0.1:7001".to_owned())
            .unwrap();
        let (encoded, state) = store.read_state().unwrap();
        assert_eq!(encode_local_control_state(&state).unwrap(), encoded);

        let text = String::from_utf8(encoded).unwrap();
        let spaced = text.replacen('{', "{ ", 1);
        assert!(matches!(
            decode_local_control_state(spaced.as_bytes()),
            Err(ControlError::Codec(_))
        ));
        let beyond_allocator = text.replacen("\"next_lease_id\":1", "\"next_lease_id\":0", 1);
        assert_ne!(beyond_allocator, text);
        assert!(matches!(
            decode_local_control_state(beyond_allocator.as_bytes()),
            Err(ControlError::Codec(_))
        ));
        let placement = text
            .split("\"root_placements\":[")
            .nth(1)
            .and_then(|rest| rest.split(']').next())
            .unwrap()
            .to_owned();
        let repeated = text.replacen(&placement, &format!("{placement},{placement}"), 1);
        assert!(matches!(
            decode_local_control_state(repeated.as_bytes()),
            Err(ControlError::Codec(_))
        ));
    }
}
