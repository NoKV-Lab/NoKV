use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::digest::{hex, sha256};
use crate::store::{
    strict_range, ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome,
    ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange,
};
use crate::ProviderHandleIdentity;

static TEMP_OBJECT_ID: AtomicU64 = AtomicU64::new(1);
const INTERNAL_TEMP_DIR: &str = ".nokv-artifact-temp";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalHotTierOptions {
    pub root: PathBuf,
    pub max_bytes: u64,
    pub sync_on_create: bool,
}

impl LocalHotTierOptions {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        Self {
            root: root.into(),
            max_bytes,
            sync_on_create: false,
        }
    }

    pub fn with_sync_on_create(mut self, sync_on_create: bool) -> Self {
        self.sync_on_create = sync_on_create;
        self
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LocalHotTierStats {
    pub resident_objects: u64,
    pub resident_bytes: u64,
    pub creates: u64,
    pub replays: u64,
    pub reads: u64,
    pub read_bytes: u64,
    pub evictions: u64,
    pub eviction_bytes: u64,
    pub admission_rejections: u64,
    pub deletes: u64,
}

#[derive(Clone, Debug)]
pub struct LocalHotTier {
    root: Arc<PathBuf>,
    max_bytes: u64,
    sync_on_create: bool,
    state: Arc<Mutex<LocalHotTierState>>,
    handle_identity: ProviderHandleIdentity,
}

#[derive(Clone, Debug, Default)]
struct LocalHotTierState {
    residents: BTreeMap<ObjectKey, LocalResidency>,
    access_clock: u64,
    stats: LocalHotTierStats,
}

#[derive(Clone, Copy, Debug)]
struct LocalResidency {
    bytes: u64,
    last_access: u64,
}

impl LocalHotTier {
    pub fn new(options: LocalHotTierOptions) -> Result<Self, ObjectError> {
        fs::create_dir_all(&options.root).map_err(ObjectError::backend)?;
        fs::create_dir_all(options.root.join(INTERNAL_TEMP_DIR)).map_err(ObjectError::backend)?;
        let state = scan_residents(&options.root)?;
        let tier = Self {
            root: Arc::new(options.root),
            max_bytes: options.max_bytes,
            sync_on_create: options.sync_on_create,
            state: Arc::new(Mutex::new(state)),
            handle_identity: ProviderHandleIdentity::new(),
        };
        tier.enforce_capacity()?;
        Ok(tier)
    }

    pub fn stats(&self) -> Result<LocalHotTierStats, ObjectError> {
        self.state
            .lock()
            .map(|state| state.stats)
            .map_err(ObjectError::poisoned)
    }

    fn object_path(&self, key: &ObjectKey) -> PathBuf {
        self.root.join(key.as_str())
    }

    fn reconcile_existing(
        &self,
        key: &ObjectKey,
        expected: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        match fs::read(self.object_path(key)) {
            Ok(actual) if actual == expected => {
                self.record_resident(key.clone(), actual.len() as u64, false)?;
                let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
                state.stats.replays = state.stats.replays.saturating_add(1);
                Ok(ImmutableCreateOutcome::Replayed)
            }
            Ok(actual) => Err(ObjectError::ImmutableCollision {
                key: key.clone(),
                expected_sha256: hex(&sha256(expected)),
                actual_sha256: hex(&sha256(&actual)),
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: "create-if-absent reported a collision but the object is absent"
                        .to_owned(),
                })
            }
            Err(error) => Err(ObjectError::CreateAmbiguous {
                key: key.clone(),
                detail: format!("could not reconcile existing local hot object: {error}"),
            }),
        }
    }

    fn record_resident(
        &self,
        key: ObjectKey,
        bytes: u64,
        created: bool,
    ) -> Result<(), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        state.access_clock = state.access_clock.saturating_add(1);
        let last_access = state.access_clock;
        if let Some(previous) = state
            .residents
            .insert(key, LocalResidency { bytes, last_access })
        {
            state.stats.resident_bytes = state.stats.resident_bytes.saturating_sub(previous.bytes);
        } else {
            state.stats.resident_objects = state.stats.resident_objects.saturating_add(1);
        }
        state.stats.resident_bytes = state.stats.resident_bytes.saturating_add(bytes);
        if created {
            state.stats.creates = state.stats.creates.saturating_add(1);
        }
        drop(state);
        self.enforce_capacity()
    }

    fn touch(&self, key: &ObjectKey, bytes: u64) -> Result<(), ObjectError> {
        self.record_resident(key.clone(), bytes, false)
    }

    fn forget(&self, key: &ObjectKey) -> Result<(), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        if let Some(previous) = state.residents.remove(key) {
            state.stats.resident_objects = state.stats.resident_objects.saturating_sub(1);
            state.stats.resident_bytes = state.stats.resident_bytes.saturating_sub(previous.bytes);
        }
        Ok(())
    }

    fn enforce_capacity(&self) -> Result<(), ObjectError> {
        let victims = {
            let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
            let mut victims = Vec::new();
            while state.stats.resident_bytes > self.max_bytes {
                let Some((key, residency)) = state
                    .residents
                    .iter()
                    .min_by_key(|(_, residency)| residency.last_access)
                    .map(|(key, residency)| (key.clone(), *residency))
                else {
                    break;
                };
                state.residents.remove(&key);
                state.stats.resident_objects = state.stats.resident_objects.saturating_sub(1);
                state.stats.resident_bytes =
                    state.stats.resident_bytes.saturating_sub(residency.bytes);
                state.stats.evictions = state.stats.evictions.saturating_add(1);
                state.stats.eviction_bytes =
                    state.stats.eviction_bytes.saturating_add(residency.bytes);
                victims.push(key);
            }
            victims
        };
        for victim in victims {
            match fs::remove_file(self.object_path(&victim)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(ObjectError::backend(error)),
            }
        }
        Ok(())
    }

    fn ensure_parent(path: &Path) -> Result<(), ObjectError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(ObjectError::backend)?;
        }
        Ok(())
    }
}

impl ArtifactObjectStore for LocalHotTier {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        ArtifactStoreCapabilities::default()
    }

    fn provider_handle_identity(&self) -> ProviderHandleIdentity {
        self.handle_identity
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        if bytes.len() as u64 > self.max_bytes {
            let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
            state.stats.admission_rejections = state.stats.admission_rejections.saturating_add(1);
            return Err(ObjectError::backend_failure(
                format!(
                    "local hot admission rejected {} bytes with capacity {}",
                    bytes.len(),
                    self.max_bytes
                ),
                false,
            ));
        }

        let destination = self.object_path(key);
        Self::ensure_parent(&destination)?;
        if destination.exists() {
            return self.reconcile_existing(key, bytes);
        }

        let temp_id = TEMP_OBJECT_ID.fetch_add(1, Ordering::Relaxed);
        let temporary = self
            .root
            .join(INTERNAL_TEMP_DIR)
            .join(format!("{temp_id:016x}"));
        let create_result = (|| {
            let mut output = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(ObjectError::backend)?;
            output.write_all(bytes).map_err(ObjectError::backend)?;
            if self.sync_on_create {
                output.sync_all().map_err(ObjectError::backend)?;
            }
            match fs::hard_link(&temporary, &destination) {
                Ok(()) => Ok(ImmutableCreateOutcome::Created),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    self.reconcile_existing(key, bytes)
                }
                Err(error) => Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: error.to_string(),
                }),
            }
        })();
        let _ = fs::remove_file(&temporary);
        if create_result == Ok(ImmutableCreateOutcome::Created) {
            if let Err(error) = self.record_resident(key.clone(), bytes.len() as u64, true) {
                return Err(ObjectError::CreateAmbiguous {
                    key: key.clone(),
                    detail: format!("object was created but hot-tier accounting failed: {error}"),
                });
            }
        }
        create_result
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let bytes = match fs::read(self.object_path(key)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.forget(key)?;
                return Err(ObjectError::ObjectNotFound { key: key.clone() });
            }
            Err(error) => return Err(ObjectError::backend(error)),
        };
        let result = strict_range(&bytes, range)?;
        self.touch(key, bytes.len() as u64)?;
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        state.stats.reads = state.stats.reads.saturating_add(1);
        state.stats.read_bytes = state.stats.read_bytes.saturating_add(result.len() as u64);
        Ok(result)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match fs::metadata(self.object_path(key)) {
            Ok(metadata) => {
                self.touch(key, metadata.len())?;
                Ok(Some(ObjectInfo {
                    key: key.clone(),
                    size: metadata.len(),
                }))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.forget(key)?;
                Ok(None)
            }
            Err(error) => Err(ObjectError::backend(error)),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        match fs::remove_file(self.object_path(key)) {
            Ok(()) => {
                self.forget(key)?;
                let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
                state.stats.deletes = state.stats.deletes.saturating_add(1);
                Ok(ObjectDeleteOutcome::Deleted)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.forget(key)?;
                Ok(ObjectDeleteOutcome::Absent)
            }
            Err(error) => Err(ObjectError::DeleteAmbiguous {
                key: key.clone(),
                detail: error.to_string(),
            }),
        }
    }
}

fn scan_residents(root: &Path) -> Result<LocalHotTierState, ObjectError> {
    let mut state = LocalHotTierState::default();
    scan_resident_directory(root, root, &mut state)?;
    Ok(state)
}

fn scan_resident_directory(
    root: &Path,
    directory: &Path,
    state: &mut LocalHotTierState,
) -> Result<(), ObjectError> {
    for entry in fs::read_dir(directory).map_err(ObjectError::backend)? {
        let entry = entry.map_err(ObjectError::backend)?;
        let path = entry.path();
        if path == root.join(INTERNAL_TEMP_DIR) {
            continue;
        }
        let metadata = entry.metadata().map_err(ObjectError::backend)?;
        if metadata.is_dir() {
            scan_resident_directory(root, &path, state)?;
            continue;
        }
        if !metadata.is_file() {
            continue;
        }
        let relative = path
            .strip_prefix(root)
            .map_err(ObjectError::backend)?
            .components()
            .map(|component| component.as_os_str().to_str())
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| ObjectError::backend_failure("local hot key is not valid UTF-8", false))?
            .join("/");
        let key = ObjectKey::new(relative)?;
        state.access_clock = state.access_clock.saturating_add(1);
        state.residents.insert(
            key,
            LocalResidency {
                bytes: metadata.len(),
                last_access: state.access_clock,
            },
        );
        state.stats.resident_objects = state.stats.resident_objects.saturating_add(1);
        state.stats.resident_bytes = state.stats.resident_bytes.saturating_add(metadata.len());
    }
    Ok(())
}
