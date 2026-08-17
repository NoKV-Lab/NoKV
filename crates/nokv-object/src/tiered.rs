use std::sync::{Arc, Mutex};

use crate::{
    ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome, ObjectDeleteOutcome,
    ObjectError, ObjectInfo, ObjectKey, ObjectRange, ProviderAdmissionReceipt,
    ProviderHandleIdentity,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TieredArtifactStoreOptions {
    pub populate_hot_on_read: bool,
}

impl Default for TieredArtifactStoreOptions {
    fn default() -> Self {
        Self {
            populate_hot_on_read: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TieredArtifactStoreStats {
    pub durable_creates: u64,
    pub durable_replays: u64,
    pub hot_populates: u64,
    pub hot_populate_errors: u64,
    pub hot_reads: u64,
    pub hot_hits: u64,
    pub hot_misses: u64,
    pub hot_errors: u64,
    pub durable_reads: u64,
    pub durable_read_bytes: u64,
    pub durable_deletes: u64,
    pub hot_delete_errors: u64,
}

#[derive(Clone, Debug)]
pub struct TieredArtifactStore<Hot, Durable> {
    hot: Hot,
    durable: Durable,
    options: TieredArtifactStoreOptions,
    stats: Arc<Mutex<TieredArtifactStoreStats>>,
}

impl<Hot, Durable> TieredArtifactStore<Hot, Durable> {
    pub fn new(hot: Hot, durable: Durable, options: TieredArtifactStoreOptions) -> Self {
        Self {
            hot,
            durable,
            options,
            stats: Arc::new(Mutex::new(TieredArtifactStoreStats::default())),
        }
    }

    pub fn hot(&self) -> &Hot {
        &self.hot
    }

    pub fn durable(&self) -> &Durable {
        &self.durable
    }

    pub fn stats(&self) -> Result<TieredArtifactStoreStats, ObjectError> {
        self.stats
            .lock()
            .map(|stats| *stats)
            .map_err(ObjectError::poisoned)
    }

    fn record(
        &self,
        update: impl FnOnce(&mut TieredArtifactStoreStats),
    ) -> Result<(), ObjectError> {
        let mut stats = self.stats.lock().map_err(ObjectError::poisoned)?;
        update(&mut stats);
        Ok(())
    }
}

impl<Hot, Durable> TieredArtifactStore<Hot, Durable>
where
    Hot: ArtifactObjectStore,
    Durable: ArtifactObjectStore,
{
    fn populate_hot(&self, key: &ObjectKey, bytes: &[u8]) -> Result<(), ObjectError> {
        let result = match self.hot.create_immutable(key, bytes) {
            Ok(_) => Ok(()),
            Err(ObjectError::ImmutableCollision { .. }) => {
                let _ = self.hot.delete(key);
                self.hot.create_immutable(key, bytes).map(|_| ())
            }
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => self.record(|stats| {
                stats.hot_populates = stats.hot_populates.saturating_add(1);
            }),
            Err(_) => self.record(|stats| {
                stats.hot_populate_errors = stats.hot_populate_errors.saturating_add(1);
            }),
        }
    }
}

impl<Hot, Durable> ArtifactObjectStore for TieredArtifactStore<Hot, Durable>
where
    Hot: ArtifactObjectStore,
    Durable: ArtifactObjectStore,
{
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        self.durable.capabilities()
    }

    fn provider_handle_identity(&self) -> ProviderHandleIdentity {
        self.durable.provider_handle_identity()
    }

    fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
        self.durable.provider_admission_receipt()
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        let outcome = self.durable.create_immutable(key, bytes)?;
        self.record(|stats| match outcome {
            ImmutableCreateOutcome::Created => {
                stats.durable_creates = stats.durable_creates.saturating_add(1);
            }
            ImmutableCreateOutcome::Replayed => {
                stats.durable_replays = stats.durable_replays.saturating_add(1);
            }
        })?;
        self.populate_hot(key, bytes)?;
        Ok(outcome)
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.record(|stats| {
            stats.hot_reads = stats.hot_reads.saturating_add(1);
        })?;
        match self.hot.read(key, range) {
            Ok(bytes) => {
                self.record(|stats| {
                    stats.hot_hits = stats.hot_hits.saturating_add(1);
                })?;
                return Ok(bytes);
            }
            Err(ObjectError::ObjectNotFound { .. }) => {
                self.record(|stats| {
                    stats.hot_misses = stats.hot_misses.saturating_add(1);
                })?;
            }
            Err(_) => {
                self.record(|stats| {
                    stats.hot_errors = stats.hot_errors.saturating_add(1);
                })?;
            }
        }

        let bytes = self.durable.read(key, range)?;
        self.record(|stats| {
            stats.durable_reads = stats.durable_reads.saturating_add(1);
            stats.durable_read_bytes = stats.durable_read_bytes.saturating_add(bytes.len() as u64);
        })?;
        if self.options.populate_hot_on_read && range.is_none() {
            self.populate_hot(key, &bytes)?;
        }
        Ok(bytes)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.durable.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        let outcome = self.durable.delete(key)?;
        if outcome == ObjectDeleteOutcome::Deleted {
            self.record(|stats| {
                stats.durable_deletes = stats.durable_deletes.saturating_add(1);
            })?;
        }
        if self.hot.delete(key).is_err() {
            self.record(|stats| {
                stats.hot_delete_errors = stats.hot_delete_errors.saturating_add(1);
            })?;
        }
        Ok(outcome)
    }
}
