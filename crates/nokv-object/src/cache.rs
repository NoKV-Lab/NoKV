use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use crate::{ObjectError, ObjectKey};

pub trait ArtifactBlockCache {
    fn get(&self, key: &ObjectKey) -> Result<Option<Vec<u8>>, ObjectError>;
    fn insert(&self, key: ObjectKey, bytes: Vec<u8>) -> Result<(), ObjectError>;
    fn remove(&self, key: &ObjectKey) -> Result<bool, ObjectError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryArtifactCacheOptions {
    pub max_bytes: u64,
    pub max_entries: usize,
}

impl Default for MemoryArtifactCacheOptions {
    fn default() -> Self {
        Self {
            max_bytes: 256 * 1024 * 1024,
            max_entries: 8 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ArtifactCacheStats {
    pub entries: usize,
    pub bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub inserts: u64,
    pub insert_bytes: u64,
    pub evictions: u64,
    pub eviction_bytes: u64,
    pub removals: u64,
}

#[derive(Clone, Debug)]
pub struct MemoryArtifactCache {
    state: Arc<Mutex<MemoryArtifactCacheState>>,
}

#[derive(Clone, Debug)]
struct MemoryArtifactCacheState {
    options: MemoryArtifactCacheOptions,
    entries: HashMap<ObjectKey, Vec<u8>>,
    recency: VecDeque<ObjectKey>,
    stats: ArtifactCacheStats,
}

impl MemoryArtifactCache {
    pub fn new(options: MemoryArtifactCacheOptions) -> Self {
        Self {
            state: Arc::new(Mutex::new(MemoryArtifactCacheState {
                options,
                entries: HashMap::new(),
                recency: VecDeque::new(),
                stats: ArtifactCacheStats::default(),
            })),
        }
    }

    pub fn stats(&self) -> Result<ArtifactCacheStats, ObjectError> {
        self.state
            .lock()
            .map(|state| state.stats)
            .map_err(ObjectError::poisoned)
    }
}

impl Default for MemoryArtifactCache {
    fn default() -> Self {
        Self::new(MemoryArtifactCacheOptions::default())
    }
}

impl ArtifactBlockCache for MemoryArtifactCache {
    fn get(&self, key: &ObjectKey) -> Result<Option<Vec<u8>>, ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        let Some(bytes) = state.entries.get(key).cloned() else {
            state.stats.misses = state.stats.misses.saturating_add(1);
            return Ok(None);
        };
        state.stats.hits = state.stats.hits.saturating_add(1);
        touch(&mut state.recency, key);
        Ok(Some(bytes))
    }

    fn insert(&self, key: ObjectKey, bytes: Vec<u8>) -> Result<(), ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        let bytes_len = bytes.len() as u64;
        if state.options.max_entries == 0
            || state.options.max_bytes == 0
            || bytes_len > state.options.max_bytes
        {
            return Ok(());
        }
        if let Some(previous) = state.entries.insert(key.clone(), bytes) {
            state.stats.bytes = state.stats.bytes.saturating_sub(previous.len() as u64);
        } else {
            state.stats.entries = state.stats.entries.saturating_add(1);
        }
        state.stats.bytes = state.stats.bytes.saturating_add(bytes_len);
        state.stats.inserts = state.stats.inserts.saturating_add(1);
        state.stats.insert_bytes = state.stats.insert_bytes.saturating_add(bytes_len);
        touch(&mut state.recency, &key);

        while state.stats.entries > state.options.max_entries
            || state.stats.bytes > state.options.max_bytes
        {
            let Some(victim) = state.recency.pop_front() else {
                break;
            };
            if let Some(evicted) = state.entries.remove(&victim) {
                let evicted_len = evicted.len() as u64;
                state.stats.entries = state.stats.entries.saturating_sub(1);
                state.stats.bytes = state.stats.bytes.saturating_sub(evicted_len);
                state.stats.evictions = state.stats.evictions.saturating_add(1);
                state.stats.eviction_bytes = state.stats.eviction_bytes.saturating_add(evicted_len);
            }
        }
        Ok(())
    }

    fn remove(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        let Some(removed) = state.entries.remove(key) else {
            return Ok(false);
        };
        state.stats.entries = state.stats.entries.saturating_sub(1);
        state.stats.bytes = state.stats.bytes.saturating_sub(removed.len() as u64);
        state.stats.removals = state.stats.removals.saturating_add(1);
        state.recency.retain(|candidate| candidate != key);
        Ok(true)
    }
}

fn touch(recency: &mut VecDeque<ObjectKey>, key: &ObjectKey) {
    recency.retain(|candidate| candidate != key);
    recency.push_back(key.clone());
}
