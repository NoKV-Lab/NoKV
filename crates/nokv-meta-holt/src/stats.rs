/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::cell::RefCell;
use std::fmt;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use holt::{DBStats, ScanStats};
use nokv_meta_store::StoreError;

use crate::HoltStore;

/// Holt read diagnostics captured by one explicit session.
///
/// Cursor counters cover Holt iterators opened through [`HoltStore`]. Storage
/// counters are deltas from cumulative counters shared by the store. A storage
/// delta belongs to one workload only when the store has no concurrent work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HoltReadStats {
    pub scan_cursors: u64,
    /// Holt cursor work units, not physical rows or device reads.
    pub scan_visited_units: u64,
    pub scan_returned_keys: u64,
    pub scan_common_prefixes: u64,
    pub scan_restarts: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub full_blob_reads: u64,
    pub full_blob_read_bytes: u64,
    pub point_full_blob_reads: u64,
    pub scan_full_blob_reads: u64,
    pub silent_full_blob_reads: u64,
    pub read_page_hits: u64,
    pub read_page_misses: u64,
    pub read_index_cache_hits: u64,
    pub read_index_cache_misses: u64,
    pub read_index_loads: u64,
    pub read_index_dir_read_bytes: u64,
    pub read_index_bucket_reads: u64,
    pub read_index_bucket_read_bytes: u64,
    pub read_index_inline_hits: u64,
    pub read_index_value_hits: u64,
    pub read_index_value_read_bytes: u64,
    pub read_index_offset_hits: u64,
    pub read_index_negative_hits: u64,
    pub read_index_crossing_hits: u64,
    pub read_index_unknowns: u64,
    pub optimistic_restarts: u64,
    pub range_restarts: u64,
}

impl HoltReadStats {
    /// Return a checked counter delta from an earlier storage snapshot.
    ///
    /// A decreasing field means the snapshots came from different store
    /// lifetimes or were supplied in reverse order.
    pub fn delta_since(&self, earlier: &Self) -> Result<Self, HoltReadStatsDeltaError> {
        macro_rules! delta {
            ($field:ident) => {
                counter_delta(stringify!($field), self.$field, earlier.$field)?
            };
        }

        Ok(Self {
            scan_cursors: delta!(scan_cursors),
            scan_visited_units: delta!(scan_visited_units),
            scan_returned_keys: delta!(scan_returned_keys),
            scan_common_prefixes: delta!(scan_common_prefixes),
            scan_restarts: delta!(scan_restarts),
            cache_hits: delta!(cache_hits),
            cache_misses: delta!(cache_misses),
            full_blob_reads: delta!(full_blob_reads),
            full_blob_read_bytes: delta!(full_blob_read_bytes),
            point_full_blob_reads: delta!(point_full_blob_reads),
            scan_full_blob_reads: delta!(scan_full_blob_reads),
            silent_full_blob_reads: delta!(silent_full_blob_reads),
            read_page_hits: delta!(read_page_hits),
            read_page_misses: delta!(read_page_misses),
            read_index_cache_hits: delta!(read_index_cache_hits),
            read_index_cache_misses: delta!(read_index_cache_misses),
            read_index_loads: delta!(read_index_loads),
            read_index_dir_read_bytes: delta!(read_index_dir_read_bytes),
            read_index_bucket_reads: delta!(read_index_bucket_reads),
            read_index_bucket_read_bytes: delta!(read_index_bucket_read_bytes),
            read_index_inline_hits: delta!(read_index_inline_hits),
            read_index_value_hits: delta!(read_index_value_hits),
            read_index_value_read_bytes: delta!(read_index_value_read_bytes),
            read_index_offset_hits: delta!(read_index_offset_hits),
            read_index_negative_hits: delta!(read_index_negative_hits),
            read_index_crossing_hits: delta!(read_index_crossing_hits),
            read_index_unknowns: delta!(read_index_unknowns),
            optimistic_restarts: delta!(optimistic_restarts),
            range_restarts: delta!(range_restarts),
        })
    }
}

/// One Holt cumulative counter moved backwards between session snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoltReadStatsDeltaError {
    field: &'static str,
    earlier: u64,
    later: u64,
}

impl fmt::Display for HoltReadStatsDeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Holt read counter {} decreased from {} to {}",
            self.field, self.earlier, self.later
        )
    }
}

impl std::error::Error for HoltReadStatsDeltaError {}

/// Failure to start or finish a Holt read-statistics session.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HoltReadStatsSessionError {
    ThreadSessionAlreadyActive,
    ThreadSessionMissing,
    ThreadSessionStoreMismatch,
    StoreSessionAlreadyActive,
    CounterRegression(HoltReadStatsDeltaError),
    Store(StoreError),
}

impl fmt::Display for HoltReadStatsSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ThreadSessionAlreadyActive => {
                formatter.write_str("a Holt read-stats session is already active on this thread")
            }
            Self::ThreadSessionMissing => {
                formatter.write_str("the Holt read-stats session is not active on this thread")
            }
            Self::ThreadSessionStoreMismatch => {
                formatter.write_str("the active Holt read-stats session belongs to another store")
            }
            Self::StoreSessionAlreadyActive => {
                formatter.write_str("a Holt read-stats session is already active for this store")
            }
            Self::CounterRegression(error) => error.fmt(formatter),
            Self::Store(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HoltReadStatsSessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CounterRegression(error) => Some(error),
            Self::Store(error) => Some(error),
            _ => None,
        }
    }
}

impl From<HoltReadStatsDeltaError> for HoltReadStatsSessionError {
    fn from(error: HoltReadStatsDeltaError) -> Self {
        Self::CounterRegression(error)
    }
}

impl From<StoreError> for HoltReadStatsSessionError {
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

fn counter_delta(
    field: &'static str,
    later: u64,
    earlier: u64,
) -> Result<u64, HoltReadStatsDeltaError> {
    later.checked_sub(earlier).ok_or(HoltReadStatsDeltaError {
        field,
        earlier,
        later,
    })
}

#[derive(Default)]
pub(crate) struct ReadStatsState {
    active: AtomicBool,
}

impl ReadStatsState {
    fn claim(&self) -> Result<(), HoltReadStatsSessionError> {
        self.active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map(|_| ())
            .map_err(|_| HoltReadStatsSessionError::StoreSessionAlreadyActive)
    }

    fn release(&self) {
        self.active.store(false, Ordering::Release);
    }
}

struct ActiveHoltReadStats {
    store_key: usize,
    counters: HoltReadStats,
}

thread_local! {
    static ACTIVE_HOLT_READ_STATS: RefCell<Option<ActiveHoltReadStats>> = const {
        RefCell::new(None)
    };
}

fn begin_session(store_key: usize) -> Result<(), HoltReadStatsSessionError> {
    ACTIVE_HOLT_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return Err(HoltReadStatsSessionError::ThreadSessionAlreadyActive);
        }
        *slot = Some(ActiveHoltReadStats {
            store_key,
            counters: HoltReadStats::default(),
        });
        Ok(())
    })
}

fn finish_session(store_key: usize) -> Result<HoltReadStats, HoltReadStatsSessionError> {
    ACTIVE_HOLT_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        let active = slot
            .take()
            .ok_or(HoltReadStatsSessionError::ThreadSessionMissing)?;
        if active.store_key != store_key {
            *slot = Some(active);
            return Err(HoltReadStatsSessionError::ThreadSessionStoreMismatch);
        }
        Ok(active.counters)
    })
}

fn cancel_session(store_key: usize) {
    let _ = ACTIVE_HOLT_READ_STATS.try_with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|active| active.store_key == store_key)
        {
            *slot = None;
        }
    });
}

pub(crate) fn record_scan(store_key: usize, scan: ScanStats) {
    ACTIVE_HOLT_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(active) = slot.as_mut() else {
            return;
        };
        if active.store_key != store_key {
            return;
        }
        active.counters.scan_cursors = active.counters.scan_cursors.saturating_add(1);
        active.counters.scan_visited_units = active
            .counters
            .scan_visited_units
            .saturating_add(scan.visited);
        active.counters.scan_returned_keys = active
            .counters
            .scan_returned_keys
            .saturating_add(scan.returned);
        active.counters.scan_common_prefixes = active
            .counters
            .scan_common_prefixes
            .saturating_add(scan.rollup);
        active.counters.scan_restarts = active.counters.scan_restarts.saturating_add(scan.restarts);
    });
}

pub(crate) fn storage_snapshot(storage: &DBStats) -> HoltReadStats {
    HoltReadStats {
        cache_hits: storage.bm_cache_hits,
        cache_misses: storage.bm_cache_misses,
        full_blob_reads: storage.bm_full_blob_reads,
        full_blob_read_bytes: storage.bm_full_blob_read_bytes,
        point_full_blob_reads: storage.bm_point_full_blob_reads,
        scan_full_blob_reads: storage.bm_scan_full_blob_reads,
        silent_full_blob_reads: storage.bm_silent_full_blob_reads,
        read_page_hits: storage.bm_read_page_hits,
        read_page_misses: storage.bm_read_page_misses,
        read_index_cache_hits: storage.bm_read_index_cache_hits,
        read_index_cache_misses: storage.bm_read_index_cache_misses,
        read_index_loads: storage.bm_read_index_loads,
        read_index_dir_read_bytes: storage.bm_read_index_dir_read_bytes,
        read_index_bucket_reads: storage.bm_read_index_bucket_reads,
        read_index_bucket_read_bytes: storage.bm_read_index_bucket_read_bytes,
        read_index_inline_hits: storage.bm_read_index_inline_hits,
        read_index_value_hits: storage.bm_read_index_value_hits,
        read_index_value_read_bytes: storage.bm_read_index_value_read_bytes,
        read_index_offset_hits: storage.bm_read_index_offset_hits,
        read_index_negative_hits: storage.bm_read_index_negative_hits,
        read_index_crossing_hits: storage.bm_read_index_crossing_hits,
        read_index_unknowns: storage.bm_read_index_unknowns,
        optimistic_restarts: storage.bm_optimistic_restarts,
        range_restarts: storage.bm_range_restarts,
        ..HoltReadStats::default()
    }
}

fn merge_cursor_stats(storage: &mut HoltReadStats, cursor: HoltReadStats) {
    storage.scan_cursors = cursor.scan_cursors;
    storage.scan_visited_units = cursor.scan_visited_units;
    storage.scan_returned_keys = cursor.scan_returned_keys;
    storage.scan_common_prefixes = cursor.scan_common_prefixes;
    storage.scan_restarts = cursor.scan_restarts;
}

/// Thread-bound Holt cursor counters plus store-wide Holt I/O telemetry.
#[must_use = "finish the session to obtain counters, or drop it to cancel collection"]
pub struct HoltReadStatsSession<'a> {
    store: &'a HoltStore,
    store_key: usize,
    storage_before: HoltReadStats,
    active: bool,
    not_send: PhantomData<Rc<()>>,
}

impl HoltReadStatsSession<'_> {
    pub fn finish(mut self) -> Result<HoltReadStats, HoltReadStatsSessionError> {
        let cursor = finish_session(self.store_key)?;
        let mut storage = self
            .store
            .read_stats_snapshot()?
            .delta_since(&self.storage_before)?;
        merge_cursor_stats(&mut storage, cursor);
        self.release();
        Ok(storage)
    }

    fn release(&mut self) {
        self.active = false;
        self.store.read_stats.release();
    }
}

impl Drop for HoltReadStatsSession<'_> {
    fn drop(&mut self) {
        if self.active {
            cancel_session(self.store_key);
            self.release();
        }
    }
}

impl HoltStore {
    /// Start an explicit Holt read-statistics session.
    ///
    /// Only one session may be active for this store. The guard must be
    /// finished on the thread that started it. Session setup and finish can be
    /// placed outside a benchmark's timed interval.
    pub fn begin_read_stats_session(
        &self,
    ) -> Result<HoltReadStatsSession<'_>, HoltReadStatsSessionError> {
        self.read_stats.claim()?;
        let store_key = self.read_stats_store_key();
        let storage_before = match self.read_stats_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.read_stats.release();
                return Err(error.into());
            }
        };
        if let Err(error) = begin_session(store_key) {
            self.read_stats.release();
            return Err(error);
        }
        Ok(HoltReadStatsSession {
            store: self,
            store_key,
            storage_before,
            active: true,
            not_send: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_rejects_counter_regression() {
        let earlier = HoltReadStats {
            cache_hits: 2,
            ..HoltReadStats::default()
        };
        let later = HoltReadStats {
            cache_hits: 1,
            ..HoltReadStats::default()
        };

        let error = later.delta_since(&earlier).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Holt read counter cache_hits decreased from 2 to 1"
        );
    }

    #[test]
    fn delta_preserves_every_counter() {
        let earlier = HoltReadStats::default();
        let later = HoltReadStats {
            scan_cursors: 1,
            scan_visited_units: 2,
            scan_returned_keys: 3,
            scan_common_prefixes: 4,
            scan_restarts: 5,
            cache_hits: 6,
            cache_misses: 7,
            full_blob_reads: 8,
            full_blob_read_bytes: 9,
            point_full_blob_reads: 10,
            scan_full_blob_reads: 11,
            silent_full_blob_reads: 12,
            read_page_hits: 13,
            read_page_misses: 14,
            read_index_cache_hits: 15,
            read_index_cache_misses: 16,
            read_index_loads: 17,
            read_index_dir_read_bytes: 18,
            read_index_bucket_reads: 19,
            read_index_bucket_read_bytes: 20,
            read_index_inline_hits: 21,
            read_index_value_hits: 22,
            read_index_value_read_bytes: 23,
            read_index_offset_hits: 24,
            read_index_negative_hits: 25,
            read_index_crossing_hits: 26,
            read_index_unknowns: 27,
            optimistic_restarts: 28,
            range_restarts: 29,
        };

        assert_eq!(later.delta_since(&earlier).unwrap(), later);
    }
}
