/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

#[cfg(feature = "metadata-read-stats")]
use std::cell::RefCell;

#[cfg(feature = "metadata-read-stats")]
use super::engine::MetadataPointReadSource;

/// Storage-independent metadata read counters from one diagnostic session.
///
/// Counters are thread-local and include reads through clones of the store that
/// started the session. Coverage is limited to fenced query point and range
/// paths; write-transaction and recovery-internal reads are not counted. A
/// range read that fails before iterator completion may not contribute final
/// byte counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataReadStats {
    pub point_reads_system: u64,
    pub point_reads_root_fence: u64,
    pub point_reads_workspace_current: u64,
    pub point_reads_path_current: u64,
    pub point_reads_other: u64,
    pub point_hits: u64,
    pub point_misses: u64,
    /// Value bytes returned by successful metadata point reads.
    pub point_value_bytes: u64,
    /// NoKV range-read operations requested by callers.
    pub scan_calls: u64,
    /// Key and value bytes emitted by range reads.
    pub scan_key_bytes: u64,
    pub scan_value_bytes: u64,
    /// Cursors stopped because NoKV filled its raw page budget.
    ///
    /// This does not prove that another storage entry exists.
    pub scan_raw_limit_stops: u64,
}

impl MetadataReadStats {
    #[must_use]
    pub fn point_reads_total(&self) -> u64 {
        self.point_reads_system
            .saturating_add(self.point_reads_root_fence)
            .saturating_add(self.point_reads_workspace_current)
            .saturating_add(self.point_reads_path_current)
            .saturating_add(self.point_reads_other)
    }
}

/// Holt-specific read diagnostics from one metadata read session.
///
/// Cursor counters cover Holt iterators opened by NoKV. Storage counters are
/// deltas from the shared database's cumulative counters. A storage-counter
/// delta belongs to one workload only when the database has no concurrent
/// activity.
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
    /// lifetimes (or were supplied in reverse order), so silently saturating
    /// would produce invalid benchmark evidence.
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

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataReadStatsSessionError {
    ThreadSessionAlreadyActive,
    ThreadSessionMissing,
    ThreadSessionStoreMismatch,
    StoreSessionAlreadyActive,
}

impl fmt::Display for MetadataReadStatsSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ThreadSessionAlreadyActive => formatter
                .write_str("a metadata read-stats session is already active on this thread"),
            Self::ThreadSessionMissing => {
                formatter.write_str("the metadata read-stats session is not active on this thread")
            }
            Self::ThreadSessionStoreMismatch => formatter
                .write_str("the active metadata read-stats session belongs to another store"),
            Self::StoreSessionAlreadyActive => formatter
                .write_str("a metadata read-stats session is already active for this store"),
        }
    }
}

impl std::error::Error for MetadataReadStatsSessionError {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum HoltReadStatsSessionError {
    ThreadSessionAlreadyActive,
    ThreadSessionMissing,
    ThreadSessionStoreMismatch,
    StoreSessionAlreadyActive,
    CounterRegression(HoltReadStatsDeltaError),
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
        }
    }
}

impl std::error::Error for HoltReadStatsSessionError {}

impl From<HoltReadStatsDeltaError> for HoltReadStatsSessionError {
    fn from(error: HoltReadStatsDeltaError) -> Self {
        Self::CounterRegression(error)
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

#[cfg(feature = "metadata-read-stats")]
struct ActiveMetadataReadStats {
    store_key: usize,
    counters: MetadataReadStats,
}

#[cfg(feature = "metadata-read-stats")]
struct ActiveHoltReadStats {
    store_key: usize,
    counters: HoltReadStats,
}

#[cfg(feature = "metadata-read-stats")]
thread_local! {
    static ACTIVE_METADATA_READ_STATS: RefCell<Option<ActiveMetadataReadStats>> = const {
        RefCell::new(None)
    };
    static ACTIVE_HOLT_READ_STATS: RefCell<Option<ActiveHoltReadStats>> = const {
        RefCell::new(None)
    };
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn begin_session(store_key: usize) -> Result<(), MetadataReadStatsSessionError> {
    ACTIVE_METADATA_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return Err(MetadataReadStatsSessionError::ThreadSessionAlreadyActive);
        }
        *slot = Some(ActiveMetadataReadStats {
            store_key,
            counters: MetadataReadStats::default(),
        });
        Ok(())
    })
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn finish_session(
    store_key: usize,
) -> Result<MetadataReadStats, MetadataReadStatsSessionError> {
    ACTIVE_METADATA_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        let active = slot
            .take()
            .ok_or(MetadataReadStatsSessionError::ThreadSessionMissing)?;
        if active.store_key != store_key {
            *slot = Some(active);
            return Err(MetadataReadStatsSessionError::ThreadSessionStoreMismatch);
        }
        Ok(active.counters)
    })
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn cancel_session(store_key: usize) {
    let _ = ACTIVE_METADATA_READ_STATS.try_with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|active| active.store_key == store_key)
        {
            *slot = None;
        }
    });
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn begin_holt_session(store_key: usize) -> Result<(), HoltReadStatsSessionError> {
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

#[cfg(feature = "metadata-read-stats")]
pub(super) fn finish_holt_session(
    store_key: usize,
) -> Result<HoltReadStats, HoltReadStatsSessionError> {
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

#[cfg(feature = "metadata-read-stats")]
pub(super) fn cancel_holt_session(store_key: usize) {
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

#[cfg(feature = "metadata-read-stats")]
fn with_active_metadata_counters(store_key: usize, record: impl FnOnce(&mut MetadataReadStats)) {
    ACTIVE_METADATA_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(active) = slot.as_mut() else {
            return;
        };
        if active.store_key == store_key {
            record(&mut active.counters);
        }
    });
}

#[cfg(feature = "metadata-read-stats")]
fn with_active_holt_counters(store_key: usize, record: impl FnOnce(&mut HoltReadStats)) {
    ACTIVE_HOLT_READ_STATS.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(active) = slot.as_mut() else {
            return;
        };
        if active.store_key == store_key {
            record(&mut active.counters);
        }
    });
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn record_point(
    store_key: usize,
    source: MetadataPointReadSource,
    value_bytes: Option<usize>,
) {
    with_active_metadata_counters(store_key, |counters| {
        let source_counter = match source {
            MetadataPointReadSource::System => &mut counters.point_reads_system,
            MetadataPointReadSource::RootFence => &mut counters.point_reads_root_fence,
            MetadataPointReadSource::WorkspaceCurrent => {
                &mut counters.point_reads_workspace_current
            }
            MetadataPointReadSource::PathCurrent => &mut counters.point_reads_path_current,
            MetadataPointReadSource::Other => &mut counters.point_reads_other,
        };
        *source_counter = source_counter.saturating_add(1);
        match value_bytes {
            Some(bytes) => {
                counters.point_hits = counters.point_hits.saturating_add(1);
                counters.point_value_bytes = counters
                    .point_value_bytes
                    .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
            }
            None => counters.point_misses = counters.point_misses.saturating_add(1),
        }
    });
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn record_scan_call(store_key: usize) {
    with_active_metadata_counters(store_key, |counters| {
        counters.scan_calls = counters.scan_calls.saturating_add(1);
    });
}

#[cfg(feature = "metadata-read-stats")]
#[allow(clippy::too_many_arguments)]
pub(super) fn record_scan_cursor(
    store_key: usize,
    visited_units: u64,
    returned_keys: u64,
    common_prefixes: u64,
    restarts: u64,
    key_bytes: u64,
    value_bytes: u64,
    stopped_at_limit: bool,
) {
    with_active_holt_counters(store_key, |counters| {
        counters.scan_cursors = counters.scan_cursors.saturating_add(1);
        counters.scan_visited_units = counters.scan_visited_units.saturating_add(visited_units);
        counters.scan_returned_keys = counters.scan_returned_keys.saturating_add(returned_keys);
        counters.scan_common_prefixes = counters
            .scan_common_prefixes
            .saturating_add(common_prefixes);
        counters.scan_restarts = counters.scan_restarts.saturating_add(restarts);
    });
    with_active_metadata_counters(store_key, |counters| {
        counters.scan_key_bytes = counters.scan_key_bytes.saturating_add(key_bytes);
        counters.scan_value_bytes = counters.scan_value_bytes.saturating_add(value_bytes);
        if stopped_at_limit {
            counters.scan_raw_limit_stops = counters.scan_raw_limit_stops.saturating_add(1);
        }
    });
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn merge_holt_cursor_stats(storage: &mut HoltReadStats, cursor: HoltReadStats) {
    storage.scan_cursors = cursor.scan_cursors;
    storage.scan_visited_units = cursor.scan_visited_units;
    storage.scan_returned_keys = cursor.scan_returned_keys;
    storage.scan_common_prefixes = cursor.scan_common_prefixes;
    storage.scan_restarts = cursor.scan_restarts;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holt_delta_rejects_counter_regression() {
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
    fn holt_delta_preserves_every_counter() {
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

    #[cfg(feature = "metadata-read-stats")]
    #[test]
    fn session_is_store_scoped_and_rejects_thread_local_nesting() {
        begin_session(7).unwrap();
        record_point(7, MetadataPointReadSource::PathCurrent, Some(12));
        record_point(8, MetadataPointReadSource::Other, Some(99));
        assert!(begin_session(7)
            .unwrap_err()
            .to_string()
            .contains("already active"));

        let stats = finish_session(7).unwrap();
        assert_eq!(stats.point_reads_path_current, 1);
        assert_eq!(stats.point_reads_other, 0);
        assert_eq!(stats.point_value_bytes, 12);

        begin_session(8).unwrap();
        cancel_session(8);
        begin_session(9).unwrap();
        assert_eq!(finish_session(9).unwrap(), MetadataReadStats::default());
    }

    #[cfg(feature = "metadata-read-stats")]
    #[test]
    fn metadata_and_holt_sessions_collect_independently() {
        begin_session(7).unwrap();
        begin_holt_session(7).unwrap();
        record_scan_call(7);
        record_scan_cursor(7, 2, 3, 4, 5, 6, 7, true);

        let metadata = finish_session(7).unwrap();
        assert_eq!(metadata.scan_calls, 1);
        assert_eq!(metadata.scan_key_bytes, 6);
        assert_eq!(metadata.scan_value_bytes, 7);
        assert_eq!(metadata.scan_raw_limit_stops, 1);

        let holt = finish_holt_session(7).unwrap();
        assert_eq!(holt.scan_cursors, 1);
        assert_eq!(holt.scan_visited_units, 2);
        assert_eq!(holt.scan_returned_keys, 3);
        assert_eq!(holt.scan_common_prefixes, 4);
        assert_eq!(holt.scan_restarts, 5);
    }
}
