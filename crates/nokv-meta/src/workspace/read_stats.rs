/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

#[cfg(feature = "metadata-read-stats")]
use std::cell::RefCell;

#[cfg(feature = "metadata-read-stats")]
use super::engine::MetadataPointReadSource;

/// Metadata read counters captured by one explicit diagnostic session.
///
/// Logical counters are thread-local and include reads through clones of the
/// store that started the session. The `holt_*` fields are mapped from the
/// shared database's cumulative counters; no Holt type crosses the `nokv-meta`
/// package boundary. A physical-counter delta is attributable to one workload
/// only when the store is dedicated to it and has no concurrent maintenance.
/// Logical coverage is deliberately limited to fenced query point/range paths;
/// write-transaction and recovery-internal reads are not counted. A range read
/// that fails before iterator completion may not contribute final cursor stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataReadStats {
    pub point_reads_system: u64,
    pub point_reads_root_fence: u64,
    pub point_reads_workspace_current: u64,
    pub point_reads_path_current: u64,
    pub point_reads_other: u64,
    pub point_hits: u64,
    pub point_misses: u64,
    /// Value bytes returned by successful Holt point lookups.
    pub point_value_bytes: u64,
    /// NoKV range-read operations requested by callers.
    pub scan_calls: u64,
    /// Holt cursors opened by those operations. Historical reads may open two.
    pub scan_cursors: u64,
    /// Holt cursor work units, not physical rows or device reads.
    pub holt_scan_visited_units: u64,
    pub holt_scan_returned_keys: u64,
    pub holt_scan_common_prefixes: u64,
    pub holt_scan_restarts: u64,
    /// Key and value bytes emitted by Holt range cursors.
    pub scan_key_bytes: u64,
    pub scan_value_bytes: u64,
    /// Cursors stopped because NoKV filled its raw page budget.
    ///
    /// This does not prove that another storage entry exists.
    pub scan_raw_limit_stops: u64,
    pub holt_cache_hits: u64,
    pub holt_cache_misses: u64,
    pub holt_full_blob_reads: u64,
    pub holt_full_blob_read_bytes: u64,
    pub holt_point_full_blob_reads: u64,
    pub holt_scan_full_blob_reads: u64,
    pub holt_silent_full_blob_reads: u64,
    pub holt_read_page_hits: u64,
    pub holt_read_page_misses: u64,
    pub holt_read_index_cache_hits: u64,
    pub holt_read_index_cache_misses: u64,
    pub holt_read_index_loads: u64,
    pub holt_read_index_dir_read_bytes: u64,
    pub holt_read_index_bucket_reads: u64,
    pub holt_read_index_bucket_read_bytes: u64,
    pub holt_read_index_inline_hits: u64,
    pub holt_read_index_value_hits: u64,
    pub holt_read_index_value_read_bytes: u64,
    pub holt_read_index_offset_hits: u64,
    pub holt_read_index_negative_hits: u64,
    pub holt_read_index_crossing_hits: u64,
    pub holt_read_index_unknowns: u64,
    pub holt_optimistic_restarts: u64,
    pub holt_range_restarts: u64,
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

    /// Return a checked counter delta from an earlier snapshot.
    ///
    /// A decreasing field means the snapshots came from different store
    /// lifetimes (or were supplied in reverse order), so silently saturating
    /// would produce invalid benchmark evidence.
    pub fn delta_since(&self, earlier: &Self) -> Result<Self, MetadataReadStatsDeltaError> {
        macro_rules! delta {
            ($field:ident) => {
                counter_delta(stringify!($field), self.$field, earlier.$field)?
            };
        }

        Ok(Self {
            point_reads_system: delta!(point_reads_system),
            point_reads_root_fence: delta!(point_reads_root_fence),
            point_reads_workspace_current: delta!(point_reads_workspace_current),
            point_reads_path_current: delta!(point_reads_path_current),
            point_reads_other: delta!(point_reads_other),
            point_hits: delta!(point_hits),
            point_misses: delta!(point_misses),
            point_value_bytes: delta!(point_value_bytes),
            scan_calls: delta!(scan_calls),
            scan_cursors: delta!(scan_cursors),
            holt_scan_visited_units: delta!(holt_scan_visited_units),
            holt_scan_returned_keys: delta!(holt_scan_returned_keys),
            holt_scan_common_prefixes: delta!(holt_scan_common_prefixes),
            holt_scan_restarts: delta!(holt_scan_restarts),
            scan_key_bytes: delta!(scan_key_bytes),
            scan_value_bytes: delta!(scan_value_bytes),
            scan_raw_limit_stops: delta!(scan_raw_limit_stops),
            holt_cache_hits: delta!(holt_cache_hits),
            holt_cache_misses: delta!(holt_cache_misses),
            holt_full_blob_reads: delta!(holt_full_blob_reads),
            holt_full_blob_read_bytes: delta!(holt_full_blob_read_bytes),
            holt_point_full_blob_reads: delta!(holt_point_full_blob_reads),
            holt_scan_full_blob_reads: delta!(holt_scan_full_blob_reads),
            holt_silent_full_blob_reads: delta!(holt_silent_full_blob_reads),
            holt_read_page_hits: delta!(holt_read_page_hits),
            holt_read_page_misses: delta!(holt_read_page_misses),
            holt_read_index_cache_hits: delta!(holt_read_index_cache_hits),
            holt_read_index_cache_misses: delta!(holt_read_index_cache_misses),
            holt_read_index_loads: delta!(holt_read_index_loads),
            holt_read_index_dir_read_bytes: delta!(holt_read_index_dir_read_bytes),
            holt_read_index_bucket_reads: delta!(holt_read_index_bucket_reads),
            holt_read_index_bucket_read_bytes: delta!(holt_read_index_bucket_read_bytes),
            holt_read_index_inline_hits: delta!(holt_read_index_inline_hits),
            holt_read_index_value_hits: delta!(holt_read_index_value_hits),
            holt_read_index_value_read_bytes: delta!(holt_read_index_value_read_bytes),
            holt_read_index_offset_hits: delta!(holt_read_index_offset_hits),
            holt_read_index_negative_hits: delta!(holt_read_index_negative_hits),
            holt_read_index_crossing_hits: delta!(holt_read_index_crossing_hits),
            holt_read_index_unknowns: delta!(holt_read_index_unknowns),
            holt_optimistic_restarts: delta!(holt_optimistic_restarts),
            holt_range_restarts: delta!(holt_range_restarts),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataReadStatsDeltaError {
    field: &'static str,
    earlier: u64,
    later: u64,
}

impl fmt::Display for MetadataReadStatsDeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "metadata read counter {} decreased from {} to {}",
            self.field, self.earlier, self.later
        )
    }
}

impl std::error::Error for MetadataReadStatsDeltaError {}

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetadataReadStatsSessionError {
    ThreadSessionAlreadyActive,
    ThreadSessionMissing,
    ThreadSessionStoreMismatch,
    StoreSessionAlreadyActive,
    CounterRegression(MetadataReadStatsDeltaError),
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
            Self::CounterRegression(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for MetadataReadStatsSessionError {}

impl From<MetadataReadStatsDeltaError> for MetadataReadStatsSessionError {
    fn from(error: MetadataReadStatsDeltaError) -> Self {
        Self::CounterRegression(error)
    }
}

fn counter_delta(
    field: &'static str,
    later: u64,
    earlier: u64,
) -> Result<u64, MetadataReadStatsDeltaError> {
    later
        .checked_sub(earlier)
        .ok_or(MetadataReadStatsDeltaError {
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
thread_local! {
    static ACTIVE_READ_STATS: RefCell<Option<ActiveMetadataReadStats>> = const {
        RefCell::new(None)
    };
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn begin_session(store_key: usize) -> Result<(), MetadataReadStatsSessionError> {
    ACTIVE_READ_STATS.with(|slot| {
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
    ACTIVE_READ_STATS.with(|slot| {
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
    let _ = ACTIVE_READ_STATS.try_with(|slot| {
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
fn with_active_counters(store_key: usize, record: impl FnOnce(&mut MetadataReadStats)) {
    ACTIVE_READ_STATS.with(|slot| {
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
    with_active_counters(store_key, |counters| {
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
    with_active_counters(store_key, |counters| {
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
    with_active_counters(store_key, |counters| {
        counters.scan_cursors = counters.scan_cursors.saturating_add(1);
        counters.holt_scan_visited_units = counters
            .holt_scan_visited_units
            .saturating_add(visited_units);
        counters.holt_scan_returned_keys = counters
            .holt_scan_returned_keys
            .saturating_add(returned_keys);
        counters.holt_scan_common_prefixes = counters
            .holt_scan_common_prefixes
            .saturating_add(common_prefixes);
        counters.holt_scan_restarts = counters.holt_scan_restarts.saturating_add(restarts);
        counters.scan_key_bytes = counters.scan_key_bytes.saturating_add(key_bytes);
        counters.scan_value_bytes = counters.scan_value_bytes.saturating_add(value_bytes);
        if stopped_at_limit {
            counters.scan_raw_limit_stops = counters.scan_raw_limit_stops.saturating_add(1);
        }
    });
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn storage_snapshot(storage: &holt::DBStats) -> MetadataReadStats {
    MetadataReadStats {
        holt_cache_hits: storage.bm_cache_hits,
        holt_cache_misses: storage.bm_cache_misses,
        holt_full_blob_reads: storage.bm_full_blob_reads,
        holt_full_blob_read_bytes: storage.bm_full_blob_read_bytes,
        holt_point_full_blob_reads: storage.bm_point_full_blob_reads,
        holt_scan_full_blob_reads: storage.bm_scan_full_blob_reads,
        holt_silent_full_blob_reads: storage.bm_silent_full_blob_reads,
        holt_read_page_hits: storage.bm_read_page_hits,
        holt_read_page_misses: storage.bm_read_page_misses,
        holt_read_index_cache_hits: storage.bm_read_index_cache_hits,
        holt_read_index_cache_misses: storage.bm_read_index_cache_misses,
        holt_read_index_loads: storage.bm_read_index_loads,
        holt_read_index_dir_read_bytes: storage.bm_read_index_dir_read_bytes,
        holt_read_index_bucket_reads: storage.bm_read_index_bucket_reads,
        holt_read_index_bucket_read_bytes: storage.bm_read_index_bucket_read_bytes,
        holt_read_index_inline_hits: storage.bm_read_index_inline_hits,
        holt_read_index_value_hits: storage.bm_read_index_value_hits,
        holt_read_index_value_read_bytes: storage.bm_read_index_value_read_bytes,
        holt_read_index_offset_hits: storage.bm_read_index_offset_hits,
        holt_read_index_negative_hits: storage.bm_read_index_negative_hits,
        holt_read_index_crossing_hits: storage.bm_read_index_crossing_hits,
        holt_read_index_unknowns: storage.bm_read_index_unknowns,
        holt_optimistic_restarts: storage.bm_optimistic_restarts,
        holt_range_restarts: storage.bm_range_restarts,
        ..MetadataReadStats::default()
    }
}

#[cfg(feature = "metadata-read-stats")]
pub(super) fn merge_logical_counters(physical: &mut MetadataReadStats, logical: MetadataReadStats) {
    physical.point_reads_system = logical.point_reads_system;
    physical.point_reads_root_fence = logical.point_reads_root_fence;
    physical.point_reads_workspace_current = logical.point_reads_workspace_current;
    physical.point_reads_path_current = logical.point_reads_path_current;
    physical.point_reads_other = logical.point_reads_other;
    physical.point_hits = logical.point_hits;
    physical.point_misses = logical.point_misses;
    physical.point_value_bytes = logical.point_value_bytes;
    physical.scan_calls = logical.scan_calls;
    physical.scan_cursors = logical.scan_cursors;
    physical.holt_scan_visited_units = logical.holt_scan_visited_units;
    physical.holt_scan_returned_keys = logical.holt_scan_returned_keys;
    physical.holt_scan_common_prefixes = logical.holt_scan_common_prefixes;
    physical.holt_scan_restarts = logical.holt_scan_restarts;
    physical.scan_key_bytes = logical.scan_key_bytes;
    physical.scan_value_bytes = logical.scan_value_bytes;
    physical.scan_raw_limit_stops = logical.scan_raw_limit_stops;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_rejects_counter_regression() {
        let earlier = MetadataReadStats {
            point_reads_path_current: 2,
            ..MetadataReadStats::default()
        };
        let later = MetadataReadStats {
            point_reads_path_current: 1,
            ..MetadataReadStats::default()
        };

        let error = later.delta_since(&earlier).unwrap_err();
        assert_eq!(
            error.to_string(),
            "metadata read counter point_reads_path_current decreased from 2 to 1"
        );
    }

    #[test]
    fn delta_preserves_every_counter_family() {
        let earlier = MetadataReadStats::default();
        let later = MetadataReadStats {
            point_reads_system: 1,
            point_reads_root_fence: 2,
            point_reads_workspace_current: 3,
            point_reads_path_current: 4,
            point_reads_other: 5,
            point_hits: 14,
            point_misses: 1,
            point_value_bytes: 6,
            scan_calls: 7,
            scan_cursors: 8,
            holt_scan_visited_units: 9,
            holt_scan_returned_keys: 10,
            holt_scan_common_prefixes: 11,
            holt_scan_restarts: 12,
            scan_key_bytes: 13,
            scan_value_bytes: 14,
            scan_raw_limit_stops: 15,
            holt_cache_hits: 16,
            holt_cache_misses: 17,
            holt_full_blob_reads: 18,
            holt_full_blob_read_bytes: 19,
            holt_point_full_blob_reads: 20,
            holt_scan_full_blob_reads: 21,
            holt_silent_full_blob_reads: 22,
            holt_read_page_hits: 23,
            holt_read_page_misses: 24,
            holt_read_index_cache_hits: 25,
            holt_read_index_cache_misses: 26,
            holt_read_index_loads: 27,
            holt_read_index_dir_read_bytes: 28,
            holt_read_index_bucket_reads: 29,
            holt_read_index_bucket_read_bytes: 30,
            holt_read_index_inline_hits: 31,
            holt_read_index_value_hits: 32,
            holt_read_index_value_read_bytes: 33,
            holt_read_index_offset_hits: 34,
            holt_read_index_negative_hits: 35,
            holt_read_index_crossing_hits: 36,
            holt_read_index_unknowns: 37,
            holt_optimistic_restarts: 38,
            holt_range_restarts: 39,
        };

        assert_eq!(later.delta_since(&earlier).unwrap(), later);
        assert_eq!(later.point_reads_total(), 15);
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
}
