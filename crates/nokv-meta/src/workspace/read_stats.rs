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
/// store that started the session. `provider_*` fields are deltas of
/// provider-wide cumulative counters. Each optional field is `Some` only when
/// the active provider defines that dimension; `None` means unsupported, not
/// zero. A provider-counter delta is attributable to one workload only when the
/// store is dedicated to it and has no concurrent maintenance. Logical coverage
/// is deliberately limited to fenced query point/range paths; write-transaction
/// and recovery-internal reads are not counted. A range read that fails before
/// iterator completion may not contribute final cursor stats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MetadataReadStats {
    pub point_reads_system: u64,
    pub point_reads_root_fence: u64,
    pub point_reads_workspace_current: u64,
    pub point_reads_path_current: u64,
    pub point_reads_other: u64,
    pub point_hits: u64,
    pub point_misses: u64,
    /// Value bytes returned by successful provider point lookups.
    pub point_value_bytes: u64,
    /// NoKV range-read operations requested by callers.
    pub scan_calls: u64,
    /// Provider cursors opened by those operations. Historical reads may open two.
    pub scan_cursors: u64,
    /// Provider-reported cursor work units, not physical rows or device reads.
    pub scan_visited_units: u64,
    pub scan_returned_keys: u64,
    pub scan_common_prefixes: u64,
    pub scan_restarts: u64,
    /// Key and value bytes emitted by provider range cursors.
    pub scan_key_bytes: u64,
    pub scan_value_bytes: u64,
    /// Cursors stopped because NoKV filled its raw page budget.
    ///
    /// This does not prove that another storage entry exists.
    pub scan_raw_limit_stops: u64,
    /// Provider cache lookups served without a backing-store read.
    pub provider_cache_hits: Option<u64>,
    /// Provider cache lookups that fell through to the backing store.
    pub provider_cache_misses: Option<u64>,
    /// Successful whole-storage-object reads from the backing store.
    pub provider_full_read_operations: Option<u64>,
    /// Bytes returned by whole-storage-object reads.
    pub provider_full_read_bytes: Option<u64>,
    /// Whole-storage-object reads attributed to point paths.
    pub provider_point_full_read_operations: Option<u64>,
    /// Whole-storage-object reads attributed to scan paths.
    pub provider_scan_full_read_operations: Option<u64>,
    /// Whole-storage-object reads attributed to provider-internal paths.
    pub provider_internal_full_read_operations: Option<u64>,
    /// Partial-read cache hits served without a whole-object read.
    pub provider_partial_read_cache_hits: Option<u64>,
    /// Partial-read cache misses that required positional or whole-object reads.
    pub provider_partial_read_cache_misses: Option<u64>,
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

        macro_rules! optional_delta {
            ($field:ident) => {
                optional_counter_delta(stringify!($field), self.$field, earlier.$field)?
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
            scan_visited_units: delta!(scan_visited_units),
            scan_returned_keys: delta!(scan_returned_keys),
            scan_common_prefixes: delta!(scan_common_prefixes),
            scan_restarts: delta!(scan_restarts),
            scan_key_bytes: delta!(scan_key_bytes),
            scan_value_bytes: delta!(scan_value_bytes),
            scan_raw_limit_stops: delta!(scan_raw_limit_stops),
            provider_cache_hits: optional_delta!(provider_cache_hits),
            provider_cache_misses: optional_delta!(provider_cache_misses),
            provider_full_read_operations: optional_delta!(provider_full_read_operations),
            provider_full_read_bytes: optional_delta!(provider_full_read_bytes),
            provider_point_full_read_operations: optional_delta!(
                provider_point_full_read_operations
            ),
            provider_scan_full_read_operations: optional_delta!(provider_scan_full_read_operations),
            provider_internal_full_read_operations: optional_delta!(
                provider_internal_full_read_operations
            ),
            provider_partial_read_cache_hits: optional_delta!(provider_partial_read_cache_hits),
            provider_partial_read_cache_misses: optional_delta!(provider_partial_read_cache_misses),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataReadStatsDeltaError {
    field: &'static str,
    earlier: Option<u64>,
    later: Option<u64>,
}

impl fmt::Display for MetadataReadStatsDeltaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.earlier, self.later) {
            (Some(earlier), Some(later)) => write!(
                formatter,
                "metadata read counter {} decreased from {} to {}",
                self.field, earlier, later
            ),
            (earlier, later) => write!(
                formatter,
                "metadata read counter {} availability changed from {} to {}",
                self.field,
                counter_availability(earlier),
                counter_availability(later)
            ),
        }
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
    Provider(String),
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
            Self::Provider(message) => {
                write!(formatter, "metadata provider stats failed: {message}")
            }
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
            earlier: Some(earlier),
            later: Some(later),
        })
}

fn optional_counter_delta(
    field: &'static str,
    later: Option<u64>,
    earlier: Option<u64>,
) -> Result<Option<u64>, MetadataReadStatsDeltaError> {
    match (later, earlier) {
        (Some(later), Some(earlier)) => counter_delta(field, later, earlier).map(Some),
        (None, None) => Ok(None),
        (later, earlier) => Err(MetadataReadStatsDeltaError {
            field,
            earlier,
            later,
        }),
    }
}

fn counter_availability(value: Option<u64>) -> &'static str {
    if value.is_some() {
        "supported"
    } else {
        "unsupported"
    }
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
        counters.scan_visited_units = counters.scan_visited_units.saturating_add(visited_units);
        counters.scan_returned_keys = counters.scan_returned_keys.saturating_add(returned_keys);
        counters.scan_common_prefixes = counters
            .scan_common_prefixes
            .saturating_add(common_prefixes);
        counters.scan_restarts = counters.scan_restarts.saturating_add(restarts);
        counters.scan_key_bytes = counters.scan_key_bytes.saturating_add(key_bytes);
        counters.scan_value_bytes = counters.scan_value_bytes.saturating_add(value_bytes);
        if stopped_at_limit {
            counters.scan_raw_limit_stops = counters.scan_raw_limit_stops.saturating_add(1);
        }
    });
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
    physical.scan_visited_units = logical.scan_visited_units;
    physical.scan_returned_keys = logical.scan_returned_keys;
    physical.scan_common_prefixes = logical.scan_common_prefixes;
    physical.scan_restarts = logical.scan_restarts;
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
        let earlier = MetadataReadStats {
            provider_cache_hits: Some(0),
            provider_cache_misses: Some(0),
            provider_full_read_operations: Some(0),
            provider_full_read_bytes: Some(0),
            provider_point_full_read_operations: Some(0),
            provider_scan_full_read_operations: Some(0),
            provider_internal_full_read_operations: Some(0),
            provider_partial_read_cache_hits: Some(0),
            provider_partial_read_cache_misses: Some(0),
            ..MetadataReadStats::default()
        };
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
            scan_visited_units: 9,
            scan_returned_keys: 10,
            scan_common_prefixes: 11,
            scan_restarts: 12,
            scan_key_bytes: 13,
            scan_value_bytes: 14,
            scan_raw_limit_stops: 15,
            provider_cache_hits: Some(16),
            provider_cache_misses: Some(17),
            provider_full_read_operations: Some(18),
            provider_full_read_bytes: Some(19),
            provider_point_full_read_operations: Some(20),
            provider_scan_full_read_operations: Some(21),
            provider_internal_full_read_operations: Some(22),
            provider_partial_read_cache_hits: Some(23),
            provider_partial_read_cache_misses: Some(24),
        };

        assert_eq!(later.delta_since(&earlier).unwrap(), later);
        assert_eq!(later.point_reads_total(), 15);
    }

    #[test]
    fn delta_preserves_unsupported_provider_dimensions() {
        let earlier = MetadataReadStats::default();
        let later = MetadataReadStats {
            point_reads_other: 1,
            ..MetadataReadStats::default()
        };

        assert_eq!(
            later.delta_since(&earlier).unwrap(),
            MetadataReadStats {
                point_reads_other: 1,
                ..MetadataReadStats::default()
            }
        );
    }

    #[test]
    fn delta_rejects_provider_counter_availability_changes() {
        let earlier = MetadataReadStats::default();
        let later = MetadataReadStats {
            provider_cache_hits: Some(0),
            ..MetadataReadStats::default()
        };

        let error = later.delta_since(&earlier).unwrap_err();
        assert_eq!(
            error.to_string(),
            "metadata read counter provider_cache_hits availability changed from unsupported to supported"
        );
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
