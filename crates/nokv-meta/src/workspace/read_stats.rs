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

#[cfg(feature = "metadata-read-stats")]
struct ActiveMetadataReadStats {
    store_key: usize,
    counters: MetadataReadStats,
}

#[cfg(feature = "metadata-read-stats")]
thread_local! {
    static ACTIVE_METADATA_READ_STATS: RefCell<Option<ActiveMetadataReadStats>> = const {
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
pub(super) fn record_scan_result(
    store_key: usize,
    key_bytes: u64,
    value_bytes: u64,
    stopped_at_limit: bool,
) {
    with_active_metadata_counters(store_key, |counters| {
        counters.scan_key_bytes = counters.scan_key_bytes.saturating_add(key_bytes);
        counters.scan_value_bytes = counters.scan_value_bytes.saturating_add(value_bytes);
        if stopped_at_limit {
            counters.scan_raw_limit_stops = counters.scan_raw_limit_stops.saturating_add(1);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn scan_results_record_only_storage_independent_counters() {
        begin_session(7).unwrap();
        record_scan_call(7);
        record_scan_result(7, 6, 7, true);

        let metadata = finish_session(7).unwrap();
        assert_eq!(metadata.scan_calls, 1);
        assert_eq!(metadata.scan_key_bytes, 6);
        assert_eq!(metadata.scan_value_bytes, 7);
        assert_eq!(metadata.scan_raw_limit_stops, 1);
    }
}
