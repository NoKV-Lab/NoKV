/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::{AtomicU64, Ordering};

use nokv_meta_store::{Commit, StoreError};

/// Read-only counters for transactions dispatched by one [`crate::FdbStore`].
///
/// Counters never participate in transaction planning, validation, retry, or
/// result selection. A snapshot may therefore be sampled by qualification and
/// observability code without changing store semantics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FdbStoreDiagnostics {
    pub commit_attempts: u64,
    pub commits_applied: u64,
    pub commit_conflicts: u64,
    pub commit_errors: u64,
    pub approximate_size_observations: u64,
    pub last_approximate_size_bytes: u64,
    pub max_approximate_size_bytes: u64,
    pub physical_guard_rejections: u64,
}

#[derive(Default)]
pub(crate) struct FdbStoreDiagnosticCounters {
    commit_attempts: AtomicU64,
    commits_applied: AtomicU64,
    commit_conflicts: AtomicU64,
    commit_errors: AtomicU64,
    approximate_size_observations: AtomicU64,
    last_approximate_size_bytes: AtomicU64,
    max_approximate_size_bytes: AtomicU64,
    physical_guard_rejections: AtomicU64,
}

impl FdbStoreDiagnosticCounters {
    pub(crate) fn record_attempt(&self) {
        self.commit_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_approximate_size(&self, bytes: i64) {
        let Ok(bytes) = u64::try_from(bytes) else {
            return;
        };
        self.approximate_size_observations
            .fetch_add(1, Ordering::Relaxed);
        self.last_approximate_size_bytes
            .store(bytes, Ordering::Relaxed);
        self.max_approximate_size_bytes
            .fetch_max(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_physical_guard_rejection(&self) {
        self.physical_guard_rejections
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_result(&self, result: &Result<Commit, StoreError>) {
        let counter = match result {
            Ok(Commit::Applied) => &self.commits_applied,
            Ok(Commit::Conflict) => &self.commit_conflicts,
            Err(_) => &self.commit_errors,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> FdbStoreDiagnostics {
        FdbStoreDiagnostics {
            commit_attempts: self.commit_attempts.load(Ordering::Relaxed),
            commits_applied: self.commits_applied.load(Ordering::Relaxed),
            commit_conflicts: self.commit_conflicts.load(Ordering::Relaxed),
            commit_errors: self.commit_errors.load(Ordering::Relaxed),
            approximate_size_observations: self
                .approximate_size_observations
                .load(Ordering::Relaxed),
            last_approximate_size_bytes: self.last_approximate_size_bytes.load(Ordering::Relaxed),
            max_approximate_size_bytes: self.max_approximate_size_bytes.load(Ordering::Relaxed),
            physical_guard_rejections: self.physical_guard_rejections.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use nokv_meta_store::LimitKind;

    use super::*;

    #[test]
    fn counters_are_monotonic_and_do_not_reclassify_results() {
        let counters = FdbStoreDiagnosticCounters::default();
        for result in [Ok(Commit::Applied), Ok(Commit::Conflict)] {
            counters.record_attempt();
            counters.record_result(&result);
        }
        counters.record_attempt();
        counters.record_approximate_size(41);
        counters.record_approximate_size(17);
        counters.record_physical_guard_rejection();
        counters.record_result(&Err(StoreError::LimitExceeded {
            kind: LimitKind::TransactionBytes,
            actual: 42,
            maximum: 40,
        }));

        assert_eq!(
            counters.snapshot(),
            FdbStoreDiagnostics {
                commit_attempts: 3,
                commits_applied: 1,
                commit_conflicts: 1,
                commit_errors: 1,
                approximate_size_observations: 2,
                last_approximate_size_bytes: 17,
                max_approximate_size_bytes: 41,
                physical_guard_rejections: 1,
            }
        );
    }

    #[test]
    fn invalid_approximate_size_is_not_published_as_unsigned_evidence() {
        let counters = FdbStoreDiagnosticCounters::default();
        counters.record_approximate_size(-1);
        assert_eq!(counters.snapshot(), FdbStoreDiagnostics::default());
    }
}
