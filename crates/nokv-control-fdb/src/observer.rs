/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use nokv_control::{LogicalShardId, OwnershipSnapshot};

#[derive(Clone)]
struct ObservedOwnership {
    snapshot: OwnershipSnapshot,
    unchanged_since: Duration,
}

/// Process-local monotonic observation state. Durable values never contain a
/// local timestamp, and no wall clock participates in takeover correctness.
#[derive(Default)]
pub(crate) struct OwnershipObserver {
    observations: Mutex<BTreeMap<LogicalShardId, ObservedOwnership>>,
}

impl OwnershipObserver {
    pub(crate) fn record(&self, snapshot: &OwnershipSnapshot, now: Duration) {
        let shard = snapshot.route().logical_shard_id();
        let mut observations = self
            .observations
            .lock()
            .expect("ownership observation mutex poisoned");
        match observations.get_mut(&shard) {
            Some(observed) if observed.snapshot == *snapshot => {}
            Some(observed) => {
                *observed = ObservedOwnership {
                    snapshot: snapshot.clone(),
                    unchanged_since: now,
                };
            }
            None => {
                observations.insert(
                    shard,
                    ObservedOwnership {
                        snapshot: snapshot.clone(),
                        unchanged_since: now,
                    },
                );
            }
        }
    }

    /// Return the remaining TTL. `None` means the exact snapshot has remained
    /// unchanged long enough to contend.
    pub(crate) fn remaining(
        &self,
        snapshot: &OwnershipSnapshot,
        now: Duration,
        ttl: Duration,
    ) -> Option<Duration> {
        let shard = snapshot.route().logical_shard_id();
        let mut observations = self
            .observations
            .lock()
            .expect("ownership observation mutex poisoned");
        let observed = observations
            .entry(shard)
            .or_insert_with(|| ObservedOwnership {
                snapshot: snapshot.clone(),
                unchanged_since: now,
            });
        if observed.snapshot != *snapshot {
            *observed = ObservedOwnership {
                snapshot: snapshot.clone(),
                unchanged_since: now,
            };
            return Some(ttl);
        }
        let elapsed = now.saturating_sub(observed.unchanged_since);
        if elapsed >= ttl {
            None
        } else {
            Some(ttl - elapsed)
        }
    }
}
