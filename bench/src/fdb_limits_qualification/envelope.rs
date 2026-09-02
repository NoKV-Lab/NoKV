/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_meta_fdb::{FdbStore, FdbStoreDiagnostics, FDB_PHYSICAL_TRANSACTION_GUARD_BYTES};
use nokv_meta_store::{Commit, Key, Keyspace, Mutation, StoreLimits, TxnStore, WriteTxn};
use serde::Serialize;

const QUALIFICATION_KEYSPACE: Keyspace = Keyspace::new(0x7e09);

#[derive(Clone, Debug, Serialize)]
pub(crate) struct DiagnosticsEvidence {
    commit_attempts: u64,
    commits_applied: u64,
    commit_conflicts: u64,
    commit_errors: u64,
    approximate_size_observations: u64,
    last_approximate_size_bytes: u64,
    max_approximate_size_bytes: u64,
    physical_guard_rejections: u64,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct EnvelopePoint {
    target_logical_bytes: usize,
    logical_request_bytes: usize,
    conservative_affected_bytes: usize,
    observed_approximate_physical_bytes: u64,
    mutation_count: usize,
    outcome: &'static str,
    diagnostics_delta: DiagnosticsEvidence,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct EnvelopeReport {
    schema: &'static str,
    transaction_target_bytes: usize,
    logical_transaction_limit_bytes: usize,
    physical_guard_bytes: usize,
    maximum_observed_approximate_physical_bytes: u64,
    planner_target_observed_approximate_physical_bytes: u64,
    final_diagnostics: DiagnosticsEvidence,
    points: Vec<EnvelopePoint>,
}

pub(crate) fn run(store: &FdbStore) -> Result<EnvelopeReport, String> {
    let profile = store.profile();
    let target = profile.transaction_target_bytes;
    let logical_limit = profile.limits.max_transaction_bytes;
    let mut targets = vec![64 * 1024, 256 * 1024, target, target * 2];
    targets.push(logical_limit.saturating_sub(100_000));
    targets.sort_unstable();
    targets.dedup();
    if targets
        .iter()
        .any(|bytes| *bytes == 0 || *bytes > logical_limit)
    {
        return Err("Gate 9 transaction points exceed the advertised logical limit".to_owned());
    }

    let mut points = Vec::with_capacity(targets.len());
    for (case, target_bytes) in targets.into_iter().enumerate() {
        let transaction = build_transaction(case, target_bytes, &profile.limits)?;
        let logical_request_bytes = logical_bytes(&transaction)?;
        if logical_request_bytes != target_bytes {
            return Err(format!(
                "Gate 9 transaction builder produced {logical_request_bytes} bytes for target {target_bytes}"
            ));
        }
        let conservative_affected_bytes = store
            .conservative_affected_bytes(&transaction)
            .map_err(|error| error.to_string())?;
        if conservative_affected_bytes >= FDB_PHYSICAL_TRANSACTION_GUARD_BYTES {
            return Err(format!(
                "conservative affected bytes {conservative_affected_bytes} reach the physical guard"
            ));
        }
        let mutation_count = transaction.mutations.len();
        let before = store.diagnostics();
        let outcome = store
            .commit(transaction)
            .map_err(|error| error.to_string())?;
        let after = store.diagnostics();
        if outcome != Commit::Applied {
            return Err(format!(
                "Gate 9 envelope point {target_bytes} returned {outcome:?}"
            ));
        }
        let delta = diagnostics_delta(before, after)?;
        if delta.commit_attempts != 1
            || delta.commits_applied != 1
            || delta.commit_conflicts != 0
            || delta.commit_errors != 0
            || delta.approximate_size_observations != 1
            || delta.physical_guard_rejections != 0
        {
            return Err(format!(
                "Gate 9 envelope point {target_bytes} has invalid diagnostics delta {delta:?}"
            ));
        }
        let observed = after.last_approximate_size_bytes;
        if observed == 0 || observed >= FDB_PHYSICAL_TRANSACTION_GUARD_BYTES as u64 {
            return Err(format!(
                "observed physical transaction size {observed} is outside the accepted envelope"
            ));
        }
        points.push(EnvelopePoint {
            target_logical_bytes: target_bytes,
            logical_request_bytes,
            conservative_affected_bytes,
            observed_approximate_physical_bytes: observed,
            mutation_count,
            outcome: "Applied",
            diagnostics_delta: delta,
        });
    }

    let planner_target_observed_approximate_physical_bytes = points
        .iter()
        .find(|point| point.logical_request_bytes == target)
        .map(|point| point.observed_approximate_physical_bytes)
        .ok_or_else(|| "Gate 9 omitted the advertised planner target".to_owned())?;
    let maximum_observed_approximate_physical_bytes = points
        .iter()
        .map(|point| point.observed_approximate_physical_bytes)
        .max()
        .unwrap_or(0);
    if planner_target_observed_approximate_physical_bytes
        >= FDB_PHYSICAL_TRANSACTION_GUARD_BYTES as u64
    {
        return Err("the measured planner target reaches the physical guard".to_owned());
    }

    Ok(EnvelopeReport {
        schema: "nokv.fdb.limits-qualification.envelope.v1",
        transaction_target_bytes: target,
        logical_transaction_limit_bytes: logical_limit,
        physical_guard_bytes: FDB_PHYSICAL_TRANSACTION_GUARD_BYTES,
        maximum_observed_approximate_physical_bytes,
        planner_target_observed_approximate_physical_bytes,
        final_diagnostics: store.diagnostics().into(),
        points,
    })
}

fn build_transaction(
    case: usize,
    target_bytes: usize,
    limits: &StoreLimits,
) -> Result<WriteTxn, String> {
    let sample_key = format!("gate9-envelope-{case:02}-{:04}", 0);
    let key_bytes = sample_key.len();
    let row_capacity = key_bytes
        .checked_add(limits.max_value_bytes)
        .ok_or_else(|| "Gate 9 row capacity overflows usize".to_owned())?;
    let rows = target_bytes
        .checked_add(row_capacity - 1)
        .ok_or_else(|| "Gate 9 row rounding overflows usize".to_owned())?
        / row_capacity;
    if rows == 0 || rows > limits.max_mutations || target_bytes < rows * key_bytes {
        return Err("Gate 9 target cannot be represented within store limits".to_owned());
    }
    let mut remaining_value_bytes = target_bytes - rows * key_bytes;
    let mut mutations = Vec::with_capacity(rows);
    for row in 0..rows {
        let key = format!("gate9-envelope-{case:02}-{row:04}").into_bytes();
        if key.len() != key_bytes {
            return Err("Gate 9 transaction key length drifted".to_owned());
        }
        let value_bytes = remaining_value_bytes.min(limits.max_value_bytes);
        remaining_value_bytes -= value_bytes;
        let value = (0..value_bytes)
            .map(|offset| ((offset as u8).wrapping_mul(31)) ^ case as u8 ^ row as u8)
            .collect();
        mutations.push(Mutation::Put {
            key: Key::new(QUALIFICATION_KEYSPACE, key),
            value,
        });
    }
    if remaining_value_bytes != 0 {
        return Err("Gate 9 transaction value distribution is incomplete".to_owned());
    }
    Ok(WriteTxn {
        checks: Vec::new(),
        mutations,
    })
}

fn logical_bytes(transaction: &WriteTxn) -> Result<usize, String> {
    transaction
        .mutations
        .iter()
        .try_fold(0_usize, |bytes, mutation| {
            let (key, value_bytes) = match mutation {
                Mutation::Put { key, value } => (key, value.len()),
                Mutation::Delete { key } => (key, 0),
            };
            bytes
                .checked_add(key.bytes.len())
                .and_then(|bytes| bytes.checked_add(value_bytes))
                .ok_or_else(|| "Gate 9 logical transaction bytes overflow usize".to_owned())
        })
}

fn diagnostics_delta(
    before: FdbStoreDiagnostics,
    after: FdbStoreDiagnostics,
) -> Result<DiagnosticsEvidence, String> {
    Ok(DiagnosticsEvidence {
        commit_attempts: subtract(after.commit_attempts, before.commit_attempts)?,
        commits_applied: subtract(after.commits_applied, before.commits_applied)?,
        commit_conflicts: subtract(after.commit_conflicts, before.commit_conflicts)?,
        commit_errors: subtract(after.commit_errors, before.commit_errors)?,
        approximate_size_observations: subtract(
            after.approximate_size_observations,
            before.approximate_size_observations,
        )?,
        last_approximate_size_bytes: after.last_approximate_size_bytes,
        max_approximate_size_bytes: after.max_approximate_size_bytes,
        physical_guard_rejections: subtract(
            after.physical_guard_rejections,
            before.physical_guard_rejections,
        )?,
    })
}

fn subtract(after: u64, before: u64) -> Result<u64, String> {
    after
        .checked_sub(before)
        .ok_or_else(|| "FDB diagnostics counters regressed".to_owned())
}

impl From<FdbStoreDiagnostics> for DiagnosticsEvidence {
    fn from(value: FdbStoreDiagnostics) -> Self {
        Self {
            commit_attempts: value.commit_attempts,
            commits_applied: value.commits_applied,
            commit_conflicts: value.commit_conflicts,
            commit_errors: value.commit_errors,
            approximate_size_observations: value.approximate_size_observations,
            last_approximate_size_bytes: value.last_approximate_size_bytes,
            max_approximate_size_bytes: value.max_approximate_size_bytes,
            physical_guard_rejections: value.physical_guard_rejections,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_builder_hits_exact_logical_targets() {
        let limits = StoreLimits {
            max_reads: 8,
            max_checks: 1024,
            max_mutations: 1024,
            max_key_bytes: 8205,
            max_value_bytes: 65_535,
            max_read_bytes: 4_500_000,
            max_transaction_bytes: 2_900_000,
            max_result_rows: 1024,
            max_result_bytes: 8 * 1024 * 1024,
        };
        for target in [65_536, 900_000, 2_800_000] {
            let transaction = build_transaction(1, target, &limits).unwrap();
            assert_eq!(logical_bytes(&transaction).unwrap(), target);
            transaction.validate(&limits).unwrap();
        }
    }
}
