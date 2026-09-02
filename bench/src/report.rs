/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::time::{Duration, Instant};

use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct LatencyDistribution {
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkloadReport {
    pub workload: String,
    pub attempted: u64,
    pub successful: u64,
    pub conflicted: u64,
    pub retried: u64,
    pub failed: u64,
    pub elapsed_seconds: f64,
    pub operations_per_second: f64,
    pub latency: LatencyDistribution,
    pub result_checksum: u64,
    pub latency_estimator: &'static str,
    pub completion_policy: &'static str,
}

pub struct MeasuredWorkload<S> {
    pub report: WorkloadReport,
    pub diagnostics: S,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClassifiedOutcome {
    Successful,
    Conflicted,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClassifiedLatencySample {
    pub latency_ns: u64,
    pub outcome: ClassifiedOutcome,
    pub checksum: u64,
}

/// Summarize already-timed concurrent samples with the same nearest-rank
/// estimator used by the sequential benchmark helper.
pub fn summarize_classified_samples(
    workload: impl Into<String>,
    samples: &[ClassifiedLatencySample],
    retried: u64,
    elapsed: Duration,
) -> Result<WorkloadReport, String> {
    if samples.is_empty() {
        return Err("classified workload must contain at least one sample".to_owned());
    }
    let mut latencies = samples
        .iter()
        .map(|sample| sample.latency_ns)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let successful = samples
        .iter()
        .filter(|sample| sample.outcome == ClassifiedOutcome::Successful)
        .count() as u64;
    let conflicted = samples
        .iter()
        .filter(|sample| sample.outcome == ClassifiedOutcome::Conflicted)
        .count() as u64;
    let failed = samples
        .iter()
        .filter(|sample| sample.outcome == ClassifiedOutcome::Failed)
        .count() as u64;
    let attempted = samples.len() as u64;
    let checksum = samples.iter().fold(0_u64, |checksum, sample| {
        checksum.rotate_left(7) ^ sample.checksum
    });
    let elapsed_seconds = elapsed.as_secs_f64();
    Ok(WorkloadReport {
        workload: workload.into(),
        attempted,
        successful,
        conflicted,
        retried,
        failed,
        elapsed_seconds,
        operations_per_second: if elapsed_seconds == 0.0 {
            0.0
        } else {
            attempted as f64 / elapsed_seconds
        },
        latency: LatencyDistribution {
            p50_ns: percentile(&latencies, 50),
            p95_ns: percentile(&latencies, 95),
            p99_ns: percentile(&latencies, 99),
            max_ns: *latencies.last().expect("samples are nonempty"),
        },
        result_checksum: checksum,
        latency_estimator: "nearest_rank",
        completion_policy: "retain_all_terminal_outcomes",
    })
}

/// Measure one workload and bracket only its timed operations with diagnostics.
///
/// Warmup runs before `begin_diagnostics`; allocation plus diagnostic session
/// setup and finish are outside the elapsed interval and every latency sample.
pub fn measure_with_diagnostics<D, S>(
    workload: impl Into<String>,
    warmup: u64,
    iterations: u64,
    mut operation: impl FnMut() -> Result<u64, String>,
    begin_diagnostics: impl FnOnce() -> Result<D, String>,
    finish_diagnostics: impl FnOnce(D) -> Result<S, String>,
) -> Result<MeasuredWorkload<S>, String> {
    for _ in 0..warmup {
        std::hint::black_box(operation()?);
    }

    let capacity = usize::try_from(iterations)
        .map_err(|_| "iterations do not fit in process address space".to_owned())?;
    let mut samples = Vec::with_capacity(capacity);
    let mut checksum = 0_u64;
    let diagnostics = begin_diagnostics()?;
    let interval = Instant::now();
    for _ in 0..iterations {
        let started = Instant::now();
        let value = operation()?;
        let elapsed = started.elapsed();
        samples.push(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        checksum = checksum.rotate_left(7) ^ value;
        std::hint::black_box(checksum);
    }
    let elapsed = interval.elapsed();
    let diagnostics = finish_diagnostics(diagnostics)?;
    let classified = samples
        .iter()
        .map(|latency_ns| ClassifiedLatencySample {
            latency_ns: *latency_ns,
            outcome: ClassifiedOutcome::Successful,
            checksum,
        })
        .collect::<Vec<_>>();
    let mut report = summarize_classified_samples(workload, &classified, 0, elapsed)?;
    report.result_checksum = checksum;
    report.completion_policy = "abort_without_report_on_first_operation_error";
    Ok(MeasuredWorkload {
        report,
        diagnostics,
    })
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ActiveDiagnostic<'a>(&'a std::cell::Cell<bool>);

    impl Drop for ActiveDiagnostic<'_> {
        fn drop(&mut self) {
            self.0.set(false);
        }
    }

    #[test]
    fn measurement_reports_distribution_checksum_and_timed_counter_delta() {
        let value = std::cell::Cell::new(0_u64);
        let measured = measure_with_diagnostics(
            "unit",
            2,
            4,
            || {
                let next = value.get() + 1;
                value.set(next);
                Ok(next)
            },
            || Ok(value.get()),
            |before| Ok(value.get() - before),
        )
        .unwrap();
        let report = measured.report;

        assert_eq!(report.workload, "unit");
        assert_eq!(report.attempted, 4);
        assert_eq!(report.successful, 4);
        assert_eq!(report.failed, 0);
        assert!(report.latency.p50_ns <= report.latency.max_ns);
        assert_ne!(report.result_checksum, 0);
        assert_eq!(measured.diagnostics, 4);
    }

    #[test]
    fn percentile_uses_nearest_rank() {
        let samples = [1, 2, 100];
        assert_eq!(percentile(&samples, 50), 2);
        assert_eq!(percentile(&samples, 95), 100);
        assert_eq!(percentile(&samples, 99), 100);
    }

    #[test]
    fn operation_error_drops_active_diagnostics() {
        let active = std::cell::Cell::new(false);
        let result = measure_with_diagnostics(
            "failing",
            0,
            1,
            || Err("operation failed".to_owned()),
            || {
                active.set(true);
                Ok(ActiveDiagnostic(&active))
            },
            |_diagnostic| Ok(()),
        );

        assert_eq!(result.err().unwrap(), "operation failed");
        assert!(!active.get());
    }

    #[test]
    fn classified_summary_retains_conflicts_retries_and_failures() {
        let report = summarize_classified_samples(
            "mixed",
            &[
                ClassifiedLatencySample {
                    latency_ns: 10,
                    outcome: ClassifiedOutcome::Successful,
                    checksum: 1,
                },
                ClassifiedLatencySample {
                    latency_ns: 20,
                    outcome: ClassifiedOutcome::Conflicted,
                    checksum: 2,
                },
                ClassifiedLatencySample {
                    latency_ns: 30,
                    outcome: ClassifiedOutcome::Failed,
                    checksum: 3,
                },
            ],
            2,
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(report.attempted, 3);
        assert_eq!(report.successful, 1);
        assert_eq!(report.conflicted, 1);
        assert_eq!(report.retried, 2);
        assert_eq!(report.failed, 1);
        assert_eq!(report.latency.p99_ns, 30);
    }
}
