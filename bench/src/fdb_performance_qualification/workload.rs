/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use nokv_client::{
    ClientError, ClientOptions, FramedTcpOptions, FramedTcpTransport, RpcTransport,
    SeedRouteOptions, SeedRouteResolver, TransportError, WorkspaceClient,
};
use nokv_protocol::{
    CreateWorkspaceRequest, ErrorCode, RelativePath, RenamePathRequest, RootIdentity,
    WorkbenchName, WorkspaceCapability, WorkspaceIdentity, WorkspacePath,
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};

use crate::report::{
    summarize_classified_samples, ClassifiedLatencySample, ClassifiedOutcome, WorkloadReport,
};

type BenchClient = WorkspaceClient<CountingTransport, SeedRouteResolver<CountingTransport>>;

#[derive(Clone)]
pub(crate) struct CountingTransport {
    inner: FramedTcpTransport,
    round_trips: Arc<AtomicU64>,
}

impl CountingTransport {
    fn new(timeout: Duration) -> Result<Self, String> {
        let inner = FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: timeout.min(Duration::from_secs(2)),
            handshake_timeout: timeout,
            read_timeout: timeout,
            write_timeout: timeout,
        })
        .map_err(|error| error.to_string())?;
        Ok(Self {
            inner,
            round_trips: Arc::new(AtomicU64::new(0)),
        })
    }

    fn round_trips(&self) -> u64 {
        self.round_trips.load(Ordering::Relaxed)
    }
}

impl RpcTransport for CountingTransport {
    fn round_trip(&self, endpoint: SocketAddr, request: &[u8]) -> Result<Vec<u8>, TransportError> {
        self.round_trips.fetch_add(1, Ordering::Relaxed);
        self.inner.round_trip(endpoint, request)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ContentionFixture {
    pub(crate) workbench: WorkbenchName,
    pub(crate) sources: Vec<RelativePath>,
    pub(crate) artifact_payload_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct PerformanceProfileReport {
    pub(crate) schema: &'static str,
    pub(crate) profile: &'static str,
    pub(crate) warmup_operations: usize,
    pub(crate) measured_operations: usize,
    pub(crate) concurrency: usize,
    pub(crate) application_payload_bytes: usize,
    pub(crate) key_distribution: &'static str,
    pub(crate) thread_model: &'static str,
    pub(crate) client_max_attempts: u32,
    pub(crate) transport_round_trips: u64,
    pub(crate) retry_attribution: &'static str,
    pub(crate) outcome_codes: BTreeMap<String, u64>,
    pub(crate) failure_messages: Vec<String>,
    pub(crate) contention_groups: Option<usize>,
    pub(crate) groups_with_exactly_one_success: Option<usize>,
    pub(crate) performance: WorkloadReport,
    pub(crate) qualification: &'static str,
    pub(crate) qualification_errors: Vec<String>,
}

#[derive(Clone, Debug)]
struct TerminalOperation {
    outcome: ClassifiedOutcome,
    checksum: u64,
    code: String,
    error: Option<String>,
    group: Option<usize>,
}

struct BatchMeasurement {
    samples: Vec<ClassifiedLatencySample>,
    terminals: Vec<TerminalOperation>,
    elapsed: Duration,
}

pub(crate) fn client(
    root: RootIdentity,
    seed: SocketAddr,
    timeout: Duration,
) -> Result<(BenchClient, CountingTransport), String> {
    let transport = CountingTransport::new(timeout)?;
    let resolver = SeedRouteResolver::new(
        transport.clone(),
        [seed],
        SeedRouteOptions {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(5),
            maximum_backoff: Duration::from_millis(10),
        },
    )
    .map_err(|error| error.to_string())?;
    let client = WorkspaceClient::new(
        root,
        transport.clone(),
        resolver,
        ClientOptions { max_attempts: 2 },
    )
    .map_err(|error| error.to_string())?;
    client
        .preflight(std::iter::empty::<WorkspaceCapability>())
        .map_err(|error| format!("Gate 10 seed preflight failed: {error}"))?;
    Ok((client, transport))
}

pub(crate) fn uncontended(
    client: &BenchClient,
    transport: &CountingTransport,
    run_id: &str,
    warmup: usize,
    operations: usize,
    concurrency: usize,
) -> Result<PerformanceProfileReport, String> {
    let warmup_measurement = execute_batches(0, warmup, concurrency, |index| {
        create_workspace(client, run_id, index)
    })?;
    require_all_success(&warmup_measurement, "uncontended warmup")?;

    let before_round_trips = transport.round_trips();
    let measured = execute_batches(warmup, operations, concurrency, |index| {
        create_workspace(client, run_id, index)
    })?;
    let transport_round_trips = transport
        .round_trips()
        .checked_sub(before_round_trips)
        .ok_or_else(|| "transport round-trip counter regressed".to_owned())?;
    profile_report(
        "uncontended",
        warmup,
        operations,
        concurrency,
        0,
        "one independent Workbench identity per operation",
        measured,
        transport_round_trips,
        None,
    )
}

pub(crate) fn contended(
    client: &BenchClient,
    transport: &CountingTransport,
    fixture: &ContentionFixture,
    warmup: usize,
    operations: usize,
    concurrency: usize,
) -> Result<PerformanceProfileReport, String> {
    let warmup_measurement = execute_batches(0, warmup, concurrency, |index| {
        rename_path(client, fixture, index, concurrency)
    })?;
    require_contention_shape(&warmup_measurement, warmup / concurrency, concurrency)?;

    let before_round_trips = transport.round_trips();
    let measured = execute_batches(warmup, operations, concurrency, |index| {
        rename_path(client, fixture, index, concurrency)
    })?;
    let transport_round_trips = transport
        .round_trips()
        .checked_sub(before_round_trips)
        .ok_or_else(|| "transport round-trip counter regressed".to_owned())?;
    profile_report(
        "contended",
        warmup,
        operations,
        concurrency,
        fixture.artifact_payload_bytes,
        "one generation-1 source path per group; all group members race distinct destinations",
        measured,
        transport_round_trips,
        Some(operations / concurrency),
    )
}

fn create_workspace(client: &BenchClient, run_id: &str, index: usize) -> TerminalOperation {
    let name = match WorkbenchName::new(format!("fdb-gate10-u-{run_id}-{index:06}")) {
        Ok(name) => name,
        Err(error) => return failed(error.to_string(), None),
    };
    let identity = WorkspaceIdentity(identity(run_id, "uncontended-workspace", index));
    match client.create_workspace(
        client.new_request_id(),
        CreateWorkspaceRequest {
            workbench: name.clone(),
            workspace_incarnation_id: identity,
        },
    ) {
        Ok(call) if !call.replayed && call.commit_version.is_some() => TerminalOperation {
            outcome: ClassifiedOutcome::Successful,
            checksum: checksum(
                call.commit_version.unwrap_or_default(),
                name.as_str().as_bytes(),
            ),
            code: "success".to_owned(),
            error: None,
            group: None,
        },
        Ok(_) => failed(
            "uncontended create returned replayed or unversioned success".to_owned(),
            None,
        ),
        Err(error) => failed(error.to_string(), None),
    }
}

fn rename_path(
    client: &BenchClient,
    fixture: &ContentionFixture,
    index: usize,
    concurrency: usize,
) -> TerminalOperation {
    let group = index / concurrency;
    let slot = index % concurrency;
    let Some(source) = fixture.sources.get(group).cloned() else {
        return failed(
            "contention fixture has no source for group".to_owned(),
            Some(group),
        );
    };
    let destination = match RelativePath::new(format!("destination-{group:04}-{slot:04}.txt")) {
        Ok(path) => path,
        Err(error) => return failed(error.to_string(), Some(group)),
    };
    let source = WorkspacePath {
        workbench: fixture.workbench.clone(),
        path: source,
    };
    let destination = WorkspacePath {
        workbench: fixture.workbench.clone(),
        path: destination,
    };
    match client.rename_path(
        client.new_request_id(),
        RenamePathRequest {
            source,
            destination,
            expected_generation: 1,
        },
    ) {
        Ok(call) if !call.replayed && call.commit_version.is_some() => TerminalOperation {
            outcome: ClassifiedOutcome::Successful,
            checksum: checksum(
                call.commit_version.unwrap_or_default(),
                &index.to_be_bytes(),
            ),
            code: "success".to_owned(),
            error: None,
            group: Some(group),
        },
        Ok(_) => failed(
            "contended rename returned replayed or unversioned success".to_owned(),
            Some(group),
        ),
        Err(error) => match conflict_code(&error) {
            Some(code) => TerminalOperation {
                outcome: ClassifiedOutcome::Conflicted,
                checksum: checksum(index as u64, code.as_bytes()),
                code,
                error: None,
                group: Some(group),
            },
            None => failed(error.to_string(), Some(group)),
        },
    }
}

fn execute_batches(
    start_index: usize,
    operations: usize,
    concurrency: usize,
    operation: impl Fn(usize) -> TerminalOperation + Sync,
) -> Result<BatchMeasurement, String> {
    let mut samples = Vec::with_capacity(operations);
    let mut terminals = Vec::with_capacity(operations);
    let mut elapsed = Duration::ZERO;
    for batch_start in (start_index..start_index + operations).step_by(concurrency) {
        let barrier = Arc::new(Barrier::new(concurrency + 1));
        let (batch, batch_elapsed) = thread::scope(|scope| {
            let mut workers = Vec::with_capacity(concurrency);
            for offset in 0..concurrency {
                let barrier = Arc::clone(&barrier);
                let operation = &operation;
                workers.push(scope.spawn(move || {
                    barrier.wait();
                    let started = Instant::now();
                    let terminal = operation(batch_start + offset);
                    let latency_ns =
                        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
                    (terminal, latency_ns)
                }));
            }
            let started = Instant::now();
            barrier.wait();
            let batch = workers
                .into_iter()
                .map(|worker| {
                    worker
                        .join()
                        .map_err(|_| "Gate 10 workload worker panicked".to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, String>((batch, started.elapsed()))
        })?;
        elapsed += batch_elapsed;
        for (terminal, latency_ns) in batch {
            samples.push(ClassifiedLatencySample {
                latency_ns,
                outcome: terminal.outcome,
                checksum: terminal.checksum,
            });
            terminals.push(terminal);
        }
    }
    Ok(BatchMeasurement {
        samples,
        terminals,
        elapsed,
    })
}

#[allow(clippy::too_many_arguments)]
fn profile_report(
    profile: &'static str,
    warmup: usize,
    operations: usize,
    concurrency: usize,
    payload_bytes: usize,
    key_distribution: &'static str,
    measurement: BatchMeasurement,
    transport_round_trips: u64,
    contention_groups: Option<usize>,
) -> Result<PerformanceProfileReport, String> {
    let retried = transport_round_trips.saturating_sub(operations as u64);
    let performance = summarize_classified_samples(
        format!("fdb_{profile}_workspace_wire"),
        &measurement.samples,
        retried,
        measurement.elapsed,
    )?;
    let mut outcome_codes = BTreeMap::new();
    for terminal in &measurement.terminals {
        *outcome_codes.entry(terminal.code.clone()).or_insert(0) += 1;
    }
    let failure_messages = measurement
        .terminals
        .iter()
        .filter_map(|terminal| terminal.error.clone())
        .take(32)
        .collect::<Vec<_>>();
    let groups_with_exactly_one_success = contention_groups.map(|_| {
        let mut successes = BTreeMap::<usize, usize>::new();
        for terminal in &measurement.terminals {
            if terminal.outcome == ClassifiedOutcome::Successful {
                *successes
                    .entry(terminal.group.expect("contended terminals have groups"))
                    .or_insert(0) += 1;
            }
        }
        successes.values().filter(|count| **count == 1).count()
    });
    let mut qualification_errors = Vec::new();
    if transport_round_trips < operations as u64 {
        qualification_errors
            .push("transport round trips are fewer than attempted operations".to_owned());
    }
    if performance.failed != 0 {
        qualification_errors.push(format!("{} measured operations failed", performance.failed));
    }
    match contention_groups {
        None => {
            if performance.successful != operations as u64 || performance.conflicted != 0 {
                qualification_errors.push(
                    "uncontended operations did not all complete without conflict".to_owned(),
                );
            }
        }
        Some(groups) => {
            let expected_conflicts = operations.saturating_sub(groups) as u64;
            if performance.successful != groups as u64
                || performance.conflicted != expected_conflicts
                || groups_with_exactly_one_success != Some(groups)
            {
                qualification_errors.push(
                    "contended groups did not each produce one success and the expected conflicts"
                        .to_owned(),
                );
            }
        }
    }
    if performance.operations_per_second <= 0.0
        || performance.latency.p50_ns == 0
        || performance.latency.p50_ns > performance.latency.p95_ns
        || performance.latency.p95_ns > performance.latency.p99_ns
        || performance.latency.p99_ns > performance.latency.max_ns
    {
        qualification_errors.push("latency or throughput summary is invalid".to_owned());
    }
    Ok(PerformanceProfileReport {
        schema: "nokv.fdb.performance-qualification.profile.v1",
        profile,
        warmup_operations: warmup,
        measured_operations: operations,
        concurrency,
        application_payload_bytes: payload_bytes,
        key_distribution,
        thread_model: "fixed-size scoped batches; thread creation excluded from elapsed interval",
        client_max_attempts: 2,
        transport_round_trips,
        retry_attribution:
            "transport round trips minus measured logical operations after a primed seed cache",
        outcome_codes,
        failure_messages,
        contention_groups,
        groups_with_exactly_one_success,
        performance,
        qualification: if qualification_errors.is_empty() {
            "PASS"
        } else {
            "FAIL"
        },
        qualification_errors,
    })
}

fn require_all_success(measurement: &BatchMeasurement, label: &str) -> Result<(), String> {
    if measurement
        .terminals
        .iter()
        .all(|terminal| terminal.outcome == ClassifiedOutcome::Successful)
    {
        Ok(())
    } else {
        Err(format!(
            "{label} did not complete without conflicts or failures"
        ))
    }
}

fn require_contention_shape(
    measurement: &BatchMeasurement,
    groups: usize,
    concurrency: usize,
) -> Result<(), String> {
    for group in 0..groups {
        let terminals = measurement
            .terminals
            .iter()
            .filter(|terminal| terminal.group == Some(group))
            .collect::<Vec<_>>();
        let successes = terminals
            .iter()
            .filter(|terminal| terminal.outcome == ClassifiedOutcome::Successful)
            .count();
        let conflicts = terminals
            .iter()
            .filter(|terminal| terminal.outcome == ClassifiedOutcome::Conflicted)
            .count();
        if terminals.len() != concurrency || successes != 1 || conflicts != concurrency - 1 {
            return Err(format!(
                "contended warmup group {group} did not produce one success and {} conflicts",
                concurrency - 1
            ));
        }
    }
    Ok(())
}

fn conflict_code(error: &ClientError) -> Option<String> {
    let failure = match error {
        ClientError::Rpc(failure) | ClientError::Discovery(failure) => Some(failure),
        ClientError::RetryExhausted { last_error, .. } => return conflict_code(last_error),
        _ => None,
    }?;
    matches!(
        failure.code,
        ErrorCode::NotFound
            | ErrorCode::AlreadyExists
            | ErrorCode::Conflict
            | ErrorCode::PreconditionFailed
    )
    .then(|| format!("rpc:{:?}", failure.code))
}

fn failed(error: String, group: Option<usize>) -> TerminalOperation {
    TerminalOperation {
        outcome: ClassifiedOutcome::Failed,
        checksum: checksum(0, error.as_bytes()),
        code: "failed".to_owned(),
        error: Some(error),
        group,
    }
}

fn checksum(seed: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(seed, |value, byte| {
        value.wrapping_mul(0x100_0000_01b3) ^ u64::from(*byte)
    })
}

fn identity(run_id: &str, domain: &str, index: usize) -> [u8; 16] {
    let mut digest = Sha256::new();
    digest.update(b"nokv/fdb-performance-qualification/identity/v1\0");
    digest.update(run_id.as_bytes());
    digest.update([0]);
    digest.update(domain.as_bytes());
    digest.update(index.to_be_bytes());
    digest.finalize()[..16]
        .try_into()
        .expect("SHA-256 prefix has fixed length")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_identity_is_stable_and_domain_separated() {
        assert_eq!(identity("run", "a", 1), identity("run", "a", 1));
        assert_ne!(identity("run", "a", 1), identity("run", "b", 1));
        assert_ne!(identity("run", "a", 1), identity("run", "a", 2));
    }
}
