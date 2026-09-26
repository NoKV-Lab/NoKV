/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Functional regression gate for the concurrent read-x-append visibility
//! chain, porting the observable semantics of the pre-#423
//! `crates/nokv-client/tests/concurrent_read_append.rs`.
//!
//! A read racing an append must never hide the last append: every read
//! observed during the hammer must be one of the exact prefix states of
//! the final body, and a fresh observer client must read the complete
//! final body as soon as every append has returned.
//!
//! The hammer synchronizes real threads without sleeping: the writer waits
//! on a read counter before the first append and after every intermediate
//! append, so each round deterministically observes the base state, at
//! least one intermediate prefix state, and the final state.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use common::{append, connect, harness, publish_base, read_all};

const ROUNDS: usize = 3;
const APPENDS: usize = 8;
const READERS: usize = 4;
/// Hard safety cap per round: the synchronized counter design already bounds
/// the observation count, this only prevents unbounded growth if the writer
/// thread panics and never releases the readers.
const MAX_OBSERVATIONS_PER_ROUND: usize = 4096;

fn prefix_states(base: &[u8], deltas: &[Vec<u8>]) -> Vec<Vec<u8>> {
    // Each committed state is the base plus the first k deltas (including
    // k = 0, the base itself); every such boundary is a legal observation.
    let mut states = Vec::with_capacity(deltas.len() + 1);
    let mut current = base.to_vec();
    states.push(current.clone());
    for delta in deltas {
        current.extend_from_slice(delta);
        states.push(current.clone());
    }
    states
}

/// Spin-yield until the read counter reaches `target`, with a generous
/// deadline so a wedged reader fails loudly instead of hanging CI.
fn wait_until_reads(counter: &AtomicUsize, target: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while counter.load(Ordering::Relaxed) < target {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {target} reads"
        );
        thread::yield_now();
    }
}

#[test]
fn appends_stay_visible_under_concurrent_reads() {
    let h = harness("race-wb");
    // Observer uses a fresh client connected to the same server, so any
    // poisoned state would have to live server-side, not client-side.
    let observer = connect(h.bind);
    let observer_store = h.store.clone();

    for round in 0..ROUNDS {
        let path = format!("input/log-{round}.txt");
        let base = b"seg0|".to_vec();
        // Identity space: 0x20 stride per round keeps every publish and
        // append operation identity globally unique (the durable replay
        // registry is workspace-global, not per-path).
        let round_seed = (round as u8).saturating_mul(0x20);
        publish_base(
            &h.client,
            &h.store,
            &h.workbench,
            &path,
            &base,
            0x10 + round_seed,
        );

        let mut deltas: Vec<Vec<u8>> = Vec::with_capacity(APPENDS);
        for index in 1..=APPENDS {
            deltas.push(format!("seg{index}|").into_bytes());
        }
        // Legal prefix states include the base itself (zero deltas applied).
        let legal_states = prefix_states(&base, &deltas);

        let stop = Arc::new(AtomicBool::new(false));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let reads_done = Arc::new(AtomicUsize::new(0));
        let mut readers = Vec::new();
        for _ in 0..READERS {
            let reader_client = connect(h.bind);
            let reader_store = h.store.clone();
            let workbench = h.workbench.clone();
            let path = path.clone();
            let stop = Arc::clone(&stop);
            let observations = Arc::clone(&observations);
            let reads_done = Arc::clone(&reads_done);
            readers.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let body = read_all(&reader_client, &reader_store, &workbench, &path);
                    if reads_done.fetch_add(1, Ordering::Relaxed) < MAX_OBSERVATIONS_PER_ROUND {
                        observations.lock().unwrap().push(body);
                    }
                }
            }));
        }

        // Writer protocol: before the first append every reader must have
        // completed one read (deterministic base observation), and after
        // every intermediate append the writer blocks until READERS more
        // reads completed (deterministic intermediate-prefix observation).
        wait_until_reads(&reads_done, READERS);
        for (index, delta) in deltas.iter().take(APPENDS - 1).enumerate() {
            append(
                &h.client,
                &h.store,
                &h.workbench,
                &path,
                delta,
                0x14 + round_seed + (index as u8),
            );
            wait_until_reads(&reads_done, READERS * (index + 2));
        }
        append(
            &h.client,
            &h.store,
            &h.workbench,
            &path,
            deltas.last().unwrap(),
            0x14 + round_seed + (APPENDS - 1) as u8,
        );
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().unwrap();
        }

        let observations = Arc::try_unwrap(observations).unwrap().into_inner().unwrap();
        // Non-vacuous: the synchronized protocol guarantees a real number of
        // completed reads, not a token best-effort pass.
        assert!(
            reads_done.load(Ordering::Relaxed) >= READERS * APPENDS,
            "round {round}: only {} reads completed, expected at least {}",
            reads_done.load(Ordering::Relaxed),
            READERS * APPENDS
        );

        // Every observation is exactly one committed prefix state: a
        // poisoned entry (short read, missing descriptor, or reordered
        // delta) would fall outside this set.
        for observed in &observations {
            assert!(
                legal_states.contains(observed),
                "round {round}: observed illegal body of {} bytes; \
                 legal states: {}",
                observed.len(),
                legal_states.len()
            );
        }

        // Non-vacuous content: the base state and at least one intermediate
        // prefix (not the base, not the final body) were really observed,
        // so the racing window itself produced evidence.
        assert!(
            observations.iter().any(|body| body == &base),
            "round {round}: the base state was never observed"
        );
        assert!(
            observations.iter().any(|body| {
                body != &base && body.as_slice() != legal_states.last().unwrap().as_slice()
            }),
            "round {round}: no intermediate prefix state was observed"
        );

        // Fresh-client observation after every append returned.
        let final_body = read_all(&observer, &observer_store, &h.workbench, &path);
        assert_eq!(
            final_body.as_slice(),
            legal_states.last().unwrap().as_slice(),
            "fresh client diverged from the final body in round {round}"
        );
    }
}
