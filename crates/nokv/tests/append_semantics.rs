/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Functional regression gate for the append write-semantics chain.
//!
//! Ports the observable semantics of the pre-#423
//! `crates/nokv-client/tests/file_client_append.rs` onto the current
//! operations architecture: a real in-process server plus the real SDK
//! client against a memory object store. It asserts user-visible behavior
//! (delta extension, create-on-append, generation fencing, conflict-free
//! concurrent appends, rematerialization after the dependency-depth bound,
//! content-type inheritance, exact replay), not the removed dentry/inode
//! API shapes.

mod common;

use std::sync::{Arc, Barrier};
use std::time::Instant;

use common::{append, connect, harness, publish_base, read_all, target, Harness};
use nokv_client::ArtifactAppendOptions;
use nokv_protocol::{ArtifactRevisionIdentity, ContentType, OperationIdentity};

fn append_with_seed(h: &Harness, path: &str, delta: &[u8], seed: u8) -> (bool, u64, u64) {
    append(&h.client, &h.store, &h.workbench, path, delta, seed)
}

fn publish_with_seed(h: &Harness, path: &str, bytes: &[u8], seed: u8) -> u64 {
    publish_base(&h.client, &h.store, &h.workbench, path, bytes, seed)
}

fn read(h: &Harness, path: &str) -> Vec<u8> {
    read_all(&h.client, &h.store, &h.workbench, path)
}

/// Identity bytes with one writer-specific prefix byte and one attempt
/// index byte; the remaining bytes are zero, so every retry identity stays
/// inside its own writer's space.
fn identity_bytes(prefix: u8, index: u8) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[0] = prefix;
    bytes[1] = index;
    bytes
}

#[test]
fn append_extends_existing_artifact_as_delta() {
    let h = harness("append-wb");
    let base_generation = publish_with_seed(&h, "input/log.txt", b"hello ", 0x10);
    let (created, generation, new_size) = append_with_seed(&h, "input/log.txt", b"world", 0x20);

    assert!(!created);
    assert_eq!(new_size, 11);
    assert!(generation > base_generation);
    assert_eq!(read(&h, "input/log.txt"), b"hello world");
}

#[test]
fn append_creates_missing_artifact() {
    let h = harness("append-wb");
    let (created, generation, new_size) = append_with_seed(&h, "input/fresh.txt", b"seed", 0x30);

    assert!(created);
    assert_eq!(new_size, 4);
    assert!(generation >= 1);
    assert_eq!(read(&h, "input/fresh.txt"), b"seed");
}

#[test]
fn repeated_appends_stay_readable_across_dependency_bound() {
    let h = harness("append-wb");
    let mut expected = b"seg0|".to_vec();
    publish_with_seed(&h, "input/chain.txt", &expected, 0x40);

    // Twelve appends push the delta chain well past the dependency-depth
    // bound (8); content must read back whole at every depth, including
    // after the client rematerializes the base.
    for index in 1..=12_u8 {
        let delta = format!("seg{index}|").into_bytes();
        expected.extend_from_slice(&delta);
        append_with_seed(&h, "input/chain.txt", &delta, 0x50 + index);
        assert_eq!(
            read(&h, "input/chain.txt"),
            expected,
            "content diverged after append {index}"
        );
    }
}

/// Two clients hammer one artifact from two threads released by the same
/// barrier. A racing append loses its generation CAS and surfaces as a typed
/// path-generation conflict; the caller retries with a fresh identity, which
/// is the documented concurrency contract. Every append must land exactly
/// once: the final generation is the base generation plus the append count,
/// and the final body contains every delta exactly once in some order.
#[test]
fn concurrent_appends_from_two_clients_lose_no_data() {
    const APPENDS_PER_CLIENT: usize = 12;

    let h = harness("append-race-wb");
    let base_generation = publish_with_seed(&h, "input/race.txt", b"base|", 0x60);

    let barrier = Arc::new(Barrier::new(3));
    let deltas_a: Vec<Vec<u8>> = (0..APPENDS_PER_CLIENT)
        .map(|index| format!("a{index}|").into_bytes())
        .collect();
    let deltas_b: Vec<Vec<u8>> = (0..APPENDS_PER_CLIENT)
        .map(|index| format!("b{index}|").into_bytes())
        .collect();

    let run_writer = |deltas: Vec<Vec<u8>>, prefix: u8| {
        let client = connect(h.bind);
        let store = h.store.clone();
        let workbench = h.workbench.clone();
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            let began = Instant::now();
            let mut generations = Vec::with_capacity(deltas.len());
            let mut conflicts = 0usize;
            // Per-writer identity space: prefix byte plus a monotonically
            // increasing index, so race retries can never collide with the
            // other writer (or with themselves).
            let mut index = 0u8;
            for delta in &deltas {
                let generation = loop {
                    let options = ArtifactAppendOptions::new(
                        OperationIdentity(identity_bytes(prefix, index)),
                        ArtifactRevisionIdentity(identity_bytes(prefix + 1, index)),
                        target(&workbench, "input/race.txt"),
                        ContentType::new("text/plain").unwrap(),
                    )
                    .with_block_size(2);
                    match client.append_artifact(&store, options, delta) {
                        Ok(outcome) => break outcome.publication.value.generation,
                        // The racing append lost its generation CAS. The
                        // typed conflict is the concurrency contract: retry
                        // with a fresh identity.
                        Err(error)
                            if matches!(
                                &error,
                                nokv_client::ClientError::ArtifactPublishFailed { source, .. }
                                    if matches!(
                                        &**source,
                                        nokv_client::ClientError::Rpc(failure)
                                            if failure.code == nokv_protocol::ErrorCode::Conflict
                                    )
                            ) =>
                        {
                            conflicts += 1;
                            index = index.wrapping_add(1);
                            continue;
                        }
                        Err(error) => panic!("append failed with non-race error: {error}"),
                    }
                };
                generations.push(generation);
                index = index.wrapping_add(1);
            }
            let ended = Instant::now();
            (began, ended, generations, conflicts)
        })
    };

    let writer_a = run_writer(deltas_a.clone(), 0x70);
    let writer_b = run_writer(deltas_b.clone(), 0x90);
    barrier.wait();

    let (began_a, ended_a, generations_a, conflicts_a) = writer_a.join().unwrap();
    let (began_b, ended_b, generations_b, conflicts_b) = writer_b.join().unwrap();

    // Both writers really ran concurrently: their measured execution
    // intervals overlap, so the fence-conflict path had to resolve at
    // least one racing pair instead of serializing by accident.
    let overlap_start = began_a.max(began_b);
    let overlap_end = ended_a.min(ended_b);
    assert!(
        overlap_start < overlap_end,
        "writer intervals did not overlap: [{:?}, {:?}] vs [{:?}, {:?}]",
        began_a,
        ended_a,
        began_b,
        ended_b
    );

    // Non-vacuous: the synchronized writers really collided at least once,
    // and the conflict surfaced as the typed race, not a silent loss.
    assert!(
        conflicts_a + conflicts_b >= 1,
        "no generation CAS conflict was observed, so the race path was never exercised"
    );

    // Each writer's own appends advanced the generation fence monotonically.
    for generations in [&generations_a, &generations_b] {
        assert!(
            generations.windows(2).all(|pair| pair[0] < pair[1]),
            "writer generations not strictly increasing: {generations:?}"
        );
    }

    // Every append landed exactly once: the path advanced by exactly the
    // combined append count from the published base.
    let final_body = read(&h, "input/race.txt");
    let final_generation = h
        .client
        .read_artifact(
            &h.store,
            None,
            target(&h.workbench, "input/race.txt"),
            nokv_protocol::WorkspaceReadView::Live,
        )
        .unwrap()
        .metadata
        .generation;
    assert_eq!(
        final_generation,
        base_generation + 2 * APPENDS_PER_CLIENT as u64,
        "generation advanced by the wrong amount (an append was lost or applied twice)"
    );

    // No delta was lost or duplicated: the body starts with the base, has
    // the exact combined length, and contains every delta.
    assert!(final_body.starts_with(b"base|"));
    let expected_len = b"base|".len()
        + deltas_a
            .iter()
            .chain(&deltas_b)
            .map(|delta| delta.len())
            .sum::<usize>();
    assert_eq!(
        final_body.len(),
        expected_len,
        "final body length diverged from exactly-once appends"
    );
    for delta in deltas_a.iter().chain(deltas_b.iter()) {
        assert!(
            final_body
                .windows(delta.len())
                .any(|window| window == delta.as_slice()),
            "delta {delta:?} missing from the final body"
        );
    }
}

#[test]
fn append_inherits_base_content_type() {
    let h = harness("append-wb");
    publish_with_seed(&h, "input/typed.bin", b"abc", 0x90);
    // No explicit content-type override: the resulting descriptor inherits
    // the base artifact's content type.
    let options = ArtifactAppendOptions::new(
        OperationIdentity([0xA0; 16]),
        ArtifactRevisionIdentity([0xA1; 16]),
        target(&h.workbench, "input/typed.bin"),
        ContentType::new("application/octet-stream").unwrap(),
    )
    .with_block_size(2);
    let outcome = h.client.append_artifact(&h.store, options, b"def").unwrap();
    assert!(!outcome.created);
    assert_eq!(outcome.descriptor.content_type.as_str(), "text/plain");
    assert_eq!(read(&h, "input/typed.bin"), b"abcdef");
}

/// Re-issuing the exact same append options after a successful call is
/// rejected by the client-side revision fence with a typed error, so the
/// delta can never be applied twice. (Response loss *during* a call is
/// instead absorbed by the client's attempt replay; that path is covered by
/// the fault-injection gate.)
#[test]
fn exact_retry_with_same_identity_does_not_double_append() {
    let h = harness("append-wb");
    publish_with_seed(&h, "input/replay.txt", b"base|", 0xB0);
    let options = || {
        ArtifactAppendOptions::new(
            OperationIdentity([0xC0; 16]),
            ArtifactRevisionIdentity([0xC1; 16]),
            target(&h.workbench, "input/replay.txt"),
            ContentType::new("text/plain").unwrap(),
        )
        .with_block_size(2)
    };

    let first = h
        .client
        .append_artifact(&h.store, options(), b"delta|")
        .unwrap();
    assert_eq!(first.publication.value.logical_size, 11);
    assert!(!first.publication.replayed);

    // The exact same identity is rejected before any dispatch: the current
    // revision already consumed the supplied revision identity.
    let replay = h
        .client
        .append_artifact(&h.store, options(), b"delta|")
        .unwrap_err();
    assert!(
        matches!(replay, nokv_client::ClientError::InvalidOptions(_)),
        "expected a typed invalid-options rejection, got {replay:?}"
    );
    assert_eq!(read(&h, "input/replay.txt"), b"base|delta|");
}
