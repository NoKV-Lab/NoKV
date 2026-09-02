/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::lifecycle::{RuntimeAcquireError, RuntimeRegistry};
use crate::*;

#[test]
fn common_options_require_an_absolute_bounded_cluster_file() {
    let valid = FdbConnectionOptions::new("/tmp/fdb.cluster");
    valid.validate().unwrap();
    assert_eq!(valid.transaction_timeout(), Duration::from_secs(4));

    assert_eq!(
        FdbConnectionOptions::new("relative.cluster").validate(),
        Err(FdbConfigError::ClusterFileNotAbsolute)
    );
    assert_eq!(
        FdbConnectionOptions::new(PathBuf::from("/")).validate(),
        Err(FdbConfigError::ClusterFileMissingName)
    );
    assert_eq!(
        FdbConnectionOptions::new("/tmp/fdb\0.cluster").validate(),
        Err(FdbConfigError::ClusterFileContainsNul)
    );
    assert!(matches!(
        valid
            .clone()
            .with_transaction_timeout(Duration::ZERO)
            .validate(),
        Err(FdbConfigError::TransactionTimeoutOutsideBounds { .. })
    ));
    assert!(matches!(
        valid
            .with_transaction_timeout(Duration::from_millis(4_001))
            .validate(),
        Err(FdbConfigError::TransactionTimeoutOutsideBounds { .. })
    ));
}

#[test]
fn physical_prefix_is_versioned_component_safe_and_binary() {
    let first = FdbStorePrefix::new(b"a").unwrap();
    let second = FdbStorePrefix::new(b"ab").unwrap();
    assert!(!second.as_bytes().starts_with(first.as_bytes()));
    assert_eq!(first.token(), b"a");

    let binary = FdbStorePrefix::new([0, u8::MAX]).unwrap();
    let metadata = binary.subspace(FdbSubspaceKind::Metadata);
    let short = metadata.component(b"a").unwrap();
    let long = metadata.component(b"ab").unwrap();
    assert!(!long.as_bytes().starts_with(short.as_bytes()));
    assert_eq!(short.key(&[0, u8::MAX]), {
        let mut expected = short.as_bytes().to_vec();
        expected.extend_from_slice(&[0, u8::MAX]);
        expected
    });

    assert!(matches!(
        FdbStorePrefix::new([]),
        Err(FdbConfigError::StorePrefixLength { .. })
    ));
    assert!(matches!(
        FdbStorePrefix::new(vec![0; MAX_STORE_PREFIX_BYTES + 1]),
        Err(FdbConfigError::StorePrefixLength { .. })
    ));
    assert_eq!(lexicographic_successor(&[0, u8::MAX]), Some(vec![1]));
    assert_eq!(lexicographic_successor(&[u8::MAX]), None);
}

#[test]
fn common_error_classification_preserves_commit_ambiguity_and_limits() {
    assert_eq!(classify_error(1020, false), FdbErrorDisposition::Conflict);
    assert_eq!(
        classify_error(1021, false),
        FdbErrorDisposition::CommitUnknown
    );
    assert_eq!(
        classify_error(1020, true),
        FdbErrorDisposition::CommitUnknown
    );
    assert_eq!(
        classify_error(2101, false),
        FdbErrorDisposition::Limit(FdbLimit::TransactionBytes)
    );
    assert_eq!(
        classify_error(2102, false),
        FdbErrorDisposition::Limit(FdbLimit::KeyBytes)
    );
    assert_eq!(
        classify_error(2103, false),
        FdbErrorDisposition::Limit(FdbLimit::ValueBytes)
    );
    assert_eq!(
        classify_error(1007, false),
        FdbErrorDisposition::Unavailable
    );
}

struct CountedResource(Arc<AtomicUsize>);

impl Drop for CountedResource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn runtime_starts_once_shares_handles_and_never_restarts_after_stop() {
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(RuntimeRegistry::new());

    let first = registry
        .acquire({
            let starts = Arc::clone(&starts);
            let stops = Arc::clone(&stops);
            move || {
                starts.fetch_add(1, Ordering::SeqCst);
                Ok::<_, ()>(CountedResource(stops))
            }
        })
        .unwrap();
    let second = registry
        .acquire(|| -> Result<CountedResource, ()> { panic!("shared runtime must not boot twice") })
        .unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(starts.load(Ordering::SeqCst), 1);

    drop(first);
    assert_eq!(stops.load(Ordering::SeqCst), 0);
    drop(second);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        registry.acquire(|| Ok::<_, ()>(CountedResource(Arc::clone(&stops)))),
        Err(RuntimeAcquireError::Stopped)
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

#[test]
fn runtime_start_failure_is_terminal() {
    let registry = Arc::<RuntimeRegistry<CountedResource>>::new(RuntimeRegistry::new());
    assert!(matches!(
        registry.acquire(|| Err::<CountedResource, _>("boot failed")),
        Err(RuntimeAcquireError::Start("boot failed"))
    ));
    assert!(matches!(
        registry.acquire(|| -> Result<CountedResource, &str> {
            panic!("failed runtime must not retry boot")
        }),
        Err(RuntimeAcquireError::Stopped)
    ));
}

#[test]
fn concurrent_runtime_acquisition_boots_one_shared_resource() {
    let starts = Arc::new(AtomicUsize::new(0));
    let stops = Arc::new(AtomicUsize::new(0));
    let registry = Arc::new(RuntimeRegistry::new());
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let starts = Arc::clone(&starts);
        let stops = Arc::clone(&stops);
        let registry = Arc::clone(&registry);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let core = registry
                .acquire(|| {
                    starts.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(CountedResource(stops))
                })
                .unwrap();
            let identity = Arc::as_ptr(&core) as usize;
            barrier.wait();
            identity
        }));
    }
    barrier.wait();
    let identities = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(identities[0], identities[1]);
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(stops.load(Ordering::SeqCst), 1);
}
