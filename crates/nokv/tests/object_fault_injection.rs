/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Functional regression gate for the object-layer fault-injection chain,
//! porting the observable semantics of the pre-#423
//! `crates/nokv-client/tests/object_gc_fence_live.rs` onto the current
//! operations architecture.
//!
//! The old harness paused a multi-block RustFS PUT mid-flight and verified
//! that a stale prepare was rejected, refreshed, and restaged, and that
//! fork-borrowed blocks survived GC windows. The current architecture has no
//! forks or read-lease grace; its equivalent is the durable staged-publication
//! pipeline with per-operation resumption. This gate injects an ambiguous
//! acknowledgement loss *after* a durable object write at the
//! `ArtifactObjectStore` boundary of a real in-process server and asserts
//! that the client resumes the same operation identity to a typed durable
//! outcome: every block created exactly once, the publication replayed
//! marker set, intact content, and no partial metadata.

mod common;

use std::sync::{Arc, Mutex};

use common::{append, connect, publish_base, read_all, spawn_server, Harness};
use nokv_client::ArtifactPublishOptions;
use nokv_object::{
    ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome, MemoryArtifactStore,
    ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange, ProviderAdmissionReceipt,
    ProviderHandleIdentity,
};
use nokv_protocol::{
    ArtifactRevisionIdentity, ContentType, GetPathRequest, OperationIdentity, PublishCondition,
    WorkspaceReadView,
};
use nokv_types::{ObjectNamespaceId, RootId};

#[derive(Clone, Debug, Default)]
struct FaultState {
    /// One-shot: the create at this global create index writes through to
    /// the inner store, then reports a retryable backend error, simulating
    /// an acknowledgement loss after apply.
    write_then_ambiguous_at: Option<usize>,
    /// Creates dispatched since the last `arm` (after the namespace marker).
    create_count: usize,
    /// Inner creates that durably created a new object since the last `arm`.
    created_count: usize,
    /// True once the injected acknowledgement loss fired.
    ambiguous_fired: bool,
}

impl FaultState {
    fn write_then_ambiguous_at(index: usize) -> Self {
        Self {
            write_then_ambiguous_at: Some(index),
            ..Self::default()
        }
    }
}

/// Fault-injecting wrapper around the memory store, mirroring the old
/// `PausingPutStore` idea at the `ArtifactObjectStore` boundary.
#[derive(Clone, Debug)]
struct FaultStore {
    inner: MemoryArtifactStore,
    state: Arc<Mutex<FaultState>>,
}

impl FaultStore {
    fn new(inner: MemoryArtifactStore) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(FaultState::default())),
        }
    }

    fn arm(&self, state: FaultState) {
        *self.state.lock().unwrap() = state;
    }

    fn created_count(&self) -> usize {
        self.state.lock().unwrap().created_count
    }

    fn ambiguous_fired(&self) -> bool {
        self.state.lock().unwrap().ambiguous_fired
    }
}

impl ArtifactObjectStore for FaultStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        self.inner.capabilities()
    }

    // Forward the inner provider identity and its write-conformance receipt
    // so the publish path admits this fault-injection wrapper exactly like
    // the memory store it wraps.
    fn provider_handle_identity(&self) -> ProviderHandleIdentity {
        self.inner.provider_handle_identity()
    }

    fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
        self.inner.provider_admission_receipt()
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        let mut state = self.state.lock().unwrap();
        state.create_count += 1;
        if let Some(index) = state.write_then_ambiguous_at {
            if state.create_count - 1 == index {
                state.write_then_ambiguous_at = None;
                // The write really happened; only the acknowledgement is
                // lost. The client must resume the same operation and let
                // the idempotent create replay absorb the ambiguity.
                let outcome = self.inner.create_immutable(key, bytes)?;
                if matches!(outcome, ImmutableCreateOutcome::Created) {
                    state.created_count += 1;
                }
                state.ambiguous_fired = true;
                return Err(ObjectError::Backend {
                    detail: "injected post-write acknowledgement loss".to_owned(),
                    retryable: true,
                });
            }
        }
        let outcome = self.inner.create_immutable(key, bytes);
        if matches!(outcome, Ok(ImmutableCreateOutcome::Created)) {
            state.created_count += 1;
        }
        outcome
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.read(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        self.inner.delete(key)
    }
}

/// Build a harness whose client store is the fault wrapper, namespace-bound
/// after the marker write (so the marker itself is never fault-injected).
fn fault_harness() -> (Harness<FaultStore>, FaultStore) {
    let root = RootId::from_bytes(common::ROOT_BYTES);
    let (bind, control) = spawn_server(&[root]);
    let client = connect(bind);
    let fault = FaultStore::new(MemoryArtifactStore::new());
    let namespace_id = ObjectNamespaceId::from_bytes(common::NAMESPACE_BYTES);
    nokv_object::ensure_object_namespace(&fault, namespace_id).unwrap();
    let store = nokv_object::BoundArtifactStore::open(fault.clone(), namespace_id).unwrap();
    let workbench = nokv_protocol::WorkbenchName::new("fault-wb").unwrap();
    client
        .create_workspace(
            client.new_request_id(),
            nokv_protocol::CreateWorkspaceRequest {
                workbench: workbench.clone(),
                workspace_incarnation_id: nokv_protocol::WorkspaceIdentity([1; 16]),
            },
        )
        .unwrap();
    let h = Harness {
        _control: control,
        bind,
        client,
        store,
        workbench,
    };
    assert!(h.bind.port() != 0);
    (h, fault)
}

const PAYLOAD: &[u8] = b"0123456789abcdef"; // 16 bytes -> 8 creates at block size 2

/// An ambiguous acknowledgement loss after a durable object write must
/// resume the same operation identity to a typed durable outcome: the
/// publication reports the replayed marker, every block was created exactly
/// once, the content reads back intact, and no partial metadata is left.
#[test]
fn ambiguous_post_apply_acknowledgement_loss_resumes_to_typed_outcome() {
    let (h, fault) = fault_harness();
    publish_base(
        &h.client,
        &h.store,
        &h.workbench,
        "input/base.bin",
        b"seed",
        0x20,
    );

    // The third staged block create writes through and then loses its
    // acknowledgement. The client must resume the same operation identity
    // (the publish attempt loop replays the identical begin request).
    fault.arm(FaultState::write_then_ambiguous_at(2));
    let options = ArtifactPublishOptions::new(
        OperationIdentity([0x30; 16]),
        ArtifactRevisionIdentity([0x31; 16]),
        common::target(&h.workbench, "input/faulty.bin"),
        PublishCondition::CreateOnly,
        ContentType::new("text/plain").unwrap(),
    )
    .with_block_size(2);
    let outcome = h
        .client
        .publish_artifact(&h.store, options, PAYLOAD)
        .unwrap();

    // Typed durable outcome: the resumption is recorded on the returned
    // call and the publication carries the exact durable result.
    assert!(
        outcome.publication.replayed,
        "resumed publication must report the replayed marker"
    );
    assert_eq!(outcome.publication.value.generation, 1);
    assert_eq!(outcome.publication.value.logical_size, PAYLOAD.len() as u64);
    // Non-vacuous: the injected loss really fired, and the idempotent
    // replay path absorbed at least one re-uploaded block.
    assert!(fault.ambiguous_fired(), "injected fault never fired");
    assert!(
        outcome.upload_stats.replayed >= 1,
        "resumed upload must replay at least one block"
    );

    // Every block was durably created exactly once despite the lost
    // acknowledgement: no block missing, none created twice.
    assert_eq!(
        fault.created_count(),
        PAYLOAD.len() / 2,
        "a block was created more than once or not at all"
    );

    assert_eq!(
        read_all(&h.client, &h.store, &h.workbench, "input/faulty.bin"),
        PAYLOAD
    );

    // No partial metadata state: the path carries one complete descriptor.
    let metadata = h
        .client
        .get_path(GetPathRequest {
            target: common::target(&h.workbench, "input/faulty.bin"),
            view: WorkspaceReadView::Live,
            expected_read_version: None,
            range: None,
            plan_page: None,
            if_none_match: None,
        })
        .unwrap();
    let path_metadata = metadata
        .value
        .metadata
        .expect("published path has metadata");
    assert_eq!(path_metadata.descriptor.logical_size, PAYLOAD.len() as u64);
    assert_eq!(path_metadata.generation, 1);
}

/// An append whose delta-block upload loses its acknowledgement after a
/// durable write must be absorbed by the client attempt replay: the same
/// operation identity resumes, the delta lands exactly once, and the
/// returned publication carries the typed replayed marker.
#[test]
fn ambiguous_append_upload_resumes_without_double_append() {
    let (h, fault) = fault_harness();
    publish_base(
        &h.client,
        &h.store,
        &h.workbench,
        "input/append.bin",
        b"base|",
        0x40,
    );

    // "delta|" is 6 bytes -> 3 creates at block size 2; the second create
    // writes through and then loses its acknowledgement.
    fault.arm(FaultState::write_then_ambiguous_at(1));
    let (created, generation, new_size) = append(
        &h.client,
        &h.store,
        &h.workbench,
        "input/append.bin",
        b"delta|",
        0x50,
    );
    assert!(!created);
    assert_eq!(new_size, 11);
    assert_eq!(generation, 2);

    // The injected loss really fired, and the delta blocks were durably
    // created exactly once each.
    assert!(fault.ambiguous_fired(), "injected fault never fired");
    assert_eq!(
        fault.created_count(),
        3,
        "a delta block was duplicated or lost"
    );

    assert_eq!(
        read_all(&h.client, &h.store, &h.workbench, "input/append.bin"),
        b"base|delta|"
    );
}
