//! Provider-neutral owner-session conformance checks.
//!
//! Every `ControlStore` backend must pass the same owner/epoch/lease
//! transition rules regardless of how it proves session liveness (etcd
//! lease, in-memory session map, or a future local OS-lock owner). The checks
//! here are the executable form of that contract. A harness supplies the
//! store plus one primitive the public trait deliberately lacks: making an
//! owner session disappear without a clean release, which is what an owner
//! process crash looks like to the backend.

use crate::{
    ControlError, ControlStore, LogicalShardId, LogicalShardLease, LogicalShardState, NodeId,
    PlacementGeneration, RecoveryPublication, RecoveryUploadIntent, RootId, RootPlacement,
    RootPlacementLifecycle,
};

pub(crate) trait ConformanceHarness {
    fn store(&self) -> &dyn ControlStore;

    /// Simulate owner-process death: the durable record must remain, the
    /// liveness session must be gone when this returns.
    fn lose_session(&self, lease: &LogicalShardLease);

    /// Backends that key state by identity need distinct ids per scenario.
    fn fresh_shard(&self, scenario: u8) -> (RootId, LogicalShardId);
}

fn empty_publication() -> RecoveryPublication {
    RecoveryPublication {
        checkpoint: None,
        log: None,
        durable_lsn: 0,
    }
}

fn first_upload_intent() -> RecoveryUploadIntent {
    RecoveryUploadIntent {
        object_namespace_id: nokv_types::ObjectNamespaceId::from_bytes([7; 16]),
        first_lsn: 1,
        last_lsn: 3,
        previous_chain_digest: std::iter::repeat_n('0', 64).collect(),
        last_chain_digest: std::iter::repeat_n('b', 64).collect(),
        segment_digest: std::iter::repeat_n('a', 64).collect(),
        manifest_key: "nokv/recovery/log-segments/v1/1-3".to_owned(),
        receipt: vec![4, 5, 6],
        plan: vec![1, 2, 3],
    }
}

fn node(name: &str) -> NodeId {
    NodeId::new(name).unwrap()
}

fn place_and_acquire(
    harness: &dyn ConformanceHarness,
    scenario: u8,
) -> (LogicalShardId, LogicalShardLease) {
    let store = harness.store();
    let (root_id, logical_shard_id) = harness.fresh_shard(scenario);
    store.create_logical_shard(logical_shard_id).unwrap();
    store
        .create_root_placement(RootPlacement {
            root_id,
            logical_shard_id,
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        })
        .unwrap();
    let lease = store
        .acquire_owner(&logical_shard_id, node("node-a"), "node-a:7000".to_owned())
        .unwrap();
    assert_eq!(lease.owner_epoch.get(), 1);
    (logical_shard_id, lease)
}

fn is_fenced(result: &Result<impl std::fmt::Debug, ControlError>) -> bool {
    matches!(
        result,
        Err(ControlError::StaleLease(_)) | Err(ControlError::NotOwner { .. })
    )
}

/// Every owner-only mutation must reject `stale` with a fence error, and must
/// leave the durable record exactly as it was.
fn assert_stale_lease_is_fenced_everywhere(
    store: &dyn ControlStore,
    stale: &LogicalShardLease,
    context: &str,
) {
    let before = store
        .get_logical_shard(&stale.logical_shard_id)
        .unwrap()
        .expect("record exists");
    let intent = first_upload_intent();

    let renew = store.renew_owner(stale);
    assert!(
        is_fenced(&renew),
        "{context}: renew_owner accepted stale lease: {renew:?}"
    );
    let serving = store.mark_serving(stale, empty_publication());
    assert!(
        is_fenced(&serving),
        "{context}: mark_serving accepted stale lease: {serving:?}"
    );
    let prepared = store.prepare_recovery_upload(stale, intent.clone());
    assert!(
        is_fenced(&prepared),
        "{context}: prepare_recovery_upload accepted stale lease: {prepared:?}"
    );
    let finalized = store.finalize_recovery_upload(stale, &intent, empty_publication());
    assert!(
        is_fenced(&finalized),
        "{context}: finalize_recovery_upload accepted stale lease: {finalized:?}"
    );
    let aborted = store.abort_recovery_upload(stale, &intent);
    assert!(
        is_fenced(&aborted),
        "{context}: abort_recovery_upload accepted stale lease: {aborted:?}"
    );
    let suspended = store.suspend_recovery(stale);
    assert!(
        is_fenced(&suspended)
            || matches!(suspended, Err(ControlError::RecoveryStateConflict { .. })),
        "{context}: suspend_recovery accepted stale lease: {suspended:?}"
    );
    let released = store.release_owner(stale);
    assert!(
        is_fenced(&released),
        "{context}: release_owner accepted stale lease: {released:?}"
    );

    let after = store
        .get_logical_shard(&stale.logical_shard_id)
        .unwrap()
        .expect("record exists");
    assert_eq!(
        before, after,
        "{context}: a fenced mutation changed the durable record"
    );
}

/// Handoff red light 5 (first half): a crash while `Recovering(E)` must be
/// rebound at exactly `E`; the successor path is refused; the crashed lease
/// stays fenced for every owner verb (red light 6).
pub(crate) fn recovering_crash_rebinds_same_epoch_and_fences_old_lease(
    harness: &dyn ConformanceHarness,
) {
    let store = harness.store();
    let (shard, first) = place_and_acquire(harness, 1);

    harness.lose_session(&first);

    let record = store.get_logical_shard(&shard).unwrap().unwrap();
    assert_eq!(record.state, LogicalShardState::Recovering);
    assert_eq!(record.owner_epoch, Some(first.owner_epoch));
    assert_eq!(
        record.lease_id, first.lease_id,
        "durable record keeps the crashed lease id"
    );

    assert!(matches!(
        store.acquire_successor(
            &shard,
            first.owner_epoch,
            node("node-b"),
            "node-b:7000".to_owned()
        ),
        Err(ControlError::RecoveryAttemptPending { .. })
    ));

    // Rebind with the same owner identity and epoch: only the lease id can
    // distinguish the crashed session from the rebound one.
    let rebound = store
        .reacquire_recovery(
            &shard,
            first.owner_epoch,
            first.owner.clone(),
            "node-a:7001".to_owned(),
        )
        .unwrap();
    assert_eq!(rebound.owner, first.owner);
    assert_eq!(
        rebound.owner_epoch, first.owner_epoch,
        "recovery rebind must not consume an epoch"
    );
    assert_ne!(rebound.lease_id, first.lease_id);

    assert_stale_lease_is_fenced_everywhere(store, &first, "crashed Recovering lease after rebind");

    assert!(matches!(
        store.reacquire_recovery(
            &shard,
            first.owner_epoch,
            node("node-c"),
            "node-c:7000".to_owned()
        ),
        Err(ControlError::PreviousOwnerSessionLive { .. })
    ));

    let serving = store.mark_serving(&rebound, empty_publication()).unwrap();
    assert_eq!(serving.state, LogicalShardState::Serving);
    assert_eq!(serving.owner_epoch, Some(first.owner_epoch));
    store.release_owner(&rebound).unwrap();
}

/// Handoff red light 5 (second half) and the core judgment: only a
/// successor of a `Serving` owner advances the epoch, it advances by exactly
/// one, and the dead owner's authority can never come back.
pub(crate) fn serving_crash_admits_exactly_one_successor_and_old_authority_never_returns(
    harness: &dyn ConformanceHarness,
) {
    let store = harness.store();
    let (shard, first) = place_and_acquire(harness, 2);
    store.mark_serving(&first, empty_publication()).unwrap();

    // Live owner blocks every takeover path.
    assert!(matches!(
        store.acquire_successor(
            &shard,
            first.owner_epoch,
            node("node-b"),
            "node-b:7000".to_owned()
        ),
        Err(ControlError::PreviousOwnerSessionLive { .. })
    ));

    harness.lose_session(&first);

    // A dead Serving owner cannot be "rebound": recovery rebind is only for
    // an unfinished attempt.
    let rebind = store.reacquire_recovery(
        &shard,
        first.owner_epoch,
        node("node-b"),
        "node-b:7000".to_owned(),
    );
    assert!(
        matches!(rebind, Err(ControlError::RecoveryStateConflict { .. })),
        "reacquire_recovery on a dead Serving owner must be refused: {rebind:?}"
    );

    let second = store
        .acquire_successor(
            &shard,
            first.owner_epoch,
            node("node-b"),
            "node-b:7000".to_owned(),
        )
        .unwrap();
    assert_eq!(second.owner_epoch.get(), first.owner_epoch.get() + 1);

    // Exactly one successor: the same expectation cannot be consumed twice,
    // and the new epoch cannot be skipped while its owner is live.
    assert!(matches!(
        store.acquire_successor(
            &shard,
            first.owner_epoch,
            node("node-c"),
            "node-c:7000".to_owned()
        ),
        Err(ControlError::StaleOwnerEpoch { .. })
    ));
    let overtake = store.acquire_successor(
        &shard,
        second.owner_epoch,
        node("node-c"),
        "node-c:7000".to_owned(),
    );
    assert!(
        matches!(
            overtake,
            Err(ControlError::RecoveryAttemptPending { .. })
                | Err(ControlError::PreviousOwnerSessionLive { .. })
        ),
        "a live successor must not be overtaken: {overtake:?}"
    );

    assert_stale_lease_is_fenced_everywhere(store, &first, "dead Serving lease after successor");

    store.mark_serving(&second, empty_publication()).unwrap();
    assert_stale_lease_is_fenced_everywhere(
        store,
        &first,
        "dead Serving lease after successor is Serving",
    );

    let released = store.release_owner(&second).unwrap();
    assert_eq!(released.state, LogicalShardState::Unassigned);
    assert_eq!(
        released.owner_epoch,
        Some(second.owner_epoch),
        "release retains the last epoch"
    );
    assert_eq!(released.lease_id, 0);

    // After a clean release the old authority is still dead.
    assert_stale_lease_is_fenced_everywhere(
        store,
        &first,
        "dead Serving lease after clean release",
    );
    assert_stale_lease_is_fenced_everywhere(store, &second, "released lease");
}

/// Handoff red light 4: the legal/illegal transition table that every
/// provider must agree on.
pub(crate) fn transition_table_is_provider_neutral(harness: &dyn ConformanceHarness) {
    let store = harness.store();
    let (shard, first) = place_and_acquire(harness, 3);

    // Recovering cannot be released; it can only be suspended or served.
    assert!(matches!(
        store.release_owner(&first),
        Err(ControlError::RecoveryAttemptPending { .. })
    ));
    // Cannot acquire first owner twice.
    assert!(matches!(
        store.acquire_owner(&shard, node("node-x"), "node-x:7000".to_owned()),
        Err(ControlError::LogicalShardAlreadyOwned { .. })
    ));

    // Recovering -> Serving; Serving cannot be suspended as recovery.
    store.mark_serving(&first, empty_publication()).unwrap();
    assert!(matches!(
        store.suspend_recovery(&first),
        Err(ControlError::RecoveryStateConflict { .. })
    ));
    // Serving -> Serving replay with the same frontier is idempotent.
    let again = store.mark_serving(&first, empty_publication()).unwrap();
    assert_eq!(again.state, LogicalShardState::Serving);

    // Clean release, then a successor without any crash.
    store.release_owner(&first).unwrap();
    assert!(matches!(
        store.acquire_owner(&shard, node("node-y"), "node-y:7000".to_owned()),
        Err(ControlError::StaleOwnerEpoch { .. })
            | Err(ControlError::LogicalShardAlreadyOwned { .. })
    ));
    let second = store
        .acquire_successor(
            &shard,
            first.owner_epoch,
            node("node-b"),
            "node-b:7000".to_owned(),
        )
        .unwrap();
    assert_eq!(second.owner_epoch.get(), 2);

    // A suspended Recovering(E+1) keeps its epoch and rebinds without E+2.
    store.suspend_recovery(&second).unwrap();
    assert!(matches!(
        store.acquire_successor(
            &shard,
            second.owner_epoch,
            node("node-c"),
            "node-c:7000".to_owned()
        ),
        Err(ControlError::RecoveryAttemptPending { .. })
    ));
    let rebound = store
        .reacquire_recovery(
            &shard,
            second.owner_epoch,
            node("node-c"),
            "node-c:7000".to_owned(),
        )
        .unwrap();
    assert_eq!(rebound.owner_epoch.get(), 2);
    store.mark_serving(&rebound, empty_publication()).unwrap();
    store.release_owner(&rebound).unwrap();
}

pub(crate) fn run_all(harness: &dyn ConformanceHarness) {
    recovering_crash_rebinds_same_epoch_and_fences_old_lease(harness);
    serving_crash_admits_exactly_one_successor_and_old_authority_never_returns(harness);
    transition_table_is_provider_neutral(harness);
}
