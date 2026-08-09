use std::future::Future;
use std::num::NonZeroU64;
use std::time::{Duration, Instant};

use etcd_client::{
    Client, Compare, CompareOp, GetOptions, LeaseTimeToLiveOptions, PutOptions, Txn, TxnOp,
    TxnOpResponse, TxnResponse,
};
use sha2::{Digest, Sha256};
use tokio::runtime::{Builder, Runtime};

use crate::codec::{
    decode_logical_shard_record, decode_metadata_authority_record, decode_owner_admission_claim,
    decode_owner_admission_plan_sentinel, decode_owner_session, decode_planned_owner_admission,
    decode_root_placement, encode_fixed_id, encode_logical_shard_record,
    encode_metadata_authority_record, encode_owner_admission_claim, encode_owner_admission_intent,
    encode_owner_admission_plan_sentinel, encode_owner_session, encode_planned_owner_admission,
    encode_root_placement,
};
use crate::owner_admission_command::{
    AbortOwnerAdmissionCommandV1, AbortOwnerAdmissionNotDispatchedV1, AbortOwnerAdmissionOutcomeV1,
    AbortOwnerAdmissionResultV1, ClaimedCommitOwnerAdmissionCommandV1,
    ClaimedPublishOwnerServingCommandV1, ClaimedReconcileOwnerAdmissionCommandV1,
    ClaimedRenewOwnerSessionCommandV1, CommitOwnerAdmissionCommandV1,
    CommitOwnerAdmissionNotDispatchedV1, CommitOwnerAdmissionOutcomeV1,
    CommitOwnerAdmissionResultV1, PrepareOwnerAdmissionCommandV1,
    PrepareOwnerAdmissionNotDispatchedV1, PrepareOwnerAdmissionOutcomeV1,
    PrepareOwnerAdmissionResultV1, PublishOwnerServingCommandV1,
    PublishOwnerServingNotDispatchedV1, PublishOwnerServingOutcomeV1, PublishOwnerServingResultV1,
    ReconcileOwnerAdmissionCommandV1, ReconcileOwnerAdmissionNotDispatchedV1,
    ReconcileOwnerAdmissionOutcomeV1, ReconcileOwnerAdmissionResultV1,
    ReconcileOwnerAdmissionTargetV1, RenewOwnerSessionCommandV1, RenewOwnerSessionNotDispatchedV1,
    RenewOwnerSessionOutcomeV1, RenewOwnerSessionResultV1, TerminateOwnerAdmissionCommandV1,
    TerminateOwnerAdmissionNotDispatchedV1, TerminateOwnerAdmissionOutcomeV1,
    TerminateOwnerAdmissionResultV1,
};
use crate::owner_admission_state::{
    classify_owner_session_renewal, plan_owner_admission_abort, plan_owner_admission_commit,
    plan_owner_admission_intent_seal_rejected, plan_owner_admission_prepare,
    plan_owner_admission_terminate, plan_owner_serving_publication, reconcile_owner_admission,
    reconcile_owner_admission_intent, validate_terminated_record_descendant,
    AuthoritativeSentinelAbsenceEvidence, AuthoritativeSessionAbsenceEvidence,
    OwnerAdmissionExactSnapshot, OwnerAdmissionInconsistencyCode, OwnerAdmissionMutationPlan,
    OwnerAdmissionReconcileClassification, OwnerAdmissionReconcileDecision,
    OwnerAdmissionSentinelExpectation, OwnerAdmissionSessionExpectation,
    OwnerAdmissionStateDecision, OwnerServingPublicationStateDecision,
    OwnerSessionRenewalStateDecision,
};
use crate::store::{
    classify_fresh_root_provisioning_replay, ensure_unowned_for_cutover,
    metadata_authority_update_enters_ready, metadata_authority_update_installs_source_receipt,
    metadata_authority_update_requires_unowned, prepare_expired_owner_cleanup,
    prepare_mark_serving, prepare_owner_acquisition, prepare_owner_release,
    validate_authority_for_owner_operation, validate_fresh_authority_shard,
    validate_fresh_root_provisioning_input, validate_lease_serving_admission,
    validate_logical_shard_record, validate_metadata_authority_update,
    validate_new_metadata_authority, validate_new_root_placement, validate_record_lease,
    validate_root_placement_update, validate_source_receipt_control_epoch, ControlStore,
    OwnerServingAdmission,
};
#[cfg(test)]
use crate::RootPlacementLifecycle;
use crate::{
    ControlError, EtcdControlStoreOptions, FreshRootProvisioningDisposition,
    FreshRootProvisioningOutcome, LogicalShardId, LogicalShardLease, LogicalShardRecord,
    MetadataAuthorityRecord, NodeId, OwnerAdmissionClaimPhaseV1, OwnerAdmissionClaimV1,
    OwnerAdmissionIntentV1, OwnerAdmissionPlanSentinelV1, OwnerAdmissionTerminationReasonV1,
    OwnerEpoch, OwnerIncarnationId, OwnerLeaseExpiryEvidenceDigest, OwnerLeaseModel,
    OwnerReleaseOutcome, OwnerSessionLifetimeProofDigestV1, OwnerSessionRenewalTargetV1,
    PlannedOwnerAdmissionV1, PlannedOwnerServingPublicationV1, RecoveryPublication, RootId,
    RootPlacement,
};

const ETCD_OWNER_LEASE_MODEL: OwnerLeaseModel = OwnerLeaseModel::FiniteAuthoritativeTtl;

pub struct EtcdControlStore {
    options: EtcdControlStoreOptions,
    runtime: Runtime,
    client: Client,
}

struct StoredRootPlacement {
    placement: RootPlacement,
    encoded: Vec<u8>,
}

struct StoredLogicalShard {
    record: LogicalShardRecord,
    encoded: Vec<u8>,
}

struct StoredMetadataAuthority {
    authority: MetadataAuthorityRecord,
    encoded: Vec<u8>,
}

struct StoredOwnerSession {
    lease: LogicalShardLease,
    encoded: Vec<u8>,
    attached_lease_id: i64,
}

struct PreparedOwnerInstallation<'a> {
    current: &'a StoredLogicalShard,
    placement: &'a StoredRootPlacement,
    authority: &'a StoredMetadataAuthority,
    next: &'a LogicalShardRecord,
    lease: &'a LogicalShardLease,
    expected_owner_epoch: Option<OwnerEpoch>,
}

struct FreshRootProvisioningTxn {
    shard_key: Vec<u8>,
    authority_key: Vec<u8>,
    placement_key: Vec<u8>,
    session_key: Vec<u8>,
    shard_value: Vec<u8>,
    authority_value: Vec<u8>,
    placement_value: Vec<u8>,
}

#[derive(Clone)]
struct OwnerAdmissionSnapshotKeys {
    shard: Vec<u8>,
    placement: Vec<u8>,
    authority: Vec<u8>,
    session: Vec<u8>,
    plan: Vec<u8>,
    sentinel: Vec<u8>,
    candidate_claim: Vec<u8>,
    previous_claim: Option<Vec<u8>>,
}

impl OwnerAdmissionSnapshotKeys {
    fn new(options: &EtcdControlStoreOptions, intent: &OwnerAdmissionIntentV1) -> Self {
        let logical_shard_id = intent.logical_shard_id();
        Self {
            shard: options.logical_shard_record_key(&logical_shard_id),
            placement: options.root_placement_key(&intent.admission().placement().root_id),
            authority: metadata_authority_key(options, &logical_shard_id),
            session: options.logical_shard_session_key(&logical_shard_id),
            plan: options.owner_admission_plan_key(&logical_shard_id),
            sentinel: options.owner_admission_sentinel_key(&logical_shard_id),
            candidate_claim: options
                .owner_incarnation_claim_key(&logical_shard_id, &intent.owner_incarnation_id()),
            previous_claim: intent.expected_previous_claim().map(|claim| {
                options.owner_incarnation_claim_key(
                    &claim.identity().logical_shard_id(),
                    &claim.identity().owner_incarnation_id(),
                )
            }),
        }
    }

    fn ordered(&self) -> Vec<Vec<u8>> {
        let mut keys = vec![
            self.shard.clone(),
            self.placement.clone(),
            self.authority.clone(),
            self.session.clone(),
            self.plan.clone(),
            self.sentinel.clone(),
            self.candidate_claim.clone(),
        ];
        if let Some(previous_claim) = self.previous_claim.as_ref() {
            keys.push(previous_claim.clone());
        }
        keys
    }

    fn read_ops(&self) -> Vec<TxnOp> {
        self.ordered()
            .into_iter()
            .map(|key| TxnOp::get(key, None))
            .collect()
    }
}

#[derive(Clone, PartialEq, Eq)]
struct EtcdSnapshotEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    attached_lease_id: i64,
}

struct EtcdOwnerAdmissionSnapshot {
    cluster_id: u64,
    revision: i64,
    state: OwnerAdmissionExactSnapshot,
}

#[derive(Clone, PartialEq, Eq)]
enum EtcdExpectedValue {
    Absent,
    Exact {
        value: Vec<u8>,
        attached_lease_id: i64,
    },
}

#[derive(Clone, PartialEq, Eq)]
struct EtcdExactCompare {
    key: Vec<u8>,
    expected: EtcdExpectedValue,
}

#[derive(Clone, PartialEq, Eq)]
enum EtcdWriteOperation {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        lease_id: Option<i64>,
    },
    Delete {
        key: Vec<u8>,
    },
}

struct EtcdOwnerAdmissionWritePlan {
    comparisons: Vec<EtcdExactCompare>,
    operations: Vec<EtcdWriteOperation>,
    else_keys: OwnerAdmissionSnapshotKeys,
}

impl EtcdOwnerAdmissionWritePlan {
    fn into_txn(self) -> Txn {
        let mut comparisons = Vec::new();
        for comparison in self.comparisons {
            match comparison.expected {
                EtcdExpectedValue::Absent => {
                    comparisons.push(Compare::version(comparison.key, CompareOp::Equal, 0))
                }
                EtcdExpectedValue::Exact {
                    value,
                    attached_lease_id,
                } => {
                    comparisons.push(Compare::value(
                        comparison.key.clone(),
                        CompareOp::Equal,
                        value,
                    ));
                    comparisons.push(Compare::lease(
                        comparison.key,
                        CompareOp::Equal,
                        attached_lease_id,
                    ));
                }
            }
        }
        let operations = self
            .operations
            .into_iter()
            .map(|operation| match operation {
                EtcdWriteOperation::Put {
                    key,
                    value,
                    lease_id,
                } => TxnOp::put(
                    key,
                    value,
                    lease_id.map(|lease_id| PutOptions::new().with_lease(lease_id)),
                ),
                EtcdWriteOperation::Delete { key } => TxnOp::delete(key, None),
            })
            .collect::<Vec<_>>();
        Txn::new()
            .when(comparisons)
            .and_then(operations)
            .or_else(self.else_keys.read_ops())
    }
}

#[derive(Clone, Copy)]
enum EtcdOwnerAdmissionReadError {
    Io,
    Inconsistent(OwnerAdmissionInconsistencyCode),
}

#[derive(Clone, PartialEq, Eq)]
enum OwnerAdmissionExpiryCandidate {
    Sentinel(PlannedOwnerAdmissionV1),
    Session(PlannedOwnerAdmissionV1),
}

struct LeaseDeadProof {
    cluster_id: u64,
    revision: i64,
    lease_id: i64,
    ttl: i64,
    granted_ttl: i64,
}

enum EtcdClosedResult<R> {
    Proven(R),
    OutcomeUnknown,
}

enum EtcdCommitDisposition {
    Live {
        already_committed: bool,
        shard: LogicalShardRecord,
        lease: LogicalShardLease,
        claim: OwnerAdmissionClaimV1,
    },
    Closed(CommitOwnerAdmissionResultV1),
}

enum EtcdReconcileDisposition {
    Live {
        plan: PlannedOwnerAdmissionV1,
        shard: LogicalShardRecord,
        lease: LogicalShardLease,
        claim: OwnerAdmissionClaimV1,
    },
    Closed(ReconcileOwnerAdmissionResultV1),
}

enum EtcdPublishDisposition {
    Live {
        already_published: bool,
        shard: LogicalShardRecord,
        lease: LogicalShardLease,
        claim: OwnerAdmissionClaimV1,
    },
    Closed(PublishOwnerServingResultV1),
}

enum EtcdReconcileStateDecision {
    Admission(OwnerAdmissionReconcileDecision),
    Serving(OwnerServingPublicationStateDecision),
}

struct EtcdLiveOwnerSessionProof {
    shard: LogicalShardRecord,
    lease: LogicalShardLease,
    claim: OwnerAdmissionClaimV1,
    observed_ttl_seconds: NonZeroU64,
    proof_digest: OwnerSessionLifetimeProofDigestV1,
}

fn live_proof_descends_from(
    observed_shard: &LogicalShardRecord,
    observed_lease: &LogicalShardLease,
    observed_claim: &OwnerAdmissionClaimV1,
    proof: &EtcdLiveOwnerSessionProof,
) -> bool {
    if proof.lease != *observed_lease || proof.claim != *observed_claim {
        return false;
    }
    proof.shard == *observed_shard
        || (observed_shard.state == crate::LogicalShardState::Recovering
            && proof.shard.state == crate::LogicalShardState::Serving)
}

enum EtcdPreparePreGrant {
    AllocateLease,
    Closed(Box<PrepareOwnerAdmissionResultV1>),
}

impl FreshRootProvisioningTxn {
    fn new(
        options: &EtcdControlStoreOptions,
        initial_placement: &RootPlacement,
        initial_authority: &MetadataAuthorityRecord,
    ) -> Result<Self, ControlError> {
        let desired_shard = LogicalShardRecord::unassigned(initial_placement.logical_shard_id);
        Ok(Self {
            shard_key: options.logical_shard_record_key(&initial_placement.logical_shard_id),
            authority_key: metadata_authority_key(options, &initial_placement.logical_shard_id),
            placement_key: options.root_placement_key(&initial_placement.root_id),
            session_key: options.logical_shard_session_key(&initial_placement.logical_shard_id),
            shard_value: encode_logical_shard_record(&desired_shard)?,
            authority_value: encode_metadata_authority_record(initial_authority)?,
            placement_value: encode_root_placement(initial_placement)?,
        })
    }

    fn into_txn(self) -> Txn {
        Txn::new()
            .when(vec![
                Compare::version(self.shard_key.clone(), CompareOp::Equal, 0),
                Compare::version(self.authority_key.clone(), CompareOp::Equal, 0),
                Compare::version(self.placement_key.clone(), CompareOp::Equal, 0),
                Compare::version(self.session_key, CompareOp::Equal, 0),
            ])
            .and_then(vec![
                TxnOp::put(self.shard_key, self.shard_value, None),
                TxnOp::put(self.authority_key, self.authority_value, None),
                TxnOp::put(self.placement_key, self.placement_value, None),
            ])
    }
}

impl EtcdControlStore {
    pub fn connect(options: EtcdControlStoreOptions) -> Result<Self, ControlError> {
        options.validate()?;
        let runtime = Builder::new_multi_thread()
            .thread_name("nokv-control-etcd")
            .enable_all()
            .build()
            .map_err(|error| {
                ControlError::Backend(format!("etcd runtime initialization failed: {error}"))
            })?;
        let client = runtime
            .block_on(Client::connect(options.endpoints(), None))
            .map_err(etcd_backend)?;
        Ok(Self {
            options,
            runtime,
            client,
        })
    }

    pub fn options(&self) -> &EtcdControlStoreOptions {
        &self.options
    }

    fn block_on<T>(
        &self,
        future: impl Future<Output = Result<T, ControlError>>,
    ) -> Result<T, ControlError> {
        self.runtime.block_on(future)
    }

    fn block_on_closed<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime.block_on(future)
    }
}

fn snapshot_entries_from_txn(
    response: &TxnResponse,
    expected_count: usize,
) -> Result<(u64, i64, Vec<Option<EtcdSnapshotEntry>>), OwnerAdmissionInconsistencyCode> {
    let header = response
        .header()
        .ok_or(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
    if header.cluster_id() == 0 || header.revision() <= 0 {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    }
    let responses = response.op_responses();
    if responses.len() != expected_count {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    }
    let mut entries = Vec::with_capacity(expected_count);
    for response in responses {
        let TxnOpResponse::Get(get) = response else {
            return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
        };
        if get.more() || get.kvs().len() > 1 {
            return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
        }
        entries.push(get.kvs().first().map(|kv| EtcdSnapshotEntry {
            key: kv.key().to_vec(),
            value: kv.value().to_vec(),
            attached_lease_id: kv.lease(),
        }));
    }
    Ok((header.cluster_id(), header.revision(), entries))
}

fn require_snapshot_entry<'a>(
    entry: &'a Option<EtcdSnapshotEntry>,
    expected_key: &[u8],
    mismatch: OwnerAdmissionInconsistencyCode,
) -> Result<Option<&'a EtcdSnapshotEntry>, OwnerAdmissionInconsistencyCode> {
    match entry.as_ref() {
        Some(entry) if entry.key == expected_key => Ok(Some(entry)),
        Some(_) => Err(mismatch),
        None => Ok(None),
    }
}

fn decode_unleased_snapshot_value<T>(
    entry: &Option<EtcdSnapshotEntry>,
    expected_key: &[u8],
    mismatch: OwnerAdmissionInconsistencyCode,
    decode: impl FnOnce(&[u8]) -> Result<T, ControlError>,
) -> Result<Option<T>, OwnerAdmissionInconsistencyCode> {
    let Some(entry) = require_snapshot_entry(entry, expected_key, mismatch)? else {
        return Ok(None);
    };
    if entry.attached_lease_id != 0 {
        return Err(mismatch);
    }
    decode(&entry.value)
        .map(Some)
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)
}

fn decode_owner_admission_snapshot(
    keys: &OwnerAdmissionSnapshotKeys,
    cluster_id: u64,
    revision: i64,
    entries: Vec<Option<EtcdSnapshotEntry>>,
) -> Result<EtcdOwnerAdmissionSnapshot, OwnerAdmissionInconsistencyCode> {
    let expected_count = keys.ordered().len();
    if cluster_id == 0 || revision <= 0 || entries.len() != expected_count {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    }
    let shard = decode_unleased_snapshot_value(
        &entries[0],
        &keys.shard,
        OwnerAdmissionInconsistencyCode::LogicalShardKeyMismatch,
        decode_logical_shard_record,
    )?;
    let placement = decode_unleased_snapshot_value(
        &entries[1],
        &keys.placement,
        OwnerAdmissionInconsistencyCode::PlacementKeyMismatch,
        decode_root_placement,
    )?;
    let authority = decode_unleased_snapshot_value(
        &entries[2],
        &keys.authority,
        OwnerAdmissionInconsistencyCode::AuthorityKeyMismatch,
        decode_metadata_authority_record,
    )?;

    let session = match require_snapshot_entry(
        &entries[3],
        &keys.session,
        OwnerAdmissionInconsistencyCode::SessionKeyMismatch,
    )? {
        Some(entry) => {
            let session = decode_owner_session(&entry.value)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
            if lease_id_i64(session.lease_id).ok() != Some(entry.attached_lease_id) {
                return Err(OwnerAdmissionInconsistencyCode::SessionKeyMismatch);
            }
            Some(session)
        }
        None => None,
    };
    let active_plan = decode_unleased_snapshot_value(
        &entries[4],
        &keys.plan,
        OwnerAdmissionInconsistencyCode::ActivePlanShardMismatch,
        decode_planned_owner_admission,
    )?;
    let sentinel = match require_snapshot_entry(
        &entries[5],
        &keys.sentinel,
        OwnerAdmissionInconsistencyCode::SentinelShardMismatch,
    )? {
        Some(entry) => {
            let sentinel = decode_owner_admission_plan_sentinel(&entry.value)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
            if lease_id_i64(sentinel.lease_id()).ok() != Some(entry.attached_lease_id) {
                return Err(OwnerAdmissionInconsistencyCode::SentinelShardMismatch);
            }
            Some(sentinel)
        }
        None => None,
    };
    let candidate_claim = decode_unleased_snapshot_value(
        &entries[6],
        &keys.candidate_claim,
        OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch,
        decode_owner_admission_claim,
    )?;
    let previous_claim = if let Some(previous_key) = keys.previous_claim.as_ref() {
        decode_unleased_snapshot_value(
            &entries[7],
            previous_key,
            OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch,
            decode_owner_admission_claim,
        )?
    } else {
        None
    };

    Ok(EtcdOwnerAdmissionSnapshot {
        cluster_id,
        revision,
        state: OwnerAdmissionExactSnapshot::new(
            shard,
            placement,
            authority,
            session,
            None,
            active_plan,
            sentinel,
            None,
            candidate_claim,
            previous_claim,
        ),
    })
}

fn owner_admission_snapshot_from_txn(
    keys: &OwnerAdmissionSnapshotKeys,
    response: &TxnResponse,
) -> Result<EtcdOwnerAdmissionSnapshot, OwnerAdmissionInconsistencyCode> {
    let (cluster_id, revision, entries) =
        snapshot_entries_from_txn(response, keys.ordered().len())?;
    decode_owner_admission_snapshot(keys, cluster_id, revision, entries)
}

async fn read_owner_admission_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
) -> Result<EtcdOwnerAdmissionSnapshot, EtcdOwnerAdmissionReadError> {
    let response = client
        .txn(Txn::new().and_then(keys.read_ops()))
        .await
        .map_err(|_| EtcdOwnerAdmissionReadError::Io)?;
    owner_admission_snapshot_from_txn(keys, &response)
        .map_err(EtcdOwnerAdmissionReadError::Inconsistent)
}

fn claim_matches_intent(claim: &OwnerAdmissionClaimV1, intent: &OwnerAdmissionIntentV1) -> bool {
    let identity = claim.identity();
    identity.logical_shard_id() == intent.logical_shard_id()
        && identity.owner_incarnation_id() == intent.owner_incarnation_id()
        && identity.intent_digest() == intent.digest()
        && identity.reservation_digest() == intent.reservation_digest()
        && identity.planned_epoch() == intent.planned_epoch()
}

fn reconstruct_owner_admission_plan(
    intent: &OwnerAdmissionIntentV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<PlannedOwnerAdmissionV1, OwnerAdmissionInconsistencyCode> {
    if !claim_matches_intent(claim, intent) {
        return Err(OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch);
    }
    let (lease, plan_digest) = match claim.phase() {
        OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest }
        | OwnerAdmissionClaimPhaseV1::Aborted {
            lease, plan_digest, ..
        }
        | OwnerAdmissionClaimPhaseV1::Terminated {
            lease, plan_digest, ..
        } => (lease, *plan_digest),
        OwnerAdmissionClaimPhaseV1::Rejected { .. } => {
            return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
        }
    };
    let plan = PlannedOwnerAdmissionV1::new(intent.clone(), lease.clone())
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
    if plan.digest() != plan_digest {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    }
    Ok(plan)
}

fn plan_for_expiry_candidate(
    target: &ReconcileOwnerAdmissionTargetV1,
    claim: &OwnerAdmissionClaimV1,
) -> Option<PlannedOwnerAdmissionV1> {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            reconstruct_owner_admission_plan(intent, claim).ok()
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => Some(plan.clone()),
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            Some(publication.plan().clone())
        }
    }
}

fn owner_admission_expiry_candidate(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Option<OwnerAdmissionExpiryCandidate> {
    let claim = snapshot.candidate_claim.as_ref()?;
    let plan = plan_for_expiry_candidate(target, claim)?;
    match claim.phase() {
        OwnerAdmissionClaimPhaseV1::Prepared { .. }
            if OwnerAdmissionClaimV1::prepared(&plan).ok().as_ref() == Some(claim)
                && snapshot.active_plan.as_ref() == Some(&plan)
                && snapshot.sentinel.is_none() =>
        {
            Some(OwnerAdmissionExpiryCandidate::Sentinel(plan))
        }
        OwnerAdmissionClaimPhaseV1::Committed { .. }
            if OwnerAdmissionClaimV1::prepared(&plan)
                .and_then(OwnerAdmissionClaimV1::commit)
                .ok()
                .as_ref()
                == Some(claim)
                && snapshot.active_plan.is_none()
                && snapshot.sentinel.is_none()
                && snapshot.session.is_none() =>
        {
            Some(OwnerAdmissionExpiryCandidate::Session(plan))
        }
        _ => None,
    }
}

async fn prove_owner_admission_lease_dead(
    client: &mut Client,
    observed_cluster_id: u64,
    observed_revision: i64,
    lease_id: u64,
) -> Result<Option<LeaseDeadProof>, EtcdOwnerAdmissionReadError> {
    let lease_id = lease_id_i64(lease_id).map_err(|_| {
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    let response = client
        .lease_time_to_live(lease_id, None)
        .await
        .map_err(|_| EtcdOwnerAdmissionReadError::Io)?;
    if response.id() != lease_id {
        return Err(EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        ));
    }
    if response.ttl() != -1 {
        return Ok(None);
    }
    let header = response.header().ok_or({
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    if header.cluster_id() != observed_cluster_id || header.revision() < observed_revision {
        return Err(EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        ));
    }
    Ok(Some(LeaseDeadProof {
        cluster_id: header.cluster_id(),
        revision: header.revision(),
        lease_id,
        ttl: response.ttl(),
        granted_ttl: response.granted_ttl(),
    }))
}

fn update_digest_bytes(
    hasher: &mut Sha256,
    bytes: &[u8],
) -> Result<(), OwnerAdmissionInconsistencyCode> {
    let length = u64::try_from(bytes.len())
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn derive_owner_lease_expiry_digest(
    snapshot: &EtcdOwnerAdmissionSnapshot,
    keys: &OwnerAdmissionSnapshotKeys,
    plan: &PlannedOwnerAdmissionV1,
    proof: &LeaseDeadProof,
) -> Result<OwnerLeaseExpiryEvidenceDigest, OwnerAdmissionInconsistencyCode> {
    let session = encode_owner_session(plan.lease())
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-owner-lease-expiry-evidence-v1\0");
    hasher.update(snapshot.cluster_id.to_be_bytes());
    hasher.update(snapshot.revision.to_be_bytes());
    hasher.update(proof.cluster_id.to_be_bytes());
    hasher.update(proof.revision.to_be_bytes());
    hasher.update(proof.lease_id.to_be_bytes());
    hasher.update(proof.ttl.to_be_bytes());
    hasher.update(proof.granted_ttl.to_be_bytes());
    update_digest_bytes(&mut hasher, &keys.session)?;
    update_digest_bytes(&mut hasher, &session)?;
    hasher.update(plan.digest().as_bytes());
    OwnerLeaseExpiryEvidenceDigest::from_bytes(hasher.finalize().into())
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)
}

fn exact_live_owner_session(
    plan: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<
    (LogicalShardRecord, LogicalShardLease, OwnerAdmissionClaimV1),
    EtcdOwnerAdmissionReadError,
> {
    match reconcile_owner_admission(plan, snapshot) {
        OwnerAdmissionReconcileDecision::Classified(classification) => match *classification {
            OwnerAdmissionReconcileClassification::Committed {
                claim,
                record,
                session,
            } => Ok((record, session, claim)),
            _ => Err(EtcdOwnerAdmissionReadError::Io),
        },
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            Err(EtcdOwnerAdmissionReadError::Inconsistent(code))
        }
    }
}

async fn prove_exact_live_owner_session(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    plan: &PlannedOwnerAdmissionV1,
) -> Result<EtcdLiveOwnerSessionProof, EtcdOwnerAdmissionReadError> {
    let before = read_owner_admission_snapshot(client, keys).await?;
    let (before_record, before_session, before_claim) =
        exact_live_owner_session(plan, &before.state)?;
    let lease_id = lease_id_i64(before_session.lease_id).map_err(|_| {
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;

    let query_started = Instant::now();
    let response = client
        .lease_time_to_live(lease_id, Some(LeaseTimeToLiveOptions::new().with_keys()))
        .await
        .map_err(|_| EtcdOwnerAdmissionReadError::Io)?;
    let header = response.header().ok_or({
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    let observed_ttl_seconds = u64::try_from(response.ttl())
        .ok()
        .and_then(NonZeroU64::new)
        .ok_or(EtcdOwnerAdmissionReadError::Io)?;
    if response.id() != lease_id
        || response.granted_ttl() <= 0
        || response.ttl() > response.granted_ttl()
        || header.cluster_id() != before.cluster_id
        || header.revision() < before.revision
        || response
            .keys()
            .iter()
            .filter(|key| key.as_slice() == keys.session.as_slice())
            .count()
            != 1
    {
        return Err(EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        ));
    }

    let after = read_owner_admission_snapshot(client, keys).await?;
    let (after_record, after_session, after_claim) = exact_live_owner_session(plan, &after.state)?;
    if after.cluster_id != before.cluster_id
        || after.revision < header.revision()
        || before_session != after_session
        || before_claim != after_claim
    {
        return Err(EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        ));
    }
    if query_started.elapsed() >= Duration::from_secs(observed_ttl_seconds.get()) {
        return Err(EtcdOwnerAdmissionReadError::Io);
    }

    let before_record_encoded = encode_logical_shard_record(&before_record).map_err(|_| {
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    let after_record_encoded = encode_logical_shard_record(&after_record).map_err(|_| {
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    let session = encode_owner_session(&after_session).map_err(|_| {
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    let claim = encode_owner_admission_claim(&after_claim).map_err(|_| {
        EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        )
    })?;
    let mut hasher = Sha256::new();
    hasher.update(b"nokv-owner-session-live-evidence-v1\0");
    hasher.update(before.cluster_id.to_be_bytes());
    hasher.update(before.revision.to_be_bytes());
    hasher.update(header.revision().to_be_bytes());
    hasher.update(after.revision.to_be_bytes());
    hasher.update(lease_id.to_be_bytes());
    hasher.update(response.ttl().to_be_bytes());
    hasher.update(response.granted_ttl().to_be_bytes());
    update_digest_bytes(&mut hasher, &keys.session)
        .map_err(EtcdOwnerAdmissionReadError::Inconsistent)?;
    update_digest_bytes(&mut hasher, &session)
        .map_err(EtcdOwnerAdmissionReadError::Inconsistent)?;
    update_digest_bytes(&mut hasher, &claim).map_err(EtcdOwnerAdmissionReadError::Inconsistent)?;
    update_digest_bytes(&mut hasher, &before_record_encoded)
        .map_err(EtcdOwnerAdmissionReadError::Inconsistent)?;
    update_digest_bytes(&mut hasher, &after_record_encoded)
        .map_err(EtcdOwnerAdmissionReadError::Inconsistent)?;
    let proof_digest = OwnerSessionLifetimeProofDigestV1::from_bytes(hasher.finalize().into())
        .map_err(|_| {
            EtcdOwnerAdmissionReadError::Inconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            )
        })?;

    Ok(EtcdLiveOwnerSessionProof {
        shard: after_record,
        lease: after_session,
        claim: after_claim,
        observed_ttl_seconds,
        proof_digest,
    })
}

async fn enrich_owner_admission_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    target: &ReconcileOwnerAdmissionTargetV1,
    first: EtcdOwnerAdmissionSnapshot,
    session_digest_override: Option<OwnerLeaseExpiryEvidenceDigest>,
) -> Result<EtcdOwnerAdmissionSnapshot, EtcdOwnerAdmissionReadError> {
    let Some(candidate) = owner_admission_expiry_candidate(target, &first.state) else {
        return Ok(first);
    };
    let lease_id = match &candidate {
        OwnerAdmissionExpiryCandidate::Sentinel(plan)
        | OwnerAdmissionExpiryCandidate::Session(plan) => plan.lease().lease_id,
    };
    let Some(proof) =
        prove_owner_admission_lease_dead(client, first.cluster_id, first.revision, lease_id)
            .await?
    else {
        return Ok(first);
    };

    let mut verified = read_owner_admission_snapshot(client, keys).await?;
    if verified.cluster_id != first.cluster_id
        || verified.revision < first.revision
        || verified.revision < proof.revision
    {
        return Err(EtcdOwnerAdmissionReadError::Inconsistent(
            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
        ));
    }
    if owner_admission_expiry_candidate(target, &verified.state).as_ref() != Some(&candidate) {
        return Ok(verified);
    }
    match candidate {
        OwnerAdmissionExpiryCandidate::Sentinel(plan) => {
            verified.state.sentinel_absence_evidence = Some(
                AuthoritativeSentinelAbsenceEvidence::after_backend_check(&plan),
            );
        }
        OwnerAdmissionExpiryCandidate::Session(plan) => {
            let evidence_digest = match session_digest_override {
                Some(digest) => digest,
                None => derive_owner_lease_expiry_digest(&verified, keys, &plan, &proof)
                    .map_err(EtcdOwnerAdmissionReadError::Inconsistent)?,
            };
            verified.state.session_absence_evidence = Some(
                AuthoritativeSessionAbsenceEvidence::after_backend_check(&plan, evidence_digest),
            );
        }
    }
    Ok(verified)
}

async fn read_enriched_owner_admission_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    target: &ReconcileOwnerAdmissionTargetV1,
    session_digest_override: Option<OwnerLeaseExpiryEvidenceDigest>,
) -> Result<EtcdOwnerAdmissionSnapshot, EtcdOwnerAdmissionReadError> {
    let snapshot = read_owner_admission_snapshot(client, keys).await?;
    enrich_owner_admission_snapshot(client, keys, target, snapshot, session_digest_override).await
}

fn encoded_value<T>(
    value: Option<&T>,
    encode: impl FnOnce(&T) -> Result<Vec<u8>, ControlError>,
) -> Result<Option<Vec<u8>>, OwnerAdmissionInconsistencyCode> {
    value
        .map(encode)
        .transpose()
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)
}

fn expected_absent(key: &[u8]) -> EtcdExactCompare {
    EtcdExactCompare {
        key: key.to_vec(),
        expected: EtcdExpectedValue::Absent,
    }
}

fn expected_exact(key: &[u8], value: Vec<u8>, attached_lease_id: i64) -> EtcdExactCompare {
    EtcdExactCompare {
        key: key.to_vec(),
        expected: EtcdExpectedValue::Exact {
            value,
            attached_lease_id,
        },
    }
}

fn expected_optional(
    key: &[u8],
    value: Option<Vec<u8>>,
    attached_lease_id: i64,
) -> EtcdExactCompare {
    value.map_or_else(
        || expected_absent(key),
        |value| expected_exact(key, value, attached_lease_id),
    )
}

fn owner_admission_snapshot_comparisons(
    keys: &OwnerAdmissionSnapshotKeys,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> Result<Vec<EtcdExactCompare>, OwnerAdmissionInconsistencyCode> {
    let session_lease_id = snapshot
        .session
        .as_ref()
        .map(|lease| lease_id_i64(lease.lease_id))
        .transpose()
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?
        .unwrap_or(0);
    let sentinel_lease_id = snapshot
        .sentinel
        .as_ref()
        .map(|sentinel| lease_id_i64(sentinel.lease_id()))
        .transpose()
        .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?
        .unwrap_or(0);
    let mut comparisons = vec![
        expected_optional(
            &keys.shard,
            encoded_value(
                snapshot.logical_shard_record.as_ref(),
                encode_logical_shard_record,
            )?,
            0,
        ),
        expected_optional(
            &keys.placement,
            encoded_value(snapshot.placement.as_ref(), encode_root_placement)?,
            0,
        ),
        expected_optional(
            &keys.authority,
            encoded_value(
                snapshot.authority.as_ref(),
                encode_metadata_authority_record,
            )?,
            0,
        ),
        expected_optional(
            &keys.session,
            encoded_value(snapshot.session.as_ref(), encode_owner_session)?,
            session_lease_id,
        ),
        expected_optional(
            &keys.plan,
            encoded_value(
                snapshot.active_plan.as_ref(),
                encode_planned_owner_admission,
            )?,
            0,
        ),
        expected_optional(
            &keys.sentinel,
            encoded_value(
                snapshot.sentinel.as_ref(),
                encode_owner_admission_plan_sentinel,
            )?,
            sentinel_lease_id,
        ),
        expected_optional(
            &keys.candidate_claim,
            encoded_value(
                snapshot.candidate_claim.as_ref(),
                encode_owner_admission_claim,
            )?,
            0,
        ),
    ];
    if let Some(previous_key) = keys.previous_claim.as_ref() {
        comparisons.push(expected_optional(
            previous_key,
            encoded_value(
                snapshot.previous_claim.as_ref(),
                encode_owner_admission_claim,
            )?,
            0,
        ));
    }
    Ok(comparisons)
}

fn put_unleased(key: &[u8], value: Vec<u8>) -> EtcdWriteOperation {
    EtcdWriteOperation::Put {
        key: key.to_vec(),
        value,
        lease_id: None,
    }
}

fn put_leased(
    key: &[u8],
    value: Vec<u8>,
    lease_id: u64,
) -> Result<EtcdWriteOperation, OwnerAdmissionInconsistencyCode> {
    Ok(EtcdWriteOperation::Put {
        key: key.to_vec(),
        value,
        lease_id: Some(
            lease_id_i64(lease_id)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        ),
    })
}

fn delete_key(key: &[u8]) -> EtcdWriteOperation {
    EtcdWriteOperation::Delete { key: key.to_vec() }
}

fn prepare_owner_admission_write_plan(
    keys: &OwnerAdmissionSnapshotKeys,
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<EtcdOwnerAdmissionWritePlan, OwnerAdmissionInconsistencyCode> {
    match mutation {
        OwnerAdmissionMutationPlan::Prepare(mutation) => {
            let snapshot = OwnerAdmissionExactSnapshot::new(
                Some(mutation.expected_shard.clone()),
                Some(mutation.expected_placement.clone()),
                Some(mutation.expected_authority.clone()),
                mutation.expected_session.clone(),
                None,
                mutation.expected_active_plan.clone(),
                mutation.expected_sentinel.clone(),
                None,
                mutation.expected_candidate_claim.clone(),
                mutation.expected_previous_claim.clone(),
            );
            Ok(EtcdOwnerAdmissionWritePlan {
                comparisons: owner_admission_snapshot_comparisons(keys, &snapshot)?,
                operations: vec![
                    put_unleased(
                        &keys.candidate_claim,
                        encode_owner_admission_claim(&mutation.next_claim).map_err(|_| {
                            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed
                        })?,
                    ),
                    put_unleased(
                        &keys.plan,
                        encode_planned_owner_admission(&mutation.next_plan).map_err(|_| {
                            OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed
                        })?,
                    ),
                    put_leased(
                        &keys.sentinel,
                        encode_owner_admission_plan_sentinel(&mutation.next_sentinel).map_err(
                            |_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
                        )?,
                        mutation.next_sentinel.lease_id(),
                    )?,
                ],
                else_keys: keys.clone(),
            })
        }
        OwnerAdmissionMutationPlan::Reject(mutation) => Ok(EtcdOwnerAdmissionWritePlan {
            comparisons: owner_admission_snapshot_comparisons(keys, &mutation.expected_snapshot)?,
            operations: vec![put_unleased(
                &keys.candidate_claim,
                encode_owner_admission_claim(&mutation.next_claim)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
            )],
            else_keys: keys.clone(),
        }),
        _ => Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed),
    }
}

fn commit_owner_admission_write_plan(
    keys: &OwnerAdmissionSnapshotKeys,
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<EtcdOwnerAdmissionWritePlan, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::Commit(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    let snapshot = OwnerAdmissionExactSnapshot::new(
        Some(mutation.expected_shard.clone()),
        Some(mutation.expected_placement.clone()),
        Some(mutation.expected_authority.clone()),
        mutation.expected_session.clone(),
        None,
        Some(mutation.expected_plan.clone()),
        Some(mutation.expected_sentinel.clone()),
        None,
        Some(mutation.expected_claim.clone()),
        mutation.expected_previous_claim.clone(),
    );
    Ok(EtcdOwnerAdmissionWritePlan {
        comparisons: owner_admission_snapshot_comparisons(keys, &snapshot)?,
        operations: vec![
            put_unleased(
                &keys.shard,
                encode_logical_shard_record(&mutation.next_shard)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
            ),
            put_leased(
                &keys.session,
                encode_owner_session(&mutation.next_session)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
                mutation.next_session.lease_id,
            )?,
            put_unleased(
                &keys.candidate_claim,
                encode_owner_admission_claim(&mutation.next_claim)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
            ),
            delete_key(&keys.plan),
            delete_key(&keys.sentinel),
        ],
        else_keys: keys.clone(),
    })
}

fn publish_owner_serving_write_plan(
    keys: &OwnerAdmissionSnapshotKeys,
    snapshot: &OwnerAdmissionExactSnapshot,
    publication: &PlannedOwnerServingPublicationV1,
) -> Result<EtcdOwnerAdmissionWritePlan, OwnerAdmissionInconsistencyCode> {
    Ok(EtcdOwnerAdmissionWritePlan {
        comparisons: owner_admission_snapshot_comparisons(keys, snapshot)?,
        operations: vec![put_unleased(
            &keys.shard,
            encode_logical_shard_record(publication.target())
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        )],
        else_keys: keys.clone(),
    })
}

fn abort_owner_admission_write_plan(
    keys: &OwnerAdmissionSnapshotKeys,
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<EtcdOwnerAdmissionWritePlan, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::Abort(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    let sentinel = match &mutation.expected_sentinel {
        OwnerAdmissionSentinelExpectation::Exact(sentinel) => expected_exact(
            &keys.sentinel,
            encode_owner_admission_plan_sentinel(sentinel)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
            lease_id_i64(sentinel.lease_id())
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        ),
        OwnerAdmissionSentinelExpectation::AuthoritativelyAbsent { .. } => {
            expected_absent(&keys.sentinel)
        }
    };
    Ok(EtcdOwnerAdmissionWritePlan {
        comparisons: vec![
            expected_exact(
                &keys.shard,
                encode_logical_shard_record(&mutation.expected_shard)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
                0,
            ),
            expected_optional(
                &keys.session,
                encoded_value(mutation.expected_session.as_ref(), encode_owner_session)?,
                mutation
                    .expected_session
                    .as_ref()
                    .map(|session| lease_id_i64(session.lease_id))
                    .transpose()
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?
                    .unwrap_or(0),
            ),
            expected_exact(
                &keys.candidate_claim,
                encode_owner_admission_claim(&mutation.expected_claim)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
                0,
            ),
            expected_exact(
                &keys.plan,
                encode_planned_owner_admission(&mutation.expected_plan)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
                0,
            ),
            sentinel,
        ],
        operations: vec![
            put_unleased(
                &keys.candidate_claim,
                encode_owner_admission_claim(&mutation.next_claim)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
            ),
            delete_key(&keys.plan),
            delete_key(&keys.sentinel),
        ],
        else_keys: keys.clone(),
    })
}

fn terminate_owner_admission_write_plan(
    keys: &OwnerAdmissionSnapshotKeys,
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<EtcdOwnerAdmissionWritePlan, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::Terminate(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    let session = match &mutation.expected_session {
        OwnerAdmissionSessionExpectation::Exact(session) => expected_exact(
            &keys.session,
            encode_owner_session(session)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
            lease_id_i64(session.lease_id)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        ),
        OwnerAdmissionSessionExpectation::AuthoritativelyAbsent { .. } => {
            expected_absent(&keys.session)
        }
    };
    let mut operations = vec![
        put_unleased(
            &keys.shard,
            encode_logical_shard_record(&mutation.next_shard)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        ),
        put_unleased(
            &keys.candidate_claim,
            encode_owner_admission_claim(&mutation.next_claim)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        ),
    ];
    if mutation.delete_session.is_some() {
        operations.push(delete_key(&keys.session));
    }
    Ok(EtcdOwnerAdmissionWritePlan {
        comparisons: vec![
            expected_exact(
                &keys.shard,
                encode_logical_shard_record(&mutation.expected_shard)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
                0,
            ),
            session,
            expected_exact(
                &keys.candidate_claim,
                encode_owner_admission_claim(&mutation.expected_claim)
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
                0,
            ),
            expected_optional(
                &keys.plan,
                encoded_value(
                    mutation.expected_active_plan.as_ref(),
                    encode_planned_owner_admission,
                )?,
                0,
            ),
            expected_optional(
                &keys.sentinel,
                encoded_value(
                    mutation.expected_sentinel.as_ref(),
                    encode_owner_admission_plan_sentinel,
                )?,
                mutation
                    .expected_sentinel
                    .as_ref()
                    .map(|sentinel| lease_id_i64(sentinel.lease_id()))
                    .transpose()
                    .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?
                    .unwrap_or(0),
            ),
        ],
        operations,
        else_keys: keys.clone(),
    })
}

fn seal_owner_admission_write_plan(
    keys: &OwnerAdmissionSnapshotKeys,
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<EtcdOwnerAdmissionWritePlan, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::SealRejected(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    let candidate = expected_optional(
        &keys.candidate_claim,
        encoded_value(
            mutation.expected_candidate_claim.as_ref(),
            encode_owner_admission_claim,
        )?,
        0,
    );
    Ok(EtcdOwnerAdmissionWritePlan {
        comparisons: vec![candidate],
        operations: vec![put_unleased(
            &keys.candidate_claim,
            encode_owner_admission_claim(&mutation.next_claim)
                .map_err(|_| OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed)?,
        )],
        else_keys: keys.clone(),
    })
}

fn prepare_result_from_classification(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    classification: OwnerAdmissionReconcileClassification,
) -> PrepareOwnerAdmissionResultV1 {
    match classification {
        OwnerAdmissionReconcileClassification::NotStarted => {
            PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            )
        }
        OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => {
            snapshot.candidate_claim.clone().map_or_else(
                || {
                    PrepareOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch,
                    )
                },
                |claim| PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim },
            )
        }
        OwnerAdmissionReconcileClassification::Prepared {
            claim,
            plan,
            sentinel,
        } => PrepareOwnerAdmissionResultV1::Prepared {
            plan: Box::new(plan),
            claim,
            sentinel,
        },
        OwnerAdmissionReconcileClassification::ExpiredPrepared {
            claim,
            plan,
            expected_sentinel,
        } => PrepareOwnerAdmissionResultV1::ExpiredPrepared {
            plan: Box::new(plan),
            claim,
            expected_sentinel,
        },
        OwnerAdmissionReconcileClassification::Rejected { claim, .. } => {
            PrepareOwnerAdmissionResultV1::Rejected { claim }
        }
        OwnerAdmissionReconcileClassification::Committed { claim, .. }
        | OwnerAdmissionReconcileClassification::ExpiredCommitted { claim, .. }
        | OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            match reconstruct_owner_admission_plan(intent, &claim) {
                Ok(plan) => PrepareOwnerAdmissionResultV1::DurableConflict {
                    plan: Box::new(plan),
                    claim,
                },
                Err(code) => PrepareOwnerAdmissionResultV1::DurableInconsistent(code),
            }
        }
    }
}

fn prepare_result_from_reconcile(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    decision: OwnerAdmissionReconcileDecision,
) -> Option<PrepareOwnerAdmissionResultV1> {
    match decision {
        OwnerAdmissionReconcileDecision::Classified(classification)
            if matches!(
                classification.as_ref(),
                OwnerAdmissionReconcileClassification::NotStarted
            ) =>
        {
            None
        }
        OwnerAdmissionReconcileDecision::Classified(classification) => Some(
            prepare_result_from_classification(intent, snapshot, *classification),
        ),
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            Some(PrepareOwnerAdmissionResultV1::DurableInconsistent(code))
        }
    }
}

fn prepare_before_grant(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> EtcdPreparePreGrant {
    prepare_result_from_reconcile(
        intent,
        snapshot,
        reconcile_owner_admission_intent(intent, snapshot),
    )
    .map_or_else(
        || EtcdPreparePreGrant::AllocateLease,
        |result| EtcdPreparePreGrant::Closed(Box::new(result)),
    )
}

fn successful_prepare_mutation_result(
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<PrepareOwnerAdmissionResultV1, OwnerAdmissionInconsistencyCode> {
    match mutation {
        OwnerAdmissionMutationPlan::Prepare(mutation) => {
            Ok(PrepareOwnerAdmissionResultV1::Prepared {
                plan: Box::new(mutation.next_plan.clone()),
                claim: mutation.next_claim.clone(),
                sentinel: mutation.next_sentinel.clone(),
            })
        }
        OwnerAdmissionMutationPlan::Reject(mutation) => {
            Ok(PrepareOwnerAdmissionResultV1::Rejected {
                claim: mutation.next_claim.clone(),
            })
        }
        _ => Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed),
    }
}

fn prepare_mutation_attaches_lease(mutation: &OwnerAdmissionMutationPlan) -> bool {
    matches!(mutation, OwnerAdmissionMutationPlan::Prepare(_))
}

fn classify_prepare_snapshot_state(
    intent: &OwnerAdmissionIntentV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<PrepareOwnerAdmissionResultV1> {
    match prepare_result_from_reconcile(
        intent,
        snapshot,
        reconcile_owner_admission_intent(intent, snapshot),
    ) {
        Some(result) => EtcdClosedResult::Proven(result),
        None if false_branch => {
            EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            ))
        }
        None => EtcdClosedResult::OutcomeUnknown,
    }
}

async fn classify_prepare_after_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    intent: &OwnerAdmissionIntentV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<PrepareOwnerAdmissionResultV1> {
    let target = ReconcileOwnerAdmissionTargetV1::IntentOnly(intent.clone());
    let snapshot =
        match enrich_owner_admission_snapshot(client, keys, &target, snapshot, None).await {
            Ok(snapshot) => snapshot,
            Err(EtcdOwnerAdmissionReadError::Io) if false_branch => {
                return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                    PrepareOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                ));
            }
            Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
            Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                return EtcdClosedResult::Proven(
                    PrepareOwnerAdmissionResultV1::DurableInconsistent(code),
                );
            }
        };
    classify_prepare_snapshot_state(intent, &snapshot.state, false_branch)
}

async fn execute_etcd_prepare_owner_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    intent: &OwnerAdmissionIntentV1,
) -> EtcdClosedResult<PrepareOwnerAdmissionResultV1> {
    if encode_owner_admission_intent(intent).is_err() {
        return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
            PrepareOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, intent);
    let target = ReconcileOwnerAdmissionTargetV1::IntentOnly(intent.clone());
    let initial = match read_enriched_owner_admission_snapshot(client, &keys, &target, None).await {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    match prepare_before_grant(intent, &initial.state) {
        EtcdPreparePreGrant::AllocateLease => {}
        EtcdPreparePreGrant::Closed(result) => return EtcdClosedResult::Proven(*result),
    }

    let lease_id = match grant_lease(client, options.lease_ttl_seconds()).await {
        Ok(lease_id) => lease_id,
        Err(_) => return EtcdClosedResult::OutcomeUnknown,
    };
    let lease = LogicalShardLease {
        logical_shard_id: intent.logical_shard_id(),
        owner: intent.owner().clone(),
        owner_epoch: intent.planned_epoch(),
        owner_incarnation_id: intent.owner_incarnation_id(),
        lease_id,
        authority: intent.admission().authority().fence(),
    };
    let plan = match PlannedOwnerAdmissionV1::new(intent.clone(), lease) {
        Ok(plan) => plan,
        Err(_) => {
            revoke_lease_best_effort(client, lease_id).await;
            return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::InvalidInputBeforeEffect,
            ));
        }
    };
    let prepared = OwnerAdmissionClaimV1::prepared(&plan);
    let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&plan);
    if prepared.is_err()
        || encode_planned_owner_admission(&plan).is_err()
        || encode_owner_admission_plan_sentinel(&sentinel).is_err()
    {
        revoke_lease_best_effort(client, lease_id).await;
        return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
            PrepareOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }

    let mutation = match plan_owner_admission_prepare(&plan, &initial.state) {
        OwnerAdmissionStateDecision::Mutation(mutation) => mutation,
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            revoke_lease_best_effort(client, lease_id).await;
            return EtcdClosedResult::Proven(prepare_result_from_classification(
                intent,
                &initial.state,
                *classification,
            ));
        }
        OwnerAdmissionStateDecision::Blocked(_) => {
            revoke_lease_best_effort(client, lease_id).await;
            return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            ));
        }
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            revoke_lease_best_effort(client, lease_id).await;
            return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let write = match prepare_owner_admission_write_plan(&keys, &mutation) {
        Ok(write) => write,
        Err(_) => {
            revoke_lease_best_effort(client, lease_id).await;
            return EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
            ));
        }
    };
    let attaches_lease = prepare_mutation_attaches_lease(&mutation);
    match client.txn(write.into_txn()).await {
        Ok(response) if response.succeeded() => {
            if !attaches_lease {
                revoke_lease_best_effort(client, lease_id).await;
            }
            match successful_prepare_mutation_result(&mutation) {
                Ok(result) => EtcdClosedResult::Proven(result),
                Err(code) => EtcdClosedResult::Proven(
                    PrepareOwnerAdmissionResultV1::DurableInconsistent(code),
                ),
            }
        }
        Ok(response) => {
            revoke_lease_best_effort(client, lease_id).await;
            let snapshot = match owner_admission_snapshot_from_txn(&keys, &response) {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return EtcdClosedResult::Proven(
                        PrepareOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_prepare_after_snapshot(client, &keys, intent, snapshot, true).await
        }
        Err(_) => {
            if !attaches_lease {
                revoke_lease_best_effort(client, lease_id).await;
            }
            let snapshot = match read_owner_admission_snapshot(client, &keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => {
                    return EtcdClosedResult::OutcomeUnknown;
                }
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        PrepareOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_prepare_after_snapshot(client, &keys, intent, snapshot, false).await
        }
    }
}

fn commit_result_from_classification(
    classification: OwnerAdmissionReconcileClassification,
) -> Option<EtcdCommitDisposition> {
    match classification {
        OwnerAdmissionReconcileClassification::Committed {
            claim,
            record,
            session,
        } => Some(EtcdCommitDisposition::Live {
            already_committed: true,
            shard: record,
            lease: session,
            claim,
        }),
        OwnerAdmissionReconcileClassification::ExpiredCommitted { claim, .. }
        | OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => Some(
            EtcdCommitDisposition::Closed(CommitOwnerAdmissionResultV1::DurableConflict { claim }),
        ),
        OwnerAdmissionReconcileClassification::Rejected { .. }
        | OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => Some(
            EtcdCommitDisposition::Closed(CommitOwnerAdmissionResultV1::NotDispatched(
                CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
            )),
        ),
        OwnerAdmissionReconcileClassification::NotStarted
        | OwnerAdmissionReconcileClassification::Prepared { .. }
        | OwnerAdmissionReconcileClassification::ExpiredPrepared { .. } => None,
    }
}

fn classify_commit_snapshot_state(
    plan: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<EtcdCommitDisposition> {
    match reconcile_owner_admission(plan, snapshot) {
        OwnerAdmissionReconcileDecision::Inconsistent(code) => EtcdClosedResult::Proven(
            EtcdCommitDisposition::Closed(CommitOwnerAdmissionResultV1::DurableInconsistent(code)),
        ),
        OwnerAdmissionReconcileDecision::Classified(classification) => {
            match commit_result_from_classification(*classification) {
                Some(result) => EtcdClosedResult::Proven(result),
                None if false_branch => EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(
                    CommitOwnerAdmissionResultV1::NotDispatched(
                        CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
                    ),
                )),
                None => EtcdClosedResult::OutcomeUnknown,
            }
        }
    }
}

async fn classify_commit_after_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    plan: &PlannedOwnerAdmissionV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<EtcdCommitDisposition> {
    let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
    let snapshot =
        match enrich_owner_admission_snapshot(client, keys, &target, snapshot, None).await {
            Ok(snapshot) => snapshot,
            Err(EtcdOwnerAdmissionReadError::Io) if false_branch => {
                return EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(
                    CommitOwnerAdmissionResultV1::NotDispatched(
                        CommitOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                    ),
                ));
            }
            Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
            Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                return EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(
                    CommitOwnerAdmissionResultV1::DurableInconsistent(code),
                ));
            }
        };
    classify_commit_snapshot_state(plan, &snapshot.state, false_branch)
}

fn successful_commit_mutation_result(
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<EtcdCommitDisposition, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::Commit(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    Ok(EtcdCommitDisposition::Live {
        already_committed: false,
        shard: mutation.next_shard.clone(),
        lease: mutation.next_session.clone(),
        claim: mutation.next_claim.clone(),
    })
}

async fn finalize_etcd_commit_disposition(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    plan: &PlannedOwnerAdmissionV1,
    claimed: &ClaimedCommitOwnerAdmissionCommandV1,
    disposition: EtcdClosedResult<EtcdCommitDisposition>,
) -> EtcdClosedResult<CommitOwnerAdmissionResultV1> {
    let EtcdClosedResult::Proven(disposition) = disposition else {
        return EtcdClosedResult::OutcomeUnknown;
    };
    let (already_committed, observed_shard, observed_lease, observed_claim) = match disposition {
        EtcdCommitDisposition::Live {
            already_committed,
            shard,
            lease,
            claim,
        } => (already_committed, shard, lease, claim),
        EtcdCommitDisposition::Closed(result) => return EtcdClosedResult::Proven(result),
    };
    let proof = match prove_exact_live_owner_session(client, keys, plan).await {
        Ok(proof) => proof,
        Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    if !live_proof_descends_from(&observed_shard, &observed_lease, &observed_claim, &proof) {
        return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        ));
    }
    let lifetime = match claimed.finite_lifetime_observation(
        &proof.shard,
        &proof.lease,
        proof.observed_ttl_seconds,
        proof.proof_digest,
    ) {
        Ok(lifetime) => lifetime,
        Err(_) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ));
        }
    };
    EtcdClosedResult::Proven(if already_committed {
        CommitOwnerAdmissionResultV1::AlreadyCommitted {
            shard: proof.shard,
            lease: proof.lease,
            claim: proof.claim,
            lifetime,
        }
    } else {
        CommitOwnerAdmissionResultV1::Committed {
            shard: proof.shard,
            lease: proof.lease,
            claim: proof.claim,
            lifetime,
        }
    })
}

async fn execute_etcd_commit_owner_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    plan: &PlannedOwnerAdmissionV1,
    claimed: &ClaimedCommitOwnerAdmissionCommandV1,
) -> EtcdClosedResult<CommitOwnerAdmissionResultV1> {
    if encode_planned_owner_admission(plan).is_err() {
        return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::NotDispatched(
            CommitOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, plan.intent());
    let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
    let initial = match read_enriched_owner_admission_snapshot(client, &keys, &target, None).await {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::NotDispatched(
                CommitOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let mutation = match plan_owner_admission_commit(plan, &initial.state) {
        OwnerAdmissionStateDecision::Mutation(mutation) => mutation,
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            let disposition = match commit_result_from_classification(*classification) {
                Some(result) => EtcdClosedResult::Proven(result),
                None => EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(
                    CommitOwnerAdmissionResultV1::NotDispatched(
                        CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
                    ),
                )),
            };
            return match disposition {
                EtcdClosedResult::Proven(EtcdCommitDisposition::Live { .. }) => {
                    finalize_etcd_commit_disposition(client, &keys, plan, claimed, disposition)
                        .await
                }
                EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(result)) => {
                    EtcdClosedResult::Proven(result)
                }
                EtcdClosedResult::OutcomeUnknown => EtcdClosedResult::OutcomeUnknown,
            };
        }
        OwnerAdmissionStateDecision::Blocked(_) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::NotDispatched(
                CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
            ));
        }
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let write = match commit_owner_admission_write_plan(&keys, &mutation) {
        Ok(write) => write,
        Err(_) => {
            return EtcdClosedResult::Proven(CommitOwnerAdmissionResultV1::NotDispatched(
                CommitOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
            ));
        }
    };
    let disposition = match client.txn(write.into_txn()).await {
        Ok(response) if response.succeeded() => {
            match successful_commit_mutation_result(&mutation) {
                Ok(result) => EtcdClosedResult::Proven(result),
                Err(code) => EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(
                    CommitOwnerAdmissionResultV1::DurableInconsistent(code),
                )),
            }
        }
        Ok(response) => {
            let snapshot = match owner_admission_snapshot_from_txn(&keys, &response) {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return EtcdClosedResult::Proven(
                        CommitOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_commit_after_snapshot(client, &keys, plan, snapshot, true).await
        }
        Err(_) => {
            let snapshot = match read_owner_admission_snapshot(client, &keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => {
                    return EtcdClosedResult::OutcomeUnknown;
                }
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        CommitOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_commit_after_snapshot(client, &keys, plan, snapshot, false).await
        }
    };
    finalize_etcd_commit_disposition(client, &keys, plan, claimed, disposition).await
}

fn abort_result_from_classification(
    classification: OwnerAdmissionReconcileClassification,
) -> Option<AbortOwnerAdmissionResultV1> {
    match classification {
        OwnerAdmissionReconcileClassification::Committed { claim, .. }
        | OwnerAdmissionReconcileClassification::ExpiredCommitted { claim, .. }
        | OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => {
            Some(AbortOwnerAdmissionResultV1::DurableConflict { claim })
        }
        OwnerAdmissionReconcileClassification::Rejected { .. }
        | OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => {
            Some(AbortOwnerAdmissionResultV1::NotDispatched(
                AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
            ))
        }
        OwnerAdmissionReconcileClassification::NotStarted
        | OwnerAdmissionReconcileClassification::Prepared { .. }
        | OwnerAdmissionReconcileClassification::ExpiredPrepared { .. } => None,
    }
}

fn classify_abort_snapshot_state(
    plan: &PlannedOwnerAdmissionV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<AbortOwnerAdmissionResultV1> {
    match reconcile_owner_admission(plan, snapshot) {
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::DurableInconsistent(code))
        }
        OwnerAdmissionReconcileDecision::Classified(classification) => {
            match abort_result_from_classification(*classification) {
                Some(result) => EtcdClosedResult::Proven(result),
                None if false_branch => {
                    EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                        AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
                    ))
                }
                None => EtcdClosedResult::OutcomeUnknown,
            }
        }
    }
}

async fn classify_abort_after_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    plan: &PlannedOwnerAdmissionV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<AbortOwnerAdmissionResultV1> {
    let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
    let snapshot =
        match enrich_owner_admission_snapshot(client, keys, &target, snapshot, None).await {
            Ok(snapshot) => snapshot,
            Err(EtcdOwnerAdmissionReadError::Io) if false_branch => {
                return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                    AbortOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
                ));
            }
            Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
            Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::DurableInconsistent(
                    code,
                ));
            }
        };
    classify_abort_snapshot_state(plan, &snapshot.state, false_branch)
}

fn successful_abort_mutation_result(
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<AbortOwnerAdmissionResultV1, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::Abort(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    Ok(AbortOwnerAdmissionResultV1::Aborted {
        claim: mutation.next_claim.clone(),
    })
}

async fn execute_etcd_abort_owner_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    plan: &PlannedOwnerAdmissionV1,
    reason: crate::OwnerAdmissionAbortReasonV1,
) -> EtcdClosedResult<AbortOwnerAdmissionResultV1> {
    if encode_planned_owner_admission(plan).is_err() {
        return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
            AbortOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, plan.intent());
    let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
    let initial = match read_enriched_owner_admission_snapshot(client, &keys, &target, None).await {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                AbortOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let mutation = match plan_owner_admission_abort(plan, &initial.state, reason) {
        OwnerAdmissionStateDecision::Mutation(mutation) => mutation,
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            return match abort_result_from_classification(*classification) {
                Some(result) => EtcdClosedResult::Proven(result),
                None => EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                    AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
                )),
            };
        }
        OwnerAdmissionStateDecision::Blocked(_) => {
            return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect,
            ));
        }
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let write = match abort_owner_admission_write_plan(&keys, &mutation) {
        Ok(write) => write,
        Err(_) => {
            return EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                AbortOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
            ));
        }
    };
    match client.txn(write.into_txn()).await {
        Ok(response) if response.succeeded() => match successful_abort_mutation_result(&mutation) {
            Ok(result) => EtcdClosedResult::Proven(result),
            Err(code) => {
                EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::DurableInconsistent(code))
            }
        },
        Ok(response) => {
            let snapshot = match owner_admission_snapshot_from_txn(&keys, &response) {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return EtcdClosedResult::Proven(
                        AbortOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_abort_after_snapshot(client, &keys, plan, snapshot, true).await
        }
        Err(_) => {
            let snapshot = match read_owner_admission_snapshot(client, &keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => {
                    return EtcdClosedResult::OutcomeUnknown;
                }
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        AbortOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_abort_after_snapshot(client, &keys, plan, snapshot, false).await
        }
    }
}

fn terminate_result_from_classification(
    plan: &PlannedOwnerAdmissionV1,
    requested_reason: &OwnerAdmissionTerminationReasonV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    classification: OwnerAdmissionReconcileClassification,
) -> Option<TerminateOwnerAdmissionResultV1> {
    match classification {
        OwnerAdmissionReconcileClassification::Terminated { claim, reason } => {
            if &reason != requested_reason {
                return Some(TerminateOwnerAdmissionResultV1::DurableConflict { claim });
            }
            let Some(record) = snapshot.logical_shard_record.as_ref() else {
                return Some(TerminateOwnerAdmissionResultV1::DurableInconsistent(
                    OwnerAdmissionInconsistencyCode::TerminatedRecordNotRecoveryDescendant,
                ));
            };
            if record
                .owner_epoch
                .is_some_and(|epoch| epoch.get() > plan.lease().owner_epoch.get())
            {
                if record.logical_shard_id != plan.intent().logical_shard_id()
                    || validate_logical_shard_record(record).is_err()
                {
                    return Some(TerminateOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::SupersedingRecordInvalid,
                    ));
                }
                return Some(TerminateOwnerAdmissionResultV1::Superseded {
                    shard: record.clone(),
                    claim,
                });
            }
            if let Err(code) = validate_terminated_record_descendant(plan, record) {
                return Some(TerminateOwnerAdmissionResultV1::DurableInconsistent(code));
            }
            Some(TerminateOwnerAdmissionResultV1::AlreadyTerminated {
                shard: record.clone(),
                claim,
            })
        }
        OwnerAdmissionReconcileClassification::Aborted { claim, .. } => {
            Some(TerminateOwnerAdmissionResultV1::DurableConflict { claim })
        }
        OwnerAdmissionReconcileClassification::NotStarted
        | OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed
        | OwnerAdmissionReconcileClassification::Prepared { .. }
        | OwnerAdmissionReconcileClassification::ExpiredPrepared { .. }
        | OwnerAdmissionReconcileClassification::Committed { .. }
        | OwnerAdmissionReconcileClassification::ExpiredCommitted { .. }
        | OwnerAdmissionReconcileClassification::Rejected { .. } => None,
    }
}

fn classify_terminate_snapshot_state(
    plan: &PlannedOwnerAdmissionV1,
    requested_reason: &OwnerAdmissionTerminationReasonV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<TerminateOwnerAdmissionResultV1> {
    match reconcile_owner_admission(plan, snapshot) {
        OwnerAdmissionReconcileDecision::Inconsistent(code) => {
            EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::DurableInconsistent(code))
        }
        OwnerAdmissionReconcileDecision::Classified(classification) => {
            match terminate_result_from_classification(
                plan,
                requested_reason,
                snapshot,
                *classification,
            ) {
                Some(result) => EtcdClosedResult::Proven(result),
                None if false_branch => {
                    EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                        TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
                    ))
                }
                None => EtcdClosedResult::OutcomeUnknown,
            }
        }
    }
}

fn termination_expiry_digest(
    reason: &OwnerAdmissionTerminationReasonV1,
) -> Option<OwnerLeaseExpiryEvidenceDigest> {
    match reason {
        OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest } => {
            Some(*evidence_digest)
        }
        OwnerAdmissionTerminationReasonV1::Released
        | OwnerAdmissionTerminationReasonV1::AuthorityCutover { .. } => None,
    }
}

async fn classify_terminate_after_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    plan: &PlannedOwnerAdmissionV1,
    requested_reason: &OwnerAdmissionTerminationReasonV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<TerminateOwnerAdmissionResultV1> {
    let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
    let snapshot = match enrich_owner_admission_snapshot(
        client,
        keys,
        &target,
        snapshot,
        termination_expiry_digest(requested_reason),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) if false_branch => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                TerminateOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    classify_terminate_snapshot_state(plan, requested_reason, &snapshot.state, false_branch)
}

fn successful_terminate_mutation_result(
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<TerminateOwnerAdmissionResultV1, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::Terminate(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    Ok(TerminateOwnerAdmissionResultV1::Terminated {
        shard: mutation.next_shard.clone(),
        claim: mutation.next_claim.clone(),
    })
}

async fn execute_etcd_terminate_owner_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    plan: &PlannedOwnerAdmissionV1,
    reason: &OwnerAdmissionTerminationReasonV1,
) -> EtcdClosedResult<TerminateOwnerAdmissionResultV1> {
    if encode_planned_owner_admission(plan).is_err() {
        return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
            TerminateOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, plan.intent());
    let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
    let initial = match read_enriched_owner_admission_snapshot(
        client,
        &keys,
        &target,
        termination_expiry_digest(reason),
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                TerminateOwnerAdmissionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let mutation = match plan_owner_admission_terminate(plan, &initial.state, reason.clone()) {
        OwnerAdmissionStateDecision::Mutation(mutation) => mutation,
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            return terminate_result_from_classification(
                plan,
                reason,
                &initial.state,
                *classification,
            )
            .map_or_else(
                || {
                    EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                        TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
                    ))
                },
                EtcdClosedResult::Proven,
            );
        }
        OwnerAdmissionStateDecision::Blocked(_) => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
            ));
        }
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let write = match terminate_owner_admission_write_plan(&keys, &mutation) {
        Ok(write) => write,
        Err(_) => {
            return EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                TerminateOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
            ));
        }
    };
    match client.txn(write.into_txn()).await {
        Ok(response) if response.succeeded() => {
            match successful_terminate_mutation_result(&mutation) {
                Ok(result) => EtcdClosedResult::Proven(result),
                Err(code) => EtcdClosedResult::Proven(
                    TerminateOwnerAdmissionResultV1::DurableInconsistent(code),
                ),
            }
        }
        Ok(response) => {
            let snapshot = match owner_admission_snapshot_from_txn(&keys, &response) {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return EtcdClosedResult::Proven(
                        TerminateOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_terminate_after_snapshot(client, &keys, plan, reason, snapshot, true).await
        }
        Err(_) => {
            let snapshot = match read_owner_admission_snapshot(client, &keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => {
                    return EtcdClosedResult::OutcomeUnknown;
                }
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        TerminateOwnerAdmissionResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_terminate_after_snapshot(client, &keys, plan, reason, snapshot, false).await
        }
    }
}

fn reconcile_target_intent(target: &ReconcileOwnerAdmissionTargetV1) -> &OwnerAdmissionIntentV1 {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => intent,
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => plan.intent(),
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => publication.plan().intent(),
    }
}

fn reconcile_target_encoding_is_valid(target: &ReconcileOwnerAdmissionTargetV1) -> bool {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            encode_owner_admission_intent(intent).is_ok()
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => {
            encode_planned_owner_admission(plan).is_ok()
        }
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            publication.validate().is_ok()
        }
    }
}

fn reconcile_decision_for_target(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
) -> EtcdReconcileStateDecision {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            EtcdReconcileStateDecision::Admission(reconcile_owner_admission_intent(
                intent, snapshot,
            ))
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => {
            EtcdReconcileStateDecision::Admission(reconcile_owner_admission(plan, snapshot))
        }
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            EtcdReconcileStateDecision::Serving(plan_owner_serving_publication(
                publication,
                snapshot,
            ))
        }
    }
}

fn reconcile_plan_from_claim(
    target: &ReconcileOwnerAdmissionTargetV1,
    claim: &OwnerAdmissionClaimV1,
) -> Result<PlannedOwnerAdmissionV1, OwnerAdmissionInconsistencyCode> {
    match target {
        ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) => {
            reconstruct_owner_admission_plan(intent, claim)
        }
        ReconcileOwnerAdmissionTargetV1::ExactPlan(plan) => Ok(plan.clone()),
        ReconcileOwnerAdmissionTargetV1::ExactServing(publication) => {
            Ok(publication.plan().clone())
        }
    }
}

fn reconcile_result_from_classification(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    classification: OwnerAdmissionReconcileClassification,
) -> Option<EtcdReconcileDisposition> {
    match classification {
        OwnerAdmissionReconcileClassification::NotStarted => None,
        OwnerAdmissionReconcileClassification::IncarnationAlreadyClaimed => Some(
            EtcdReconcileDisposition::Closed(snapshot.candidate_claim.clone().map_or_else(
                || {
                    ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch,
                    )
                },
                |claim| ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { claim },
            )),
        ),
        OwnerAdmissionReconcileClassification::Rejected { claim, .. } => {
            Some(EtcdReconcileDisposition::Closed(
                if matches!(target, ReconcileOwnerAdmissionTargetV1::IntentOnly(_)) {
                    ReconcileOwnerAdmissionResultV1::Rejected { claim }
                } else {
                    ReconcileOwnerAdmissionResultV1::NotDispatched(
                        ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
                    )
                },
            ))
        }
        OwnerAdmissionReconcileClassification::Prepared {
            claim,
            plan,
            sentinel,
        } => Some(EtcdReconcileDisposition::Closed(
            ReconcileOwnerAdmissionResultV1::Prepared {
                plan: Box::new(plan),
                claim,
                sentinel,
            },
        )),
        OwnerAdmissionReconcileClassification::ExpiredPrepared {
            claim,
            plan,
            expected_sentinel,
        } => Some(EtcdReconcileDisposition::Closed(
            ReconcileOwnerAdmissionResultV1::ExpiredPrepared {
                plan: Box::new(plan),
                claim,
                expected_sentinel,
            },
        )),
        OwnerAdmissionReconcileClassification::Committed {
            claim,
            record,
            session,
        } => Some(match reconcile_plan_from_claim(target, &claim) {
            Ok(plan) => EtcdReconcileDisposition::Live {
                plan,
                shard: record,
                lease: session,
                claim,
            },
            Err(code) => EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
            ),
        }),
        OwnerAdmissionReconcileClassification::ExpiredCommitted {
            claim,
            record,
            expected_session,
            evidence_digest,
        } => Some(EtcdReconcileDisposition::Closed(
            match reconcile_plan_from_claim(target, &claim) {
                Ok(plan) => ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                    plan: Box::new(plan),
                    shard: record,
                    claim: Box::new(claim),
                    expected_session,
                    evidence_digest,
                },
                Err(code) => ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
            },
        )),
        OwnerAdmissionReconcileClassification::Aborted { claim, .. }
        | OwnerAdmissionReconcileClassification::Terminated { claim, .. } => Some(
            EtcdReconcileDisposition::Closed(match reconcile_plan_from_claim(target, &claim) {
                Ok(plan) => ReconcileOwnerAdmissionResultV1::DurableConflict {
                    plan: Box::new(plan),
                    claim,
                },
                Err(code) => ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
            }),
        ),
    }
}

fn reconcile_result_from_decision(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    decision: EtcdReconcileStateDecision,
) -> Option<EtcdReconcileDisposition> {
    match decision {
        EtcdReconcileStateDecision::Admission(OwnerAdmissionReconcileDecision::Classified(
            classification,
        )) => reconcile_result_from_classification(target, snapshot, *classification),
        EtcdReconcileStateDecision::Admission(OwnerAdmissionReconcileDecision::Inconsistent(
            code,
        )) => Some(EtcdReconcileDisposition::Closed(
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
        )),
        EtcdReconcileStateDecision::Serving(decision) => {
            let ReconcileOwnerAdmissionTargetV1::ExactServing(publication) = target else {
                return Some(EtcdReconcileDisposition::Closed(
                    ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                        OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
                    ),
                ));
            };
            Some(match decision {
                OwnerServingPublicationStateDecision::Mutation(mutation) => {
                    EtcdReconcileDisposition::Live {
                        plan: publication.plan().clone(),
                        shard: mutation.expected_shard.clone(),
                        lease: mutation.expected_session.clone(),
                        claim: mutation.expected_claim.clone(),
                    }
                }
                OwnerServingPublicationStateDecision::AlreadyPublished {
                    record,
                    claim,
                    session,
                } => EtcdReconcileDisposition::Live {
                    plan: publication.plan().clone(),
                    shard: record,
                    lease: session,
                    claim,
                },
                OwnerServingPublicationStateDecision::ExpiredCommitted {
                    record,
                    claim,
                    expected_session,
                    evidence_digest,
                } => EtcdReconcileDisposition::Closed(
                    ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                        plan: Box::new(publication.plan().clone()),
                        shard: record,
                        claim: Box::new(claim),
                        expected_session,
                        evidence_digest,
                    },
                ),
                OwnerServingPublicationStateDecision::PublicationConflict { claim, .. }
                | OwnerServingPublicationStateDecision::Terminated { claim, .. }
                | OwnerServingPublicationStateDecision::Superseded { claim, .. }
                | OwnerServingPublicationStateDecision::DurableConflict { claim, .. } => {
                    EtcdReconcileDisposition::Closed(
                        ReconcileOwnerAdmissionResultV1::DurableConflict {
                            plan: Box::new(publication.plan().clone()),
                            claim,
                        },
                    )
                }
                OwnerServingPublicationStateDecision::Blocked(_) => {
                    EtcdReconcileDisposition::Closed(
                        ReconcileOwnerAdmissionResultV1::NotDispatched(
                            ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
                        ),
                    )
                }
                OwnerServingPublicationStateDecision::Inconsistent(code) => {
                    EtcdReconcileDisposition::Closed(
                        ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
                    )
                }
            })
        }
    }
}

fn successful_seal_rejected_mutation_result(
    mutation: &OwnerAdmissionMutationPlan,
) -> Result<ReconcileOwnerAdmissionResultV1, OwnerAdmissionInconsistencyCode> {
    let OwnerAdmissionMutationPlan::SealRejected(mutation) = mutation else {
        return Err(OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed);
    };
    Ok(ReconcileOwnerAdmissionResultV1::Rejected {
        claim: mutation.next_claim.clone(),
    })
}

fn classify_reconcile_after_seal_state(
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: &OwnerAdmissionExactSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<EtcdReconcileDisposition> {
    let decision = reconcile_decision_for_target(target, snapshot);
    match reconcile_result_from_decision(target, snapshot, decision) {
        Some(result) => EtcdClosedResult::Proven(result),
        None if false_branch => EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
            ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ),
        )),
        None => EtcdClosedResult::OutcomeUnknown,
    }
}

async fn classify_reconcile_after_seal_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    target: &ReconcileOwnerAdmissionTargetV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    false_branch: bool,
) -> EtcdClosedResult<EtcdReconcileDisposition> {
    let snapshot = match enrich_owner_admission_snapshot(client, keys, target, snapshot, None).await
    {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
            ));
        }
    };
    classify_reconcile_after_seal_state(target, &snapshot.state, false_branch)
}

async fn execute_etcd_owner_admission_intent_seal(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    target: &ReconcileOwnerAdmissionTargetV1,
    initial: &EtcdOwnerAdmissionSnapshot,
) -> EtcdClosedResult<EtcdReconcileDisposition> {
    let ReconcileOwnerAdmissionTargetV1::IntentOnly(intent) = target else {
        return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
            ReconcileOwnerAdmissionResultV1::NotDispatched(
                ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            ),
        ));
    };
    let mutation = match plan_owner_admission_intent_seal_rejected(intent, &initial.state) {
        OwnerAdmissionStateDecision::Mutation(mutation) => mutation,
        OwnerAdmissionStateDecision::Reconciled(classification) => {
            return reconcile_result_from_classification(target, &initial.state, *classification)
                .map_or_else(
                    || {
                        EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                            ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
                            ),
                        ))
                    },
                    EtcdClosedResult::Proven,
                );
        }
        OwnerAdmissionStateDecision::Blocked(_) => {
            return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::NotDispatched(
                    ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
                ),
            ));
        }
        OwnerAdmissionStateDecision::Inconsistent(code) => {
            return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
            ));
        }
    };
    let write = match seal_owner_admission_write_plan(keys, &mutation) {
        Ok(write) => write,
        Err(_) => {
            return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::NotDispatched(
                    ReconcileOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
                ),
            ));
        }
    };
    match client.txn(write.into_txn()).await {
        Ok(response) if response.succeeded() => {
            match successful_seal_rejected_mutation_result(&mutation) {
                Ok(result) => EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(result)),
                Err(code) => EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                    ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
                )),
            }
        }
        Ok(response) => {
            let snapshot = match owner_admission_snapshot_from_txn(keys, &response) {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                        ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
                    ));
                }
            };
            classify_reconcile_after_seal_snapshot(client, keys, target, snapshot, true).await
        }
        Err(_) => {
            let snapshot = match read_owner_admission_snapshot(client, keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => {
                    return EtcdClosedResult::OutcomeUnknown;
                }
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                        ReconcileOwnerAdmissionResultV1::DurableInconsistent(code),
                    ));
                }
            };
            classify_reconcile_after_seal_snapshot(client, keys, target, snapshot, false).await
        }
    }
}

async fn finalize_etcd_reconcile_disposition(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    claimed: &ClaimedReconcileOwnerAdmissionCommandV1,
    disposition: EtcdClosedResult<EtcdReconcileDisposition>,
) -> EtcdClosedResult<ReconcileOwnerAdmissionResultV1> {
    let EtcdClosedResult::Proven(disposition) = disposition else {
        return EtcdClosedResult::OutcomeUnknown;
    };
    let (plan, observed_shard, observed_lease, observed_claim) = match disposition {
        EtcdReconcileDisposition::Live {
            plan,
            shard,
            lease,
            claim,
        } => (plan, shard, lease, claim),
        EtcdReconcileDisposition::Closed(result) => return EtcdClosedResult::Proven(result),
    };
    let proof = match prove_exact_live_owner_session(client, keys, &plan).await {
        Ok(proof) => proof,
        Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    if !live_proof_descends_from(&observed_shard, &observed_lease, &observed_claim, &proof) {
        return EtcdClosedResult::Proven(ReconcileOwnerAdmissionResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        ));
    }
    let lifetime = match claimed.finite_lifetime_observation(
        &plan,
        &proof.shard,
        &proof.lease,
        proof.observed_ttl_seconds,
        proof.proof_digest,
    ) {
        Ok(lifetime) => lifetime,
        Err(_) => {
            return EtcdClosedResult::Proven(ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ));
        }
    };
    EtcdClosedResult::Proven(ReconcileOwnerAdmissionResultV1::Committed {
        plan: Box::new(plan),
        shard: proof.shard,
        lease: proof.lease,
        claim: Box::new(proof.claim),
        lifetime,
    })
}

async fn execute_etcd_reconcile_owner_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    target: &ReconcileOwnerAdmissionTargetV1,
    claimed: &ClaimedReconcileOwnerAdmissionCommandV1,
) -> EtcdClosedResult<ReconcileOwnerAdmissionResultV1> {
    if !reconcile_target_encoding_is_valid(target) {
        return EtcdClosedResult::Proven(ReconcileOwnerAdmissionResultV1::NotDispatched(
            ReconcileOwnerAdmissionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, reconcile_target_intent(target));
    let initial = match read_enriched_owner_admission_snapshot(client, &keys, target, None).await {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let decision = reconcile_decision_for_target(target, &initial.state);
    if matches!(target, ReconcileOwnerAdmissionTargetV1::IntentOnly(_))
        && matches!(
            &decision,
            EtcdReconcileStateDecision::Admission(
                OwnerAdmissionReconcileDecision::Classified(classification)
            )
                if matches!(
                    classification.as_ref(),
                    OwnerAdmissionReconcileClassification::NotStarted
                )
        )
    {
        let disposition =
            execute_etcd_owner_admission_intent_seal(client, &keys, target, &initial).await;
        return finalize_etcd_reconcile_disposition(client, &keys, claimed, disposition).await;
    }
    let disposition = match reconcile_result_from_decision(target, &initial.state, decision) {
        Some(result) => EtcdClosedResult::Proven(result),
        None => EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
            ReconcileOwnerAdmissionResultV1::NotDispatched(
                ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect,
            ),
        )),
    };
    finalize_etcd_reconcile_disposition(client, &keys, claimed, disposition).await
}

fn publish_disposition_from_decision(
    decision: OwnerServingPublicationStateDecision,
    definitely_before_effect: bool,
) -> EtcdClosedResult<EtcdPublishDisposition> {
    match decision {
        OwnerServingPublicationStateDecision::Mutation(_) => {
            if definitely_before_effect {
                EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                    PublishOwnerServingResultV1::NotDispatched(
                        PublishOwnerServingNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
                    ),
                ))
            } else {
                EtcdClosedResult::OutcomeUnknown
            }
        }
        OwnerServingPublicationStateDecision::AlreadyPublished {
            record,
            claim,
            session,
        } => EtcdClosedResult::Proven(EtcdPublishDisposition::Live {
            already_published: true,
            shard: record,
            lease: session,
            claim,
        }),
        OwnerServingPublicationStateDecision::PublicationConflict { record, claim, .. } => {
            EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                PublishOwnerServingResultV1::PublicationConflict {
                    shard: record,
                    claim,
                },
            ))
        }
        OwnerServingPublicationStateDecision::ExpiredCommitted {
            record,
            claim,
            expected_session,
            evidence_digest,
        } => EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
            PublishOwnerServingResultV1::ExpiredCommitted {
                shard: record,
                claim,
                expected_session,
                evidence_digest,
            },
        )),
        OwnerServingPublicationStateDecision::Terminated { record, claim } => {
            EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                PublishOwnerServingResultV1::Terminated {
                    shard: record,
                    claim,
                },
            ))
        }
        OwnerServingPublicationStateDecision::Superseded { record, claim } => {
            EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                PublishOwnerServingResultV1::Superseded {
                    shard: record,
                    claim,
                },
            ))
        }
        OwnerServingPublicationStateDecision::DurableConflict { record, claim } => {
            EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                PublishOwnerServingResultV1::DurableConflict {
                    shard: record,
                    claim,
                },
            ))
        }
        OwnerServingPublicationStateDecision::Blocked(_) if definitely_before_effect => {
            EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                PublishOwnerServingResultV1::NotDispatched(
                    PublishOwnerServingNotDispatchedV1::ExactOwnerBindingLostBeforeEffect,
                ),
            ))
        }
        OwnerServingPublicationStateDecision::Blocked(_) => EtcdClosedResult::OutcomeUnknown,
        OwnerServingPublicationStateDecision::Inconsistent(code) => EtcdClosedResult::Proven(
            EtcdPublishDisposition::Closed(PublishOwnerServingResultV1::DurableInconsistent(code)),
        ),
    }
}

async fn classify_publish_after_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    publication: &PlannedOwnerServingPublicationV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    definitely_before_effect: bool,
) -> EtcdClosedResult<EtcdPublishDisposition> {
    let target = ReconcileOwnerAdmissionTargetV1::ExactServing(publication.clone());
    let snapshot =
        match enrich_owner_admission_snapshot(client, keys, &target, snapshot, None).await {
            Ok(snapshot) => snapshot,
            Err(EtcdOwnerAdmissionReadError::Io) if definitely_before_effect => {
                return EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                    PublishOwnerServingResultV1::NotDispatched(
                        PublishOwnerServingNotDispatchedV1::BackendUnavailableBeforeEffect,
                    ),
                ));
            }
            Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
            Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                return EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                    PublishOwnerServingResultV1::DurableInconsistent(code),
                ));
            }
        };
    publish_disposition_from_decision(
        plan_owner_serving_publication(publication, &snapshot.state),
        definitely_before_effect,
    )
}

async fn finalize_etcd_publish_disposition(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    publication: &PlannedOwnerServingPublicationV1,
    claimed: &ClaimedPublishOwnerServingCommandV1,
    disposition: EtcdClosedResult<EtcdPublishDisposition>,
) -> EtcdClosedResult<PublishOwnerServingResultV1> {
    let EtcdClosedResult::Proven(disposition) = disposition else {
        return EtcdClosedResult::OutcomeUnknown;
    };
    let (already_published, observed_shard, observed_lease, observed_claim) = match disposition {
        EtcdPublishDisposition::Live {
            already_published,
            shard,
            lease,
            claim,
        } => (already_published, shard, lease, claim),
        EtcdPublishDisposition::Closed(result) => return EtcdClosedResult::Proven(result),
    };
    let proof = match prove_exact_live_owner_session(client, keys, publication.plan()).await {
        Ok(proof) => proof,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            let snapshot = match read_owner_admission_snapshot(client, keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        PublishOwnerServingResultV1::DurableInconsistent(code),
                    );
                }
            };
            let disposition =
                classify_publish_after_snapshot(client, keys, publication, snapshot, false).await;
            return match disposition {
                EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(result)) => {
                    EtcdClosedResult::Proven(result)
                }
                _ => EtcdClosedResult::OutcomeUnknown,
            };
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(PublishOwnerServingResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    if proof.lease != observed_lease || proof.claim != observed_claim {
        return EtcdClosedResult::Proven(PublishOwnerServingResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        ));
    }
    if proof.shard != *publication.target() {
        return if proof.shard.state == crate::LogicalShardState::Serving {
            EtcdClosedResult::Proven(PublishOwnerServingResultV1::PublicationConflict {
                shard: proof.shard,
                claim: proof.claim,
            })
        } else {
            EtcdClosedResult::OutcomeUnknown
        };
    }
    if !live_proof_descends_from(&observed_shard, &observed_lease, &observed_claim, &proof) {
        return EtcdClosedResult::Proven(PublishOwnerServingResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        ));
    }
    let lifetime = match claimed.finite_lifetime_observation(
        &proof.shard,
        &proof.claim,
        &proof.lease,
        proof.observed_ttl_seconds,
        proof.proof_digest,
    ) {
        Ok(lifetime) => lifetime,
        Err(_) => {
            return EtcdClosedResult::Proven(PublishOwnerServingResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ));
        }
    };
    EtcdClosedResult::Proven(if already_published {
        PublishOwnerServingResultV1::AlreadyPublished {
            shard: proof.shard,
            claim: proof.claim,
            lifetime,
        }
    } else {
        PublishOwnerServingResultV1::Published {
            shard: proof.shard,
            claim: proof.claim,
            lifetime,
        }
    })
}

async fn execute_etcd_publish_owner_serving(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    publication: &PlannedOwnerServingPublicationV1,
    claimed: &ClaimedPublishOwnerServingCommandV1,
) -> EtcdClosedResult<PublishOwnerServingResultV1> {
    if publication.validate().is_err() {
        return EtcdClosedResult::Proven(PublishOwnerServingResultV1::NotDispatched(
            PublishOwnerServingNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, publication.plan().intent());
    let target = ReconcileOwnerAdmissionTargetV1::ExactServing(publication.clone());
    let initial = match read_enriched_owner_admission_snapshot(client, &keys, &target, None).await {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            return EtcdClosedResult::Proven(PublishOwnerServingResultV1::NotDispatched(
                PublishOwnerServingNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(PublishOwnerServingResultV1::DurableInconsistent(
                code,
            ));
        }
    };
    let mutation = match plan_owner_serving_publication(publication, &initial.state) {
        OwnerServingPublicationStateDecision::Mutation(mutation) => mutation,
        decision => {
            let disposition = publish_disposition_from_decision(decision, true);
            return finalize_etcd_publish_disposition(
                client,
                &keys,
                publication,
                claimed,
                disposition,
            )
            .await;
        }
    };
    let write = match publish_owner_serving_write_plan(&keys, &initial.state, publication) {
        Ok(write) => write,
        Err(_) => {
            return EtcdClosedResult::Proven(PublishOwnerServingResultV1::NotDispatched(
                PublishOwnerServingNotDispatchedV1::CodecRejectedBeforeEffect,
            ));
        }
    };
    let disposition = match client.txn(write.into_txn()).await {
        Ok(response) if response.succeeded() => {
            EtcdClosedResult::Proven(EtcdPublishDisposition::Live {
                already_published: false,
                shard: mutation.next_shard.clone(),
                lease: mutation.expected_session.clone(),
                claim: mutation.expected_claim.clone(),
            })
        }
        Ok(response) => {
            let snapshot = match owner_admission_snapshot_from_txn(&keys, &response) {
                Ok(snapshot) => snapshot,
                Err(code) => {
                    return EtcdClosedResult::Proven(
                        PublishOwnerServingResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_publish_after_snapshot(client, &keys, publication, snapshot, true).await
        }
        Err(_) => {
            let snapshot = match read_owner_admission_snapshot(client, &keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => {
                    return EtcdClosedResult::OutcomeUnknown;
                }
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        PublishOwnerServingResultV1::DurableInconsistent(code),
                    );
                }
            };
            classify_publish_after_snapshot(client, &keys, publication, snapshot, false).await
        }
    };
    finalize_etcd_publish_disposition(client, &keys, publication, claimed, disposition).await
}

fn renew_result_from_decision(
    decision: OwnerSessionRenewalStateDecision,
    definitely_before_effect: bool,
) -> Option<RenewOwnerSessionResultV1> {
    match decision {
        OwnerSessionRenewalStateDecision::Current { .. } => None,
        OwnerSessionRenewalStateDecision::ExpiredCommitted {
            record,
            claim,
            expected_session,
            evidence_digest,
        } => Some(RenewOwnerSessionResultV1::ExpiredCommitted {
            shard: record,
            claim,
            expected_session,
            evidence_digest,
        }),
        OwnerSessionRenewalStateDecision::Terminated { record, claim } => {
            Some(RenewOwnerSessionResultV1::Terminated {
                shard: record,
                claim,
            })
        }
        OwnerSessionRenewalStateDecision::Superseded { record, claim } => {
            Some(RenewOwnerSessionResultV1::Superseded {
                shard: record,
                claim,
            })
        }
        OwnerSessionRenewalStateDecision::DurableConflict { record, claim } => {
            Some(RenewOwnerSessionResultV1::DurableConflict {
                shard: record,
                claim,
            })
        }
        OwnerSessionRenewalStateDecision::Blocked(_) if definitely_before_effect => {
            Some(RenewOwnerSessionResultV1::NotDispatched(
                RenewOwnerSessionNotDispatchedV1::ExactSessionBindingLostBeforeEffect,
            ))
        }
        OwnerSessionRenewalStateDecision::Blocked(_) => None,
        OwnerSessionRenewalStateDecision::Inconsistent(code) => {
            Some(RenewOwnerSessionResultV1::DurableInconsistent(code))
        }
    }
}

async fn classify_renew_after_snapshot(
    client: &mut Client,
    keys: &OwnerAdmissionSnapshotKeys,
    target: &OwnerSessionRenewalTargetV1,
    snapshot: EtcdOwnerAdmissionSnapshot,
    definitely_before_effect: bool,
) -> EtcdClosedResult<RenewOwnerSessionResultV1> {
    let reconcile_target = ReconcileOwnerAdmissionTargetV1::ExactPlan(target.plan().clone());
    let snapshot = match enrich_owner_admission_snapshot(
        client,
        keys,
        &reconcile_target,
        snapshot,
        None,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) if definitely_before_effect => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::NotDispatched(
                RenewOwnerSessionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::DurableInconsistent(code));
        }
    };
    match renew_result_from_decision(
        classify_owner_session_renewal(target, &snapshot.state),
        definitely_before_effect,
    ) {
        Some(result) => EtcdClosedResult::Proven(result),
        None => EtcdClosedResult::OutcomeUnknown,
    }
}

async fn execute_etcd_renew_owner_session(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    target: &OwnerSessionRenewalTargetV1,
    claimed: &ClaimedRenewOwnerSessionCommandV1,
) -> EtcdClosedResult<RenewOwnerSessionResultV1> {
    if target.validate().is_err() {
        return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::NotDispatched(
            RenewOwnerSessionNotDispatchedV1::CodecRejectedBeforeEffect,
        ));
    }
    let keys = OwnerAdmissionSnapshotKeys::new(options, target.plan().intent());
    let reconcile_target = ReconcileOwnerAdmissionTargetV1::ExactPlan(target.plan().clone());
    let initial = match read_enriched_owner_admission_snapshot(
        client,
        &keys,
        &reconcile_target,
        None,
    )
    .await
    {
        Ok(snapshot) => snapshot,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::NotDispatched(
                RenewOwnerSessionNotDispatchedV1::BackendUnavailableBeforeEffect,
            ));
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::DurableInconsistent(code));
        }
    };
    let (observed_shard, observed_claim, observed_session) =
        match classify_owner_session_renewal(target, &initial.state) {
            OwnerSessionRenewalStateDecision::Current {
                record,
                claim,
                session,
            } => (record, claim, session),
            decision => {
                return EtcdClosedResult::Proven(
                    renew_result_from_decision(decision, true).unwrap_or(
                        RenewOwnerSessionResultV1::NotDispatched(
                            RenewOwnerSessionNotDispatchedV1::ExactSessionBindingLostBeforeEffect,
                        ),
                    ),
                );
            }
        };
    let lease_id = match lease_id_i64(observed_session.lease_id) {
        Ok(lease_id) => lease_id,
        Err(_) => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ));
        }
    };

    // `lease_keep_alive` itself sends one request before it returns. From this
    // call onward an error is possibly post-dispatch, so it cannot be mapped
    // to `NotDispatched` and must never trigger a best-effort revoke.
    let _keepalive = client.lease_keep_alive(lease_id).await;
    let proof = match prove_exact_live_owner_session(client, &keys, target.plan()).await {
        Ok(proof) => proof,
        Err(EtcdOwnerAdmissionReadError::Io) => {
            let snapshot = match read_owner_admission_snapshot(client, &keys).await {
                Ok(snapshot) => snapshot,
                Err(EtcdOwnerAdmissionReadError::Io) => return EtcdClosedResult::OutcomeUnknown,
                Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
                    return EtcdClosedResult::Proven(
                        RenewOwnerSessionResultV1::DurableInconsistent(code),
                    );
                }
            };
            return classify_renew_after_snapshot(client, &keys, target, snapshot, false).await;
        }
        Err(EtcdOwnerAdmissionReadError::Inconsistent(code)) => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::DurableInconsistent(code));
        }
    };
    if !live_proof_descends_from(&observed_shard, &observed_session, &observed_claim, &proof) {
        return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::DurableInconsistent(
            OwnerAdmissionInconsistencyCode::CommittedClaimRecordSplit,
        ));
    }
    let lifetime = match claimed.finite_lifetime_observation(
        &proof.shard,
        &proof.claim,
        &proof.lease,
        proof.observed_ttl_seconds,
        proof.proof_digest,
    ) {
        Ok(lifetime) => lifetime,
        Err(_) => {
            return EtcdClosedResult::Proven(RenewOwnerSessionResultV1::DurableInconsistent(
                OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed,
            ));
        }
    };
    EtcdClosedResult::Proven(RenewOwnerSessionResultV1::Current {
        shard: proof.shard,
        claim: proof.claim,
        lifetime,
    })
}

impl ControlStore for EtcdControlStore {
    fn owner_lease_model(&self) -> OwnerLeaseModel {
        ETCD_OWNER_LEASE_MODEL
    }

    fn prepare_owner_admission(
        &self,
        command: PrepareOwnerAdmissionCommandV1,
    ) -> PrepareOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let intent = claimed.inspect().clone();
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async move {
            execute_etcd_prepare_owner_admission(&mut client, &options, &intent).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn commit_owner_admission(
        &self,
        command: CommitOwnerAdmissionCommandV1,
    ) -> CommitOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let plan = claimed.inspect().clone();
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async {
            execute_etcd_commit_owner_admission(&mut client, &options, &plan, &claimed).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn abort_owner_admission(
        &self,
        command: AbortOwnerAdmissionCommandV1,
    ) -> AbortOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let plan = inspection.plan.clone();
        let reason = inspection.reason;
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async move {
            execute_etcd_abort_owner_admission(&mut client, &options, &plan, reason).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn terminate_owner_admission(
        &self,
        command: TerminateOwnerAdmissionCommandV1,
    ) -> TerminateOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let plan = inspection.plan.clone();
        let reason = inspection.reason.clone();
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async move {
            execute_etcd_terminate_owner_admission(&mut client, &options, &plan, &reason).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn reconcile_owner_admission(
        &self,
        command: ReconcileOwnerAdmissionCommandV1,
    ) -> ReconcileOwnerAdmissionOutcomeV1 {
        let claimed = command.claim_execution();
        let target = claimed.inspect().clone();
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async {
            execute_etcd_reconcile_owner_admission(&mut client, &options, &target, &claimed).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn publish_owner_serving(
        &self,
        command: PublishOwnerServingCommandV1,
    ) -> PublishOwnerServingOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let publication = match PlannedOwnerServingPublicationV1::new(
            inspection.plan.clone(),
            inspection.source.clone(),
            inspection.publication.clone(),
        ) {
            Ok(publication) if publication.target() == inspection.target => publication,
            _ => {
                return claimed.complete(PublishOwnerServingResultV1::NotDispatched(
                    PublishOwnerServingNotDispatchedV1::InvalidPublicationBeforeEffect,
                ));
            }
        };
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async {
            execute_etcd_publish_owner_serving(&mut client, &options, &publication, &claimed).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn renew_owner_session(
        &self,
        command: RenewOwnerSessionCommandV1,
    ) -> RenewOwnerSessionOutcomeV1 {
        let claimed = command.claim_execution();
        let inspection = claimed.inspect();
        let target = match OwnerSessionRenewalTargetV1::new(
            inspection.plan.clone(),
            inspection.claim.clone(),
        ) {
            Ok(target) if target.session() == inspection.session => target,
            _ => {
                return claimed.complete(RenewOwnerSessionResultV1::NotDispatched(
                    RenewOwnerSessionNotDispatchedV1::InvalidTargetBeforeEffect,
                ));
            }
        };
        let mut client = self.client.clone();
        let options = self.options.clone();
        let result = self.block_on_closed(async {
            execute_etcd_renew_owner_session(&mut client, &options, &target, &claimed).await
        });
        match result {
            EtcdClosedResult::Proven(result) => claimed.complete(result),
            EtcdClosedResult::OutcomeUnknown => {
                let result = claimed.outcome_unknown();
                claimed.complete(result)
            }
        }
    }

    fn provision_fresh_root(
        &self,
        initial_placement: RootPlacement,
        initial_authority: MetadataAuthorityRecord,
    ) -> Result<FreshRootProvisioningOutcome, ControlError> {
        validate_fresh_root_provisioning_input(&initial_placement, &initial_authority)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let desired_shard = LogicalShardRecord::unassigned(initial_placement.logical_shard_id);
            let txn =
                FreshRootProvisioningTxn::new(&options, &initial_placement, &initial_authority)?
                    .into_txn();
            match client.txn(txn).await {
                Ok(response) if response.succeeded() => Ok(FreshRootProvisioningOutcome {
                    disposition: FreshRootProvisioningDisposition::Created,
                    logical_shard: desired_shard,
                    metadata_authority: initial_authority,
                    root_placement: initial_placement,
                }),
                Ok(_) => {
                    read_fresh_root_provisioning_replay(
                        &mut client,
                        &options,
                        &initial_placement,
                        &initial_authority,
                    )
                    .await
                }
                Err(error) => {
                    let backend_error = etcd_backend(error);
                    match read_fresh_root_provisioning_replay(
                        &mut client,
                        &options,
                        &initial_placement,
                        &initial_authority,
                    )
                    .await
                    {
                        Ok(outcome) => Ok(outcome),
                        Err(_) => Err(backend_error),
                    }
                }
            }
        })
    }

    fn create_root_placement(
        &self,
        placement: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        validate_new_root_placement(&placement)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            if fetch_logical_shard(&mut client, &options, &placement.logical_shard_id)
                .await?
                .is_none()
            {
                return Err(ControlError::LogicalShardNotFound(
                    placement.logical_shard_id,
                ));
            }

            let key = options.root_placement_key(&placement.root_id);
            let encoded = encode_root_placement(&placement)?;
            let txn = Txn::new()
                .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                .and_then(vec![TxnOp::put(key, encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(placement);
            }

            let current = fetch_root_placement(&mut client, &options, &placement.root_id)
                .await?
                .ok_or(ControlError::RootPlacementNotFound(placement.root_id))?;
            if current.placement == placement {
                return Ok(current.placement);
            }
            if current.placement.logical_shard_id != placement.logical_shard_id {
                return Err(ControlError::ImmutableShardAffinity {
                    root_id: placement.root_id,
                    existing: current.placement.logical_shard_id,
                    requested: placement.logical_shard_id,
                });
            }
            Err(ControlError::RootPlacementAlreadyExists(placement.root_id))
        })
    }

    fn get_root_placement(&self, root_id: &RootId) -> Result<Option<RootPlacement>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let root_id = *root_id;
        self.block_on(async move {
            Ok(fetch_root_placement(&mut client, &options, &root_id)
                .await?
                .map(|stored| stored.placement))
        })
    }

    fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let mut placements = fetch_root_placements(&mut client, &options)
                .await?
                .into_iter()
                .map(|stored| stored.placement)
                .collect::<Vec<_>>();
            placements.sort_by_key(|placement| placement.root_id);
            Ok(placements)
        })
    }

    fn compare_and_set_root_placement(
        &self,
        expected: &RootPlacement,
        next: RootPlacement,
    ) -> Result<RootPlacement, ControlError> {
        validate_root_placement_update(expected, &next)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        let expected = expected.clone();
        self.block_on(async move {
            let key = options.root_placement_key(&expected.root_id);
            let expected_encoded = encode_root_placement(&expected)?;
            let next_encoded = encode_root_placement(&next)?;
            let txn = Txn::new()
                .when(vec![Compare::value(
                    key.clone(),
                    CompareOp::Equal,
                    expected_encoded,
                )])
                .and_then(vec![TxnOp::put(key, next_encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(next);
            }

            let actual = fetch_root_placement(&mut client, &options, &expected.root_id)
                .await?
                .map(|stored| stored.placement);
            if actual.as_ref() == Some(&next) {
                return Ok(next);
            }
            Err(ControlError::RootPlacementCasConflict {
                expected: Box::new(expected),
                actual: actual.map(Box::new),
            })
        })
    }

    fn create_logical_shard(
        &self,
        logical_shard_id: LogicalShardId,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let desired = LogicalShardRecord::unassigned(logical_shard_id);
            let key = options.logical_shard_record_key(&logical_shard_id);
            let encoded = encode_logical_shard_record(&desired)?;
            let txn = Txn::new()
                .when(vec![Compare::version(key.clone(), CompareOp::Equal, 0)])
                .and_then(vec![TxnOp::put(key, encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(desired);
            }

            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            if current.record == desired {
                Ok(current.record)
            } else {
                Err(ControlError::LogicalShardAlreadyExists(logical_shard_id))
            }
        })
    }

    fn get_logical_shard(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<LogicalShardRecord>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let logical_shard_id = *logical_shard_id;
        self.block_on(async move {
            Ok(
                fetch_logical_shard(&mut client, &options, &logical_shard_id)
                    .await?
                    .map(|stored| stored.record),
            )
        })
    }

    fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let mut records = fetch_logical_shards(&mut client, &options)
                .await?
                .into_iter()
                .map(|stored| stored.record)
                .collect::<Vec<_>>();
            records.sort_by_key(|record| record.logical_shard_id);
            Ok(records)
        })
    }

    fn create_metadata_authority(
        &self,
        authority: MetadataAuthorityRecord,
    ) -> Result<MetadataAuthorityRecord, ControlError> {
        validate_new_metadata_authority(&authority)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        self.block_on(async move {
            let shard = fetch_logical_shard(&mut client, &options, &authority.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(
                    authority.logical_shard_id,
                ))?;
            if let Some(current) =
                fetch_metadata_authority(&mut client, &options, &authority.logical_shard_id).await?
            {
                if current.authority == authority {
                    return Ok(authority);
                }
                return Err(ControlError::MetadataAuthorityAlreadyExists(
                    authority.logical_shard_id,
                ));
            }
            validate_fresh_authority_shard(&shard.record)?;
            if fetch_owner_session(&mut client, &options, &authority.logical_shard_id)
                .await?
                .is_some()
            {
                return Err(ControlError::MetadataAuthorityAdoptionRejected {
                    logical_shard_id: authority.logical_shard_id,
                    reason: "an owner session already exists".to_owned(),
                });
            }
            let key = metadata_authority_key(&options, &authority.logical_shard_id);
            let encoded = encode_metadata_authority_record(&authority)?;
            let txn = Txn::new()
                .when(vec![
                    Compare::version(key.clone(), CompareOp::Equal, 0),
                    Compare::value(
                        options.logical_shard_record_key(&authority.logical_shard_id),
                        CompareOp::Equal,
                        shard.encoded,
                    ),
                    Compare::create_revision(
                        options.logical_shard_session_key(&authority.logical_shard_id),
                        CompareOp::Equal,
                        0,
                    ),
                ])
                .and_then(vec![TxnOp::put(key, encoded, None)]);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(authority);
            }

            let latest_shard =
                fetch_logical_shard(&mut client, &options, &authority.logical_shard_id)
                    .await?
                    .ok_or(ControlError::LogicalShardNotFound(
                        authority.logical_shard_id,
                    ))?;
            let current =
                fetch_metadata_authority(&mut client, &options, &authority.logical_shard_id)
                    .await?;
            if current.as_ref().map(|stored| &stored.authority) == Some(&authority) {
                return Ok(authority);
            }
            if current.is_some() {
                return Err(ControlError::MetadataAuthorityAlreadyExists(
                    authority.logical_shard_id,
                ));
            }
            validate_fresh_authority_shard(&latest_shard.record)?;
            if fetch_owner_session(&mut client, &options, &authority.logical_shard_id)
                .await?
                .is_some()
            {
                return Err(ControlError::MetadataAuthorityAdoptionRejected {
                    logical_shard_id: authority.logical_shard_id,
                    reason: "an owner session appeared during authority creation".to_owned(),
                });
            }
            Err(ControlError::Backend(
                "metadata authority create CAS failed without an observable competing update"
                    .to_owned(),
            ))
        })
    }

    fn get_metadata_authority(
        &self,
        logical_shard_id: &LogicalShardId,
    ) -> Result<Option<MetadataAuthorityRecord>, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let logical_shard_id = *logical_shard_id;
        self.block_on(async move {
            Ok(
                fetch_metadata_authority(&mut client, &options, &logical_shard_id)
                    .await?
                    .map(|stored| stored.authority),
            )
        })
    }

    fn compare_and_set_metadata_authority(
        &self,
        expected: &MetadataAuthorityRecord,
        next: MetadataAuthorityRecord,
    ) -> Result<MetadataAuthorityRecord, ControlError> {
        validate_metadata_authority_update(expected, &next)?;
        let mut client = self.client.clone();
        let options = self.options.clone();
        let expected = expected.clone();
        self.block_on(async move {
            let authority_key = metadata_authority_key(&options, &expected.logical_shard_id);
            let mut comparisons = vec![Compare::value(
                authority_key.clone(),
                CompareOp::Equal,
                encode_metadata_authority_record(&expected)?,
            )];
            let mut operations = Vec::new();
            if let Some(receipt) =
                metadata_authority_update_installs_source_receipt(&expected, &next)
            {
                if !metadata_authority_update_requires_unowned(&expected, &next) {
                    let shard =
                        fetch_logical_shard(&mut client, &options, &expected.logical_shard_id)
                            .await?
                            .ok_or(ControlError::LogicalShardNotFound(
                                expected.logical_shard_id,
                            ))?;
                    let session =
                        fetch_owner_session(&mut client, &options, &expected.logical_shard_id)
                            .await?;
                    validate_source_receipt_control_epoch(
                        &expected,
                        receipt,
                        &shard.record,
                        session.as_ref().map(|stored| &stored.lease),
                    )?;
                    comparisons.push(Compare::value(
                        options.logical_shard_record_key(&expected.logical_shard_id),
                        CompareOp::Equal,
                        shard.encoded,
                    ));
                    if let Some(session) = session {
                        let session_key =
                            options.logical_shard_session_key(&expected.logical_shard_id);
                        comparisons.extend([
                            Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
                            Compare::lease(
                                session_key,
                                CompareOp::Equal,
                                session.attached_lease_id,
                            ),
                        ]);
                    } else {
                        comparisons.push(Compare::create_revision(
                            options.logical_shard_session_key(&expected.logical_shard_id),
                            CompareOp::Equal,
                            0,
                        ));
                    }
                }
            }
            if metadata_authority_update_requires_unowned(&expected, &next) {
                let shard = fetch_logical_shard(&mut client, &options, &expected.logical_shard_id)
                    .await?
                    .ok_or(ControlError::LogicalShardNotFound(
                        expected.logical_shard_id,
                    ))?;
                let session =
                    fetch_owner_session(&mut client, &options, &expected.logical_shard_id).await?;
                if session.is_some() {
                    return Err(ControlError::MetadataAuthorityAdmission {
                        logical_shard_id: expected.logical_shard_id,
                        reason: "migration cutover requires the owner session key to be absent"
                            .to_owned(),
                    });
                }
                let cleaned = if metadata_authority_update_enters_ready(&expected, &next) {
                    let receipt = next
                        .migration
                        .as_ref()
                        .and_then(|migration| migration.source_quiesce_receipt.as_ref())
                        .expect("ReadyToCutover validation requires a source receipt");
                    validate_source_receipt_control_epoch(&expected, receipt, &shard.record, None)?;
                    prepare_expired_owner_cleanup(&shard.record)?
                } else {
                    ensure_unowned_for_cutover(&shard.record)?;
                    shard.record.clone()
                };
                comparisons.extend([
                    Compare::value(
                        options.logical_shard_record_key(&expected.logical_shard_id),
                        CompareOp::Equal,
                        shard.encoded.clone(),
                    ),
                    Compare::create_revision(
                        options.logical_shard_session_key(&expected.logical_shard_id),
                        CompareOp::Equal,
                        0,
                    ),
                ]);
                if cleaned != shard.record {
                    operations.push(TxnOp::put(
                        options.logical_shard_record_key(&expected.logical_shard_id),
                        encode_logical_shard_record(&cleaned)?,
                        None,
                    ));
                }
            }
            operations.push(TxnOp::put(
                authority_key,
                encode_metadata_authority_record(&next)?,
                None,
            ));
            let txn = Txn::new().when(comparisons).and_then(operations);
            if client.txn(txn).await.map_err(etcd_backend)?.succeeded() {
                return Ok(next);
            }

            let actual =
                fetch_metadata_authority(&mut client, &options, &expected.logical_shard_id)
                    .await?
                    .map(|stored| stored.authority);
            if actual.as_ref() == Some(&next) {
                if let Some(receipt) =
                    metadata_authority_update_installs_source_receipt(&expected, &next)
                {
                    let shard =
                        fetch_logical_shard(&mut client, &options, &expected.logical_shard_id)
                            .await?
                            .ok_or(ControlError::LogicalShardNotFound(
                                expected.logical_shard_id,
                            ))?;
                    let session =
                        fetch_owner_session(&mut client, &options, &expected.logical_shard_id)
                            .await?;
                    validate_source_receipt_control_epoch(
                        &expected,
                        receipt,
                        &shard.record,
                        session.as_ref().map(|stored| &stored.lease),
                    )?;
                }
                if metadata_authority_update_requires_unowned(&expected, &next) {
                    let shard =
                        fetch_logical_shard(&mut client, &options, &expected.logical_shard_id)
                            .await?
                            .ok_or(ControlError::LogicalShardNotFound(
                                expected.logical_shard_id,
                            ))?;
                    ensure_unowned_for_cutover(&shard.record)?;
                    if fetch_owner_session(&mut client, &options, &expected.logical_shard_id)
                        .await?
                        .is_some()
                    {
                        return Err(ControlError::MetadataAuthorityAdmission {
                            logical_shard_id: expected.logical_shard_id,
                            reason: "completed cutover transition still has an owner session"
                                .to_owned(),
                        });
                    }
                }
                return Ok(next);
            }
            if actual.as_ref() == Some(&expected)
                && metadata_authority_update_requires_unowned(&expected, &next)
            {
                let shard = fetch_logical_shard(&mut client, &options, &expected.logical_shard_id)
                    .await?
                    .ok_or(ControlError::LogicalShardNotFound(
                        expected.logical_shard_id,
                    ))?;
                if fetch_owner_session(&mut client, &options, &expected.logical_shard_id)
                    .await?
                    .is_some()
                {
                    return Err(ControlError::MetadataAuthorityAdmission {
                        logical_shard_id: expected.logical_shard_id,
                        reason: "migration cutover requires the owner session key to be absent"
                            .to_owned(),
                    });
                }
                if metadata_authority_update_enters_ready(&expected, &next) {
                    prepare_expired_owner_cleanup(&shard.record)?;
                } else {
                    ensure_unowned_for_cutover(&shard.record)?;
                }
                return Err(ControlError::Backend(
                    "metadata authority cutover CAS failed without an observable competing update"
                        .to_owned(),
                ));
            }
            Err(ControlError::MetadataAuthorityCasConflict {
                expected: Box::new(expected),
                actual: actual.map(Box::new),
            })
        })
    }

    fn acquire_owner(
        &self,
        admission: &OwnerServingAdmission,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let admission = admission.clone();
        self.block_on(async move {
            let logical_shard_id = admission.logical_shard_id();
            let (placement, authority) =
                fetch_serving_admission(&mut client, &options, &admission).await?;
            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            validate_authority_for_owner_operation(&authority.authority, None)?;
            if let (Some(current_owner), Some(owner_epoch)) =
                (current.record.owner.clone(), current.record.owner_epoch)
            {
                return Err(ControlError::LogicalShardAlreadyOwned {
                    logical_shard_id,
                    owner: current_owner,
                    owner_epoch,
                });
            }
            if current.record.owner_epoch.is_some() {
                return Err(ControlError::StaleOwnerEpoch {
                    logical_shard_id,
                    expected: None,
                    actual: current.record.owner_epoch,
                });
            }
            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let (next, lease) = match prepare_owner_acquisition(
                &current.record,
                None,
                owner,
                owner_incarnation_id,
                endpoint,
                lease_id,
                authority.authority.fence(),
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    revoke_lease_best_effort(&mut client, lease_id).await;
                    return Err(error);
                }
            };
            install_owner(
                &mut client,
                &options,
                PreparedOwnerInstallation {
                    current: &current,
                    placement: &placement,
                    authority: &authority,
                    next: &next,
                    lease: &lease,
                    expected_owner_epoch: None,
                },
            )
            .await
        })
    }

    fn acquire_successor(
        &self,
        admission: &OwnerServingAdmission,
        expected_owner_epoch: OwnerEpoch,
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
    ) -> Result<LogicalShardLease, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let admission = admission.clone();
        self.block_on(async move {
            let logical_shard_id = admission.logical_shard_id();
            let (placement, authority) =
                fetch_serving_admission(&mut client, &options, &admission).await?;
            let current = fetch_logical_shard(&mut client, &options, &logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(logical_shard_id))?;
            validate_authority_for_owner_operation(&authority.authority, None)?;
            if current.record.owner_epoch != Some(expected_owner_epoch) {
                return Err(ControlError::StaleOwnerEpoch {
                    logical_shard_id,
                    expected: Some(expected_owner_epoch),
                    actual: current.record.owner_epoch,
                });
            }
            let lease_id = grant_lease(&mut client, options.lease_ttl_seconds()).await?;
            let (next, lease) = match prepare_owner_acquisition(
                &current.record,
                Some(expected_owner_epoch),
                owner,
                owner_incarnation_id,
                endpoint,
                lease_id,
                authority.authority.fence(),
            ) {
                Ok(prepared) => prepared,
                Err(error) => {
                    revoke_lease_best_effort(&mut client, lease_id).await;
                    return Err(error);
                }
            };
            install_owner(
                &mut client,
                &options,
                PreparedOwnerInstallation {
                    current: &current,
                    placement: &placement,
                    authority: &authority,
                    next: &next,
                    lease: &lease,
                    expected_owner_epoch: Some(expected_owner_epoch),
                },
            )
            .await
        })
    }

    fn renew_owner(
        &self,
        lease: &LogicalShardLease,
        admission: &OwnerServingAdmission,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        let admission = admission.clone();
        self.block_on(async move {
            if let Err(error) =
                linearize_owner_admission(&mut client, &options, &lease, &admission).await
            {
                // Any failure to linearize the exact placement/authority
                // admission makes this route unsafe to keep alive. This also
                // covers an uncertain etcd read: retaining the lease would
                // let a stale route outlive the evidence that admitted it.
                revoke_lease_best_effort(&mut client, lease.lease_id).await;
                return Err(error);
            }

            let keepalive_result: Result<(), ControlError> = async {
                let lease_id = lease_id_i64(lease.lease_id)?;
                let (mut keeper, mut stream) = client
                    .lease_keep_alive(lease_id)
                    .await
                    .map_err(etcd_backend)?;
                keeper.keep_alive().await.map_err(etcd_backend)?;
                let response = stream
                    .message()
                    .await
                    .map_err(etcd_backend)?
                    .ok_or_else(|| {
                        ControlError::Backend(
                            "etcd lease keepalive stream ended before a response".to_owned(),
                        )
                    })?;
                if response.id() != lease_id || response.ttl() <= 0 {
                    return Err(ControlError::StaleLease(lease.clone()));
                }
                Ok(())
            }
            .await;
            if let Err(error) = keepalive_result {
                // The request may have extended the lease even when its
                // response was lost. Revoke conservatively so a failed
                // renewal cannot delay successor admission until the new TTL.
                revoke_lease_best_effort(&mut client, lease.lease_id).await;
                return Err(error);
            }
            match linearize_owner_admission(&mut client, &options, &lease, &admission).await {
                Ok(record) => Ok(record),
                Err(error) => {
                    revoke_lease_best_effort(&mut client, lease.lease_id).await;
                    Err(error)
                }
            }
        })
    }

    fn mark_serving(
        &self,
        lease: &LogicalShardLease,
        admission: &OwnerServingAdmission,
        publication: RecoveryPublication,
    ) -> Result<LogicalShardRecord, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        let admission = admission.clone();
        self.block_on(async move {
            let (placement, authority) =
                fetch_serving_admission(&mut client, &options, &admission).await?;
            validate_lease_serving_admission(&lease, &admission)?;
            let current = fetch_logical_shard(&mut client, &options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            validate_authority_for_owner_operation(&authority.authority, Some(&lease))?;
            let session = validate_owner_session(&mut client, &options, &lease).await?;
            let next = prepare_mark_serving(&current.record, &lease, publication)?;
            let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
            let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
            let authority_key = metadata_authority_key(&options, &lease.logical_shard_id);
            let placement_key = options.root_placement_key(&placement.placement.root_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::value(record_key.clone(), CompareOp::Equal, current.encoded),
                    Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
                    Compare::lease(session_key, CompareOp::Equal, lease_id_i64(lease.lease_id)?),
                    Compare::value(placement_key, CompareOp::Equal, placement.encoded.clone()),
                    Compare::value(authority_key, CompareOp::Equal, authority.encoded.clone()),
                ])
                .and_then(vec![TxnOp::put(
                    record_key,
                    encode_logical_shard_record(&next)?,
                    None,
                )]);
            match client.txn(txn).await {
                Ok(response) if response.succeeded() => Ok(next),
                Ok(_) | Err(_) => {
                    classify_mark_serving_failure(&mut client, &options, &lease, &admission, &next)
                        .await
                }
            }
        })
    }

    fn release_owner(
        &self,
        lease: &LogicalShardLease,
    ) -> Result<OwnerReleaseOutcome, ControlError> {
        let mut client = self.client.clone();
        let options = self.options.clone();
        let lease = lease.clone();
        self.block_on(async move {
            let current = fetch_logical_shard(&mut client, &options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            let session =
                fetch_owner_session(&mut client, &options, &lease.logical_shard_id).await?;
            if let Some(outcome) =
                classify_terminal_owner_release(&current, session.as_ref(), &lease)?
            {
                return Ok(outcome);
            }
            let session = session.expect("non-terminal release has an exact owner session");
            let next = prepare_owner_release(&current.record, &lease)?;
            let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
            let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
            let txn = Txn::new()
                .when(vec![
                    Compare::value(record_key.clone(), CompareOp::Equal, current.encoded),
                    Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
                    Compare::lease(
                        session_key.clone(),
                        CompareOp::Equal,
                        lease_id_i64(lease.lease_id)?,
                    ),
                ])
                .and_then(vec![
                    TxnOp::put(record_key, encode_logical_shard_record(&next)?, None),
                    TxnOp::delete(session_key, None),
                ]);
            match client.txn(txn).await {
                Ok(response) if response.succeeded() => {
                    revoke_lease_best_effort(&mut client, lease.lease_id).await;
                    Ok(OwnerReleaseOutcome::Released(next))
                }
                Ok(_) | Err(_) => {
                    let outcome = classify_release_failure(&mut client, &options, &lease, &next)
                        .await
                        .unwrap_or(OwnerReleaseOutcome::OutcomeUnknown);
                    if outcome.terminal_record().is_some() {
                        revoke_lease_best_effort(&mut client, lease.lease_id).await;
                    }
                    Ok(outcome)
                }
            }
        })
    }
}

async fn read_fresh_root_provisioning_replay(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    initial_placement: &RootPlacement,
    initial_authority: &MetadataAuthorityRecord,
) -> Result<FreshRootProvisioningOutcome, ControlError> {
    let shard = fetch_logical_shard(client, options, &initial_placement.logical_shard_id).await?;
    let authority =
        fetch_metadata_authority(client, options, &initial_placement.logical_shard_id).await?;
    let placement = fetch_root_placement(client, options, &initial_placement.root_id).await?;
    let session = fetch_owner_session(client, options, &initial_placement.logical_shard_id).await?;
    classify_fresh_root_provisioning_replay(
        initial_placement,
        initial_authority,
        shard.as_ref().map(|stored| &stored.record),
        authority.as_ref().map(|stored| &stored.authority),
        placement.as_ref().map(|stored| &stored.placement),
        session.as_ref().map(|stored| &stored.lease),
    )
}

async fn fetch_root_placement(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    root_id: &RootId,
) -> Result<Option<StoredRootPlacement>, ControlError> {
    let key = options.root_placement_key(root_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let placement = decode_root_placement(kv.value())?;
    if placement.root_id != *root_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "root placement key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_root_placement(&placement)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "root placement value is not canonical".to_owned(),
        ));
    }
    Ok(Some(StoredRootPlacement {
        placement,
        encoded: canonical,
    }))
}

async fn fetch_root_placements(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
) -> Result<Vec<StoredRootPlacement>, ControlError> {
    let response = client
        .get(
            options.root_placements_prefix(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(etcd_backend)?;
    let mut placements = Vec::with_capacity(response.kvs().len());
    for kv in response.kvs() {
        let placement = decode_root_placement(kv.value())?;
        if kv.key() != options.root_placement_key(&placement.root_id) {
            return Err(ControlError::Codec(
                "root placement key and value identities differ".to_owned(),
            ));
        }
        let canonical = encode_root_placement(&placement)?;
        if canonical != kv.value() {
            return Err(ControlError::Codec(
                "root placement value is not canonical".to_owned(),
            ));
        }
        placements.push(StoredRootPlacement {
            placement,
            encoded: canonical,
        });
    }
    placements.sort_by_key(|stored| stored.placement.root_id);
    Ok(placements)
}

async fn fetch_logical_shard(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Result<Option<StoredLogicalShard>, ControlError> {
    let key = options.logical_shard_record_key(logical_shard_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let record = decode_logical_shard_record(kv.value())?;
    if record.logical_shard_id != *logical_shard_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "logical shard key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_logical_shard_record(&record)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "logical shard value is not canonical".to_owned(),
        ));
    }
    Ok(Some(StoredLogicalShard {
        record,
        encoded: canonical,
    }))
}

async fn fetch_logical_shards(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
) -> Result<Vec<StoredLogicalShard>, ControlError> {
    let response = client
        .get(
            options.logical_shard_records_prefix(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .map_err(etcd_backend)?;
    let mut records = Vec::with_capacity(response.kvs().len());
    for kv in response.kvs() {
        let record = decode_logical_shard_record(kv.value())?;
        if kv.key() != options.logical_shard_record_key(&record.logical_shard_id) {
            return Err(ControlError::Codec(
                "logical shard key and value identities differ".to_owned(),
            ));
        }
        let canonical = encode_logical_shard_record(&record)?;
        if canonical != kv.value() {
            return Err(ControlError::Codec(
                "logical shard value is not canonical".to_owned(),
            ));
        }
        records.push(StoredLogicalShard {
            record,
            encoded: canonical,
        });
    }
    records.sort_by_key(|stored| stored.record.logical_shard_id);
    Ok(records)
}

async fn fetch_metadata_authority(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Result<Option<StoredMetadataAuthority>, ControlError> {
    let key = metadata_authority_key(options, logical_shard_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let authority = decode_metadata_authority_record(kv.value())?;
    if authority.logical_shard_id != *logical_shard_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "metadata authority key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_metadata_authority_record(&authority)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "metadata authority value is not canonical".to_owned(),
        ));
    }
    Ok(Some(StoredMetadataAuthority {
        authority,
        encoded: canonical,
    }))
}

fn metadata_authority_key(
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Vec<u8> {
    format!(
        "{}/metadata-authorities/{}",
        options.key_prefix().trim_end_matches('/'),
        encode_fixed_id(logical_shard_id.as_bytes())
    )
    .into_bytes()
}

async fn fetch_serving_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    admission: &OwnerServingAdmission,
) -> Result<(StoredRootPlacement, StoredMetadataAuthority), ControlError> {
    let placement = fetch_root_placement(client, options, &admission.placement().root_id)
        .await?
        .ok_or(ControlError::RootPlacementNotFound(
            admission.placement().root_id,
        ))?;
    if &placement.placement != admission.placement() {
        return Err(ControlError::RootPlacementCasConflict {
            expected: Box::new(admission.placement().clone()),
            actual: Some(Box::new(placement.placement)),
        });
    }
    let authority = fetch_metadata_authority(client, options, &admission.logical_shard_id())
        .await?
        .ok_or(ControlError::MetadataAuthorityNotFound(
            admission.logical_shard_id(),
        ))?;
    if &authority.authority != admission.authority() {
        return Err(ControlError::MetadataAuthorityCasConflict {
            expected: Box::new(admission.authority().clone()),
            actual: Some(Box::new(authority.authority)),
        });
    }
    Ok((placement, authority))
}

async fn fetch_owner_session(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
) -> Result<Option<StoredOwnerSession>, ControlError> {
    let key = options.logical_shard_session_key(logical_shard_id);
    let response = client.get(key.clone(), None).await.map_err(etcd_backend)?;
    let Some(kv) = response.kvs().first() else {
        return Ok(None);
    };
    let lease = decode_owner_session(kv.value())?;
    if lease.logical_shard_id != *logical_shard_id || kv.key() != key.as_slice() {
        return Err(ControlError::Codec(
            "owner session key and value identities differ".to_owned(),
        ));
    }
    let canonical = encode_owner_session(&lease)?;
    if canonical != kv.value() {
        return Err(ControlError::Codec(
            "owner session value is not canonical".to_owned(),
        ));
    }
    let attached_lease_id = kv.lease();
    if attached_lease_id == 0 || attached_lease_id != lease_id_i64(lease.lease_id)? {
        return Err(ControlError::Codec(
            "owner session value and attached lease differ".to_owned(),
        ));
    }
    Ok(Some(StoredOwnerSession {
        lease,
        encoded: canonical,
        attached_lease_id,
    }))
}

async fn validate_owner_session(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
) -> Result<StoredOwnerSession, ControlError> {
    let session = fetch_owner_session(client, options, &lease.logical_shard_id)
        .await?
        .ok_or_else(|| ControlError::StaleLease(lease.clone()))?;
    if session.lease != *lease || session.attached_lease_id != lease_id_i64(lease.lease_id)? {
        return Err(ControlError::StaleLease(lease.clone()));
    }
    Ok(session)
}

async fn linearize_owner_admission(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
    admission: &OwnerServingAdmission,
) -> Result<LogicalShardRecord, ControlError> {
    let (placement, authority) = fetch_serving_admission(client, options, admission).await?;
    validate_lease_serving_admission(lease, admission)?;
    let current = fetch_logical_shard(client, options, &lease.logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
    validate_record_lease(&current.record, lease)?;
    validate_authority_for_owner_operation(&authority.authority, Some(lease))?;
    let session = validate_owner_session(client, options, lease).await?;

    let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
    let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
    let authority_key = metadata_authority_key(options, &lease.logical_shard_id);
    let placement_key = options.root_placement_key(&placement.placement.root_id);
    let transaction = Txn::new()
        .when(vec![
            Compare::value(
                record_key.clone(),
                CompareOp::Equal,
                current.encoded.clone(),
            ),
            Compare::value(session_key.clone(), CompareOp::Equal, session.encoded),
            Compare::lease(session_key, CompareOp::Equal, lease_id_i64(lease.lease_id)?),
            Compare::value(placement_key, CompareOp::Equal, placement.encoded),
            Compare::value(authority_key, CompareOp::Equal, authority.encoded),
        ])
        .and_then(vec![TxnOp::get(record_key, None)]);
    match client.txn(transaction).await {
        Ok(response) if response.succeeded() => Ok(current.record),
        Ok(_) => {
            let (_, latest_authority) = fetch_serving_admission(client, options, admission).await?;
            validate_authority_for_owner_operation(&latest_authority.authority, Some(lease))?;
            let latest = fetch_logical_shard(client, options, &lease.logical_shard_id)
                .await?
                .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
            validate_record_lease(&latest.record, lease)?;
            validate_owner_session(client, options, lease).await?;
            Err(ControlError::Backend(
                "owner admission CAS failed without an observable competing update".to_owned(),
            ))
        }
        Err(error) => Err(etcd_backend(error)),
    }
}

async fn install_owner(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    installation: PreparedOwnerInstallation<'_>,
) -> Result<LogicalShardLease, ControlError> {
    let PreparedOwnerInstallation {
        current,
        placement,
        authority,
        next,
        lease,
        expected_owner_epoch,
    } = installation;
    let record_encoded = match encode_logical_shard_record(next) {
        Ok(encoded) => encoded,
        Err(error) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            return Err(error);
        }
    };
    let session_encoded = match encode_owner_session(lease) {
        Ok(encoded) => encoded,
        Err(error) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            return Err(error);
        }
    };
    let attached_lease_id = match lease_id_i64(lease.lease_id) {
        Ok(lease_id) => lease_id,
        Err(error) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            return Err(error);
        }
    };
    let record_key = options.logical_shard_record_key(&lease.logical_shard_id);
    let session_key = options.logical_shard_session_key(&lease.logical_shard_id);
    let txn = Txn::new()
        .when(vec![
            Compare::value(
                record_key.clone(),
                CompareOp::Equal,
                current.encoded.clone(),
            ),
            Compare::create_revision(session_key.clone(), CompareOp::Equal, 0),
            Compare::value(
                options.root_placement_key(&placement.placement.root_id),
                CompareOp::Equal,
                placement.encoded.clone(),
            ),
            Compare::value(
                metadata_authority_key(options, &lease.logical_shard_id),
                CompareOp::Equal,
                authority.encoded.clone(),
            ),
        ])
        .and_then(vec![
            TxnOp::put(record_key, record_encoded, None),
            TxnOp::put(
                session_key,
                session_encoded,
                Some(PutOptions::new().with_lease(attached_lease_id)),
            ),
        ]);

    match client.txn(txn).await {
        Ok(response) if response.succeeded() => Ok(lease.clone()),
        Ok(_) => {
            revoke_lease_best_effort(client, lease.lease_id).await;
            classify_acquire_failure(
                client,
                options,
                &lease.logical_shard_id,
                expected_owner_epoch,
                placement,
                authority,
            )
            .await
        }
        Err(error) => {
            let backend_error = etcd_backend(error);
            match owner_install_is_visible(client, options, placement, authority, next, lease).await
            {
                Ok(true) => Ok(lease.clone()),
                Ok(false) => {
                    revoke_lease_best_effort(client, lease.lease_id).await;
                    Err(backend_error)
                }
                // The commit outcome is unknown. Do not revoke a lease that may
                // guard a committed owner; without keepalive it expires safely.
                Err(_) => Err(backend_error),
            }
        }
    }
}

async fn owner_install_is_visible(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    expected_placement: &StoredRootPlacement,
    expected_authority: &StoredMetadataAuthority,
    expected_record: &LogicalShardRecord,
    expected_lease: &LogicalShardLease,
) -> Result<bool, ControlError> {
    let placement =
        fetch_root_placement(client, options, &expected_placement.placement.root_id).await?;
    if placement.as_ref().map(|stored| &stored.placement) != Some(&expected_placement.placement) {
        return Ok(false);
    }
    let record = fetch_logical_shard(client, options, &expected_lease.logical_shard_id).await?;
    if record.as_ref().map(|stored| &stored.record) != Some(expected_record) {
        return Ok(false);
    }
    let authority =
        fetch_metadata_authority(client, options, &expected_lease.logical_shard_id).await?;
    if authority.as_ref().map(|stored| &stored.authority) != Some(&expected_authority.authority) {
        return Ok(false);
    }
    let attached_lease_id = lease_id_i64(expected_lease.lease_id)?;
    let session = fetch_owner_session(client, options, &expected_lease.logical_shard_id).await?;
    Ok(session.is_some_and(|session| {
        session.lease == *expected_lease && session.attached_lease_id == attached_lease_id
    }))
}

async fn classify_acquire_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    logical_shard_id: &LogicalShardId,
    expected_owner_epoch: Option<OwnerEpoch>,
    placement: &StoredRootPlacement,
    expected_authority: &StoredMetadataAuthority,
) -> Result<LogicalShardLease, ControlError> {
    let latest = fetch_logical_shard(client, options, logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(*logical_shard_id))?;
    if latest.record.owner_epoch != expected_owner_epoch {
        if expected_owner_epoch.is_none() {
            if let (Some(owner), Some(owner_epoch)) =
                (latest.record.owner.clone(), latest.record.owner_epoch)
            {
                return Err(ControlError::LogicalShardAlreadyOwned {
                    logical_shard_id: *logical_shard_id,
                    owner,
                    owner_epoch,
                });
            }
        }
        return Err(ControlError::StaleOwnerEpoch {
            logical_shard_id: *logical_shard_id,
            expected: expected_owner_epoch,
            actual: latest.record.owner_epoch,
        });
    }
    if let Some(session) = fetch_owner_session(client, options, logical_shard_id).await? {
        return Err(ControlError::PreviousOwnerSessionLive {
            logical_shard_id: *logical_shard_id,
            owner_epoch: session.lease.owner_epoch,
        });
    }
    let actual_placement = fetch_root_placement(client, options, &placement.placement.root_id)
        .await?
        .map(|stored| stored.placement);
    if actual_placement.as_ref() != Some(&placement.placement) {
        return Err(ControlError::RootPlacementCasConflict {
            expected: Box::new(placement.placement.clone()),
            actual: actual_placement.map(Box::new),
        });
    }
    let actual_authority = fetch_metadata_authority(client, options, logical_shard_id)
        .await?
        .ok_or(ControlError::MetadataAuthorityNotFound(*logical_shard_id))?;
    if actual_authority.authority != expected_authority.authority {
        validate_authority_for_owner_operation(&actual_authority.authority, None)?;
        return Err(ControlError::MetadataAuthorityAdmission {
            logical_shard_id: *logical_shard_id,
            reason: "metadata authority changed during owner acquisition".to_owned(),
        });
    }
    Err(ControlError::Backend(
        "owner acquisition CAS failed without an observable competing update".to_owned(),
    ))
}

async fn classify_mark_serving_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
    admission: &OwnerServingAdmission,
    completed: &LogicalShardRecord,
) -> Result<LogicalShardRecord, ControlError> {
    let (_, authority) = fetch_serving_admission(client, options, admission).await?;
    validate_lease_serving_admission(lease, admission)?;
    validate_authority_for_owner_operation(&authority.authority, Some(lease))?;
    let latest = fetch_logical_shard(client, options, &lease.logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
    if latest.record == *completed {
        validate_owner_session(client, options, lease).await?;
        return Ok(completed.clone());
    }
    validate_record_lease(&latest.record, lease)?;
    validate_owner_session(client, options, lease).await?;
    Err(ControlError::Backend(
        "owner mutation CAS failed while the exact owner session remained current".to_owned(),
    ))
}

async fn classify_release_failure(
    client: &mut Client,
    options: &EtcdControlStoreOptions,
    lease: &LogicalShardLease,
    completed: &LogicalShardRecord,
) -> Result<OwnerReleaseOutcome, ControlError> {
    let latest = fetch_logical_shard(client, options, &lease.logical_shard_id)
        .await?
        .ok_or(ControlError::LogicalShardNotFound(lease.logical_shard_id))?;
    let session = fetch_owner_session(client, options, &lease.logical_shard_id).await?;
    if latest.record == *completed {
        if session.is_none() {
            return Ok(OwnerReleaseOutcome::Released(completed.clone()));
        }
        return Ok(OwnerReleaseOutcome::OutcomeUnknown);
    }
    Ok(
        classify_terminal_owner_release(&latest, session.as_ref(), lease)?
            .unwrap_or(OwnerReleaseOutcome::OutcomeUnknown),
    )
}

fn classify_terminal_owner_release(
    current: &StoredLogicalShard,
    session: Option<&StoredOwnerSession>,
    lease: &LogicalShardLease,
) -> Result<Option<OwnerReleaseOutcome>, ControlError> {
    let exact_record = current.record.owner.as_ref() == Some(&lease.owner)
        && current.record.owner_epoch == Some(lease.owner_epoch)
        && current.record.owner_incarnation_id == Some(lease.owner_incarnation_id)
        && current.record.lease_id == lease.lease_id;
    let exact_session = session.is_some_and(|stored| {
        stored.lease == *lease
            && u64::try_from(stored.attached_lease_id).ok() == Some(lease.lease_id)
    });

    if exact_record && exact_session {
        return Ok(None);
    }
    if exact_session {
        return Err(ControlError::InvalidRecord(
            "owner session remains exact after its shard record changed".to_owned(),
        ));
    }
    if current.record.owner.is_none()
        && current.record.owner_epoch == Some(lease.owner_epoch)
        && current.record.owner_incarnation_id == Some(lease.owner_incarnation_id)
        && current.record.lease_id == 0
        && session.is_none()
    {
        return Ok(Some(OwnerReleaseOutcome::AlreadyReleased(
            current.record.clone(),
        )));
    }
    if current.record.owner_epoch == Some(lease.owner_epoch)
        && current.record.owner_incarnation_id != Some(lease.owner_incarnation_id)
    {
        return Err(ControlError::InvalidRecord(
            "one owner epoch cannot identify different installed incarnations".to_owned(),
        ));
    }
    if exact_record
        || current
            .record
            .owner_epoch
            .is_some_and(|epoch| epoch.get() > lease.owner_epoch.get())
    {
        return Ok(Some(OwnerReleaseOutcome::Superseded(
            current.record.clone(),
        )));
    }
    Err(ControlError::StaleLease(lease.clone()))
}

async fn grant_lease(client: &mut Client, ttl_seconds: i64) -> Result<u64, ControlError> {
    let response = client
        .lease_grant(ttl_seconds, None)
        .await
        .map_err(etcd_backend)?;
    let lease_id = u64::try_from(response.id()).map_err(|_| {
        ControlError::Backend(format!("etcd returned negative lease id {}", response.id()))
    })?;
    if lease_id == 0 {
        return Err(ControlError::Backend(
            "etcd returned lease id zero".to_owned(),
        ));
    }
    Ok(lease_id)
}

async fn revoke_lease_best_effort(client: &mut Client, lease_id: u64) {
    if let Ok(lease_id) = lease_id_i64(lease_id) {
        let _ = client.lease_revoke(lease_id).await;
    }
}

fn lease_id_i64(lease_id: u64) -> Result<i64, ControlError> {
    if lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "lease id must be non-zero".to_owned(),
        ));
    }
    i64::try_from(lease_id)
        .map_err(|_| ControlError::Codec(format!("etcd lease id {lease_id} exceeds i64")))
}

fn etcd_backend(error: etcd_client::Error) -> ControlError {
    ControlError::Backend(format!("etcd: {error}"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use crate::owner_admission_state::expected_recovering_record_for_plan;
    use crate::{
        ConsistencyDomainId, LogicalShardState, MetadataAuthorityBinding,
        MetadataAuthorityGeneration, MetadataAuthorityId, MetadataAuthorityRevision,
        MetadataContractDigest, MetadataProviderProfileId, OperationId,
        OwnerAdmissionRejectionReasonV1, OwnerRuntimeReservationDigest, PlacementGeneration,
        RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
    };

    fn initial_placement() -> RootPlacement {
        RootPlacement {
            root_id: RootId::from_bytes([1; 16]),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: LogicalShardId::from_bytes([2; 16]),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        }
    }

    fn initial_authority() -> MetadataAuthorityRecord {
        MetadataAuthorityRecord {
            logical_shard_id: LogicalShardId::from_bytes([2; 16]),
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
            active: MetadataAuthorityBinding {
                authority_id: MetadataAuthorityId::from_bytes([3; 16]),
                provider_profile_id: MetadataProviderProfileId::new("holt-primary").unwrap(),
                profile_fingerprint: [4; 32],
                consistency_domain_id: ConsistencyDomainId::from_bytes([5; 16]),
                contract_digest: MetadataContractDigest::from_bytes([6; 32]),
            },
            migration: None,
        }
    }

    fn owner_admission_intent(endpoint: &str) -> OwnerAdmissionIntentV1 {
        let mut placement = initial_placement();
        placement.lifecycle = RootPlacementLifecycle::Active;
        placement.placement_generation = PlacementGeneration::new(2).unwrap();
        let admission = OwnerServingAdmission::stable(placement, initial_authority()).unwrap();
        OwnerAdmissionIntentV1::fresh(
            admission,
            LogicalShardRecord::unassigned(LogicalShardId::from_bytes([2; 16])),
            NodeId::new("node-a").unwrap(),
            OwnerIncarnationId::from_bytes([7; 16]),
            endpoint.to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([8; 32]).unwrap(),
        )
        .unwrap()
    }

    fn owner_admission_plan(endpoint: &str) -> PlannedOwnerAdmissionV1 {
        let intent = owner_admission_intent(endpoint);
        let lease = LogicalShardLease {
            logical_shard_id: intent.logical_shard_id(),
            owner: intent.owner().clone(),
            owner_epoch: intent.planned_epoch(),
            owner_incarnation_id: intent.owner_incarnation_id(),
            lease_id: 9,
            authority: intent.admission().authority().fence(),
        };
        PlannedOwnerAdmissionV1::new(intent, lease).unwrap()
    }

    fn owner_admission_not_started_snapshot(
        intent: &OwnerAdmissionIntentV1,
    ) -> OwnerAdmissionExactSnapshot {
        OwnerAdmissionExactSnapshot::new(
            Some(intent.expected_unowned_shard().clone()),
            Some(intent.admission().placement().clone()),
            Some(intent.admission().authority().clone()),
            None,
            None,
            None,
            None,
            None,
            None,
            intent.expected_previous_claim().cloned(),
        )
    }

    fn owner_admission_prepared_snapshot(
        plan: &PlannedOwnerAdmissionV1,
    ) -> OwnerAdmissionExactSnapshot {
        let mut snapshot = owner_admission_not_started_snapshot(plan.intent());
        snapshot.candidate_claim = Some(OwnerAdmissionClaimV1::prepared(plan).unwrap());
        snapshot.active_plan = Some(plan.clone());
        snapshot.sentinel = Some(OwnerAdmissionPlanSentinelV1::for_plan(plan));
        snapshot
    }

    fn owner_admission_committed_snapshot(
        plan: &PlannedOwnerAdmissionV1,
    ) -> OwnerAdmissionExactSnapshot {
        let mut snapshot = owner_admission_not_started_snapshot(plan.intent());
        snapshot.logical_shard_record = Some(expected_recovering_record_for_plan(plan));
        snapshot.session = Some(plan.lease().clone());
        snapshot.candidate_claim = Some(
            OwnerAdmissionClaimV1::prepared(plan)
                .unwrap()
                .commit()
                .unwrap(),
        );
        snapshot
    }

    fn owner_admission_aborted_snapshot(
        plan: &PlannedOwnerAdmissionV1,
        reason: crate::OwnerAdmissionAbortReasonV1,
    ) -> OwnerAdmissionExactSnapshot {
        let mut snapshot = owner_admission_not_started_snapshot(plan.intent());
        snapshot.candidate_claim = Some(
            OwnerAdmissionClaimV1::prepared(plan)
                .unwrap()
                .abort(reason)
                .unwrap(),
        );
        snapshot
    }

    fn owner_admission_expired_committed_snapshot(
        plan: &PlannedOwnerAdmissionV1,
        evidence_digest: OwnerLeaseExpiryEvidenceDigest,
    ) -> OwnerAdmissionExactSnapshot {
        let mut snapshot = owner_admission_committed_snapshot(plan);
        snapshot.session = None;
        snapshot.session_absence_evidence = Some(
            AuthoritativeSessionAbsenceEvidence::after_backend_check(plan, evidence_digest),
        );
        snapshot
    }

    fn owner_admission_terminated_snapshot(
        plan: &PlannedOwnerAdmissionV1,
        reason: OwnerAdmissionTerminationReasonV1,
    ) -> OwnerAdmissionExactSnapshot {
        let committed = owner_admission_committed_snapshot(plan);
        let OwnerAdmissionStateDecision::Mutation(OwnerAdmissionMutationPlan::Terminate(mutation)) =
            plan_owner_admission_terminate(plan, &committed, reason)
        else {
            panic!("expected terminate mutation")
        };
        let mut snapshot = owner_admission_not_started_snapshot(plan.intent());
        snapshot.logical_shard_record = Some(mutation.next_shard.clone());
        snapshot.candidate_claim = Some(mutation.next_claim.clone());
        snapshot
    }

    fn owner_admission_rejected_snapshot(
        intent: &OwnerAdmissionIntentV1,
        reason: OwnerAdmissionRejectionReasonV1,
    ) -> OwnerAdmissionExactSnapshot {
        let mut snapshot = owner_admission_not_started_snapshot(intent);
        snapshot.candidate_claim =
            Some(OwnerAdmissionClaimV1::rejected_from_absent(intent, reason).unwrap());
        snapshot
    }

    fn stored_entry(key: &[u8], value: Vec<u8>, attached_lease_id: i64) -> EtcdSnapshotEntry {
        EtcdSnapshotEntry {
            key: key.to_vec(),
            value,
            attached_lease_id,
        }
    }

    #[test]
    fn etcd_owner_lease_model_is_statically_finite() {
        assert_eq!(
            ETCD_OWNER_LEASE_MODEL,
            OwnerLeaseModel::FiniteAuthoritativeTtl
        );
    }

    #[test]
    fn metadata_authority_uses_a_distinct_stable_keyspace() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let logical_shard_id = LogicalShardId::from_bytes([2; 16]);

        assert_eq!(
            String::from_utf8(metadata_authority_key(&options, &logical_shard_id)).unwrap(),
            "/nokv/test/metadata-authorities/02020202020202020202020202020202"
        );
        assert_ne!(
            metadata_authority_key(&options, &logical_shard_id),
            options.logical_shard_record_key(&logical_shard_id)
        );
        assert_ne!(
            metadata_authority_key(&options, &logical_shard_id),
            options.logical_shard_session_key(&logical_shard_id)
        );
    }

    #[test]
    fn terminal_release_rejects_a_foreign_incarnation_at_the_same_owner_epoch() {
        let authority = initial_authority().fence();
        let installed = LogicalShardLease {
            logical_shard_id: LogicalShardId::from_bytes([2; 16]),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            owner_incarnation_id: OwnerIncarnationId::from_bytes([7; 16]),
            lease_id: 9,
            authority,
        };
        let record = LogicalShardRecord {
            logical_shard_id: installed.logical_shard_id,
            owner: Some(installed.owner.clone()),
            owner_epoch: Some(installed.owner_epoch),
            owner_incarnation_id: Some(installed.owner_incarnation_id),
            lease_id: installed.lease_id,
            state: LogicalShardState::Recovering,
            endpoint: Some("10.0.0.1:7000".to_owned()),
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        };
        let current = StoredLogicalShard {
            encoded: encode_logical_shard_record(&record).unwrap(),
            record,
        };
        let session = StoredOwnerSession {
            encoded: encode_owner_session(&installed).unwrap(),
            attached_lease_id: lease_id_i64(installed.lease_id).unwrap(),
            lease: installed.clone(),
        };
        let foreign = LogicalShardLease {
            owner_incarnation_id: OwnerIncarnationId::from_bytes([8; 16]),
            ..installed
        };

        assert!(matches!(
            classify_terminal_owner_release(&current, Some(&session), &foreign),
            Err(ControlError::InvalidRecord(reason))
                if reason.contains("different installed incarnations")
        ));
    }

    #[test]
    fn fresh_root_provisioning_txn_uses_four_absence_keys_and_three_canonical_values() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let placement = initial_placement();
        let authority = initial_authority();
        let plan = FreshRootProvisioningTxn::new(&options, &placement, &authority).unwrap();

        assert_eq!(
            BTreeSet::from([
                plan.shard_key.clone(),
                plan.authority_key.clone(),
                plan.placement_key.clone(),
                plan.session_key.clone(),
            ])
            .len(),
            4
        );
        assert_eq!(
            decode_logical_shard_record(&plan.shard_value).unwrap(),
            LogicalShardRecord::unassigned(placement.logical_shard_id)
        );
        assert_eq!(
            decode_metadata_authority_record(&plan.authority_value).unwrap(),
            authority
        );
        assert_eq!(
            decode_root_placement(&plan.placement_value).unwrap(),
            placement
        );
        assert_eq!(
            plan.placement_value,
            encode_root_placement(&placement).unwrap()
        );

        let _txn = plan.into_txn();
    }

    #[test]
    fn planned_prepare_binds_permanent_values_and_the_exact_sentinel_lease() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let snapshot = owner_admission_not_started_snapshot(plan.intent());
        let OwnerAdmissionStateDecision::Mutation(mutation) =
            plan_owner_admission_prepare(&plan, &snapshot)
        else {
            panic!("expected prepare mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let write = prepare_owner_admission_write_plan(&keys, &mutation).unwrap();

        for key in [&keys.shard, &keys.placement, &keys.authority] {
            assert!(write.comparisons.iter().any(|comparison| {
                comparison.key == *key
                    && matches!(
                        comparison.expected,
                        EtcdExpectedValue::Exact {
                            attached_lease_id: 0,
                            ..
                        }
                    )
            }));
        }
        for key in [&keys.candidate_claim, &keys.plan, &keys.sentinel] {
            assert!(write.comparisons.iter().any(|comparison| {
                comparison.key == *key && matches!(comparison.expected, EtcdExpectedValue::Absent)
            }));
        }
        assert!(write.operations.iter().any(|operation| matches!(
            operation,
            EtcdWriteOperation::Put {
                key,
                lease_id: None,
                ..
            } if key == &keys.candidate_claim
        )));
        assert!(write.operations.iter().any(|operation| matches!(
            operation,
            EtcdWriteOperation::Put {
                key,
                lease_id: None,
                ..
            } if key == &keys.plan
        )));
        assert!(write.operations.iter().any(|operation| matches!(
            operation,
            EtcdWriteOperation::Put {
                key,
                lease_id: Some(9),
                ..
            } if key == &keys.sentinel
        )));
        assert_eq!(write.else_keys.ordered(), keys.ordered());
    }

    #[test]
    fn prepare_false_and_uncertain_error_keep_distinct_closed_results() {
        let intent = owner_admission_intent("10.0.0.1:7000");
        let snapshot = owner_admission_not_started_snapshot(&intent);
        assert!(matches!(
            classify_prepare_snapshot_state(&intent, &snapshot, true),
            EtcdClosedResult::Proven(PrepareOwnerAdmissionResultV1::NotDispatched(
                PrepareOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect
            ))
        ));
        assert!(matches!(
            classify_prepare_snapshot_state(&intent, &snapshot, false),
            EtcdClosedResult::OutcomeUnknown
        ));

        let foreign_plan = owner_admission_plan("10.0.0.2:7000");
        let mut foreign_snapshot = snapshot;
        foreign_snapshot.candidate_claim =
            Some(OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap());
        assert!(matches!(
            classify_prepare_snapshot_state(&intent, &foreign_snapshot, true),
            EtcdClosedResult::Proven(
                PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
            )
        ));
    }

    #[test]
    fn exact_prepare_replay_and_foreign_claim_close_before_lease_allocation() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let mut replay = owner_admission_not_started_snapshot(plan.intent());
        replay.candidate_claim = Some(OwnerAdmissionClaimV1::prepared(&plan).unwrap());
        replay.active_plan = Some(plan.clone());
        replay.sentinel = Some(OwnerAdmissionPlanSentinelV1::for_plan(&plan));
        assert!(matches!(
            prepare_before_grant(plan.intent(), &replay),
            EtcdPreparePreGrant::Closed(result)
                if matches!(result.as_ref(), PrepareOwnerAdmissionResultV1::Prepared { .. })
        ));

        let intent = owner_admission_intent("10.0.0.1:7000");
        let foreign_plan = owner_admission_plan("10.0.0.2:7000");
        let mut foreign = owner_admission_not_started_snapshot(&intent);
        foreign.candidate_claim = Some(OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap());
        assert!(matches!(
            prepare_before_grant(&intent, &foreign),
            EtcdPreparePreGrant::Closed(result)
                if matches!(
                    result.as_ref(),
                    PrepareOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
                )
        ));

        assert!(matches!(
            prepare_before_grant(&intent, &owner_admission_not_started_snapshot(&intent)),
            EtcdPreparePreGrant::AllocateLease
        ));
    }

    #[test]
    fn intent_ambiguity_seal_is_an_absence_cas_with_a_permanent_rejection() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let intent = owner_admission_intent("10.0.0.1:7000");
        let snapshot = owner_admission_not_started_snapshot(&intent);
        let OwnerAdmissionStateDecision::Mutation(mutation) =
            plan_owner_admission_intent_seal_rejected(&intent, &snapshot)
        else {
            panic!("expected seal mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, &intent);
        let write = seal_owner_admission_write_plan(&keys, &mutation).unwrap();
        assert_eq!(write.comparisons.len(), 1);
        assert_eq!(write.comparisons[0].key, keys.candidate_claim);
        assert!(matches!(
            write.comparisons[0].expected,
            EtcdExpectedValue::Absent
        ));
        let EtcdWriteOperation::Put {
            key,
            value,
            lease_id,
        } = &write.operations[0]
        else {
            panic!("expected permanent rejection put")
        };
        assert_eq!(key, &keys.candidate_claim);
        assert_eq!(*lease_id, None);
        let claim = decode_owner_admission_claim(value).unwrap();
        assert!(matches!(
            claim.phase(),
            OwnerAdmissionClaimPhaseV1::Rejected {
                reason: OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed
            }
        ));
    }

    #[test]
    fn commit_txn_binds_the_prepared_triple_and_installs_one_leased_session() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let snapshot = owner_admission_prepared_snapshot(&plan);
        let OwnerAdmissionStateDecision::Mutation(mutation) =
            plan_owner_admission_commit(&plan, &snapshot)
        else {
            panic!("expected commit mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let write = commit_owner_admission_write_plan(&keys, &mutation).unwrap();

        for (key, lease_id) in [
            (&keys.candidate_claim, 0),
            (&keys.plan, 0),
            (&keys.sentinel, 9),
        ] {
            assert!(write.comparisons.iter().any(|comparison| {
                comparison.key == *key
                    && matches!(
                        comparison.expected,
                        EtcdExpectedValue::Exact {
                            attached_lease_id,
                            ..
                        } if attached_lease_id == lease_id
                    )
            }));
        }
        assert!(write.comparisons.iter().any(|comparison| {
            comparison.key == keys.session
                && matches!(comparison.expected, EtcdExpectedValue::Absent)
        }));
        assert!(write.operations.iter().any(|operation| matches!(
            operation,
            EtcdWriteOperation::Put {
                key,
                lease_id: Some(9),
                ..
            } if key == &keys.session
        )));
        for key in [&keys.plan, &keys.sentinel] {
            assert!(write.operations.iter().any(|operation| matches!(
                operation,
                EtcdWriteOperation::Delete { key: deleted } if deleted == key
            )));
        }
    }

    #[test]
    fn serving_publication_txn_binds_the_full_committed_snapshot() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let source = expected_recovering_record_for_plan(&plan);
        let publication = PlannedOwnerServingPublicationV1::new(
            plan.clone(),
            source,
            RecoveryPublication {
                checkpoint: None,
                log: None,
                durable_lsn: 0,
            },
        )
        .unwrap();
        let snapshot = owner_admission_committed_snapshot(&plan);
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let write = publish_owner_serving_write_plan(&keys, &snapshot, &publication).unwrap();

        assert_eq!(write.comparisons.len(), keys.ordered().len());
        for key in keys.ordered() {
            assert!(write
                .comparisons
                .iter()
                .any(|comparison| comparison.key == key));
        }
        assert!(write.comparisons.iter().any(|comparison| {
            comparison.key == keys.session
                && matches!(
                    comparison.expected,
                    EtcdExpectedValue::Exact {
                        attached_lease_id: 9,
                        ..
                    }
                )
        }));
        assert_eq!(write.operations.len(), 1);
        assert!(matches!(
            &write.operations[0],
            EtcdWriteOperation::Put {
                key,
                value,
                lease_id: None,
            } if key == &keys.shard
                && decode_logical_shard_record(value)
                    .is_ok_and(|decoded| decoded == *publication.target())
        ));
    }

    #[test]
    fn serving_publication_and_renewal_keep_ambiguity_closed() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let publication = PlannedOwnerServingPublicationV1::new(
            plan.clone(),
            expected_recovering_record_for_plan(&plan),
            RecoveryPublication {
                checkpoint: None,
                log: None,
                durable_lsn: 0,
            },
        )
        .unwrap();
        let source_decision = plan_owner_serving_publication(
            &publication,
            &owner_admission_committed_snapshot(&plan),
        );
        assert!(matches!(
            publish_disposition_from_decision(source_decision.clone(), true),
            EtcdClosedResult::Proven(EtcdPublishDisposition::Closed(
                PublishOwnerServingResultV1::NotDispatched(
                    PublishOwnerServingNotDispatchedV1::ExactOwnerBindingLostBeforeEffect
                )
            ))
        ));
        assert!(matches!(
            publish_disposition_from_decision(source_decision, false),
            EtcdClosedResult::OutcomeUnknown
        ));

        let mut published = owner_admission_committed_snapshot(&plan);
        published.logical_shard_record = Some(publication.target().clone());
        assert!(matches!(
            publish_disposition_from_decision(
                plan_owner_serving_publication(&publication, &published),
                false,
            ),
            EtcdClosedResult::Proven(EtcdPublishDisposition::Live {
                already_published: true,
                ..
            })
        ));

        let renewal = OwnerSessionRenewalTargetV1::new(
            plan.clone(),
            OwnerAdmissionClaimV1::prepared(&plan)
                .unwrap()
                .commit()
                .unwrap(),
        )
        .unwrap();
        let expiry = OwnerLeaseExpiryEvidenceDigest::from_bytes([19; 32]).unwrap();
        assert!(matches!(
            renew_result_from_decision(
                classify_owner_session_renewal(
                    &renewal,
                    &owner_admission_expired_committed_snapshot(&plan, expiry),
                ),
                false,
            ),
            Some(RenewOwnerSessionResultV1::ExpiredCommitted {
                evidence_digest,
                ..
            }) if evidence_digest == expiry
        ));
        assert!(matches!(
            renew_result_from_decision(
                classify_owner_session_renewal(
                    &renewal,
                    &owner_admission_committed_snapshot(&plan),
                ),
                false,
            ),
            None
        ));
    }

    #[test]
    fn commit_response_loss_requires_terminal_reread_before_closing() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let prepared = owner_admission_prepared_snapshot(&plan);
        assert!(matches!(
            classify_commit_snapshot_state(&plan, &prepared, true),
            EtcdClosedResult::Proven(EtcdCommitDisposition::Closed(
                CommitOwnerAdmissionResultV1::NotDispatched(
                    CommitOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect
                )
            ))
        ));
        assert!(matches!(
            classify_commit_snapshot_state(&plan, &prepared, false),
            EtcdClosedResult::OutcomeUnknown
        ));
        assert!(matches!(
            classify_commit_snapshot_state(
                &plan,
                &owner_admission_committed_snapshot(&plan),
                false,
            ),
            EtcdClosedResult::Proven(EtcdCommitDisposition::Live {
                already_committed: true,
                ..
            })
        ));
    }

    #[test]
    fn commit_and_abort_compete_on_the_same_exact_prepared_triple() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let snapshot = owner_admission_prepared_snapshot(&plan);
        let OwnerAdmissionStateDecision::Mutation(commit) =
            plan_owner_admission_commit(&plan, &snapshot)
        else {
            panic!("expected commit mutation")
        };
        let OwnerAdmissionStateDecision::Mutation(abort) = plan_owner_admission_abort(
            &plan,
            &snapshot,
            crate::OwnerAdmissionAbortReasonV1::OwnerCasRejected,
        ) else {
            panic!("expected abort mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let commit = commit_owner_admission_write_plan(&keys, &commit).unwrap();
        let abort = abort_owner_admission_write_plan(&keys, &abort).unwrap();

        for key in [&keys.candidate_claim, &keys.plan, &keys.sentinel] {
            let commit_expected = commit
                .comparisons
                .iter()
                .find(|comparison| comparison.key == *key)
                .map(|comparison| &comparison.expected);
            let abort_expected = abort
                .comparisons
                .iter()
                .find(|comparison| comparison.key == *key)
                .map(|comparison| &comparison.expected);
            assert!(commit_expected == abort_expected);
        }
        assert!(matches!(
            commit
                .comparisons
                .iter()
                .find(|comparison| comparison.key == keys.sentinel)
                .map(|comparison| &comparison.expected),
            Some(EtcdExpectedValue::Exact {
                attached_lease_id: 9,
                ..
            })
        ));
    }

    #[test]
    fn expired_prepared_abort_compares_authoritative_sentinel_absence() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let mut snapshot = owner_admission_prepared_snapshot(&plan);
        snapshot.sentinel = None;
        snapshot.sentinel_absence_evidence = Some(
            AuthoritativeSentinelAbsenceEvidence::after_backend_check(&plan),
        );
        let OwnerAdmissionStateDecision::Mutation(mutation) = plan_owner_admission_abort(
            &plan,
            &snapshot,
            crate::OwnerAdmissionAbortReasonV1::LeaseLostBeforeCommit,
        ) else {
            panic!("expected expired-prepared abort mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let write = abort_owner_admission_write_plan(&keys, &mutation).unwrap();
        assert!(matches!(
            write
                .comparisons
                .iter()
                .find(|comparison| comparison.key == keys.sentinel)
                .map(|comparison| &comparison.expected),
            Some(EtcdExpectedValue::Absent)
        ));
    }

    #[test]
    fn abort_response_loss_requires_terminal_reread_before_closing() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let prepared = owner_admission_prepared_snapshot(&plan);
        assert!(matches!(
            classify_abort_snapshot_state(&plan, &prepared, true),
            EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::NotDispatched(
                AbortOwnerAdmissionNotDispatchedV1::PreparedBindingLostBeforeEffect
            ))
        ));
        assert!(matches!(
            classify_abort_snapshot_state(&plan, &prepared, false),
            EtcdClosedResult::OutcomeUnknown
        ));
        let aborted = owner_admission_aborted_snapshot(
            &plan,
            crate::OwnerAdmissionAbortReasonV1::OwnerCasRejected,
        );
        assert!(matches!(
            classify_abort_snapshot_state(&plan, &aborted, false),
            EtcdClosedResult::Proven(AbortOwnerAdmissionResultV1::DurableConflict { .. })
        ));
    }

    #[test]
    fn released_and_authority_cutover_terminate_bind_the_live_session() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let committed = owner_admission_committed_snapshot(&plan);
        let reasons = [
            OwnerAdmissionTerminationReasonV1::Released,
            OwnerAdmissionTerminationReasonV1::AuthorityCutover {
                migration_id: OperationId::from_bytes([11; 16]),
            },
        ];

        for reason in reasons {
            let OwnerAdmissionStateDecision::Mutation(mutation) =
                plan_owner_admission_terminate(&plan, &committed, reason)
            else {
                panic!("expected live-session terminate mutation")
            };
            let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
            let write = terminate_owner_admission_write_plan(&keys, &mutation).unwrap();
            assert!(matches!(
                write
                    .comparisons
                    .iter()
                    .find(|comparison| comparison.key == keys.session)
                    .map(|comparison| &comparison.expected),
                Some(EtcdExpectedValue::Exact {
                    attached_lease_id: 9,
                    ..
                })
            ));
            assert!(write.operations.iter().any(|operation| matches!(
                operation,
                EtcdWriteOperation::Delete { key } if key == &keys.session
            )));
        }
    }

    #[test]
    fn lease_expired_terminate_reuses_proven_digest_and_compares_session_absence() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let evidence_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([12; 32]).unwrap();
        let reason = OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest };
        let expired = owner_admission_expired_committed_snapshot(&plan, evidence_digest);
        let OwnerAdmissionStateDecision::Mutation(mutation) =
            plan_owner_admission_terminate(&plan, &expired, reason.clone())
        else {
            panic!("expected lease-expired terminate mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let write = terminate_owner_admission_write_plan(&keys, &mutation).unwrap();
        assert!(matches!(
            write
                .comparisons
                .iter()
                .find(|comparison| comparison.key == keys.session)
                .map(|comparison| &comparison.expected),
            Some(EtcdExpectedValue::Absent)
        ));
        assert!(!write.operations.iter().any(|operation| matches!(
            operation,
            EtcdWriteOperation::Delete { key } if key == &keys.session
        )));
        let next_claim = write
            .operations
            .iter()
            .find_map(|operation| match operation {
                EtcdWriteOperation::Put { key, value, .. } if key == &keys.candidate_claim => {
                    Some(decode_owner_admission_claim(value).unwrap())
                }
                _ => None,
            })
            .unwrap();
        assert!(matches!(
            next_claim.phase(),
            OwnerAdmissionClaimPhaseV1::Terminated {
                reason: stored_reason,
                ..
            } if stored_reason == &reason
        ));
    }

    #[test]
    fn terminate_replay_closes_as_already_terminated_or_superseded() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let reason = OwnerAdmissionTerminationReasonV1::Released;
        let terminated = owner_admission_terminated_snapshot(&plan, reason.clone());
        assert!(matches!(
            classify_terminate_snapshot_state(&plan, &reason, &terminated, false),
            EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::AlreadyTerminated { .. })
        ));

        let terminal_shard = terminated.logical_shard_record.clone().unwrap();
        let terminal_claim = terminated.candidate_claim.clone().unwrap();
        let successor_intent = OwnerAdmissionIntentV1::successor(
            plan.intent().admission().clone(),
            terminal_shard,
            terminal_claim,
            NodeId::new("node-b").unwrap(),
            OwnerIncarnationId::from_bytes([13; 16]),
            "10.0.0.2:7000".to_owned(),
            OwnerRuntimeReservationDigest::from_bytes([14; 32]).unwrap(),
        )
        .unwrap();
        let successor_lease = LogicalShardLease {
            logical_shard_id: successor_intent.logical_shard_id(),
            owner: successor_intent.owner().clone(),
            owner_epoch: successor_intent.planned_epoch(),
            owner_incarnation_id: successor_intent.owner_incarnation_id(),
            lease_id: 10,
            authority: successor_intent.admission().authority().fence(),
        };
        let successor_plan =
            PlannedOwnerAdmissionV1::new(successor_intent, successor_lease.clone()).unwrap();
        let mut superseded = terminated;
        superseded.logical_shard_record =
            Some(expected_recovering_record_for_plan(&successor_plan));
        superseded.session = Some(successor_lease);
        assert!(matches!(
            classify_terminate_snapshot_state(&plan, &reason, &superseded, false),
            EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::Superseded { .. })
        ));
    }

    #[test]
    fn terminate_does_not_adopt_foreign_or_differently_terminated_claims() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let reason = OwnerAdmissionTerminationReasonV1::Released;
        let mut foreign = owner_admission_not_started_snapshot(plan.intent());
        let foreign_plan = owner_admission_plan("10.0.0.2:7000");
        foreign.candidate_claim = Some(OwnerAdmissionClaimV1::prepared(&foreign_plan).unwrap());
        assert!(matches!(
            classify_terminate_snapshot_state(&plan, &reason, &foreign, true),
            EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::NotDispatched(
                TerminateOwnerAdmissionNotDispatchedV1::ExactOwnerBindingLostBeforeEffect
            ))
        ));
        assert!(matches!(
            classify_terminate_snapshot_state(&plan, &reason, &foreign, false),
            EtcdClosedResult::OutcomeUnknown
        ));

        let different = OwnerAdmissionTerminationReasonV1::AuthorityCutover {
            migration_id: OperationId::from_bytes([15; 16]),
        };
        let differently_terminated = owner_admission_terminated_snapshot(&plan, different);
        assert!(matches!(
            classify_terminate_snapshot_state(&plan, &reason, &differently_terminated, false),
            EtcdClosedResult::Proven(TerminateOwnerAdmissionResultV1::DurableConflict { .. })
        ));
    }

    #[test]
    fn prepare_and_ambiguity_seal_compete_on_the_same_absent_claim_key() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let snapshot = owner_admission_not_started_snapshot(plan.intent());
        let OwnerAdmissionStateDecision::Mutation(prepare) =
            plan_owner_admission_prepare(&plan, &snapshot)
        else {
            panic!("expected prepare mutation")
        };
        let OwnerAdmissionStateDecision::Mutation(seal) =
            plan_owner_admission_intent_seal_rejected(plan.intent(), &snapshot)
        else {
            panic!("expected seal mutation")
        };
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let prepare = prepare_owner_admission_write_plan(&keys, &prepare).unwrap();
        let seal = seal_owner_admission_write_plan(&keys, &seal).unwrap();
        let prepare_claim = prepare
            .comparisons
            .iter()
            .find(|comparison| comparison.key == keys.candidate_claim)
            .unwrap();
        let seal_claim = seal
            .comparisons
            .iter()
            .find(|comparison| comparison.key == keys.candidate_claim)
            .unwrap();
        assert!(matches!(prepare_claim.expected, EtcdExpectedValue::Absent));
        assert!(prepare_claim.expected == seal_claim.expected);
    }

    #[test]
    fn reconcile_classifies_the_complete_exact_state_matrix() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let intent_target = ReconcileOwnerAdmissionTargetV1::IntentOnly(plan.intent().clone());
        let plan_target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());

        let rejected = owner_admission_rejected_snapshot(
            plan.intent(),
            OwnerAdmissionRejectionReasonV1::ExpectedShardChanged,
        );
        assert!(matches!(
            reconcile_result_from_decision(
                &intent_target,
                &rejected,
                reconcile_decision_for_target(&intent_target, &rejected),
            ),
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::Rejected { .. }
            ))
        ));
        assert!(matches!(
            reconcile_result_from_decision(
                &plan_target,
                &rejected,
                reconcile_decision_for_target(&plan_target, &rejected),
            ),
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::NotDispatched(
                    ReconcileOwnerAdmissionNotDispatchedV1::ControlBindingLostBeforeEffect
                )
            ))
        ));

        let prepared = owner_admission_prepared_snapshot(&plan);
        assert!(matches!(
            reconcile_result_from_decision(
                &plan_target,
                &prepared,
                reconcile_decision_for_target(&plan_target, &prepared),
            ),
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::Prepared { .. }
            ))
        ));

        let mut expired_prepared = prepared;
        expired_prepared.sentinel = None;
        expired_prepared.sentinel_absence_evidence = Some(
            AuthoritativeSentinelAbsenceEvidence::after_backend_check(&plan),
        );
        assert!(matches!(
            reconcile_result_from_decision(
                &plan_target,
                &expired_prepared,
                reconcile_decision_for_target(&plan_target, &expired_prepared),
            ),
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::ExpiredPrepared { .. }
            ))
        ));

        let committed = owner_admission_committed_snapshot(&plan);
        assert!(matches!(
            reconcile_result_from_decision(
                &plan_target,
                &committed,
                reconcile_decision_for_target(&plan_target, &committed),
            ),
            Some(EtcdReconcileDisposition::Live { .. })
        ));

        let evidence_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([16; 32]).unwrap();
        let expired_committed = owner_admission_expired_committed_snapshot(&plan, evidence_digest);
        let result = reconcile_result_from_decision(
            &plan_target,
            &expired_committed,
            reconcile_decision_for_target(&plan_target, &expired_committed),
        );
        assert!(matches!(
            result,
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::ExpiredCommitted {
                    evidence_digest: actual,
                    ..
                }
            )) if actual == evidence_digest
        ));

        let aborted = owner_admission_aborted_snapshot(
            &plan,
            crate::OwnerAdmissionAbortReasonV1::OwnerCasRejected,
        );
        assert!(matches!(
            reconcile_result_from_decision(
                &plan_target,
                &aborted,
                reconcile_decision_for_target(&plan_target, &aborted),
            ),
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::DurableConflict { .. }
            ))
        ));

        let mut foreign = owner_admission_not_started_snapshot(plan.intent());
        foreign.candidate_claim =
            Some(OwnerAdmissionClaimV1::prepared(&owner_admission_plan("10.0.0.2:7000")).unwrap());
        assert!(matches!(
            reconcile_result_from_decision(
                &intent_target,
                &foreign,
                reconcile_decision_for_target(&intent_target, &foreign),
            ),
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::IncarnationAlreadyClaimed { .. }
            ))
        ));
    }

    #[test]
    fn reconcile_seal_response_loss_never_turns_bare_absence_terminal() {
        let intent = owner_admission_intent("10.0.0.1:7000");
        let target = ReconcileOwnerAdmissionTargetV1::IntentOnly(intent.clone());
        let not_started = owner_admission_not_started_snapshot(&intent);
        assert!(matches!(
            classify_reconcile_after_seal_state(&target, &not_started, false),
            EtcdClosedResult::OutcomeUnknown
        ));
        assert!(matches!(
            classify_reconcile_after_seal_state(&target, &not_started, true),
            EtcdClosedResult::Proven(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                    OwnerAdmissionInconsistencyCode::TypedValueConstructionFailed
                )
            ))
        ));

        let OwnerAdmissionStateDecision::Mutation(mutation) =
            plan_owner_admission_intent_seal_rejected(&intent, &not_started)
        else {
            panic!("expected seal mutation")
        };
        let result = successful_seal_rejected_mutation_result(&mutation).unwrap();
        assert!(matches!(
            result,
            ReconcileOwnerAdmissionResultV1::Rejected { claim }
                if matches!(
                    claim.phase(),
                    OwnerAdmissionClaimPhaseV1::Rejected {
                        reason: OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed
                    }
                )
        ));
    }

    #[test]
    fn aggregate_snapshot_rejects_permanent_and_leased_key_binding_drift() {
        let options =
            EtcdControlStoreOptions::new(["http://127.0.0.1:2379"]).with_key_prefix("/nokv/test/");
        let plan = owner_admission_plan("10.0.0.1:7000");
        let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
        let prepared_claim = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&plan);
        let entries = vec![
            Some(stored_entry(
                &keys.shard,
                encode_logical_shard_record(plan.intent().expected_unowned_shard()).unwrap(),
                0,
            )),
            Some(stored_entry(
                &keys.placement,
                encode_root_placement(plan.intent().admission().placement()).unwrap(),
                0,
            )),
            Some(stored_entry(
                &keys.authority,
                encode_metadata_authority_record(plan.intent().admission().authority()).unwrap(),
                0,
            )),
            None,
            Some(stored_entry(
                &keys.plan,
                encode_planned_owner_admission(&plan).unwrap(),
                0,
            )),
            Some(stored_entry(
                &keys.sentinel,
                encode_owner_admission_plan_sentinel(&sentinel).unwrap(),
                9,
            )),
            Some(stored_entry(
                &keys.candidate_claim,
                encode_owner_admission_claim(&prepared_claim).unwrap(),
                0,
            )),
        ];
        assert!(decode_owner_admission_snapshot(&keys, 1, 1, entries.clone()).is_ok());

        let mut leased_plan = entries.clone();
        leased_plan[4].as_mut().unwrap().attached_lease_id = 9;
        assert!(matches!(
            decode_owner_admission_snapshot(&keys, 1, 1, leased_plan),
            Err(OwnerAdmissionInconsistencyCode::ActivePlanShardMismatch)
        ));
        let mut leased_claim = entries.clone();
        leased_claim[6].as_mut().unwrap().attached_lease_id = 9;
        assert!(matches!(
            decode_owner_admission_snapshot(&keys, 1, 1, leased_claim),
            Err(OwnerAdmissionInconsistencyCode::CandidateClaimKeyMismatch)
        ));
        let mut sentinel_wrong_lease = entries;
        sentinel_wrong_lease[5].as_mut().unwrap().attached_lease_id = 10;
        assert!(matches!(
            decode_owner_admission_snapshot(&keys, 1, 1, sentinel_wrong_lease),
            Err(OwnerAdmissionInconsistencyCode::SentinelShardMismatch)
        ));
    }

    #[test]
    fn reconcile_reports_mid_state_corruption_instead_of_guessing() {
        let plan = owner_admission_plan("10.0.0.1:7000");
        let target = ReconcileOwnerAdmissionTargetV1::ExactPlan(plan.clone());
        let mut corrupt = owner_admission_prepared_snapshot(&plan);
        corrupt.active_plan = None;
        let result = reconcile_result_from_decision(
            &target,
            &corrupt,
            reconcile_decision_for_target(&target, &corrupt),
        );
        assert!(matches!(
            result,
            Some(EtcdReconcileDisposition::Closed(
                ReconcileOwnerAdmissionResultV1::DurableInconsistent(
                    OwnerAdmissionInconsistencyCode::SentinelWithoutPlan
                )
            ))
        ));
    }

    #[test]
    #[ignore = "requires NOKV_ETCD_TEST_ENDPOINT pointing at a disposable etcd"]
    fn live_false_branch_returns_one_same_revision_else_snapshot() {
        let endpoint = std::env::var("NOKV_ETCD_TEST_ENDPOINT")
            .expect("NOKV_ETCD_TEST_ENDPOINT must point at a disposable etcd");
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async move {
            let prefix = format!("/nokv/owner-admission-live-test-{}/", std::process::id());
            let options =
                EtcdControlStoreOptions::new([endpoint.as_str()]).with_key_prefix(prefix.as_str());
            let mut client = Client::connect([endpoint], None).await.unwrap();
            let plan = owner_admission_plan("10.0.0.1:7000");
            let not_started = owner_admission_not_started_snapshot(plan.intent());
            let OwnerAdmissionStateDecision::Mutation(mutation) =
                plan_owner_admission_intent_seal_rejected(plan.intent(), &not_started)
            else {
                panic!("expected seal mutation")
            };
            let keys = OwnerAdmissionSnapshotKeys::new(&options, plan.intent());
            let write = seal_owner_admission_write_plan(&keys, &mutation).unwrap();
            let prepared_claim = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
            client
                .put(
                    keys.candidate_claim.clone(),
                    encode_owner_admission_claim(&prepared_claim).unwrap(),
                    None,
                )
                .await
                .unwrap();

            let response = client.txn(write.into_txn()).await.unwrap();
            assert!(!response.succeeded());
            assert_eq!(response.op_responses().len(), keys.ordered().len());
            assert!(response
                .op_responses()
                .iter()
                .all(|response| matches!(response, TxnOpResponse::Get(_))));
            let response_revision = response.header().unwrap().revision();
            let snapshot = owner_admission_snapshot_from_txn(&keys, &response).unwrap();
            assert_eq!(snapshot.revision, response_revision);
            assert_eq!(snapshot.state.candidate_claim, Some(prepared_claim));

            client.delete(keys.candidate_claim, None).await.unwrap();
        });
    }
}
