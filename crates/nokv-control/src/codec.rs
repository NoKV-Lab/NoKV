use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::owner_admission::{
    OwnerAdmissionAbortReasonV1, OwnerAdmissionClaimDigestV1, OwnerAdmissionClaimIdentityV1,
    OwnerAdmissionClaimPhaseV1, OwnerAdmissionClaimV1, OwnerAdmissionIntentDigestV1,
    OwnerAdmissionIntentV1, OwnerAdmissionKindV1, OwnerAdmissionPlanDigestV1,
    OwnerAdmissionPlanSentinelV1, OwnerAdmissionRecordDigestV1, OwnerAdmissionRejectionReasonV1,
    OwnerAdmissionTerminationReasonV1, OwnerLeaseExpiryEvidenceDigest,
    OwnerRuntimeReservationDigest, OwnerServingPublicationDigestV1, OwnerSessionBindingDigestV1,
    OwnerSessionRenewalTargetDigestV1, PlannedOwnerAdmissionV1,
};
#[cfg(any(feature = "etcd", test))]
use crate::store::validate_owner_incarnation_id;
use crate::store::{validate_logical_shard_record, validate_metadata_authority_record};
use crate::types::SHA256_BYTES;
use crate::{
    CheckpointRef, CommitVersion, ConsistencyDomainId, ControlError, LogRef, LogSegmentRef,
    LogicalShardId, LogicalShardLease, LogicalShardRecord, LogicalShardState,
    MetadataAuthorityBinding, MetadataAuthorityFence, MetadataAuthorityGeneration,
    MetadataAuthorityId, MetadataAuthorityRecord, MetadataAuthorityRevision,
    MetadataContractDigest, MetadataMigration, MetadataMigrationPhase, MetadataProviderProfileId,
    MetadataRecoveryFrontier, NodeId, OperationId, OwnerEpoch, OwnerIncarnationId,
    OwnerServingAdmission, PlacementGeneration, RecoveryPublication, RootId, RootLayoutGeneration,
    RootLayoutProfile, RootPartitionId, RootPlacement, RootPlacementLifecycle,
    SourceQuiesceReceipt, TargetActivationToken,
};

const ROOT_PLACEMENT_CODEC_VERSION: u8 = 2;
const LOGICAL_SHARD_RECORD_CODEC_VERSION: u8 = 2;
const METADATA_AUTHORITY_RECORD_CODEC_VERSION: u8 = 2;
// Owner-admission v1 canonical bytes and digest preimages embed lowercase-hex
// canonical root-placement v2, metadata-authority v2, logical-shard-record v2,
// and owner-admission-claim v1 bytes. Changing any inner codec requires a
// simultaneous owner-admission version bump, new digest domains, and goldens.
const OWNER_ADMISSION_CODEC_VERSION: u8 = 1;
const OWNER_ADMISSION_INNER_ROOT_PLACEMENT_CODEC_VERSION: u8 = 2;
const OWNER_ADMISSION_INNER_AUTHORITY_CODEC_VERSION: u8 = 2;
const OWNER_ADMISSION_INNER_SHARD_RECORD_CODEC_VERSION: u8 = 2;
const OWNER_ADMISSION_INNER_CLAIM_CODEC_VERSION: u8 = 1;
#[cfg(any(feature = "etcd", test))]
const OWNER_SESSION_CODEC_VERSION: u8 = 3;
const MAX_NESTED_ROOT_PLACEMENT_BYTES: usize = 16 * 1_024;
const MAX_NESTED_AUTHORITY_BYTES: usize = 64 * 1_024;
const MAX_NESTED_LOGICAL_SHARD_BYTES: usize = 128 * 1_024;
const MAX_OWNER_ADMISSION_INTENT_BYTES: usize = 512 * 1_024;
const MAX_OWNER_ADMISSION_PLAN_BYTES: usize = 1_100_000;
const MAX_OWNER_ADMISSION_CLAIM_BYTES: usize = 16 * 1_024;
const MAX_OWNER_ADMISSION_SENTINEL_BYTES: usize = 1_024;
const OWNER_ADMISSION_INTENT_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-admission-intent.v1.root-v2.authority-v2.shard-v2.claim-v1\0";
const OWNER_ADMISSION_PLAN_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-admission-plan.v1.intent-v1.root-v2.authority-v2.shard-v2.claim-v1\0";
const OWNER_ADMISSION_RECORD_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-admission-record.v1.logical-shard-v2\0";
const OWNER_ADMISSION_CLAIM_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-admission-claim.v1.claim-v1\0";
const OWNER_SESSION_BINDING_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-session-binding.v1.lease-v1\0";
const OWNER_SERVING_PUBLICATION_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-serving-publication.v1.plan-v1.logical-shard-v2.recovery-publication-v1\0";
const OWNER_SESSION_RENEWAL_TARGET_DIGEST_DOMAIN: &[u8] =
    b"nokv.control.owner-session-renewal-target.v1.plan-v1.claim-v1.lease-v1\0";

const _: () = {
    assert!(ROOT_PLACEMENT_CODEC_VERSION == OWNER_ADMISSION_INNER_ROOT_PLACEMENT_CODEC_VERSION);
    assert!(
        METADATA_AUTHORITY_RECORD_CODEC_VERSION == OWNER_ADMISSION_INNER_AUTHORITY_CODEC_VERSION
    );
    assert!(LOGICAL_SHARD_RECORD_CODEC_VERSION == OWNER_ADMISSION_INNER_SHARD_RECORD_CODEC_VERSION);
    assert!(OWNER_ADMISSION_CODEC_VERSION == OWNER_ADMISSION_INNER_CLAIM_CODEC_VERSION);
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootPlacementWire {
    version: u8,
    root_id: String,
    layout_profile: u8,
    layout_generation: u64,
    partition_id: String,
    logical_shard_id: String,
    placement_generation: u64,
    lifecycle: u8,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalShardRecordWire {
    version: u8,
    logical_shard_id: String,
    owner: Option<String>,
    owner_epoch: Option<u64>,
    owner_incarnation_id: Option<String>,
    lease_id: u64,
    state: u8,
    endpoint: Option<String>,
    checkpoint: Option<CheckpointRefWire>,
    log: Option<LogRefWire>,
    durable_lsn: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataAuthorityRecordWire {
    version: u8,
    logical_shard_id: String,
    record_revision: u64,
    authority_generation: u64,
    active: MetadataAuthorityBindingWire,
    migration: Option<MetadataMigrationWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataAuthorityBindingWire {
    authority_id: String,
    provider_profile_id: String,
    profile_fingerprint: String,
    consistency_domain_id: String,
    contract_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataMigrationWire {
    migration_id: String,
    source: MetadataAuthorityBindingWire,
    target: MetadataAuthorityBindingWire,
    phase: u8,
    source_frontier: Option<MetadataRecoveryFrontierWire>,
    target_frontier: Option<MetadataRecoveryFrontierWire>,
    cutover_frontier: Option<MetadataRecoveryFrontierWire>,
    source_quiesce_receipt: Option<SourceQuiesceReceiptWire>,
    target_activation_token: Option<TargetActivationTokenWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRecoveryFrontierWire {
    recovery_lsn: u64,
    chain_digest: String,
    commit_version: u64,
    state_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceQuiesceReceiptWire {
    logical_shard_id: String,
    migration_id: String,
    source_authority_id: String,
    source_authority_generation: u64,
    owner_epoch: u64,
    frontier: MetadataRecoveryFrontierWire,
    contract_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TargetActivationTokenWire {
    logical_shard_id: String,
    migration_id: String,
    source_authority_id: String,
    source_authority_generation: u64,
    target_authority_id: String,
    target_authority_generation: u64,
    frontier: MetadataRecoveryFrontierWire,
    contract_digest: String,
    source_receipt_digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointRefWire {
    object_key: String,
    lsn: u64,
    image_bytes: u64,
    image_digest: String,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSegmentRefWire {
    segment_key: String,
    first_lsn: u64,
    last_lsn: u64,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRefWire {
    segments: Vec<LogSegmentRefWire>,
    durable_lsn: u64,
    digest: String,
}

#[cfg(any(feature = "etcd", test))]
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerSessionWire {
    version: u8,
    logical_shard_id: String,
    owner: String,
    owner_epoch: u64,
    owner_incarnation_id: String,
    lease_id: u64,
    authority_id: String,
    authority_generation: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerAdmissionIntentWire {
    version: u8,
    kind: u8,
    placement: String,
    authority: String,
    expected_unowned_shard: String,
    expected_previous_claim: Option<String>,
    owner: String,
    owner_incarnation_id: String,
    endpoint: String,
    planned_epoch: u64,
    reservation_digest: String,
    intent_digest: String,
}

#[derive(Serialize)]
struct OwnerAdmissionIntentDigestWire {
    version: u8,
    kind: u8,
    placement: String,
    authority: String,
    expected_unowned_shard: String,
    expected_previous_claim: Option<String>,
    owner: String,
    owner_incarnation_id: String,
    endpoint: String,
    planned_epoch: u64,
    reservation_digest: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerAdmissionLeaseWire {
    logical_shard_id: String,
    owner: String,
    owner_epoch: u64,
    owner_incarnation_id: String,
    lease_id: u64,
    authority_id: String,
    authority_generation: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlannedOwnerAdmissionWire {
    version: u8,
    intent: String,
    lease: OwnerAdmissionLeaseWire,
    plan_digest: String,
}

#[derive(Serialize)]
struct PlannedOwnerAdmissionDigestWire {
    version: u8,
    intent: String,
    lease: OwnerAdmissionLeaseWire,
}

#[derive(Serialize)]
struct OwnerSessionBindingDigestWire {
    version: u8,
    session: OwnerAdmissionLeaseWire,
}

#[derive(Serialize)]
struct RecoveryPublicationDigestWire {
    checkpoint: Option<CheckpointRefWire>,
    log: Option<LogRefWire>,
    durable_lsn: u64,
}

#[derive(Serialize)]
struct OwnerServingPublicationDigestWire {
    version: u8,
    plan: String,
    source: String,
    publication: RecoveryPublicationDigestWire,
    target: String,
}

#[derive(Serialize)]
struct OwnerSessionRenewalTargetDigestWire {
    version: u8,
    plan: String,
    claim: String,
    session: OwnerAdmissionLeaseWire,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerAdmissionClaimWire {
    version: u8,
    logical_shard_id: String,
    owner_incarnation_id: String,
    intent_digest: String,
    reservation_digest: String,
    planned_epoch: u64,
    phase: u8,
    lease: Option<OwnerAdmissionLeaseWire>,
    plan_digest: Option<String>,
    termination_reason: Option<OwnerAdmissionTerminationReasonWire>,
    abort_reason: Option<u8>,
    rejection_reason: Option<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerAdmissionTerminationReasonWire {
    kind: u8,
    lease_expiry_evidence_digest: Option<String>,
    migration_id: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerAdmissionPlanSentinelWire {
    version: u8,
    logical_shard_id: String,
    owner_incarnation_id: String,
    lease_id: u64,
    plan_digest: String,
}

#[derive(Deserialize)]
struct CodecVersionProbe {
    version: u8,
}

pub fn encode_root_placement(placement: &RootPlacement) -> Result<Vec<u8>, ControlError> {
    validate_root_placement(placement)?;
    serde_json::to_vec(&RootPlacementWire {
        version: ROOT_PLACEMENT_CODEC_VERSION,
        root_id: encode_fixed_id(placement.root_id.as_bytes()),
        layout_profile: placement.layout_profile.into(),
        layout_generation: placement.layout_generation.get(),
        partition_id: encode_fixed_id(placement.partition_id.as_bytes()),
        logical_shard_id: encode_fixed_id(placement.logical_shard_id.as_bytes()),
        placement_generation: placement.placement_generation.get(),
        lifecycle: placement.lifecycle.into(),
    })
    .map_err(codec_error)
}

pub fn decode_root_placement(bytes: &[u8]) -> Result<RootPlacement, ControlError> {
    let probe: CodecVersionProbe = serde_json::from_slice(bytes).map_err(codec_error)?;
    if probe.version == 1 {
        return Err(ControlError::RootPlacementCodecUpgradeRequired {
            stored_version: probe.version,
            required_version: ROOT_PLACEMENT_CODEC_VERSION,
        });
    }
    require_version(
        "root placement",
        probe.version,
        ROOT_PLACEMENT_CODEC_VERSION,
    )?;
    let wire: RootPlacementWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let placement = RootPlacement {
        root_id: RootId::from_bytes(decode_fixed_id(&wire.root_id, "root id")?),
        layout_profile: RootLayoutProfile::try_from(wire.layout_profile)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        layout_generation: RootLayoutGeneration::new(wire.layout_generation)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        partition_id: RootPartitionId::from_bytes(decode_fixed_id(
            &wire.partition_id,
            "root partition id",
        )?),
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "logical shard id",
        )?),
        placement_generation: PlacementGeneration::new(wire.placement_generation)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        lifecycle: RootPlacementLifecycle::try_from(wire.lifecycle)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
    };
    validate_root_placement(&placement)?;
    if encode_root_placement(&placement)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "root placement input is not canonical".to_owned(),
        ));
    }
    Ok(placement)
}

pub fn encode_logical_shard_record(record: &LogicalShardRecord) -> Result<Vec<u8>, ControlError> {
    validate_logical_shard_record(record)?;
    serde_json::to_vec(&LogicalShardRecordWire {
        version: LOGICAL_SHARD_RECORD_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(record.logical_shard_id.as_bytes()),
        owner: record.owner.as_ref().map(|owner| owner.as_str().to_owned()),
        owner_epoch: record.owner_epoch.map(OwnerEpoch::get),
        owner_incarnation_id: record
            .owner_incarnation_id
            .map(|id| encode_fixed_id(id.as_bytes())),
        lease_id: record.lease_id,
        state: record.state.into(),
        endpoint: record.endpoint.clone(),
        checkpoint: record.checkpoint.clone().map(Into::into),
        log: record.log.clone().map(Into::into),
        durable_lsn: record.durable_lsn,
    })
    .map_err(codec_error)
}

pub fn decode_logical_shard_record(bytes: &[u8]) -> Result<LogicalShardRecord, ControlError> {
    let probe: CodecVersionProbe = serde_json::from_slice(bytes).map_err(codec_error)?;
    if probe.version == 1 {
        return Err(ControlError::LogicalShardRecordCodecUpgradeRequired {
            stored_version: probe.version,
            required_version: LOGICAL_SHARD_RECORD_CODEC_VERSION,
        });
    }
    require_version(
        "logical shard record",
        probe.version,
        LOGICAL_SHARD_RECORD_CODEC_VERSION,
    )?;
    let wire: LogicalShardRecordWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let record = LogicalShardRecord {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "logical shard id",
        )?),
        owner: wire
            .owner
            .map(NodeId::new)
            .transpose()
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        owner_epoch: wire
            .owner_epoch
            .map(OwnerEpoch::new)
            .transpose()
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        owner_incarnation_id: wire
            .owner_incarnation_id
            .map(|encoded| {
                decode_fixed_id(&encoded, "owner incarnation id")
                    .map(OwnerIncarnationId::from_bytes)
            })
            .transpose()?,
        lease_id: wire.lease_id,
        state: LogicalShardState::try_from(wire.state)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        endpoint: wire.endpoint,
        checkpoint: wire.checkpoint.map(Into::into),
        log: wire.log.map(Into::into),
        durable_lsn: wire.durable_lsn,
    };
    validate_logical_shard_record(&record)?;
    if encode_logical_shard_record(&record)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "logical shard record input is not canonical".to_owned(),
        ));
    }
    Ok(record)
}

pub fn encode_metadata_authority_record(
    authority: &MetadataAuthorityRecord,
) -> Result<Vec<u8>, ControlError> {
    validate_metadata_authority_record(authority)?;
    serde_json::to_vec(&MetadataAuthorityRecordWire {
        version: METADATA_AUTHORITY_RECORD_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(authority.logical_shard_id.as_bytes()),
        record_revision: authority.record_revision.get(),
        authority_generation: authority.authority_generation.get(),
        active: encode_authority_binding(&authority.active),
        migration: authority.migration.as_ref().map(encode_metadata_migration),
    })
    .map_err(codec_error)
}

pub fn decode_metadata_authority_record(
    bytes: &[u8],
) -> Result<MetadataAuthorityRecord, ControlError> {
    let probe: CodecVersionProbe = serde_json::from_slice(bytes).map_err(codec_error)?;
    if probe.version == 1 {
        return Err(ControlError::MetadataAuthorityCodecUpgradeRequired {
            stored_version: probe.version,
            required_version: METADATA_AUTHORITY_RECORD_CODEC_VERSION,
        });
    }
    require_version(
        "metadata authority record",
        probe.version,
        METADATA_AUTHORITY_RECORD_CODEC_VERSION,
    )?;
    let wire: MetadataAuthorityRecordWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let authority = MetadataAuthorityRecord {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "logical shard id",
        )?),
        record_revision: MetadataAuthorityRevision::new(wire.record_revision)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        authority_generation: MetadataAuthorityGeneration::new(wire.authority_generation)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        active: decode_authority_binding(wire.active)?,
        migration: wire.migration.map(decode_metadata_migration).transpose()?,
    };
    validate_metadata_authority_record(&authority)?;
    if encode_metadata_authority_record(&authority)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "metadata authority record input is not canonical".to_owned(),
        ));
    }
    Ok(authority)
}

/// Encode one exact owner-admission intent using the canonical v1 wire.
pub fn encode_owner_admission_intent(
    intent: &OwnerAdmissionIntentV1,
) -> Result<Vec<u8>, ControlError> {
    let recomputed = compute_owner_admission_intent_digest(
        intent.kind(),
        intent.admission(),
        intent.expected_unowned_shard(),
        intent.expected_previous_claim(),
        intent.owner(),
        intent.owner_incarnation_id(),
        intent.endpoint(),
        intent.planned_epoch(),
        intent.reservation_digest(),
    )?;
    if recomputed != intent.digest() {
        return Err(ControlError::InvalidRecord(
            "owner admission intent digest is not canonical".to_owned(),
        ));
    }
    let digest_wire = owner_admission_intent_digest_wire(
        intent.kind(),
        intent.admission(),
        intent.expected_unowned_shard(),
        intent.expected_previous_claim(),
        intent.owner(),
        intent.owner_incarnation_id(),
        intent.endpoint(),
        intent.planned_epoch(),
        intent.reservation_digest(),
    )?;
    let bytes = serde_json::to_vec(&OwnerAdmissionIntentWire {
        version: digest_wire.version,
        kind: digest_wire.kind,
        placement: digest_wire.placement,
        authority: digest_wire.authority,
        expected_unowned_shard: digest_wire.expected_unowned_shard,
        expected_previous_claim: digest_wire.expected_previous_claim,
        owner: digest_wire.owner,
        owner_incarnation_id: digest_wire.owner_incarnation_id,
        endpoint: digest_wire.endpoint,
        planned_epoch: digest_wire.planned_epoch,
        reservation_digest: digest_wire.reservation_digest,
        intent_digest: encode_fixed_id(intent.digest().as_bytes()),
    })
    .map_err(codec_error)?;
    require_max_wire_size(
        "owner admission intent",
        &bytes,
        MAX_OWNER_ADMISSION_INTENT_BYTES,
    )?;
    Ok(bytes)
}

/// Decode and recompute one strict canonical v1 owner-admission intent.
pub fn decode_owner_admission_intent(bytes: &[u8]) -> Result<OwnerAdmissionIntentV1, ControlError> {
    require_max_wire_size(
        "owner admission intent",
        bytes,
        MAX_OWNER_ADMISSION_INTENT_BYTES,
    )?;
    require_owner_admission_version(bytes, "owner admission intent")?;
    let wire: OwnerAdmissionIntentWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let placement_bytes = decode_nested_hex(
        &wire.placement,
        "owner admission placement",
        MAX_NESTED_ROOT_PLACEMENT_BYTES,
    )?;
    let authority_bytes = decode_nested_hex(
        &wire.authority,
        "owner admission authority",
        MAX_NESTED_AUTHORITY_BYTES,
    )?;
    let shard_bytes = decode_nested_hex(
        &wire.expected_unowned_shard,
        "owner admission expected shard",
        MAX_NESTED_LOGICAL_SHARD_BYTES,
    )?;
    let previous_claim = wire
        .expected_previous_claim
        .map(|encoded| {
            let bytes = decode_nested_hex(
                &encoded,
                "owner admission previous claim",
                MAX_OWNER_ADMISSION_CLAIM_BYTES,
            )?;
            decode_owner_admission_claim(&bytes)
        })
        .transpose()?;
    let admission = OwnerServingAdmission::stable(
        decode_root_placement(&placement_bytes)?,
        decode_metadata_authority_record(&authority_bytes)?,
    )?;
    let intent = OwnerAdmissionIntentV1::from_durable_parts(
        OwnerAdmissionKindV1::try_from(wire.kind)?,
        admission,
        decode_logical_shard_record(&shard_bytes)?,
        previous_claim,
        NodeId::new(wire.owner).map_err(|error| ControlError::Codec(error.to_string()))?,
        OwnerIncarnationId::from_bytes(decode_fixed_id(
            &wire.owner_incarnation_id,
            "owner admission incarnation id",
        )?),
        wire.endpoint,
        OwnerRuntimeReservationDigest::from_bytes(decode_fixed_id(
            &wire.reservation_digest,
            "owner runtime reservation digest",
        )?)?,
        OwnerEpoch::new(wire.planned_epoch)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        OwnerAdmissionIntentDigestV1::from_bytes(decode_fixed_id(
            &wire.intent_digest,
            "owner admission intent digest",
        )?)?,
    )?;
    if encode_owner_admission_intent(&intent)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "owner admission intent input is not canonical".to_owned(),
        ));
    }
    Ok(intent)
}

/// Encode one exact durable owner-admission plan body.
pub fn encode_planned_owner_admission(
    plan: &PlannedOwnerAdmissionV1,
) -> Result<Vec<u8>, ControlError> {
    let recomputed = compute_owner_admission_plan_digest(plan.intent(), plan.lease())?;
    if recomputed != plan.digest() {
        return Err(ControlError::InvalidRecord(
            "owner admission plan digest is not canonical".to_owned(),
        ));
    }
    let digest_wire = planned_owner_admission_digest_wire(plan.intent(), plan.lease())?;
    let bytes = serde_json::to_vec(&PlannedOwnerAdmissionWire {
        version: digest_wire.version,
        intent: digest_wire.intent,
        lease: digest_wire.lease,
        plan_digest: encode_fixed_id(plan.digest().as_bytes()),
    })
    .map_err(codec_error)?;
    require_max_wire_size(
        "owner admission plan",
        &bytes,
        MAX_OWNER_ADMISSION_PLAN_BYTES,
    )?;
    Ok(bytes)
}

/// Decode and recompute one strict canonical v1 owner-admission plan body.
pub fn decode_planned_owner_admission(
    bytes: &[u8],
) -> Result<PlannedOwnerAdmissionV1, ControlError> {
    require_max_wire_size(
        "owner admission plan",
        bytes,
        MAX_OWNER_ADMISSION_PLAN_BYTES,
    )?;
    require_owner_admission_version(bytes, "owner admission plan")?;
    let wire: PlannedOwnerAdmissionWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let intent_bytes = decode_nested_hex(
        &wire.intent,
        "owner admission plan intent",
        MAX_OWNER_ADMISSION_INTENT_BYTES,
    )?;
    let plan = PlannedOwnerAdmissionV1::from_durable_parts(
        decode_owner_admission_intent(&intent_bytes)?,
        decode_owner_admission_lease(wire.lease)?,
        OwnerAdmissionPlanDigestV1::from_bytes(decode_fixed_id(
            &wire.plan_digest,
            "owner admission plan digest",
        )?)?,
    )?;
    if encode_planned_owner_admission(&plan)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "owner admission plan input is not canonical".to_owned(),
        ));
    }
    Ok(plan)
}

/// Encode one permanent owner-admission claim.
pub fn encode_owner_admission_claim(
    claim: &OwnerAdmissionClaimV1,
) -> Result<Vec<u8>, ControlError> {
    claim.validate()?;
    let identity = claim.identity();
    let (phase, lease, plan_digest, termination_reason, abort_reason, rejection_reason) =
        match claim.phase() {
            OwnerAdmissionClaimPhaseV1::Prepared { lease, plan_digest } => (
                1,
                Some(encode_owner_admission_lease(lease)),
                Some(encode_fixed_id(plan_digest.as_bytes())),
                None,
                None,
                None,
            ),
            OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest } => (
                2,
                Some(encode_owner_admission_lease(lease)),
                Some(encode_fixed_id(plan_digest.as_bytes())),
                None,
                None,
                None,
            ),
            OwnerAdmissionClaimPhaseV1::Terminated {
                lease,
                plan_digest,
                reason,
            } => (
                3,
                Some(encode_owner_admission_lease(lease)),
                Some(encode_fixed_id(plan_digest.as_bytes())),
                Some(encode_termination_reason(reason)),
                None,
                None,
            ),
            OwnerAdmissionClaimPhaseV1::Aborted {
                lease,
                plan_digest,
                reason,
            } => (
                4,
                Some(encode_owner_admission_lease(lease)),
                Some(encode_fixed_id(plan_digest.as_bytes())),
                None,
                Some((*reason).into()),
                None,
            ),
            OwnerAdmissionClaimPhaseV1::Rejected { reason } => {
                (5, None, None, None, None, Some((*reason).into()))
            }
        };
    let bytes = serde_json::to_vec(&OwnerAdmissionClaimWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(identity.logical_shard_id().as_bytes()),
        owner_incarnation_id: encode_fixed_id(identity.owner_incarnation_id().as_bytes()),
        intent_digest: encode_fixed_id(identity.intent_digest().as_bytes()),
        reservation_digest: encode_fixed_id(identity.reservation_digest().as_bytes()),
        planned_epoch: identity.planned_epoch().get(),
        phase,
        lease,
        plan_digest,
        termination_reason,
        abort_reason,
        rejection_reason,
    })
    .map_err(codec_error)?;
    require_max_wire_size(
        "owner admission claim",
        &bytes,
        MAX_OWNER_ADMISSION_CLAIM_BYTES,
    )?;
    Ok(bytes)
}

/// Decode one strict canonical v1 permanent owner-admission claim.
pub fn decode_owner_admission_claim(bytes: &[u8]) -> Result<OwnerAdmissionClaimV1, ControlError> {
    require_max_wire_size(
        "owner admission claim",
        bytes,
        MAX_OWNER_ADMISSION_CLAIM_BYTES,
    )?;
    require_owner_admission_version(bytes, "owner admission claim")?;
    let wire: OwnerAdmissionClaimWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let identity = OwnerAdmissionClaimIdentityV1::from_durable_parts(
        LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "owner admission claim logical shard id",
        )?),
        OwnerIncarnationId::from_bytes(decode_fixed_id(
            &wire.owner_incarnation_id,
            "owner admission claim incarnation id",
        )?),
        OwnerAdmissionIntentDigestV1::from_bytes(decode_fixed_id(
            &wire.intent_digest,
            "owner admission claim intent digest",
        )?)?,
        OwnerRuntimeReservationDigest::from_bytes(decode_fixed_id(
            &wire.reservation_digest,
            "owner admission claim reservation digest",
        )?)?,
        OwnerEpoch::new(wire.planned_epoch)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
    )?;
    let phase = decode_claim_phase(
        wire.phase,
        wire.lease,
        wire.plan_digest,
        wire.termination_reason,
        wire.abort_reason,
        wire.rejection_reason,
    )?;
    let claim = OwnerAdmissionClaimV1::from_durable_parts(identity, phase)?;
    if encode_owner_admission_claim(&claim)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "owner admission claim input is not canonical".to_owned(),
        ));
    }
    Ok(claim)
}

/// Encode the exact future lease-attached sentinel for a durable plan.
pub fn encode_owner_admission_plan_sentinel(
    sentinel: &OwnerAdmissionPlanSentinelV1,
) -> Result<Vec<u8>, ControlError> {
    let bytes = serde_json::to_vec(&OwnerAdmissionPlanSentinelWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(sentinel.logical_shard_id().as_bytes()),
        owner_incarnation_id: encode_fixed_id(sentinel.owner_incarnation_id().as_bytes()),
        lease_id: sentinel.lease_id(),
        plan_digest: encode_fixed_id(sentinel.plan_digest().as_bytes()),
    })
    .map_err(codec_error)?;
    require_max_wire_size(
        "owner admission plan sentinel",
        &bytes,
        MAX_OWNER_ADMISSION_SENTINEL_BYTES,
    )?;
    Ok(bytes)
}

/// Decode one strict canonical v1 plan sentinel.
pub fn decode_owner_admission_plan_sentinel(
    bytes: &[u8],
) -> Result<OwnerAdmissionPlanSentinelV1, ControlError> {
    require_max_wire_size(
        "owner admission plan sentinel",
        bytes,
        MAX_OWNER_ADMISSION_SENTINEL_BYTES,
    )?;
    require_owner_admission_version(bytes, "owner admission plan sentinel")?;
    let wire: OwnerAdmissionPlanSentinelWire =
        serde_json::from_slice(bytes).map_err(codec_error)?;
    let sentinel = OwnerAdmissionPlanSentinelV1::from_durable_parts(
        LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "owner admission sentinel logical shard id",
        )?),
        OwnerIncarnationId::from_bytes(decode_fixed_id(
            &wire.owner_incarnation_id,
            "owner admission sentinel incarnation id",
        )?),
        wire.lease_id,
        OwnerAdmissionPlanDigestV1::from_bytes(decode_fixed_id(
            &wire.plan_digest,
            "owner admission sentinel plan digest",
        )?)?,
    )?;
    if encode_owner_admission_plan_sentinel(&sentinel)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "owner admission plan sentinel input is not canonical".to_owned(),
        ));
    }
    Ok(sentinel)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn compute_owner_admission_intent_digest(
    kind: OwnerAdmissionKindV1,
    admission: &OwnerServingAdmission,
    expected_unowned_shard: &LogicalShardRecord,
    expected_previous_claim: Option<&OwnerAdmissionClaimV1>,
    owner: &NodeId,
    owner_incarnation_id: OwnerIncarnationId,
    endpoint: &str,
    planned_epoch: OwnerEpoch,
    reservation_digest: OwnerRuntimeReservationDigest,
) -> Result<OwnerAdmissionIntentDigestV1, ControlError> {
    let wire = owner_admission_intent_digest_wire(
        kind,
        admission,
        expected_unowned_shard,
        expected_previous_claim,
        owner,
        owner_incarnation_id,
        endpoint,
        planned_epoch,
        reservation_digest,
    )?;
    let preimage = serde_json::to_vec(&wire).map_err(codec_error)?;
    OwnerAdmissionIntentDigestV1::from_bytes(domain_separated_digest(
        OWNER_ADMISSION_INTENT_DIGEST_DOMAIN,
        &preimage,
    ))
}

pub(crate) fn compute_owner_admission_plan_digest(
    intent: &OwnerAdmissionIntentV1,
    lease: &LogicalShardLease,
) -> Result<OwnerAdmissionPlanDigestV1, ControlError> {
    let wire = planned_owner_admission_digest_wire(intent, lease)?;
    let preimage = serde_json::to_vec(&wire).map_err(codec_error)?;
    OwnerAdmissionPlanDigestV1::from_bytes(domain_separated_digest(
        OWNER_ADMISSION_PLAN_DIGEST_DOMAIN,
        &preimage,
    ))
}

pub(crate) fn compute_owner_admission_record_digest(
    record: &LogicalShardRecord,
) -> Result<OwnerAdmissionRecordDigestV1, ControlError> {
    let preimage = encode_logical_shard_record(record)?;
    OwnerAdmissionRecordDigestV1::from_canonical_bytes(domain_separated_digest(
        OWNER_ADMISSION_RECORD_DIGEST_DOMAIN,
        &preimage,
    ))
}

pub(crate) fn compute_owner_admission_claim_digest(
    claim: &OwnerAdmissionClaimV1,
) -> Result<OwnerAdmissionClaimDigestV1, ControlError> {
    let preimage = encode_owner_admission_claim(claim)?;
    OwnerAdmissionClaimDigestV1::from_canonical_bytes(domain_separated_digest(
        OWNER_ADMISSION_CLAIM_DIGEST_DOMAIN,
        &preimage,
    ))
}

pub(crate) fn compute_owner_session_binding_digest(
    session: &LogicalShardLease,
) -> Result<OwnerSessionBindingDigestV1, ControlError> {
    let preimage = serde_json::to_vec(&OwnerSessionBindingDigestWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        session: encode_owner_admission_lease(session),
    })
    .map_err(codec_error)?;
    OwnerSessionBindingDigestV1::from_canonical_bytes(domain_separated_digest(
        OWNER_SESSION_BINDING_DIGEST_DOMAIN,
        &preimage,
    ))
}

pub(crate) fn compute_owner_serving_publication_digest(
    plan: &PlannedOwnerAdmissionV1,
    source: &LogicalShardRecord,
    publication: &RecoveryPublication,
    target: &LogicalShardRecord,
) -> Result<OwnerServingPublicationDigestV1, ControlError> {
    let plan = encode_planned_owner_admission(plan)?;
    let source = encode_logical_shard_record(source)?;
    let target = encode_logical_shard_record(target)?;
    let preimage = serde_json::to_vec(&OwnerServingPublicationDigestWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        plan: encode_variable_hex(&plan),
        source: encode_variable_hex(&source),
        publication: RecoveryPublicationDigestWire {
            checkpoint: publication.checkpoint.clone().map(Into::into),
            log: publication.log.clone().map(Into::into),
            durable_lsn: publication.durable_lsn,
        },
        target: encode_variable_hex(&target),
    })
    .map_err(codec_error)?;
    OwnerServingPublicationDigestV1::from_canonical_bytes(domain_separated_digest(
        OWNER_SERVING_PUBLICATION_DIGEST_DOMAIN,
        &preimage,
    ))
}

pub(crate) fn compute_owner_session_renewal_target_digest(
    plan: &PlannedOwnerAdmissionV1,
    claim: &OwnerAdmissionClaimV1,
    session: &LogicalShardLease,
) -> Result<OwnerSessionRenewalTargetDigestV1, ControlError> {
    let plan = encode_planned_owner_admission(plan)?;
    let claim = encode_owner_admission_claim(claim)?;
    let preimage = serde_json::to_vec(&OwnerSessionRenewalTargetDigestWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        plan: encode_variable_hex(&plan),
        claim: encode_variable_hex(&claim),
        session: encode_owner_admission_lease(session),
    })
    .map_err(codec_error)?;
    OwnerSessionRenewalTargetDigestV1::from_canonical_bytes(domain_separated_digest(
        OWNER_SESSION_RENEWAL_TARGET_DIGEST_DOMAIN,
        &preimage,
    ))
}

#[allow(clippy::too_many_arguments)]
fn owner_admission_intent_digest_wire(
    kind: OwnerAdmissionKindV1,
    admission: &OwnerServingAdmission,
    expected_unowned_shard: &LogicalShardRecord,
    expected_previous_claim: Option<&OwnerAdmissionClaimV1>,
    owner: &NodeId,
    owner_incarnation_id: OwnerIncarnationId,
    endpoint: &str,
    planned_epoch: OwnerEpoch,
    reservation_digest: OwnerRuntimeReservationDigest,
) -> Result<OwnerAdmissionIntentDigestWire, ControlError> {
    let placement = encode_root_placement(admission.placement())?;
    require_max_wire_size(
        "nested owner admission placement",
        &placement,
        MAX_NESTED_ROOT_PLACEMENT_BYTES,
    )?;
    let authority = encode_metadata_authority_record(admission.authority())?;
    require_max_wire_size(
        "nested owner admission authority",
        &authority,
        MAX_NESTED_AUTHORITY_BYTES,
    )?;
    let expected_unowned_shard = encode_logical_shard_record(expected_unowned_shard)?;
    require_max_wire_size(
        "nested owner admission expected shard",
        &expected_unowned_shard,
        MAX_NESTED_LOGICAL_SHARD_BYTES,
    )?;
    let expected_previous_claim = expected_previous_claim
        .map(encode_owner_admission_claim)
        .transpose()?;
    Ok(OwnerAdmissionIntentDigestWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        kind: kind.into(),
        placement: encode_variable_hex(&placement),
        authority: encode_variable_hex(&authority),
        expected_unowned_shard: encode_variable_hex(&expected_unowned_shard),
        expected_previous_claim: expected_previous_claim.as_deref().map(encode_variable_hex),
        owner: owner.as_str().to_owned(),
        owner_incarnation_id: encode_fixed_id(owner_incarnation_id.as_bytes()),
        endpoint: endpoint.to_owned(),
        planned_epoch: planned_epoch.get(),
        reservation_digest: encode_fixed_id(reservation_digest.as_bytes()),
    })
}

fn planned_owner_admission_digest_wire(
    intent: &OwnerAdmissionIntentV1,
    lease: &LogicalShardLease,
) -> Result<PlannedOwnerAdmissionDigestWire, ControlError> {
    let intent = encode_owner_admission_intent(intent)?;
    require_max_wire_size(
        "nested owner admission plan intent",
        &intent,
        MAX_OWNER_ADMISSION_INTENT_BYTES,
    )?;
    Ok(PlannedOwnerAdmissionDigestWire {
        version: OWNER_ADMISSION_CODEC_VERSION,
        intent: encode_variable_hex(&intent),
        lease: encode_owner_admission_lease(lease),
    })
}

fn encode_owner_admission_lease(lease: &LogicalShardLease) -> OwnerAdmissionLeaseWire {
    OwnerAdmissionLeaseWire {
        logical_shard_id: encode_fixed_id(lease.logical_shard_id.as_bytes()),
        owner: lease.owner.as_str().to_owned(),
        owner_epoch: lease.owner_epoch.get(),
        owner_incarnation_id: encode_fixed_id(lease.owner_incarnation_id.as_bytes()),
        lease_id: lease.lease_id,
        authority_id: encode_fixed_id(lease.authority.authority_id.as_bytes()),
        authority_generation: lease.authority.authority_generation.get(),
    }
}

fn decode_owner_admission_lease(
    wire: OwnerAdmissionLeaseWire,
) -> Result<LogicalShardLease, ControlError> {
    let logical_shard_id = LogicalShardId::from_bytes(decode_fixed_id(
        &wire.logical_shard_id,
        "planned owner lease logical shard id",
    )?);
    Ok(LogicalShardLease {
        logical_shard_id,
        owner: NodeId::new(wire.owner).map_err(|error| ControlError::Codec(error.to_string()))?,
        owner_epoch: OwnerEpoch::new(wire.owner_epoch)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        owner_incarnation_id: OwnerIncarnationId::from_bytes(decode_fixed_id(
            &wire.owner_incarnation_id,
            "planned owner lease incarnation id",
        )?),
        lease_id: wire.lease_id,
        authority: MetadataAuthorityFence {
            logical_shard_id,
            authority_id: MetadataAuthorityId::from_bytes(decode_fixed_id(
                &wire.authority_id,
                "planned owner lease authority id",
            )?),
            authority_generation: MetadataAuthorityGeneration::new(wire.authority_generation)
                .map_err(|error| ControlError::Codec(error.to_string()))?,
        },
    })
}

fn encode_termination_reason(
    reason: &OwnerAdmissionTerminationReasonV1,
) -> OwnerAdmissionTerminationReasonWire {
    match reason {
        OwnerAdmissionTerminationReasonV1::Released => OwnerAdmissionTerminationReasonWire {
            kind: 1,
            lease_expiry_evidence_digest: None,
            migration_id: None,
        },
        OwnerAdmissionTerminationReasonV1::LeaseExpired { evidence_digest } => {
            OwnerAdmissionTerminationReasonWire {
                kind: 2,
                lease_expiry_evidence_digest: Some(encode_fixed_id(evidence_digest.as_bytes())),
                migration_id: None,
            }
        }
        OwnerAdmissionTerminationReasonV1::AuthorityCutover { migration_id } => {
            OwnerAdmissionTerminationReasonWire {
                kind: 3,
                lease_expiry_evidence_digest: None,
                migration_id: Some(encode_fixed_id(migration_id.as_bytes())),
            }
        }
    }
}

fn decode_claim_phase(
    phase: u8,
    lease: Option<OwnerAdmissionLeaseWire>,
    plan_digest: Option<String>,
    termination_reason: Option<OwnerAdmissionTerminationReasonWire>,
    abort_reason: Option<u8>,
    rejection_reason: Option<u8>,
) -> Result<OwnerAdmissionClaimPhaseV1, ControlError> {
    match (
        phase,
        lease,
        plan_digest,
        termination_reason,
        abort_reason,
        rejection_reason,
    ) {
        (1, Some(lease), Some(plan_digest), None, None, None) => {
            Ok(OwnerAdmissionClaimPhaseV1::Prepared {
                lease: decode_owner_admission_lease(lease)?,
                plan_digest: OwnerAdmissionPlanDigestV1::from_bytes(decode_fixed_id(
                    &plan_digest,
                    "prepared claim plan digest",
                )?)?,
            })
        }
        (2, Some(lease), Some(plan_digest), None, None, None) => {
            Ok(OwnerAdmissionClaimPhaseV1::Committed {
                lease: decode_owner_admission_lease(lease)?,
                plan_digest: OwnerAdmissionPlanDigestV1::from_bytes(decode_fixed_id(
                    &plan_digest,
                    "committed claim plan digest",
                )?)?,
            })
        }
        (3, Some(lease), Some(plan_digest), Some(reason), None, None) => {
            Ok(OwnerAdmissionClaimPhaseV1::Terminated {
                lease: decode_owner_admission_lease(lease)?,
                plan_digest: OwnerAdmissionPlanDigestV1::from_bytes(decode_fixed_id(
                    &plan_digest,
                    "terminated claim plan digest",
                )?)?,
                reason: decode_termination_reason(reason)?,
            })
        }
        (4, Some(lease), Some(plan_digest), None, Some(reason), None) => {
            Ok(OwnerAdmissionClaimPhaseV1::Aborted {
                lease: decode_owner_admission_lease(lease)?,
                plan_digest: OwnerAdmissionPlanDigestV1::from_bytes(decode_fixed_id(
                    &plan_digest,
                    "aborted claim plan digest",
                )?)?,
                reason: OwnerAdmissionAbortReasonV1::try_from(reason)?,
            })
        }
        (5, None, None, None, None, Some(reason)) => Ok(OwnerAdmissionClaimPhaseV1::Rejected {
            reason: OwnerAdmissionRejectionReasonV1::try_from(reason)?,
        }),
        (1..=5, _, _, _, _, _) => Err(ControlError::Codec(
            "owner admission claim phase fields form an illegal combination".to_owned(),
        )),
        (phase, _, _, _, _, _) => Err(ControlError::Codec(format!(
            "unsupported owner admission claim phase {phase}"
        ))),
    }
}

fn decode_termination_reason(
    wire: OwnerAdmissionTerminationReasonWire,
) -> Result<OwnerAdmissionTerminationReasonV1, ControlError> {
    match (
        wire.kind,
        wire.lease_expiry_evidence_digest,
        wire.migration_id,
    ) {
        (1, None, None) => Ok(OwnerAdmissionTerminationReasonV1::Released),
        (2, Some(evidence), None) => Ok(OwnerAdmissionTerminationReasonV1::LeaseExpired {
            evidence_digest: OwnerLeaseExpiryEvidenceDigest::from_bytes(decode_fixed_id(
                &evidence,
                "owner lease expiry evidence digest",
            )?)?,
        }),
        (3, None, Some(migration_id)) => Ok(OwnerAdmissionTerminationReasonV1::AuthorityCutover {
            migration_id: OperationId::from_bytes(decode_fixed_id(
                &migration_id,
                "owner authority cutover migration id",
            )?),
        }),
        (1..=3, _, _) => Err(ControlError::Codec(
            "owner admission termination reason fields form an illegal combination".to_owned(),
        )),
        (kind, _, _) => Err(ControlError::Codec(format!(
            "unsupported owner admission termination reason {kind}"
        ))),
    }
}

fn require_owner_admission_version(bytes: &[u8], type_name: &str) -> Result<(), ControlError> {
    let probe: CodecVersionProbe = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version(type_name, probe.version, OWNER_ADMISSION_CODEC_VERSION)
}

fn require_max_wire_size(
    type_name: &str,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<(), ControlError> {
    if bytes.len() > max_bytes {
        return Err(ControlError::Codec(format!(
            "{type_name} contains {} bytes, maximum is {max_bytes}",
            bytes.len()
        )));
    }
    Ok(())
}

fn domain_separated_digest(domain: &[u8], preimage: &[u8]) -> [u8; SHA256_BYTES] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update((preimage.len() as u64).to_be_bytes());
    hasher.update(preimage);
    hasher.finalize().into()
}

fn encode_variable_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_nested_hex(
    encoded: &str,
    type_name: &str,
    max_decoded_bytes: usize,
) -> Result<Vec<u8>, ControlError> {
    if !encoded.len().is_multiple_of(2) || encoded.len() / 2 > max_decoded_bytes {
        return Err(ControlError::Codec(format!(
            "{type_name} must be even-length lowercase hex encoding at most {max_decoded_bytes} bytes"
        )));
    }
    let mut decoded = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = decode_hex_digit(pair[0]).ok_or_else(|| {
            ControlError::Codec(format!("{type_name} contains a non-lowercase-hex digit"))
        })?;
        let low = decode_hex_digit(pair[1]).ok_or_else(|| {
            ControlError::Codec(format!("{type_name} contains a non-lowercase-hex digit"))
        })?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

#[cfg(any(feature = "etcd", test))]
pub(crate) fn encode_owner_session(lease: &LogicalShardLease) -> Result<Vec<u8>, ControlError> {
    validate_owner_incarnation_id(lease.owner_incarnation_id)?;
    if lease.lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "owner session lease id must be non-zero".to_owned(),
        ));
    }
    if lease.authority.logical_shard_id != lease.logical_shard_id
        || lease
            .authority
            .authority_id
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
    {
        return Err(ControlError::InvalidRecord(
            "owner session authority fence is invalid".to_owned(),
        ));
    }
    serde_json::to_vec(&OwnerSessionWire {
        version: OWNER_SESSION_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(lease.logical_shard_id.as_bytes()),
        owner: lease.owner.as_str().to_owned(),
        owner_epoch: lease.owner_epoch.get(),
        owner_incarnation_id: encode_fixed_id(lease.owner_incarnation_id.as_bytes()),
        lease_id: lease.lease_id,
        authority_id: encode_fixed_id(lease.authority.authority_id.as_bytes()),
        authority_generation: lease.authority.authority_generation.get(),
    })
    .map_err(codec_error)
}

#[cfg(any(feature = "etcd", test))]
pub(crate) fn decode_owner_session(bytes: &[u8]) -> Result<LogicalShardLease, ControlError> {
    let version: CodecVersionProbe = serde_json::from_slice(bytes).map_err(codec_error)?;
    if version.version < OWNER_SESSION_CODEC_VERSION {
        return Err(ControlError::OwnerSessionCodecUpgradeRequired {
            stored_version: version.version,
            required_version: OWNER_SESSION_CODEC_VERSION,
        });
    }
    require_version(
        "owner session",
        version.version,
        OWNER_SESSION_CODEC_VERSION,
    )?;
    let wire: OwnerSessionWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    if wire.lease_id == 0 {
        return Err(ControlError::Codec(
            "owner session lease id must be non-zero".to_owned(),
        ));
    }
    let lease = LogicalShardLease {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "logical shard id",
        )?),
        owner: NodeId::new(wire.owner).map_err(|err| ControlError::Codec(err.to_string()))?,
        owner_epoch: OwnerEpoch::new(wire.owner_epoch)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        owner_incarnation_id: OwnerIncarnationId::from_bytes(decode_fixed_id(
            &wire.owner_incarnation_id,
            "owner incarnation id",
        )?),
        lease_id: wire.lease_id,
        authority: MetadataAuthorityFence {
            logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
                &wire.logical_shard_id,
                "logical shard id",
            )?),
            authority_id: MetadataAuthorityId::from_bytes(decode_fixed_id(
                &wire.authority_id,
                "metadata authority id",
            )?),
            authority_generation: MetadataAuthorityGeneration::new(wire.authority_generation)
                .map_err(|err| ControlError::Codec(err.to_string()))?,
        },
    };
    validate_owner_incarnation_id(lease.owner_incarnation_id)?;
    if encode_owner_session(&lease)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "owner session input is not canonical".to_owned(),
        ));
    }
    Ok(lease)
}

fn encode_authority_binding(binding: &MetadataAuthorityBinding) -> MetadataAuthorityBindingWire {
    MetadataAuthorityBindingWire {
        authority_id: encode_fixed_id(binding.authority_id.as_bytes()),
        provider_profile_id: binding.provider_profile_id.as_str().to_owned(),
        profile_fingerprint: encode_fixed_id(&binding.profile_fingerprint),
        consistency_domain_id: encode_fixed_id(binding.consistency_domain_id.as_bytes()),
        contract_digest: encode_fixed_id(binding.contract_digest.as_bytes()),
    }
}

fn decode_authority_binding(
    wire: MetadataAuthorityBindingWire,
) -> Result<MetadataAuthorityBinding, ControlError> {
    Ok(MetadataAuthorityBinding {
        authority_id: MetadataAuthorityId::from_bytes(decode_fixed_id(
            &wire.authority_id,
            "metadata authority id",
        )?),
        provider_profile_id: MetadataProviderProfileId::new(wire.provider_profile_id)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        profile_fingerprint: decode_fixed_id(
            &wire.profile_fingerprint,
            "provider profile fingerprint",
        )?,
        consistency_domain_id: ConsistencyDomainId::from_bytes(decode_fixed_id(
            &wire.consistency_domain_id,
            "consistency domain id",
        )?),
        contract_digest: MetadataContractDigest::from_bytes(decode_fixed_id(
            &wire.contract_digest,
            "metadata contract digest",
        )?),
    })
}

fn encode_metadata_migration(migration: &MetadataMigration) -> MetadataMigrationWire {
    MetadataMigrationWire {
        migration_id: encode_fixed_id(migration.migration_id.as_bytes()),
        source: encode_authority_binding(&migration.source),
        target: encode_authority_binding(&migration.target),
        phase: migration.phase.into(),
        source_frontier: migration.source_frontier.map(encode_recovery_frontier),
        target_frontier: migration.target_frontier.map(encode_recovery_frontier),
        cutover_frontier: migration.cutover_frontier.map(encode_recovery_frontier),
        source_quiesce_receipt: migration
            .source_quiesce_receipt
            .map(encode_source_quiesce_receipt),
        target_activation_token: migration
            .target_activation_token
            .map(encode_target_activation_token),
    }
}

fn decode_metadata_migration(
    wire: MetadataMigrationWire,
) -> Result<MetadataMigration, ControlError> {
    Ok(MetadataMigration {
        migration_id: OperationId::from_bytes(decode_fixed_id(
            &wire.migration_id,
            "metadata migration id",
        )?),
        source: decode_authority_binding(wire.source)?,
        target: decode_authority_binding(wire.target)?,
        phase: MetadataMigrationPhase::try_from(wire.phase)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        source_frontier: wire
            .source_frontier
            .map(decode_recovery_frontier)
            .transpose()?,
        target_frontier: wire
            .target_frontier
            .map(decode_recovery_frontier)
            .transpose()?,
        cutover_frontier: wire
            .cutover_frontier
            .map(decode_recovery_frontier)
            .transpose()?,
        source_quiesce_receipt: wire
            .source_quiesce_receipt
            .map(decode_source_quiesce_receipt)
            .transpose()?,
        target_activation_token: wire
            .target_activation_token
            .map(decode_target_activation_token)
            .transpose()?,
    })
}

fn encode_recovery_frontier(frontier: MetadataRecoveryFrontier) -> MetadataRecoveryFrontierWire {
    MetadataRecoveryFrontierWire {
        recovery_lsn: frontier.recovery_lsn,
        chain_digest: encode_fixed_id(&frontier.chain_digest),
        commit_version: frontier.commit_version.get(),
        state_digest: encode_fixed_id(&frontier.state_digest),
    }
}

fn decode_recovery_frontier(
    wire: MetadataRecoveryFrontierWire,
) -> Result<MetadataRecoveryFrontier, ControlError> {
    Ok(MetadataRecoveryFrontier {
        recovery_lsn: wire.recovery_lsn,
        chain_digest: decode_fixed_id(&wire.chain_digest, "recovery chain digest")?,
        commit_version: CommitVersion::new(wire.commit_version)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        state_digest: decode_fixed_id(&wire.state_digest, "recovery state digest")?,
    })
}

fn encode_source_quiesce_receipt(receipt: SourceQuiesceReceipt) -> SourceQuiesceReceiptWire {
    SourceQuiesceReceiptWire {
        logical_shard_id: encode_fixed_id(receipt.logical_shard_id.as_bytes()),
        migration_id: encode_fixed_id(receipt.migration_id.as_bytes()),
        source_authority_id: encode_fixed_id(receipt.source_authority_id.as_bytes()),
        source_authority_generation: receipt.source_authority_generation.get(),
        owner_epoch: receipt.owner_epoch.get(),
        frontier: encode_recovery_frontier(receipt.frontier),
        contract_digest: encode_fixed_id(receipt.contract_digest.as_bytes()),
    }
}

fn decode_source_quiesce_receipt(
    wire: SourceQuiesceReceiptWire,
) -> Result<SourceQuiesceReceipt, ControlError> {
    Ok(SourceQuiesceReceipt {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "source receipt logical shard id",
        )?),
        migration_id: OperationId::from_bytes(decode_fixed_id(
            &wire.migration_id,
            "source receipt migration id",
        )?),
        source_authority_id: MetadataAuthorityId::from_bytes(decode_fixed_id(
            &wire.source_authority_id,
            "source receipt authority id",
        )?),
        source_authority_generation: MetadataAuthorityGeneration::new(
            wire.source_authority_generation,
        )
        .map_err(|error| ControlError::Codec(error.to_string()))?,
        owner_epoch: OwnerEpoch::new(wire.owner_epoch)
            .map_err(|error| ControlError::Codec(error.to_string()))?,
        frontier: decode_recovery_frontier(wire.frontier)?,
        contract_digest: MetadataContractDigest::from_bytes(decode_fixed_id(
            &wire.contract_digest,
            "source receipt metadata contract digest",
        )?),
    })
}

fn encode_target_activation_token(token: TargetActivationToken) -> TargetActivationTokenWire {
    TargetActivationTokenWire {
        logical_shard_id: encode_fixed_id(token.logical_shard_id.as_bytes()),
        migration_id: encode_fixed_id(token.migration_id.as_bytes()),
        source_authority_id: encode_fixed_id(token.source_authority_id.as_bytes()),
        source_authority_generation: token.source_authority_generation.get(),
        target_authority_id: encode_fixed_id(token.target_authority_id.as_bytes()),
        target_authority_generation: token.target_authority_generation.get(),
        frontier: encode_recovery_frontier(token.frontier),
        contract_digest: encode_fixed_id(token.contract_digest.as_bytes()),
        source_receipt_digest: encode_fixed_id(&token.source_receipt_digest),
    }
}

fn decode_target_activation_token(
    wire: TargetActivationTokenWire,
) -> Result<TargetActivationToken, ControlError> {
    Ok(TargetActivationToken {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "target token logical shard id",
        )?),
        migration_id: OperationId::from_bytes(decode_fixed_id(
            &wire.migration_id,
            "target token migration id",
        )?),
        source_authority_id: MetadataAuthorityId::from_bytes(decode_fixed_id(
            &wire.source_authority_id,
            "target token source authority id",
        )?),
        source_authority_generation: MetadataAuthorityGeneration::new(
            wire.source_authority_generation,
        )
        .map_err(|error| ControlError::Codec(error.to_string()))?,
        target_authority_id: MetadataAuthorityId::from_bytes(decode_fixed_id(
            &wire.target_authority_id,
            "target token authority id",
        )?),
        target_authority_generation: MetadataAuthorityGeneration::new(
            wire.target_authority_generation,
        )
        .map_err(|error| ControlError::Codec(error.to_string()))?,
        frontier: decode_recovery_frontier(wire.frontier)?,
        contract_digest: MetadataContractDigest::from_bytes(decode_fixed_id(
            &wire.contract_digest,
            "target token metadata contract digest",
        )?),
        source_receipt_digest: decode_fixed_id(
            &wire.source_receipt_digest,
            "source receipt digest",
        )?,
    })
}

pub(crate) fn encode_fixed_id<const N: usize>(bytes: &[u8; N]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(N * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_fixed_id<const N: usize>(
    encoded: &str,
    type_name: &str,
) -> Result<[u8; N], ControlError> {
    if encoded.len() != N * 2 {
        return Err(ControlError::Codec(format!(
            "{type_name} must contain exactly {} lowercase hex digits",
            N * 2
        )));
    }
    let mut decoded = [0_u8; N];
    let bytes = encoded.as_bytes();
    for (index, output) in decoded.iter_mut().enumerate() {
        let high = decode_hex_digit(bytes[index * 2]).ok_or_else(|| {
            ControlError::Codec(format!("{type_name} contains a non-lowercase-hex digit"))
        })?;
        let low = decode_hex_digit(bytes[index * 2 + 1]).ok_or_else(|| {
            ControlError::Codec(format!("{type_name} contains a non-lowercase-hex digit"))
        })?;
        *output = (high << 4) | low;
    }
    Ok(decoded)
}

fn decode_hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn require_version(type_name: &str, actual: u8, expected: u8) -> Result<(), ControlError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ControlError::Codec(format!(
            "unsupported {type_name} codec version {actual}"
        )))
    }
}

fn validate_root_placement(placement: &RootPlacement) -> Result<(), ControlError> {
    if placement.layout_profile == RootLayoutProfile::SingleShardRoot
        && placement.partition_id != RootPartitionId::SINGLE_SHARD
    {
        return Err(ControlError::InvalidRecord(
            "SingleShardRoot must use the reserved SINGLE_SHARD partition id".to_owned(),
        ));
    }
    let generation = placement.placement_generation.get();
    let valid = match placement.lifecycle {
        RootPlacementLifecycle::Provisioning => generation == 1,
        RootPlacementLifecycle::Active => generation >= 2 && generation.is_multiple_of(2),
        RootPlacementLifecycle::Draining => generation >= 3 && !generation.is_multiple_of(2),
        RootPlacementLifecycle::Retired => generation >= 2 && generation.is_multiple_of(2),
    };
    if !valid {
        return Err(ControlError::InvalidRecord(format!(
            "root placement generation {generation} is inconsistent with {:?} lifecycle",
            placement.lifecycle
        )));
    }
    Ok(())
}

fn codec_error(error: serde_json::Error) -> ControlError {
    ControlError::Codec(error.to_string())
}

impl From<CheckpointRef> for CheckpointRefWire {
    fn from(value: CheckpointRef) -> Self {
        Self {
            object_key: value.object_key,
            lsn: value.lsn,
            image_bytes: value.image_bytes,
            image_digest: value.image_digest,
            digest: value.digest,
        }
    }
}

impl From<CheckpointRefWire> for CheckpointRef {
    fn from(value: CheckpointRefWire) -> Self {
        Self {
            object_key: value.object_key,
            lsn: value.lsn,
            image_bytes: value.image_bytes,
            image_digest: value.image_digest,
            digest: value.digest,
        }
    }
}

impl From<LogRef> for LogRefWire {
    fn from(value: LogRef) -> Self {
        Self {
            segments: value.segments.into_iter().map(Into::into).collect(),
            durable_lsn: value.durable_lsn,
            digest: value.digest,
        }
    }
}

impl From<LogRefWire> for LogRef {
    fn from(value: LogRefWire) -> Self {
        Self {
            segments: value.segments.into_iter().map(Into::into).collect(),
            durable_lsn: value.durable_lsn,
            digest: value.digest,
        }
    }
}

impl From<LogSegmentRef> for LogSegmentRefWire {
    fn from(value: LogSegmentRef) -> Self {
        Self {
            segment_key: value.segment_key,
            first_lsn: value.first_lsn,
            last_lsn: value.last_lsn,
            digest: value.digest,
        }
    }
}

impl From<LogSegmentRefWire> for LogSegmentRef {
    fn from(value: LogSegmentRefWire) -> Self {
        Self {
            segment_key: value.segment_key,
            first_lsn: value.first_lsn,
            last_lsn: value.last_lsn,
            digest: value.digest,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogicalShardState;

    fn root_id(value: u8) -> RootId {
        RootId::from_bytes([value; 16])
    }

    fn shard_id(value: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([value; 16])
    }

    fn incarnation(value: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([value; 16])
    }

    fn placement() -> RootPlacement {
        RootPlacement {
            root_id: root_id(1),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: shard_id(2),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        }
    }

    fn authority_binding(value: u8, profile: &str) -> MetadataAuthorityBinding {
        MetadataAuthorityBinding {
            authority_id: MetadataAuthorityId::from_bytes([value; 16]),
            provider_profile_id: MetadataProviderProfileId::new(profile).unwrap(),
            profile_fingerprint: [value; 32],
            consistency_domain_id: ConsistencyDomainId::from_bytes([value; 16]),
            contract_digest: MetadataContractDigest::from_bytes([9; 32]),
        }
    }

    fn authority() -> MetadataAuthorityRecord {
        MetadataAuthorityRecord {
            logical_shard_id: shard_id(2),
            record_revision: MetadataAuthorityRevision::new(1).unwrap(),
            authority_generation: MetadataAuthorityGeneration::new(1).unwrap(),
            active: authority_binding(1, "holt-primary"),
            migration: None,
        }
    }

    fn ready_authority() -> MetadataAuthorityRecord {
        let frontier = MetadataRecoveryFrontier {
            recovery_lsn: 17,
            chain_digest: [7; 32],
            commit_version: CommitVersion::new(11).unwrap(),
            state_digest: [8; 32],
        };
        let mut authority = authority();
        authority.record_revision = MetadataAuthorityRevision::new(6).unwrap();
        authority.migration = Some(MetadataMigration {
            migration_id: OperationId::from_bytes([3; 16]),
            source: authority.active.clone(),
            target: authority_binding(2, "fdb-primary"),
            phase: MetadataMigrationPhase::ReadyToCutover,
            source_frontier: Some(frontier),
            target_frontier: Some(frontier),
            cutover_frontier: Some(frontier),
            source_quiesce_receipt: Some(SourceQuiesceReceipt {
                logical_shard_id: authority.logical_shard_id,
                migration_id: OperationId::from_bytes([3; 16]),
                source_authority_id: authority.active.authority_id,
                source_authority_generation: authority.authority_generation,
                owner_epoch: OwnerEpoch::new(7).unwrap(),
                frontier,
                contract_digest: authority.active.contract_digest,
            }),
            target_activation_token: None,
        });
        authority
    }

    fn serving_record() -> LogicalShardRecord {
        LogicalShardRecord {
            logical_shard_id: shard_id(2),
            owner: Some(NodeId::new("node-a").unwrap()),
            owner_epoch: Some(OwnerEpoch::new(7).unwrap()),
            owner_incarnation_id: Some(incarnation(8)),
            lease_id: 42,
            state: LogicalShardState::Serving,
            endpoint: Some("10.0.0.1:7000".to_owned()),
            checkpoint: Some(CheckpointRef {
                object_key: "checkpoints/7".to_owned(),
                lsn: 128,
                image_bytes: 4096,
                image_digest: "sha256:image".to_owned(),
                digest: "state-128".to_owned(),
            }),
            log: Some(LogRef {
                segments: vec![LogSegmentRef {
                    segment_key: "logs/129-144".to_owned(),
                    first_lsn: 129,
                    last_lsn: 144,
                    digest: "state-144".to_owned(),
                }],
                durable_lsn: 144,
                digest: "state-144".to_owned(),
            }),
            durable_lsn: 144,
        }
    }

    fn owner_admission() -> OwnerServingAdmission {
        let mut placement = placement();
        placement.placement_generation = PlacementGeneration::new(2).unwrap();
        placement.lifecycle = RootPlacementLifecycle::Active;
        OwnerServingAdmission::stable(placement, authority()).unwrap()
    }

    fn reservation_digest(value: u8) -> OwnerRuntimeReservationDigest {
        OwnerRuntimeReservationDigest::from_bytes([value; 32]).unwrap()
    }

    fn fresh_owner_intent() -> OwnerAdmissionIntentV1 {
        OwnerAdmissionIntentV1::fresh(
            owner_admission(),
            LogicalShardRecord::unassigned(shard_id(2)),
            NodeId::new("node-a").unwrap(),
            incarnation(8),
            "10.0.0.1:7000".to_owned(),
            reservation_digest(6),
        )
        .unwrap()
    }

    fn fresh_owner_plan() -> PlannedOwnerAdmissionV1 {
        let intent = fresh_owner_intent();
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

    fn released_first_owner() -> LogicalShardRecord {
        LogicalShardRecord {
            logical_shard_id: shard_id(2),
            owner: None,
            owner_epoch: Some(OwnerEpoch::new(1).unwrap()),
            owner_incarnation_id: Some(incarnation(8)),
            lease_id: 0,
            state: LogicalShardState::Unassigned,
            endpoint: None,
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        }
    }

    fn terminated_first_owner() -> OwnerAdmissionClaimV1 {
        OwnerAdmissionClaimV1::prepared(&fresh_owner_plan())
            .unwrap()
            .commit()
            .unwrap()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap()
    }

    fn successor_owner_intent() -> OwnerAdmissionIntentV1 {
        OwnerAdmissionIntentV1::successor(
            owner_admission(),
            released_first_owner(),
            terminated_first_owner(),
            NodeId::new("node-b").unwrap(),
            incarnation(9),
            "10.0.0.2:7000".to_owned(),
            reservation_digest(7),
        )
        .unwrap()
    }

    #[test]
    fn strict_codecs_round_trip_final_records() {
        let placement = placement();
        assert_eq!(
            decode_root_placement(&encode_root_placement(&placement).unwrap()).unwrap(),
            placement
        );

        let record = serving_record();
        assert_eq!(
            decode_logical_shard_record(&encode_logical_shard_record(&record).unwrap()).unwrap(),
            record
        );

        let authority_record = authority();
        assert_eq!(
            decode_metadata_authority_record(
                &encode_metadata_authority_record(&authority_record).unwrap()
            )
            .unwrap(),
            authority_record
        );

        let migrating = ready_authority();
        assert_eq!(
            decode_metadata_authority_record(
                &encode_metadata_authority_record(&migrating).unwrap()
            )
            .unwrap(),
            migrating
        );
    }

    #[test]
    fn codec_golden_bytes_freeze_durable_schema() {
        assert_eq!(
            encode_root_placement(&placement()).unwrap(),
            br#"{"version":2,"root_id":"01010101010101010101010101010101","layout_profile":1,"layout_generation":1,"partition_id":"00000000000000000000000000000000","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#
        );
        assert_eq!(
            encode_logical_shard_record(&LogicalShardRecord::unassigned(shard_id(2))).unwrap(),
            br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"owner_incarnation_id":null,"lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#
        );
        let released = LogicalShardRecord {
            logical_shard_id: shard_id(2),
            owner: None,
            owner_epoch: Some(OwnerEpoch::new(7).unwrap()),
            owner_incarnation_id: Some(incarnation(8)),
            lease_id: 0,
            state: LogicalShardState::Unassigned,
            endpoint: None,
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        };
        assert_eq!(
            encode_logical_shard_record(&released).unwrap(),
            br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":7,"owner_incarnation_id":"08080808080808080808080808080808","lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#
        );
        assert_eq!(
            encode_metadata_authority_record(&authority()).unwrap(),
            br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","record_revision":1,"authority_generation":1,"active":{"authority_id":"01010101010101010101010101010101","provider_profile_id":"holt-primary","profile_fingerprint":"0101010101010101010101010101010101010101010101010101010101010101","consistency_domain_id":"01010101010101010101010101010101","contract_digest":"0909090909090909090909090909090909090909090909090909090909090909"},"migration":null}"#
        );
        assert_eq!(
            encode_metadata_authority_record(&ready_authority()).unwrap(),
            br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","record_revision":6,"authority_generation":1,"active":{"authority_id":"01010101010101010101010101010101","provider_profile_id":"holt-primary","profile_fingerprint":"0101010101010101010101010101010101010101010101010101010101010101","consistency_domain_id":"01010101010101010101010101010101","contract_digest":"0909090909090909090909090909090909090909090909090909090909090909"},"migration":{"migration_id":"03030303030303030303030303030303","source":{"authority_id":"01010101010101010101010101010101","provider_profile_id":"holt-primary","profile_fingerprint":"0101010101010101010101010101010101010101010101010101010101010101","consistency_domain_id":"01010101010101010101010101010101","contract_digest":"0909090909090909090909090909090909090909090909090909090909090909"},"target":{"authority_id":"02020202020202020202020202020202","provider_profile_id":"fdb-primary","profile_fingerprint":"0202020202020202020202020202020202020202020202020202020202020202","consistency_domain_id":"02020202020202020202020202020202","contract_digest":"0909090909090909090909090909090909090909090909090909090909090909"},"phase":5,"source_frontier":{"recovery_lsn":17,"chain_digest":"0707070707070707070707070707070707070707070707070707070707070707","commit_version":11,"state_digest":"0808080808080808080808080808080808080808080808080808080808080808"},"target_frontier":{"recovery_lsn":17,"chain_digest":"0707070707070707070707070707070707070707070707070707070707070707","commit_version":11,"state_digest":"0808080808080808080808080808080808080808080808080808080808080808"},"cutover_frontier":{"recovery_lsn":17,"chain_digest":"0707070707070707070707070707070707070707070707070707070707070707","commit_version":11,"state_digest":"0808080808080808080808080808080808080808080808080808080808080808"},"source_quiesce_receipt":{"logical_shard_id":"02020202020202020202020202020202","migration_id":"03030303030303030303030303030303","source_authority_id":"01010101010101010101010101010101","source_authority_generation":1,"owner_epoch":7,"frontier":{"recovery_lsn":17,"chain_digest":"0707070707070707070707070707070707070707070707070707070707070707","commit_version":11,"state_digest":"0808080808080808080808080808080808080808080808080808080808080808"},"contract_digest":"0909090909090909090909090909090909090909090909090909090909090909"},"target_activation_token":null}}"#
        );
    }

    #[test]
    fn codec_rejects_unknown_versions() {
        let version_one = br#"{"version":1,"root_id":"01010101010101010101010101010101","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#;
        assert!(matches!(
            decode_root_placement(version_one),
            Err(ControlError::RootPlacementCodecUpgradeRequired {
                stored_version: 1,
                required_version: 2,
            })
        ));

        let bytes = br#"{"version":99,"root_id":"01010101010101010101010101010101","layout_profile":1,"layout_generation":1,"partition_id":"00000000000000000000000000000000","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#;
        assert!(matches!(
            decode_root_placement(bytes),
            Err(ControlError::Codec(_))
        ));

        let version_one_record = br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#;
        assert!(matches!(
            decode_logical_shard_record(version_one_record),
            Err(ControlError::LogicalShardRecordCodecUpgradeRequired {
                stored_version: 1,
                required_version: 2,
            })
        ));

        let mut version_one_authority: serde_json::Value =
            serde_json::from_slice(&encode_metadata_authority_record(&authority()).unwrap())
                .unwrap();
        version_one_authority["version"] = serde_json::json!(1);
        assert!(matches!(
            decode_metadata_authority_record(&serde_json::to_vec(&version_one_authority).unwrap()),
            Err(ControlError::MetadataAuthorityCodecUpgradeRequired {
                stored_version: 1,
                required_version: 2,
            })
        ));
    }

    #[test]
    fn partitioned_layout_is_durable_even_though_admission_is_not_qualified() {
        let partitioned = RootPlacement {
            layout_profile: RootLayoutProfile::PartitionedRoot,
            layout_generation: RootLayoutGeneration::new(7).unwrap(),
            partition_id: RootPartitionId::from_bytes([3; 16]),
            ..placement()
        };
        let encoded = encode_root_placement(&partitioned).unwrap();
        assert_eq!(decode_root_placement(&encoded).unwrap(), partitioned);
    }

    #[test]
    fn codec_rejects_unknown_enum_discriminants() {
        let bytes = br#"{"version":2,"root_id":"01010101010101010101010101010101","layout_profile":1,"layout_generation":1,"partition_id":"00000000000000000000000000000000","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":99}"#;
        assert!(matches!(
            decode_root_placement(bytes),
            Err(ControlError::Codec(_))
        ));

        let bytes = br#"{"version":2,"root_id":"01010101010101010101010101010101","layout_profile":99,"layout_generation":1,"partition_id":"00000000000000000000000000000000","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#;
        assert!(matches!(
            decode_root_placement(bytes),
            Err(ControlError::Codec(_))
        ));

        let bytes = br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"owner_incarnation_id":null,"lease_id":0,"state":99,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#;
        assert!(matches!(
            decode_logical_shard_record(bytes),
            Err(ControlError::Codec(_))
        ));

        let invalid_phase = encode_metadata_authority_record(&authority())
            .unwrap()
            .into_iter()
            .collect::<Vec<_>>();
        let mut document: serde_json::Value = serde_json::from_slice(&invalid_phase).unwrap();
        document["migration"] = serde_json::json!({
            "migration_id": "03030303030303030303030303030303",
            "source": document["active"].clone(),
            "target": {
                "authority_id": "02020202020202020202020202020202",
                "provider_profile_id": "fdb-primary",
                "profile_fingerprint": "0202020202020202020202020202020202020202020202020202020202020202",
                "consistency_domain_id": "02020202020202020202020202020202",
                "contract_digest": "0909090909090909090909090909090909090909090909090909090909090909"
            },
            "phase": 99,
            "source_frontier": null,
            "target_frontier": null,
            "cutover_frontier": null
        });
        assert!(matches!(
            decode_metadata_authority_record(&serde_json::to_vec(&document).unwrap()),
            Err(ControlError::Codec(_))
        ));
    }

    #[test]
    fn codec_rejects_lifecycle_generation_combinations_that_cannot_be_reached() {
        let invalid = [
            (1, RootPlacementLifecycle::Active),
            (2, RootPlacementLifecycle::Provisioning),
            (2, RootPlacementLifecycle::Draining),
            (3, RootPlacementLifecycle::Active),
            (3, RootPlacementLifecycle::Retired),
        ];
        for (generation, lifecycle) in invalid {
            let placement = RootPlacement {
                placement_generation: PlacementGeneration::new(generation).unwrap(),
                lifecycle,
                ..placement()
            };
            assert!(matches!(
                encode_root_placement(&placement),
                Err(ControlError::InvalidRecord(_))
            ));
        }
    }

    #[test]
    fn codec_rejects_unknown_fields_and_trailing_input() {
        let mut root = encode_root_placement(&placement()).unwrap();
        root.push(b' ');
        assert!(matches!(
            decode_root_placement(&root),
            Err(ControlError::Codec(_))
        ));

        let unknown = br#"{"version":2,"root_id":"01010101010101010101010101010101","layout_profile":1,"layout_generation":1,"partition_id":"00000000000000000000000000000000","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1,"unexpected":true}"#;
        assert!(matches!(
            decode_root_placement(unknown),
            Err(ControlError::Codec(_))
        ));

        let mut authority = encode_metadata_authority_record(&ready_authority()).unwrap();
        authority.push(b' ');
        assert!(matches!(
            decode_metadata_authority_record(&authority),
            Err(ControlError::Codec(_))
        ));
    }

    #[test]
    fn codec_rejects_incomplete_owner_tuple() {
        let bytes = br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"owner_incarnation_id":"08080808080808080808080808080808","lease_id":42,"state":3,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#;
        assert!(matches!(
            decode_logical_shard_record(bytes),
            Err(ControlError::InvalidRecord(_))
        ));

        let mut missing_incarnation = serving_record();
        missing_incarnation.owner_incarnation_id = None;
        assert!(matches!(
            encode_logical_shard_record(&missing_incarnation),
            Err(ControlError::InvalidRecord(_))
        ));

        let mut zero_incarnation = serving_record();
        zero_incarnation.owner_incarnation_id = Some(incarnation(0));
        assert!(matches!(
            encode_logical_shard_record(&zero_incarnation),
            Err(ControlError::InvalidRecord(_))
        ));
    }

    #[test]
    fn owner_session_codec_is_strict() {
        let lease = LogicalShardLease {
            logical_shard_id: shard_id(2),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            owner_incarnation_id: incarnation(8),
            lease_id: 9,
            authority: authority().fence(),
        };
        let encoded = encode_owner_session(&lease).unwrap();
        assert_eq!(
            encoded,
            br#"{"version":3,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":1,"owner_incarnation_id":"08080808080808080808080808080808","lease_id":9,"authority_id":"01010101010101010101010101010101","authority_generation":1}"#
        );
        assert_eq!(decode_owner_session(&encoded).unwrap(), lease);
        let mut trailing = encoded;
        trailing.push(b'!');
        assert!(matches!(
            decode_owner_session(&trailing),
            Err(ControlError::Codec(_))
        ));

        let mut zero_incarnation = lease.clone();
        zero_incarnation.owner_incarnation_id = incarnation(0);
        assert!(matches!(
            encode_owner_session(&zero_incarnation),
            Err(ControlError::InvalidRecord(_))
        ));
        let zero_incarnation = br#"{"version":3,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":1,"owner_incarnation_id":"00000000000000000000000000000000","lease_id":9,"authority_id":"01010101010101010101010101010101","authority_generation":1}"#;
        assert!(matches!(
            decode_owner_session(zero_incarnation),
            Err(ControlError::InvalidRecord(_))
        ));

        let version_one = br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":1,"lease_id":9}"#;
        assert!(matches!(
            decode_owner_session(version_one),
            Err(ControlError::OwnerSessionCodecUpgradeRequired {
                stored_version: 1,
                required_version: 3
            })
        ));
        let version_two = br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":1,"lease_id":9,"authority_id":"01010101010101010101010101010101","authority_generation":1}"#;
        assert!(matches!(
            decode_owner_session(version_two),
            Err(ControlError::OwnerSessionCodecUpgradeRequired {
                stored_version: 2,
                required_version: 3
            })
        ));
    }

    #[test]
    fn owner_admission_v1_codecs_round_trip_every_durable_value() {
        let fresh = fresh_owner_intent();
        assert_eq!(fresh.planned_epoch().get(), 1);
        assert_eq!(
            decode_owner_admission_intent(&encode_owner_admission_intent(&fresh).unwrap()).unwrap(),
            fresh
        );

        let successor = successor_owner_intent();
        assert_eq!(successor.planned_epoch().get(), 2);
        assert_eq!(
            decode_owner_admission_intent(&encode_owner_admission_intent(&successor).unwrap())
                .unwrap(),
            successor
        );

        let plan = fresh_owner_plan();
        assert_eq!(
            decode_planned_owner_admission(&encode_planned_owner_admission(&plan).unwrap())
                .unwrap(),
            plan
        );
        let prepared = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        assert_eq!(
            decode_owner_admission_claim(&encode_owner_admission_claim(&prepared).unwrap())
                .unwrap(),
            prepared
        );
        let committed = prepared.clone().commit().unwrap();
        assert_eq!(
            decode_owner_admission_claim(&encode_owner_admission_claim(&committed).unwrap())
                .unwrap(),
            committed
        );
        let terminated = committed
            .clone()
            .terminate(OwnerAdmissionTerminationReasonV1::LeaseExpired {
                evidence_digest: OwnerLeaseExpiryEvidenceDigest::from_bytes([4; 32]).unwrap(),
            })
            .unwrap();
        assert_eq!(
            decode_owner_admission_claim(&encode_owner_admission_claim(&terminated).unwrap())
                .unwrap(),
            terminated
        );
        let aborted = prepared
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap();
        assert_eq!(
            decode_owner_admission_claim(&encode_owner_admission_claim(&aborted).unwrap()).unwrap(),
            aborted
        );
        let rejected = OwnerAdmissionClaimV1::rejected_from_absent(
            plan.intent(),
            OwnerAdmissionRejectionReasonV1::ActivePlanExists,
        )
        .unwrap();
        assert_eq!(
            decode_owner_admission_claim(&encode_owner_admission_claim(&rejected).unwrap())
                .unwrap(),
            rejected
        );
        let ambiguity_sealed = OwnerAdmissionClaimV1::rejected_from_absent(
            plan.intent(),
            OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
        )
        .unwrap();
        let ambiguity_sealed_bytes = encode_owner_admission_claim(&ambiguity_sealed).unwrap();
        assert!(ambiguity_sealed_bytes.ends_with(br#""rejection_reason":6}"#));
        assert_eq!(
            decode_owner_admission_claim(&ambiguity_sealed_bytes).unwrap(),
            ambiguity_sealed
        );

        let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&plan);
        let decoded = decode_owner_admission_plan_sentinel(
            &encode_owner_admission_plan_sentinel(&sentinel).unwrap(),
        )
        .unwrap();
        assert_eq!(decoded, sentinel);
        decoded.validate_plan(&plan).unwrap();
    }

    #[test]
    fn owner_admission_intents_enforce_fresh_successor_and_no_burn() {
        let never_owned = LogicalShardRecord::unassigned(shard_id(2));
        let fresh = OwnerAdmissionIntentV1::fresh(
            owner_admission(),
            never_owned.clone(),
            NodeId::new("node-a").unwrap(),
            incarnation(8),
            "10.0.0.1:7000".to_owned(),
            reservation_digest(6),
        )
        .unwrap();
        assert_eq!(never_owned.owner_epoch, None);
        assert_eq!(fresh.expected_unowned_shard(), &never_owned);
        assert_eq!(fresh.planned_epoch().get(), 1);

        let rejected = OwnerAdmissionClaimV1::rejected_from_absent(
            &fresh,
            OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
        )
        .unwrap();
        assert!(matches!(
            rejected.phase(),
            OwnerAdmissionClaimPhaseV1::Rejected { .. }
        ));
        assert_eq!(fresh.expected_unowned_shard().owner_epoch, None);

        assert!(OwnerAdmissionIntentV1::fresh(
            owner_admission(),
            released_first_owner(),
            NodeId::new("node-b").unwrap(),
            incarnation(9),
            "10.0.0.2:7000".to_owned(),
            reservation_digest(7),
        )
        .is_err());

        let released = released_first_owner();
        let successor = successor_owner_intent();
        assert_eq!(released.owner_epoch, Some(OwnerEpoch::new(1).unwrap()));
        assert_eq!(successor.expected_unowned_shard(), &released);
        assert_eq!(successor.planned_epoch().get(), 2);

        let live_previous = OwnerAdmissionClaimV1::prepared(&fresh_owner_plan())
            .unwrap()
            .commit()
            .unwrap();
        assert!(OwnerAdmissionIntentV1::successor(
            owner_admission(),
            released_first_owner(),
            live_previous,
            NodeId::new("node-b").unwrap(),
            incarnation(9),
            "10.0.0.2:7000".to_owned(),
            reservation_digest(7),
        )
        .is_err());
    }

    #[test]
    fn owner_admission_claim_transitions_retain_exact_plan_and_reject_regression() {
        let plan = fresh_owner_plan();
        let expected_lease = plan.lease().clone();
        let expected_digest = plan.digest();
        let prepared = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        let committed = prepared.commit().unwrap();
        assert!(matches!(
            committed.phase(),
            OwnerAdmissionClaimPhaseV1::Committed { lease, plan_digest }
                if lease == &expected_lease && *plan_digest == expected_digest
        ));
        assert!(committed
            .clone()
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .is_err());
        let terminated = committed
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        assert!(matches!(
            terminated.phase(),
            OwnerAdmissionClaimPhaseV1::Terminated { lease, plan_digest, .. }
                if lease == &expected_lease && *plan_digest == expected_digest
        ));
        assert!(terminated.commit().is_err());
    }

    #[test]
    fn owner_admission_debug_is_redacted_and_transition_errors_use_only_phase_tags() {
        const SECRET: &str = "owner-admission-secret-sentinel";
        let mut placement = placement();
        placement.placement_generation = PlacementGeneration::new(2).unwrap();
        placement.lifecycle = RootPlacementLifecycle::Active;
        let mut authority = authority();
        authority.active.provider_profile_id =
            MetadataProviderProfileId::new(format!("profile-{SECRET}")).unwrap();
        let admission = OwnerServingAdmission::stable(placement, authority).unwrap();
        let reservation = OwnerRuntimeReservationDigest::from_bytes([173; 32]).unwrap();
        let intent = OwnerAdmissionIntentV1::fresh(
            admission,
            LogicalShardRecord::unassigned(shard_id(2)),
            NodeId::new(format!("node-{SECRET}")).unwrap(),
            incarnation(8),
            format!("endpoint-{SECRET}"),
            reservation,
        )
        .unwrap();
        let intent_digest = intent.digest();
        let lease = LogicalShardLease {
            logical_shard_id: intent.logical_shard_id(),
            owner: intent.owner().clone(),
            owner_epoch: intent.planned_epoch(),
            owner_incarnation_id: intent.owner_incarnation_id(),
            lease_id: 9,
            authority: intent.admission().authority().fence(),
        };
        let plan = PlannedOwnerAdmissionV1::new(intent, lease).unwrap();
        let plan_digest = plan.digest();
        let prepared = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        let prepared_phase_debug = format!("{:?}", prepared.phase());
        let transition_error = prepared
            .clone()
            .commit()
            .unwrap()
            .abort(OwnerAdmissionAbortReasonV1::OwnerCasRejected)
            .unwrap_err();
        let expiry_digest = OwnerLeaseExpiryEvidenceDigest::from_bytes([174; 32]).unwrap();
        let termination_reason = OwnerAdmissionTerminationReasonV1::LeaseExpired {
            evidence_digest: expiry_digest,
        };
        assert_eq!(format!("{termination_reason:?}"), "LeaseExpired");
        let terminated = prepared
            .commit()
            .unwrap()
            .terminate(termination_reason)
            .unwrap();
        let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&plan);

        assert_eq!(
            format!("{reservation:?}"),
            "OwnerRuntimeReservationDigest(<redacted>)"
        );
        assert_eq!(
            format!("{intent_digest:?}"),
            "OwnerAdmissionIntentDigestV1(<redacted>)"
        );
        assert_eq!(
            format!("{plan_digest:?}"),
            "OwnerAdmissionPlanDigestV1(<redacted>)"
        );
        assert_eq!(
            format!("{expiry_digest:?}"),
            "OwnerLeaseExpiryEvidenceDigest(<redacted>)"
        );
        assert_eq!(
            format!("{sentinel:?}"),
            "OwnerAdmissionPlanSentinelV1(<redacted>)"
        );
        assert_eq!(
            format!("{:?}", terminated.identity()),
            "OwnerAdmissionClaimIdentityV1(<redacted>)"
        );
        assert_eq!(format!("{:?}", terminated.phase()), "Terminated");

        for rendered in [
            format!("{:?}", plan.intent()),
            format!("{plan:?}"),
            format!("{terminated:?}"),
            format!("{:?}", terminated.phase()),
            prepared_phase_debug,
            transition_error.to_string(),
        ] {
            assert!(!rendered.contains(SECRET), "debug leak: {rendered}");
            assert!(!rendered.contains("173"), "digest leak: {rendered}");
            assert!(!rendered.contains("174"), "digest leak: {rendered}");
        }
        assert_eq!(
            transition_error.to_string(),
            "invalid control record: owner admission claim transition from Committed to Aborted is not allowed"
        );
    }

    #[test]
    fn owner_admission_v1_codecs_reject_tampering_and_noncanonical_input() {
        let intent = fresh_owner_intent();
        let mut intent_bytes = encode_owner_admission_intent(&intent).unwrap();
        tamper_hex_field(&mut intent_bytes, b"\"intent_digest\":\"");
        assert!(decode_owner_admission_intent(&intent_bytes).is_err());

        let plan = fresh_owner_plan();
        let mut plan_bytes = encode_planned_owner_admission(&plan).unwrap();
        tamper_hex_field(&mut plan_bytes, b"\"plan_digest\":\"");
        assert!(decode_planned_owner_admission(&plan_bytes).is_err());

        let claim = OwnerAdmissionClaimV1::prepared(&plan).unwrap();
        let claim_bytes = encode_owner_admission_claim(&claim).unwrap();
        let mut illegal_phase: serde_json::Value = serde_json::from_slice(&claim_bytes).unwrap();
        illegal_phase["phase"] = serde_json::json!(2);
        illegal_phase["lease"] = serde_json::Value::Null;
        illegal_phase["plan_digest"] = serde_json::Value::Null;
        assert!(
            decode_owner_admission_claim(&serde_json::to_vec(&illegal_phase).unwrap()).is_err()
        );

        let mut unknown_phase: serde_json::Value = serde_json::from_slice(&claim_bytes).unwrap();
        unknown_phase["phase"] = serde_json::json!(99);
        assert!(
            decode_owner_admission_claim(&serde_json::to_vec(&unknown_phase).unwrap()).is_err()
        );

        let sealed = OwnerAdmissionClaimV1::rejected_from_absent(
            plan.intent(),
            OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
        )
        .unwrap();
        let mut unknown_rejection: serde_json::Value =
            serde_json::from_slice(&encode_owner_admission_claim(&sealed).unwrap()).unwrap();
        unknown_rejection["rejection_reason"] = serde_json::json!(99);
        assert!(
            decode_owner_admission_claim(&serde_json::to_vec(&unknown_rejection).unwrap()).is_err()
        );

        let terminated = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .commit()
            .unwrap()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        let mut removed_supersession: serde_json::Value =
            serde_json::from_slice(&encode_owner_admission_claim(&terminated).unwrap()).unwrap();
        removed_supersession["termination_reason"]["kind"] = serde_json::json!(4);
        removed_supersession["termination_reason"]["new_incarnation"] =
            serde_json::json!("09090909090909090909090909090909");
        removed_supersession["termination_reason"]["new_epoch"] = serde_json::json!(2);
        assert!(
            decode_owner_admission_claim(&serde_json::to_vec(&removed_supersession).unwrap())
                .is_err()
        );

        let mut future_version = encode_owner_admission_intent(&intent).unwrap();
        replace_first(&mut future_version, b"\"version\":1", b"\"version\":9");
        assert!(decode_owner_admission_intent(&future_version).is_err());

        let mut extra_field =
            encode_owner_admission_plan_sentinel(&OwnerAdmissionPlanSentinelV1::for_plan(&plan))
                .unwrap();
        let close = extra_field.pop().unwrap();
        assert_eq!(close, b'}');
        extra_field.extend_from_slice(b",\"unexpected\":true}");
        assert!(decode_owner_admission_plan_sentinel(&extra_field).is_err());

        let mut trailing = encode_owner_admission_claim(&claim).unwrap();
        trailing.push(b' ');
        assert!(decode_owner_admission_claim(&trailing).is_err());

        assert!(
            decode_owner_admission_intent(&vec![b'x'; MAX_OWNER_ADMISSION_INTENT_BYTES + 1])
                .is_err()
        );
        assert!(OwnerRuntimeReservationDigest::from_bytes([0; 32]).is_err());
        assert!(OwnerAdmissionIntentDigestV1::from_bytes([0; 32]).is_err());
        assert!(OwnerAdmissionPlanDigestV1::from_bytes([0; 32]).is_err());
        assert!(OwnerLeaseExpiryEvidenceDigest::from_bytes([0; 32]).is_err());
    }

    #[test]
    fn owner_admission_v1_canonical_wire_goldens() {
        let plan = fresh_owner_plan();
        let claim = OwnerAdmissionClaimV1::prepared(&plan)
            .unwrap()
            .commit()
            .unwrap()
            .terminate(OwnerAdmissionTerminationReasonV1::Released)
            .unwrap();
        let sentinel = OwnerAdmissionPlanSentinelV1::for_plan(&plan);
        let ambiguity_sealed = OwnerAdmissionClaimV1::rejected_from_absent(
            plan.intent(),
            OwnerAdmissionRejectionReasonV1::PrepareAmbiguitySealed,
        )
        .unwrap();
        let intent_bytes = encode_owner_admission_intent(plan.intent()).unwrap();
        let plan_bytes = encode_planned_owner_admission(&plan).unwrap();
        let claim_bytes = encode_owner_admission_claim(&claim).unwrap();
        let sentinel_bytes = encode_owner_admission_plan_sentinel(&sentinel).unwrap();
        let ambiguity_sealed_bytes = encode_owner_admission_claim(&ambiguity_sealed).unwrap();
        assert_wire_golden(
            &intent_bytes,
            2_228,
            "814ede0b179f8d93c3909b31592f8b71a5d9acf090ab30a769ad7a8640eec979",
        );
        assert_wire_golden(
            &plan_bytes,
            4_805,
            "b90b50a2454aaac18ce449dd6d2f2d2bcbce79b12665bcff7da99adfb531b3c9",
        );
        assert_eq!(
            claim_bytes,
            br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner_incarnation_id":"08080808080808080808080808080808","intent_digest":"f48674f711991cd5c27d70fe99fa143664b9d863cc2c17f652142ae61d835bd4","reservation_digest":"0606060606060606060606060606060606060606060606060606060606060606","planned_epoch":1,"phase":3,"lease":{"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":1,"owner_incarnation_id":"08080808080808080808080808080808","lease_id":9,"authority_id":"01010101010101010101010101010101","authority_generation":1},"plan_digest":"dd814077bb0486baf265da69f7d06788a3e207c61daaae72e6527b76c5547966","termination_reason":{"kind":1,"lease_expiry_evidence_digest":null,"migration_id":null},"abort_reason":null,"rejection_reason":null}"#
        );
        assert_eq!(
            sentinel_bytes,
            br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner_incarnation_id":"08080808080808080808080808080808","lease_id":9,"plan_digest":"dd814077bb0486baf265da69f7d06788a3e207c61daaae72e6527b76c5547966"}"#
        );
        assert_wire_golden(
            &claim_bytes,
            780,
            "1ba0c55e1446d8212ccee4461a75db0222913233714d1fab38a1c7d9aa9c82cb",
        );
        assert_wire_golden(
            &sentinel_bytes,
            219,
            "062c281371fb1d8b7535f0e40197faaedb4b6d08140f4fe787857aa15d053e66",
        );
        assert_wire_golden(
            &ambiguity_sealed_bytes,
            423,
            "13c22b8abb78eae8e08a20086ed7e107b0ecb14039088a4bf0d82bad80deee34",
        );
    }

    #[test]
    fn owner_admission_v1_inner_codec_and_digest_domain_coupling_is_frozen() {
        assert_eq!(OWNER_ADMISSION_CODEC_VERSION, 1);
        assert_eq!(
            (
                ROOT_PLACEMENT_CODEC_VERSION,
                METADATA_AUTHORITY_RECORD_CODEC_VERSION,
                LOGICAL_SHARD_RECORD_CODEC_VERSION,
                OWNER_ADMISSION_INNER_CLAIM_CODEC_VERSION,
            ),
            (2, 2, 2, 1),
            "changing an embedded codec requires owner-admission version, digest-domain, and golden updates"
        );
        assert_eq!(
            OWNER_ADMISSION_INTENT_DIGEST_DOMAIN,
            b"nokv.control.owner-admission-intent.v1.root-v2.authority-v2.shard-v2.claim-v1\0"
        );
        assert_eq!(
            OWNER_ADMISSION_PLAN_DIGEST_DOMAIN,
            b"nokv.control.owner-admission-plan.v1.intent-v1.root-v2.authority-v2.shard-v2.claim-v1\0"
        );
    }

    fn assert_wire_golden(bytes: &[u8], expected_len: usize, expected_sha256: &str) {
        assert_eq!(bytes.len(), expected_len);
        let digest: [u8; SHA256_BYTES] = Sha256::digest(bytes).into();
        assert_eq!(encode_fixed_id(&digest), expected_sha256);
    }

    fn tamper_hex_field(bytes: &mut [u8], marker: &[u8]) {
        let start = bytes
            .windows(marker.len())
            .position(|window| window == marker)
            .unwrap()
            + marker.len();
        bytes[start] = if bytes[start] == b'a' { b'b' } else { b'a' };
    }

    fn replace_first(bytes: &mut [u8], needle: &[u8], replacement: &[u8]) {
        assert_eq!(needle.len(), replacement.len());
        let start = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .unwrap();
        bytes[start..start + needle.len()].copy_from_slice(replacement);
    }
}
