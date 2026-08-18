use serde::{Deserialize, Serialize};

use crate::store::{validate_logical_shard_record, MAX_LOGICAL_SHARD_RECORD_BYTES};
#[cfg(any(feature = "etcd", test))]
use crate::LogicalShardLease;
use crate::{
    AgentId, CheckpointRef, ControlError, LogRef, LogSegmentRef, LogicalShardId,
    LogicalShardRecord, LogicalShardRecoveryState, LogicalShardState, NodeId, ObjectNamespaceId,
    OwnerEpoch, PlacementGeneration, RecoveryUploadIntent, RootAgentBinding, RootId,
    RootObjectNamespaceBinding, RootPlacement, RootPlacementLifecycle,
};

const ROOT_PLACEMENT_CODEC_VERSION: u8 = 1;
const ROOT_AGENT_BINDING_CODEC_VERSION: u8 = 1;
const ROOT_OBJECT_NAMESPACE_CODEC_VERSION: u8 = 1;
/// Wire version of the client-facing logical shard routing record.
///
/// This is a compatibility contract, not a counter: every reader that
/// understands version 1 (every released NoKV client) must keep decoding the
/// value stored at the logical shard record key. Owner-side recovery state is
/// persisted under its own key and codec version so that it can evolve
/// without touching this schema. Bumping this version is a deliberate,
/// client-breaking release decision.
pub const LOGICAL_SHARD_ROUTING_CODEC_VERSION: u8 = 1;
/// Wire version of the owner-only logical shard recovery state.
pub const LOGICAL_SHARD_RECOVERY_CODEC_VERSION: u8 = 1;
/// Highest legacy combined record version still readable at the routing key.
///
/// Versions 2 and 3 folded recovery state into the routing record; owners
/// that still write them are read but never written by this crate.
const LEGACY_COMBINED_LOGICAL_SHARD_RECORD_MAX_VERSION: u8 = 3;
#[cfg(any(feature = "etcd", test))]
const OWNER_SESSION_CODEC_VERSION: u8 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootPlacementWire {
    version: u8,
    root_id: String,
    logical_shard_id: String,
    placement_generation: u64,
    lifecycle: u8,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootAgentBindingWire {
    version: u8,
    root_id: String,
    agent_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RootObjectNamespaceBindingWire {
    version: u8,
    root_id: String,
    object_namespace_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalShardRecordWireV3 {
    version: u8,
    logical_shard_id: String,
    owner: Option<String>,
    owner_epoch: Option<u64>,
    lease_id: u64,
    state: u8,
    endpoint: Option<String>,
    checkpoint: Option<CheckpointRefWire>,
    log: Option<LogRefWire>,
    durable_lsn: u64,
    pending_recovery_upload: Option<RecoveryUploadIntentWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalShardRecordWireV2 {
    version: u8,
    logical_shard_id: String,
    owner: Option<String>,
    owner_epoch: Option<u64>,
    lease_id: u64,
    state: u8,
    endpoint: Option<String>,
    checkpoint: Option<CheckpointRefWireV2>,
    log: Option<LogRefWire>,
    durable_lsn: u64,
    pending_recovery_upload: Option<RecoveryUploadIntentWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalShardRecordWireV1 {
    version: u8,
    logical_shard_id: String,
    owner: Option<String>,
    owner_epoch: Option<u64>,
    lease_id: u64,
    state: u8,
    endpoint: Option<String>,
    checkpoint: Option<CheckpointRefWireV2>,
    log: Option<LogRefWireV1>,
    durable_lsn: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalShardRecoveryWireV1 {
    version: u8,
    logical_shard_id: String,
    checkpoint: Option<CheckpointRefWire>,
    log: Option<LogRefWire>,
    durable_lsn: u64,
    pending_recovery_upload: Option<RecoveryUploadIntentWire>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryUploadIntentWire {
    object_namespace_id: String,
    first_lsn: u64,
    last_lsn: u64,
    previous_chain_digest: String,
    last_chain_digest: String,
    segment_digest: String,
    manifest_key: String,
    receipt: Vec<u8>,
    plan: Vec<u8>,
}

#[derive(Deserialize)]
struct VersionWire {
    version: u8,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointRefWire {
    object_key: String,
    lsn: u64,
    image_bytes: u64,
    image_digest: String,
    digest: String,
    receipt: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointRefWireV2 {
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
    receipt: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRefWire {
    segments: Vec<LogSegmentRefWire>,
    durable_lsn: u64,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSegmentRefWireV1 {
    segment_key: String,
    first_lsn: u64,
    last_lsn: u64,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogRefWireV1 {
    segments: Vec<LogSegmentRefWireV1>,
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
    lease_id: u64,
}

pub fn encode_root_placement(placement: &RootPlacement) -> Result<Vec<u8>, ControlError> {
    validate_root_placement(placement)?;
    serde_json::to_vec(&RootPlacementWire {
        version: ROOT_PLACEMENT_CODEC_VERSION,
        root_id: encode_fixed_id(placement.root_id.as_bytes()),
        logical_shard_id: encode_fixed_id(placement.logical_shard_id.as_bytes()),
        placement_generation: placement.placement_generation.get(),
        lifecycle: placement.lifecycle.into(),
    })
    .map_err(codec_error)
}

pub fn decode_root_placement(bytes: &[u8]) -> Result<RootPlacement, ControlError> {
    let wire: RootPlacementWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version("root placement", wire.version, ROOT_PLACEMENT_CODEC_VERSION)?;
    let placement = RootPlacement {
        root_id: RootId::from_bytes(decode_fixed_id(&wire.root_id, "root id")?),
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

pub fn encode_root_agent_binding(binding: &RootAgentBinding) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(&RootAgentBindingWire {
        version: ROOT_AGENT_BINDING_CODEC_VERSION,
        root_id: encode_fixed_id(binding.root_id.as_bytes()),
        agent_id: encode_fixed_id(binding.agent_id.as_bytes()),
    })
    .map_err(codec_error)
}

pub fn decode_root_agent_binding(bytes: &[u8]) -> Result<RootAgentBinding, ControlError> {
    let wire: RootAgentBindingWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version(
        "root Agent binding",
        wire.version,
        ROOT_AGENT_BINDING_CODEC_VERSION,
    )?;
    let binding = RootAgentBinding {
        root_id: RootId::from_bytes(decode_fixed_id(&wire.root_id, "root id")?),
        agent_id: AgentId::from_bytes(decode_fixed_id(&wire.agent_id, "Agent id")?),
    };
    if encode_root_agent_binding(&binding)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "root Agent binding input is not canonical".to_owned(),
        ));
    }
    Ok(binding)
}

pub fn encode_root_object_namespace_binding(
    binding: &RootObjectNamespaceBinding,
) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(&RootObjectNamespaceBindingWire {
        version: ROOT_OBJECT_NAMESPACE_CODEC_VERSION,
        root_id: encode_fixed_id(binding.root_id.as_bytes()),
        object_namespace_id: encode_fixed_id(binding.object_namespace_id.as_bytes()),
    })
    .map_err(codec_error)
}

pub fn decode_root_object_namespace_binding(
    bytes: &[u8],
) -> Result<RootObjectNamespaceBinding, ControlError> {
    let wire: RootObjectNamespaceBindingWire =
        serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version(
        "root object namespace binding",
        wire.version,
        ROOT_OBJECT_NAMESPACE_CODEC_VERSION,
    )?;
    let binding = RootObjectNamespaceBinding {
        root_id: RootId::from_bytes(decode_fixed_id(&wire.root_id, "root id")?),
        object_namespace_id: ObjectNamespaceId::from_bytes(decode_fixed_id(
            &wire.object_namespace_id,
            "object namespace id",
        )?),
    };
    if encode_root_object_namespace_binding(&binding)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "root object namespace binding input is not canonical".to_owned(),
        ));
    }
    Ok(binding)
}

/// Encode the client-facing routing record stored at the logical shard key.
///
/// The value is always the version-1 wire schema with every recovery field
/// cleared: checkpoint and log are `null` and `durable_lsn` is zero, which is
/// the only recovery frontier a version-1 reader can validate. The full
/// record is validated first so an inconsistent owner state never reaches
/// either key.
pub fn encode_logical_shard_routing_record(
    record: &LogicalShardRecord,
) -> Result<Vec<u8>, ControlError> {
    validate_logical_shard_record(record)?;
    encode_logical_shard_routing_record_wire(record)
}

/// Encode the owner-side recovery state stored next to the routing record.
///
/// Returns `None` when the record carries no recovery state at all; the
/// backend then removes the recovery key so that absence and emptiness are
/// the same durable fact.
pub fn encode_logical_shard_recovery_state(
    record: &LogicalShardRecord,
) -> Result<Option<Vec<u8>>, ControlError> {
    validate_logical_shard_record(record)?;
    encode_logical_shard_recovery_state_wire(record)
}

pub(crate) fn validate_logical_shard_record_encoded_size(
    record: &LogicalShardRecord,
) -> Result<(), ControlError> {
    let routing = encode_logical_shard_routing_record_wire(record)?;
    if routing.len() > MAX_LOGICAL_SHARD_RECORD_BYTES {
        return Err(ControlError::InvalidRecord(format!(
            "encoded logical shard routing record is {} bytes; maximum is {MAX_LOGICAL_SHARD_RECORD_BYTES}",
            routing.len()
        )));
    }
    if let Some(recovery) = encode_logical_shard_recovery_state_wire(record)? {
        if recovery.len() > MAX_LOGICAL_SHARD_RECORD_BYTES {
            return Err(ControlError::InvalidRecord(format!(
                "encoded logical shard recovery state is {} bytes; maximum is {MAX_LOGICAL_SHARD_RECORD_BYTES}",
                recovery.len()
            )));
        }
    }
    Ok(())
}

fn encode_logical_shard_routing_record_wire(
    record: &LogicalShardRecord,
) -> Result<Vec<u8>, ControlError> {
    serde_json::to_vec(&LogicalShardRecordWireV1 {
        version: LOGICAL_SHARD_ROUTING_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(record.logical_shard_id.as_bytes()),
        owner: record.owner.as_ref().map(|owner| owner.as_str().to_owned()),
        owner_epoch: record.owner_epoch.map(OwnerEpoch::get),
        lease_id: record.lease_id,
        state: record.state.into(),
        endpoint: record.endpoint.clone(),
        checkpoint: None,
        log: None,
        durable_lsn: 0,
    })
    .map_err(codec_error)
}

fn encode_logical_shard_recovery_state_wire(
    record: &LogicalShardRecord,
) -> Result<Option<Vec<u8>>, ControlError> {
    if !record.has_recovery_state() {
        return Ok(None);
    }
    serde_json::to_vec(&LogicalShardRecoveryWireV1 {
        version: LOGICAL_SHARD_RECOVERY_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(record.logical_shard_id.as_bytes()),
        checkpoint: record.checkpoint.clone().map(Into::into),
        log: record.log.clone().map(Into::into),
        durable_lsn: record.durable_lsn,
        pending_recovery_upload: record.pending_recovery_upload.clone().map(Into::into),
    })
    .map(Some)
    .map_err(codec_error)
}

/// How the value at the logical shard record key was written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalShardRecordWireKind {
    /// The current version-1 routing record; recovery state lives separately.
    Routing,
    /// A legacy version-2 or version-3 record that folds recovery state in.
    LegacyCombined { version: u8 },
}

/// One decoded logical shard record value plus how it was written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedLogicalShardRecord {
    pub record: LogicalShardRecord,
    pub wire: LogicalShardRecordWireKind,
}

/// Decode the value stored at the logical shard record key.
///
/// A version-1 value is a routing record whose recovery fields are absent by
/// construction; the backend reattaches the separately stored recovery state.
/// Legacy version-2/3 values written by owners before the split are complete
/// records and take precedence over any recovery key left next to them.
pub fn decode_logical_shard_record_value(
    bytes: &[u8],
) -> Result<DecodedLogicalShardRecord, ControlError> {
    let version: VersionWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    let (record, wire) = match version.version {
        LOGICAL_SHARD_ROUTING_CODEC_VERSION => (
            decode_logical_shard_record_v1(bytes)?,
            LogicalShardRecordWireKind::Routing,
        ),
        2 => (
            decode_logical_shard_record_v2(bytes)?,
            LogicalShardRecordWireKind::LegacyCombined { version: 2 },
        ),
        3 => (
            decode_logical_shard_record_v3(bytes)?,
            LogicalShardRecordWireKind::LegacyCombined { version: 3 },
        ),
        actual => {
            return Err(ControlError::UnsupportedRecordVersion {
                record: "logical shard record",
                version: actual,
                supported: LEGACY_COMBINED_LOGICAL_SHARD_RECORD_MAX_VERSION,
            });
        }
    };
    validate_logical_shard_record(&record)?;
    Ok(DecodedLogicalShardRecord { record, wire })
}

/// Decode the value stored at the logical shard record key into a record.
pub fn decode_logical_shard_record(bytes: &[u8]) -> Result<LogicalShardRecord, ControlError> {
    decode_logical_shard_record_value(bytes).map(|decoded| decoded.record)
}

/// Decode the owner-side recovery state stored next to a routing record.
pub fn decode_logical_shard_recovery_state(
    bytes: &[u8],
) -> Result<LogicalShardRecoveryState, ControlError> {
    let version: VersionWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    if version.version != LOGICAL_SHARD_RECOVERY_CODEC_VERSION {
        return Err(ControlError::UnsupportedRecordVersion {
            record: "logical shard recovery state",
            version: version.version,
            supported: LOGICAL_SHARD_RECOVERY_CODEC_VERSION,
        });
    }
    let wire: LogicalShardRecoveryWireV1 = serde_json::from_slice(bytes).map_err(codec_error)?;
    if serde_json::to_vec(&wire).map_err(codec_error)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "logical shard recovery state input is not canonical".to_owned(),
        ));
    }
    let state = LogicalShardRecoveryState {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &wire.logical_shard_id,
            "logical shard id",
        )?),
        checkpoint: wire.checkpoint.map(Into::into),
        log: wire.log.map(Into::into),
        durable_lsn: wire.durable_lsn,
        pending_recovery_upload: wire
            .pending_recovery_upload
            .map(RecoveryUploadIntent::try_from)
            .transpose()?,
    };
    if !LogicalShardRecord::unassigned(state.logical_shard_id)
        .with_recovery_state(state.clone())?
        .has_recovery_state()
    {
        return Err(ControlError::Codec(
            "logical shard recovery state must not be stored when it is empty".to_owned(),
        ));
    }
    Ok(state)
}

fn decode_logical_shard_record_v3(bytes: &[u8]) -> Result<LogicalShardRecord, ControlError> {
    let wire: LogicalShardRecordWireV3 = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version("logical shard record", wire.version, 3)?;
    if serde_json::to_vec(&wire).map_err(codec_error)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "logical shard record v3 input is not canonical".to_owned(),
        ));
    }
    logical_shard_record_from_wire(
        wire.logical_shard_id,
        wire.owner,
        wire.owner_epoch,
        wire.lease_id,
        wire.state,
        wire.endpoint,
        wire.checkpoint,
        wire.log,
        wire.durable_lsn,
        wire.pending_recovery_upload
            .map(RecoveryUploadIntent::try_from)
            .transpose()?,
    )
}

fn decode_logical_shard_record_v2(bytes: &[u8]) -> Result<LogicalShardRecord, ControlError> {
    let wire: LogicalShardRecordWireV2 = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version("logical shard record", wire.version, 2)?;
    if serde_json::to_vec(&wire).map_err(codec_error)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "logical shard record v2 input is not canonical".to_owned(),
        ));
    }
    if wire.checkpoint.is_some() {
        return Err(ControlError::Codec(
            "logical shard record v2 checkpoint lacks a recovery receipt".to_owned(),
        ));
    }
    logical_shard_record_from_wire(
        wire.logical_shard_id,
        wire.owner,
        wire.owner_epoch,
        wire.lease_id,
        wire.state,
        wire.endpoint,
        None,
        wire.log,
        wire.durable_lsn,
        wire.pending_recovery_upload
            .map(RecoveryUploadIntent::try_from)
            .transpose()?,
    )
}

fn decode_logical_shard_record_v1(bytes: &[u8]) -> Result<LogicalShardRecord, ControlError> {
    let wire: LogicalShardRecordWireV1 = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version("logical shard record", wire.version, 1)?;
    if serde_json::to_vec(&wire).map_err(codec_error)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "logical shard record v1 input is not canonical".to_owned(),
        ));
    }
    if wire.checkpoint.is_some() {
        return Err(ControlError::Codec(
            "logical shard record v1 checkpoint lacks a recovery receipt".to_owned(),
        ));
    }
    if wire.log.is_some() {
        return Err(ControlError::Codec(
            "logical shard record v1 log lacks a recovery receipt".to_owned(),
        ));
    }
    logical_shard_record_from_wire(
        wire.logical_shard_id,
        wire.owner,
        wire.owner_epoch,
        wire.lease_id,
        wire.state,
        wire.endpoint,
        None,
        None,
        wire.durable_lsn,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn logical_shard_record_from_wire(
    logical_shard_id: String,
    owner: Option<String>,
    owner_epoch: Option<u64>,
    lease_id: u64,
    state: u8,
    endpoint: Option<String>,
    checkpoint: Option<CheckpointRefWire>,
    log: Option<LogRefWire>,
    durable_lsn: u64,
    pending_recovery_upload: Option<RecoveryUploadIntent>,
) -> Result<LogicalShardRecord, ControlError> {
    let record = LogicalShardRecord {
        logical_shard_id: LogicalShardId::from_bytes(decode_fixed_id(
            &logical_shard_id,
            "logical shard id",
        )?),
        owner: owner
            .map(NodeId::new)
            .transpose()
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        owner_epoch: owner_epoch
            .map(OwnerEpoch::new)
            .transpose()
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        lease_id,
        state: LogicalShardState::try_from(state)
            .map_err(|err| ControlError::Codec(err.to_string()))?,
        endpoint,
        checkpoint: checkpoint.map(Into::into),
        log: log.map(Into::into),
        durable_lsn,
        pending_recovery_upload,
    };
    Ok(record)
}

#[cfg(any(feature = "etcd", test))]
pub(crate) fn encode_owner_session(lease: &LogicalShardLease) -> Result<Vec<u8>, ControlError> {
    if lease.lease_id == 0 {
        return Err(ControlError::InvalidRecord(
            "owner session lease id must be non-zero".to_owned(),
        ));
    }
    serde_json::to_vec(&OwnerSessionWire {
        version: OWNER_SESSION_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(lease.logical_shard_id.as_bytes()),
        owner: lease.owner.as_str().to_owned(),
        owner_epoch: lease.owner_epoch.get(),
        lease_id: lease.lease_id,
    })
    .map_err(codec_error)
}

#[cfg(any(feature = "etcd", test))]
pub(crate) fn decode_owner_session(bytes: &[u8]) -> Result<LogicalShardLease, ControlError> {
    let wire: OwnerSessionWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version("owner session", wire.version, OWNER_SESSION_CODEC_VERSION)?;
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
        lease_id: wire.lease_id,
    };
    if encode_owner_session(&lease)?.as_slice() != bytes {
        return Err(ControlError::Codec(
            "owner session input is not canonical".to_owned(),
        ));
    }
    Ok(lease)
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

fn require_version(type_name: &'static str, actual: u8, expected: u8) -> Result<(), ControlError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ControlError::UnsupportedRecordVersion {
            record: type_name,
            version: actual,
            supported: expected,
        })
    }
}

fn validate_root_placement(placement: &RootPlacement) -> Result<(), ControlError> {
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
            receipt: value.receipt,
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
            receipt: value.receipt,
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
            receipt: value.receipt,
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
            receipt: value.receipt,
        }
    }
}

impl From<RecoveryUploadIntent> for RecoveryUploadIntentWire {
    fn from(intent: RecoveryUploadIntent) -> Self {
        Self {
            object_namespace_id: encode_fixed_id(intent.object_namespace_id.as_bytes()),
            first_lsn: intent.first_lsn,
            last_lsn: intent.last_lsn,
            previous_chain_digest: intent.previous_chain_digest,
            last_chain_digest: intent.last_chain_digest,
            segment_digest: intent.segment_digest,
            manifest_key: intent.manifest_key,
            receipt: intent.receipt,
            plan: intent.plan,
        }
    }
}

impl TryFrom<RecoveryUploadIntentWire> for RecoveryUploadIntent {
    type Error = ControlError;

    fn try_from(intent: RecoveryUploadIntentWire) -> Result<Self, Self::Error> {
        Ok(Self {
            object_namespace_id: ObjectNamespaceId::from_bytes(decode_fixed_id(
                &intent.object_namespace_id,
                "object namespace id",
            )?),
            first_lsn: intent.first_lsn,
            last_lsn: intent.last_lsn,
            previous_chain_digest: intent.previous_chain_digest,
            last_chain_digest: intent.last_chain_digest,
            segment_digest: intent.segment_digest,
            manifest_key: intent.manifest_key,
            receipt: intent.receipt,
            plan: intent.plan,
        })
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

    fn placement() -> RootPlacement {
        RootPlacement {
            root_id: root_id(1),
            logical_shard_id: shard_id(2),
            placement_generation: PlacementGeneration::new(1).unwrap(),
            lifecycle: RootPlacementLifecycle::Provisioning,
        }
    }

    fn object_namespace_binding() -> RootObjectNamespaceBinding {
        RootObjectNamespaceBinding {
            root_id: root_id(1),
            object_namespace_id: ObjectNamespaceId::from_bytes([3; 16]),
        }
    }

    fn agent_binding() -> RootAgentBinding {
        RootAgentBinding {
            root_id: root_id(1),
            agent_id: AgentId::from_bytes([4; 16]),
        }
    }

    fn serving_record() -> LogicalShardRecord {
        LogicalShardRecord {
            logical_shard_id: shard_id(2),
            owner: Some(NodeId::new("node-a").unwrap()),
            owner_epoch: Some(OwnerEpoch::new(7).unwrap()),
            lease_id: 42,
            state: LogicalShardState::Serving,
            endpoint: Some("10.0.0.1:7000".to_owned()),
            checkpoint: Some(CheckpointRef {
                object_key: "checkpoints/7".to_owned(),
                lsn: 128,
                image_bytes: 4096,
                image_digest: "sha256:image".to_owned(),
                digest: "state-128".to_owned(),
                receipt: vec![1, 2, 3],
            }),
            log: Some(LogRef {
                segments: vec![LogSegmentRef {
                    segment_key: "logs/129-144".to_owned(),
                    first_lsn: 129,
                    last_lsn: 144,
                    digest: "state-144".to_owned(),
                    receipt: vec![4, 5, 6],
                }],
                durable_lsn: 144,
                digest: "state-144".to_owned(),
            }),
            durable_lsn: 144,
            pending_recovery_upload: None,
        }
    }

    fn recovering_record_with_upload() -> LogicalShardRecord {
        LogicalShardRecord {
            logical_shard_id: shard_id(2),
            owner: Some(NodeId::new("node-a").unwrap()),
            owner_epoch: Some(OwnerEpoch::new(7).unwrap()),
            lease_id: 42,
            state: LogicalShardState::Recovering,
            endpoint: Some("10.0.0.1:7000".to_owned()),
            checkpoint: None,
            log: None,
            durable_lsn: 0,
            pending_recovery_upload: Some(RecoveryUploadIntent {
                object_namespace_id: ObjectNamespaceId::from_bytes([3; 16]),
                first_lsn: 1,
                last_lsn: 2,
                previous_chain_digest: "0".repeat(64),
                last_chain_digest: "1".repeat(64),
                segment_digest: "2".repeat(64),
                manifest_key: "nokv/recovery/log-segments/v1/manifest".to_owned(),
                receipt: vec![4, 5, 6],
                plan: vec![1, 2, 3],
            }),
        }
    }

    /// The logical shard record reader that shipped in NoKV 0.10.0, vendored
    /// byte for byte from `crates/nokv-control/src/codec.rs` at tag `v0.10.0`.
    ///
    /// Every client released before the recovery-state split decodes the
    /// routing record with these exact structs (`deny_unknown_fields`, one
    /// exact version) and then applies the same durable-LSN consistency rule
    /// as `validate_logical_shard_record` did at that tag. This module is the
    /// executable form of the compatibility contract: whatever this crate
    /// stores at the logical shard record key must decode here.
    mod frozen_v0_10_0_reader {
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub(super) struct RootPlacementWire {
            pub version: u8,
            pub root_id: String,
            pub logical_shard_id: String,
            pub placement_generation: u64,
            pub lifecycle: u8,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub(super) struct LogicalShardRecordWire {
            pub version: u8,
            pub logical_shard_id: String,
            pub owner: Option<String>,
            pub owner_epoch: Option<u64>,
            pub lease_id: u64,
            pub state: u8,
            pub endpoint: Option<String>,
            pub checkpoint: Option<CheckpointRefWire>,
            pub log: Option<LogRefWire>,
            pub durable_lsn: u64,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub(super) struct CheckpointRefWire {
            pub object_key: String,
            pub lsn: u64,
            pub image_bytes: u64,
            pub image_digest: String,
            pub digest: String,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub(super) struct LogSegmentRefWire {
            pub segment_key: String,
            pub first_lsn: u64,
            pub last_lsn: u64,
            pub digest: String,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
        #[serde(deny_unknown_fields)]
        pub(super) struct LogRefWire {
            pub segments: Vec<LogSegmentRefWire>,
            pub durable_lsn: u64,
            pub digest: String,
        }

        /// Decode exactly like the 0.10.0 client route resolver did.
        pub(super) fn decode_logical_shard_record(
            bytes: &[u8],
        ) -> Result<LogicalShardRecordWire, String> {
            let wire: LogicalShardRecordWire =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            if wire.version != 1 {
                return Err(format!(
                    "unsupported logical shard record codec version {}",
                    wire.version
                ));
            }
            if serde_json::to_vec(&wire).map_err(|error| error.to_string())? != bytes {
                return Err("logical shard record input is not canonical".to_owned());
            }
            let reference_lsn = match (wire.checkpoint.as_ref(), wire.log.as_ref()) {
                (Some(checkpoint), Some(log)) => checkpoint.lsn.max(log.durable_lsn),
                (Some(checkpoint), None) => checkpoint.lsn,
                (None, Some(log)) => log.durable_lsn,
                (None, None) => 0,
            };
            if wire.durable_lsn != reference_lsn {
                return Err(format!(
                    "durable LSN {} does not match recovery reference tail {reference_lsn}",
                    wire.durable_lsn
                ));
            }
            Ok(wire)
        }

        pub(super) fn decode_root_placement(bytes: &[u8]) -> Result<RootPlacementWire, String> {
            let wire: RootPlacementWire =
                serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
            if wire.version != 1 {
                return Err(format!(
                    "unsupported root placement codec version {}",
                    wire.version
                ));
            }
            Ok(wire)
        }
    }

    fn released_record() -> LogicalShardRecord {
        let mut record = serving_record();
        record.owner = None;
        record.owner_epoch = Some(OwnerEpoch::new(7).unwrap());
        record.lease_id = 0;
        record.state = LogicalShardState::Unassigned;
        record.endpoint = None;
        record
    }

    /// Records covering every routing shape the store persists, with and
    /// without owner-side recovery state.
    fn routing_contract_records() -> Vec<LogicalShardRecord> {
        let mut serving_without_recovery = serving_record();
        serving_without_recovery.checkpoint = None;
        serving_without_recovery.log = None;
        serving_without_recovery.durable_lsn = 0;
        vec![
            LogicalShardRecord::unassigned(shard_id(2)),
            serving_without_recovery,
            serving_record(),
            recovering_record_with_upload(),
            released_record(),
        ]
    }

    #[test]
    fn routing_record_stays_decodable_by_the_frozen_v0_10_0_reader() {
        assert_eq!(LOGICAL_SHARD_ROUTING_CODEC_VERSION, 1);
        for record in routing_contract_records() {
            let bytes = encode_logical_shard_routing_record(&record).unwrap();
            let frozen = frozen_v0_10_0_reader::decode_logical_shard_record(&bytes)
                .unwrap_or_else(|error| panic!("a 0.10.0 client must decode {record:?}: {error}"));
            assert_eq!(frozen.version, 1);
            assert_eq!(
                frozen.logical_shard_id,
                encode_fixed_id(record.logical_shard_id.as_bytes())
            );
            assert_eq!(
                frozen.owner,
                record.owner.as_ref().map(|owner| owner.as_str().to_owned())
            );
            assert_eq!(frozen.owner_epoch, record.owner_epoch.map(OwnerEpoch::get));
            assert_eq!(frozen.lease_id, record.lease_id);
            assert_eq!(frozen.state, u8::from(record.state));
            assert_eq!(frozen.endpoint, record.endpoint);
            // A frozen reader sees no shared recovery frontier; the frontier
            // lives in the recovery state that only owners read.
            assert_eq!(frozen.checkpoint, None);
            assert_eq!(frozen.log, None);
            assert_eq!(frozen.durable_lsn, 0);
        }
        let placement = encode_root_placement(&placement()).unwrap();
        assert_eq!(
            frozen_v0_10_0_reader::decode_root_placement(&placement)
                .unwrap()
                .version,
            1
        );
    }

    #[test]
    fn recovery_state_never_leaks_into_the_routing_value() {
        for record in routing_contract_records() {
            let bytes = encode_logical_shard_routing_record(&record).unwrap();
            let text = String::from_utf8(bytes).unwrap();
            assert!(!text.contains("pending_recovery_upload"), "{text}");
            assert!(!text.contains("receipt"), "{text}");
            assert!(!text.contains("plan"), "{text}");
            match encode_logical_shard_recovery_state(&record).unwrap() {
                None => assert!(!record.has_recovery_state()),
                Some(recovery) => {
                    assert!(record.has_recovery_state());
                    let state = decode_logical_shard_recovery_state(&recovery).unwrap();
                    assert_eq!(state, record.recovery_state());
                }
            }
        }
    }

    #[test]
    fn records_written_by_a_0_10_0_owner_stay_readable_and_canonical() {
        // Exact bytes a 0.10.0 owner wrote for a serving shard without a
        // shared recovery frontier (field order is the 0.10.0 struct order).
        let written_by_0_10_0 = br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"lease_id":42,"state":3,"endpoint":"10.0.0.1:7000","checkpoint":null,"log":null,"durable_lsn":0}"#;
        let decoded = decode_logical_shard_record_value(written_by_0_10_0).unwrap();
        assert_eq!(decoded.wire, LogicalShardRecordWireKind::Routing);
        assert!(!decoded.record.has_recovery_state());
        assert_eq!(
            decoded.record.owner_epoch,
            Some(OwnerEpoch::new(7).unwrap())
        );
        assert_eq!(decoded.record.state, LogicalShardState::Serving);
        // Re-encoding through this crate reproduces the same bytes, so a
        // record that predates the split is canonical without a rewrite.
        assert_eq!(
            encode_logical_shard_routing_record(&decoded.record).unwrap(),
            written_by_0_10_0
        );
    }

    #[test]
    fn legacy_combined_values_are_read_and_resplit() {
        // Exact bytes a 0.11.0 owner wrote (combined version 3) for the two
        // fixture records above.
        let serving_v3 = br#"{"version":3,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"lease_id":42,"state":3,"endpoint":"10.0.0.1:7000","checkpoint":{"object_key":"checkpoints/7","lsn":128,"image_bytes":4096,"image_digest":"sha256:image","digest":"state-128","receipt":[1,2,3]},"log":{"segments":[{"segment_key":"logs/129-144","first_lsn":129,"last_lsn":144,"digest":"state-144","receipt":[4,5,6]}],"durable_lsn":144,"digest":"state-144"},"durable_lsn":144,"pending_recovery_upload":null}"#;
        let recovering_v3 = br#"{"version":3,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"lease_id":42,"state":2,"endpoint":"10.0.0.1:7000","checkpoint":null,"log":null,"durable_lsn":0,"pending_recovery_upload":{"object_namespace_id":"03030303030303030303030303030303","first_lsn":1,"last_lsn":2,"previous_chain_digest":"0000000000000000000000000000000000000000000000000000000000000000","last_chain_digest":"1111111111111111111111111111111111111111111111111111111111111111","segment_digest":"2222222222222222222222222222222222222222222222222222222222222222","manifest_key":"nokv/recovery/log-segments/v1/manifest","receipt":[4,5,6],"plan":[1,2,3]}}"#;
        for (bytes, expected) in [
            (serving_v3.as_slice(), serving_record()),
            (recovering_v3.as_slice(), recovering_record_with_upload()),
        ] {
            let decoded = decode_logical_shard_record_value(bytes).unwrap();
            assert_eq!(
                decoded.wire,
                LogicalShardRecordWireKind::LegacyCombined { version: 3 }
            );
            assert_eq!(decoded.record, expected);
            // The very same value is unreadable by a 0.10.0 client; that is
            // the incompatibility the split removes on the next owner write.
            assert!(frozen_v0_10_0_reader::decode_logical_shard_record(bytes).is_err());
            assert_eq!(round_trip_split_record(&decoded.record), expected);
        }
    }

    #[test]
    fn recovery_state_codec_is_strict() {
        let empty = br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","checkpoint":null,"log":null,"durable_lsn":0,"pending_recovery_upload":null}"#;
        assert!(matches!(
            decode_logical_shard_recovery_state(empty),
            Err(ControlError::Codec(_))
        ));
        let recovery = encode_logical_shard_recovery_state(&serving_record())
            .unwrap()
            .unwrap();
        let mut foreign = decode_logical_shard_recovery_state(&recovery).unwrap();
        foreign.logical_shard_id = shard_id(9);
        assert!(matches!(
            LogicalShardRecord::unassigned(shard_id(2)).with_recovery_state(foreign),
            Err(ControlError::Codec(_))
        ));
        let mut trailing = recovery.clone();
        trailing.extend_from_slice(b" ");
        assert!(matches!(
            decode_logical_shard_recovery_state(&trailing),
            Err(ControlError::Codec(_))
        ));
    }

    #[test]
    fn strict_codecs_round_trip_final_records() {
        let placement = placement();
        assert_eq!(
            decode_root_placement(&encode_root_placement(&placement).unwrap()).unwrap(),
            placement
        );
        let binding = object_namespace_binding();
        assert_eq!(
            decode_root_object_namespace_binding(
                &encode_root_object_namespace_binding(&binding).unwrap()
            )
            .unwrap(),
            binding
        );
        let binding = agent_binding();
        assert_eq!(
            decode_root_agent_binding(&encode_root_agent_binding(&binding).unwrap()).unwrap(),
            binding
        );

        for record in [serving_record(), recovering_record_with_upload()] {
            assert_eq!(round_trip_split_record(&record), record);
        }
    }

    /// Persist a record the way the etcd backend does (routing value plus the
    /// optional recovery value) and read it back through both decoders.
    fn round_trip_split_record(record: &LogicalShardRecord) -> LogicalShardRecord {
        let routing = encode_logical_shard_routing_record(record).unwrap();
        let decoded = decode_logical_shard_record_value(&routing).unwrap();
        assert_eq!(decoded.wire, LogicalShardRecordWireKind::Routing);
        assert_eq!(decoded.record, record.routing_projection());
        match encode_logical_shard_recovery_state(record).unwrap() {
            None => {
                assert!(!record.has_recovery_state());
                decoded.record
            }
            Some(recovery) => decoded
                .record
                .with_recovery_state(decode_logical_shard_recovery_state(&recovery).unwrap())
                .unwrap(),
        }
    }

    #[test]
    fn codec_golden_bytes_freeze_durable_schema() {
        assert_eq!(
            encode_root_placement(&placement()).unwrap(),
            br#"{"version":1,"root_id":"01010101010101010101010101010101","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#
        );
        assert_eq!(
            encode_root_object_namespace_binding(&object_namespace_binding()).unwrap(),
            br#"{"version":1,"root_id":"01010101010101010101010101010101","object_namespace_id":"03030303030303030303030303030303"}"#
        );
        assert_eq!(
            encode_root_agent_binding(&agent_binding()).unwrap(),
            br#"{"version":1,"root_id":"01010101010101010101010101010101","agent_id":"04040404040404040404040404040404"}"#
        );
        assert_eq!(
            encode_logical_shard_routing_record(&LogicalShardRecord::unassigned(shard_id(2)))
                .unwrap(),
            br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#
        );
        assert_eq!(
            encode_logical_shard_recovery_state(&LogicalShardRecord::unassigned(shard_id(2)))
                .unwrap(),
            None
        );
        assert_eq!(
            decode_logical_shard_record(
                br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#
            )
            .unwrap(),
            LogicalShardRecord::unassigned(shard_id(2)),
            "the explicit v1 decoder installs no pending upload"
        );
        assert_eq!(
            decode_logical_shard_record(
                br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0,"pending_recovery_upload":null}"#
            )
            .unwrap(),
            LogicalShardRecord::unassigned(shard_id(2)),
            "the explicit v2 decoder remains readable when no legacy checkpoint exists"
        );
    }

    #[test]
    fn legacy_checkpoint_without_receipt_fails_closed() {
        for encoded in [
            br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"lease_id":42,"state":3,"endpoint":"10.0.0.1:7000","checkpoint":{"object_key":"checkpoints/7","lsn":128,"image_bytes":4096,"image_digest":"sha256:image","digest":"state-128"},"log":null,"durable_lsn":128}"#.as_slice(),
            br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"lease_id":42,"state":3,"endpoint":"10.0.0.1:7000","checkpoint":{"object_key":"checkpoints/7","lsn":128,"image_bytes":4096,"image_digest":"sha256:image","digest":"state-128"},"log":null,"durable_lsn":128,"pending_recovery_upload":null}"#.as_slice(),
        ] {
            let error = decode_logical_shard_record(encoded)
                .expect_err("a checkpoint without a receipt cannot be located after restart");
            assert!(error.to_string().contains("checkpoint lacks a recovery receipt"));
        }
    }

    #[test]
    fn codec_rejects_unknown_versions_with_a_typed_upgrade_error() {
        let bytes = br#"{"version":99,"root_id":"01010101010101010101010101010101","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#;
        assert!(matches!(
            decode_root_placement(bytes),
            Err(ControlError::UnsupportedRecordVersion {
                record: "root placement",
                version: 99,
                supported: 1,
            })
        ));

        let bytes = br#"{"version":99,"root_id":"01010101010101010101010101010101","agent_id":"04040404040404040404040404040404"}"#;
        assert!(matches!(
            decode_root_agent_binding(bytes),
            Err(ControlError::UnsupportedRecordVersion {
                record: "root Agent binding",
                ..
            })
        ));

        // A newer routing or recovery record must name the version gap before
        // any field parsing, so an old reader reports "upgrade me" instead of
        // an unknown-field parse error.
        let bytes = br#"{"version":4,"logical_shard_id":"02020202020202020202020202020202","future_field":true}"#;
        let error = decode_logical_shard_record(bytes).unwrap_err();
        assert!(matches!(
            error,
            ControlError::UnsupportedRecordVersion {
                record: "logical shard record",
                version: 4,
                supported: 3,
            }
        ));
        assert!(error.to_string().contains("must be upgraded"));
        let bytes = br#"{"version":2,"logical_shard_id":"02020202020202020202020202020202","future_field":true}"#;
        assert!(matches!(
            decode_logical_shard_recovery_state(bytes),
            Err(ControlError::UnsupportedRecordVersion {
                record: "logical shard recovery state",
                version: 2,
                supported: 1,
            })
        ));
    }

    #[test]
    fn agent_binding_codec_rejects_noncanonical_or_unknown_fields() {
        let uppercase = br#"{"version":1,"root_id":"01010101010101010101010101010101","agent_id":"ABABABABABABABABABABABABABABABAB"}"#;
        assert!(matches!(
            decode_root_agent_binding(uppercase),
            Err(ControlError::Codec(_))
        ));
        let unknown = br#"{"version":1,"root_id":"01010101010101010101010101010101","agent_id":"04040404040404040404040404040404","extra":true}"#;
        assert!(matches!(
            decode_root_agent_binding(unknown),
            Err(ControlError::Codec(_))
        ));
    }

    #[test]
    fn codec_rejects_unknown_enum_discriminants() {
        let bytes = br#"{"version":1,"root_id":"01010101010101010101010101010101","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":99}"#;
        assert!(matches!(
            decode_root_placement(bytes),
            Err(ControlError::Codec(_))
        ));

        let bytes = br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"lease_id":0,"state":99,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#;
        assert!(matches!(
            decode_logical_shard_record(bytes),
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

        let unknown = br#"{"version":1,"root_id":"01010101010101010101010101010101","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1,"unexpected":true}"#;
        assert!(matches!(
            decode_root_placement(unknown),
            Err(ControlError::Codec(_))
        ));
    }

    #[test]
    fn codec_rejects_incomplete_owner_tuple() {
        let bytes = br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":"node-a","owner_epoch":7,"lease_id":42,"state":3,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#;
        assert!(matches!(
            decode_logical_shard_record(bytes),
            Err(ControlError::InvalidRecord(_))
        ));
    }

    #[test]
    fn owner_session_codec_is_strict() {
        let lease = LogicalShardLease {
            logical_shard_id: shard_id(2),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            lease_id: 9,
        };
        let encoded = encode_owner_session(&lease).unwrap();
        assert_eq!(decode_owner_session(&encoded).unwrap(), lease);
        let mut trailing = encoded;
        trailing.push(b'!');
        assert!(matches!(
            decode_owner_session(&trailing),
            Err(ControlError::Codec(_))
        ));
    }
}
