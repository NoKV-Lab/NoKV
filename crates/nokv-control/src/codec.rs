use serde::{Deserialize, Serialize};

use crate::store::validate_logical_shard_record;
#[cfg(any(feature = "etcd", test))]
use crate::LogicalShardLease;
use crate::{
    CheckpointRef, ControlError, LogRef, LogSegmentRef, LogicalShardId, LogicalShardRecord,
    LogicalShardState, NodeId, ObjectNamespaceId, OwnerEpoch, PlacementGeneration, RootId,
    RootObjectNamespaceBinding, RootPlacement, RootPlacementLifecycle,
};

const ROOT_PLACEMENT_CODEC_VERSION: u8 = 1;
const ROOT_OBJECT_NAMESPACE_CODEC_VERSION: u8 = 1;
const LOGICAL_SHARD_RECORD_CODEC_VERSION: u8 = 1;
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
struct RootObjectNamespaceBindingWire {
    version: u8,
    root_id: String,
    object_namespace_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LogicalShardRecordWire {
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

pub fn encode_logical_shard_record(record: &LogicalShardRecord) -> Result<Vec<u8>, ControlError> {
    validate_logical_shard_record(record)?;
    serde_json::to_vec(&LogicalShardRecordWire {
        version: LOGICAL_SHARD_RECORD_CODEC_VERSION,
        logical_shard_id: encode_fixed_id(record.logical_shard_id.as_bytes()),
        owner: record.owner.as_ref().map(|owner| owner.as_str().to_owned()),
        owner_epoch: record.owner_epoch.map(OwnerEpoch::get),
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
    let wire: LogicalShardRecordWire = serde_json::from_slice(bytes).map_err(codec_error)?;
    require_version(
        "logical shard record",
        wire.version,
        LOGICAL_SHARD_RECORD_CODEC_VERSION,
    )?;
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

        let record = serving_record();
        assert_eq!(
            decode_logical_shard_record(&encode_logical_shard_record(&record).unwrap()).unwrap(),
            record
        );
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
            encode_logical_shard_record(&LogicalShardRecord::unassigned(shard_id(2))).unwrap(),
            br#"{"version":1,"logical_shard_id":"02020202020202020202020202020202","owner":null,"owner_epoch":null,"lease_id":0,"state":1,"endpoint":null,"checkpoint":null,"log":null,"durable_lsn":0}"#
        );
    }

    #[test]
    fn codec_rejects_unknown_versions() {
        let bytes = br#"{"version":99,"root_id":"01010101010101010101010101010101","logical_shard_id":"02020202020202020202020202020202","placement_generation":1,"lifecycle":1}"#;
        assert!(matches!(
            decode_root_placement(bytes),
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
