use serde::{Deserialize, Serialize};

use crate::{ControlError, ShardRecord};

const SHARD_RECORD_CODEC_VERSION: u8 = 1;

#[derive(Serialize, Deserialize)]
struct ShardRecordEnvelope {
    version: u8,
    record: ShardRecord,
}

pub fn encode_shard_record(record: &ShardRecord) -> Result<Vec<u8>, ControlError> {
    let envelope = ShardRecordEnvelope {
        version: SHARD_RECORD_CODEC_VERSION,
        record: record.clone(),
    };
    serde_json::to_vec(&envelope).map_err(|err| ControlError::Codec(err.to_string()))
}

pub fn decode_shard_record(bytes: &[u8]) -> Result<ShardRecord, ControlError> {
    let envelope: ShardRecordEnvelope =
        serde_json::from_slice(bytes).map_err(|err| ControlError::Codec(err.to_string()))?;
    if envelope.version != SHARD_RECORD_CODEC_VERSION {
        return Err(ControlError::Codec(format!(
            "unsupported shard record codec version {}",
            envelope.version
        )));
    }
    Ok(envelope.record)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CheckpointRef, LogRef, LogSegmentRef, NodeId, ShardId, ShardRecord, ShardState};

    #[test]
    fn shard_record_codec_round_trips_control_state() {
        let record = ShardRecord {
            shard_id: ShardId::new("mount-1:/dataset"),
            owner: Some(NodeId::new("node-a")),
            epoch: 7,
            lease_id: 42,
            state: ShardState::Serving,
            checkpoint: Some(CheckpointRef {
                object_key: "meta/checkpoints/7".to_owned(),
                lsn: 128,
                image_bytes: 4096,
                image_digest: "sha256:image".to_owned(),
                digest: "ckpt-digest".to_owned(),
            }),
            log: Some(LogRef {
                segments: vec![LogSegmentRef {
                    segment_key: "meta/logs/segment".to_owned(),
                    first_lsn: 129,
                    last_lsn: 144,
                    digest: "log-digest".to_owned(),
                }],
                durable_lsn: 144,
                digest: "log-digest".to_owned(),
            }),
            durable_lsn: 144,
            ever_served: true,
            endpoint: Some("10.0.0.1:7000".to_owned()),
            prefix: "/dataset".to_owned(),
            shard_index: 4,
            subtree_root_inode: Some(0x0004_0000_0000_0002),
        };

        let encoded = encode_shard_record(&record).unwrap();
        assert_eq!(decode_shard_record(&encoded).unwrap(), record);
    }

    #[test]
    fn shard_record_codec_rejects_unknown_version() {
        let bytes = br#"{"version":99,"record":{"shard_id":"s","owner":null,"epoch":0,"lease_id":0,"state":"unassigned","checkpoint":null,"log":null,"durable_lsn":0}}"#;

        let err = decode_shard_record(bytes).unwrap_err();

        assert!(
            matches!(err, ControlError::Codec(message) if message.contains("unsupported shard record codec version"))
        );
    }

    #[test]
    fn legacy_shard_record_defaults_to_ever_served_fail_closed() {
        let bytes = br#"{"version":1,"record":{"shard_id":"mount-1:/","owner":null,"epoch":3,"lease_id":7,"state":"unassigned","checkpoint":null,"log":null,"durable_lsn":0}}"#;

        let record = decode_shard_record(bytes).unwrap();

        assert!(record.ever_served);
    }
}
