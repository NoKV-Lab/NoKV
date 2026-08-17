/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::{Arc, Mutex};

use nokv_control::{
    ControlError, ControlStore, LogRef, LogSegmentRef, LogicalShardLease, LogicalShardRecord,
    RecoveryPublication, RecoveryUploadIntent, MAX_RECOVERY_LOG_SEGMENTS,
};
use nokv_meta::workspace::{
    MetaError, MetaShard, RecoveryOutboxSegment, RecoveryState, MAX_RECOVERY_SEGMENT_RECORDS,
};
use nokv_meta_store::StoreError;
use nokv_object::{
    plan_recovery_log_segment, write_recovery_log_segment_from_plan, ArtifactObjectStore,
    ObjectError, RecoveryLogSegmentError, RecoveryLogSegmentIdentity, RecoveryLogSegmentPlan,
    RecoveryLogSegmentReceipt, DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE,
};
use nokv_protocol::{ConflictKind, ErrorCode, RpcFailure, WorkspaceRpcRequest};
use sha2::{Digest, Sha256};

use crate::{ExecutedRequest, WorkspaceRequestExecutor};

const MAX_SEGMENTS_PER_PUBLISH: usize = 16;

/// Owner-fenced publisher that makes the metadata outbox recoverable before a
/// request result can leave the shard.
pub struct RecoveryPublisher {
    control: Arc<dyn ControlStore>,
    lease: LogicalShardLease,
    meta: Arc<MetaShard>,
    objects: Arc<dyn ArtifactObjectStore>,
    object_namespace_id: nokv_types::ObjectNamespaceId,
    serialized: Mutex<()>,
}

/// Fail-closed recovery publication error.
#[derive(Debug)]
pub enum RecoveryPublisherError {
    InvalidState(String),
    Control(ControlError),
    Meta(MetaError),
    Object(RecoveryLogSegmentError),
    Backlog { remaining_after_lsn: u64 },
    Poisoned,
}

/// Executor adapter that repairs an earlier pending upload before dispatch and
/// publishes every local write before returning its response.
pub struct RecoveryPublishingExecutor {
    inner: Arc<dyn WorkspaceRequestExecutor>,
    publisher: Arc<RecoveryPublisher>,
}

impl RecoveryPublisher {
    pub fn new(
        control: Arc<dyn ControlStore>,
        lease: LogicalShardLease,
        meta: Arc<MetaShard>,
        objects: Arc<dyn ArtifactObjectStore>,
    ) -> Result<Self, RecoveryPublisherError> {
        let object_namespace_id = objects.object_namespace().ok_or_else(|| {
            RecoveryPublisherError::InvalidState(
                "recovery publisher requires a verified object namespace".to_owned(),
            )
        })?;
        if meta.logical_shard_id() != lease.logical_shard_id {
            return Err(RecoveryPublisherError::InvalidState(
                "recovery publisher metadata shard and owner lease differ".to_owned(),
            ));
        }
        Ok(Self {
            control,
            lease,
            meta,
            objects,
            object_namespace_id,
            serialized: Mutex::new(()),
        })
    }

    pub fn publish_current(&self) -> Result<LogicalShardRecord, RecoveryPublisherError> {
        let _guard = self
            .serialized
            .lock()
            .map_err(|_| RecoveryPublisherError::Poisoned)?;
        let required = self.meta.recovery_state()?;
        self.publish_required(required)
    }

    fn publish_required(
        &self,
        required: RecoveryState,
    ) -> Result<LogicalShardRecord, RecoveryPublisherError> {
        for _ in 0..MAX_SEGMENTS_PER_PUBLISH {
            // Recovery publication is also the ACK barrier. Even when the
            // local and durable frontiers already match, prove that this exact
            // owner session is still live before allowing a response to leave
            // the shard.
            let mut record = self.control.renew_owner(&self.lease)?;
            self.validate_control_frontier(&record, required)?;
            if record.pending_recovery_upload.is_none()
                && record.durable_lsn == required.applied_recovery_lsn
            {
                return Ok(record);
            }

            let (segment, encoded, plan, intent) = match record.pending_recovery_upload.clone() {
                Some(intent) => self.restore_pending_segment(&record, intent)?,
                None => self.plan_next_segment(&record, required)?,
            };
            record = self
                .control
                .prepare_recovery_upload(&self.lease, intent.clone())?;
            write_recovery_log_segment_from_plan(self.objects.as_ref(), &plan, &encoded)?;
            let publication = publication_after_segment(&record, &segment, &intent)?;
            record = self
                .control
                .finalize_recovery_upload(&self.lease, &intent, publication)?;
            if record.durable_lsn >= required.applied_recovery_lsn
                && record.pending_recovery_upload.is_none()
            {
                self.validate_control_frontier(&record, required)?;
                return Ok(record);
            }
        }
        Err(RecoveryPublisherError::Backlog {
            remaining_after_lsn: self
                .control
                .get_logical_shard(&self.lease.logical_shard_id)?
                .map_or(0, |record| record.durable_lsn),
        })
    }

    fn plan_next_segment(
        &self,
        record: &LogicalShardRecord,
        required: RecoveryState,
    ) -> Result<SegmentPlan, RecoveryPublisherError> {
        ensure_log_chain_capacity(record)?;
        let boundary = self.control_boundary(record)?;
        let remaining = required
            .applied_recovery_lsn
            .checked_sub(boundary.applied_recovery_lsn)
            .and_then(|remaining| usize::try_from(remaining).ok())
            .filter(|remaining| *remaining > 0)
            .ok_or_else(|| {
                RecoveryPublisherError::InvalidState(
                    "sampled recovery frontier has no unpublished records".to_owned(),
                )
            })?;
        let segment = self
            .meta
            .recovery_segment_after(boundary, remaining.min(MAX_RECOVERY_SEGMENT_RECORDS))?
            .ok_or_else(|| {
                RecoveryPublisherError::InvalidState(
                    "local recovery tail is ahead of control but no segment can be sealed"
                        .to_owned(),
                )
            })?;
        if segment.last_lsn > required.applied_recovery_lsn {
            return Err(RecoveryPublisherError::InvalidState(
                "sealed recovery segment advanced beyond the sampled ACK frontier".to_owned(),
            ));
        }
        let encoded = segment.encode().map_err(|error| {
            RecoveryPublisherError::InvalidState(format!(
                "cannot encode local recovery segment: {error}"
            ))
        })?;
        let identity = self.segment_identity(&segment, &encoded);
        let plan =
            plan_recovery_log_segment(identity, &encoded, DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE)?;
        let intent = intent_from_plan(&plan)?;
        Ok((segment, encoded, plan, intent))
    }

    fn restore_pending_segment(
        &self,
        record: &LogicalShardRecord,
        intent: RecoveryUploadIntent,
    ) -> Result<SegmentPlan, RecoveryPublisherError> {
        if intent.object_namespace_id != self.object_namespace_id {
            return Err(RecoveryPublisherError::InvalidState(
                "pending recovery upload belongs to another object namespace".to_owned(),
            ));
        }
        let boundary_lsn = intent.first_lsn.checked_sub(1).ok_or_else(|| {
            RecoveryPublisherError::InvalidState(
                "pending recovery upload starts at LSN zero".to_owned(),
            )
        })?;
        if boundary_lsn != record.durable_lsn {
            return Err(RecoveryPublisherError::InvalidState(
                "pending recovery upload is not adjacent to the durable control tail".to_owned(),
            ));
        }
        let boundary = RecoveryState {
            applied_recovery_lsn: boundary_lsn,
            chain_digest: decode_digest(&intent.previous_chain_digest)?,
        };
        let record_count = intent
            .last_lsn
            .checked_sub(intent.first_lsn)
            .and_then(|count| count.checked_add(1))
            .and_then(|count| usize::try_from(count).ok())
            .filter(|count| *count <= MAX_RECOVERY_SEGMENT_RECORDS)
            .ok_or_else(|| {
                RecoveryPublisherError::InvalidState(
                    "pending recovery upload record count is out of bounds".to_owned(),
                )
            })?;
        let segment = self
            .meta
            .recovery_segment_after(boundary, record_count)?
            .ok_or_else(|| {
                RecoveryPublisherError::InvalidState(
                    "pending recovery upload has no local outbox segment".to_owned(),
                )
            })?;
        if segment.last_lsn != intent.last_lsn
            || hex(&segment.last_chain_digest) != intent.last_chain_digest
        {
            return Err(RecoveryPublisherError::InvalidState(
                "pending recovery upload differs from the local outbox segment".to_owned(),
            ));
        }
        let encoded = segment.encode().map_err(|error| {
            RecoveryPublisherError::InvalidState(format!(
                "cannot encode pending recovery segment: {error}"
            ))
        })?;
        let identity = self.segment_identity(&segment, &encoded);
        if identity != identity_from_intent(&intent, self.lease.logical_shard_id)? {
            return Err(RecoveryPublisherError::InvalidState(
                "pending recovery upload identity differs from local bytes".to_owned(),
            ));
        }
        let plan = RecoveryLogSegmentPlan::decode(&intent.plan)?;
        let receipt = RecoveryLogSegmentReceipt::decode(&intent.receipt)?;
        let rebuilt = plan_recovery_log_segment(
            identity,
            &encoded,
            usize::try_from(plan.receipt().chunk_size()).map_err(|_| {
                RecoveryPublisherError::InvalidState(
                    "pending recovery chunk size does not fit this platform".to_owned(),
                )
            })?,
        )?;
        if plan.encode() != intent.plan
            || rebuilt.encode() != intent.plan
            || plan.receipt() != &receipt
            || rebuilt.receipt() != &receipt
            || plan.receipt().manifest_key()?.as_str() != intent.manifest_key
        {
            return Err(RecoveryPublisherError::InvalidState(
                "pending recovery upload plan is not the canonical plan for local bytes".to_owned(),
            ));
        }
        Ok((segment, encoded, plan, intent))
    }

    fn control_boundary(
        &self,
        record: &LogicalShardRecord,
    ) -> Result<RecoveryState, RecoveryPublisherError> {
        if record.durable_lsn == 0 {
            let first = self.meta.recovery_outbox_after(0, 1)?;
            let chain_digest = first
                .first()
                .map(|row| row.previous_chain_digest)
                .unwrap_or(self.meta.recovery_state()?.chain_digest);
            return Ok(RecoveryState {
                applied_recovery_lsn: 0,
                chain_digest,
            });
        }
        Ok(RecoveryState {
            applied_recovery_lsn: record.durable_lsn,
            chain_digest: decode_digest(&control_tail_digest(record)?)?,
        })
    }

    fn validate_control_frontier(
        &self,
        record: &LogicalShardRecord,
        required: RecoveryState,
    ) -> Result<(), RecoveryPublisherError> {
        if record.logical_shard_id != self.lease.logical_shard_id {
            return Err(RecoveryPublisherError::InvalidState(
                "control returned another logical shard".to_owned(),
            ));
        }
        if record.durable_lsn > required.applied_recovery_lsn {
            return Err(RecoveryPublisherError::InvalidState(format!(
                "control recovery LSN {} is ahead of local LSN {}",
                record.durable_lsn, required.applied_recovery_lsn
            )));
        }
        if record.durable_lsn == required.applied_recovery_lsn
            && record.durable_lsn != 0
            && decode_digest(&control_tail_digest(record)?)? != required.chain_digest
        {
            return Err(RecoveryPublisherError::InvalidState(
                "control and local recovery digests differ at the same LSN".to_owned(),
            ));
        }
        Ok(())
    }

    fn segment_identity(
        &self,
        segment: &RecoveryOutboxSegment,
        encoded: &[u8],
    ) -> RecoveryLogSegmentIdentity {
        RecoveryLogSegmentIdentity::new(
            self.object_namespace_id,
            self.lease.logical_shard_id,
            segment.first_lsn,
            segment.last_lsn,
            segment.previous_chain_digest,
            segment.last_chain_digest,
            Sha256::digest(encoded).into(),
        )
    }
}

fn ensure_log_chain_capacity(record: &LogicalShardRecord) -> Result<(), RecoveryPublisherError> {
    let retained_segments = record.log.as_ref().map_or(0, |log| log.segments.len());
    if retained_segments >= MAX_RECOVERY_LOG_SEGMENTS {
        return Err(RecoveryPublisherError::InvalidState(format!(
            "shared recovery log already retains {retained_segments} segments; publish a checkpoint before appending another segment"
        )));
    }
    Ok(())
}

impl RecoveryPublishingExecutor {
    pub fn new(
        inner: Arc<dyn WorkspaceRequestExecutor>,
        publisher: Arc<RecoveryPublisher>,
    ) -> Self {
        Self { inner, publisher }
    }
}

impl WorkspaceRequestExecutor for RecoveryPublishingExecutor {
    fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
        self.publisher
            .publish_current()
            .map_err(RecoveryPublisherError::rpc_failure)?;
        let outcome = self.inner.execute(request);
        self.publisher
            .publish_current()
            .map_err(RecoveryPublisherError::rpc_failure)?;
        outcome
    }
}

impl RecoveryPublisherError {
    fn terminal(&self) -> bool {
        match self {
            Self::Control(ControlError::Backend(_))
            | Self::Meta(MetaError::Store {
                source: StoreError::Unavailable(_),
                ..
            })
            | Self::Object(RecoveryLogSegmentError::CreateOutcomeUnknown { .. })
            | Self::Object(RecoveryLogSegmentError::Object(ObjectError::CreateAmbiguous {
                ..
            }))
            | Self::Object(RecoveryLogSegmentError::Object(ObjectError::Backend {
                retryable: true,
                ..
            }))
            | Self::Backlog { .. } => false,
            Self::Object(RecoveryLogSegmentError::Object(ObjectError::Backend {
                retryable: false,
                ..
            })) => true,
            _ => true,
        }
    }

    pub(crate) fn retryable(&self) -> bool {
        !self.terminal()
    }

    fn rpc_failure(self) -> RpcFailure {
        let terminal = self.terminal();
        let object_unavailable = matches!(
            &self,
            Self::Object(RecoveryLogSegmentError::Object(ObjectError::Backend { .. }))
        );
        RpcFailure {
            code: if terminal {
                ErrorCode::NotOwner
            } else if object_unavailable {
                ErrorCode::ObjectUnavailable
            } else {
                ErrorCode::Internal
            },
            message: self.to_string(),
            retryable: !terminal,
            conflict: terminal.then_some(ConflictKind::RootPlacement),
            current_generation: None,
            route_hint: None,
        }
    }
}

impl From<ControlError> for RecoveryPublisherError {
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<MetaError> for RecoveryPublisherError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<RecoveryLogSegmentError> for RecoveryPublisherError {
    fn from(error: RecoveryLogSegmentError) -> Self {
        Self::Object(error)
    }
}

impl fmt::Display for RecoveryPublisherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState(reason) => {
                write!(formatter, "recovery publication invariant failed: {reason}")
            }
            Self::Control(error) => {
                write!(formatter, "recovery control publication failed: {error}")
            }
            Self::Meta(error) => write!(formatter, "recovery outbox read failed: {error}"),
            Self::Object(error) => write!(formatter, "recovery object publication failed: {error}"),
            Self::Backlog {
                remaining_after_lsn,
            } => write!(
                formatter,
                "recovery publication budget ended after durable LSN {remaining_after_lsn}"
            ),
            Self::Poisoned => formatter.write_str("recovery publisher lock is poisoned"),
        }
    }
}

impl std::error::Error for RecoveryPublisherError {}

type SegmentPlan = (
    RecoveryOutboxSegment,
    Vec<u8>,
    RecoveryLogSegmentPlan,
    RecoveryUploadIntent,
);

fn intent_from_plan(
    plan: &RecoveryLogSegmentPlan,
) -> Result<RecoveryUploadIntent, RecoveryPublisherError> {
    let receipt = plan.receipt();
    let identity = receipt.identity();
    Ok(RecoveryUploadIntent {
        object_namespace_id: identity.object_namespace(),
        first_lsn: identity.first_lsn(),
        last_lsn: identity.last_lsn(),
        previous_chain_digest: hex(&identity.previous_chain_digest()),
        last_chain_digest: hex(&identity.last_chain_digest()),
        segment_digest: hex(&identity.segment_digest()),
        manifest_key: receipt.manifest_key()?.as_str().to_owned(),
        receipt: receipt.encode(),
        plan: plan.encode(),
    })
}

fn identity_from_intent(
    intent: &RecoveryUploadIntent,
    logical_shard_id: nokv_types::LogicalShardId,
) -> Result<RecoveryLogSegmentIdentity, RecoveryPublisherError> {
    Ok(RecoveryLogSegmentIdentity::new(
        intent.object_namespace_id,
        logical_shard_id,
        intent.first_lsn,
        intent.last_lsn,
        decode_digest(&intent.previous_chain_digest)?,
        decode_digest(&intent.last_chain_digest)?,
        decode_digest(&intent.segment_digest)?,
    ))
}

fn publication_after_segment(
    record: &LogicalShardRecord,
    segment: &RecoveryOutboxSegment,
    intent: &RecoveryUploadIntent,
) -> Result<RecoveryPublication, RecoveryPublisherError> {
    if record.pending_recovery_upload.as_ref() != Some(intent) {
        return Err(RecoveryPublisherError::InvalidState(
            "control did not retain the exact recovery upload intent".to_owned(),
        ));
    }
    let mut segments = record
        .log
        .as_ref()
        .map(|log| log.segments.clone())
        .unwrap_or_default();
    segments.push(LogSegmentRef {
        segment_key: intent.manifest_key.clone(),
        first_lsn: segment.first_lsn,
        last_lsn: segment.last_lsn,
        digest: intent.last_chain_digest.clone(),
        receipt: intent.receipt.clone(),
    });
    Ok(RecoveryPublication {
        // RecoveryPublication is a patch: `None` retains the current checkpoint.
        // Re-sending an older checkpoint after the log advances would make a
        // later segment fail only after its immutable objects were created.
        checkpoint: None,
        log: Some(LogRef {
            segments,
            durable_lsn: segment.last_lsn,
            digest: intent.last_chain_digest.clone(),
        }),
        durable_lsn: segment.last_lsn,
    })
}

fn control_tail_digest(record: &LogicalShardRecord) -> Result<String, RecoveryPublisherError> {
    let checkpoint = record
        .checkpoint
        .as_ref()
        .filter(|checkpoint| checkpoint.lsn == record.durable_lsn)
        .map(|checkpoint| checkpoint.digest.as_str());
    let log = record
        .log
        .as_ref()
        .filter(|log| log.durable_lsn == record.durable_lsn)
        .map(|log| log.digest.as_str());
    if let (Some(checkpoint), Some(log)) = (checkpoint, log) {
        if checkpoint != log {
            return Err(RecoveryPublisherError::InvalidState(
                "checkpoint and log digests differ at the durable control tail".to_owned(),
            ));
        }
    }
    log.or(checkpoint).map(str::to_owned).ok_or_else(|| {
        RecoveryPublisherError::InvalidState(
            "non-zero control recovery tail has no digest-bearing reference".to_owned(),
        )
    })
}

fn hex(bytes: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize] as char);
        encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn decode_digest(value: &str) -> Result<[u8; 32], RecoveryPublisherError> {
    if value.len() != 64 {
        return Err(RecoveryPublisherError::InvalidState(
            "recovery digest is not canonical SHA-256 hex".to_owned(),
        ));
    }
    let mut decoded = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (decode_hex_digit(pair[0])? << 4) | decode_hex_digit(pair[1])?;
    }
    Ok(decoded)
}

fn decode_hex_digit(value: u8) -> Result<u8, RecoveryPublisherError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(RecoveryPublisherError::InvalidState(
            "recovery digest is not canonical SHA-256 hex".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use nokv_control::{
        InMemoryControlStore, LogRef, LogSegmentRef, LogicalShardRecord, NodeId, OwnerEpoch,
        MAX_RECOVERY_LOG_SEGMENTS,
    };
    use nokv_object::{ensure_object_namespace, BoundArtifactStore, MemoryArtifactStore};
    use nokv_types::{LogicalShardId, ObjectNamespaceId, FIXED_ID_BYTES};

    use super::*;

    #[test]
    fn publication_segment_stops_at_the_sampled_ack_frontier() {
        let logical_shard_id = LogicalShardId::from_bytes([7; FIXED_ID_BYTES]);
        let meta = crate::test_support::meta_shard(logical_shard_id);
        let first_epoch = OwnerEpoch::new(1).unwrap();
        meta.advance_owner_epoch(None, first_epoch).unwrap();
        let sampled = meta.recovery_state().unwrap();
        meta.advance_owner_epoch(first_epoch.into(), OwnerEpoch::new(2).unwrap())
            .unwrap();
        assert!(meta.recovery_state().unwrap().applied_recovery_lsn > sampled.applied_recovery_lsn);

        let namespace = ObjectNamespaceId::from_bytes([9; FIXED_ID_BYTES]);
        let raw = MemoryArtifactStore::new();
        ensure_object_namespace(&raw, namespace).unwrap();
        let objects: Arc<dyn ArtifactObjectStore> =
            Arc::new(BoundArtifactStore::open(raw, namespace).unwrap());
        let control: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
        let publisher = RecoveryPublisher::new(
            control,
            LogicalShardLease {
                logical_shard_id,
                owner: NodeId::new("sampled-owner").unwrap(),
                owner_epoch: first_epoch,
                lease_id: 1,
            },
            meta,
            objects,
        )
        .unwrap();
        let record = LogicalShardRecord::unassigned(logical_shard_id);

        let (segment, _, _, intent) = publisher.plan_next_segment(&record, sampled).unwrap();
        assert_eq!(segment.last_lsn, sampled.applied_recovery_lsn);
        assert_eq!(intent.last_lsn, sampled.applied_recovery_lsn);
    }

    #[test]
    fn full_control_log_chain_fails_before_planning_an_object_write() {
        let segments = (1..=MAX_RECOVERY_LOG_SEGMENTS)
            .map(|lsn| LogSegmentRef {
                segment_key: format!("logs/{lsn}-{lsn}"),
                first_lsn: lsn as u64,
                last_lsn: lsn as u64,
                digest: format!("state-{lsn}"),
                receipt: vec![1],
            })
            .collect::<Vec<_>>();
        let mut record =
            LogicalShardRecord::unassigned(LogicalShardId::from_bytes([7; FIXED_ID_BYTES]));
        record.durable_lsn = segments.last().unwrap().last_lsn;
        record.log = Some(LogRef {
            durable_lsn: record.durable_lsn,
            digest: segments.last().unwrap().digest.clone(),
            segments,
        });

        let error = ensure_log_chain_capacity(&record)
            .expect_err("a full chain must fail before plan or object creation");
        assert!(matches!(error, RecoveryPublisherError::InvalidState(_)));
        assert!(!error.retryable());
    }
}
