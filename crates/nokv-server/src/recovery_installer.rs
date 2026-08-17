/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Receipt-directed installation of one exact control-plane recovery log.
//!
//! Recovery never discovers authority from provider state. The immutable
//! receipts already committed in [`LogicalShardRecord`] are the sole object
//! lookup inputs; every recovered mutation is replayed through [`MetaShard`].

use std::fmt;

use nokv_control::{CheckpointRef, LogSegmentRef, LogicalShardRecord, RecoveryUploadIntent};
use nokv_meta::workspace::{
    MetaError, MetaShard, RecoveryCodecError, RecoveryFsckReport, RecoveryOutboxSegment,
    RecoveryState, MAX_RECOVERY_SEGMENT_RECORDS,
};
use nokv_object::{
    plan_recovery_log_segment, read_recovery_log_segment, ArtifactObjectStore, ObjectDeleteOutcome,
    ObjectError, RecoveryCheckpointBlobReceipt, RecoveryCheckpointError, RecoveryLogSegmentError,
    RecoveryLogSegmentIdentity, RecoveryLogSegmentPlan, RecoveryLogSegmentReceipt,
};
use nokv_types::{LogicalShardId, ObjectNamespaceId, SHA256_BYTES};
use sha2::{Digest, Sha256};

/// Maximum number of immutable log segments accepted by one log-only install.
pub const MAX_RECOVERY_INSTALL_SEGMENTS: usize = nokv_control::MAX_RECOVERY_LOG_SEGMENTS;
/// Maximum aggregate encoded receipt bytes accepted by one log-only install.
pub const MAX_RECOVERY_INSTALL_RECEIPT_BYTES: usize = nokv_control::MAX_RECOVERY_LOG_RECEIPT_BYTES;
/// Maximum aggregate recovered segment payload bytes accepted by one install.
pub const MAX_RECOVERY_INSTALL_PAYLOAD_BYTES: u64 = nokv_control::MAX_RECOVERY_LOG_SEGMENTS as u64
    * nokv_object::MAX_RECOVERY_LOG_SEGMENT_BYTES as u64;

/// Verified outcome of one fresh or resumable log-only recovery installation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryInstallationReport {
    pub initial_state: RecoveryState,
    pub final_state: RecoveryState,
    pub verified_segments: usize,
    pub payload_bytes: u64,
    pub recovered_pending_upload: bool,
    pub fsck: RecoveryFsckReport,
}

/// Read-only proof that one local recovery chain contains the exact durable
/// Control prefix and, when present, the exact pending upload segment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalRecoveryPrefixReport {
    pub local_state: RecoveryState,
    pub control_state: RecoveryState,
    pub pending_state: Option<RecoveryState>,
}

/// Strict read-only validation of every Control recovery reference, independent
/// of whether the local shard has installed that frontier yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryControlReferenceReport {
    pub durable_state: RecoveryState,
    pub pending_state: Option<RecoveryState>,
}

/// Definite immutable-object condition that permits abandoning an
/// unacknowledged pending upload while retaining the durable Control frontier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingRecoveryAbortReason {
    MissingObject,
    CorruptObject,
}

/// Owner-fenced resolution of one exact pending recovery upload.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingRecoveryInstallOutcome {
    NoPendingUpload,
    Installed {
        initial_state: RecoveryState,
        final_state: RecoveryState,
        payload_bytes: u64,
        fsck: RecoveryFsckReport,
    },
    CleanupRequired {
        reason: PendingRecoveryAbortReason,
    },
}

/// Confirmed exact-object cleanup completed before Control intent removal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PendingRecoveryCleanupReport {
    pub attempted: usize,
    pub deleted: usize,
    pub already_absent: usize,
}

/// Fail-closed validation, object-read, codec, or authoritative-replay error.
#[derive(Debug)]
pub enum RecoveryInstallerError {
    InvalidControl(String),
    Checkpoint(RecoveryCheckpointError),
    Object(RecoveryLogSegmentError),
    Codec(RecoveryCodecError),
    Meta(MetaError),
    PendingCleanup { key: String, source: ObjectError },
}

/// Prove, without object I/O or metadata writes, that `meta` contains the
/// exact recovery prefix committed by Control.
///
/// A local shard may be ahead of the durable Control frontier because its
/// outbox is published object-first. It may never be behind or divergent at
/// Control's boundary. Every durable object receipt is decoded and rebound to
/// the expected namespace, shard, LSN range, chain boundary, and derived key
/// before an owner acquisition may advance the local outbox.
pub fn validate_local_recovery_prefix(
    record: &LogicalShardRecord,
    object_namespace_id: ObjectNamespaceId,
    meta: &MetaShard,
) -> Result<LocalRecoveryPrefixReport, RecoveryInstallerError> {
    validate_local_recovery_prefix_with_pending(
        record,
        object_namespace_id,
        meta,
        PendingPrefixRequirement::ExactLocalSegment,
    )
}

/// Decode and canonically rebind every checkpoint, log, and pending receipt
/// before a fresh recovery is allowed to mutate local metadata.
pub fn validate_recovery_control_references(
    record: &LogicalShardRecord,
    object_namespace_id: ObjectNamespaceId,
    meta: &MetaShard,
) -> Result<RecoveryControlReferenceReport, RecoveryInstallerError> {
    if record.logical_shard_id != meta.logical_shard_id() {
        return Err(invalid_control(
            "control record and local metadata name different logical shards",
        ));
    }
    let local_state = meta.fsck_recovery()?.state;
    let genesis_digest = local_genesis_digest(meta, local_state)?;
    let checkpoint_state = record
        .checkpoint
        .as_ref()
        .map(|checkpoint| {
            validate_checkpoint_reference(checkpoint, object_namespace_id, record.logical_shard_id)
        })
        .transpose()?;
    let base_state = checkpoint_state.unwrap_or(RecoveryState {
        applied_recovery_lsn: 0,
        chain_digest: genesis_digest,
    });
    let control_state = validate_durable_log_receipts(record, object_namespace_id, base_state)?;
    if control_state.applied_recovery_lsn != record.durable_lsn {
        return Err(invalid_control(format!(
            "validated recovery reference tail LSN {} differs from Control durable LSN {}",
            control_state.applied_recovery_lsn, record.durable_lsn
        )));
    }
    validate_aggregate_reference_bounds(
        record
            .log
            .as_ref()
            .map_or(&[], |log| log.segments.as_slice()),
        record
            .pending_recovery_upload
            .as_ref()
            .map(|intent| intent.receipt.as_slice()),
    )?;
    let pending = record
        .pending_recovery_upload
        .as_ref()
        .map(|intent| {
            validate_pending_upload(
                intent,
                object_namespace_id,
                record.logical_shard_id,
                control_state
                    .applied_recovery_lsn
                    .checked_add(1)
                    .ok_or_else(|| invalid_control("Control durable recovery LSN is exhausted"))?,
                control_state.chain_digest,
            )
        })
        .transpose()?;
    Ok(RecoveryControlReferenceReport {
        durable_state: control_state,
        pending_state: pending.as_ref().map(|pending| RecoveryState {
            applied_recovery_lsn: pending.segment.identity.last_lsn(),
            chain_digest: pending.segment.identity.last_chain_digest(),
        }),
    })
}

/// Prove the exact durable Control prefix while validating, but not requiring
/// local installation of, a pending upload. Fresh log recovery uses this
/// before owner acquisition because only the durable frontier is authority.
pub fn validate_local_durable_recovery_prefix(
    record: &LogicalShardRecord,
    object_namespace_id: ObjectNamespaceId,
    meta: &MetaShard,
) -> Result<LocalRecoveryPrefixReport, RecoveryInstallerError> {
    validate_local_recovery_prefix_with_pending(
        record,
        object_namespace_id,
        meta,
        PendingPrefixRequirement::ControlOnly,
    )
}

#[derive(Clone, Copy)]
enum PendingPrefixRequirement {
    ControlOnly,
    ExactLocalSegment,
}

fn validate_local_recovery_prefix_with_pending(
    record: &LogicalShardRecord,
    object_namespace_id: ObjectNamespaceId,
    meta: &MetaShard,
    pending_requirement: PendingPrefixRequirement,
) -> Result<LocalRecoveryPrefixReport, RecoveryInstallerError> {
    if record.logical_shard_id != meta.logical_shard_id() {
        return Err(invalid_control(
            "control record and local metadata name different logical shards",
        ));
    }
    let local_state = meta.fsck_recovery()?.state;
    let genesis_digest = local_genesis_digest(meta, local_state)?;
    let checkpoint_state = record
        .checkpoint
        .as_ref()
        .map(|checkpoint| {
            validate_checkpoint_reference(checkpoint, object_namespace_id, record.logical_shard_id)
        })
        .transpose()?;
    let base_state = checkpoint_state.unwrap_or(RecoveryState {
        applied_recovery_lsn: 0,
        chain_digest: genesis_digest,
    });
    let control_state = validate_durable_log_receipts(record, object_namespace_id, base_state)?;
    if control_state.applied_recovery_lsn != record.durable_lsn {
        return Err(invalid_control(format!(
            "validated recovery reference tail LSN {} differs from Control durable LSN {}",
            control_state.applied_recovery_lsn, record.durable_lsn
        )));
    }

    if control_state.applied_recovery_lsn > local_state.applied_recovery_lsn {
        return Err(invalid_control(format!(
            "Control durable LSN {} is ahead of local recovery LSN {}",
            control_state.applied_recovery_lsn, local_state.applied_recovery_lsn
        )));
    }
    if control_state.applied_recovery_lsn == local_state.applied_recovery_lsn {
        if control_state.chain_digest != local_state.chain_digest {
            return Err(invalid_control(
                "local recovery digest differs at the Control durable LSN",
            ));
        }
    } else if control_state.applied_recovery_lsn != 0 {
        let start_after = control_state
            .applied_recovery_lsn
            .checked_sub(1)
            .expect("non-zero Control LSN has a predecessor");
        let boundary = meta.recovery_outbox_after(start_after, 1)?;
        let boundary = boundary.first().ok_or_else(|| {
            invalid_control("local recovery chain has no row at the Control durable LSN")
        })?;
        if boundary.recovery_lsn != control_state.applied_recovery_lsn
            || boundary.chain_digest != control_state.chain_digest
        {
            return Err(invalid_control(
                "local recovery chain diverges at the Control durable boundary",
            ));
        }
    }

    let validated_pending = record
        .pending_recovery_upload
        .as_ref()
        .map(|intent| {
            validate_pending_upload(
                intent,
                object_namespace_id,
                record.logical_shard_id,
                control_state
                    .applied_recovery_lsn
                    .checked_add(1)
                    .ok_or_else(|| invalid_control("Control durable recovery LSN is exhausted"))?,
                control_state.chain_digest,
            )
        })
        .transpose()?;
    let pending_state = match pending_requirement {
        PendingPrefixRequirement::ControlOnly => None,
        PendingPrefixRequirement::ExactLocalSegment => validated_pending
            .as_ref()
            .map(|pending| validate_local_pending_segment(meta, control_state, pending))
            .transpose()?,
    };

    Ok(LocalRecoveryPrefixReport {
        local_state,
        control_state,
        pending_state,
    })
}

/// Install the exact log-only frontier named by `record` into `meta`.
pub fn install_recovery_log(
    record: &LogicalShardRecord,
    objects: &dyn ArtifactObjectStore,
    meta: &MetaShard,
) -> Result<RecoveryInstallationReport, RecoveryInstallerError> {
    if record.checkpoint.is_some() {
        return Err(invalid_control(
            "log-only recovery cannot install a checkpoint frontier",
        ));
    }
    if record.logical_shard_id != meta.logical_shard_id() {
        return Err(invalid_control(
            "control record and target metadata name different logical shards",
        ));
    }
    let object_namespace_id = objects.object_namespace().ok_or_else(|| {
        invalid_control("log-only recovery requires a namespace-bound object store")
    })?;

    // Validate the local prefix before trusting it as a resumable boundary.
    let initial_fsck = meta.fsck_recovery()?;
    let initial_state = initial_fsck.state;
    let log_segments = match record.log.as_ref() {
        Some(log) => {
            if record.durable_lsn == 0 {
                return Err(invalid_control(
                    "zero durable LSN must not carry a recovery log reference",
                ));
            }
            if log.durable_lsn != record.durable_lsn {
                return Err(invalid_control(format!(
                    "log durable LSN {} differs from record durable LSN {}",
                    log.durable_lsn, record.durable_lsn
                )));
            }
            if log.segments.is_empty() {
                return Err(invalid_control(
                    "non-zero durable LSN requires at least one recovery segment",
                ));
            }
            log.segments.as_slice()
        }
        None if record.durable_lsn == 0 => &[],
        None => {
            return Err(invalid_control(
                "non-zero durable LSN requires a recovery log reference",
            ));
        }
    };
    validate_aggregate_reference_bounds(
        log_segments,
        record
            .pending_recovery_upload
            .as_ref()
            .map(|intent| intent.receipt.as_slice()),
    )?;

    let genesis_digest = if initial_state.applied_recovery_lsn == 0 {
        initial_state.chain_digest
    } else {
        let first = meta.recovery_outbox_after(0, 1)?;
        let first = first.first().ok_or_else(|| {
            invalid_control("non-empty target has no first recovery outbox record")
        })?;
        if first.recovery_lsn != 1 {
            return Err(invalid_control(format!(
                "target recovery chain begins at LSN {}, expected 1",
                first.recovery_lsn
            )));
        }
        first.previous_chain_digest
    };

    // Validate the complete receipt chain and all aggregate bounds before the
    // first object read or authoritative replay.
    let mut validated = Vec::with_capacity(log_segments.len());
    let mut expected_first_lsn = 1_u64;
    let mut expected_previous_digest = genesis_digest;
    let mut payload_bytes = 0_u64;
    for segment_ref in log_segments {
        let receipt = RecoveryLogSegmentReceipt::decode(&segment_ref.receipt)?;
        if receipt.encode() != segment_ref.receipt {
            return Err(invalid_control(
                "recovery segment receipt is not canonically encoded",
            ));
        }
        let identity = receipt.identity();
        if identity.object_namespace() != object_namespace_id {
            return Err(RecoveryLogSegmentError::ForeignNamespace.into());
        }
        if identity.logical_shard() != record.logical_shard_id {
            return Err(RecoveryLogSegmentError::ForeignShard.into());
        }
        if identity.first_lsn() != expected_first_lsn {
            return Err(invalid_control(format!(
                "recovery log expected first LSN {expected_first_lsn}, found {}",
                identity.first_lsn()
            )));
        }
        validate_control_segment(segment_ref, &receipt)?;
        if identity.previous_chain_digest() != expected_previous_digest {
            return Err(invalid_control(format!(
                "recovery segment at LSN {} does not follow the previous chain digest",
                identity.first_lsn()
            )));
        }
        validate_segment_record_count(identity)?;
        add_payload_bytes(&mut payload_bytes, receipt.segment_len())?;
        expected_first_lsn = identity
            .last_lsn()
            .checked_add(1)
            .ok_or_else(|| invalid_control("recovery log LSN tail overflows"))?;
        expected_previous_digest = identity.last_chain_digest();
        validated.push(ValidatedSegment { identity, receipt });
    }

    let durable_tail_lsn = expected_first_lsn
        .checked_sub(1)
        .expect("recovery installation always begins at LSN one");
    if durable_tail_lsn != record.durable_lsn {
        return Err(invalid_control(format!(
            "recovery segment tail LSN {durable_tail_lsn} differs from durable LSN {}",
            record.durable_lsn
        )));
    }
    if let Some(log) = record.log.as_ref() {
        if decode_digest(&log.digest, "log digest")? != expected_previous_digest {
            return Err(invalid_control(
                "recovery log digest differs from its final segment",
            ));
        }
    }

    let validated_pending = record
        .pending_recovery_upload
        .as_ref()
        .map(|intent| {
            validate_pending_upload(
                intent,
                object_namespace_id,
                record.logical_shard_id,
                expected_first_lsn,
                expected_previous_digest,
            )
        })
        .transpose()?;
    if let Some(pending) = validated_pending.as_ref() {
        add_payload_bytes(&mut payload_bytes, pending.segment.receipt.segment_len())?;
        expected_first_lsn = pending
            .segment
            .identity
            .last_lsn()
            .checked_add(1)
            .ok_or_else(|| invalid_control("recovery pending-upload LSN tail overflows"))?;
        expected_previous_digest = pending.segment.identity.last_chain_digest();
    }
    let expected_tail_lsn = expected_first_lsn
        .checked_sub(1)
        .expect("recovery installation always begins at LSN one");
    if initial_state.applied_recovery_lsn > expected_tail_lsn {
        return Err(invalid_control(format!(
            "target recovery LSN {} is ahead of required recovery LSN {expected_tail_lsn}",
            initial_state.applied_recovery_lsn
        )));
    }

    // A pending upload may already have completed every immutable create even
    // though Control has not finalized its durable log pointer. Verify and
    // decode that exact object before replaying any older row, so an incomplete
    // or corrupted pending upload leaves a fresh target untouched.
    let pending_segment = validated_pending
        .as_ref()
        .map(|pending| load_segment(objects, &pending.segment))
        .transpose()?;

    for validated_segment in &validated {
        let segment = load_segment(objects, validated_segment)?;
        meta.replay_recovery_segment(&segment)?;
    }
    if let Some(segment) = pending_segment.as_ref() {
        meta.replay_recovery_segment(segment)?;
    }

    let fsck = meta.fsck_recovery()?;
    let final_state = fsck.state;
    if final_state.applied_recovery_lsn != expected_tail_lsn
        || final_state.chain_digest != expected_previous_digest
    {
        return Err(invalid_control(format!(
            "installed recovery tail LSN {} does not match required recovery tail LSN {expected_tail_lsn}",
            final_state.applied_recovery_lsn
        )));
    }
    Ok(RecoveryInstallationReport {
        initial_state,
        final_state,
        verified_segments: validated.len() + usize::from(validated_pending.is_some()),
        payload_bytes,
        recovered_pending_upload: validated_pending.is_some(),
        fsck,
    })
}

/// Install only the durable Control log, deliberately excluding any pending
/// upload whose object publication has not yet been acknowledged by Control.
pub fn install_durable_recovery_log(
    record: &LogicalShardRecord,
    objects: &dyn ArtifactObjectStore,
    meta: &MetaShard,
) -> Result<RecoveryInstallationReport, RecoveryInstallerError> {
    if record.checkpoint.is_some() {
        return Err(invalid_control(
            "log-only recovery cannot install a checkpoint frontier",
        ));
    }
    let object_namespace_id = objects.object_namespace().ok_or_else(|| {
        invalid_control("log-only recovery requires a namespace-bound object store")
    })?;
    let references = validate_recovery_control_references(record, object_namespace_id, meta)?;
    let local_fsck = meta.fsck_recovery()?;
    if local_fsck.state.applied_recovery_lsn >= references.durable_state.applied_recovery_lsn {
        let proof = validate_local_durable_recovery_prefix(record, object_namespace_id, meta)?;
        if proof.control_state != references.durable_state {
            return Err(invalid_control(
                "durable recovery proof changed while validating a resumable target",
            ));
        }
        return Ok(RecoveryInstallationReport {
            initial_state: local_fsck.state,
            final_state: local_fsck.state,
            verified_segments: 0,
            payload_bytes: 0,
            recovered_pending_upload: false,
            fsck: local_fsck,
        });
    }
    let mut durable = record.clone();
    durable.pending_recovery_upload = None;
    install_recovery_log(&durable, objects, meta)
}

/// Resolve the exact pending upload after acquiring the Control owner fence.
///
/// A complete exact object is replayed authoritatively. Definite absence or
/// corruption is returned as a typed abort decision; transient provider and
/// local metadata failures remain errors and must not clear Control intent.
pub fn install_pending_recovery_upload(
    record: &LogicalShardRecord,
    objects: &dyn ArtifactObjectStore,
    meta: &MetaShard,
) -> Result<PendingRecoveryInstallOutcome, RecoveryInstallerError> {
    if record.checkpoint.is_some() {
        return Err(invalid_control(
            "log-only recovery cannot install a checkpoint frontier",
        ));
    }
    if record.logical_shard_id != meta.logical_shard_id() {
        return Err(invalid_control(
            "control record and target metadata name different logical shards",
        ));
    }
    let object_namespace_id = objects.object_namespace().ok_or_else(|| {
        invalid_control("pending recovery requires a namespace-bound object store")
    })?;
    let durable_proof = validate_local_durable_recovery_prefix(record, object_namespace_id, meta)?;
    let Some(intent) = record.pending_recovery_upload.as_ref() else {
        return Ok(PendingRecoveryInstallOutcome::NoPendingUpload);
    };
    let expected_first_lsn = durable_proof
        .control_state
        .applied_recovery_lsn
        .checked_add(1)
        .ok_or_else(|| invalid_control("Control durable recovery LSN is exhausted"))?;
    let pending = validate_pending_upload(
        intent,
        object_namespace_id,
        record.logical_shard_id,
        expected_first_lsn,
        durable_proof.control_state.chain_digest,
    )?;

    if durable_proof.local_state.applied_recovery_lsn >= pending.segment.identity.last_lsn() {
        let final_state =
            validate_local_pending_segment(meta, durable_proof.control_state, &pending)?;
        let fsck = meta.fsck_recovery()?;
        return Ok(PendingRecoveryInstallOutcome::Installed {
            initial_state: durable_proof.local_state,
            final_state,
            payload_bytes: 0,
            fsck,
        });
    }
    if durable_proof.local_state != durable_proof.control_state {
        return Err(invalid_control(
            "local recovery tail stops inside the pending upload LSN range",
        ));
    }

    let payload_bytes = pending.segment.receipt.segment_len();
    add_payload_bytes(&mut 0_u64, payload_bytes)?;
    let segment = match load_pending_segment(objects, &pending.segment)? {
        Ok(segment) => segment,
        Err(reason) => {
            return Ok(PendingRecoveryInstallOutcome::CleanupRequired { reason });
        }
    };
    meta.replay_recovery_segment(&segment)?;
    let final_state = validate_local_pending_segment(meta, durable_proof.control_state, &pending)?;
    let fsck = meta.fsck_recovery()?;
    Ok(PendingRecoveryInstallOutcome::Installed {
        initial_state: durable_proof.local_state,
        final_state,
        payload_bytes,
        fsck,
    })
}

/// Delete every exact key named by the canonical pending plan.
///
/// Control intent must remain durable until this returns success. A partial or
/// ambiguous cleanup is retryable from the same plan because both `Deleted`
/// and `Absent` are accepted terminal outcomes per key.
pub fn cleanup_pending_recovery_upload(
    record: &LogicalShardRecord,
    objects: &dyn ArtifactObjectStore,
    meta: &MetaShard,
) -> Result<PendingRecoveryCleanupReport, RecoveryInstallerError> {
    if record.checkpoint.is_some() {
        return Err(invalid_control(
            "log-only recovery cannot clean a checkpoint frontier",
        ));
    }
    let object_namespace_id = objects.object_namespace().ok_or_else(|| {
        invalid_control("pending recovery cleanup requires a namespace-bound object store")
    })?;
    let proof = validate_local_durable_recovery_prefix(record, object_namespace_id, meta)?;
    let intent = record
        .pending_recovery_upload
        .as_ref()
        .ok_or_else(|| invalid_control("pending recovery cleanup requires an exact intent"))?;
    let pending = validate_pending_upload(
        intent,
        object_namespace_id,
        record.logical_shard_id,
        proof
            .control_state
            .applied_recovery_lsn
            .checked_add(1)
            .ok_or_else(|| invalid_control("Control durable recovery LSN is exhausted"))?,
        proof.control_state.chain_digest,
    )?;

    let mut deleted = 0_usize;
    let mut already_absent = 0_usize;
    for key in pending.plan.cleanup_keys() {
        match objects.delete(key) {
            Ok(ObjectDeleteOutcome::Deleted) => deleted += 1,
            Ok(ObjectDeleteOutcome::Absent) => already_absent += 1,
            Err(source) => {
                return Err(RecoveryInstallerError::PendingCleanup {
                    key: key.as_str().to_owned(),
                    source,
                });
            }
        }
    }
    Ok(PendingRecoveryCleanupReport {
        attempted: pending.plan.cleanup_keys().len(),
        deleted,
        already_absent,
    })
}

struct ValidatedSegment {
    identity: RecoveryLogSegmentIdentity,
    receipt: RecoveryLogSegmentReceipt,
}

struct ValidatedPending {
    segment: ValidatedSegment,
    plan: RecoveryLogSegmentPlan,
}

fn local_genesis_digest(
    meta: &MetaShard,
    local_state: RecoveryState,
) -> Result<[u8; SHA256_BYTES], RecoveryInstallerError> {
    if local_state.applied_recovery_lsn == 0 {
        return Ok(local_state.chain_digest);
    }
    let first = meta.recovery_outbox_after(0, 1)?;
    let first = first
        .first()
        .ok_or_else(|| invalid_control("non-empty local shard has no first recovery row"))?;
    if first.recovery_lsn != 1 {
        return Err(invalid_control(format!(
            "local recovery chain begins at LSN {}, expected 1",
            first.recovery_lsn
        )));
    }
    Ok(first.previous_chain_digest)
}

fn validate_checkpoint_reference(
    checkpoint: &CheckpointRef,
    object_namespace_id: ObjectNamespaceId,
    logical_shard_id: LogicalShardId,
) -> Result<RecoveryState, RecoveryInstallerError> {
    let receipt = RecoveryCheckpointBlobReceipt::decode(&checkpoint.receipt)?;
    if receipt.encode() != checkpoint.receipt {
        return Err(invalid_control(
            "checkpoint receipt is not canonically encoded",
        ));
    }
    let identity = receipt.identity();
    if identity.object_namespace() != object_namespace_id {
        return Err(RecoveryCheckpointError::ForeignNamespace.into());
    }
    if identity.logical_shard() != logical_shard_id {
        return Err(RecoveryCheckpointError::ForeignShard.into());
    }
    let boundary = identity.boundary();
    let state_digest = decode_digest(&checkpoint.digest, "checkpoint state digest")?;
    if boundary.recovery_lsn() != checkpoint.lsn || boundary.chain_digest() != state_digest {
        return Err(RecoveryCheckpointError::ForeignBoundary.into());
    }
    if receipt.manifest_key()?.as_str() != checkpoint.object_key {
        return Err(invalid_control(
            "checkpoint object key differs from its exact receipt manifest key",
        ));
    }
    if receipt.envelope_len() != checkpoint.image_bytes
        || receipt.envelope_digest()
            != decode_digest(&checkpoint.image_digest, "checkpoint image digest")?
    {
        return Err(invalid_control(
            "checkpoint image length or digest differs from its exact receipt",
        ));
    }
    Ok(RecoveryState {
        applied_recovery_lsn: checkpoint.lsn,
        chain_digest: state_digest,
    })
}

fn validate_durable_log_receipts(
    record: &LogicalShardRecord,
    object_namespace_id: ObjectNamespaceId,
    base_state: RecoveryState,
) -> Result<RecoveryState, RecoveryInstallerError> {
    let Some(log) = record.log.as_ref() else {
        return Ok(base_state);
    };
    if log.segments.is_empty() {
        return Err(invalid_control("durable recovery log is empty"));
    }
    validate_aggregate_reference_bounds(
        &log.segments,
        record
            .pending_recovery_upload
            .as_ref()
            .map(|intent| intent.receipt.as_slice()),
    )?;
    let mut expected_first_lsn = base_state
        .applied_recovery_lsn
        .checked_add(1)
        .ok_or_else(|| invalid_control("checkpoint recovery LSN is exhausted"))?;
    let mut expected_previous_digest = base_state.chain_digest;
    for segment_ref in &log.segments {
        let receipt = RecoveryLogSegmentReceipt::decode(&segment_ref.receipt)?;
        if receipt.encode() != segment_ref.receipt {
            return Err(invalid_control(
                "durable log receipt is not canonically encoded",
            ));
        }
        let identity = receipt.identity();
        if identity.object_namespace() != object_namespace_id {
            return Err(RecoveryLogSegmentError::ForeignNamespace.into());
        }
        if identity.logical_shard() != record.logical_shard_id {
            return Err(RecoveryLogSegmentError::ForeignShard.into());
        }
        if identity.first_lsn() != expected_first_lsn {
            return Err(invalid_control(format!(
                "durable recovery log expected first LSN {expected_first_lsn}, found {}",
                identity.first_lsn()
            )));
        }
        if identity.previous_chain_digest() != expected_previous_digest {
            return Err(invalid_control(format!(
                "durable recovery segment at LSN {} does not follow its predecessor digest",
                identity.first_lsn()
            )));
        }
        validate_control_segment(segment_ref, &receipt)?;
        validate_segment_record_count(identity)?;
        expected_first_lsn = identity
            .last_lsn()
            .checked_add(1)
            .ok_or_else(|| invalid_control("durable recovery log LSN tail overflows"))?;
        expected_previous_digest = identity.last_chain_digest();
    }
    let durable_lsn = expected_first_lsn
        .checked_sub(1)
        .expect("non-empty durable log advances the base LSN");
    if durable_lsn != log.durable_lsn {
        return Err(invalid_control(format!(
            "durable log receipt tail LSN {durable_lsn} differs from log LSN {}",
            log.durable_lsn
        )));
    }
    if decode_digest(&log.digest, "durable log digest")? != expected_previous_digest {
        return Err(invalid_control(
            "durable log digest differs from its final exact receipt",
        ));
    }
    Ok(RecoveryState {
        applied_recovery_lsn: durable_lsn,
        chain_digest: expected_previous_digest,
    })
}

fn validate_local_pending_segment(
    meta: &MetaShard,
    control_state: RecoveryState,
    pending: &ValidatedPending,
) -> Result<RecoveryState, RecoveryInstallerError> {
    let identity = pending.segment.identity;
    let local_state = meta.recovery_state()?;
    if local_state.applied_recovery_lsn < identity.last_lsn() {
        return Err(invalid_control(format!(
            "local recovery LSN {} does not contain pending upload tail LSN {}",
            local_state.applied_recovery_lsn,
            identity.last_lsn()
        )));
    }
    let record_count = identity
        .last_lsn()
        .checked_sub(identity.first_lsn())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| invalid_control("pending recovery LSN range overflows"))?;
    let segment = meta
        .recovery_segment_after(control_state, record_count)?
        .ok_or_else(|| invalid_control("local shard has no pending recovery segment"))?;
    validate_decoded_segment(&segment, identity)?;
    let encoded = segment.encode()?;
    let segment_digest: [u8; SHA256_BYTES] = Sha256::digest(&encoded).into();
    if segment_digest != identity.segment_digest() {
        return Err(RecoveryLogSegmentError::ForeignSegmentDigest.into());
    }
    let chunk_size = usize::try_from(pending.plan.receipt().chunk_size())
        .map_err(|_| invalid_control("pending recovery chunk size does not fit this platform"))?;
    let rebuilt = plan_recovery_log_segment(identity, &encoded, chunk_size)?;
    if rebuilt.encode() != pending.plan.encode() || rebuilt.receipt() != &pending.segment.receipt {
        return Err(invalid_control(
            "local pending segment is not the exact persisted object plan",
        ));
    }
    Ok(RecoveryState {
        applied_recovery_lsn: identity.last_lsn(),
        chain_digest: identity.last_chain_digest(),
    })
}

fn validate_pending_upload(
    intent: &RecoveryUploadIntent,
    object_namespace_id: nokv_types::ObjectNamespaceId,
    logical_shard_id: nokv_types::LogicalShardId,
    expected_first_lsn: u64,
    expected_previous_digest: [u8; SHA256_BYTES],
) -> Result<ValidatedPending, RecoveryInstallerError> {
    if intent.object_namespace_id != object_namespace_id {
        return Err(RecoveryLogSegmentError::ForeignNamespace.into());
    }
    let receipt = RecoveryLogSegmentReceipt::decode(&intent.receipt)?;
    if receipt.encode() != intent.receipt {
        return Err(invalid_control(
            "pending recovery receipt is not canonically encoded",
        ));
    }
    let plan = RecoveryLogSegmentPlan::decode(&intent.plan)?;
    if plan.encode() != intent.plan || plan.receipt() != &receipt {
        return Err(invalid_control(
            "pending recovery plan does not exactly bind its receipt",
        ));
    }
    let identity = receipt.identity();
    if identity.object_namespace() != object_namespace_id {
        return Err(RecoveryLogSegmentError::ForeignNamespace.into());
    }
    if identity.logical_shard() != logical_shard_id {
        return Err(RecoveryLogSegmentError::ForeignShard.into());
    }
    if identity.first_lsn() != intent.first_lsn || identity.last_lsn() != intent.last_lsn {
        return Err(RecoveryLogSegmentError::ForeignLsnRange.into());
    }
    if identity.previous_chain_digest()
        != decode_digest(
            &intent.previous_chain_digest,
            "pending previous-chain digest",
        )?
        || identity.last_chain_digest()
            != decode_digest(&intent.last_chain_digest, "pending last-chain digest")?
    {
        return Err(RecoveryLogSegmentError::ForeignChainBoundary.into());
    }
    if identity.segment_digest() != decode_digest(&intent.segment_digest, "pending segment digest")?
    {
        return Err(RecoveryLogSegmentError::ForeignSegmentDigest.into());
    }
    if identity.first_lsn() != expected_first_lsn {
        return Err(invalid_control(format!(
            "pending recovery upload starts at LSN {}, expected {expected_first_lsn}",
            identity.first_lsn()
        )));
    }
    if identity.previous_chain_digest() != expected_previous_digest {
        return Err(invalid_control(
            "pending recovery upload does not follow the durable chain digest",
        ));
    }
    validate_segment_record_count(identity)?;
    if receipt.manifest_key()?.as_str() != intent.manifest_key {
        return Err(invalid_control(
            "pending recovery manifest key differs from its exact receipt",
        ));
    }
    Ok(ValidatedPending {
        segment: ValidatedSegment { identity, receipt },
        plan,
    })
}

fn validate_segment_record_count(
    identity: RecoveryLogSegmentIdentity,
) -> Result<(), RecoveryInstallerError> {
    let record_count = identity
        .last_lsn()
        .checked_sub(identity.first_lsn())
        .and_then(|count| count.checked_add(1))
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| invalid_control("recovery segment LSN range overflows"))?;
    if record_count > MAX_RECOVERY_SEGMENT_RECORDS {
        return Err(invalid_control(format!(
            "recovery segment record count {record_count} exceeds maximum {MAX_RECOVERY_SEGMENT_RECORDS}"
        )));
    }
    Ok(())
}

fn add_payload_bytes(total: &mut u64, segment_len: u64) -> Result<(), RecoveryInstallerError> {
    *total = total
        .checked_add(segment_len)
        .ok_or_else(|| invalid_control("aggregate recovery payload bytes overflow"))?;
    if *total > MAX_RECOVERY_INSTALL_PAYLOAD_BYTES {
        return Err(invalid_control(format!(
            "aggregate recovery payload bytes {total} exceed maximum {MAX_RECOVERY_INSTALL_PAYLOAD_BYTES}"
        )));
    }
    Ok(())
}

fn load_segment(
    objects: &dyn ArtifactObjectStore,
    validated: &ValidatedSegment,
) -> Result<RecoveryOutboxSegment, RecoveryInstallerError> {
    let object = read_recovery_log_segment(objects, validated.identity, &validated.receipt)?;
    let segment = RecoveryOutboxSegment::decode(object.bytes())?;
    validate_decoded_segment(&segment, validated.identity)?;
    Ok(segment)
}

fn load_pending_segment(
    objects: &dyn ArtifactObjectStore,
    validated: &ValidatedSegment,
) -> Result<Result<RecoveryOutboxSegment, PendingRecoveryAbortReason>, RecoveryInstallerError> {
    let object = match read_recovery_log_segment(objects, validated.identity, &validated.receipt) {
        Ok(object) => object,
        Err(RecoveryLogSegmentError::Object(ObjectError::ObjectNotFound { .. })) => {
            return Ok(Err(PendingRecoveryAbortReason::MissingObject));
        }
        Err(
            RecoveryLogSegmentError::SegmentDigestMismatch
            | RecoveryLogSegmentError::InvalidManifest(_)
            | RecoveryLogSegmentError::Object(
                ObjectError::ImmutableCollision { .. }
                | ObjectError::DigestMismatch { .. }
                | ObjectError::InvalidManifest(_),
            ),
        ) => return Ok(Err(PendingRecoveryAbortReason::CorruptObject)),
        Err(error) => return Err(error.into()),
    };
    let segment = match RecoveryOutboxSegment::decode(object.bytes()) {
        Ok(segment) => segment,
        Err(_) => return Ok(Err(PendingRecoveryAbortReason::CorruptObject)),
    };
    if validate_decoded_segment(&segment, validated.identity).is_err() {
        return Ok(Err(PendingRecoveryAbortReason::CorruptObject));
    }
    Ok(Ok(segment))
}

fn validate_aggregate_reference_bounds(
    segments: &[LogSegmentRef],
    pending_receipt: Option<&[u8]>,
) -> Result<usize, RecoveryInstallerError> {
    let segment_count = segments
        .len()
        .checked_add(usize::from(pending_receipt.is_some()))
        .ok_or_else(|| invalid_control("recovery segment count overflows"))?;
    if segment_count > MAX_RECOVERY_INSTALL_SEGMENTS {
        return Err(invalid_control(format!(
            "recovery segment count {} exceeds maximum {}",
            segment_count, MAX_RECOVERY_INSTALL_SEGMENTS
        )));
    }
    let mut receipt_bytes = segments.iter().try_fold(0_usize, |total, segment| {
        total
            .checked_add(segment.receipt.len())
            .ok_or_else(|| invalid_control("aggregate recovery receipt bytes overflow"))
    })?;
    if let Some(receipt) = pending_receipt {
        receipt_bytes = receipt_bytes
            .checked_add(receipt.len())
            .ok_or_else(|| invalid_control("aggregate recovery receipt bytes overflow"))?;
    }
    if receipt_bytes > MAX_RECOVERY_INSTALL_RECEIPT_BYTES {
        return Err(invalid_control(format!(
            "aggregate recovery receipt bytes {receipt_bytes} exceed maximum {MAX_RECOVERY_INSTALL_RECEIPT_BYTES}"
        )));
    }
    Ok(receipt_bytes)
}

fn validate_control_segment(
    segment_ref: &LogSegmentRef,
    receipt: &RecoveryLogSegmentReceipt,
) -> Result<(), RecoveryInstallerError> {
    let identity = receipt.identity();
    if segment_ref.first_lsn != identity.first_lsn() || segment_ref.last_lsn != identity.last_lsn()
    {
        return Err(RecoveryLogSegmentError::ForeignLsnRange.into());
    }
    if decode_digest(&segment_ref.digest, "segment digest")? != identity.last_chain_digest() {
        return Err(RecoveryLogSegmentError::ForeignChainBoundary.into());
    }
    if identity.first_lsn() == 0 || identity.last_lsn() < identity.first_lsn() {
        return Err(invalid_control("recovery segment has an invalid LSN range"));
    }
    // Derivation validates the key syntax in addition to exact receipt binding.
    if receipt.manifest_key()?.as_str() != segment_ref.segment_key {
        return Err(invalid_control(
            "recovery segment key differs from its exact receipt manifest key",
        ));
    }
    Ok(())
}

fn validate_decoded_segment(
    segment: &RecoveryOutboxSegment,
    identity: RecoveryLogSegmentIdentity,
) -> Result<(), RecoveryInstallerError> {
    if segment.logical_shard_id != identity.logical_shard()
        || segment.first_lsn != identity.first_lsn()
        || segment.last_lsn != identity.last_lsn()
        || segment.previous_chain_digest != identity.previous_chain_digest()
        || segment.last_chain_digest != identity.last_chain_digest()
    {
        return Err(invalid_control(
            "decoded recovery segment differs from its exact object receipt",
        ));
    }
    Ok(())
}

fn decode_digest(
    value: &str,
    field: &'static str,
) -> Result<[u8; SHA256_BYTES], RecoveryInstallerError> {
    if value.len() != SHA256_BYTES * 2 {
        return Err(invalid_control(format!(
            "{field} is not canonical SHA-256 hex"
        )));
    }
    let mut decoded = [0_u8; SHA256_BYTES];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] =
            (decode_hex_digit(pair[0], field)? << 4) | decode_hex_digit(pair[1], field)?;
    }
    Ok(decoded)
}

fn decode_hex_digit(value: u8, field: &'static str) -> Result<u8, RecoveryInstallerError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_control(format!(
            "{field} is not canonical SHA-256 hex"
        ))),
    }
}

fn invalid_control(reason: impl Into<String>) -> RecoveryInstallerError {
    RecoveryInstallerError::InvalidControl(reason.into())
}

impl From<RecoveryLogSegmentError> for RecoveryInstallerError {
    fn from(error: RecoveryLogSegmentError) -> Self {
        Self::Object(error)
    }
}

impl From<RecoveryCheckpointError> for RecoveryInstallerError {
    fn from(error: RecoveryCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<RecoveryCodecError> for RecoveryInstallerError {
    fn from(error: RecoveryCodecError) -> Self {
        Self::Codec(error)
    }
}

impl From<MetaError> for RecoveryInstallerError {
    fn from(error: MetaError) -> Self {
        Self::Meta(error)
    }
}

impl fmt::Display for RecoveryInstallerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidControl(reason) => {
                write!(formatter, "invalid control recovery frontier: {reason}")
            }
            Self::Checkpoint(error) => {
                write!(
                    formatter,
                    "recovery checkpoint receipt validation failed: {error}"
                )
            }
            Self::Object(error) => write!(formatter, "recovery object read failed: {error}"),
            Self::Codec(error) => write!(formatter, "recovery segment decode failed: {error}"),
            Self::Meta(error) => write!(formatter, "metadata recovery replay failed: {error}"),
            Self::PendingCleanup { key, source } => {
                write!(
                    formatter,
                    "pending recovery cleanup failed for {key}: {source}"
                )
            }
        }
    }
}

impl std::error::Error for RecoveryInstallerError {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use nokv_control::{
        CheckpointRef, LogRef, LogSegmentRef, LogicalShardRecord, LogicalShardState, NodeId,
        RecoveryUploadIntent,
    };
    use nokv_meta::workspace::{
        MetaShard, MetadataCommand, RecoveryOutboxSegment, RootFenceAction, SCHEMA_ID,
    };
    use nokv_object::{
        ensure_object_namespace, plan_recovery_checkpoint_blob, plan_recovery_log_segment,
        write_recovery_log_segment_from_plan, ArtifactObjectStore, ArtifactStoreCapabilities,
        BoundArtifactStore, ImmutableCreateOutcome, MemoryArtifactStore, ObjectDeleteOutcome,
        ObjectError, ObjectInfo, ObjectKey, ObjectRange, ProviderAdmissionReceipt,
        ProviderHandleIdentity, RecoveryCheckpointBoundary, RecoveryCheckpointIdentity,
        RecoveryLogSegmentError, RecoveryLogSegmentIdentity,
        DEFAULT_RECOVERY_CHECKPOINT_CHUNK_SIZE, DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE,
    };
    use nokv_types::{
        CommandDigest, LogicalShardId, ObjectNamespaceId, OwnerEpoch, PlacementGeneration,
        RequestId, RootId, FIXED_ID_BYTES, SHA256_BYTES,
    };
    use sha2::{Digest, Sha256};

    use crate::test_support::meta_shard;

    use super::*;

    struct Fixture {
        source: std::sync::Arc<MetaShard>,
        target: std::sync::Arc<MetaShard>,
        objects: BoundArtifactStore<MemoryArtifactStore>,
        record: LogicalShardRecord,
        segments: Vec<RecoveryOutboxSegment>,
    }

    struct SpyStore {
        inner: BoundArtifactStore<MemoryArtifactStore>,
        reads: AtomicUsize,
        heads: AtomicUsize,
    }

    struct TamperReadStore {
        inner: BoundArtifactStore<MemoryArtifactStore>,
    }

    struct FailDeleteStore {
        inner: BoundArtifactStore<MemoryArtifactStore>,
        fail_at: usize,
        deletes: AtomicUsize,
    }

    impl SpyStore {
        fn new(inner: BoundArtifactStore<MemoryArtifactStore>) -> Self {
            Self {
                inner,
                reads: AtomicUsize::new(0),
                heads: AtomicUsize::new(0),
            }
        }
    }

    impl ArtifactObjectStore for SpyStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.inner.object_namespace()
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

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
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.read(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.heads.fetch_add(1, Ordering::Relaxed);
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            self.inner.delete(key)
        }
    }

    impl ArtifactObjectStore for TamperReadStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.inner.object_namespace()
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

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
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            let mut bytes = self.inner.read(key, range)?;
            if let Some(first) = bytes.first_mut() {
                *first ^= 0x01;
            }
            Ok(bytes)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            self.inner.delete(key)
        }
    }

    impl ArtifactObjectStore for FailDeleteStore {
        fn object_namespace(&self) -> Option<ObjectNamespaceId> {
            self.inner.object_namespace()
        }

        fn capabilities(&self) -> ArtifactStoreCapabilities {
            self.inner.capabilities()
        }

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
            self.inner.create_immutable(key, bytes)
        }

        fn read(
            &self,
            key: &ObjectKey,
            range: Option<ObjectRange>,
        ) -> Result<Vec<u8>, ObjectError> {
            self.inner.read(key, range)
        }

        fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
            self.inner.head(key)
        }

        fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
            let attempt = self.deletes.fetch_add(1, Ordering::Relaxed) + 1;
            if attempt == self.fail_at {
                return Err(ObjectError::DeleteAmbiguous {
                    key: key.clone(),
                    detail: "injected pending cleanup ambiguity".to_owned(),
                });
            }
            self.inner.delete(key)
        }
    }

    fn shard(byte: u8) -> LogicalShardId {
        LogicalShardId::from_bytes([byte; FIXED_ID_BYTES])
    }

    fn namespace(byte: u8) -> ObjectNamespaceId {
        ObjectNamespaceId::from_bytes([byte; FIXED_ID_BYTES])
    }

    fn root(byte: u8) -> RootId {
        RootId::from_bytes([byte; FIXED_ID_BYTES])
    }

    fn canonical_hex(bytes: &[u8; SHA256_BYTES]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut encoded = String::with_capacity(SHA256_BYTES * 2);
        for byte in bytes {
            encoded.push(DIGITS[(byte >> 4) as usize] as char);
            encoded.push(DIGITS[(byte & 0x0f) as usize] as char);
        }
        encoded
    }

    fn install_root(source: &MetaShard) {
        source
            .advance_owner_epoch(None, OwnerEpoch::new(1).unwrap())
            .unwrap();
        source
            .execute(
                &MetadataCommand {
                    schema_id: SCHEMA_ID.to_owned(),
                    root_id: root(7),
                    logical_shard_id: shard(1),
                    object_namespace_id: Some(namespace(9)),
                    placement_generation: PlacementGeneration::new(1).unwrap(),
                    owner_epoch: OwnerEpoch::new(1).unwrap(),
                    request_id: RequestId::from_bytes([3; FIXED_ID_BYTES]),
                    command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
                    read_version: source.current_read_version().unwrap(),
                    root_fence_action: RootFenceAction::Install,
                    predicates: Vec::new(),
                    mutations: Vec::new(),
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: b"installed".to_vec(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn plan_segment(
        objects: &dyn ArtifactObjectStore,
        segment: &RecoveryOutboxSegment,
    ) -> (Vec<u8>, RecoveryLogSegmentPlan) {
        let encoded = segment.encode().unwrap();
        let identity = RecoveryLogSegmentIdentity::new(
            objects.object_namespace().unwrap(),
            segment.logical_shard_id,
            segment.first_lsn,
            segment.last_lsn,
            segment.previous_chain_digest,
            segment.last_chain_digest,
            Sha256::digest(&encoded).into(),
        );
        let plan =
            plan_recovery_log_segment(identity, &encoded, DEFAULT_RECOVERY_LOG_SEGMENT_CHUNK_SIZE)
                .unwrap();
        (encoded, plan)
    }

    fn publish_segment(
        objects: &dyn ArtifactObjectStore,
        segment: &RecoveryOutboxSegment,
    ) -> LogSegmentRef {
        let (encoded, plan) = plan_segment(objects, segment);
        write_recovery_log_segment_from_plan(objects, &plan, &encoded).unwrap();
        LogSegmentRef {
            segment_key: plan.receipt().manifest_key().unwrap().as_str().to_owned(),
            first_lsn: segment.first_lsn,
            last_lsn: segment.last_lsn,
            digest: canonical_hex(&segment.last_chain_digest),
            receipt: plan.receipt().encode(),
        }
    }

    fn fixture() -> Fixture {
        let raw = MemoryArtifactStore::new();
        ensure_object_namespace(&raw, namespace(9)).unwrap();
        let objects = BoundArtifactStore::open(raw, namespace(9)).unwrap();
        let source = meta_shard(shard(1));
        install_root(source.as_ref());
        let rows = source.recovery_outbox_after(0, 16).unwrap();
        assert_eq!(rows.len(), 2);
        let segments = vec![
            RecoveryOutboxSegment::seal(shard(1), rows[..1].to_vec()).unwrap(),
            RecoveryOutboxSegment::seal(shard(1), rows[1..].to_vec()).unwrap(),
        ];
        let refs = segments
            .iter()
            .map(|segment| publish_segment(&objects, segment))
            .collect::<Vec<_>>();
        let tail = source.recovery_state().unwrap();
        let mut record = LogicalShardRecord::unassigned(shard(1));
        record.owner_epoch = Some(OwnerEpoch::new(1).unwrap());
        record.log = Some(LogRef {
            segments: refs,
            durable_lsn: tail.applied_recovery_lsn,
            digest: canonical_hex(&tail.chain_digest),
        });
        record.durable_lsn = tail.applied_recovery_lsn;
        Fixture {
            source,
            target: meta_shard(shard(1)),
            objects,
            record,
            segments,
        }
    }

    fn pending_fixture() -> Fixture {
        let mut fixture = fixture();
        let first = fixture.record.log.as_ref().unwrap().segments[0].clone();
        let (encoded, plan) = plan_segment(&fixture.objects, &fixture.segments[1]);
        write_recovery_log_segment_from_plan(&fixture.objects, &plan, &encoded).unwrap();
        let receipt = plan.receipt();
        let identity = receipt.identity();
        fixture.record.log = Some(LogRef {
            segments: vec![first.clone()],
            durable_lsn: first.last_lsn,
            digest: first.digest,
        });
        fixture.record.durable_lsn = first.last_lsn;
        fixture.record.owner = Some(NodeId::new("pending-owner").unwrap());
        fixture.record.lease_id = 7;
        fixture.record.state = LogicalShardState::Recovering;
        fixture.record.endpoint = Some("127.0.0.1:9000".to_owned());
        fixture.record.pending_recovery_upload = Some(RecoveryUploadIntent {
            object_namespace_id: identity.object_namespace(),
            first_lsn: identity.first_lsn(),
            last_lsn: identity.last_lsn(),
            previous_chain_digest: canonical_hex(&identity.previous_chain_digest()),
            last_chain_digest: canonical_hex(&identity.last_chain_digest()),
            segment_digest: canonical_hex(&identity.segment_digest()),
            manifest_key: receipt.manifest_key().unwrap().as_str().to_owned(),
            receipt: receipt.encode(),
            plan: plan.encode(),
        });
        fixture
    }

    fn checkpoint_fixture() -> Fixture {
        let mut fixture = fixture();
        let checkpoint_segment = &fixture.segments[0];
        let envelope = b"exact checkpoint envelope";
        let plan = plan_recovery_checkpoint_blob(
            RecoveryCheckpointIdentity::new(
                namespace(9),
                shard(1),
                RecoveryCheckpointBoundary::new(
                    checkpoint_segment.last_lsn,
                    checkpoint_segment.last_chain_digest,
                ),
            ),
            envelope,
            DEFAULT_RECOVERY_CHECKPOINT_CHUNK_SIZE,
        )
        .unwrap();
        let receipt = plan.receipt();
        let tail = fixture.source.recovery_state().unwrap();
        let second = fixture.record.log.as_ref().unwrap().segments[1].clone();
        fixture.record.checkpoint = Some(CheckpointRef {
            object_key: receipt.manifest_key().unwrap().as_str().to_owned(),
            lsn: checkpoint_segment.last_lsn,
            image_bytes: envelope.len() as u64,
            image_digest: canonical_hex(&Sha256::digest(envelope).into()),
            digest: canonical_hex(&checkpoint_segment.last_chain_digest),
            receipt: receipt.encode(),
        });
        fixture.record.log = Some(LogRef {
            segments: vec![second],
            durable_lsn: tail.applied_recovery_lsn,
            digest: canonical_hex(&tail.chain_digest),
        });
        fixture
    }

    #[test]
    fn installs_two_exact_segments_and_fscks_the_control_tail_without_head() {
        let fixture = fixture();
        let spy = SpyStore::new(fixture.objects.clone());

        let report = install_recovery_log(&fixture.record, &spy, fixture.target.as_ref()).unwrap();

        assert_eq!(report.initial_state.applied_recovery_lsn, 0);
        assert_eq!(report.final_state, fixture.source.recovery_state().unwrap());
        assert_eq!(report.verified_segments, 2);
        assert_eq!(report.fsck.state, report.final_state);
        assert_eq!(report.fsck.outbox_records, 2);
        assert_eq!(report.fsck.metadata_command_records, 1);
        assert_eq!(report.fsck.dedupe_records, 1);
        assert!(spy.reads.load(Ordering::Relaxed) >= 4);
        assert_eq!(spy.heads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn resumes_from_an_exact_overlap_inside_the_control_log() {
        let fixture = fixture();
        fixture
            .target
            .replay_recovery_segment(&fixture.segments[0])
            .unwrap();
        let before = fixture.target.recovery_state().unwrap();

        let report =
            install_recovery_log(&fixture.record, &fixture.objects, fixture.target.as_ref())
                .unwrap();

        assert_eq!(report.initial_state, before);
        assert_eq!(report.final_state, fixture.source.recovery_state().unwrap());
        assert_eq!(
            fixture.target.recovery_outbox_after(0, 16).unwrap().len(),
            2
        );
    }

    #[test]
    fn missing_object_stops_without_guessing_an_alternate_key() {
        let fixture = fixture();
        let missing = ObjectKey::new(
            fixture.record.log.as_ref().unwrap().segments[1]
                .segment_key
                .clone(),
        )
        .unwrap();
        fixture.objects.delete(&missing).unwrap();
        let spy = SpyStore::new(fixture.objects.clone());

        let error = install_recovery_log(&fixture.record, &spy, fixture.target.as_ref())
            .expect_err("missing receipt-addressed manifest must fail");

        assert!(matches!(
            error,
            RecoveryInstallerError::Object(RecoveryLogSegmentError::Object(
                ObjectError::ObjectNotFound { .. }
            ))
        ));
        assert_eq!(spy.heads.load(Ordering::Relaxed), 0);
        assert_eq!(
            fixture
                .target
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn,
            1
        );
    }

    #[test]
    fn rejects_tampered_foreign_and_gapped_control_receipts_before_replay() {
        let mut tampered = fixture();
        tampered.record.log.as_mut().unwrap().segments[0].receipt[0] ^= 0x01;
        assert!(matches!(
            install_recovery_log(
                &tampered.record,
                &tampered.objects,
                tampered.target.as_ref()
            ),
            Err(RecoveryInstallerError::Object(
                RecoveryLogSegmentError::InvalidReceipt(_)
            ))
        ));
        assert_eq!(
            tampered
                .target
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn,
            0
        );

        let mut foreign = fixture();
        let receipt = &mut foreign.record.log.as_mut().unwrap().segments[0].receipt;
        let shard_offset = 8 + 2 + FIXED_ID_BYTES;
        receipt[shard_offset] ^= 0x01;
        assert!(
            install_recovery_log(&foreign.record, &foreign.objects, foreign.target.as_ref())
                .unwrap_err()
                .to_string()
                .contains("another logical shard")
        );
        assert_eq!(
            foreign
                .target
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn,
            0
        );

        let mut gapped = fixture();
        gapped.record.log.as_mut().unwrap().segments.remove(0);
        assert!(
            install_recovery_log(&gapped.record, &gapped.objects, gapped.target.as_ref())
                .unwrap_err()
                .to_string()
                .contains("expected first LSN 1")
        );
        assert_eq!(
            gapped.target.recovery_state().unwrap().applied_recovery_lsn,
            0
        );
    }

    #[test]
    fn rejects_checkpoint_frontiers_until_checkpoint_installation_is_supported() {
        let mut checkpoint = fixture();
        checkpoint.record.checkpoint = Some(CheckpointRef {
            object_key: "nokv/recovery/checkpoint".to_owned(),
            lsn: 1,
            image_bytes: 1,
            image_digest: "00".repeat(SHA256_BYTES),
            digest: "00".repeat(SHA256_BYTES),
            receipt: vec![1],
        });
        assert!(install_recovery_log(
            &checkpoint.record,
            &checkpoint.objects,
            checkpoint.target.as_ref()
        )
        .unwrap_err()
        .to_string()
        .contains("checkpoint"));
    }

    #[test]
    fn installs_a_fully_published_pending_upload_for_idempotent_control_finalize() {
        let fixture = pending_fixture();

        let report =
            install_recovery_log(&fixture.record, &fixture.objects, fixture.target.as_ref())
                .unwrap();

        assert!(report.recovered_pending_upload);
        assert_eq!(report.verified_segments, 2);
        assert_eq!(report.final_state, fixture.source.recovery_state().unwrap());
        assert_eq!(report.fsck.state, report.final_state);
        assert_eq!(
            fixture.target.recovery_outbox_after(0, 16).unwrap().len(),
            2
        );
    }

    #[test]
    fn durable_only_install_defers_pending_object_resolution_until_owner_fenced() {
        let fixture = pending_fixture();
        let manifest_key = ObjectKey::new(
            fixture
                .record
                .pending_recovery_upload
                .as_ref()
                .unwrap()
                .manifest_key
                .clone(),
        )
        .unwrap();
        fixture.objects.delete(&manifest_key).unwrap();

        let report = install_durable_recovery_log(
            &fixture.record,
            &fixture.objects,
            fixture.target.as_ref(),
        )
        .unwrap();
        let durable = fixture
            .record
            .log
            .as_ref()
            .expect("pending fixture retains one durable segment");

        assert_eq!(report.final_state.applied_recovery_lsn, durable.durable_lsn);
        assert!(!report.recovered_pending_upload);
        let proof = validate_local_durable_recovery_prefix(
            &fixture.record,
            namespace(9),
            fixture.target.as_ref(),
        )
        .unwrap();
        assert_eq!(
            proof.control_state.applied_recovery_lsn,
            durable.durable_lsn
        );
        assert_eq!(proof.pending_state, None);
    }

    #[test]
    fn durable_only_install_noops_for_a_proven_exact_ahead_prefix_but_rejects_divergence() {
        let mut fixture = fixture();
        let first = fixture.record.log.as_ref().unwrap().segments[0].clone();
        fixture.record.log = Some(LogRef {
            segments: vec![first.clone()],
            durable_lsn: first.last_lsn,
            digest: first.digest,
        });
        fixture.record.durable_lsn = first.last_lsn;
        let before = fixture.source.recovery_state().unwrap();

        let report = install_durable_recovery_log(
            &fixture.record,
            &fixture.objects,
            fixture.source.as_ref(),
        )
        .unwrap();
        assert_eq!(report.initial_state, before);
        assert_eq!(report.final_state, before);
        assert_eq!(report.verified_segments, 0);
        assert_eq!(fixture.source.recovery_state().unwrap(), before);

        fixture.record.log.as_mut().unwrap().digest = "ff".repeat(SHA256_BYTES);
        assert!(install_durable_recovery_log(
            &fixture.record,
            &fixture.objects,
            fixture.source.as_ref(),
        )
        .is_err());
        assert_eq!(fixture.source.recovery_state().unwrap(), before);
    }

    #[test]
    fn pending_install_requires_exact_cleanup_without_local_mutation_for_missing_or_corrupt_objects(
    ) {
        let missing = pending_fixture();
        install_durable_recovery_log(&missing.record, &missing.objects, missing.target.as_ref())
            .unwrap();
        let before = missing.target.recovery_state().unwrap();
        let manifest_key = ObjectKey::new(
            missing
                .record
                .pending_recovery_upload
                .as_ref()
                .unwrap()
                .manifest_key
                .clone(),
        )
        .unwrap();
        missing.objects.delete(&manifest_key).unwrap();

        assert_eq!(
            install_pending_recovery_upload(
                &missing.record,
                &missing.objects,
                missing.target.as_ref(),
            )
            .unwrap(),
            PendingRecoveryInstallOutcome::CleanupRequired {
                reason: PendingRecoveryAbortReason::MissingObject,
            }
        );
        assert_eq!(missing.target.recovery_state().unwrap(), before);
        let cleanup = cleanup_pending_recovery_upload(
            &missing.record,
            &missing.objects,
            missing.target.as_ref(),
        )
        .unwrap();
        let plan = RecoveryLogSegmentPlan::decode(
            &missing
                .record
                .pending_recovery_upload
                .as_ref()
                .unwrap()
                .plan,
        )
        .unwrap();
        assert_eq!(cleanup.attempted, plan.cleanup_keys().len());
        assert!(plan
            .cleanup_keys()
            .iter()
            .all(|key| missing.objects.head(key).unwrap().is_none()));

        let tampered = pending_fixture();
        install_durable_recovery_log(
            &tampered.record,
            &tampered.objects,
            tampered.target.as_ref(),
        )
        .unwrap();
        let before = tampered.target.recovery_state().unwrap();
        let tamper_store = TamperReadStore {
            inner: tampered.objects.clone(),
        };

        assert_eq!(
            install_pending_recovery_upload(
                &tampered.record,
                &tamper_store,
                tampered.target.as_ref(),
            )
            .unwrap(),
            PendingRecoveryInstallOutcome::CleanupRequired {
                reason: PendingRecoveryAbortReason::CorruptObject,
            }
        );
        assert_eq!(tampered.target.recovery_state().unwrap(), before);
    }

    #[test]
    fn ambiguous_pending_cleanup_keeps_exact_plan_retryable_until_every_key_is_absent() {
        let fixture = pending_fixture();
        install_durable_recovery_log(&fixture.record, &fixture.objects, fixture.target.as_ref())
            .unwrap();
        let plan = RecoveryLogSegmentPlan::decode(
            &fixture
                .record
                .pending_recovery_upload
                .as_ref()
                .unwrap()
                .plan,
        )
        .unwrap();
        assert!(plan.cleanup_keys().len() >= 2);
        let interrupted = FailDeleteStore {
            inner: fixture.objects.clone(),
            fail_at: 2,
            deletes: AtomicUsize::new(0),
        };

        assert!(matches!(
            cleanup_pending_recovery_upload(&fixture.record, &interrupted, fixture.target.as_ref(),),
            Err(RecoveryInstallerError::PendingCleanup {
                source: ObjectError::DeleteAmbiguous { .. },
                ..
            })
        ));
        assert!(fixture
            .objects
            .head(&plan.cleanup_keys()[0])
            .unwrap()
            .is_none());
        assert!(fixture
            .objects
            .head(&plan.cleanup_keys()[1])
            .unwrap()
            .is_some());

        let retried = cleanup_pending_recovery_upload(
            &fixture.record,
            &fixture.objects,
            fixture.target.as_ref(),
        )
        .unwrap();
        assert_eq!(retried.attempted, plan.cleanup_keys().len());
        assert!(retried.already_absent >= 1);
        assert!(plan
            .cleanup_keys()
            .iter()
            .all(|key| fixture.objects.head(key).unwrap().is_none()));
    }

    #[test]
    fn corrupt_pending_plan_is_not_guessed_or_cleared_without_cleanup_authority() {
        let mut fixture = pending_fixture();
        install_durable_recovery_log(&fixture.record, &fixture.objects, fixture.target.as_ref())
            .unwrap();
        let exact_plan = RecoveryLogSegmentPlan::decode(
            &fixture
                .record
                .pending_recovery_upload
                .as_ref()
                .unwrap()
                .plan,
        )
        .unwrap();
        fixture
            .record
            .pending_recovery_upload
            .as_mut()
            .unwrap()
            .plan[0] ^= 0x01;

        assert!(cleanup_pending_recovery_upload(
            &fixture.record,
            &fixture.objects,
            fixture.target.as_ref(),
        )
        .is_err());
        assert!(exact_plan.cleanup_keys().iter().all(|key| fixture
            .objects
            .head(key)
            .unwrap()
            .is_some()));
        assert!(fixture.record.pending_recovery_upload.is_some());
    }

    #[test]
    fn pending_install_replays_a_complete_exact_object_for_publisher_finalize() {
        let fixture = pending_fixture();
        install_durable_recovery_log(&fixture.record, &fixture.objects, fixture.target.as_ref())
            .unwrap();

        let outcome = install_pending_recovery_upload(
            &fixture.record,
            &fixture.objects,
            fixture.target.as_ref(),
        )
        .unwrap();

        assert!(matches!(
            outcome,
            PendingRecoveryInstallOutcome::Installed { final_state, .. }
                if final_state == fixture.source.recovery_state().unwrap()
        ));
        validate_local_recovery_prefix(&fixture.record, namespace(9), fixture.target.as_ref())
            .unwrap();
    }

    #[test]
    fn missing_or_tampered_pending_object_fails_before_any_metadata_replay() {
        let missing = pending_fixture();
        let manifest_key = ObjectKey::new(
            missing
                .record
                .pending_recovery_upload
                .as_ref()
                .unwrap()
                .manifest_key
                .clone(),
        )
        .unwrap();
        missing.objects.delete(&manifest_key).unwrap();
        assert!(matches!(
            install_recovery_log(&missing.record, &missing.objects, missing.target.as_ref()),
            Err(RecoveryInstallerError::Object(
                RecoveryLogSegmentError::Object(ObjectError::ObjectNotFound { .. })
            ))
        ));
        assert_eq!(
            missing
                .target
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn,
            0
        );

        let tampered = pending_fixture();
        let tamper_store = TamperReadStore {
            inner: tampered.objects.clone(),
        };
        assert!(
            install_recovery_log(&tampered.record, &tamper_store, tampered.target.as_ref())
                .is_err()
        );
        assert_eq!(
            tampered
                .target
                .recovery_state()
                .unwrap()
                .applied_recovery_lsn,
            0
        );
    }

    #[test]
    fn read_only_prefix_proof_accepts_local_ahead_and_an_exact_pending_segment() {
        let mut ahead = fixture();
        let first = ahead.record.log.as_ref().unwrap().segments[0].clone();
        ahead.record.log = Some(LogRef {
            segments: vec![first.clone()],
            durable_lsn: first.last_lsn,
            digest: first.digest,
        });
        ahead.record.durable_lsn = first.last_lsn;
        let before = ahead.source.recovery_state().unwrap();

        let report =
            validate_local_recovery_prefix(&ahead.record, namespace(9), ahead.source.as_ref())
                .unwrap();

        assert_eq!(report.local_state, before);
        assert_eq!(report.control_state.applied_recovery_lsn, 1);
        assert_eq!(report.pending_state, None);
        assert_eq!(ahead.source.recovery_state().unwrap(), before);

        let pending = pending_fixture();
        let before = pending.source.recovery_state().unwrap();
        let report =
            validate_local_recovery_prefix(&pending.record, namespace(9), pending.source.as_ref())
                .unwrap();
        assert_eq!(report.local_state, before);
        assert_eq!(report.control_state.applied_recovery_lsn, 1);
        assert_eq!(report.pending_state, Some(before));
        assert_eq!(pending.source.recovery_state().unwrap(), before);
    }

    #[test]
    fn read_only_prefix_proof_rejects_behind_or_divergent_local_state() {
        let behind = fixture();
        let before = behind.target.recovery_state().unwrap();
        assert!(validate_local_recovery_prefix(
            &behind.record,
            namespace(9),
            behind.target.as_ref()
        )
        .unwrap_err()
        .to_string()
        .contains("ahead of local"));
        assert_eq!(behind.target.recovery_state().unwrap(), before);

        let mut divergent = fixture();
        let mut wrong = [0_u8; SHA256_BYTES];
        wrong[0] = 1;
        let wrong = canonical_hex(&wrong);
        let log = divergent.record.log.as_mut().unwrap();
        log.digest = wrong.clone();
        log.segments.last_mut().unwrap().digest = wrong;
        let before = divergent.source.recovery_state().unwrap();
        assert!(validate_local_recovery_prefix(
            &divergent.record,
            namespace(9),
            divergent.source.as_ref()
        )
        .is_err());
        assert_eq!(divergent.source.recovery_state().unwrap(), before);
    }

    #[test]
    fn admission_preflight_strictly_binds_log_and_checkpoint_receipts_without_object_io() {
        let checkpoint = checkpoint_fixture();
        let before = checkpoint.source.recovery_state().unwrap();
        let report = validate_local_recovery_prefix(
            &checkpoint.record,
            namespace(9),
            checkpoint.source.as_ref(),
        )
        .unwrap();
        assert_eq!(report.control_state, before);
        assert_eq!(checkpoint.source.recovery_state().unwrap(), before);

        let mut garbage_log = fixture();
        garbage_log.record.log.as_mut().unwrap().segments[0].receipt = vec![0; 32];
        let before = garbage_log.source.recovery_state().unwrap();
        assert!(matches!(
            validate_local_recovery_prefix(
                &garbage_log.record,
                namespace(9),
                garbage_log.source.as_ref()
            ),
            Err(RecoveryInstallerError::Object(
                RecoveryLogSegmentError::InvalidReceipt(_)
            ))
        ));
        assert_eq!(garbage_log.source.recovery_state().unwrap(), before);

        let mut foreign_log = fixture();
        let receipt = &mut foreign_log.record.log.as_mut().unwrap().segments[0].receipt;
        let namespace_offset = 8 + 2;
        receipt[namespace_offset] ^= 0x01;
        let before = foreign_log.source.recovery_state().unwrap();
        assert!(matches!(
            validate_local_recovery_prefix(
                &foreign_log.record,
                namespace(9),
                foreign_log.source.as_ref()
            ),
            Err(RecoveryInstallerError::Object(
                RecoveryLogSegmentError::ForeignNamespace
            ))
        ));
        assert_eq!(foreign_log.source.recovery_state().unwrap(), before);

        let mut garbage_checkpoint = checkpoint_fixture();
        garbage_checkpoint
            .record
            .checkpoint
            .as_mut()
            .unwrap()
            .receipt = vec![0; 32];
        let before = garbage_checkpoint.source.recovery_state().unwrap();
        assert!(matches!(
            validate_local_recovery_prefix(
                &garbage_checkpoint.record,
                namespace(9),
                garbage_checkpoint.source.as_ref()
            ),
            Err(RecoveryInstallerError::Checkpoint(
                RecoveryCheckpointError::InvalidReceipt(_)
            ))
        ));
        assert_eq!(garbage_checkpoint.source.recovery_state().unwrap(), before);
    }

    #[test]
    fn bounds_segment_count_and_aggregate_receipt_bytes_before_object_reads() {
        let mut too_many = fixture();
        let repeated = too_many.record.log.as_ref().unwrap().segments[0].clone();
        let exact_maximum = vec![repeated.clone(); MAX_RECOVERY_INSTALL_SEGMENTS];
        assert!(validate_aggregate_reference_bounds(&exact_maximum, None).is_ok());

        let mut exact_receipt_maximum = vec![repeated.clone(); 16];
        for segment in &mut exact_receipt_maximum {
            segment.receipt = vec![0; MAX_RECOVERY_INSTALL_RECEIPT_BYTES / 16];
        }
        assert_eq!(
            validate_aggregate_reference_bounds(&exact_receipt_maximum, None).unwrap(),
            MAX_RECOVERY_INSTALL_RECEIPT_BYTES
        );
        exact_receipt_maximum[0].receipt.push(0);
        assert!(validate_aggregate_reference_bounds(&exact_receipt_maximum, None).is_err());

        too_many.record.log.as_mut().unwrap().segments =
            vec![repeated; MAX_RECOVERY_INSTALL_SEGMENTS + 1];
        let spy = SpyStore::new(too_many.objects.clone());
        assert!(
            install_recovery_log(&too_many.record, &spy, too_many.target.as_ref())
                .unwrap_err()
                .to_string()
                .contains("segment count")
        );
        assert_eq!(spy.reads.load(Ordering::Relaxed), 0);

        let mut oversized_receipts = fixture();
        oversized_receipts.record.log.as_mut().unwrap().segments[0].receipt =
            vec![0; MAX_RECOVERY_INSTALL_RECEIPT_BYTES + 1];
        let spy = SpyStore::new(oversized_receipts.objects.clone());
        assert!(install_recovery_log(
            &oversized_receipts.record,
            &spy,
            oversized_receipts.target.as_ref()
        )
        .unwrap_err()
        .to_string()
        .contains("receipt bytes"));
        assert_eq!(spy.reads.load(Ordering::Relaxed), 0);
    }
}
