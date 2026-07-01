//! Logical metadata log records for shard recovery.
//!
//! The log records storage-neutral [`MetadataCommand`] entries and their commit
//! result. It is not Holt's private WAL format and it does not own object-store
//! upload or shard-owner lifecycle.

use std::fmt;

use nokv_types::RecordFamily;
use sha2::{Digest, Sha256};

use crate::{
    CommandKind, CommitResult, MetadataCommand, MetadataError, Mutation, MutationOp, Predicate,
    PredicateRef, Value, Version, WatchProjection,
};

pub const METADATA_LOG_ZERO_DIGEST: [u8; 32] = [0; 32];
const METADATA_LOG_SEGMENT_MAGIC: &[u8; 8] = b"NOKVMLG1";
const METADATA_LOG_SEGMENT_VERSION: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogEntry {
    pub shard_id: String,
    pub epoch: u64,
    pub lsn: u64,
    pub request_id: Vec<u8>,
    pub command: MetadataCommand,
    pub result: CommitResult,
    pub prev_digest: [u8; 32],
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataLogSegment {
    pub shard_id: String,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub first_lsn: u64,
    pub last_lsn: u64,
    pub prev_digest: [u8; 32],
    pub last_digest: [u8; 32],
    pub entries: Vec<MetadataLogEntry>,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataLogError {
    EmptyShardId,
    ZeroEpoch,
    ZeroLsn,
    EmptySegment,
    Command(MetadataError),
    ResultVersionMismatch { command: u64, result: u64 },
    AppliedMutationMismatch { applied: usize, expected: usize },
    WatchEventMismatch { emitted: usize, expected: usize },
    DigestMismatch,
    SegmentDigestMismatch,
    SegmentHeaderMismatch,
    ChainShardMismatch { previous: String, next: String },
    ChainLsnGap { previous: u64, next: u64 },
    ChainDigestMismatch,
    ChainEpochRegression { previous: u64, next: u64 },
    InvalidSegmentMagic,
    UnsupportedSegmentVersion(u64),
    Truncated,
    TrailingBytes,
    InvalidCommandKind(u8),
    InvalidRecordFamily(u8),
    InvalidMutationOp(u8),
    InvalidPredicateTag(u8),
    InvalidValueTag(u8),
    InvalidUtf8,
    LengthOverflow,
    EntryRequestIdMismatch,
}

impl MetadataLogEntry {
    pub fn seal(
        shard_id: impl Into<String>,
        epoch: u64,
        lsn: u64,
        command: MetadataCommand,
        result: CommitResult,
        prev_digest: [u8; 32],
    ) -> Result<Self, MetadataLogError> {
        let shard_id = shard_id.into();
        validate_log_input(&shard_id, epoch, lsn, &command, &result)?;
        let request_id = command.request_id.clone();
        let digest = metadata_log_digest(&shard_id, epoch, lsn, &command, &result, &prev_digest);
        Ok(Self {
            shard_id,
            epoch,
            lsn,
            request_id,
            command,
            result,
            prev_digest,
            digest,
        })
    }

    pub fn verify_digest(&self) -> Result<(), MetadataLogError> {
        if self.request_id != self.command.request_id {
            return Err(MetadataLogError::EntryRequestIdMismatch);
        }
        validate_log_input(
            &self.shard_id,
            self.epoch,
            self.lsn,
            &self.command,
            &self.result,
        )?;
        let expected = metadata_log_digest(
            &self.shard_id,
            self.epoch,
            self.lsn,
            &self.command,
            &self.result,
            &self.prev_digest,
        );
        if self.digest != expected {
            return Err(MetadataLogError::DigestMismatch);
        }
        Ok(())
    }

    pub fn verify_follows(&self, previous: &Self) -> Result<(), MetadataLogError> {
        self.verify_digest()?;
        previous.verify_digest()?;
        if self.shard_id != previous.shard_id {
            return Err(MetadataLogError::ChainShardMismatch {
                previous: previous.shard_id.clone(),
                next: self.shard_id.clone(),
            });
        }
        if self.lsn != previous.lsn.saturating_add(1) {
            return Err(MetadataLogError::ChainLsnGap {
                previous: previous.lsn,
                next: self.lsn,
            });
        }
        if self.prev_digest != previous.digest {
            return Err(MetadataLogError::ChainDigestMismatch);
        }
        if self.epoch < previous.epoch {
            return Err(MetadataLogError::ChainEpochRegression {
                previous: previous.epoch,
                next: self.epoch,
            });
        }
        Ok(())
    }
}

impl MetadataLogSegment {
    pub fn seal(entries: Vec<MetadataLogEntry>) -> Result<Self, MetadataLogError> {
        let first = entries.first().ok_or(MetadataLogError::EmptySegment)?;
        first.verify_digest()?;
        for pair in entries.windows(2) {
            pair[1].verify_follows(&pair[0])?;
        }
        let last = entries.last().expect("non-empty segment");
        let digest = metadata_log_segment_digest(SegmentDigestInput {
            shard_id: &first.shard_id,
            first_epoch: first.epoch,
            last_epoch: last.epoch,
            first_lsn: first.lsn,
            last_lsn: last.lsn,
            prev_digest: &first.prev_digest,
            last_digest: &last.digest,
            entries: &entries,
        });
        Ok(Self {
            shard_id: first.shard_id.clone(),
            first_epoch: first.epoch,
            last_epoch: last.epoch,
            first_lsn: first.lsn,
            last_lsn: last.lsn,
            prev_digest: first.prev_digest,
            last_digest: last.digest,
            entries,
            digest,
        })
    }

    pub fn verify(&self) -> Result<(), MetadataLogError> {
        let expected = Self::seal(self.entries.clone())?;
        if self.shard_id != expected.shard_id
            || self.first_epoch != expected.first_epoch
            || self.last_epoch != expected.last_epoch
            || self.first_lsn != expected.first_lsn
            || self.last_lsn != expected.last_lsn
            || self.prev_digest != expected.prev_digest
            || self.last_digest != expected.last_digest
        {
            return Err(MetadataLogError::SegmentHeaderMismatch);
        }
        if self.digest != expected.digest {
            return Err(MetadataLogError::SegmentDigestMismatch);
        }
        Ok(())
    }

    pub fn verify_follows(
        &self,
        previous_lsn: u64,
        previous_digest: [u8; 32],
    ) -> Result<(), MetadataLogError> {
        self.verify()?;
        if self.first_lsn != previous_lsn.saturating_add(1) {
            return Err(MetadataLogError::ChainLsnGap {
                previous: previous_lsn,
                next: self.first_lsn,
            });
        }
        if self.prev_digest != previous_digest {
            return Err(MetadataLogError::ChainDigestMismatch);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, MetadataLogError> {
        self.verify()?;
        let mut out = Vec::new();
        out.extend_from_slice(METADATA_LOG_SEGMENT_MAGIC);
        append_u64_to_vec(&mut out, METADATA_LOG_SEGMENT_VERSION);
        put_string(&mut out, &self.shard_id);
        append_u64_to_vec(&mut out, self.first_epoch);
        append_u64_to_vec(&mut out, self.last_epoch);
        append_u64_to_vec(&mut out, self.first_lsn);
        append_u64_to_vec(&mut out, self.last_lsn);
        out.extend_from_slice(&self.prev_digest);
        out.extend_from_slice(&self.last_digest);
        out.extend_from_slice(&self.digest);
        append_u64_to_vec(&mut out, self.entries.len() as u64);
        for entry in &self.entries {
            encode_log_entry(&mut out, entry)?;
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataLogError> {
        let mut input = LogDecoder::new(bytes);
        if input.take(METADATA_LOG_SEGMENT_MAGIC.len())? != METADATA_LOG_SEGMENT_MAGIC {
            return Err(MetadataLogError::InvalidSegmentMagic);
        }
        let version = input.u64()?;
        if version != METADATA_LOG_SEGMENT_VERSION {
            return Err(MetadataLogError::UnsupportedSegmentVersion(version));
        }
        let shard_id = input.string()?;
        let first_epoch = input.u64()?;
        let last_epoch = input.u64()?;
        let first_lsn = input.u64()?;
        let last_lsn = input.u64()?;
        let prev_digest = input.digest()?;
        let last_digest = input.digest()?;
        let digest = input.digest()?;
        let entry_count = input.count()?;
        let mut entries = Vec::with_capacity(entry_count.min(1024));
        for _ in 0..entry_count {
            entries.push(decode_log_entry(&mut input)?);
        }
        input.finish()?;

        let segment = Self::seal(entries)?;
        if segment.shard_id != shard_id
            || segment.first_epoch != first_epoch
            || segment.last_epoch != last_epoch
            || segment.first_lsn != first_lsn
            || segment.last_lsn != last_lsn
            || segment.prev_digest != prev_digest
            || segment.last_digest != last_digest
        {
            return Err(MetadataLogError::SegmentHeaderMismatch);
        }
        if segment.digest != digest {
            return Err(MetadataLogError::SegmentDigestMismatch);
        }
        Ok(segment)
    }
}

pub fn metadata_log_replay_entries(
    segments: &[MetadataLogSegment],
    checkpoint_lsn: u64,
    checkpoint_digest: [u8; 32],
) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
    let mut previous_lsn = checkpoint_lsn;
    let mut previous_digest = checkpoint_digest;
    let mut out = Vec::new();
    for segment in segments {
        segment.verify_follows(previous_lsn, previous_digest)?;
        out.extend(segment.entries.iter().cloned());
        previous_lsn = segment.last_lsn;
        previous_digest = segment.last_digest;
    }
    Ok(out)
}

fn validate_log_input(
    shard_id: &str,
    epoch: u64,
    lsn: u64,
    command: &MetadataCommand,
    result: &CommitResult,
) -> Result<(), MetadataLogError> {
    if shard_id.is_empty() {
        return Err(MetadataLogError::EmptyShardId);
    }
    if epoch == 0 {
        return Err(MetadataLogError::ZeroEpoch);
    }
    if lsn == 0 {
        return Err(MetadataLogError::ZeroLsn);
    }
    command.validate().map_err(MetadataLogError::Command)?;
    if command.commit_version != result.commit_version {
        return Err(MetadataLogError::ResultVersionMismatch {
            command: command.commit_version.get(),
            result: result.commit_version.get(),
        });
    }
    if result.applied_mutations != command.mutations.len() {
        return Err(MetadataLogError::AppliedMutationMismatch {
            applied: result.applied_mutations,
            expected: command.mutations.len(),
        });
    }
    if result.watch_events != command.watch.len() {
        return Err(MetadataLogError::WatchEventMismatch {
            emitted: result.watch_events,
            expected: command.watch.len(),
        });
    }
    Ok(())
}

fn metadata_log_digest(
    shard_id: &str,
    epoch: u64,
    lsn: u64,
    command: &MetadataCommand,
    result: &CommitResult,
    prev_digest: &[u8; 32],
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    append_bytes(&mut hasher, b"nokv.metadata-log.v1");
    append_bytes(&mut hasher, shard_id.as_bytes());
    append_u64(&mut hasher, epoch);
    append_u64(&mut hasher, lsn);
    hasher.update(prev_digest);
    append_command(&mut hasher, command);
    append_commit_result(&mut hasher, result);
    hasher.finalize().into()
}

struct SegmentDigestInput<'a> {
    shard_id: &'a str,
    first_epoch: u64,
    last_epoch: u64,
    first_lsn: u64,
    last_lsn: u64,
    prev_digest: &'a [u8; 32],
    last_digest: &'a [u8; 32],
    entries: &'a [MetadataLogEntry],
}

fn metadata_log_segment_digest(input: SegmentDigestInput<'_>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    append_bytes(&mut hasher, b"nokv.metadata-log-segment.v1");
    append_bytes(&mut hasher, input.shard_id.as_bytes());
    append_u64(&mut hasher, input.first_epoch);
    append_u64(&mut hasher, input.last_epoch);
    append_u64(&mut hasher, input.first_lsn);
    append_u64(&mut hasher, input.last_lsn);
    hasher.update(input.prev_digest);
    hasher.update(input.last_digest);
    append_u64(&mut hasher, input.entries.len() as u64);
    for entry in input.entries {
        hasher.update(entry.digest);
    }
    hasher.finalize().into()
}

fn append_command(hasher: &mut Sha256, command: &MetadataCommand) {
    append_bytes(hasher, &command.request_id);
    append_u8(hasher, command_kind_tag(command.kind));
    append_version(hasher, command.read_version);
    append_version(hasher, command.commit_version);
    append_u8(hasher, record_family_tag(command.primary_family));
    append_bytes(hasher, &command.primary_key);
    append_u64(hasher, command.predicates.len() as u64);
    for predicate in &command.predicates {
        append_u8(hasher, record_family_tag(predicate.family));
        append_bytes(hasher, &predicate.key);
        append_predicate(hasher, predicate.predicate);
    }
    append_u64(hasher, command.mutations.len() as u64);
    for mutation in &command.mutations {
        append_u8(hasher, record_family_tag(mutation.family));
        append_bytes(hasher, &mutation.key);
        append_u8(hasher, mutation_op_tag(mutation.op));
        match &mutation.value {
            Some(value) => {
                append_u8(hasher, 1);
                append_bytes(hasher, &value.0);
            }
            None => append_u8(hasher, 0),
        }
    }
    append_u64(hasher, command.watch.len() as u64);
    for event in &command.watch {
        append_u8(hasher, record_family_tag(event.family));
        append_bytes(hasher, &event.key);
        append_bytes(hasher, &event.event);
    }
}

fn append_commit_result(hasher: &mut Sha256, result: &CommitResult) {
    append_version(hasher, result.commit_version);
    append_u64(hasher, result.applied_mutations as u64);
    append_u64(hasher, result.watch_events as u64);
}

fn append_predicate(hasher: &mut Sha256, predicate: Predicate) {
    match predicate {
        Predicate::Exists => append_u8(hasher, 1),
        Predicate::NotExists => append_u8(hasher, 2),
        Predicate::PrefixEmpty => append_u8(hasher, 3),
        Predicate::VersionEquals(version) => {
            append_u8(hasher, 4);
            append_version(hasher, version);
        }
    }
}

fn append_version(hasher: &mut Sha256, version: Version) {
    append_u64(hasher, version.get());
}

fn append_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    append_u64(hasher, bytes.len() as u64);
    hasher.update(bytes);
}

fn append_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

fn append_u8(hasher: &mut Sha256, value: u8) {
    hasher.update([value]);
}

fn encode_log_entry(out: &mut Vec<u8>, entry: &MetadataLogEntry) -> Result<(), MetadataLogError> {
    entry.verify_digest()?;
    put_string(out, &entry.shard_id);
    append_u64_to_vec(out, entry.epoch);
    append_u64_to_vec(out, entry.lsn);
    encode_command(out, &entry.command);
    encode_commit_result(out, &entry.result);
    out.extend_from_slice(&entry.prev_digest);
    out.extend_from_slice(&entry.digest);
    Ok(())
}

fn decode_log_entry(input: &mut LogDecoder<'_>) -> Result<MetadataLogEntry, MetadataLogError> {
    let shard_id = input.string()?;
    let epoch = input.u64()?;
    let lsn = input.u64()?;
    let command = decode_command(input)?;
    let result = decode_commit_result(input)?;
    let prev_digest = input.digest()?;
    let digest = input.digest()?;
    let entry = MetadataLogEntry {
        shard_id,
        epoch,
        lsn,
        request_id: command.request_id.clone(),
        command,
        result,
        prev_digest,
        digest,
    };
    entry.verify_digest()?;
    Ok(entry)
}

fn encode_command(out: &mut Vec<u8>, command: &MetadataCommand) {
    put_bytes(out, &command.request_id);
    out.push(command_kind_tag(command.kind));
    append_u64_to_vec(out, command.read_version.get());
    append_u64_to_vec(out, command.commit_version.get());
    out.push(record_family_tag(command.primary_family));
    put_bytes(out, &command.primary_key);
    append_u64_to_vec(out, command.predicates.len() as u64);
    for predicate in &command.predicates {
        out.push(record_family_tag(predicate.family));
        put_bytes(out, &predicate.key);
        encode_predicate(out, predicate.predicate);
    }
    append_u64_to_vec(out, command.mutations.len() as u64);
    for mutation in &command.mutations {
        out.push(record_family_tag(mutation.family));
        put_bytes(out, &mutation.key);
        out.push(mutation_op_tag(mutation.op));
        match &mutation.value {
            Some(value) => {
                out.push(1);
                put_bytes(out, &value.0);
            }
            None => out.push(0),
        }
    }
    append_u64_to_vec(out, command.watch.len() as u64);
    for event in &command.watch {
        out.push(record_family_tag(event.family));
        put_bytes(out, &event.key);
        put_bytes(out, &event.event);
    }
}

fn decode_command(input: &mut LogDecoder<'_>) -> Result<MetadataCommand, MetadataLogError> {
    let request_id = input.bytes()?.to_vec();
    let kind = command_kind_from_tag(input.u8()?)?;
    let read_version = version(input.u64()?)?;
    let commit_version = version(input.u64()?)?;
    let primary_family = record_family_from_tag(input.u8()?)?;
    let primary_key = input.bytes()?.to_vec();
    let predicate_count = input.count()?;
    let mut predicates = Vec::with_capacity(predicate_count.min(1024));
    for _ in 0..predicate_count {
        predicates.push(PredicateRef {
            family: record_family_from_tag(input.u8()?)?,
            key: input.bytes()?.to_vec(),
            predicate: decode_predicate(input)?,
        });
    }
    let mutation_count = input.count()?;
    let mut mutations = Vec::with_capacity(mutation_count.min(1024));
    for _ in 0..mutation_count {
        let family = record_family_from_tag(input.u8()?)?;
        let key = input.bytes()?.to_vec();
        let op = mutation_op_from_tag(input.u8()?)?;
        let value = match input.u8()? {
            0 => None,
            1 => Some(Value(input.bytes()?.to_vec())),
            tag => return Err(MetadataLogError::InvalidValueTag(tag)),
        };
        mutations.push(Mutation {
            family,
            key,
            op,
            value,
        });
    }
    let watch_count = input.count()?;
    let mut watch = Vec::with_capacity(watch_count.min(1024));
    for _ in 0..watch_count {
        watch.push(WatchProjection {
            family: record_family_from_tag(input.u8()?)?,
            key: input.bytes()?.to_vec(),
            event: input.bytes()?.to_vec(),
        });
    }
    let command = MetadataCommand {
        request_id,
        kind,
        read_version,
        commit_version,
        primary_family,
        primary_key,
        predicates,
        mutations,
        watch,
    };
    command.validate().map_err(MetadataLogError::Command)?;
    Ok(command)
}

fn encode_commit_result(out: &mut Vec<u8>, result: &CommitResult) {
    append_u64_to_vec(out, result.commit_version.get());
    append_u64_to_vec(out, result.applied_mutations as u64);
    append_u64_to_vec(out, result.watch_events as u64);
}

fn decode_commit_result(input: &mut LogDecoder<'_>) -> Result<CommitResult, MetadataLogError> {
    let commit_version = version(input.u64()?)?;
    let applied_mutations =
        usize::try_from(input.u64()?).map_err(|_| MetadataLogError::LengthOverflow)?;
    let watch_events =
        usize::try_from(input.u64()?).map_err(|_| MetadataLogError::LengthOverflow)?;
    Ok(CommitResult {
        commit_version,
        applied_mutations,
        watch_events,
    })
}

fn encode_predicate(out: &mut Vec<u8>, predicate: Predicate) {
    match predicate {
        Predicate::Exists => out.push(1),
        Predicate::NotExists => out.push(2),
        Predicate::PrefixEmpty => out.push(3),
        Predicate::VersionEquals(version) => {
            out.push(4);
            append_u64_to_vec(out, version.get());
        }
    }
}

fn decode_predicate(input: &mut LogDecoder<'_>) -> Result<Predicate, MetadataLogError> {
    match input.u8()? {
        1 => Ok(Predicate::Exists),
        2 => Ok(Predicate::NotExists),
        3 => Ok(Predicate::PrefixEmpty),
        4 => Ok(Predicate::VersionEquals(version(input.u64()?)?)),
        tag => Err(MetadataLogError::InvalidPredicateTag(tag)),
    }
}

fn version(raw: u64) -> Result<Version, MetadataLogError> {
    Version::new(raw).map_err(MetadataLogError::Command)
}

fn put_string(out: &mut Vec<u8>, value: &str) {
    put_bytes(out, value.as_bytes());
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    append_u64_to_vec(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn append_u64_to_vec(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn command_kind_tag(kind: CommandKind) -> u8 {
    match kind {
        CommandKind::ReserveAllocator => 1,
        CommandKind::CreateFile => 2,
        CommandKind::CreateFiles => 3,
        CommandKind::CreateDir => 4,
        CommandKind::CreateSymlink => 5,
        CommandKind::CreateSpecialNode => 6,
        CommandKind::UpdateAttr => 7,
        CommandKind::SetXattr => 8,
        CommandKind::RemoveXattr => 9,
        CommandKind::Rename => 10,
        CommandKind::RenameReplace => 11,
        CommandKind::Link => 12,
        CommandKind::RemoveFile => 13,
        CommandKind::RemoveEmptyDir => 14,
        CommandKind::PublishArtifact => 15,
        CommandKind::ReplaceArtifact => 16,
        CommandKind::SnapshotSubtree => 17,
        CommandKind::RetireSnapshot => 18,
        CommandKind::RenewSnapshot => 19,
        CommandKind::WatchSubtree => 20,
        CommandKind::CleanupObjects => 21,
        CommandKind::RegisterNamespaceIndex => 22,
    }
}

fn command_kind_from_tag(tag: u8) -> Result<CommandKind, MetadataLogError> {
    match tag {
        1 => Ok(CommandKind::ReserveAllocator),
        2 => Ok(CommandKind::CreateFile),
        3 => Ok(CommandKind::CreateFiles),
        4 => Ok(CommandKind::CreateDir),
        5 => Ok(CommandKind::CreateSymlink),
        6 => Ok(CommandKind::CreateSpecialNode),
        7 => Ok(CommandKind::UpdateAttr),
        8 => Ok(CommandKind::SetXattr),
        9 => Ok(CommandKind::RemoveXattr),
        10 => Ok(CommandKind::Rename),
        11 => Ok(CommandKind::RenameReplace),
        12 => Ok(CommandKind::Link),
        13 => Ok(CommandKind::RemoveFile),
        14 => Ok(CommandKind::RemoveEmptyDir),
        15 => Ok(CommandKind::PublishArtifact),
        16 => Ok(CommandKind::ReplaceArtifact),
        17 => Ok(CommandKind::SnapshotSubtree),
        18 => Ok(CommandKind::RetireSnapshot),
        19 => Ok(CommandKind::RenewSnapshot),
        20 => Ok(CommandKind::WatchSubtree),
        21 => Ok(CommandKind::CleanupObjects),
        22 => Ok(CommandKind::RegisterNamespaceIndex),
        tag => Err(MetadataLogError::InvalidCommandKind(tag)),
    }
}

fn mutation_op_tag(op: MutationOp) -> u8 {
    match op {
        MutationOp::Put => 1,
        MutationOp::Delete => 2,
    }
}

fn mutation_op_from_tag(tag: u8) -> Result<MutationOp, MetadataLogError> {
    match tag {
        1 => Ok(MutationOp::Put),
        2 => Ok(MutationOp::Delete),
        tag => Err(MetadataLogError::InvalidMutationOp(tag)),
    }
}

fn record_family_tag(family: RecordFamily) -> u8 {
    match family {
        RecordFamily::System => 1,
        RecordFamily::Mount => 2,
        RecordFamily::Inode => 3,
        RecordFamily::Dentry => 4,
        RecordFamily::Parent => 5,
        RecordFamily::Xattr => 6,
        RecordFamily::ChunkManifest => 7,
        RecordFamily::Session => 8,
        RecordFamily::PathIndex => 9,
        RecordFamily::Watch => 10,
        RecordFamily::Snapshot => 11,
        RecordFamily::Gc => 12,
        RecordFamily::CommandDedupe => 13,
        RecordFamily::History => 14,
        RecordFamily::ForkBinding => 15,
        RecordFamily::ForkShadow => 16,
    }
}

fn record_family_from_tag(tag: u8) -> Result<RecordFamily, MetadataLogError> {
    match tag {
        1 => Ok(RecordFamily::System),
        2 => Ok(RecordFamily::Mount),
        3 => Ok(RecordFamily::Inode),
        4 => Ok(RecordFamily::Dentry),
        5 => Ok(RecordFamily::Parent),
        6 => Ok(RecordFamily::Xattr),
        7 => Ok(RecordFamily::ChunkManifest),
        8 => Ok(RecordFamily::Session),
        9 => Ok(RecordFamily::PathIndex),
        10 => Ok(RecordFamily::Watch),
        11 => Ok(RecordFamily::Snapshot),
        12 => Ok(RecordFamily::Gc),
        13 => Ok(RecordFamily::CommandDedupe),
        14 => Ok(RecordFamily::History),
        15 => Ok(RecordFamily::ForkBinding),
        16 => Ok(RecordFamily::ForkShadow),
        tag => Err(MetadataLogError::InvalidRecordFamily(tag)),
    }
}

struct LogDecoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> LogDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn u8(&mut self) -> Result<u8, MetadataLogError> {
        if self.offset >= self.bytes.len() {
            return Err(MetadataLogError::Truncated);
        }
        let value = self.bytes[self.offset];
        self.offset += 1;
        Ok(value)
    }

    fn u64(&mut self) -> Result<u64, MetadataLogError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes(bytes.try_into().unwrap()))
    }

    fn count(&mut self) -> Result<usize, MetadataLogError> {
        usize::try_from(self.u64()?).map_err(|_| MetadataLogError::LengthOverflow)
    }

    fn bytes(&mut self) -> Result<&'a [u8], MetadataLogError> {
        let len = self.count()?;
        self.take(len)
    }

    fn string(&mut self) -> Result<String, MetadataLogError> {
        String::from_utf8(self.bytes()?.to_vec()).map_err(|_| MetadataLogError::InvalidUtf8)
    }

    fn digest(&mut self) -> Result<[u8; 32], MetadataLogError> {
        Ok(self.take(32)?.try_into().unwrap())
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], MetadataLogError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(MetadataLogError::Truncated)?;
        if end > self.bytes.len() {
            return Err(MetadataLogError::Truncated);
        }
        let out = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn finish(self) -> Result<(), MetadataLogError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(MetadataLogError::TrailingBytes)
        }
    }
}

impl fmt::Display for MetadataLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyShardId => write!(f, "metadata log shard id is empty"),
            Self::ZeroEpoch => write!(f, "metadata log epoch must be non-zero"),
            Self::ZeroLsn => write!(f, "metadata log lsn must be non-zero"),
            Self::EmptySegment => write!(f, "metadata log segment is empty"),
            Self::Command(err) => write!(f, "invalid metadata log command: {err}"),
            Self::ResultVersionMismatch { command, result } => write!(
                f,
                "metadata log result version {result} does not match command version {command}"
            ),
            Self::AppliedMutationMismatch { applied, expected } => write!(
                f,
                "metadata log applied {applied} mutations but command expects {expected}"
            ),
            Self::WatchEventMismatch { emitted, expected } => write!(
                f,
                "metadata log emitted {emitted} watch events but command expects {expected}"
            ),
            Self::DigestMismatch => write!(f, "metadata log digest mismatch"),
            Self::SegmentDigestMismatch => write!(f, "metadata log segment digest mismatch"),
            Self::SegmentHeaderMismatch => write!(f, "metadata log segment header mismatch"),
            Self::ChainShardMismatch { previous, next } => {
                write!(f, "metadata log shard changed from {previous} to {next}")
            }
            Self::ChainLsnGap { previous, next } => {
                write!(f, "metadata log lsn gap after {previous}: next is {next}")
            }
            Self::ChainDigestMismatch => write!(f, "metadata log previous digest mismatch"),
            Self::ChainEpochRegression { previous, next } => {
                write!(f, "metadata log epoch regressed from {previous} to {next}")
            }
            Self::InvalidSegmentMagic => write!(f, "invalid metadata log segment magic"),
            Self::UnsupportedSegmentVersion(version) => {
                write!(f, "unsupported metadata log segment version {version}")
            }
            Self::Truncated => write!(f, "metadata log segment is truncated"),
            Self::TrailingBytes => write!(f, "metadata log segment has trailing bytes"),
            Self::InvalidCommandKind(tag) => write!(f, "invalid metadata command kind tag {tag}"),
            Self::InvalidRecordFamily(tag) => write!(f, "invalid metadata record family tag {tag}"),
            Self::InvalidMutationOp(tag) => write!(f, "invalid metadata mutation op tag {tag}"),
            Self::InvalidPredicateTag(tag) => write!(f, "invalid metadata predicate tag {tag}"),
            Self::InvalidValueTag(tag) => write!(f, "invalid metadata value tag {tag}"),
            Self::InvalidUtf8 => write!(f, "metadata log segment contains invalid utf-8"),
            Self::LengthOverflow => write!(f, "metadata log segment length overflows usize"),
            Self::EntryRequestIdMismatch => {
                write!(f, "metadata log entry request id does not match command")
            }
        }
    }
}

impl std::error::Error for MetadataLogError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Mutation, PredicateRef, Value, WatchProjection};

    fn version(raw: u64) -> Version {
        Version::new(raw).unwrap()
    }

    fn command(request_id: &[u8], commit_version: u64) -> MetadataCommand {
        MetadataCommand {
            request_id: request_id.to_vec(),
            kind: CommandKind::CreateFile,
            read_version: version(commit_version - 1),
            commit_version: version(commit_version),
            primary_family: RecordFamily::Dentry,
            primary_key: b"primary".to_vec(),
            predicates: vec![PredicateRef {
                family: RecordFamily::Dentry,
                key: b"primary".to_vec(),
                predicate: Predicate::NotExists,
            }],
            mutations: vec![Mutation {
                family: RecordFamily::Dentry,
                key: b"primary".to_vec(),
                op: MutationOp::Put,
                value: Some(Value(b"value".to_vec())),
            }],
            watch: vec![WatchProjection {
                family: RecordFamily::Watch,
                key: b"watch".to_vec(),
                event: b"event".to_vec(),
            }],
        }
    }

    fn entry(
        request_id: &[u8],
        epoch: u64,
        lsn: u64,
        commit_version: u64,
        prev_digest: [u8; 32],
    ) -> MetadataLogEntry {
        MetadataLogEntry::seal(
            "mount-1:/runs",
            epoch,
            lsn,
            command(request_id, commit_version),
            result(commit_version),
            prev_digest,
        )
        .unwrap()
    }

    fn result(commit_version: u64) -> CommitResult {
        CommitResult {
            commit_version: version(commit_version),
            applied_mutations: 1,
            watch_events: 1,
        }
    }

    #[test]
    fn seal_sets_request_id_and_verifies_digest() {
        let entry = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            result(11),
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap();

        assert_eq!(entry.request_id, b"req-1");
        assert_ne!(entry.digest, METADATA_LOG_ZERO_DIGEST);
        entry.verify_digest().unwrap();
    }

    #[test]
    fn digest_changes_when_command_changes() {
        let first = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            result(11),
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap();
        let second = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-2", 11),
            result(11),
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap();

        assert_ne!(first.digest, second.digest);
    }

    #[test]
    fn entries_verify_chain_order_and_previous_digest() {
        let first = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            result(11),
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap();
        let second = MetadataLogEntry::seal(
            "mount-1:/runs",
            8,
            2,
            command(b"req-2", 12),
            result(12),
            first.digest,
        )
        .unwrap();

        second.verify_follows(&first).unwrap();
    }

    #[test]
    fn chain_verification_rejects_lsn_gap() {
        let first = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            result(11),
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap();
        let second = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            3,
            command(b"req-2", 12),
            result(12),
            first.digest,
        )
        .unwrap();

        assert!(matches!(
            second.verify_follows(&first),
            Err(MetadataLogError::ChainLsnGap {
                previous: 1,
                next: 3
            })
        ));
    }

    #[test]
    fn seal_rejects_result_version_mismatch() {
        let err = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            result(12),
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap_err();

        assert!(matches!(
            err,
            MetadataLogError::ResultVersionMismatch {
                command: 11,
                result: 12
            }
        ));
    }

    #[test]
    fn seal_rejects_result_count_mismatch() {
        let mut mutation_result = result(11);
        mutation_result.applied_mutations = 0;
        let mutation_err = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            mutation_result,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap_err();

        assert!(matches!(
            mutation_err,
            MetadataLogError::AppliedMutationMismatch {
                applied: 0,
                expected: 1
            }
        ));

        let mut watch_result = result(11);
        watch_result.watch_events = 0;
        let watch_err = MetadataLogEntry::seal(
            "mount-1:/runs",
            7,
            1,
            command(b"req-1", 11),
            watch_result,
            METADATA_LOG_ZERO_DIGEST,
        )
        .unwrap_err();

        assert!(matches!(
            watch_err,
            MetadataLogError::WatchEventMismatch {
                emitted: 0,
                expected: 1
            }
        ));
    }

    #[test]
    fn segment_encode_decode_round_trips_entries_and_header() {
        let first = entry(b"req-1", 7, 1, 11, METADATA_LOG_ZERO_DIGEST);
        let second = entry(b"req-2", 8, 2, 12, first.digest);
        let segment = MetadataLogSegment::seal(vec![first.clone(), second.clone()]).unwrap();

        assert_eq!(segment.shard_id, "mount-1:/runs");
        assert_eq!(segment.first_epoch, 7);
        assert_eq!(segment.last_epoch, 8);
        assert_eq!(segment.first_lsn, 1);
        assert_eq!(segment.last_lsn, 2);
        assert_eq!(segment.prev_digest, METADATA_LOG_ZERO_DIGEST);
        assert_eq!(segment.last_digest, second.digest);
        assert_ne!(segment.digest, METADATA_LOG_ZERO_DIGEST);

        let encoded = segment.encode().unwrap();
        let decoded = MetadataLogSegment::decode(&encoded).unwrap();
        assert_eq!(decoded, segment);
        assert_eq!(decoded.entries, vec![first, second]);
    }

    #[test]
    fn segment_decode_rejects_tampered_bytes() {
        let first = entry(b"req-1", 7, 1, 11, METADATA_LOG_ZERO_DIGEST);
        let segment = MetadataLogSegment::seal(vec![first]).unwrap();
        let mut encoded = segment.encode().unwrap();
        let last = encoded.len() - 1;
        encoded[last] ^= 0x40;

        assert!(matches!(
            MetadataLogSegment::decode(&encoded),
            Err(MetadataLogError::DigestMismatch)
                | Err(MetadataLogError::SegmentDigestMismatch)
                | Err(MetadataLogError::Command(_))
        ));
    }

    #[test]
    fn segment_seal_rejects_non_contiguous_entries() {
        let first = entry(b"req-1", 7, 1, 11, METADATA_LOG_ZERO_DIGEST);
        let second = entry(b"req-2", 7, 3, 12, first.digest);

        assert!(matches!(
            MetadataLogSegment::seal(vec![first, second]),
            Err(MetadataLogError::ChainLsnGap {
                previous: 1,
                next: 3
            })
        ));
    }

    #[test]
    fn replay_entries_verify_segment_chain_after_checkpoint() {
        let first = entry(b"req-1", 7, 1, 11, METADATA_LOG_ZERO_DIGEST);
        let second = entry(b"req-2", 7, 2, 12, first.digest);
        let third = entry(b"req-3", 8, 3, 13, second.digest);
        let left = MetadataLogSegment::seal(vec![first.clone(), second.clone()]).unwrap();
        let right = MetadataLogSegment::seal(vec![third.clone()]).unwrap();

        let replay =
            metadata_log_replay_entries(&[left, right], 0, METADATA_LOG_ZERO_DIGEST).unwrap();

        assert_eq!(replay, vec![first, second, third]);
    }

    #[test]
    fn replay_entries_reject_segment_gap_after_checkpoint() {
        let first = entry(b"req-1", 7, 2, 11, METADATA_LOG_ZERO_DIGEST);
        let segment = MetadataLogSegment::seal(vec![first]).unwrap();

        assert!(matches!(
            metadata_log_replay_entries(&[segment], 0, METADATA_LOG_ZERO_DIGEST),
            Err(MetadataLogError::ChainLsnGap {
                previous: 0,
                next: 2
            })
        ));
    }
}
