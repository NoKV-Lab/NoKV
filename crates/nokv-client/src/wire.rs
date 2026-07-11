use nokv_meta::{SubtreeDelta, SubtreeDeltaKind, UpdateAttr, XattrSetMode};
use nokv_object::{ObjectReadBlock, ObjectReadPlan, StagedObjectSet};
use nokv_protocol::{
    WireAdvisoryLock, WireBodyDescriptor, WireBodyReadPlan, WireChunkManifest, WireDentryWithAttr,
    WireMetadataError, WireObjectReadBlock, WirePathMetadata, WirePreparedArtifact, WireReadLease,
    WireStagedObject, WireStagedObjectSet, WireSubtreeDelta, WireSubtreeDeltaKind, WireUpdateAttr,
    WireXattrSetMode,
};
use nokv_types::{
    parse_absolute_path, AdvisoryLock, BodyDescriptor, ChunkManifest, DentryName, InodeId,
    PathMetadata, ReadLease, SnapshotPin,
};

use crate::service::ClientPreparedArtifact;
use crate::ClientError;

pub(crate) fn rpc_parent_and_name(path: &str) -> Result<(String, String), ClientError> {
    let mut components = parse_absolute_path(path)?;
    let name = components.pop().ok_or(ClientError::RootHasNoParent)?;
    let name = rpc_name(&name)?;
    let mut parent = String::from("/");
    for (index, component) in components.iter().enumerate() {
        if index > 0 {
            parent.push('/');
        }
        parent.push_str(&rpc_name(component)?);
    }
    Ok((parent, name))
}

pub(crate) fn rpc_name(name: &DentryName) -> Result<String, ClientError> {
    String::from_utf8(name.as_bytes().to_vec())
        .map_err(|_| ClientError::InvalidName("metadata rpc requires utf-8 names".to_owned()))
}

pub(crate) fn rpc_components(components: &[DentryName]) -> Result<Vec<String>, ClientError> {
    components.iter().map(rpc_name).collect()
}

pub(crate) fn update_attr_to_wire(changes: UpdateAttr) -> WireUpdateAttr {
    WireUpdateAttr {
        mode: changes.mode,
        uid: changes.uid,
        gid: changes.gid,
        size: changes.size,
        mtime_ms: changes.mtime_ms,
        ctime_ms: changes.ctime_ms,
    }
}

pub(crate) fn xattr_set_mode_to_wire(mode: XattrSetMode) -> WireXattrSetMode {
    match mode {
        XattrSetMode::Any => WireXattrSetMode::Any,
        XattrSetMode::Create => WireXattrSetMode::Create,
        XattrSetMode::Replace => WireXattrSetMode::Replace,
    }
}

pub(crate) fn wire_dentry(
    entry: WireDentryWithAttr,
) -> Result<nokv_meta::DentryWithAttr, ClientError> {
    Ok(nokv_meta::DentryWithAttr {
        dentry: entry.dentry.into_dentry_record().map_err(protocol_error)?,
        attr: entry.attr.into_inode_attr().map_err(protocol_error)?,
        body: entry.body.map(|body| body.into_body_descriptor()),
    })
}

pub(crate) fn wire_path_metadata(metadata: WirePathMetadata) -> Result<PathMetadata, ClientError> {
    metadata.into_path_metadata().map_err(protocol_error)
}

pub(crate) fn wire_prepared_artifact(
    prepared: WirePreparedArtifact,
) -> Result<ClientPreparedArtifact, ClientError> {
    Ok(ClientPreparedArtifact {
        mount: prepared.mount,
        parent: inode_id(prepared.parent)?,
        name: DentryName::new(prepared.name.into_bytes())
            .map_err(|err| ClientError::InvalidName(err.to_string()))?,
        path: prepared.path,
        inode: inode_id(prepared.inode)?,
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
        object_gc_claim_version: prepared.object_gc_claim_version,
    })
}

pub(crate) fn prepared_artifact_to_wire(
    prepared: &ClientPreparedArtifact,
) -> Result<WirePreparedArtifact, ClientError> {
    Ok(WirePreparedArtifact {
        mount: prepared.mount,
        parent: prepared.parent.get(),
        name: rpc_name(&prepared.name)?,
        path: prepared.path.clone(),
        inode: prepared.inode.get(),
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
        object_gc_claim_version: prepared.object_gc_claim_version,
    })
}

pub(crate) fn body_to_wire(body: &BodyDescriptor) -> WireBodyDescriptor {
    WireBodyDescriptor::from_body_descriptor(body)
}

pub(crate) fn chunk_to_wire(chunk: &ChunkManifest) -> WireChunkManifest {
    WireChunkManifest::from_chunk_manifest(chunk)
}

pub(crate) fn staged_object_set_to_wire(staged: &StagedObjectSet) -> WireStagedObjectSet {
    WireStagedObjectSet {
        objects: staged
            .objects()
            .iter()
            .map(|object| WireStagedObject {
                key: object.key.as_str().to_owned(),
                size: object.size,
            })
            .collect(),
    }
}

pub(crate) fn wire_body_read_plan(plan: WireBodyReadPlan) -> Result<ObjectReadPlan, ClientError> {
    Ok(ObjectReadPlan::new(
        usize::try_from(plan.output_len).map_err(|_| {
            ClientError::Protocol("body read plan output length exceeds platform limit".to_owned())
        })?,
        plan.blocks
            .into_iter()
            .map(wire_object_read_block)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn wire_object_read_block(block: WireObjectReadBlock) -> Result<ObjectReadBlock, ClientError> {
    Ok(ObjectReadBlock {
        object_key: block.object_key,
        digest_uri: block.digest_uri,
        object_offset: block.object_offset,
        object_len: block.object_len,
        len: usize::try_from(block.len).map_err(|_| {
            ClientError::Protocol("body read block length exceeds platform limit".to_owned())
        })?,
        output_offset: usize::try_from(block.output_offset).map_err(|_| {
            ClientError::Protocol("body read block offset exceeds platform limit".to_owned())
        })?,
    })
}

pub(crate) fn wire_snapshot(
    snapshot: nokv_protocol::WireSnapshotPin,
) -> Result<SnapshotPin, ClientError> {
    snapshot.into_snapshot_pin().map_err(protocol_error)
}

pub(crate) fn wire_read_lease(lease: WireReadLease) -> Result<ReadLease, ClientError> {
    lease.into_read_lease().map_err(protocol_error)
}

pub(crate) fn subtree_delta(delta: WireSubtreeDelta) -> SubtreeDelta {
    SubtreeDelta {
        path: delta.path,
        kind: match delta.kind {
            WireSubtreeDeltaKind::Added => SubtreeDeltaKind::Added,
            WireSubtreeDeltaKind::Removed => SubtreeDeltaKind::Removed,
            WireSubtreeDeltaKind::Modified => SubtreeDeltaKind::Modified,
        },
        digest: delta.digest,
        size_delta: delta.size_delta,
    }
}

pub(crate) fn wire_advisory_lock(lock: WireAdvisoryLock) -> Result<AdvisoryLock, ClientError> {
    lock.into_advisory_lock().map_err(protocol_error)
}

pub(crate) fn inode_id(raw: u64) -> Result<InodeId, ClientError> {
    InodeId::new(raw).map_err(|err| ClientError::Protocol(err.to_string()))
}

pub(crate) fn protocol_error(err: nokv_protocol::MetadataProtocolError) -> ClientError {
    ClientError::Protocol(err.to_string())
}

pub(crate) fn dentry_result(
    result: nokv_protocol::MetadataRpcResult,
) -> Result<nokv_meta::DentryWithAttr, ClientError> {
    match result {
        nokv_protocol::MetadataRpcResult::Dentry { entry: Some(entry) } => wire_dentry(*entry),
        other => Err(unexpected_result(other)),
    }
}

pub(crate) fn envelope_result(
    envelope: nokv_protocol::MetadataRpcEnvelope,
) -> Result<nokv_protocol::MetadataRpcResult, ClientError> {
    if !envelope.ok {
        let message = envelope
            .error
            .unwrap_or_else(|| "unknown metadata service error".to_owned());
        let Some(error) = envelope.error_kind else {
            return Err(ClientError::Protocol(format!(
                "metadata rpc error is missing typed error_kind: {message}"
            )));
        };
        return Err(client_error_from_wire_error(error));
    }
    envelope
        .result
        .ok_or_else(|| ClientError::Protocol("metadata rpc response missing result".to_owned()))
}

pub(crate) fn client_error_from_wire_error(error: WireMetadataError) -> ClientError {
    match error {
        WireMetadataError::NotFound => ClientError::Metadata(nokv_meta::MetadError::NotFound),
        WireMetadataError::NotFile => ClientError::Metadata(nokv_meta::MetadError::NotFile),
        WireMetadataError::NotDirectory => {
            ClientError::Metadata(nokv_meta::MetadError::NotDirectory)
        }
        WireMetadataError::DirectoryNotEmpty => {
            ClientError::Metadata(nokv_meta::MetadError::DirectoryNotEmpty)
        }
        WireMetadataError::MissingBodyDescriptor => {
            ClientError::Metadata(nokv_meta::MetadError::MissingBodyDescriptor)
        }
        WireMetadataError::PredicateFailed => ClientError::Metadata(
            nokv_meta::MetadError::Metadata(nokv_meta::MetadataError::PredicateFailed),
        ),
        WireMetadataError::StaleBodyGeneration { expected, current } => {
            ClientError::Metadata(nokv_meta::MetadError::StaleBodyGeneration { expected, current })
        }
        WireMetadataError::StaleOwnerEpoch {
            owner_epoch,
            required_epoch,
        } => ClientError::Metadata(nokv_meta::MetadError::StaleOwnerEpoch {
            owner_epoch,
            required_epoch,
        }),
        WireMetadataError::LeaseExpired {
            now_ms,
            deadline_ms,
        } => ClientError::Metadata(nokv_meta::MetadError::LeaseExpired {
            now_ms,
            deadline_ms,
        }),
        WireMetadataError::NotOwner { shard_id, endpoint } => {
            ClientError::Metadata(nokv_meta::MetadError::NotOwner { shard_id, endpoint })
        }
        WireMetadataError::CrossShard {
            source_shard,
            dest_shard,
        } => ClientError::Metadata(nokv_meta::MetadError::CrossShard {
            source_shard,
            dest_shard,
        }),
        WireMetadataError::GraftPoint => ClientError::Metadata(nokv_meta::MetadError::GraftPoint),
        WireMetadataError::RestoreInProgress => {
            ClientError::Metadata(nokv_meta::MetadError::RestoreInProgress)
        }
        WireMetadataError::RestoreResourceLimit {
            resource,
            limit,
            actual,
        } => ClientError::Metadata(nokv_meta::MetadError::RestoreResourceLimit {
            resource,
            limit,
            actual,
        }),
        WireMetadataError::StalePreparedArtifactObjectGcEpoch { expected, current } => {
            ClientError::Metadata(nokv_meta::MetadError::StalePreparedArtifactObjectGcEpoch {
                expected,
                current,
            })
        }
        WireMetadataError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms,
            now_ms,
        } => ClientError::Metadata(nokv_meta::MetadError::SnapshotLeaseExpired {
            snapshot_id,
            lease_expires_unix_ms,
            now_ms,
        }),
        WireMetadataError::SnapshotRootMismatch {
            snapshot_id,
            expected_root,
            actual_root,
            actual_shard,
        } => match (
            InodeId::new(expected_root),
            actual_root.map(InodeId::new).transpose(),
        ) {
            (Ok(expected_root), Ok(actual_root)) => {
                ClientError::Metadata(nokv_meta::MetadError::SnapshotRootMismatch {
                    snapshot_id,
                    expected_root,
                    actual_root,
                    actual_shard,
                })
            }
            _ => ClientError::Protocol("snapshot root error carries an invalid inode".to_owned()),
        },
        WireMetadataError::SnapshotBindingChanged { root_path } => {
            ClientError::Metadata(nokv_meta::MetadError::SnapshotBindingChanged { root_path })
        }
        WireMetadataError::SnapshotRenewContended {
            snapshot_id,
            attempts,
        } => ClientError::Metadata(nokv_meta::MetadError::SnapshotRenewContended {
            snapshot_id,
            attempts,
        }),
        WireMetadataError::RestoreHardlinkUnsupported { inode } => match inode_id(inode) {
            Ok(inode) => {
                ClientError::Metadata(nokv_meta::MetadError::RestoreHardlinkUnsupported { inode })
            }
            Err(err) => err,
        },
        WireMetadataError::RestoreCrossShardUnsupported { inode } => match inode_id(inode) {
            Ok(inode) => {
                ClientError::Metadata(nokv_meta::MetadError::RestoreCrossShardUnsupported { inode })
            }
            Err(err) => err,
        },
        WireMetadataError::RestoreDestinationConflict { destination } => {
            ClientError::Metadata(nokv_meta::MetadError::RestoreDestinationConflict { destination })
        }
        WireMetadataError::SyncLogArchiveFailed { committed, message } => {
            ClientError::Metadata(nokv_meta::MetadError::SyncLogArchiveFailed {
                committed,
                message,
            })
        }
        WireMetadataError::LockConflict { lock } => match wire_advisory_lock(lock) {
            Ok(lock) => ClientError::LockConflict(lock),
            Err(err) => err,
        },
        WireMetadataError::InvalidPath { message } => {
            ClientError::Metadata(nokv_meta::MetadError::InvalidPath(message))
        }
        WireMetadataError::InvalidQuery { message } => {
            ClientError::Metadata(nokv_meta::MetadError::InvalidQuery(message))
        }
        WireMetadataError::Metadata { message } => ClientError::Metadata(
            nokv_meta::MetadError::Metadata(nokv_meta::MetadataError::Backend(message)),
        ),
        WireMetadataError::Object { message } => {
            ClientError::Object(nokv_object::ObjectError::Backend(message))
        }
        WireMetadataError::Io { message } => ClientError::Io(message),
        WireMetadataError::Protocol { message } => ClientError::Protocol(message),
    }
}

pub(crate) fn unexpected_result(result: nokv_protocol::MetadataRpcResult) -> ClientError {
    ClientError::Protocol(format!("unexpected metadata rpc result {result:?}"))
}
