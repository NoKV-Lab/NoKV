use nokv_meta::{
    DentryWithAttr, MetadError, PreparedArtifact, SubtreeDelta, SubtreeDeltaKind, UpdateAttr,
    XattrSetMode,
};
use nokv_object::{ObjectKey, ObjectReadBlock, StagedObject, StagedObjectSet};
use nokv_protocol::{
    MetadataProtocolError, MetadataRpcEnvelope, MetadataRpcResult, WireAdvisoryLock,
    WireBodyReadPlan, WireDentryWithAttr, WireMetadataError, WireObjectReadBlock,
    WireOpenPathReadPlan, WirePathMetadata, WirePreparedArtifact, WireReadLease,
    WireStagedObjectSet, WireSubtreeDelta, WireSubtreeDeltaKind, WireUpdateAttr, WireXattrSetMode,
};
use nokv_types::{DentryName, InodeId, MountId};

use crate::server::ServerError;

pub(super) fn ok_envelope(result: MetadataRpcResult) -> MetadataRpcEnvelope {
    MetadataRpcEnvelope {
        ok: true,
        result: Some(result),
        error: None,
        error_kind: None,
    }
}

pub(super) fn err_envelope(err: ServerError) -> MetadataRpcEnvelope {
    let error_kind = wire_server_error(&err);
    MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(error_kind),
    }
}

pub(super) fn wire_server_error(err: &ServerError) -> WireMetadataError {
    match err {
        ServerError::Io(err) => WireMetadataError::Io {
            message: err.to_string(),
        },
        ServerError::Object(err) => WireMetadataError::Object {
            message: err.to_string(),
        },
        ServerError::Control(err) => WireMetadataError::Metadata {
            message: err.to_string(),
        },
        ServerError::NotOwner { shard_id, endpoint } => WireMetadataError::NotOwner {
            shard_id: shard_id.clone(),
            endpoint: endpoint.clone(),
        },
        ServerError::Metadata(err) => wire_metad_error(err),
    }
}

fn wire_metad_error(err: &MetadError) -> WireMetadataError {
    match err {
        MetadError::Metadata(nokv_meta::MetadataError::PredicateFailed) => {
            WireMetadataError::PredicateFailed
        }
        MetadError::Metadata(err) => WireMetadataError::Metadata {
            message: err.to_string(),
        },
        MetadError::Object(err) => WireMetadataError::Object {
            message: err.to_string(),
        },
        MetadError::PublishArtifactFailed { source, .. } => wire_metad_error(source),
        MetadError::StaleBodyGeneration { expected, current } => {
            WireMetadataError::StaleBodyGeneration {
                expected: *expected,
                current: *current,
            }
        }
        MetadError::StaleOwnerEpoch {
            owner_epoch,
            required_epoch,
        } => WireMetadataError::StaleOwnerEpoch {
            owner_epoch: *owner_epoch,
            required_epoch: *required_epoch,
        },
        MetadError::LeaseExpired {
            now_ms,
            deadline_ms,
        } => WireMetadataError::LeaseExpired {
            now_ms: *now_ms,
            deadline_ms: *deadline_ms,
        },
        MetadError::SyncLogArchiveFailed { committed, message } => {
            WireMetadataError::SyncLogArchiveFailed {
                committed: *committed,
                message: message.clone(),
            }
        }
        MetadError::LockConflict(lock) => WireMetadataError::LockConflict {
            lock: WireAdvisoryLock::from_advisory_lock(lock),
        },
        MetadError::InvalidPath(message) => WireMetadataError::InvalidPath {
            message: message.clone(),
        },
        MetadError::InvalidQuery(message) => WireMetadataError::InvalidQuery {
            message: message.clone(),
        },
        MetadError::NotFound => WireMetadataError::NotFound,
        MetadError::NotFile => WireMetadataError::NotFile,
        MetadError::NotDirectory => WireMetadataError::NotDirectory,
        MetadError::DirectoryNotEmpty => WireMetadataError::DirectoryNotEmpty,
        MetadError::MissingBodyDescriptor => WireMetadataError::MissingBodyDescriptor,
        MetadError::CrossShard {
            source_shard,
            dest_shard,
        } => WireMetadataError::CrossShard {
            source_shard: *source_shard,
            dest_shard: *dest_shard,
        },
        MetadError::GraftPoint => WireMetadataError::GraftPoint,
        other => WireMetadataError::Metadata {
            message: other.to_string(),
        },
    }
}

pub(super) fn inode_id(raw: u64) -> Result<InodeId, MetadError> {
    InodeId::new(raw).map_err(Into::into)
}

pub(super) fn dentry_name(name: String) -> Result<DentryName, MetadError> {
    DentryName::new(name.into_bytes()).map_err(|err| MetadError::Codec(err.to_string()))
}

pub(super) fn update_attr(wire: WireUpdateAttr) -> UpdateAttr {
    UpdateAttr {
        mode: wire.mode,
        uid: wire.uid,
        gid: wire.gid,
        size: wire.size,
        mtime_ms: wire.mtime_ms,
        ctime_ms: wire.ctime_ms,
    }
}

pub(super) fn xattr_set_mode(wire: WireXattrSetMode) -> XattrSetMode {
    match wire {
        WireXattrSetMode::Any => XattrSetMode::Any,
        WireXattrSetMode::Create => XattrSetMode::Create,
        WireXattrSetMode::Replace => XattrSetMode::Replace,
    }
}

pub(super) fn wire_dentry(entry: &DentryWithAttr) -> WireDentryWithAttr {
    WireDentryWithAttr {
        dentry: nokv_protocol::WireDentryRecord::from_dentry_record(&entry.dentry),
        attr: nokv_protocol::WireInodeAttr::from_inode_attr(&entry.attr),
        body: entry
            .body
            .as_ref()
            .map(nokv_protocol::WireBodyDescriptor::from_body_descriptor),
    }
}

pub(super) fn wire_subtree_delta(delta: &SubtreeDelta) -> WireSubtreeDelta {
    WireSubtreeDelta {
        path: delta.path.clone(),
        kind: match delta.kind {
            SubtreeDeltaKind::Added => WireSubtreeDeltaKind::Added,
            SubtreeDeltaKind::Removed => WireSubtreeDeltaKind::Removed,
            SubtreeDeltaKind::Modified => WireSubtreeDeltaKind::Modified,
        },
        digest: delta.digest.clone(),
        size_delta: delta.size_delta,
    }
}

pub(super) fn wire_prepared_artifact(
    mount: MountId,
    prepared: &PreparedArtifact,
) -> WirePreparedArtifact {
    WirePreparedArtifact {
        mount: mount.get(),
        parent: prepared.parent.get(),
        name: String::from_utf8(prepared.name.as_bytes().to_vec())
            .expect("metadata prepared artifact names are utf-8"),
        path: prepared.path.clone(),
        inode: prepared.inode.get(),
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    }
}

pub(super) fn prepared_artifact(
    prepared: WirePreparedArtifact,
) -> Result<PreparedArtifact, MetadError> {
    MountId::new(prepared.mount)?;
    Ok(PreparedArtifact {
        parent: inode_id(prepared.parent)?,
        name: dentry_name(prepared.name)?,
        path: prepared.path,
        inode: inode_id(prepared.inode)?,
        generation: prepared.generation,
        mtime_ms: prepared.mtime_ms,
        ctime_ms: prepared.ctime_ms,
        replace: prepared.replace,
        dentry_version: prepared.dentry_version,
        old_generation: prepared.old_generation,
    })
}

pub(super) fn staged_object_set(
    staged: WireStagedObjectSet,
) -> Result<StagedObjectSet, MetadError> {
    staged
        .objects
        .into_iter()
        .map(|object| {
            Ok(StagedObject {
                key: ObjectKey::new(object.key)?,
                size: object.size,
            })
        })
        .collect::<Result<Vec<_>, MetadError>>()
        .map(StagedObjectSet::new)
}

pub(super) fn wire_body_read_plan(plan: &nokv_meta::BodyReadPlan) -> WireBodyReadPlan {
    WireBodyReadPlan {
        output_len: plan.output_len as u64,
        blocks: plan.blocks.iter().map(wire_object_read_block).collect(),
    }
}

pub(super) fn wire_open_path_read_plan(open: &nokv_meta::OpenPathReadPlan) -> WireOpenPathReadPlan {
    WireOpenPathReadPlan {
        metadata: WirePathMetadata::from_path_metadata(&open.metadata),
        lease: WireReadLease::from_read_lease(&open.lease),
        plan: wire_body_read_plan(&open.plan),
    }
}

fn wire_object_read_block(block: &ObjectReadBlock) -> WireObjectReadBlock {
    WireObjectReadBlock {
        object_key: block.object_key.clone(),
        digest_uri: block.digest_uri.clone(),
        object_offset: block.object_offset,
        object_len: block.object_len,
        len: block.len as u64,
        output_offset: block.output_offset as u64,
    }
}

pub(super) fn protocol_error(err: MetadataProtocolError) -> MetadError {
    MetadError::Codec(err.to_string())
}
