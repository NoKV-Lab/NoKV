use std::io;

mod batch;
#[cfg(test)]
mod peer;
mod transport;
mod wire;

#[cfg(test)]
pub(crate) use peer::FramedRpcClient;
pub(crate) use transport::{
    default_framed_rpc_queue_capacity, default_framed_rpc_worker_count,
    handle_framed_stream_after_magic, RpcWorkerPool, FRAMED_RPC_MAGIC,
};
#[cfg(test)]
pub(crate) use transport::{
    read_frame, write_frame, MAX_FRAMED_RPC_WORKERS, MIN_FRAMED_RPC_WORKERS,
};

use nokv_meta::{
    MetadError, OpenPathReadPlanRequest, PublishArtifactStagedSession, RestoreInitialization,
    RestoreInitializationFile, RestoreState,
};
use nokv_protocol::{
    decode_advisory_lock_kind, decode_file_type, decode_name_cursor, decode_request,
    decode_xattr_name, encode_envelope, encode_name_cursor, encode_xattr_name, MetadataRpcEnvelope,
    MetadataRpcRequest, MetadataRpcResult, WireAdvisoryLock, WireMetadataCapabilities,
    WireMetadataError, WireOpenPathReadPlanRequest, WirePathMetadata, WireRestoreOutcome,
    WireRestoreState,
};
use nokv_types::{AdvisoryLockRequest, SpecialNodeSpec};

use crate::server::{Server, ServerError};

use batch::{create_path_batch_envelopes, execute_batch, CreatePathKind};
use wire::{
    dentry_name, err_envelope, inode_id, namespace_aggregate_request, namespace_find_request,
    namespace_grep_request, namespace_index_registration, namespace_read_options,
    prepared_artifact, protocol_error, staged_object_set, update_attr, wire_body_read_plan,
    wire_dentry, wire_namespace_aggregate_result, wire_namespace_card, wire_namespace_find_result,
    wire_namespace_grep_result, wire_namespace_list_page, wire_namespace_read_page,
    wire_open_path_read_plan, wire_prepared_artifact, wire_subtree_delta, xattr_set_mode,
};

fn handle_binary_rpc(server: &Server, body: &[u8]) -> Result<Vec<u8>, ServerError> {
    let envelope = match decode_request(body) {
        Ok(request) => match execute_visible(server, request) {
            Ok(result) => MetadataRpcEnvelope {
                ok: true,
                result: Some(result),
                error: None,
                error_kind: None,
            },
            Err(err) => err_envelope(err),
        },
        Err(err) => MetadataRpcEnvelope {
            ok: false,
            result: None,
            error: Some(format!("invalid metadata binary rpc request: {err}")),
            error_kind: Some(WireMetadataError::Protocol {
                message: err.to_string(),
            }),
        },
    };
    encode_envelope(&envelope).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata binary rpc response encode failed: {err}"),
        ))
    })
}

fn execute_visible(
    server: &Server,
    request: MetadataRpcRequest,
) -> Result<MetadataRpcResult, ServerError> {
    if matches!(request, MetadataRpcRequest::Batch { .. }) {
        return execute_unfenced(server, request);
    }
    let slot = server.route(&request)?;
    let commits_metadata = commits_metadata_view(&request);
    server
        .execute_with_shard_visibility(slot, commits_metadata, || execute_unfenced(server, request))
}

fn execute_unfenced(
    server: &Server,
    request: MetadataRpcRequest,
) -> Result<MetadataRpcResult, ServerError> {
    // A batch re-routes each sub-request to its own shard, so resolve it there.
    if let MetadataRpcRequest::Batch { requests } = request {
        return execute_batch(server, requests);
    }
    let slot = server.route(&request)?;
    if refreshes_metadata_view(&request) {
        slot.service()
            .refresh_allocator_state()
            .map_err(ServerError::Metadata)?;
    }
    match request {
        MetadataRpcRequest::Batch { .. } => unreachable!("batch is handled before routing"),
        MetadataRpcRequest::BootstrapRoot { mode, uid, gid } => {
            let attr = slot.service().bootstrap_root(mode, uid, gid)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: Some(nokv_protocol::WireInodeAttr::from_inode_attr(&attr)),
            })
        }
        MetadataRpcRequest::GetAttr { inode } => {
            let attr = slot.service().get_attr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr
                    .as_ref()
                    .map(nokv_protocol::WireInodeAttr::from_inode_attr),
            })
        }
        MetadataRpcRequest::GetAttrAtSnapshot {
            root_path,
            snapshot_id,
            path_components,
        } => {
            let path_components = dentry_components(path_components)?;
            let attr =
                slot.service()
                    .get_attr_at_snapshot(&root_path, snapshot_id, &path_components)?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: attr
                    .as_ref()
                    .map(nokv_protocol::WireInodeAttr::from_inode_attr),
            })
        }
        MetadataRpcRequest::LookupPlus { parent, name } => {
            let entry = slot
                .service()
                .lookup_plus(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.as_ref().map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::CurrentDentryVersion { parent, name } => {
            let version = slot
                .service()
                .current_dentry_version(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::DentryVersion { version })
        }
        MetadataRpcRequest::LookupPlusAtSnapshot {
            root_path,
            snapshot_id,
            parent_components,
            name,
        } => {
            let parent_components = dentry_components(parent_components)?;
            let entry = slot.service().lookup_plus_at_snapshot(
                &root_path,
                snapshot_id,
                &parent_components,
                &dentry_name(name)?,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.as_ref().map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::LookupPath { path } => {
            let entry = slot.service().lookup_path(&path)?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.as_ref().map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::StatPath { path } => {
            let metadata = slot.service().stat_path(&path)?;
            Ok(MetadataRpcResult::PathMetadata {
                metadata: metadata.as_ref().map(WirePathMetadata::from_path_metadata),
            })
        }
        MetadataRpcRequest::ReadDirPlus { parent } => {
            let entries = slot.service().read_dir_plus(inode_id(parent)?)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::ReadDirPlusPage {
            parent,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page =
                slot.service()
                    .read_dir_plus_page(inode_id(parent)?, after.as_ref(), limit)?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
            })
        }
        MetadataRpcRequest::ReadDirPlusAtSnapshot {
            root_path,
            snapshot_id,
            path_components,
        } => {
            let path_components = dentry_components(path_components)?;
            let entries = slot.service().read_dir_plus_at_snapshot(
                &root_path,
                snapshot_id,
                &path_components,
            )?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::ReadDirPlusPath { path } => {
            let entries = slot.service().read_dir_plus_path(&path)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::ReadDirPlusPathPage {
            path,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page = slot
                .service()
                .read_dir_plus_path_page(&path, after.as_ref(), limit)?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
            })
        }
        MetadataRpcRequest::ReadIndexedPathPage {
            path,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page = slot
                .service()
                .list_indexed_path_page(&path, after.as_ref(), limit)?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
            })
        }
        MetadataRpcRequest::StatCard { path } => {
            let card = slot.service().stat_card(&path)?;
            Ok(MetadataRpcResult::NamespaceCard {
                card: card
                    .as_ref()
                    .map(|card| Box::new(wire_namespace_card(card))),
            })
        }
        MetadataRpcRequest::ListPage {
            path,
            cursor,
            limit,
        } => {
            let limit = usize::try_from(limit).map_err(|_| {
                ServerError::Metadata(MetadError::InvalidQuery(
                    "namespace list limit exceeds platform limit".to_owned(),
                ))
            })?;
            let page = slot
                .service()
                .list_page(&path, nokv_meta::NamespaceListOptions { cursor, limit })?;
            Ok(MetadataRpcResult::NamespaceListPage {
                page: Box::new(wire_namespace_list_page(&page)?),
            })
        }
        MetadataRpcRequest::FindPaths { request } => {
            let result = slot
                .service()
                .find_paths(namespace_find_request(*request)?)?;
            Ok(MetadataRpcResult::NamespaceFindResult {
                result: Box::new(wire_namespace_find_result(&result)?),
            })
        }
        MetadataRpcRequest::RegisterNamespaceIndex { registration } => {
            slot.service()
                .register_namespace_index(namespace_index_registration(*registration))?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::AggregatePaths { request } => {
            let result = slot
                .service()
                .aggregate_paths(namespace_aggregate_request(*request)?)?;
            Ok(MetadataRpcResult::NamespaceAggregateResult {
                result: Box::new(wire_namespace_aggregate_result(&result)),
            })
        }
        MetadataRpcRequest::GrepPaths { request } => {
            let result = slot
                .service()
                .grep_paths(namespace_grep_request(*request)?)?;
            Ok(MetadataRpcResult::NamespaceGrepResult {
                result: Box::new(wire_namespace_grep_result(&result)?),
            })
        }
        MetadataRpcRequest::ReadPage { path, options } => {
            let page = slot
                .service()
                .read_page(&path, namespace_read_options(*options)?)?;
            Ok(MetadataRpcResult::NamespaceReadPage {
                page: Box::new(wire_namespace_read_page(&page)?),
            })
        }
        MetadataRpcRequest::CreateDir {
            parent,
            name,
            mode,
            uid,
            gid,
        } => {
            let entry =
                slot.service()
                    .create_dir(inode_id(parent)?, dentry_name(name)?, mode, uid, gid)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateGraft {
            parent,
            name,
            target_inode,
            mode,
            uid,
            gid,
        } => {
            let entry = slot.service().create_graft(
                inode_id(parent)?,
                dentry_name(name)?,
                inode_id(target_inode)?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveGraft { parent, name } => {
            let entry = slot
                .service()
                .remove_graft(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: entry.map(|entry| Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateDirPath {
            path,
            mode,
            uid,
            gid,
        } => {
            let entry = slot.service().create_dir_path(&path, mode, uid, gid)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateFile {
            parent,
            name,
            mode,
            uid,
            gid,
        } => {
            let entry = slot.service().create_file(
                inode_id(parent)?,
                dentry_name(name)?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateFilePrepared {
            parent,
            name,
            mode,
            uid,
            gid,
        } => {
            let created = slot.service().create_file_prepared(
                inode_id(parent)?,
                dentry_name(name)?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::CreatedPreparedArtifact {
                entry: Box::new(wire_dentry(&created.entry)),
                prepared: wire_prepared_artifact(slot.service().mount_id(), &created.prepared),
            })
        }
        MetadataRpcRequest::CreateSymlink {
            parent,
            name,
            target,
            mode,
            uid,
            gid,
        } => {
            let entry = slot.service().create_symlink(
                inode_id(parent)?,
                dentry_name(name)?,
                target,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateSpecialNode {
            parent,
            name,
            file_type,
            mode,
            rdev,
            uid,
            gid,
        } => {
            let file_type = decode_file_type(&file_type).map_err(protocol_error)?;
            let entry = slot.service().create_special_node(
                inode_id(parent)?,
                dentry_name(name)?,
                SpecialNodeSpec {
                    file_type,
                    mode,
                    rdev,
                    uid,
                    gid,
                },
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::UpdateAttrs {
            parent,
            name,
            changes,
        } => {
            let entry = slot.service().update_attrs(
                inode_id(parent)?,
                &dentry_name(name)?,
                update_attr(changes),
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::UpdateRootAttrs { changes } => {
            let attr = slot.service().update_root_attrs(update_attr(changes))?;
            Ok(MetadataRpcResult::InodeAttr {
                attr: Some(nokv_protocol::WireInodeAttr::from_inode_attr(&attr)),
            })
        }
        MetadataRpcRequest::SetXattr {
            inode,
            name_hex,
            value,
            mode,
        } => {
            let name = decode_xattr_name(&name_hex).map_err(protocol_error)?;
            slot.service()
                .set_xattr(inode_id(inode)?, &name, value, xattr_set_mode(mode))?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::GetXattr { inode, name_hex } => {
            let name = decode_xattr_name(&name_hex).map_err(protocol_error)?;
            let value = slot.service().get_xattr(inode_id(inode)?, &name)?;
            Ok(MetadataRpcResult::XattrValue { value })
        }
        MetadataRpcRequest::ListXattr { inode } => {
            let names = slot.service().list_xattr(inode_id(inode)?)?;
            Ok(MetadataRpcResult::XattrNames {
                names_hex: names.iter().map(|name| encode_xattr_name(name)).collect(),
            })
        }
        MetadataRpcRequest::RemoveXattr { inode, name_hex } => {
            let name = decode_xattr_name(&name_hex).map_err(protocol_error)?;
            slot.service().remove_xattr(inode_id(inode)?, &name)?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::GetAdvisoryLock {
            inode,
            owner,
            start,
            end,
            kind,
            pid,
        } => {
            let lock = slot.service().get_advisory_lock(AdvisoryLockRequest {
                inode: inode_id(inode)?,
                owner,
                start,
                end,
                kind: decode_advisory_lock_kind(&kind).map_err(protocol_error)?,
                pid,
                wait: false,
            })?;
            Ok(MetadataRpcResult::AdvisoryLock {
                lock: lock.as_ref().map(WireAdvisoryLock::from_advisory_lock),
            })
        }
        MetadataRpcRequest::SetAdvisoryLock {
            inode,
            owner,
            start,
            end,
            kind,
            pid,
            wait,
        } => {
            slot.service().set_advisory_lock(AdvisoryLockRequest {
                inode: inode_id(inode)?,
                owner,
                start,
                end,
                kind: decode_advisory_lock_kind(&kind).map_err(protocol_error)?,
                pid,
                wait,
            })?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        } => {
            let entry = slot.service().create_file_path(&path, mode, uid, gid)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::CreateFilesInDirPath {
            parent_path,
            names,
            mode,
            uid,
            gid,
        } => Ok(MetadataRpcResult::Batch {
            results: create_path_batch_envelopes(
                server,
                slot,
                CreatePathKind::File,
                &parent_path,
                names,
                mode,
                uid,
                gid,
            )?,
        }),
        MetadataRpcRequest::RemoveFile { parent, name } => {
            let entry = slot
                .service()
                .remove_file(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveFilePath { path } => {
            let entry = slot.service().remove_file_path(&path)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveEmptyDir { parent, name } => {
            let entry = slot
                .service()
                .remove_empty_dir(inode_id(parent)?, &dentry_name(name)?)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RemoveEmptyDirPath { path } => {
            let entry = slot.service().remove_empty_dir_path(&path)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::Link {
            inode,
            new_parent,
            new_name,
        } => {
            let entry = slot.service().link(
                inode_id(inode)?,
                inode_id(new_parent)?,
                dentry_name(new_name)?,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::Rename {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            let entry = slot.service().rename(
                inode_id(parent)?,
                &dentry_name(name)?,
                inode_id(new_parent)?,
                dentry_name(new_name)?,
            )?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RenamePath {
            source,
            destination,
        } => {
            let entry = slot.service().rename_path(&source, &destination)?;
            Ok(MetadataRpcResult::Dentry {
                entry: Some(Box::new(wire_dentry(&entry))),
            })
        }
        MetadataRpcRequest::RenameReplace {
            parent,
            name,
            new_parent,
            new_name,
        } => {
            let result = slot.service().rename_replace(
                inode_id(parent)?,
                &dentry_name(name)?,
                inode_id(new_parent)?,
                dentry_name(new_name)?,
            )?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::RenameReplacePath {
            source,
            destination,
        } => {
            let result = slot.service().rename_replace_path(&source, &destination)?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::SnapshotPin {
            root_path,
            snapshot_id,
        } => {
            let snapshot = slot.service().snapshot_pin_path(&root_path, snapshot_id)?;
            Ok(MetadataRpcResult::SnapshotPin {
                snapshot: snapshot
                    .as_ref()
                    .map(nokv_protocol::WireSnapshotPin::from_snapshot_pin),
                server_now_ms: slot.service().now_ms(),
            })
        }
        MetadataRpcRequest::SnapshotSubtreePath {
            root_path,
            lease_ms,
        } => {
            let snapshot = slot
                .service()
                .snapshot_subtree_path_with_lease(&root_path, lease_ms)?;
            Ok(MetadataRpcResult::Snapshot {
                snapshot: nokv_protocol::WireSnapshotPin::from_snapshot_pin(&snapshot),
            })
        }
        MetadataRpcRequest::CloneSubtreePath { src_path, dst_path } => {
            let handle = slot
                .service()
                .clone_subtree_path_into(&src_path, &dst_path)?;
            Ok(MetadataRpcResult::CloneSubtree {
                root: handle.root.get(),
                snapshot_id: handle.snapshot_id,
            })
        }
        MetadataRpcRequest::MetadataCapabilities { .. } => {
            Ok(MetadataRpcResult::MetadataCapabilities {
                capabilities: WireMetadataCapabilities {
                    mount_id: slot.service().mount_id().get(),
                    restore_to_fork_v1: true,
                },
            })
        }
        MetadataRpcRequest::RestoreSubtreePathToFork {
            source_path,
            snapshot_id,
            destination_path,
            initialization,
        } => {
            let outcome = slot
                .service()
                .with_immediate_sync_metadata_log_publication(|| {
                    slot.service().restore_subtree_path_to_fork_initialized(
                        &source_path,
                        snapshot_id,
                        &destination_path,
                        RestoreInitialization {
                            remove_relative_paths: initialization.remove_relative_paths,
                            files: initialization
                                .files
                                .into_iter()
                                .map(|file| RestoreInitializationFile {
                                    relative_path: file.relative_path,
                                    bytes: file.bytes,
                                    content_type: file.content_type,
                                    mode: file.mode,
                                    uid: file.uid,
                                    gid: file.gid,
                                })
                                .collect(),
                        },
                    )
                })?;
            Ok(MetadataRpcResult::Restore {
                outcome: WireRestoreOutcome {
                    operation_id: outcome.operation_id,
                    state: match outcome.state {
                        RestoreState::Complete => WireRestoreState::Complete,
                    },
                    source_root: outcome.source_root.get(),
                    destination_root: outcome.destination_root.get(),
                    snapshot_id: outcome.snapshot_id,
                    read_version: outcome.read_version,
                    cleanup_pending: outcome.cleanup_pending,
                },
            })
        }
        MetadataRpcRequest::DiffSubtrees { a_path, b_path } => {
            let deltas = slot.service().diff_subtrees_path(&a_path, &b_path)?;
            Ok(MetadataRpcResult::SubtreeDeltas {
                deltas: deltas.iter().map(wire_subtree_delta).collect(),
            })
        }
        MetadataRpcRequest::RollbackSubtreePath {
            target_path,
            snapshot_id,
        } => {
            slot.service()
                .rollback_subtree_path(&target_path, snapshot_id)?;
            Ok(MetadataRpcResult::Unit)
        }
        MetadataRpcRequest::StatPathAtSnapshot {
            root_path,
            snapshot_id,
            path,
        } => {
            let metadata = slot
                .service()
                .stat_path_at_snapshot(&root_path, snapshot_id, &path)?;
            Ok(MetadataRpcResult::PathMetadata {
                metadata: metadata.as_ref().map(WirePathMetadata::from_path_metadata),
            })
        }
        MetadataRpcRequest::ReadDirPlusPathAtSnapshot {
            root_path,
            snapshot_id,
            path,
        } => {
            let entries =
                slot.service()
                    .read_dir_plus_path_at_snapshot(&root_path, snapshot_id, &path)?;
            Ok(MetadataRpcResult::Dentries {
                entries: entries.iter().map(wire_dentry).collect(),
            })
        }
        MetadataRpcRequest::ReadDirPlusPathAtSnapshotPage {
            root_path,
            snapshot_id,
            path,
            after_name_hex,
            limit,
        } => {
            let after = after_name_hex
                .as_deref()
                .map(decode_name_cursor)
                .transpose()
                .map_err(protocol_error)?;
            let page = slot.service().read_dir_plus_path_at_snapshot_page(
                &root_path,
                snapshot_id,
                &path,
                after.as_ref(),
                limit,
            )?;
            Ok(MetadataRpcResult::DentriesPage {
                entries: page.entries.iter().map(wire_dentry).collect(),
                next_name_hex: page.next_cursor.as_ref().map(encode_name_cursor),
            })
        }
        MetadataRpcRequest::RetireSnapshot {
            root_path,
            snapshot_id,
        } => {
            let retired = slot
                .service()
                .retire_snapshot_path(&root_path, snapshot_id)?;
            Ok(MetadataRpcResult::RetiredSnapshot { retired })
        }
        MetadataRpcRequest::RenewSnapshot {
            root_path,
            snapshot_id,
            lease_ms,
        } => {
            let outcome = slot
                .service()
                .renew_snapshot_path(&root_path, snapshot_id, lease_ms)?;
            let outcome = match outcome {
                nokv_meta::SnapshotRenewOutcome::Renewed { pin, extended } => {
                    nokv_protocol::WireSnapshotRenewOutcome::Renewed {
                        pin: nokv_protocol::WireSnapshotPin::from_snapshot_pin(&pin),
                        extended,
                    }
                }
                nokv_meta::SnapshotRenewOutcome::Missing { snapshot_id } => {
                    nokv_protocol::WireSnapshotRenewOutcome::Missing { snapshot_id }
                }
            };
            Ok(MetadataRpcResult::RenewedSnapshot { outcome })
        }
        MetadataRpcRequest::OpenPathReadPlan {
            path,
            offset,
            len,
            expected_generation,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "path read length exceeds platform limit".to_owned(),
                ))
            })?;
            let open =
                slot.service()
                    .open_path_read_plan(&path, offset, len, expected_generation)?;
            let open = wire_open_path_read_plan(&open);
            Ok(MetadataRpcResult::OpenPathReadPlan {
                metadata: open.metadata,
                lease: open.lease,
                plan: open.plan,
            })
        }
        MetadataRpcRequest::OpenPathReadPlanBatch { requests } => {
            let requests = requests
                .into_iter()
                .map(open_path_read_plan_request)
                .collect::<Result<Vec<_>, _>>()?;
            let plans = slot.service().open_path_read_plan_batch(&requests)?;
            Ok(MetadataRpcResult::OpenPathReadPlanBatch {
                plans: plans.iter().map(wire_open_path_read_plan).collect(),
            })
        }
        MetadataRpcRequest::ReadBodyPlan {
            inode,
            generation,
            offset,
            len,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "body read length exceeds platform limit".to_owned(),
                ))
            })?;
            let plan = slot
                .service()
                .read_file_plan(inode_id(inode)?, generation, offset, len)?;
            Ok(MetadataRpcResult::BodyReadPlan {
                plan: wire_body_read_plan(&plan),
            })
        }
        MetadataRpcRequest::ReadArtifactPathAtSnapshot {
            root_path,
            snapshot_id,
            path,
        } => {
            let bytes =
                slot.service()
                    .read_artifact_path_at_snapshot(&root_path, snapshot_id, &path)?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadFilePathAtSnapshot {
            root_path,
            snapshot_id,
            path,
            offset,
            len,
        } => {
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "snapshot read length exceeds platform limit".to_owned(),
                ))
            })?;
            let bytes = slot.service().read_file_path_at_snapshot(
                &root_path,
                snapshot_id,
                &path,
                offset,
                len,
            )?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadFileAtSnapshot {
            root_path,
            snapshot_id,
            path_components,
            offset,
            len,
        } => {
            let path_components = dentry_components(path_components)?;
            let len = usize::try_from(len).map_err(|_| {
                ServerError::Metadata(MetadError::Codec(
                    "snapshot read length exceeds platform limit".to_owned(),
                ))
            })?;
            let bytes = slot.service().read_file_at_snapshot(
                &root_path,
                snapshot_id,
                &path_components,
                offset,
                len,
            )?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadSymlink { inode } => {
            let bytes = slot.service().read_symlink(inode_id(inode)?)?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::ReadSymlinkAtSnapshot {
            root_path,
            snapshot_id,
            path_components,
        } => {
            let path_components = dentry_components(path_components)?;
            let bytes = slot.service().read_symlink_at_snapshot(
                &root_path,
                snapshot_id,
                &path_components,
            )?;
            Ok(MetadataRpcResult::FileBytes { bytes })
        }
        MetadataRpcRequest::PrepareArtifact {
            parent,
            name,
            replace,
        } => {
            let name = dentry_name(name)?;
            let prepared = if replace {
                slot.service()
                    .prepare_artifact_replace(inode_id(parent)?, name)?
            } else {
                slot.service()
                    .prepare_artifact_create(inode_id(parent)?, name)?
            };
            Ok(MetadataRpcResult::PreparedArtifact {
                prepared: wire_prepared_artifact(slot.service().mount_id(), &prepared),
            })
        }
        MetadataRpcRequest::PrepareArtifactPath { path, replace } => {
            let prepared = if replace {
                slot.service().prepare_artifact_replace_path(&path)?
            } else {
                slot.service().prepare_artifact_create_path(&path)?
            };
            Ok(MetadataRpcResult::PreparedArtifact {
                prepared: wire_prepared_artifact(slot.service().mount_id(), &prepared),
            })
        }
        MetadataRpcRequest::RefreshPreparedArtifactObjectGcEpoch { prepared } => {
            if prepared.mount != slot.service().mount_id().get() {
                return Err(ServerError::Metadata(MetadError::Codec(
                    "prepared artifact mount does not match server mount".to_owned(),
                )));
            }
            let prepared = slot
                .service()
                .refresh_prepared_artifact_object_gc_epoch(prepared_artifact(prepared)?)?;
            Ok(MetadataRpcResult::PreparedArtifact {
                prepared: wire_prepared_artifact(slot.service().mount_id(), &prepared),
            })
        }
        MetadataRpcRequest::PublishPreparedArtifact {
            prepared,
            body,
            chunks,
            mode,
            uid,
            gid,
        } => {
            if prepared.mount != slot.service().mount_id().get() {
                return Err(ServerError::Metadata(MetadError::Codec(
                    "prepared artifact mount does not match server mount".to_owned(),
                )));
            }
            let result = slot.service().publish_prepared_artifact(
                prepared_artifact(prepared)?,
                (*body).into_body_descriptor(),
                chunks
                    .into_iter()
                    .map(|chunk| chunk.into_chunk_manifest().map_err(protocol_error))
                    .collect::<Result<Vec<_>, _>>()?,
                mode,
                uid,
                gid,
            )?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
        MetadataRpcRequest::PublishPreparedArtifactStagedSession {
            prepared,
            producer,
            digest_uri,
            content_type,
            manifest_id,
            size,
            chunks,
            staged,
            mode,
            uid,
            gid,
        } => {
            if prepared.mount != slot.service().mount_id().get() {
                return Err(ServerError::Metadata(MetadError::Codec(
                    "prepared artifact mount does not match server mount".to_owned(),
                )));
            }
            let prepared = prepared_artifact(prepared)?;
            let result = slot.service().publish_prepared_artifact_staged_session(
                prepared.clone(),
                PublishArtifactStagedSession {
                    parent: prepared.parent,
                    name: prepared.name,
                    producer,
                    digest_uri,
                    content_type,
                    manifest_id,
                    size,
                    chunks: chunks
                        .into_iter()
                        .map(|chunk| {
                            chunk
                                .into_chunk_manifest()
                                .map_err(|err| MetadError::Codec(err.to_string()))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    staged: staged_object_set(staged)?,
                    mode,
                    uid,
                    gid,
                },
            )?;
            Ok(MetadataRpcResult::RenameReplace {
                entry: Box::new(wire_dentry(&result.entry)),
                replaced: result
                    .replaced
                    .as_ref()
                    .map(|entry| Box::new(wire_dentry(entry))),
            })
        }
    }
}

fn open_path_read_plan_request(
    request: WireOpenPathReadPlanRequest,
) -> Result<OpenPathReadPlanRequest, ServerError> {
    let len = usize::try_from(request.len).map_err(|_| {
        ServerError::Metadata(MetadError::Codec(
            "path read length exceeds platform limit".to_owned(),
        ))
    })?;
    Ok(OpenPathReadPlanRequest {
        path: request.path,
        offset: request.offset,
        len,
        expected_generation: request.expected_generation,
    })
}

fn dentry_components(components: Vec<String>) -> Result<Vec<nokv_types::DentryName>, MetadError> {
    components.into_iter().map(dentry_name).collect()
}

fn refreshes_metadata_view(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => requests.iter().any(refreshes_metadata_view),
        MetadataRpcRequest::GetAttr { .. }
        | MetadataRpcRequest::GetAttrAtSnapshot { .. }
        | MetadataRpcRequest::LookupPlus { .. }
        | MetadataRpcRequest::CurrentDentryVersion { .. }
        | MetadataRpcRequest::LookupPlusAtSnapshot { .. }
        | MetadataRpcRequest::LookupPath { .. }
        | MetadataRpcRequest::StatPath { .. }
        | MetadataRpcRequest::ReadDirPlus { .. }
        | MetadataRpcRequest::ReadDirPlusPage { .. }
        | MetadataRpcRequest::ReadDirPlusAtSnapshot { .. }
        | MetadataRpcRequest::ReadDirPlusPath { .. }
        | MetadataRpcRequest::ReadDirPlusPathPage { .. }
        | MetadataRpcRequest::ReadIndexedPathPage { .. }
        | MetadataRpcRequest::StatCard { .. }
        | MetadataRpcRequest::ListPage { .. }
        | MetadataRpcRequest::FindPaths { .. }
        | MetadataRpcRequest::AggregatePaths { .. }
        | MetadataRpcRequest::GrepPaths { .. }
        | MetadataRpcRequest::ReadPage { .. }
        | MetadataRpcRequest::StatPathAtSnapshot { .. }
        | MetadataRpcRequest::ReadDirPlusPathAtSnapshot { .. }
        | MetadataRpcRequest::ReadDirPlusPathAtSnapshotPage { .. }
        | MetadataRpcRequest::ReadFileAtSnapshot { .. }
        | MetadataRpcRequest::ReadFilePathAtSnapshot { .. }
        | MetadataRpcRequest::ReadSymlink { .. }
        | MetadataRpcRequest::ReadSymlinkAtSnapshot { .. }
        | MetadataRpcRequest::GetXattr { .. }
        | MetadataRpcRequest::ListXattr { .. }
        | MetadataRpcRequest::OpenPathReadPlan { .. }
        | MetadataRpcRequest::OpenPathReadPlanBatch { .. }
        | MetadataRpcRequest::ReadBodyPlan { .. }
        | MetadataRpcRequest::ReadArtifactPathAtSnapshot { .. }
        | MetadataRpcRequest::DiffSubtrees { .. }
        | MetadataRpcRequest::SnapshotPin { .. } => true,
        MetadataRpcRequest::MetadataCapabilities { .. } => true,
        MetadataRpcRequest::BootstrapRoot { .. }
        | MetadataRpcRequest::CreateDir { .. }
        | MetadataRpcRequest::CreateGraft { .. }
        | MetadataRpcRequest::CreateDirPath { .. }
        | MetadataRpcRequest::CreateFile { .. }
        | MetadataRpcRequest::CreateFilePrepared { .. }
        | MetadataRpcRequest::CreateSymlink { .. }
        | MetadataRpcRequest::CreateSpecialNode { .. }
        | MetadataRpcRequest::UpdateAttrs { .. }
        | MetadataRpcRequest::UpdateRootAttrs { .. }
        | MetadataRpcRequest::SetXattr { .. }
        | MetadataRpcRequest::GetAdvisoryLock { .. }
        | MetadataRpcRequest::SetAdvisoryLock { .. }
        | MetadataRpcRequest::RemoveXattr { .. }
        | MetadataRpcRequest::CreateFilePath { .. }
        | MetadataRpcRequest::CreateFilesInDirPath { .. }
        | MetadataRpcRequest::RemoveGraft { .. }
        | MetadataRpcRequest::RemoveFile { .. }
        | MetadataRpcRequest::RemoveFilePath { .. }
        | MetadataRpcRequest::RemoveEmptyDir { .. }
        | MetadataRpcRequest::RemoveEmptyDirPath { .. }
        | MetadataRpcRequest::Link { .. }
        | MetadataRpcRequest::Rename { .. }
        | MetadataRpcRequest::RenamePath { .. }
        | MetadataRpcRequest::RenameReplace { .. }
        | MetadataRpcRequest::RenameReplacePath { .. }
        | MetadataRpcRequest::SnapshotSubtreePath { .. }
        | MetadataRpcRequest::CloneSubtreePath { .. }
        | MetadataRpcRequest::RestoreSubtreePathToFork { .. }
        | MetadataRpcRequest::RollbackSubtreePath { .. }
        | MetadataRpcRequest::RetireSnapshot { .. }
        | MetadataRpcRequest::RenewSnapshot { .. }
        | MetadataRpcRequest::PrepareArtifact { .. }
        | MetadataRpcRequest::PrepareArtifactPath { .. }
        | MetadataRpcRequest::RefreshPreparedArtifactObjectGcEpoch { .. }
        | MetadataRpcRequest::PublishPreparedArtifact { .. }
        | MetadataRpcRequest::RegisterNamespaceIndex { .. }
        | MetadataRpcRequest::PublishPreparedArtifactStagedSession { .. } => false,
    }
}

fn commits_metadata_view(request: &MetadataRpcRequest) -> bool {
    match request {
        MetadataRpcRequest::Batch { requests } => requests.iter().any(commits_metadata_view),
        request => !refreshes_metadata_view(request),
    }
}

#[cfg(test)]
mod tests;
