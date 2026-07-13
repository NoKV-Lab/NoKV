use nokv_meta::{CreateInDirPathBatch, DentryWithAttr, MetadError};
use nokv_protocol::{MetadataRpcEnvelope, MetadataRpcRequest, MetadataRpcResult};

use super::wire::{dentry_name, err_envelope, ok_envelope, wire_dentry};
use crate::server::{Server, ServerError, ShardSlot};

pub(super) fn execute_batch(
    server: &Server,
    requests: Vec<MetadataRpcRequest>,
) -> Result<MetadataRpcResult, ServerError> {
    // A batch coalesces into single-shard engine calls, so every sub-request must
    // route to the same shard. Resolve that single slot up front and reject
    // cross-shard batches (the client must split them per shard).
    let slot = resolve_batch_slot(server, &requests)?;
    execute_resolved_batch(server, slot, requests)
}

/// Opportunistically coalesce independently framed requests. A routing or
/// nested-batch mismatch is reported as `None` before execution so transport can
/// fall back to individual frames. Any error after resolution is returned and
/// must not trigger re-execution because some batch entries may have committed.
pub(super) fn try_execute_coalesced_batch(
    server: &Server,
    requests: Vec<MetadataRpcRequest>,
) -> Result<Option<Vec<MetadataRpcEnvelope>>, ServerError> {
    let slot = match resolve_batch_slot(server, &requests) {
        Ok(slot) => slot,
        Err(_) => return Ok(None),
    };
    match execute_resolved_batch(server, slot, requests)? {
        MetadataRpcResult::Batch { results } => Ok(Some(results)),
        _ => unreachable!("resolved batch executor always returns a batch result"),
    }
}

fn execute_resolved_batch(
    server: &Server,
    slot: &ShardSlot,
    requests: Vec<MetadataRpcRequest>,
) -> Result<MetadataRpcResult, ServerError> {
    let may_commit = requests.iter().any(super::commits_metadata_view);
    // Resolve selected one shard before taking the visibility gate. The whole
    // mixed/coalesced batch then holds that gate once; subrequests execute raw
    // to avoid lock re-entry, and the post barrier runs on both Ok and Err.
    server.execute_with_shard_visibility(slot, may_commit, || {
        execute_batch_on_slot(server, slot, requests)
    })
}

fn execute_batch_on_slot(
    server: &Server,
    slot: &ShardSlot,
    requests: Vec<MetadataRpcRequest>,
) -> Result<MetadataRpcResult, ServerError> {
    let mut results = Vec::with_capacity(requests.len());
    let mut iter = requests.into_iter().peekable();
    while let Some(request) = iter.next() {
        let Some(parts) = create_path_parts(&request) else {
            let Some(parts) = remove_path_parts(&request) else {
                results.push(execute_envelope(server, request));
                continue;
            };
            let mut group = RemovePathGroup::from_parts(parts);
            while let Some(next) = iter.peek() {
                let Some(next_parts) = remove_path_parts(next) else {
                    break;
                };
                if !group.can_absorb(&next_parts) {
                    break;
                }
                iter.next();
                group.names.push(next_parts.name);
            }
            results.extend(remove_path_batch_envelopes(
                server,
                slot,
                group.kind,
                &group.parent_path,
                group.names,
            ));
            continue;
        };

        let kind = parts.kind;
        let mut groups = Vec::new();
        let mut group = CreatePathGroup::from_parts(parts);
        while let Some(next) = iter.peek() {
            let Some(next_parts) = create_path_parts(next) else {
                break;
            };
            if next_parts.kind != kind {
                break;
            }
            iter.next();
            if group.can_absorb(&next_parts) {
                group.names.push(next_parts.name);
            } else {
                groups.push(group);
                group = CreatePathGroup::from_parts(next_parts);
            }
        }
        groups.push(group);
        if groups.len() == 1 {
            let group = groups.pop().expect("one create group");
            results.extend(create_path_batch_envelopes(
                server,
                slot,
                kind,
                &group.parent_path,
                group.names,
                group.mode,
                group.uid,
                group.gid,
            )?);
        } else {
            results.extend(create_path_group_envelopes(server, slot, kind, groups)?);
        }
    }
    Ok(MetadataRpcResult::Batch { results })
}

/// Resolve the single shard every sub-request must target, rejecting cross-shard
/// batches. Nested batches are not allowed either.
fn resolve_batch_slot<'a>(
    server: &'a Server,
    requests: &[MetadataRpcRequest],
) -> Result<&'a ShardSlot, ServerError> {
    let mut resolved: Option<&'a ShardSlot> = None;
    for request in requests {
        if matches!(request, MetadataRpcRequest::Batch { .. }) {
            return Err(ServerError::Metadata(MetadError::InvalidQuery(
                "nested batch is not supported".into(),
            )));
        }
        let slot = server.route(request)?;
        match resolved {
            None => resolved = Some(slot),
            Some(existing) if existing.shard_index() != slot.shard_index() => {
                return Err(ServerError::Metadata(MetadError::InvalidQuery(
                    "cross-shard batch".into(),
                )));
            }
            Some(_) => {}
        }
    }
    // An empty batch has no addressable shard; fall back to the default slot.
    resolved.map(Ok).unwrap_or_else(|| {
        server.route(&MetadataRpcRequest::BootstrapRoot {
            mode: 0,
            uid: 0,
            gid: 0,
        })
    })
}

fn execute_envelope(server: &Server, request: MetadataRpcRequest) -> MetadataRpcEnvelope {
    match super::execute_unfenced(server, request) {
        Ok(result) => ok_envelope(result),
        Err(err) => err_envelope(err),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CreatePathKind {
    Directory,
    File,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RemovePathKind {
    File,
    EmptyDir,
}

struct CreatePathParts {
    kind: CreatePathKind,
    parent_path: String,
    name: String,
    mode: u32,
    uid: u32,
    gid: u32,
}

struct CreatePathGroup {
    parent_path: String,
    names: Vec<String>,
    mode: u32,
    uid: u32,
    gid: u32,
}

impl CreatePathGroup {
    fn from_parts(parts: CreatePathParts) -> Self {
        Self {
            parent_path: parts.parent_path,
            names: vec![parts.name],
            mode: parts.mode,
            uid: parts.uid,
            gid: parts.gid,
        }
    }

    fn can_absorb(&self, parts: &CreatePathParts) -> bool {
        self.parent_path == parts.parent_path
            && self.mode == parts.mode
            && self.uid == parts.uid
            && self.gid == parts.gid
    }
}

struct RemovePathParts {
    kind: RemovePathKind,
    parent_path: String,
    name: String,
}

struct RemovePathGroup {
    kind: RemovePathKind,
    parent_path: String,
    names: Vec<String>,
}

impl RemovePathGroup {
    fn from_parts(parts: RemovePathParts) -> Self {
        Self {
            kind: parts.kind,
            parent_path: parts.parent_path,
            names: vec![parts.name],
        }
    }

    fn can_absorb(&self, parts: &RemovePathParts) -> bool {
        self.kind == parts.kind
            && self.parent_path == parts.parent_path
            && !self.names.contains(&parts.name)
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn create_path_batch_envelopes(
    server: &Server,
    slot: &ShardSlot,
    kind: CreatePathKind,
    parent_path: &str,
    names: Vec<String>,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<Vec<MetadataRpcEnvelope>, ServerError> {
    let parsed = names
        .iter()
        .map(|name| dentry_name(name.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ServerError::Metadata);
    let coalesced = parsed.and_then(|names| {
        match kind {
            CreatePathKind::Directory => {
                slot.service()
                    .create_dirs_in_dir_path(parent_path, names, mode, uid, gid)
            }
            CreatePathKind::File => {
                slot.service()
                    .create_files_in_dir_path(parent_path, names, mode, uid, gid)
            }
        }
        .map_err(ServerError::Metadata)
    });
    match coalesced {
        Ok(entries) => Ok(entries
            .iter()
            .map(|entry| {
                ok_envelope(MetadataRpcResult::Dentry {
                    entry: Some(Box::new(wire_dentry(entry))),
                })
            })
            .collect()),
        Err(_) => Ok(names
            .into_iter()
            .map(|name| {
                execute_envelope(
                    server,
                    create_path_request(kind, parent_path, &name, mode, uid, gid),
                )
            })
            .collect()),
    }
}

fn remove_path_batch_envelopes(
    server: &Server,
    slot: &ShardSlot,
    kind: RemovePathKind,
    parent_path: &str,
    names: Vec<String>,
) -> Vec<MetadataRpcEnvelope> {
    let parsed = names
        .iter()
        .map(|name| dentry_name(name.clone()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(ServerError::Metadata);
    let committed = parsed.and_then(|names| {
        match kind {
            RemovePathKind::File => slot.service().remove_files_in_dir_path(parent_path, names),
            RemovePathKind::EmptyDir => slot
                .service()
                .remove_empty_dirs_in_dir_path(parent_path, names),
        }
        .map_err(ServerError::Metadata)
    });
    match committed {
        Ok(results) => results
            .into_iter()
            .map(|result| match result {
                Ok(entry) => ok_envelope(MetadataRpcResult::Dentry {
                    entry: Some(Box::new(wire_dentry(&entry))),
                }),
                Err(err) => err_envelope(ServerError::Metadata(err)),
            })
            .collect(),
        Err(_) => names
            .into_iter()
            .map(|name| execute_envelope(server, remove_path_request(kind, parent_path, &name)))
            .collect(),
    }
}

fn create_path_group_envelopes(
    server: &Server,
    slot: &ShardSlot,
    kind: CreatePathKind,
    groups: Vec<CreatePathGroup>,
) -> Result<Vec<MetadataRpcEnvelope>, ServerError> {
    let parsed = groups
        .iter()
        .map(|group| {
            let names = group
                .names
                .iter()
                .map(|name| dentry_name(name.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ServerError::Metadata)?;
            Ok(CreateInDirPathBatch {
                parent_path: group.parent_path.clone(),
                names,
                mode: group.mode,
                uid: group.uid,
                gid: group.gid,
            })
        })
        .collect::<Result<Vec<_>, ServerError>>();

    let committed = parsed.map(|batches| {
        let results: Vec<Result<Vec<DentryWithAttr>, MetadError>> = match kind {
            CreatePathKind::Directory => slot.service().create_dir_batches_in_dir_path(batches),
            CreatePathKind::File => slot.service().create_file_batches_in_dir_path(batches),
        };
        results
    });

    let mut out = Vec::new();
    match committed {
        Ok(group_results) => {
            for (group, result) in groups.into_iter().zip(group_results) {
                match result {
                    Ok(entries) => out.extend(entries.iter().map(|entry| {
                        ok_envelope(MetadataRpcResult::Dentry {
                            entry: Some(Box::new(wire_dentry(entry))),
                        })
                    })),
                    Err(_err) => {
                        out.extend(create_path_batch_envelopes(
                            server,
                            slot,
                            kind,
                            &group.parent_path,
                            group.names,
                            group.mode,
                            group.uid,
                            group.gid,
                        )?);
                    }
                }
            }
        }
        Err(_) => {
            for group in groups {
                out.extend(create_path_batch_envelopes(
                    server,
                    slot,
                    kind,
                    &group.parent_path,
                    group.names,
                    group.mode,
                    group.uid,
                    group.gid,
                )?);
            }
        }
    }
    Ok(out)
}

fn remove_path_parts(request: &MetadataRpcRequest) -> Option<RemovePathParts> {
    match request {
        MetadataRpcRequest::RemoveFilePath { path } => {
            let (parent_path, name) = split_parent_path(path)?;
            Some(RemovePathParts {
                kind: RemovePathKind::File,
                parent_path,
                name,
            })
        }
        MetadataRpcRequest::RemoveEmptyDirPath { path } => {
            let (parent_path, name) = split_parent_path(path)?;
            Some(RemovePathParts {
                kind: RemovePathKind::EmptyDir,
                parent_path,
                name,
            })
        }
        _ => None,
    }
}

fn create_path_parts(request: &MetadataRpcRequest) -> Option<CreatePathParts> {
    match request {
        MetadataRpcRequest::CreateDirPath {
            path,
            mode,
            uid,
            gid,
        } => create_path_parts_from_path(CreatePathKind::Directory, path, *mode, *uid, *gid),
        MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        } => create_path_parts_from_path(CreatePathKind::File, path, *mode, *uid, *gid),
        _ => None,
    }
}

fn create_path_parts_from_path(
    kind: CreatePathKind,
    path: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Option<CreatePathParts> {
    let (parent_path, name) = split_parent_path(path)?;
    Some(CreatePathParts {
        kind,
        parent_path,
        name,
        mode,
        uid,
        gid,
    })
}

fn create_path_request(
    kind: CreatePathKind,
    parent_path: &str,
    name: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> MetadataRpcRequest {
    let path = child_path(parent_path, name);
    match kind {
        CreatePathKind::Directory => MetadataRpcRequest::CreateDirPath {
            path,
            mode,
            uid,
            gid,
        },
        CreatePathKind::File => MetadataRpcRequest::CreateFilePath {
            path,
            mode,
            uid,
            gid,
        },
    }
}

fn remove_path_request(kind: RemovePathKind, parent_path: &str, name: &str) -> MetadataRpcRequest {
    let path = child_path(parent_path, name);
    match kind {
        RemovePathKind::File => MetadataRpcRequest::RemoveFilePath { path },
        RemovePathKind::EmptyDir => MetadataRpcRequest::RemoveEmptyDirPath { path },
    }
}

fn split_parent_path(path: &str) -> Option<(String, String)> {
    if !path.starts_with('/') || path == "/" {
        return None;
    }
    let slash = path.rfind('/')?;
    let name = path.get(slash + 1..)?;
    if name.is_empty() {
        return None;
    }
    let parent = if slash == 0 { "/" } else { &path[..slash] };
    Some((parent.to_owned(), name.to_owned()))
}

fn child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{name}")
    } else {
        format!("{parent}/{name}")
    }
}
