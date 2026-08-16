/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Production Workbench facade backed by the typed workspace SDK.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use nokv_agent as agent;
use nokv_client::{
    ArtifactAppendOptions, ArtifactPublishOptions, ClientError, CommitRecoveryRequest,
    CommitWorkflowError, CommitWorkflowIdentities, CommitWorkflowOptions, CommitWorkflowRequest,
    RestoreRecoveryRequest, RestoreWorkflowError, RestoreWorkflowIdentities,
    RestoreWorkflowOptions, RestoreWorkflowRequest, SnapshotMintOptions, SnapshotRenewOptions,
    SnapshotRetireOptions,
};
use nokv_protocol as wire;
use nokv_types::{NormalizedRelativePath, WorkbenchId};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};

use super::connection::CliWorkspaceClient;
use super::object_store::CliObjectStore;
use crate::encode_lowercase_hex;

const RUN_MANIFEST_PATH: &str = "metadata/run_manifest.json";
const RESTORE_MANIFEST_PATH: &str = "metadata/restore_manifest.json";
const JSON_CONTENT_TYPE: &str = "application/json";
const CURSOR_VERSION: &[u8] = b"nokv.workspace.cursor\0";
const GREP_CURSOR_VERSION: &[u8] = b"nokv.workspace.grep-cursor.v2\0";
const LIST_CURSOR_VERSION: &[u8] = b"nokv.workspace.list-cursor.v3\0";
const QUERY_SERVER_PAGE_LIMIT: u32 = wire::MAX_QUERY_PAGE_LIMIT;
const CONSISTENT_READ_MAX_ATTEMPTS: u32 = 3;

static ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Concrete Workbench backend used by the custom CLI and MCP adapter.
#[derive(Clone)]
pub struct CliWorkbenchBackend {
    client: CliWorkspaceClient,
    objects: Arc<CliObjectStore>,
    max_artifact_bytes: usize,
}

struct WorkspaceAdmission {
    workspace: wire::WorkspaceSummary,
    created: bool,
}

impl CliWorkbenchBackend {
    pub fn new(
        client: CliWorkspaceClient,
        objects: Arc<CliObjectStore>,
        max_artifact_bytes: usize,
    ) -> Self {
        Self {
            client,
            objects,
            max_artifact_bytes,
        }
    }

    fn workspace(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<wire::WorkspaceSummary, agent::BackendError> {
        self.client
            .get_workspace(wire::GetWorkspaceRequest {
                workbench: workbench_name(workbench_id)?,
            })
            .map(|call| call.value)
            .map_err(map_client_error)
    }

    fn optional_workspace(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Option<wire::WorkspaceSummary>, agent::BackendError> {
        match self.client.get_workspace(wire::GetWorkspaceRequest {
            workbench: workbench_name(workbench_id)?,
        }) {
            Ok(call) => Ok(Some(call.value)),
            Err(error) if rpc_code(&error) == Some(wire::ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(map_client_error(error)),
        }
    }

    fn create_or_observe_workbench(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<WorkspaceAdmission, agent::BackendError> {
        if let Some(workspace) = self.optional_workspace(workbench_id)? {
            return Ok(WorkspaceAdmission {
                workspace,
                created: false,
            });
        }
        let incarnation = wire::WorkspaceIdentity(self.fresh_fixed_identity(
            b"nokv.cli.workspace-incarnation\0",
            &[workbench_id.as_bytes()],
        ));
        let created = self.client.create_workspace(
            self.client.new_request_id(),
            wire::CreateWorkspaceRequest {
                workbench: workbench_name(workbench_id)?,
                workspace_incarnation_id: incarnation,
            },
        );
        let replayed = match created {
            Ok(call) => call.replayed,
            Err(error) if rpc_code(&error) == Some(wire::ErrorCode::AlreadyExists) => {
                return Ok(WorkspaceAdmission {
                    workspace: self.workspace(workbench_id)?,
                    created: false,
                });
            }
            Err(error) => return Err(map_client_error(error)),
        };
        let workspace = self.workspace(workbench_id)?;
        if workspace.workspace_incarnation_id != incarnation {
            return Err(protocol_mismatch(
                "created workbench resolved to a different incarnation",
            ));
        }
        Ok(WorkspaceAdmission {
            workspace,
            created: !replayed,
        })
    }

    fn resolved_view(
        &self,
        _workbench_id: &WorkbenchId,
        view: &agent::ReadView,
    ) -> Result<wire::WorkspaceReadView, agent::BackendError> {
        match view {
            agent::ReadView::Live => Ok(wire::WorkspaceReadView::Live),
            agent::ReadView::Snapshot(selector) => Ok(wire::WorkspaceReadView::Snapshot(
                snapshot_selector(selector)?,
            )),
        }
    }

    fn path_metadata(
        &self,
        path: &agent::ScopedPath,
        view: wire::WorkspaceReadView,
    ) -> Result<Option<wire::PathMetadata>, agent::BackendError> {
        let target = workspace_path(path)?;
        match self.client.get_path(wire::GetPathRequest {
            target: target.clone(),
            view,
            range: None,
            plan_page: None,
            if_none_match: None,
        }) {
            Ok(call) => {
                let metadata = call
                    .value
                    .metadata
                    .ok_or_else(|| protocol_mismatch("path stat omitted metadata"))?;
                if metadata.path != target {
                    return Err(protocol_mismatch(
                        "get_path returned metadata for a different path",
                    ));
                }
                Ok(Some(metadata))
            }
            Err(error) if rpc_code(&error) == Some(wire::ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(map_client_error(error)),
        }
    }

    fn publish_artifact(
        &self,
        options: ArtifactPublishOptions,
        bytes: &[u8],
    ) -> Result<nokv_client::ArtifactPublishOutcome, agent::BackendError> {
        self.ensure_artifact_size(bytes)?;
        self.client
            .publish_artifact(self.objects.as_ref(), options, bytes)
            .map_err(map_client_error)
    }

    fn ensure_artifact_size(&self, bytes: &[u8]) -> Result<(), agent::BackendError> {
        if bytes.len() > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "artifact is {} bytes, maximum is {}",
                bytes.len(),
                self.max_artifact_bytes
            )));
        }
        Ok(())
    }

    fn fresh_fixed_identity(&self, domain: &[u8], pieces: &[&[u8]]) -> [u8; 16] {
        let sequence = ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut hasher = Sha256::new();
        hasher.update(domain);
        hasher.update(self.client.root_id().0);
        hasher.update(std::process::id().to_be_bytes());
        hasher.update(sequence.to_be_bytes());
        hasher.update(now.to_be_bytes());
        for piece in pieces {
            hash_len64(&mut hasher, piece);
        }
        digest_prefix(hasher.finalize().into())
    }

    fn snapshot(
        &self,
        workbench_id: &WorkbenchId,
        selector: &agent::SnapshotSelector,
    ) -> Result<agent::SnapshotRecord, agent::BackendError> {
        self.client
            .get_snapshot(wire::GetSnapshotRequest {
                workbench: workbench_name(workbench_id)?,
                selector: snapshot_selector(selector)?,
            })
            .map_err(map_snapshot_client_error)
            .and_then(|call| snapshot_record(call.value))
    }
}

impl agent::WorkbenchBackend for CliWorkbenchBackend {
    fn create_workbench(&self, workbench_id: &WorkbenchId) -> Result<bool, agent::BackendError> {
        self.create_or_observe_workbench(workbench_id)
            .map(|admission| admission.created)
    }

    fn stat(
        &self,
        path: &agent::ScopedPath,
        view: &agent::ReadView,
    ) -> Result<Option<agent::StatRecord>, agent::BackendError> {
        if path.section.is_none() && path.relative_path.is_none() {
            return Ok(self
                .optional_workspace(&path.workbench_id)?
                .map(|_| agent::StatRecord {
                    path: path.clone(),
                    kind: agent::ArtifactKind::Workbench,
                    artifact: None,
                }));
        }
        let resolved_view = self.resolved_view(&path.workbench_id, view)?;
        if path.section.is_some() && path.relative_path.is_none() {
            self.workspace(&path.workbench_id)?;
            return Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Section,
                artifact: None,
            }));
        }
        if let Some(metadata) = self.path_metadata(path, resolved_view.clone())? {
            return Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Artifact,
                artifact: Some(artifact_metadata(&metadata)),
            }));
        }
        let prefix_path = full_path(path)?;
        let prefix = relative_path(&prefix_path)?;
        let page = self
            .client
            .list_paths(wire::ListPathsRequest {
                workbench: workbench_name(&path.workbench_id)?,
                prefix: Some(prefix),
                recursive: false,
                view: resolved_view,
                expected_read_version: None,
                page: wire::PageRequest {
                    cursor: None,
                    limit: 1,
                },
            })
            .map_err(map_client_error)?;
        let mut entries = page.value.entries.into_iter();
        let Some(entry) = entries.next() else {
            return Ok(None);
        };
        if entries.next().is_some() {
            return Err(protocol_mismatch(
                "list_paths returned more rows than the requested stat probe limit",
            ));
        }
        validate_response_workbench(entry.path(), &path.workbench_id)?;
        let source = NormalizedRelativePath::new(entry.path().path.as_str().to_owned())
            .map_err(domain_input)?;
        match (direct_list_child(Some(&prefix_path), source)?, entry) {
            (Some(_), _) => Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Directory,
                artifact: None,
            })),
            (None, wire::PathListEntry::Artifact(metadata)) => Ok(Some(agent::StatRecord {
                path: path.clone(),
                kind: agent::ArtifactKind::Artifact,
                artifact: Some(artifact_metadata(&metadata)),
            })),
            (None, wire::PathListEntry::Prefix(_)) => Err(protocol_mismatch(
                "list_paths returned an exact implicit prefix",
            )),
        }
    }

    fn list(&self, request: agent::ListRequest) -> Result<agent::ListPage, agent::BackendError> {
        let limit = request
            .limit
            .max(1)
            .min(wire::PageRequest::MAX_LIMIT as usize);
        let view = self.resolved_view(&request.path.workbench_id, &request.view)?;
        let prefix = full_path_optional(&request.path)?;
        let scope_digest =
            list_scope_digest(&request.path.workbench_id, prefix.as_ref(), &request.view);
        let supplied_cursor = request.cursor.is_some();
        let decoded_cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_list_cursor(cursor, scope_digest))
            .transpose()?;
        let after = decoded_cursor.as_ref().map(|cursor| &cursor.anchor);
        let max_attempts = if supplied_cursor {
            1
        } else {
            CONSISTENT_READ_MAX_ATTEMPTS
        };

        'attempt: for attempt in 1..=max_attempts {
            let mut candidates = BTreeMap::<NormalizedRelativePath, agent::ListEntry>::new();
            if prefix.is_none() {
                for section in agent::WORKBENCH_SECTIONS {
                    let path = NormalizedRelativePath::new(section.as_str().to_owned())
                        .map_err(domain_input)?;
                    if after.is_none_or(|after| path > *after) {
                        candidates.insert(
                            path,
                            agent::ListEntry {
                                path: agent::ScopedPath {
                                    workbench_id: request.path.workbench_id.clone(),
                                    section: Some(section),
                                    relative_path: None,
                                },
                                kind: agent::ArtifactKind::Section,
                                artifact: None,
                            },
                        );
                    }
                }
            }

            let mut read_version = decoded_cursor.as_ref().map(|cursor| cursor.read_version);
            let mut scan_cursor = after.map(|path| path.as_str().as_bytes().to_vec());
            let mut seen_server_cursors = BTreeSet::new();
            let mut first_server_page = true;
            if let Some(cursor) = scan_cursor.clone() {
                seen_server_cursors.insert(cursor);
            }
            loop {
                let requested_cursor = scan_cursor.clone();
                let requested_limit =
                    list_server_page_limit(limit, candidates.len(), first_server_page);
                let call = match self.client.list_paths(wire::ListPathsRequest {
                    workbench: workbench_name(&request.path.workbench_id)?,
                    prefix: prefix.as_ref().map(relative_path).transpose()?,
                    recursive: false,
                    view: view.clone(),
                    expected_read_version: read_version,
                    page: wire::PageRequest {
                        cursor: requested_cursor.clone(),
                        limit: requested_limit,
                    },
                }) {
                    Ok(call) => call,
                    Err(error)
                        if !supplied_cursor
                            && attempt < max_attempts
                            && is_read_version_conflict(&error) =>
                    {
                        continue 'attempt;
                    }
                    Err(error) => return Err(map_client_error(error)),
                };
                first_server_page = false;
                if read_version.is_some_and(|expected| expected != call.value.read_version) {
                    return Err(protocol_mismatch(
                        "list_paths returned a page outside the requested read-version fence",
                    ));
                }
                read_version.get_or_insert(call.value.read_version);
                if call.value.entries.len()
                    > usize::try_from(requested_limit)
                        .expect("protocol page limit always fits usize")
                {
                    return Err(protocol_mismatch(
                        "list_paths returned more rows than the requested page limit",
                    ));
                }
                let next_server_cursor = call.value.next_cursor;
                if next_server_cursor.is_some() && call.value.entries.is_empty() {
                    return Err(protocol_mismatch(
                        "list_paths returned an empty non-terminal page",
                    ));
                }
                if let Some(next) = next_server_cursor.as_ref() {
                    if !seen_server_cursors.insert(next.clone()) {
                        return Err(protocol_mismatch("list_paths returned a repeated cursor"));
                    }
                }
                let exhausted = next_server_cursor.is_none();
                let mut last_authoritative_path = None;
                for listing in call.value.entries {
                    let (source_path, metadata) = match listing {
                        wire::PathListEntry::Artifact(metadata) => {
                            validate_response_workbench(
                                &metadata.path,
                                &request.path.workbench_id,
                            )?;
                            (metadata.path.path.as_str().to_owned(), Some(metadata))
                        }
                        wire::PathListEntry::Prefix(path) => {
                            validate_response_workbench(&path, &request.path.workbench_id)?;
                            (path.path.as_str().to_owned(), None)
                        }
                    };
                    let source = NormalizedRelativePath::new(source_path).map_err(domain_input)?;
                    last_authoritative_path = Some(source.clone());
                    let Some(child) = direct_list_child(prefix.as_ref(), source)? else {
                        continue;
                    };
                    if after.is_some_and(|after| child <= *after) {
                        continue;
                    }
                    merge_list_candidate(
                        &mut candidates,
                        child,
                        metadata,
                        &request.path.workbench_id,
                        prefix.is_none(),
                    )?;
                }
                let covered_union_cutoff = candidates.len() > limit
                    && last_authoritative_path.as_ref().is_some_and(|last| {
                        candidates
                            .keys()
                            .nth(limit)
                            .is_some_and(|cutoff| last >= cutoff)
                    });
                if exhausted || covered_union_cutoff {
                    break;
                }
                scan_cursor = next_server_cursor;
            }

            let read_version = read_version.expect("every list attempt performs one RPC");
            let has_more = candidates.len() > limit;
            let mut ordered = candidates.into_iter().collect::<Vec<_>>();
            ordered.truncate(limit);
            let next_cursor = has_more
                .then(|| {
                    ordered
                        .last()
                        .map(|(path, _)| encode_list_cursor(read_version, scope_digest, path))
                })
                .flatten();
            return Ok(agent::ListPage {
                entries: ordered.into_iter().map(|(_, entry)| entry).collect(),
                next_cursor,
                read_version,
            });
        }

        unreachable!("consistent read attempt count is non-zero")
    }

    fn read(
        &self,
        request: agent::ReadRequest,
    ) -> Result<Option<agent::ArtifactBody>, agent::BackendError> {
        let view = self.resolved_view(&request.path.workbench_id, &request.view)?;
        let Some(metadata) = self.path_metadata(&request.path, view.clone())? else {
            return Ok(None);
        };
        let size = usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX);
        if size > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "artifact is {size} bytes, maximum is {}",
                self.max_artifact_bytes
            )));
        }
        let outcome = self
            .client
            .read_artifact(
                self.objects.as_ref(),
                None,
                workspace_path(&request.path)?,
                view,
            )
            .map_err(map_client_error)?;
        Ok(Some(agent::ArtifactBody {
            path: request.path,
            metadata: artifact_metadata(&outcome.metadata),
            bytes: outcome.bytes,
        }))
    }

    fn publish(
        &self,
        request: agent::PublishRequest,
    ) -> Result<agent::PublishOutcome, agent::BackendError> {
        self.ensure_artifact_size(&request.body)?;
        let full_path = full_path(&request.path)?;
        let body_digest = Sha256::digest(&request.body);
        let condition = match request.condition {
            agent::PublishCondition::CreateOnly => wire::PublishCondition::CreateOnly,
            agent::PublishCondition::ReplaceOnly {
                expected_generation,
            } => wire::PublishCondition::ReplaceOnly {
                expected_generation,
            },
        };
        let condition_bytes = match condition {
            wire::PublishCondition::CreateOnly => 0_u64.to_be_bytes(),
            wire::PublishCondition::ReplaceOnly {
                expected_generation,
            } => expected_generation.to_be_bytes(),
            wire::PublishCondition::Append { .. } => unreachable!("facade publish has no append"),
        };
        let content_type =
            wire::ContentType::new(request.content_type.clone()).map_err(protocol_input)?;
        let workspace = self
            .create_or_observe_workbench(&request.path.workbench_id)?
            .workspace;
        let operation_id = wire::OperationIdentity(self.fresh_fixed_identity(
            b"nokv.cli.artifact-operation\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                &condition_bytes,
                body_digest.as_slice(),
            ],
        ));
        let revision_id = wire::ArtifactRevisionIdentity(self.fresh_fixed_identity(
            b"nokv.cli.artifact-revision\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                body_digest.as_slice(),
            ],
        ));
        let created = matches!(condition, wire::PublishCondition::CreateOnly);
        let options = ArtifactPublishOptions::new(
            operation_id,
            revision_id,
            workspace_path(&request.path)?,
            condition,
            content_type,
        );
        let outcome = self.publish_artifact(options, &request.body)?;
        let publication = outcome.publication.value;
        Ok(agent::PublishOutcome {
            metadata: agent::ArtifactMetadata {
                generation: publication.generation,
                size_bytes: publication.logical_size,
                digest_uri: publication.body_digest.as_str().to_owned(),
                content_type: request.content_type,
                producer: None,
                manifest_id: None,
                indexed_fields: BTreeMap::new(),
            },
            created,
        })
    }

    fn append(
        &self,
        request: agent::AppendRequest,
    ) -> Result<agent::AppendOutcome, agent::BackendError> {
        if request.delta.len() > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "append delta is {} bytes, maximum is {}",
                request.delta.len(),
                self.max_artifact_bytes
            )));
        }
        let full_path = full_path(&request.path)?;
        let create_content_type =
            wire::ContentType::new(request.create_content_type).map_err(protocol_input)?;
        let content_type = request
            .content_type
            .map(wire::ContentType::new)
            .transpose()
            .map_err(protocol_input)?;
        let workspace = self
            .create_or_observe_workbench(&request.path.workbench_id)?
            .workspace;
        let delta_digest: [u8; 32] = Sha256::digest(&request.delta).into();
        let content_type_identity = content_type
            .as_ref()
            .map(wire::ContentType::as_str)
            .unwrap_or("")
            .as_bytes();
        let operation_id = wire::OperationIdentity(self.fresh_fixed_identity(
            b"nokv.cli.append-operation\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                &delta_digest,
                content_type_identity,
            ],
        ));
        let revision_id = wire::ArtifactRevisionIdentity(self.fresh_fixed_identity(
            b"nokv.cli.append-revision\0",
            &[
                &workspace.workspace_incarnation_id.0,
                full_path.as_str().as_bytes(),
                &delta_digest,
            ],
        ));
        let mut options = ArtifactAppendOptions::new(
            operation_id,
            revision_id,
            workspace_path(&request.path)?,
            create_content_type,
        );
        if let Some(content_type) = content_type {
            options = options.with_content_type(content_type);
        }
        if let Some(max_logical_size) = request.max_logical_size {
            options = options.with_max_logical_size(max_logical_size);
        }

        let outcome = self
            .client
            .append_artifact(self.objects.as_ref(), options, &request.delta)
            .map_err(map_client_error)?;
        let publication = outcome.publication.value;
        let descriptor = outcome.descriptor;
        Ok(agent::AppendOutcome {
            metadata: agent::ArtifactMetadata {
                generation: publication.generation,
                size_bytes: publication.logical_size,
                digest_uri: publication.body_digest.as_str().to_owned(),
                content_type: descriptor.content_type.as_str().to_owned(),
                producer: descriptor.producer,
                manifest_id: descriptor.manifest_identity,
                indexed_fields: descriptor
                    .index_fields
                    .iter()
                    .map(|field| (field.field_id.clone(), scalar_value(&field.value)))
                    .collect(),
            },
            created: outcome.created,
        })
    }

    fn grep_candidates(
        &self,
        request: agent::GrepCandidateRequest,
    ) -> Result<agent::GrepCandidatePage, agent::BackendError> {
        let prefix = full_path_optional(&request.scope)?;
        let scope_digest = grep_scope_digest(
            &request.scope.workbench_id,
            prefix.as_ref(),
            request.recursive,
        );
        let cursor = request
            .cursor
            .as_deref()
            .map(|cursor| decode_grep_cursor(cursor, scope_digest))
            .transpose()?;
        let expected_read_version = cursor.as_ref().map(|cursor| cursor.read_version);
        let call = self
            .client
            .list_paths(wire::ListPathsRequest {
                workbench: workbench_name(&request.scope.workbench_id)?,
                prefix: prefix.as_ref().map(relative_path).transpose()?,
                recursive: request.recursive,
                view: wire::WorkspaceReadView::Live,
                expected_read_version,
                page: wire::PageRequest {
                    cursor: cursor.map(|cursor| cursor.server_cursor),
                    limit: page_limit(request.limit)?,
                },
            })
            .map_err(map_client_error)?;
        if expected_read_version.is_some_and(|expected| expected != call.value.read_version) {
            return Err(protocol_mismatch(
                "grep candidate page escaped its requested read-version fence",
            ));
        }
        let read_version = call.value.read_version;
        let mut candidates = Vec::new();
        for entry in call.value.entries {
            validate_response_workbench(entry.path(), &request.scope.workbench_id)?;
            let metadata = match entry {
                wire::PathListEntry::Artifact(metadata) => metadata,
                wire::PathListEntry::Prefix(_) if !request.recursive => continue,
                wire::PathListEntry::Prefix(_) => {
                    return Err(protocol_mismatch(
                        "recursive list_paths returned an implicit prefix",
                    ));
                }
            };
            let cursor_after = encode_grep_cursor(
                read_version,
                scope_digest,
                metadata.path.path.as_str().as_bytes(),
            );
            candidates.push(agent::GrepCandidate {
                path: scoped_path(&metadata.path)?,
                cursor_after,
            });
        }
        Ok(agent::GrepCandidatePage {
            candidates,
            next_cursor: call
                .value
                .next_cursor
                .as_deref()
                .map(|cursor| encode_grep_cursor(read_version, scope_digest, cursor)),
        })
    }

    fn search(
        &self,
        request: agent::SearchRequest,
    ) -> Result<agent::SearchPage, agent::BackendError> {
        let call = self
            .client
            .search(wire::SearchRequest {
                scope: query_scope(&request.scope)?,
                predicates: request
                    .predicates
                    .iter()
                    .map(query_predicate)
                    .collect::<Result<Vec<_>, _>>()?,
                projection: request.fields,
                sort: query_sort(&request.sort),
                facets: request.facets,
                page: wire::PageRequest {
                    cursor: request
                        .cursor
                        .as_deref()
                        .map(|cursor| decode_cursor("search", cursor))
                        .transpose()?,
                    limit: query_page_limit(request.limit)?,
                },
            })
            .map_err(map_client_error)?;
        Ok(agent::SearchPage {
            hits: call
                .value
                .hits
                .into_iter()
                .map(search_hit)
                .collect::<Result<Vec<_>, _>>()?,
            facets: call
                .value
                .facets
                .into_iter()
                .map(facet_result)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: call
                .value
                .next_cursor
                .as_deref()
                .map(|cursor| encode_cursor("search", cursor)),
            read_version: call.value.read_version,
        })
    }

    fn aggregate(
        &self,
        request: agent::AggregateRequest,
    ) -> Result<agent::AggregatePage, agent::BackendError> {
        let call = self
            .client
            .aggregate(wire::AggregateRequest {
                scope: query_scope(&request.scope)?,
                predicates: request
                    .predicates
                    .iter()
                    .map(query_predicate)
                    .collect::<Result<Vec<_>, _>>()?,
                group_by: request.group_by,
                aggregates: request
                    .measures
                    .into_iter()
                    .map(|measure| wire::AggregateSpec {
                        function: match measure.operator {
                            agent::AggregateOperator::Count => wire::AggregateFunction::Count,
                            agent::AggregateOperator::Sum => wire::AggregateFunction::Sum,
                            agent::AggregateOperator::Average => wire::AggregateFunction::Average,
                            agent::AggregateOperator::Minimum => wire::AggregateFunction::Minimum,
                            agent::AggregateOperator::Maximum => wire::AggregateFunction::Maximum,
                        },
                        field_id: measure.field,
                        result_id: measure.name,
                    })
                    .collect(),
                sort: query_sort(&request.sort),
                page: wire::PageRequest {
                    cursor: None,
                    limit: query_page_limit(request.limit)?,
                },
            })
            .map_err(map_client_error)?;
        Ok(agent::AggregatePage {
            rows: call
                .value
                .groups
                .into_iter()
                .map(|group| {
                    Ok(agent::AggregateRow {
                        groups: field_map(group.keys)?,
                        measures: field_map(group.values)?,
                    })
                })
                .collect::<Result<Vec<_>, agent::BackendError>>()?,
            read_version: call.value.read_version,
        })
    }

    fn catalog(
        &self,
        request: agent::CatalogRequest,
    ) -> Result<agent::CatalogResult, agent::BackendError> {
        let scope = query_scope(&request.scope)?;
        'attempt: for attempt in 1..=CONSISTENT_READ_MAX_ATTEMPTS {
            let mut cursor = None;
            let mut fields = Vec::new();
            let mut read_version = None;
            loop {
                let call = match self.client.catalog(wire::CatalogRequest {
                    scope: scope.clone(),
                    field_prefix: request.field_prefix.clone(),
                    page: wire::PageRequest {
                        cursor,
                        limit: QUERY_SERVER_PAGE_LIMIT,
                    },
                }) {
                    Ok(call) => call,
                    Err(error)
                        if attempt < CONSISTENT_READ_MAX_ATTEMPTS
                            && is_read_version_conflict(&error) =>
                    {
                        continue 'attempt;
                    }
                    Err(error) => return Err(map_client_error(error)),
                };
                if read_version.is_some_and(|expected| expected != call.value.read_version) {
                    return Err(protocol_mismatch(
                        "catalog returned a page outside its cursor read version",
                    ));
                }
                read_version.get_or_insert(call.value.read_version);
                fields.extend(call.value.fields.into_iter().map(|field| {
                    agent::CatalogField {
                        field: field.field_id,
                        scalar_type: field.scalar_type,
                        operators: field
                            .operators
                            .into_iter()
                            .map(query_operator_name)
                            .map(str::to_owned)
                            .collect(),
                        sortable: field.sortable,
                        facetable: request.include_facets && field.facetable,
                        aggregatable: field.aggregatable,
                    }
                }));
                let Some(next) = call.value.next_cursor else {
                    return Ok(agent::CatalogResult {
                        fields,
                        read_version: read_version.expect("catalog always reads one page"),
                    });
                };
                cursor = Some(next);
            }
        }

        unreachable!("consistent read attempt count is non-zero")
    }

    fn find_workbenches(
        &self,
        request: agent::FindRequest,
    ) -> Result<agent::FindPage, agent::BackendError> {
        let call = self
            .client
            .find_workspaces(wire::FindWorkspacesRequest {
                committed_only: request.committed == Some(true),
                page: wire::PageRequest {
                    cursor: request
                        .cursor
                        .as_deref()
                        .map(|cursor| decode_cursor("find", cursor))
                        .transpose()?,
                    limit: query_page_limit(request.limit)?,
                },
            })
            .map_err(map_client_error)?;
        let entry_count = call.value.workspaces.len();
        let mut workbenches = Vec::new();
        for discovered in call.value.workspaces {
            let committed = discovered.workspace.commit_head.is_some();
            if request.committed.is_some_and(|wanted| wanted != committed) {
                continue;
            }
            let workbench_id = WorkbenchId::new(discovered.workspace.workbench.as_str().to_owned())
                .map_err(domain_input)?;
            let manifest_projection =
                self.read_run_manifest(&discovered.workspace, &workbench_id)?;
            let canonical_manifest = manifest_projection
                .as_ref()
                .map(|projection| projection.verified.canonical_envelope.as_slice());
            if !find_request_matches_canonical_manifest(&request, canonical_manifest) {
                continue;
            }
            workbenches.push(agent::WorkbenchSummary {
                workbench_id,
                committed,
                commit_id: discovered.workspace.commit_head.map(|identity| identity.0),
                manifest_metadata: manifest_projection
                    .as_ref()
                    .map(|projection| projection.metadata.clone()),
                manifest: manifest_projection.map(|projection| projection.verified.envelope),
            });
        }
        Ok(agent::FindPage {
            workbenches,
            entry_count,
            next_cursor: call
                .value
                .next_cursor
                .as_deref()
                .map(|cursor| encode_cursor("find", cursor)),
            read_version: call.value.read_version,
        })
    }

    fn commit(
        &self,
        request: agent::CommitRequest,
    ) -> Result<agent::CommitOutcome, agent::BackendError> {
        // Validate the storage-neutral facade request before creating durable
        // state. The real envelope is built only after the metadata owner has
        // returned its first, durable commit timestamp.
        drop(canonical_commit_envelope(&request, 1)?);
        let projection_input_digest = commit_projection_input_digest(&request);

        let commit_id = wire::CommitIdentity(request.stable_commit_id);
        let identities = CommitWorkflowIdentities::derive(self.client.root_id(), commit_id);
        let manifest_path = scoped_full_path(&request.workbench_id, RUN_MANIFEST_PATH)?;
        let manifest_target = workspace_path(&manifest_path)?;
        let manifest_content_type =
            wire::ContentType::new(JSON_CONTENT_TYPE.to_owned()).map_err(protocol_input)?;
        let content_digest =
            wire::DigestUri::new(request.content_digest_uri.clone()).map_err(protocol_input)?;
        let manifest_digest =
            wire::DigestUri::new(request.manifest_digest_uri.clone()).map_err(protocol_input)?;

        // Stable commit identity is available without reading the mutable
        // workspace. Recover it first so an old exact request never observes a
        // newer head or run-manifest path.
        let recovered = self.client.commit_workflow(
            self.objects.as_ref(),
            CommitWorkflowOptions {
                identities,
                request: CommitWorkflowRequest::Recover(CommitRecoveryRequest {
                    operation_id: identities.operation_id,
                    workbench: workbench_name(&request.workbench_id)?,
                    commit_id,
                    content_digest: content_digest.clone(),
                    manifest_digest: manifest_digest.clone(),
                    projection_input_digest,
                    tree_manifest_revision_id: identities.tree_manifest_revision_id,
                    replace: request.replace,
                }),
                manifest_target: manifest_target.clone(),
                manifest_content_type: manifest_content_type.clone(),
            },
            |committed_at_unix_seconds| {
                let envelope = canonical_commit_envelope(&request, committed_at_unix_seconds)?;
                self.ensure_artifact_size(&envelope)?;
                Ok(envelope)
            },
        );
        match recovered {
            Ok(workflow) => {
                return Ok(commit_outcome(
                    workflow.result,
                    workflow.replayed,
                    &workflow.manifest,
                ));
            }
            Err(CommitWorkflowError::Lookup(error))
                if rpc_code(&error) == Some(wire::ErrorCode::NotFound) => {}
            Err(CommitWorkflowError::Lookup(error) | CommitWorkflowError::Client(error)) => {
                return Err(map_client_error(error));
            }
            Err(CommitWorkflowError::BuildManifest(error)) => return Err(error),
        }

        let workspace = self
            .create_or_observe_workbench(&request.workbench_id)?
            .workspace;
        let current_manifest = self.read_run_manifest(&workspace, &request.workbench_id)?;
        let manifest_matches = current_manifest
            .as_ref()
            .is_some_and(|projection| run_manifest_matches_request(projection, &request));
        if workspace.commit_head == Some(commit_id) {
            return Err(protocol_mismatch(
                "commit head exists without its durable commit operation",
            ));
        }
        if workspace.commit_head.is_some() && !request.replace {
            return Err(agent::BackendError::conflict(
                "workbench already has a different commit head; set replace=true",
            ));
        }
        if manifest_matches {
            return Err(protocol_mismatch(
                "an uncommitted head cannot already expose the requested canonical run manifest",
            ));
        }
        let condition = match current_manifest.as_ref() {
            None => wire::PublishCondition::CreateOnly,
            Some(projection) if request.replace => wire::PublishCondition::ReplaceOnly {
                expected_generation: projection.wire_metadata.generation,
            },
            Some(_) => {
                return Err(agent::BackendError::conflict(
                    "metadata/run_manifest.json already contains a different v1 envelope",
                ));
            }
        };
        let mut parents = workspace.commit_head.into_iter().collect::<Vec<_>>();
        parents.sort_unstable();
        let commit_request = wire::CommitRequest {
            operation_id: identities.operation_id,
            workbench: workspace.workbench.clone(),
            workspace_incarnation_id: workspace.workspace_incarnation_id,
            commit_id,
            content_digest,
            manifest_digest,
            projection_input_digest,
            tree_manifest_revision_id: identities.tree_manifest_revision_id,
            replace: request.replace,
            run_manifest_condition: condition,
            expected_head_generation: workspace.commit_head_generation,
            parents,
            producer: None,
            lineage_projection: Vec::new(),
        };
        let workflow = self.client.commit_workflow(
            self.objects.as_ref(),
            CommitWorkflowOptions {
                identities,
                request: CommitWorkflowRequest::Fresh(commit_request),
                manifest_target,
                manifest_content_type,
            },
            |committed_at_unix_seconds| {
                let envelope = canonical_commit_envelope(&request, committed_at_unix_seconds)?;
                self.ensure_artifact_size(&envelope)?;
                Ok(envelope)
            },
        );
        let workflow = match workflow {
            Ok(outcome) => outcome,
            Err(CommitWorkflowError::Lookup(error) | CommitWorkflowError::Client(error)) => {
                return Err(map_client_error(error));
            }
            Err(CommitWorkflowError::BuildManifest(error)) => return Err(error),
        };
        Ok(commit_outcome(
            workflow.result,
            workflow.replayed,
            &workflow.manifest,
        ))
    }

    fn mint_snapshot(
        &self,
        request: agent::SnapshotMintRequest,
    ) -> Result<agent::SnapshotRecord, agent::BackendError> {
        let workspace = self.workspace(&request.workbench_id)?;
        if workspace.commit_head.is_none() {
            return Err(agent::BackendError::new(
                agent::BackendErrorKind::InvalidState,
                "only a committed workbench can be snapshotted",
                false,
                json!({"workbench_id": request.workbench_id.as_str()}),
            ));
        }
        let lease_deadline_ms = lease_deadline(request.lease_millis)?;
        let annotation = serde_json::to_vec(&request.annotation)
            .map_err(|error| invalid_state("snapshot annotation is not serializable", error))?;
        let alias = request
            .name
            .map(wire::SnapshotAlias::new)
            .transpose()
            .map_err(protocol_input)?;
        self.client
            .mint_snapshot_workflow(SnapshotMintOptions {
                workbench: workspace.workbench,
                workspace_incarnation_id: workspace.workspace_incarnation_id,
                lease_deadline_ms,
                alias,
                annotation,
            })
            .map_err(map_snapshot_client_error)
            .and_then(|call| snapshot_record(call.value))
    }

    fn renew_snapshot(
        &self,
        request: agent::SnapshotRenewRequest,
    ) -> Result<agent::SnapshotRecord, agent::BackendError> {
        let deadline = lease_deadline(request.lease_millis)?;
        self.client
            .renew_snapshot_workflow(SnapshotRenewOptions {
                workbench: workbench_name(&request.workbench_id)?,
                selector: snapshot_selector(&request.selector)?,
                lease_deadline_ms: deadline,
            })
            .map_err(map_snapshot_client_error)
            .and_then(|call| snapshot_record(call.value))
    }

    fn retire_snapshot(
        &self,
        request: agent::SnapshotRetireRequest,
    ) -> Result<agent::SnapshotRetireOutcome, agent::BackendError> {
        let retire_annotation = request
            .reason
            .map(|reason| {
                agent::canonical_json_bytes(&json!({"reason": reason, "metadata": Value::Null}))
                    .map_err(|error| {
                        invalid_state("retire annotation canonicalization failed", error)
                    })
            })
            .transpose()?;
        let outcome = self
            .client
            .retire_snapshot_workflow(SnapshotRetireOptions {
                workbench: workbench_name(&request.workbench_id)?,
                selector: snapshot_selector(&request.selector)?,
                retire_annotation,
            })
            .map_err(map_snapshot_client_error)?;
        let record = snapshot_record(outcome.snapshot)?;
        Ok(agent::SnapshotRetireOutcome {
            snapshot_id: record.snapshot_id,
            name: record.name,
            retired: outcome.retired,
            state: record.state,
            retire_annotation: record.retire_annotation,
        })
    }

    fn list_snapshots(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Vec<agent::SnapshotRecord>, agent::BackendError> {
        self.client
            .list_all_snapshots(workbench_name(workbench_id)?)
            .map_err(map_snapshot_client_error)?
            .into_iter()
            .map(snapshot_record)
            .collect()
    }

    fn restore(
        &self,
        request: agent::RestoreRequest,
    ) -> Result<agent::RestoreOutcome, agent::BackendError> {
        let destination_workbench = workbench_name(&request.destination_workbench_id)?;
        let destination = self.optional_workspace(&request.destination_workbench_id)?;
        let manifest_path =
            scoped_full_path(&request.destination_workbench_id, RESTORE_MANIFEST_PATH)?;
        let manifest_target = workspace_path(&manifest_path)?;
        let (identities, manifest_identities, workflow_request, canonical_manifest, snapshot_id) =
            match destination {
                Some(destination) => {
                    let projection = self
                        .read_restore_manifest(&destination, &request.destination_workbench_id)?;
                    let verified = &projection.verified;
                    if verified.source_workbench_id != request.source_workbench_id
                        || verified.source_path != request.source_workbench_path
                        || verified.destination_workbench_id != request.destination_workbench_id
                        || verified.destination_path != request.destination_workbench_path
                        || matches!(
                            request.selector,
                            agent::SnapshotSelector::Id(snapshot_id)
                                if snapshot_id != verified.snapshot_id
                        )
                    {
                        return Err(agent::BackendError::conflict(
                            "existing destination restore manifest belongs to different provenance",
                        ));
                    }
                    let identities = RestoreWorkflowIdentities {
                        operation_id: wire::OperationIdentity(verified.operation_id),
                        destination_workspace_incarnation_id: destination.workspace_incarnation_id,
                    };
                    let manifest_identities = identities
                        .manifest_identities(self.client.root_id(), &verified.envelope_digest_uri);
                    if projection.metadata.artifact_revision_id != manifest_identities.revision_id {
                        return Err(protocol_mismatch(
                            "restore manifest revision does not match its deterministic operation",
                        ));
                    }
                    let recovery = RestoreRecoveryRequest {
                        source_workbench: workbench_name(&verified.source_workbench_id)?,
                        source: wire::RestoreSource::Snapshot(wire::SnapshotSelector::Id(
                            verified.snapshot_id,
                        )),
                        destination_workbench: destination.workbench.clone(),
                        destination_workspace_incarnation_id: destination.workspace_incarnation_id,
                        restore_manifest: wire::RestoreManifestDescriptor {
                            body_digest: projection.metadata.descriptor.body_digest.clone(),
                            logical_size: projection.metadata.descriptor.logical_size,
                            content_type: projection.metadata.descriptor.content_type.clone(),
                        },
                    };
                    (
                        identities,
                        manifest_identities,
                        RestoreWorkflowRequest::Recover(recovery),
                        projection.verified.canonical_envelope.clone(),
                        verified.snapshot_id,
                    )
                }
                None => {
                    let source_workspace = self.workspace(&request.source_workbench_id)?;
                    let snapshot =
                        self.snapshot(&request.source_workbench_id, &request.selector)?;
                    if snapshot.state != agent::SnapshotLifecycleState::Alive {
                        return Err(snapshot_expired(
                            &request.source_workbench_id,
                            snapshot.snapshot_id,
                        ));
                    }
                    let identities = RestoreWorkflowIdentities::derive_snapshot(
                        self.client.root_id(),
                        &source_workspace.workbench,
                        source_workspace.workspace_incarnation_id,
                        snapshot.snapshot_id,
                        &destination_workbench,
                    );
                    let canonical_manifest = agent::build_restore_manifest_v1(
                        identities.operation_id.0,
                        &request.source_workbench_id,
                        &request.source_workbench_path,
                        &request.destination_workbench_id,
                        &request.destination_workbench_path,
                        snapshot.snapshot_id,
                    )
                    .map_err(|error| {
                        invalid_state("facade supplied invalid restore provenance", error)
                    })?;
                    let verified = agent::verify_restore_manifest_v1(&canonical_manifest).map_err(
                        |error| invalid_state("canonical restore manifest is invalid", error),
                    )?;
                    self.ensure_artifact_size(&canonical_manifest)?;
                    let manifest_identities = identities
                        .manifest_identities(self.client.root_id(), &verified.envelope_digest_uri);
                    let prepare = wire::PrepareRestoreRequest {
                        source_workbench: source_workspace.workbench,
                        source_workspace_incarnation_id: source_workspace.workspace_incarnation_id,
                        source: wire::RestoreSource::Snapshot(wire::SnapshotSelector::Id(
                            snapshot.snapshot_id,
                        )),
                        destination_workbench: destination_workbench.clone(),
                        destination_workspace_incarnation_id: identities
                            .destination_workspace_incarnation_id,
                        restore_manifest: wire::RestoreManifestDescriptor {
                            body_digest: wire::DigestUri::new(verified.envelope_digest_uri.clone())
                                .map_err(protocol_input)?,
                            logical_size: canonical_manifest.len() as u64,
                            content_type: wire::ContentType::new(JSON_CONTENT_TYPE.to_owned())
                                .map_err(protocol_input)?,
                        },
                    };
                    (
                        identities,
                        manifest_identities,
                        RestoreWorkflowRequest::Fresh(prepare),
                        canonical_manifest,
                        snapshot.snapshot_id,
                    )
                }
            };
        let workflow = self
            .client
            .restore_workflow(
                self.objects.as_ref(),
                RestoreWorkflowOptions {
                    identities,
                    manifest_identities,
                    request: workflow_request,
                    manifest_target,
                    manifest_bytes: canonical_manifest,
                },
            )
            .map_err(map_restore_workflow_error)?;
        let read_version = workflow.source_snapshot_read_version.ok_or_else(|| {
            protocol_mismatch("snapshot restore operation omitted its durable read version")
        })?;
        Ok(agent::RestoreOutcome {
            operation_id: identities.operation_id.0,
            snapshot_id,
            read_version,
            destination_generation: workflow.result.destination.workspace_revision,
            idempotent_replay: workflow.replayed,
        })
    }
}

impl CliWorkbenchBackend {
    fn read_run_manifest(
        &self,
        workspace: &wire::WorkspaceSummary,
        workbench_id: &WorkbenchId,
    ) -> Result<Option<RunManifestProjection>, agent::BackendError> {
        let path = scoped_full_path(workbench_id, RUN_MANIFEST_PATH)?;
        let view = wire::WorkspaceReadView::Live;
        let Some(metadata) = self.path_metadata(&path, view.clone())? else {
            if workspace.commit_head.is_some() {
                return Err(protocol_mismatch(
                    "committed workbench has no metadata/run_manifest.json",
                ));
            }
            return Ok(None);
        };
        let size = usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX);
        if size > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "run manifest is {size} bytes, maximum is {}",
                self.max_artifact_bytes
            )));
        }
        let body = self
            .client
            .read_artifact(self.objects.as_ref(), None, workspace_path(&path)?, view)
            .map_err(map_client_error)?;
        let verified = agent::verify_run_manifest_v1(&body.bytes)
            .map_err(|error| invalid_state("run manifest violates the v1 projection", error))?;
        if verified.workbench_id != *workbench_id
            || verified.envelope_digest_uri != metadata.descriptor.body_digest.as_str()
            || verified.canonical_envelope.len() as u64 != metadata.descriptor.logical_size
            || metadata.descriptor.content_type.as_str() != JSON_CONTENT_TYPE
            || workspace
                .commit_head
                .is_some_and(|commit_id| commit_id.0 != verified.commit_identity)
        {
            return Err(protocol_mismatch(
                "run manifest envelope does not match its path descriptor or typed commit head",
            ));
        }
        Ok(Some(RunManifestProjection {
            metadata: artifact_metadata(&metadata),
            wire_metadata: metadata,
            verified,
        }))
    }

    fn read_restore_manifest(
        &self,
        workspace: &wire::WorkspaceSummary,
        workbench_id: &WorkbenchId,
    ) -> Result<RestoreManifestProjection, agent::BackendError> {
        let path = scoped_full_path(workbench_id, RESTORE_MANIFEST_PATH)?;
        let view = wire::WorkspaceReadView::Live;
        let Some(metadata) = self.path_metadata(&path, view.clone())? else {
            return Err(agent::BackendError::conflict(
                "restore destination already exists without metadata/restore_manifest.json",
            ));
        };
        let size = usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX);
        if size > self.max_artifact_bytes {
            return Err(resource_exhausted(format!(
                "restore manifest is {size} bytes, maximum is {}",
                self.max_artifact_bytes
            )));
        }
        let body = self
            .client
            .read_artifact(self.objects.as_ref(), None, workspace_path(&path)?, view)
            .map_err(map_client_error)?;
        let verified = agent::verify_restore_manifest_v1(&body.bytes)
            .map_err(|error| invalid_state("restore manifest violates the v1 projection", error))?;
        if body.metadata != metadata
            || metadata.workspace_incarnation_id != workspace.workspace_incarnation_id
            || verified.destination_workbench_id != *workbench_id
            || verified.envelope_digest_uri != metadata.descriptor.body_digest.as_str()
            || verified.canonical_envelope.len() as u64 != metadata.descriptor.logical_size
            || metadata.descriptor.content_type.as_str() != JSON_CONTENT_TYPE
        {
            return Err(protocol_mismatch(
                "restore manifest envelope does not match its live destination path descriptor",
            ));
        }
        Ok(RestoreManifestProjection { metadata, verified })
    }
}

struct RunManifestProjection {
    metadata: agent::ArtifactMetadata,
    wire_metadata: wire::PathMetadata,
    verified: agent::VerifiedRunManifestV1,
}

struct RestoreManifestProjection {
    metadata: wire::PathMetadata,
    verified: agent::VerifiedRestoreManifestV1,
}

fn canonical_commit_envelope(
    request: &agent::CommitRequest,
    committed_at_unix_seconds: u64,
) -> Result<Vec<u8>, agent::BackendError> {
    agent::build_run_manifest_v1(
        &request.workbench_id,
        &request.workbench_path,
        &request.content_digest_uri,
        &request.canonical_manifest,
        &request.manifest_digest_uri,
        request.stable_commit_id,
        committed_at_unix_seconds,
    )
    .map_err(|error| invalid_state("commit request cannot form a canonical run manifest", error))
}

fn commit_projection_input_digest(request: &agent::CommitRequest) -> wire::Digest {
    wire::Digest(agent::run_manifest_projection_input_digest_v1(
        &request.workbench_id,
        &request.workbench_path,
        &request.content_digest_uri,
        &request.canonical_manifest,
        &request.manifest_digest_uri,
        request.stable_commit_id,
    ))
}

fn run_manifest_matches_request(
    projection: &RunManifestProjection,
    request: &agent::CommitRequest,
) -> bool {
    projection.verified.workbench_id == request.workbench_id
        && projection.verified.workbench_path == request.workbench_path
        && projection.verified.content_digest_uri == request.content_digest_uri
        && projection.verified.manifest_digest_uri == request.manifest_digest_uri
        && projection.verified.commit_identity == request.stable_commit_id
        && projection.verified.canonical_manifest == request.canonical_manifest
}

fn workbench_name(workbench_id: &WorkbenchId) -> Result<wire::WorkbenchName, agent::BackendError> {
    wire::WorkbenchName::new(workbench_id.as_str().to_owned()).map_err(protocol_input)
}

fn snapshot_selector(
    selector: &agent::SnapshotSelector,
) -> Result<wire::SnapshotSelector, agent::BackendError> {
    match selector {
        agent::SnapshotSelector::Id(snapshot_id) => Ok(wire::SnapshotSelector::Id(*snapshot_id)),
        agent::SnapshotSelector::Name(name) => wire::SnapshotAlias::new(name.clone())
            .map(wire::SnapshotSelector::Alias)
            .map_err(protocol_input),
    }
}

fn relative_path(path: &NormalizedRelativePath) -> Result<wire::RelativePath, agent::BackendError> {
    wire::RelativePath::new(path.as_str().to_owned()).map_err(protocol_input)
}

fn workspace_path(path: &agent::ScopedPath) -> Result<wire::WorkspacePath, agent::BackendError> {
    Ok(wire::WorkspacePath {
        workbench: workbench_name(&path.workbench_id)?,
        path: relative_path(&full_path(path)?)?,
    })
}

fn validate_response_workbench(
    path: &wire::WorkspacePath,
    expected: &WorkbenchId,
) -> Result<(), agent::BackendError> {
    if path.workbench.as_str() != expected.as_str() {
        return Err(protocol_mismatch(
            "list_paths returned an entry for a different workbench",
        ));
    }
    Ok(())
}

fn full_path(path: &agent::ScopedPath) -> Result<NormalizedRelativePath, agent::BackendError> {
    let value = path.logical_path();
    if value.is_empty() {
        return Err(agent::BackendError::new(
            agent::BackendErrorKind::InvalidState,
            "artifact path is required",
            false,
            json!({"workbench_id": path.workbench_id.as_str()}),
        ));
    }
    NormalizedRelativePath::new(value).map_err(domain_input)
}

fn full_path_optional(
    path: &agent::ScopedPath,
) -> Result<Option<NormalizedRelativePath>, agent::BackendError> {
    let value = path.logical_path();
    if value.is_empty() {
        Ok(None)
    } else {
        NormalizedRelativePath::new(value)
            .map(Some)
            .map_err(domain_input)
    }
}

fn scoped_full_path(
    workbench_id: &WorkbenchId,
    full_path: &str,
) -> Result<agent::ScopedPath, agent::BackendError> {
    let path = NormalizedRelativePath::new(full_path.to_owned()).map_err(domain_input)?;
    scoped_normalized_path(workbench_id.clone(), path)
}

fn scoped_path(path: &wire::WorkspacePath) -> Result<agent::ScopedPath, agent::BackendError> {
    let workbench_id =
        WorkbenchId::new(path.workbench.as_str().to_owned()).map_err(domain_input)?;
    let path = NormalizedRelativePath::new(path.path.as_str().to_owned()).map_err(domain_input)?;
    scoped_normalized_path(workbench_id, path)
}

fn scoped_normalized_path(
    workbench_id: WorkbenchId,
    path: NormalizedRelativePath,
) -> Result<agent::ScopedPath, agent::BackendError> {
    let text = path.as_str();
    let (first, remainder) = text
        .split_once('/')
        .map_or((text, None), |(first, rest)| (first, Some(rest)));
    if let Some(section) = section(first) {
        let relative_path = remainder
            .map(|remainder| NormalizedRelativePath::new(remainder.to_owned()))
            .transpose()
            .map_err(domain_input)?;
        Ok(agent::ScopedPath {
            workbench_id,
            section: Some(section),
            relative_path,
        })
    } else {
        Ok(agent::ScopedPath {
            workbench_id,
            section: None,
            relative_path: Some(path),
        })
    }
}

fn section(value: &str) -> Option<agent::Section> {
    agent::WORKBENCH_SECTIONS
        .into_iter()
        .find(|section| section.as_str() == value)
}

fn artifact_metadata(metadata: &wire::PathMetadata) -> agent::ArtifactMetadata {
    agent::ArtifactMetadata {
        generation: metadata.generation,
        size_bytes: metadata.descriptor.logical_size,
        digest_uri: metadata.descriptor.body_digest.as_str().to_owned(),
        content_type: metadata.descriptor.content_type.as_str().to_owned(),
        producer: metadata.descriptor.producer.clone(),
        manifest_id: metadata.descriptor.manifest_identity.clone(),
        indexed_fields: metadata
            .descriptor
            .index_fields
            .iter()
            .map(|field| (field.field_id.clone(), scalar_value(&field.value)))
            .collect(),
    }
}

fn merge_list_candidate(
    candidates: &mut BTreeMap<NormalizedRelativePath, agent::ListEntry>,
    child: NormalizedRelativePath,
    metadata: Option<wire::PathMetadata>,
    workbench_id: &WorkbenchId,
    root_scope: bool,
) -> Result<(), agent::BackendError> {
    let root_section = root_scope
        .then(|| child.components().next().and_then(section))
        .flatten();
    let (kind, artifact) = if root_section.is_some() {
        (agent::ArtifactKind::Section, None)
    } else if let Some(metadata) = metadata.as_ref() {
        (
            agent::ArtifactKind::Artifact,
            Some(artifact_metadata(metadata)),
        )
    } else {
        (agent::ArtifactKind::Directory, None)
    };
    let candidate = agent::ListEntry {
        path: scoped_normalized_path(workbench_id.clone(), child.clone())?,
        kind,
        artifact,
    };
    match candidates.get(&child) {
        Some(existing)
            if list_kind_priority(&existing.kind) >= list_kind_priority(&candidate.kind) => {}
        _ => {
            candidates.insert(child, candidate);
        }
    }
    Ok(())
}

fn direct_list_child(
    prefix: Option<&NormalizedRelativePath>,
    source: NormalizedRelativePath,
) -> Result<Option<NormalizedRelativePath>, agent::BackendError> {
    match prefix {
        None if source.component_count() == 1 => Ok(Some(source)),
        None => Err(protocol_mismatch(
            "non-recursive root listing returned a nested path",
        )),
        Some(prefix) if &source == prefix => Ok(None),
        Some(prefix) => {
            let remainder = source
                .as_str()
                .strip_prefix(prefix.as_str())
                .and_then(|value| value.strip_prefix('/'))
                .ok_or_else(|| {
                    protocol_mismatch("non-recursive listing returned a path outside its prefix")
                })?;
            if remainder.is_empty() || remainder.contains('/') {
                return Err(protocol_mismatch(
                    "non-recursive listing returned a non-direct descendant",
                ));
            }
            Ok(Some(source))
        }
    }
}

fn list_kind_priority(kind: &agent::ArtifactKind) -> u8 {
    match kind {
        agent::ArtifactKind::Directory => 0,
        agent::ArtifactKind::Artifact => 1,
        agent::ArtifactKind::Section => 2,
        agent::ArtifactKind::Workbench => 3,
    }
}

fn query_scope(scope: &agent::QueryScope) -> Result<wire::QueryScope, agent::BackendError> {
    let path_prefix = match (scope.section, &scope.path) {
        (None, None) => None,
        (None, Some(path)) => Some(path.clone()),
        (Some(section), None) => {
            Some(NormalizedRelativePath::new(section.as_str().to_owned()).map_err(domain_input)?)
        }
        (Some(section), Some(path)) => Some(
            NormalizedRelativePath::new(format!("{}/{path}", section.as_str()))
                .map_err(domain_input)?,
        ),
    };
    let path_prefix = path_prefix.as_ref().map(relative_path).transpose()?;
    match &scope.workbench_id {
        Some(workbench_id) => Ok(wire::QueryScope::Workspace {
            workbench: workbench_name(workbench_id)?,
            path_prefix,
        }),
        None => Ok(wire::QueryScope::Root { path_prefix }),
    }
}

fn query_predicate(
    predicate: &agent::QueryPredicate,
) -> Result<wire::QueryPredicate, agent::BackendError> {
    let operator = match predicate.operator {
        agent::PredicateOperator::Equal => wire::QueryOperator::Equal,
        agent::PredicateOperator::NotEqual => wire::QueryOperator::NotEqual,
        agent::PredicateOperator::In => wire::QueryOperator::In,
        agent::PredicateOperator::Prefix => wire::QueryOperator::Prefix,
        agent::PredicateOperator::Suffix => wire::QueryOperator::Suffix,
        agent::PredicateOperator::Contains => wire::QueryOperator::Contains,
        agent::PredicateOperator::Greater => wire::QueryOperator::Greater,
        agent::PredicateOperator::GreaterOrEqual => wire::QueryOperator::GreaterOrEqual,
        agent::PredicateOperator::Less => wire::QueryOperator::Less,
        agent::PredicateOperator::LessOrEqual => wire::QueryOperator::LessOrEqual,
        agent::PredicateOperator::Exists => wire::QueryOperator::Exists,
        agent::PredicateOperator::NotExists => wire::QueryOperator::NotExists,
    };
    let operand = match (&predicate.operator, predicate.value.as_ref()) {
        (agent::PredicateOperator::Exists | agent::PredicateOperator::NotExists, None) => {
            wire::QueryOperand::None
        }
        (agent::PredicateOperator::In, Some(agent::QueryValue::List(values))) => {
            wire::QueryOperand::Set(
                values
                    .iter()
                    .map(query_scalar)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        }
        (agent::PredicateOperator::In, _) => {
            return Err(invalid_backend_input(
                "in predicate requires a list operand",
            ));
        }
        (_, Some(value)) => wire::QueryOperand::Scalar(query_scalar(value)?),
        (_, None) => {
            return Err(invalid_backend_input(
                "comparison predicate requires a scalar operand",
            ));
        }
    };
    Ok(wire::QueryPredicate {
        field_id: predicate.field.clone(),
        operator,
        operand,
    })
}

fn query_scalar(value: &agent::QueryValue) -> Result<wire::ScalarValue, agent::BackendError> {
    match value {
        agent::QueryValue::Null => Ok(wire::ScalarValue::Null),
        agent::QueryValue::Boolean(value) => Ok(wire::ScalarValue::Boolean(*value)),
        agent::QueryValue::Unsigned(value) => Ok(wire::ScalarValue::Unsigned(*value)),
        agent::QueryValue::Signed(value) => Ok(wire::ScalarValue::Signed(*value)),
        agent::QueryValue::Float(value) if value.is_finite() => {
            Ok(wire::ScalarValue::Decimal(value.to_string()))
        }
        agent::QueryValue::Float(_) => Err(invalid_backend_input("query float must be finite")),
        agent::QueryValue::String(value) => Ok(wire::ScalarValue::String(value.clone())),
        agent::QueryValue::List(_) => Err(invalid_backend_input(
            "nested query lists are not scalar values",
        )),
    }
}

fn scalar_value(value: &wire::ScalarValue) -> agent::QueryValue {
    match value {
        wire::ScalarValue::Null => agent::QueryValue::Null,
        wire::ScalarValue::Boolean(value) => agent::QueryValue::Boolean(*value),
        wire::ScalarValue::Signed(value) | wire::ScalarValue::Timestamp(value) => {
            agent::QueryValue::Signed(*value)
        }
        wire::ScalarValue::Unsigned(value) => agent::QueryValue::Unsigned(*value),
        wire::ScalarValue::Decimal(value) => value.parse::<f64>().map_or_else(
            |_| agent::QueryValue::String(value.clone()),
            agent::QueryValue::Float,
        ),
        wire::ScalarValue::String(value) => agent::QueryValue::String(value.clone()),
        wire::ScalarValue::Bytes(value) => agent::QueryValue::String(URL_SAFE_NO_PAD.encode(value)),
    }
}

fn query_sort(sort: &[agent::QuerySort]) -> Vec<wire::SortField> {
    sort.iter()
        .map(|sort| wire::SortField {
            field_id: sort.field.clone(),
            direction: match sort.direction {
                agent::SortDirection::Ascending => wire::SortDirection::Ascending,
                agent::SortDirection::Descending => wire::SortDirection::Descending,
            },
        })
        .collect()
}

fn field_map(
    fields: Vec<wire::FieldValue>,
) -> Result<BTreeMap<String, agent::QueryValue>, agent::BackendError> {
    let mut values = BTreeMap::new();
    for field in fields {
        if values
            .insert(field.field_id.clone(), scalar_value(&field.value))
            .is_some()
        {
            return Err(protocol_mismatch("query result contains a duplicate field"));
        }
    }
    Ok(values)
}

fn search_hit(hit: wire::SearchHit) -> Result<agent::SearchHit, agent::BackendError> {
    Ok(agent::SearchHit {
        workbench_id: WorkbenchId::new(hit.metadata.path.workbench.as_str().to_owned())
            .map_err(domain_input)?,
        path: NormalizedRelativePath::new(hit.metadata.path.path.as_str().to_owned())
            .map_err(domain_input)?,
        metadata: artifact_metadata(&hit.metadata),
        projection: field_map(hit.projection)?,
    })
}

fn facet_result(facet: wire::FacetResult) -> Result<agent::FacetResult, agent::BackendError> {
    Ok(agent::FacetResult {
        field: facet.field_id,
        buckets: facet
            .buckets
            .into_iter()
            .map(|bucket| agent::FacetBucket {
                value: scalar_value(&bucket.value),
                count: bucket.count,
            })
            .collect(),
        distinct_count: facet.distinct_count,
        truncated: facet.truncated,
    })
}

fn query_operator_name(operator: wire::QueryOperator) -> &'static str {
    match operator {
        wire::QueryOperator::Equal => "eq",
        wire::QueryOperator::NotEqual => "ne",
        wire::QueryOperator::In => "in",
        wire::QueryOperator::Less => "lt",
        wire::QueryOperator::LessOrEqual => "lte",
        wire::QueryOperator::Greater => "gt",
        wire::QueryOperator::GreaterOrEqual => "gte",
        wire::QueryOperator::Prefix => "prefix",
        wire::QueryOperator::Suffix => "suffix",
        wire::QueryOperator::Contains => "contains",
        wire::QueryOperator::Exists => "exists",
        wire::QueryOperator::NotExists => "not_exists",
    }
}

fn snapshot_record(
    result: wire::SnapshotResult,
) -> Result<agent::SnapshotRecord, agent::BackendError> {
    let annotation = serde_json::from_slice(&result.annotation)
        .map_err(|error| invalid_state("snapshot annotation is not valid JSON", error))?;
    let retire_annotation = result
        .retire_annotation
        .map(|annotation| {
            serde_json::from_slice(&annotation).map_err(|error| {
                invalid_state("snapshot retire annotation is not valid JSON", error)
            })
        })
        .transpose()?;
    Ok(agent::SnapshotRecord {
        snapshot_id: result.snapshot_id,
        name: result.alias.map(|alias| alias.as_str().to_owned()),
        read_version: result.read_version,
        lease_expires_unix_ms: Some(result.lease_deadline_ms),
        annotation,
        retire_annotation,
        state: match result.status {
            wire::SnapshotStatus::Alive => agent::SnapshotLifecycleState::Alive,
            wire::SnapshotStatus::Expired | wire::SnapshotStatus::ReapClaimed => {
                agent::SnapshotLifecycleState::Expired
            }
            wire::SnapshotStatus::Retired => agent::SnapshotLifecycleState::Retired,
            wire::SnapshotStatus::Reaped => agent::SnapshotLifecycleState::Reaped,
        },
    })
}

fn commit_outcome(
    result: wire::CommitResult,
    replayed: bool,
    manifest: &wire::CommitManifestBinding,
) -> agent::CommitOutcome {
    agent::CommitOutcome {
        commit_id: result.commit_id.0,
        generation: result.head_generation,
        manifest_size_bytes: manifest.descriptor.logical_size,
        envelope_digest_uri: manifest.descriptor.body_digest.as_str().to_owned(),
        tree_digest_uri: format!("sha256:{}", encode_lowercase_hex(&result.member_digest.0)),
        idempotent_replay: replayed,
    }
}

fn lease_deadline(lease_millis: u64) -> Result<u64, agent::BackendError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let now = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
    now.checked_add(lease_millis)
        .filter(|deadline| *deadline != 0)
        .ok_or_else(|| invalid_backend_input("snapshot lease deadline overflows"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ListCursor {
    read_version: u64,
    anchor: NormalizedRelativePath,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct GrepCursor {
    read_version: u64,
    server_cursor: Vec<u8>,
}

fn grep_scope_digest(
    workbench_id: &WorkbenchId,
    prefix: Option<&NormalizedRelativePath>,
    recursive: bool,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workspace.grep-scope.v1\0");
    hash_len64(&mut hasher, workbench_id.as_bytes());
    match prefix {
        None => hasher.update([0]),
        Some(prefix) => {
            hasher.update([1]);
            hash_len64(&mut hasher, prefix.as_str().as_bytes());
        }
    }
    hasher.update([u8::from(recursive)]);
    hasher.finalize().into()
}

fn encode_grep_cursor(read_version: u64, scope_digest: [u8; 32], server_cursor: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(
        GREP_CURSOR_VERSION.len() + 8 + scope_digest.len() + server_cursor.len(),
    );
    encoded.extend_from_slice(GREP_CURSOR_VERSION);
    encoded.extend_from_slice(&read_version.to_be_bytes());
    encoded.extend_from_slice(&scope_digest);
    encoded.extend_from_slice(server_cursor);
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_grep_cursor(
    cursor: &str,
    expected_scope_digest: [u8; 32],
) -> Result<GrepCursor, agent::BackendError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| invalid_state("grep cursor is not canonical base64url", error))?;
    let payload = decoded.strip_prefix(GREP_CURSOR_VERSION).ok_or_else(|| {
        invalid_backend_input("grep cursor does not use the current scope-bound schema")
    })?;
    let (read_version, payload) = payload
        .split_first_chunk::<8>()
        .ok_or_else(|| invalid_backend_input("grep cursor omits its read version"))?;
    let read_version = u64::from_be_bytes(*read_version);
    if read_version == 0 {
        return Err(invalid_backend_input(
            "grep cursor read version must be greater than zero",
        ));
    }
    let (scope_digest, server_cursor) = payload
        .split_first_chunk::<32>()
        .ok_or_else(|| invalid_backend_input("grep cursor omits its scope digest"))?;
    if scope_digest != &expected_scope_digest {
        return Err(invalid_backend_input(
            "grep cursor belongs to a different workbench, prefix, or recursion mode",
        ));
    }
    if server_cursor.is_empty() {
        return Err(invalid_backend_input("grep cursor omits its server anchor"));
    }
    Ok(GrepCursor {
        read_version,
        server_cursor: server_cursor.to_vec(),
    })
}

fn list_scope_digest(
    workbench_id: &WorkbenchId,
    prefix: Option<&NormalizedRelativePath>,
    view: &agent::ReadView,
) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.workspace.list-scope.v1\0");
    hash_len64(&mut hasher, workbench_id.as_bytes());
    match prefix {
        None => hasher.update([0]),
        Some(prefix) => {
            hasher.update([1]);
            hash_len64(&mut hasher, prefix.as_str().as_bytes());
        }
    }
    match view {
        agent::ReadView::Live => hasher.update([0]),
        agent::ReadView::Snapshot(agent::SnapshotSelector::Id(snapshot_id)) => {
            hasher.update([1]);
            hasher.update(snapshot_id.to_be_bytes());
        }
        agent::ReadView::Snapshot(agent::SnapshotSelector::Name(alias)) => {
            hasher.update([2]);
            hash_len64(&mut hasher, alias.as_bytes());
        }
    }
    hasher.finalize().into()
}

fn encode_list_cursor(
    read_version: u64,
    scope_digest: [u8; 32],
    anchor: &NormalizedRelativePath,
) -> String {
    let mut encoded = Vec::with_capacity(
        LIST_CURSOR_VERSION.len() + 8 + scope_digest.len() + anchor.as_str().len(),
    );
    encoded.extend_from_slice(LIST_CURSOR_VERSION);
    encoded.extend_from_slice(&read_version.to_be_bytes());
    encoded.extend_from_slice(&scope_digest);
    encoded.extend_from_slice(anchor.as_str().as_bytes());
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_list_cursor(
    cursor: &str,
    expected_scope_digest: [u8; 32],
) -> Result<ListCursor, agent::BackendError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| invalid_state("list cursor is not canonical base64url", error))?;
    let payload = decoded.strip_prefix(LIST_CURSOR_VERSION).ok_or_else(|| {
        invalid_backend_input("list cursor does not use the current scope-bound schema")
    })?;
    let (read_version, payload) = payload
        .split_first_chunk::<8>()
        .ok_or_else(|| invalid_backend_input("list cursor omits its read version"))?;
    let read_version = u64::from_be_bytes(*read_version);
    if read_version == 0 {
        return Err(invalid_backend_input(
            "list cursor read version must be greater than zero",
        ));
    }
    let (scope_digest, anchor) = payload
        .split_first_chunk::<32>()
        .ok_or_else(|| invalid_backend_input("list cursor omits its scope digest"))?;
    if scope_digest != &expected_scope_digest {
        return Err(invalid_backend_input(
            "list cursor belongs to a different workbench, prefix, or read view",
        ));
    }
    let anchor = String::from_utf8(anchor.to_vec())
        .map_err(|error| invalid_state("list cursor anchor is not valid UTF-8", error))?;
    let anchor = NormalizedRelativePath::new(anchor).map_err(domain_input)?;
    Ok(ListCursor {
        read_version,
        anchor,
    })
}

fn encode_cursor(kind: &str, payload: &[u8]) -> String {
    let mut encoded = Vec::with_capacity(CURSOR_VERSION.len() + kind.len() + 1 + payload.len());
    encoded.extend_from_slice(CURSOR_VERSION);
    encoded.extend_from_slice(kind.as_bytes());
    encoded.push(0);
    encoded.extend_from_slice(payload);
    URL_SAFE_NO_PAD.encode(encoded)
}

fn decode_cursor(kind: &str, cursor: &str) -> Result<Vec<u8>, agent::BackendError> {
    let decoded = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|error| invalid_state("cursor is not canonical base64url", error))?;
    let expected = [CURSOR_VERSION, kind.as_bytes(), &[0]].concat();
    decoded
        .strip_prefix(expected.as_slice())
        .map(ToOwned::to_owned)
        .ok_or_else(|| invalid_backend_input("cursor belongs to a different operation"))
}

fn page_limit(limit: usize) -> Result<u32, agent::BackendError> {
    let limit = u32::try_from(limit)
        .map_err(|_| invalid_backend_input("page limit does not fit the protocol"))?;
    if !(1..=wire::PageRequest::MAX_LIMIT).contains(&limit) {
        return Err(invalid_backend_input(format!(
            "page limit must be between 1 and {}",
            wire::PageRequest::MAX_LIMIT
        )));
    }
    Ok(limit)
}

fn query_page_limit(limit: usize) -> Result<u32, agent::BackendError> {
    let limit = page_limit(limit)?;
    if limit > wire::MAX_QUERY_PAGE_LIMIT {
        return Err(invalid_backend_input(format!(
            "query page limit must be between 1 and {}",
            wire::MAX_QUERY_PAGE_LIMIT
        )));
    }
    Ok(limit)
}

fn list_server_page_limit(limit: usize, candidate_count: usize, first_page: bool) -> u32 {
    let union_target = limit.saturating_add(1);
    let requested = if first_page {
        union_target
    } else {
        union_target.saturating_sub(candidate_count).max(1)
    };
    u32::try_from(requested.min(wire::PageRequest::MAX_LIMIT as usize))
        .expect("protocol page limit fits u32")
}

fn find_request_matches_canonical_manifest(
    request: &agent::FindRequest,
    canonical_manifest: Option<&[u8]>,
) -> bool {
    let Some(pattern) = request.manifest_pattern.as_deref() else {
        return true;
    };
    let Some(bytes) = canonical_manifest else {
        return false;
    };
    if pattern.is_empty() {
        return true;
    }
    bytes
        .windows(pattern.len())
        .any(|window| window.eq_ignore_ascii_case(pattern.as_bytes()))
}

fn digest_prefix(digest: [u8; 32]) -> [u8; 16] {
    digest[..16]
        .try_into()
        .expect("SHA-256 prefix has fixed width")
}

fn hash_len64(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn rpc_failure(error: &ClientError) -> Option<&wire::RpcFailure> {
    match error {
        ClientError::Rpc(failure) => Some(failure),
        ClientError::ArtifactPublishFailed { source, .. } => rpc_failure(source),
        ClientError::RetryExhausted { last_error, .. } => rpc_failure(last_error),
        _ => None,
    }
}

fn rpc_code(error: &ClientError) -> Option<wire::ErrorCode> {
    rpc_failure(error).map(|failure| failure.code)
}

fn is_read_version_conflict(error: &ClientError) -> bool {
    rpc_failure(error).is_some_and(|failure| {
        failure.code == wire::ErrorCode::PreconditionFailed
            && failure.conflict == Some(wire::ConflictKind::ReadVersion)
    })
}

fn map_client_error(error: ClientError) -> agent::BackendError {
    if let Some(failure) = rpc_failure(&error).cloned() {
        let attempts = retry_attempts(&error);
        let mut mapped = map_rpc_failure(failure);
        if let Some(attempts) = attempts {
            if let Value::Object(details) = &mut mapped.details {
                details.insert("attempts".to_owned(), json!(attempts));
            }
            if mapped.kind == agent::BackendErrorKind::Conflict {
                mapped.retryable = true;
            }
        }
        return mapped;
    }
    let kind = if object_failure(&error) {
        agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
    } else {
        match &error {
            ClientError::Protocol(_)
            | ClientError::ResponseMismatch(_)
            | ClientError::MissingCapabilities(_)
            | ClientError::ArtifactIntegrity(_)
            | ClientError::ArtifactReadFenceChanged => agent::BackendErrorKind::InvalidState,
            ClientError::Transport(_) => agent::BackendErrorKind::Other("Transport".to_owned()),
            ClientError::Object(_) | ClientError::ArtifactUpload(_) => unreachable!(),
            ClientError::InvalidOptions(_) | ClientError::InvalidRoute(_) => {
                agent::BackendErrorKind::InvalidState
            }
            ClientError::ArtifactPublishFailed { .. } | ClientError::RetryExhausted { .. } => {
                agent::BackendErrorKind::Other("ClientFailure".to_owned())
            }
            ClientError::Rpc(_) => unreachable!("RPC failures returned above"),
        }
    };
    let mut mapped = agent::BackendError::new(
        kind,
        error.to_string(),
        error.retryable(),
        json!({"source": "nokv-client"}),
    );
    if let Some(attempts) = retry_attempts(&error) {
        if let Value::Object(details) = &mut mapped.details {
            details.insert("attempts".to_owned(), json!(attempts));
        }
    }
    mapped
}

fn object_failure(error: &ClientError) -> bool {
    match error {
        ClientError::Object(_) | ClientError::ArtifactUpload(_) => true,
        ClientError::ArtifactPublishFailed { source, .. } => object_failure(source),
        ClientError::RetryExhausted { last_error, .. } => object_failure(last_error),
        _ => false,
    }
}

fn retry_attempts(error: &ClientError) -> Option<u32> {
    match error {
        ClientError::RetryExhausted { attempts, .. } => Some(*attempts),
        ClientError::ArtifactPublishFailed { source, .. } => retry_attempts(source),
        _ => None,
    }
}

fn map_snapshot_client_error(error: ClientError) -> agent::BackendError {
    if rpc_code(&error) == Some(wire::ErrorCode::NotFound) {
        return agent::BackendError::new(
            agent::BackendErrorKind::SnapshotNotFound,
            error.to_string(),
            false,
            json!({"source": "nokv-rpc"}),
        );
    }
    map_client_error(error)
}

fn map_restore_workflow_error(error: RestoreWorkflowError) -> agent::BackendError {
    if matches!(
        &error,
        RestoreWorkflowError::Prepare(source)
            if rpc_failure(source).is_some_and(|failure| {
                failure.code == wire::ErrorCode::NotFound
                    && failure.conflict == Some(wire::ConflictKind::SnapshotLifecycle)
            })
    ) {
        return agent::BackendError::new(
            agent::BackendErrorKind::SnapshotNotFound,
            error.to_string(),
            false,
            json!({"source": "nokv-rpc"}),
        );
    }
    map_client_error(error.into_client_error())
}

fn map_rpc_failure(failure: wire::RpcFailure) -> agent::BackendError {
    let kind = match failure.code {
        wire::ErrorCode::NotFound => agent::BackendErrorKind::NotFound,
        wire::ErrorCode::AlreadyExists => agent::BackendErrorKind::AlreadyExists,
        wire::ErrorCode::Conflict => agent::BackendErrorKind::Conflict,
        wire::ErrorCode::SnapshotExpired | wire::ErrorCode::SnapshotReaped => {
            agent::BackendErrorKind::SnapshotExpired
        }
        wire::ErrorCode::PreconditionFailed
            if failure.conflict == Some(wire::ConflictKind::SnapshotLifecycle)
                && failure.message.contains("active fork consumers") =>
        {
            agent::BackendErrorKind::ForkRetentionActive
        }
        wire::ErrorCode::InvalidArgument
        | wire::ErrorCode::PreconditionFailed
        | wire::ErrorCode::RequestReplayMismatch
        | wire::ErrorCode::CommitRetiring
        | wire::ErrorCode::OperationFailed
        | wire::ErrorCode::Quarantined => agent::BackendErrorKind::InvalidState,
        code => agent::BackendErrorKind::Other(error_code_name(code).to_owned()),
    };
    agent::BackendError::new(
        kind,
        failure.message,
        failure.retryable,
        json!({
            "source": "nokv-rpc",
            "code": error_code_name(failure.code),
            "conflict": failure.conflict.map(|value| format!("{value:?}")),
            "current_generation": failure.current_generation,
        }),
    )
}

fn error_code_name(code: wire::ErrorCode) -> &'static str {
    match code {
        wire::ErrorCode::InvalidArgument => "InvalidArgument",
        wire::ErrorCode::NotFound => "NotFound",
        wire::ErrorCode::AlreadyExists => "AlreadyExists",
        wire::ErrorCode::Conflict => "Conflict",
        wire::ErrorCode::PreconditionFailed => "PreconditionFailed",
        wire::ErrorCode::RequestReplayMismatch => "RequestReplayMismatch",
        wire::ErrorCode::NotOwner => "NotOwner",
        wire::ErrorCode::SnapshotExpired => "SnapshotExpired",
        wire::ErrorCode::SnapshotReaped => "SnapshotReaped",
        wire::ErrorCode::CommitRetiring => "CommitRetiring",
        wire::ErrorCode::OperationFailed => "OperationFailed",
        wire::ErrorCode::ObjectUnavailable => "ObjectUnavailable",
        wire::ErrorCode::Quarantined => "Quarantined",
        wire::ErrorCode::ResourceExhausted => "ResourceExhausted",
        wire::ErrorCode::Internal => "Internal",
    }
}

fn protocol_input(error: wire::ProtocolError) -> agent::BackendError {
    invalid_state("typed protocol input was rejected", error)
}

fn domain_input(error: impl ToString) -> agent::BackendError {
    invalid_backend_input(error.to_string())
}

fn invalid_backend_input(message: impl Into<String>) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::InvalidState,
        message,
        false,
        json!({"source": "workbench-backend"}),
    )
}

fn invalid_state(message: &str, error: impl ToString) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::InvalidState,
        format!("{message}: {}", error.to_string()),
        false,
        json!({"source": "workbench-backend"}),
    )
}

fn protocol_mismatch(message: impl Into<String>) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::InvalidState,
        message,
        false,
        json!({"source": "nokv-protocol"}),
    )
}

fn resource_exhausted(message: impl Into<String>) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::Other("ResourceExhausted".to_owned()),
        message,
        false,
        json!({"source": "workbench-backend"}),
    )
}

fn snapshot_expired(workbench_id: &WorkbenchId, snapshot_id: u64) -> agent::BackendError {
    agent::BackendError::new(
        agent::BackendErrorKind::SnapshotExpired,
        "snapshot is not alive",
        false,
        json!({
            "workbench_id": workbench_id.as_str(),
            "snapshot_id": snapshot_id,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Mutex;
    use std::thread;

    use nokv_client::{
        ClientOptions, FramedTcpOptions, FramedTcpTransport, RouteResolver, StaticRouteResolver,
        WorkspaceClient,
    };

    use super::*;

    fn test_route() -> wire::RootRoute {
        wire::RootRoute {
            root_id: wire::RootIdentity([1; 16]),
            logical_shard_id: wire::LogicalShardIdentity([2; 16]),
            object_namespace_id: wire::ObjectNamespaceIdentity([8; 16]),
            placement_generation: 3,
            owner_epoch: 4,
        }
    }

    fn read_version_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::PreconditionFailed,
            message: "read version changed".to_owned(),
            retryable: false,
            conflict: Some(wire::ConflictKind::ReadVersion),
            current_generation: None,
            route_hint: None,
        })
    }

    fn not_found_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::NotFound,
            message: "missing".to_owned(),
            retryable: false,
            conflict: None,
            current_generation: None,
            route_hint: None,
        })
    }

    fn already_exists_failure() -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Failure(wire::RpcFailure {
            code: wire::ErrorCode::AlreadyExists,
            message: "workbench already exists".to_owned(),
            retryable: false,
            conflict: Some(wire::ConflictKind::Workspace),
            current_generation: None,
            route_hint: None,
        })
    }

    fn success(result: wire::WorkspaceResult) -> wire::WorkspaceRpcOutcome {
        wire::WorkspaceRpcOutcome::Success(Box::new(result))
    }

    fn scripted_backend(
        outcomes: Vec<wire::WorkspaceRpcOutcome>,
    ) -> (
        CliWorkbenchBackend,
        Arc<Mutex<Vec<wire::WorkspaceRpcRequest>>>,
        thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let server = thread::spawn(move || {
            for outcome in outcomes {
                let (mut stream, _) = listener.accept().unwrap();
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).unwrap();
                let mut encoded = vec![0_u8; u32::from_be_bytes(length) as usize];
                stream.read_exact(&mut encoded).unwrap();
                let request = wire::decode_request(&encoded).unwrap();
                let response = wire::WorkspaceRpcResponse {
                    route: request.route,
                    request_id: request.request_id,
                    commit_version: None,
                    replayed: false,
                    outcome,
                };
                captured.lock().unwrap().push(request);
                let encoded = wire::encode_response(&response).unwrap();
                stream
                    .write_all(&u32::try_from(encoded.len()).unwrap().to_be_bytes())
                    .unwrap();
                stream.write_all(&encoded).unwrap();
                stream.flush().unwrap();
            }
        });

        let route = test_route();
        let resolver: Arc<dyn RouteResolver> =
            Arc::new(StaticRouteResolver::new(route, endpoint).unwrap());
        let client = WorkspaceClient::new(
            route.root_id,
            FramedTcpTransport::new(FramedTcpOptions::default()).unwrap(),
            resolver,
            ClientOptions { max_attempts: 1 },
        )
        .unwrap();
        let objects = Arc::new(
            CliObjectStore::build(&crate::cli::ObjectConfig {
                bucket: Some("unused-test-bucket".to_owned()),
                ..crate::cli::ObjectConfig::default()
            })
            .unwrap(),
        );
        (
            CliWorkbenchBackend::new(client, objects, 1024),
            requests,
            server,
        )
    }

    fn list_request(path: agent::ScopedPath, cursor: Option<String>) -> agent::ListRequest {
        agent::ListRequest {
            path,
            view: agent::ReadView::Live,
            cursor,
            limit: 1,
        }
    }

    fn scoped_root() -> agent::ScopedPath {
        agent::ScopedPath {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            section: None,
            relative_path: None,
        }
    }

    fn scoped_outputs() -> agent::ScopedPath {
        agent::ScopedPath {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            section: Some(agent::Section::Outputs),
            relative_path: None,
        }
    }

    fn wire_prefix(path: &str) -> wire::PathListEntry {
        wire_prefix_for("run-42", path)
    }

    fn wire_prefix_for(workbench: &str, path: &str) -> wire::PathListEntry {
        wire::PathListEntry::Prefix(wire::WorkspacePath {
            workbench: wire::WorkbenchName::new(workbench).unwrap(),
            path: wire::RelativePath::new(path).unwrap(),
        })
    }

    fn wire_artifact(path: &str) -> wire::PathListEntry {
        wire::PathListEntry::Artifact(wire::PathMetadata {
            path: wire::WorkspacePath {
                workbench: wire::WorkbenchName::new("run-42").unwrap(),
                path: wire::RelativePath::new(path).unwrap(),
            },
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            workspace_revision: 1,
            generation: 1,
            artifact_revision_id: wire::ArtifactRevisionIdentity([6; 16]),
            dependency_count: 0,
            dependency_depth: 0,
            descriptor: wire::ArtifactDescriptor {
                logical_size: 1,
                body_digest: wire::DigestUri::new(format!("sha256:{}", "07".repeat(32))).unwrap(),
                manifest_digest: wire::DigestUri::new(format!("sha256:{}", "08".repeat(32)))
                    .unwrap(),
                content_type: wire::ContentType::new("application/octet-stream").unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        })
    }

    fn workspace_summary(workbench: &str, incarnation: [u8; 16]) -> wire::WorkspaceSummary {
        wire::WorkspaceSummary {
            workbench: wire::WorkbenchName::new(workbench).unwrap(),
            workspace_incarnation_id: wire::WorkspaceIdentity(incarnation),
            workspace_revision: 0,
            commit_head: None,
            commit_head_generation: None,
        }
    }

    fn publish_request(condition: agent::PublishCondition) -> agent::PublishRequest {
        agent::PublishRequest {
            path: scoped_full_path(
                &WorkbenchId::new("implicit-publish").unwrap(),
                "outputs/result.json",
            )
            .unwrap(),
            body: br#"{"status":"ok"}"#.to_vec(),
            content_type: JSON_CONTENT_TYPE.to_owned(),
            condition,
        }
    }

    fn append_request() -> agent::AppendRequest {
        agent::AppendRequest {
            path: scoped_full_path(
                &WorkbenchId::new("implicit-append").unwrap(),
                "logs/events.jsonl",
            )
            .unwrap(),
            delta: b"{}\n".to_vec(),
            content_type: None,
            create_content_type: "application/jsonl".to_owned(),
            max_logical_size: None,
        }
    }

    fn catalog_field(field_id: &str) -> wire::CatalogField {
        wire::CatalogField {
            field_id: field_id.to_owned(),
            scalar_type: "string".to_owned(),
            operators: vec![wire::QueryOperator::Equal],
            sortable: true,
            facetable: true,
            aggregatable: false,
        }
    }

    fn commit_request() -> agent::CommitRequest {
        let workbench_id = WorkbenchId::new("clock-stable-run").unwrap();
        let canonical_manifest = agent::canonical_json_bytes(&json!({
            "model": "viking",
            "steps": [1, 2],
        }))
        .unwrap();
        let manifest_digest_uri = format!(
            "sha256:{}",
            encode_lowercase_hex(&Sha256::digest(&canonical_manifest))
        );
        let content_digest_uri = format!("sha256:{}", "ab".repeat(32));
        let stable_commit_id = agent::workbench_commit_identity(
            &workbench_id,
            &content_digest_uri,
            &manifest_digest_uri,
        );
        agent::CommitRequest {
            workbench_id,
            canonical_manifest,
            workbench_path: "/agents/test/wb/clock-stable-run".to_owned(),
            content_digest_uri,
            manifest_digest_uri,
            stable_commit_id,
            replace: false,
        }
    }

    fn digest_uri(bytes: &[u8]) -> wire::DigestUri {
        wire::DigestUri::new(format!(
            "sha256:{}",
            encode_lowercase_hex(&Sha256::digest(bytes))
        ))
        .unwrap()
    }

    fn terminal_commit_replay_fixture(
        request: &agent::CommitRequest,
    ) -> (
        wire::OperationStatus,
        wire::OperationStatus,
        wire::CommitRequest,
        CommitWorkflowIdentities,
    ) {
        let commit_id = wire::CommitIdentity(request.stable_commit_id);
        let identities = CommitWorkflowIdentities::derive(test_route().root_id, commit_id);
        let workbench = wire::WorkbenchName::new(request.workbench_id.as_str()).unwrap();
        let manifest_target = wire::WorkspacePath {
            workbench: workbench.clone(),
            path: wire::RelativePath::new(RUN_MANIFEST_PATH).unwrap(),
        };
        let committed_at_unix_seconds = 1_700_000_123;
        let manifest_bytes = canonical_commit_envelope(request, committed_at_unix_seconds).unwrap();
        let binding = wire::CommitManifestBinding {
            workspace_incarnation_id: wire::WorkspaceIdentity([5; 16]),
            artifact_revision_id: identities.tree_manifest_revision_id,
            descriptor: wire::ArtifactDescriptor {
                logical_size: manifest_bytes.len() as u64,
                body_digest: digest_uri(&manifest_bytes),
                manifest_digest: digest_uri(b"durable-manifest-plan"),
                content_type: wire::ContentType::new(JSON_CONTENT_TYPE).unwrap(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        };
        let exact_request = wire::CommitRequest {
            operation_id: identities.operation_id,
            workbench: workbench.clone(),
            workspace_incarnation_id: binding.workspace_incarnation_id,
            commit_id,
            content_digest: wire::DigestUri::new(request.content_digest_uri.clone()).unwrap(),
            manifest_digest: wire::DigestUri::new(request.manifest_digest_uri.clone()).unwrap(),
            projection_input_digest: commit_projection_input_digest(request),
            tree_manifest_revision_id: identities.tree_manifest_revision_id,
            replace: request.replace,
            run_manifest_condition: wire::PublishCondition::CreateOnly,
            expected_head_generation: None,
            parents: Vec::new(),
            producer: None,
            lineage_projection: Vec::new(),
        };
        let commit_status = wire::OperationStatus {
            token: wire::OperationToken {
                operation_id: identities.operation_id,
                state_digest: wire::Digest([0x61; 32]),
            },
            kind: wire::OperationKind::Commit,
            commit_preparation: Some(Box::new(wire::CommitPreparation {
                request: Box::new(exact_request.clone()),
                committed_at_unix_seconds,
                manifest: Some(binding.clone()),
            })),
            restore_preparation: None,
            state: wire::OperationState::Succeeded,
            progress: wire::OperationProgress {
                completed_rows: 3,
                total_rows: Some(3),
                completed_bytes: 0,
                total_bytes: None,
            },
            result: Some(wire::OperationResult::Commit(wire::CommitResult {
                operation_id: identities.operation_id,
                commit_id,
                workbench,
                head_generation: 1,
                member_count: 3,
                member_digest: wire::Digest([0x62; 32]),
            })),
            failure: None,
        };
        let publish_status = wire::OperationStatus {
            token: wire::OperationToken {
                operation_id: identities.manifest_publish_operation_id,
                state_digest: wire::Digest([0x63; 32]),
            },
            kind: wire::OperationKind::ArtifactPublish,
            commit_preparation: None,
            restore_preparation: None,
            state: wire::OperationState::Succeeded,
            progress: wire::OperationProgress {
                completed_rows: 1,
                total_rows: Some(1),
                completed_bytes: binding.descriptor.logical_size,
                total_bytes: Some(binding.descriptor.logical_size),
            },
            result: Some(wire::OperationResult::ArtifactPublish(
                wire::PublishResult {
                    operation_id: identities.manifest_publish_operation_id,
                    target: manifest_target,
                    workspace_revision: 1,
                    generation: 1,
                    artifact_revision_id: binding.artifact_revision_id,
                    logical_size: binding.descriptor.logical_size,
                    body_digest: binding.descriptor.body_digest,
                },
            )),
            failure: None,
        };
        (commit_status, publish_status, exact_request, identities)
    }

    fn running_commit_replay_fixture(
        request: &agent::CommitRequest,
    ) -> (
        wire::OperationStatus,
        wire::CommitRequest,
        CommitWorkflowIdentities,
    ) {
        let (mut status, _, exact_request, identities) = terminal_commit_replay_fixture(request);
        status.state = wire::OperationState::Running;
        status.result = None;
        status.commit_preparation.as_mut().unwrap().manifest = None;
        (status, exact_request, identities)
    }

    #[test]
    fn cursor_is_base64url_opaque_and_operation_bound() {
        let cursor = encode_cursor("search", b"opaque\0bytes");
        assert_eq!(decode_cursor("search", &cursor).unwrap(), b"opaque\0bytes");
        assert!(decode_cursor("find", &cursor).is_err());
        assert!(!cursor.contains("opaque"));
    }

    #[test]
    fn grep_cursor_is_bound_to_workbench_prefix_recursion_and_read_version() {
        let scope = scoped_outputs();
        let prefix = full_path_optional(&scope).unwrap().unwrap();
        let scope_digest = grep_scope_digest(&scope.workbench_id, Some(&prefix), true);
        let cursor = encode_grep_cursor(41, scope_digest, b"outputs/a.txt");

        assert_eq!(
            decode_grep_cursor(&cursor, scope_digest).unwrap(),
            GrepCursor {
                read_version: 41,
                server_cursor: b"outputs/a.txt".to_vec(),
            }
        );
        let non_recursive = grep_scope_digest(&scope.workbench_id, Some(&prefix), false);
        assert!(decode_grep_cursor(&cursor, non_recursive).is_err());
        assert!(
            decode_grep_cursor(&encode_cursor("grep", b"outputs/a.txt"), scope_digest).is_err()
        );
    }

    #[test]
    fn grep_continuation_sends_the_cursor_read_version_fence() {
        let scope = scoped_outputs();
        let prefix = full_path_optional(&scope).unwrap().unwrap();
        let scope_digest = grep_scope_digest(&scope.workbench_id, Some(&prefix), true);
        let cursor = encode_grep_cursor(41, scope_digest, b"outputs/a.txt");
        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_artifact("outputs/b.txt")],
                next_cursor: None,
                read_version: 41,
            }),
        )]);

        let page = agent::WorkbenchBackend::grep_candidates(
            &backend,
            agent::GrepCandidateRequest {
                scope,
                recursive: true,
                cursor: Some(cursor),
                limit: 1,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(page.candidates.len(), 1);
        assert_eq!(page.next_cursor, None);
        assert_eq!(
            decode_grep_cursor(&page.candidates[0].cursor_after, scope_digest).unwrap(),
            GrepCursor {
                read_version: 41,
                server_cursor: b"outputs/b.txt".to_vec(),
            }
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::ListPaths(list) = &requests[0].operation else {
            panic!("grep continuation must call list_paths");
        };
        assert_eq!(list.expected_read_version, Some(41));
        assert_eq!(
            list.page.cursor.as_deref(),
            Some(b"outputs/a.txt".as_slice())
        );
        assert!(list.recursive);
    }

    #[test]
    fn list_cursor_is_bound_to_workbench_prefix_view_and_breaks_the_old_schema() {
        let workbench = WorkbenchId::new("run-42").unwrap();
        let outputs = NormalizedRelativePath::new("outputs".to_owned()).unwrap();
        let anchor = NormalizedRelativePath::new("outputs/a".to_owned()).unwrap();
        let scope = list_scope_digest(&workbench, Some(&outputs), &agent::ReadView::Live);
        let cursor = encode_list_cursor(7, scope, &anchor);
        assert_eq!(
            decode_list_cursor(&cursor, scope).unwrap(),
            ListCursor {
                read_version: 7,
                anchor,
            }
        );

        let snapshot_scope = list_scope_digest(
            &workbench,
            Some(&outputs),
            &agent::ReadView::Snapshot(agent::SnapshotSelector::Name("checkpoint".to_owned())),
        );
        assert!(decode_list_cursor(&cursor, snapshot_scope).is_err());
        assert!(decode_list_cursor(&encode_cursor("list", b"outputs/a"), scope).is_err());
    }

    #[test]
    fn list_server_pages_track_only_the_union_lookahead() {
        assert_eq!(list_server_page_limit(1, 0, true), 2);
        assert_eq!(list_server_page_limit(10, 5, true), 11);
        assert_eq!(list_server_page_limit(1_000, 0, true), 1_000);
        assert_eq!(list_server_page_limit(1_000, 1_000, false), 1);
        assert_eq!(list_server_page_limit(10, 4, false), 7);
    }

    #[test]
    fn query_page_limits_match_the_execution_bound() {
        assert_eq!(query_page_limit(256).unwrap(), 256);
        assert!(query_page_limit(257).is_err());
        assert_eq!(page_limit(1_000).unwrap(), 1_000);
        assert_eq!(list_server_page_limit(1_000, 0, true), 1_000);
    }

    #[test]
    fn publish_implicitly_admits_a_missing_workbench_before_provider_use() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            already_exists_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "implicit-publish",
                [5; 16],
            ))),
        ]);

        let error = agent::WorkbenchBackend::publish(
            &backend,
            publish_request(agent::PublishCondition::CreateOnly),
        )
        .unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("not verified"));
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::CreateWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
    }

    #[test]
    fn append_implicitly_admits_a_missing_workbench_before_provider_use() {
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            already_exists_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "implicit-append",
                [6; 16],
            ))),
        ]);

        let error = agent::WorkbenchBackend::append(&backend, append_request()).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("not verified"));
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::CreateWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
    }

    #[test]
    fn commit_recovers_identity_before_implicitly_admitting_a_missing_workbench() {
        let request = commit_request();
        let (backend, requests, server) = scripted_backend(vec![
            not_found_failure(),
            not_found_failure(),
            already_exists_failure(),
            success(wire::WorkspaceResult::Workspace(workspace_summary(
                "clock-stable-run",
                [7; 16],
            ))),
            not_found_failure(),
            not_found_failure(),
            not_found_failure(),
        ]);

        let error = agent::WorkbenchBackend::commit(&backend, request).unwrap_err();
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::NotFound);

        let requests = requests.lock().unwrap();
        assert!(matches!(
            requests[0].operation,
            wire::WorkspaceRequest::GetOperation(_)
        ));
        assert!(matches!(
            requests[1].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[2].operation,
            wire::WorkspaceRequest::CreateWorkspace(_)
        ));
        assert!(matches!(
            requests[3].operation,
            wire::WorkspaceRequest::GetWorkspace(_)
        ));
        assert!(matches!(
            requests[4].operation,
            wire::WorkspaceRequest::GetPath(_)
        ));
        assert!(matches!(
            requests[5].operation,
            wire::WorkspaceRequest::GetOperation(_)
        ));
        assert!(matches!(
            requests[6].operation,
            wire::WorkspaceRequest::Commit(_)
        ));
    }

    #[test]
    fn manifest_literal_matching_is_ascii_case_insensitive_for_nested_json() {
        let manifest = json!({
            "Manifest": {
                "NestedKey": "MiXeD-Value",
                "unicode": "Straße",
            },
        });
        let canonical_manifest = agent::canonical_json_bytes(&manifest).unwrap();
        let request = |pattern: &str| agent::FindRequest {
            committed: None,
            manifest_pattern: Some(pattern.to_owned()),
            include_manifest: false,
            cursor: None,
            limit: 1,
        };

        assert!(find_request_matches_canonical_manifest(
            &request("MANIFEST"),
            Some(&canonical_manifest),
        ));
        assert!(find_request_matches_canonical_manifest(
            &request(r#""nestedkey":"mixed-value""#),
            Some(&canonical_manifest),
        ));
        assert!(!find_request_matches_canonical_manifest(
            &request("mixed.*value"),
            Some(&canonical_manifest),
        ));
        assert!(!find_request_matches_canonical_manifest(
            &request("STRASSE"),
            Some(&canonical_manifest),
        ));
        assert!(!find_request_matches_canonical_manifest(&request(""), None,));
    }

    #[test]
    fn manifest_literal_matching_is_independent_of_projection_and_page_cursor() {
        let manifest = json!({"manifest": {"Task": "DiFfErEnT"}});
        let canonical_manifest = agent::canonical_json_bytes(&manifest).unwrap();
        let page_two = encode_cursor("find", b"page-two");

        for include_manifest in [false, true] {
            for cursor in [None, Some(page_two.clone())] {
                let request = agent::FindRequest {
                    committed: Some(true),
                    manifest_pattern: Some("different".to_owned()),
                    include_manifest,
                    cursor: cursor.clone(),
                    limit: 1,
                };

                assert!(find_request_matches_canonical_manifest(
                    &request,
                    Some(&canonical_manifest),
                ));
                assert_eq!(request.include_manifest, include_manifest);
                assert_eq!(request.cursor, cursor);
            }
        }
        assert_eq!(decode_cursor("find", &page_two).unwrap(), b"page-two");
    }

    #[test]
    fn virtual_only_list_still_performs_one_authoritative_path_read() {
        let (backend, requests, server) = scripted_backend(vec![success(
            wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 41,
            }),
        )]);
        let page =
            agent::WorkbenchBackend::list(&backend, list_request(scoped_root(), None)).unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 41);
        assert_eq!(page.entries.len(), 1);
        assert!(page.next_cursor.is_some());
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::ListPaths(list) = &requests[0].operation else {
            panic!("only request must be list_paths");
        };
        assert_eq!(list.expected_read_version, None);
        assert!(!list.recursive);
        assert_eq!(list.page.limit, 2);
    }

    #[test]
    fn root_list_merges_authoritative_children_with_virtual_sections_without_skipping() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: ["a", "b", "c"].map(wire_prefix).to_vec(),
                next_cursor: Some(b"c".to_vec()),
                read_version: 41,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: ["c", "z"].map(wire_prefix).to_vec(),
                next_cursor: None,
                read_version: 41,
            })),
        ]);

        let mut request = list_request(scoped_root(), None);
        request.limit = 2;
        let first = agent::WorkbenchBackend::list(&backend, request.clone()).unwrap();
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.path.relative_path.as_ref().map(|path| path.as_str()))
                .collect::<Vec<_>>(),
            [Some("a"), Some("b")]
        );
        request.cursor = first.next_cursor;
        let second = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();
        assert_eq!(
            second
                .entries
                .iter()
                .map(|entry| {
                    entry
                        .path
                        .relative_path
                        .as_ref()
                        .map(|path| path.as_str())
                        .or_else(|| entry.path.section.map(agent::Section::as_str))
                })
                .collect::<Vec<_>>(),
            [Some("c"), Some("input")]
        );
        let requests = requests.lock().unwrap();
        let lists = requests
            .iter()
            .filter_map(|request| match &request.operation {
                wire::WorkspaceRequest::ListPaths(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lists.len(), 2);
        assert!(!lists[0].recursive);
        assert_eq!(lists[0].page.limit, 3);
        assert_eq!(lists[1].page.cursor.as_deref(), Some(b"b".as_slice()));
        assert_eq!(lists[1].page.limit, 3);
        assert_eq!(lists[1].expected_read_version, Some(41));
    }

    #[test]
    fn prefixed_list_exposes_direct_children_but_not_the_exact_prefix_artifact() {
        let (backend, _, server) = scripted_backend(vec![success(wire::WorkspaceResult::Paths(
            wire::PathPage {
                entries: vec![wire_prefix("outputs/child"), wire_artifact("outputs")],
                next_cursor: None,
                read_version: 41,
            },
        ))]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;
        let page = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();

        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].kind, agent::ArtifactKind::Directory);
        assert_eq!(
            page.entries[0]
                .path
                .relative_path
                .as_ref()
                .map(NormalizedRelativePath::as_str),
            Some("child")
        );
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn live_list_discards_a_drifted_attempt_and_restarts_from_page_one() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/a")],
                next_cursor: Some(b"page-2".to_vec()),
                read_version: 7,
            })),
            read_version_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: Vec::new(),
                next_cursor: None,
                read_version: 8,
            })),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;
        let page = agent::WorkbenchBackend::list(&backend, request).unwrap();
        server.join().unwrap();

        assert_eq!(page.read_version, 8);
        let requests = requests.lock().unwrap();
        let lists = requests
            .iter()
            .filter_map(|request| match &request.operation {
                wire::WorkspaceRequest::ListPaths(list) => Some(list),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lists.len(), 3);
        assert_eq!(lists[0].expected_read_version, None);
        assert_eq!(lists[1].expected_read_version, Some(7));
        assert_eq!(lists[1].page.cursor.as_deref(), Some(b"page-2".as_slice()));
        assert_eq!(lists[2].expected_read_version, None);
        assert!(lists[2].page.cursor.is_none());
    }

    #[test]
    fn list_rejects_a_server_cursor_cycle() {
        let (backend, _, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/a")],
                next_cursor: Some(b"cursor-a".to_vec()),
                read_version: 7,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/b")],
                next_cursor: Some(b"cursor-b".to_vec()),
                read_version: 7,
            })),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix("outputs/c")],
                next_cursor: Some(b"cursor-a".to_vec()),
                read_version: 7,
            })),
        ]);
        let mut request = list_request(scoped_outputs(), None);
        request.limit = 10;
        let error = agent::WorkbenchBackend::list(&backend, request).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("repeated cursor"));
    }

    #[test]
    fn implicit_directory_stat_rejects_a_foreign_workbench_entry() {
        let (backend, _, server) = scripted_backend(vec![
            not_found_failure(),
            success(wire::WorkspaceResult::Paths(wire::PathPage {
                entries: vec![wire_prefix_for("other-run", "outputs/missing/child")],
                next_cursor: None,
                read_version: 7,
            })),
        ]);
        let path = agent::ScopedPath {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            section: Some(agent::Section::Outputs),
            relative_path: Some(NormalizedRelativePath::new("missing").unwrap()),
        };
        let error =
            agent::WorkbenchBackend::stat(&backend, &path, &agent::ReadView::Live).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        assert!(error.message.contains("different workbench"));
    }

    #[test]
    fn user_list_cursor_staleness_fails_without_an_automatic_restart() {
        let path = scoped_outputs();
        let prefix = full_path_optional(&path).unwrap().unwrap();
        let scope = list_scope_digest(&path.workbench_id, Some(&prefix), &agent::ReadView::Live);
        let cursor = encode_list_cursor(
            7,
            scope,
            &NormalizedRelativePath::new("outputs/old".to_owned()).unwrap(),
        );
        let (backend, requests, server) = scripted_backend(vec![read_version_failure()]);
        let error =
            agent::WorkbenchBackend::list(&backend, list_request(path, Some(cursor))).unwrap_err();
        server.join().unwrap();

        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let wire::WorkspaceRequest::ListPaths(list) = &requests[0].operation else {
            panic!("only request must be list_paths");
        };
        assert_eq!(list.expected_read_version, Some(7));
        assert_eq!(list.page.cursor.as_deref(), Some(b"outputs/old".as_slice()));
    }

    #[test]
    fn catalog_uses_its_own_version_and_restarts_without_a_search_probe() {
        let (backend, requests, server) = scripted_backend(vec![
            success(wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: vec![catalog_field("old.field")],
                next_cursor: Some(b"page-2".to_vec()),
                read_version: 50,
            })),
            read_version_failure(),
            success(wire::WorkspaceResult::Catalog(wire::CatalogResult {
                fields: vec![catalog_field("fresh.field")],
                next_cursor: None,
                read_version: 51,
            })),
        ]);
        let result = agent::WorkbenchBackend::catalog(
            &backend,
            agent::CatalogRequest {
                scope: agent::QueryScope {
                    workbench_id: None,
                    section: None,
                    path: None,
                },
                field_prefix: None,
                include_facets: true,
            },
        )
        .unwrap();
        server.join().unwrap();

        assert_eq!(result.read_version, 51);
        assert_eq!(result.fields.len(), 1);
        assert_eq!(result.fields[0].field, "fresh.field");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests
            .iter()
            .all(|request| matches!(request.operation, wire::WorkspaceRequest::Catalog(_))));
        assert!(requests.iter().all(|request| {
            let wire::WorkspaceRequest::Catalog(catalog) = &request.operation else {
                return false;
            };
            catalog.page.limit == 256
        }));
    }

    #[test]
    fn component_safe_scope_joins_section_and_relative_path() {
        let scope = agent::QueryScope {
            workbench_id: Some(WorkbenchId::new("run-1").unwrap()),
            section: Some(agent::Section::Outputs),
            path: Some(NormalizedRelativePath::new("nested/result.json").unwrap()),
        };
        let wire::QueryScope::Workspace { path_prefix, .. } = query_scope(&scope).unwrap() else {
            panic!("workspace scope expected");
        };
        assert_eq!(path_prefix.unwrap().as_str(), "outputs/nested/result.json");
    }

    #[test]
    fn cli_manifest_shaping_uses_only_the_supplied_durable_time() {
        let request = commit_request();
        let first = canonical_commit_envelope(&request, 1_700_000_000).unwrap();
        let rebuilt_request = commit_request();
        let rebuilt = canonical_commit_envelope(&rebuilt_request, 1_700_000_000).unwrap();

        assert_eq!(rebuilt_request, request);
        assert_eq!(rebuilt, first);
        assert_eq!(
            agent::verify_run_manifest_v1(&rebuilt)
                .unwrap()
                .committed_at_unix_seconds,
            1_700_000_000
        );
        assert_ne!(
            canonical_commit_envelope(&request, 1_700_000_001).unwrap(),
            first
        );
    }

    #[test]
    fn commit_projection_input_digest_has_a_frozen_domain_and_complete_binding() {
        let request = commit_request();
        let digest = commit_projection_input_digest(&request);
        assert_eq!(
            digest,
            wire::Digest([
                177, 28, 208, 61, 143, 105, 88, 11, 59, 151, 249, 176, 235, 91, 217, 187, 131, 144,
                135, 15, 210, 231, 48, 3, 244, 156, 89, 16, 114, 31, 201, 250,
            ])
        );

        let mut changes = Vec::new();
        let mut changed = request.clone();
        changed.workbench_id = WorkbenchId::new("different-workbench").unwrap();
        changes.push(changed);
        let mut changed = request.clone();
        changed.workbench_path = "/agents/test/wb/different".to_owned();
        changes.push(changed);
        let mut changed = request.clone();
        changed.content_digest_uri = format!("sha256:{}", "cd".repeat(32));
        changes.push(changed);
        let mut changed = request.clone();
        changed.canonical_manifest = b"{\"different\":true}".to_vec();
        changes.push(changed);
        let mut changed = request.clone();
        changed.manifest_digest_uri = format!("sha256:{}", "ef".repeat(32));
        changes.push(changed);
        let mut changed = request.clone();
        changed.stable_commit_id[0] ^= 0xff;
        changes.push(changed);
        assert!(changes
            .iter()
            .all(|changed| commit_projection_input_digest(changed) != digest));

        let mut admission_only = request;
        admission_only.replace = !admission_only.replace;
        assert_eq!(commit_projection_input_digest(&admission_only), digest);
    }

    #[test]
    fn terminal_commit_recovery_never_reads_the_live_workspace_or_manifest() {
        for replace in [false, true] {
            let mut request = commit_request();
            request.replace = replace;
            let (commit_status, publish_status, exact_request, identities) =
                terminal_commit_replay_fixture(&request);
            let (backend, requests, server) = scripted_backend(vec![
                success(wire::WorkspaceResult::Operation(commit_status.clone())),
                success(wire::WorkspaceResult::Operation(commit_status)),
                success(wire::WorkspaceResult::Operation(publish_status)),
            ]);

            let outcome = agent::WorkbenchBackend::commit(&backend, request).unwrap();
            server.join().unwrap();
            assert!(outcome.idempotent_replay);
            assert_eq!(outcome.generation, 1);

            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 3);
            assert!(matches!(
                &requests[0].operation,
                wire::WorkspaceRequest::GetOperation(request)
                    if request.operation_id == identities.operation_id
            ));
            assert!(matches!(
                &requests[1].operation,
                wire::WorkspaceRequest::Commit(request) if request == &exact_request
            ));
            assert!(matches!(
                &requests[2].operation,
                wire::WorkspaceRequest::GetOperation(request)
                    if request.operation_id == identities.manifest_publish_operation_id
            ));
            assert!(requests.iter().all(|request| !matches!(
                request.operation,
                wire::WorkspaceRequest::GetWorkspace(_) | wire::WorkspaceRequest::GetPath(_)
            )));
        }
    }

    #[test]
    fn running_commit_recovery_rejects_a_changed_presentation_path_before_publish() {
        let original = commit_request();
        let (running, exact_request, identities) = running_commit_replay_fixture(&original);
        let mut changed = original.clone();
        changed.workbench_path = "/agents/test/wb/different-root".to_owned();
        assert_eq!(changed.stable_commit_id, original.stable_commit_id);
        assert_ne!(
            commit_projection_input_digest(&changed),
            exact_request.projection_input_digest
        );
        let (backend, requests, server) =
            scripted_backend(vec![success(wire::WorkspaceResult::Operation(running))]);

        let error = agent::WorkbenchBackend::commit(&backend, changed).unwrap_err();
        server.join().unwrap();
        assert_eq!(error.kind, agent::BackendErrorKind::InvalidState);

        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert!(matches!(
            &requests[0].operation,
            wire::WorkspaceRequest::GetOperation(request)
                if request.operation_id == identities.operation_id
        ));
    }

    #[test]
    fn all_query_operand_shapes_map_without_stringly_operators() {
        let predicate = agent::QueryPredicate {
            field: "producer".to_owned(),
            operator: agent::PredicateOperator::In,
            value: Some(agent::QueryValue::List(vec![
                agent::QueryValue::String("agent".to_owned()),
                agent::QueryValue::Unsigned(3),
            ])),
        };
        let mapped = query_predicate(&predicate).unwrap();
        assert_eq!(mapped.operator, wire::QueryOperator::In);
        assert!(matches!(mapped.operand, wire::QueryOperand::Set(values) if values.len() == 2));
    }

    #[test]
    fn exhausted_sdk_append_conflict_remains_a_retryable_workbench_conflict() {
        let mapped = map_client_error(ClientError::RetryExhausted {
            attempts: 5,
            last_error: Box::new(ClientError::Rpc(wire::RpcFailure {
                code: wire::ErrorCode::Conflict,
                message: "append generation changed".to_owned(),
                retryable: false,
                conflict: Some(wire::ConflictKind::PathGeneration),
                current_generation: Some(9),
                route_hint: None,
            })),
        });
        assert_eq!(mapped.kind, agent::BackendErrorKind::Conflict);
        assert!(mapped.retryable);
        assert_eq!(mapped.details["attempts"], 5);
        assert_eq!(mapped.details["current_generation"], 9);
    }

    #[test]
    fn exhausted_temporary_object_failure_remains_typed_retryable_and_redacted() {
        let mapped = map_client_error(ClientError::RetryExhausted {
            attempts: 3,
            last_error: Box::new(ClientError::Object(
                nokv_object::ObjectError::backend_failure(
                    "endpoint=http://127.0.0.1:9000 bucket=private",
                    true,
                ),
            )),
        });
        assert_eq!(
            mapped.kind,
            agent::BackendErrorKind::Other("ObjectUnavailable".to_owned())
        );
        assert!(mapped.retryable);
        assert_eq!(mapped.details["attempts"], 3);
        assert!(!mapped.message.contains("127.0.0.1"));
        assert!(!mapped.message.contains("private"));
    }

    #[test]
    fn restore_not_found_is_snapshot_specific_only_for_prepare_lifecycle_failures() {
        let prepare_not_found = |conflict| {
            RestoreWorkflowError::Prepare(ClientError::Rpc(wire::RpcFailure {
                code: wire::ErrorCode::NotFound,
                message: "missing durable restore input".to_owned(),
                retryable: false,
                conflict: Some(conflict),
                current_generation: None,
                route_hint: None,
            }))
        };
        assert_eq!(
            map_restore_workflow_error(prepare_not_found(wire::ConflictKind::SnapshotLifecycle))
                .kind,
            agent::BackendErrorKind::SnapshotNotFound
        );
        assert_eq!(
            map_restore_workflow_error(prepare_not_found(wire::ConflictKind::Workspace)).kind,
            agent::BackendErrorKind::NotFound
        );
        assert_eq!(
            map_restore_workflow_error(prepare_not_found(wire::ConflictKind::OperationState)).kind,
            agent::BackendErrorKind::NotFound
        );
    }
}
