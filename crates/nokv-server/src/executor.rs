/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_meta::workspace as meta;
use nokv_protocol as protocol;
use nokv_types as types;
use sha2::{Digest as _, Sha256};

use crate::{ExecutedRequest, WorkspaceRequestExecutor};

const MANIFEST_SCAN_ROWS: usize = 256;
const MANIFEST_PLAN_CURSOR_VERSION: u8 = 1;
const MANIFEST_PLAN_CURSOR_BYTES: usize = 1 + types::FIXED_ID_BYTES * 2 + 8 * 3;
const PUBLISH_ACTIVITY_LEASE_MS: u64 = 30 * 60 * 1_000;
const RUN_MANIFEST_PATH: &str = "metadata/run_manifest.json";
const _: () = assert!(protocol::MAX_ARTIFACT_DEPENDENCY_OWNERS == meta::MAX_REVISION_DEPENDENCIES);
const _: () =
    assert!(protocol::MAX_ARTIFACT_DEPENDENCY_DEPTH == meta::MAX_REVISION_DEPENDENCY_DEPTH);
const _: () =
    assert!(protocol::PageRequest::MAX_LIMIT as usize == meta::MAX_VISIBLE_PATH_LIST_PAGE_SIZE);
const SUPPORTED_WORKSPACE_CAPABILITIES: [protocol::WorkspaceCapability; 9] = [
    protocol::WorkspaceCapability::ArtifactPublishV1,
    protocol::WorkspaceCapability::ArtifactRangeReadV1,
    protocol::WorkspaceCapability::ChangeFeedV1,
    protocol::WorkspaceCapability::CommitV1,
    protocol::WorkspaceCapability::QueryV1,
    protocol::WorkspaceCapability::RestoreV1,
    protocol::WorkspaceCapability::SnapshotLeaseV1,
    protocol::WorkspaceCapability::WorkspaceLifecycleV1,
    protocol::WorkspaceCapability::WorkspacePathV1,
];

/// Storage-neutral protocol adapter over one authoritative Holt metadata shard.
///
/// The adapter owns DTO conversion and exact RPC request binding only. Durable
/// lifecycle transitions remain in `nokv-meta`.
#[derive(Clone)]
pub struct MetadataWorkspaceRequestExecutor {
    store: Arc<meta::AgentMetadataStore>,
}

impl MetadataWorkspaceRequestExecutor {
    pub fn new(store: Arc<meta::AgentMetadataStore>) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &Arc<meta::AgentMetadataStore> {
        &self.store
    }

    fn execute_request(
        &self,
        request: &protocol::WorkspaceRpcRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        request
            .validate()
            .map_err(|error| invalid_argument(error.to_string()))?;
        match &request.operation {
            protocol::WorkspaceRequest::Preflight(preflight) => Self::preflight(request, preflight),
            protocol::WorkspaceRequest::CreateWorkspace(create) => {
                self.create_workspace(request, create)
            }
            protocol::WorkspaceRequest::GetWorkspace(get) => self.get_workspace(request, get),
            protocol::WorkspaceRequest::GetPath(get) => self.get_path(request, get),
            protocol::WorkspaceRequest::ListPaths(list) => self.list_paths(request, list),
            protocol::WorkspaceRequest::RemovePath(remove) => self.remove_path(request, remove),
            protocol::WorkspaceRequest::BeginArtifactPublish(begin) => {
                self.begin_artifact_publish(request, begin)
            }
            protocol::WorkspaceRequest::StageArtifactObjects(stage) => {
                self.stage_artifact_objects(request, stage)
            }
            protocol::WorkspaceRequest::MarkArtifactObjectsUploaded(mark) => {
                self.mark_artifact_objects_uploaded(request, mark)
            }
            protocol::WorkspaceRequest::StageArtifactManifest(stage) => {
                self.stage_artifact_manifest(request, stage)
            }
            protocol::WorkspaceRequest::CompleteArtifactPublish(complete) => {
                self.complete_artifact_publish(request, complete)
            }
            protocol::WorkspaceRequest::AbortArtifactPublish(abort) => {
                self.abort_artifact_publish(request, abort)
            }
            protocol::WorkspaceRequest::ReconcileQuarantinedArtifactPublish(reconcile) => {
                self.reconcile_quarantined_artifact_publish(request, reconcile)
            }
            protocol::WorkspaceRequest::Commit(commit) => self.commit(request, commit),
            protocol::WorkspaceRequest::GetSnapshot(get) => self.get_snapshot(request, get),
            protocol::WorkspaceRequest::MintSnapshot(mint) => self.mint_snapshot(request, mint),
            protocol::WorkspaceRequest::RenewSnapshot(renew) => self.renew_snapshot(request, renew),
            protocol::WorkspaceRequest::RetireSnapshot(retire) => {
                self.retire_snapshot(request, retire)
            }
            protocol::WorkspaceRequest::ListSnapshots(list) => self.list_snapshots(request, list),
            protocol::WorkspaceRequest::PrepareRestore(prepare) => {
                self.prepare_restore(request, prepare)
            }
            protocol::WorkspaceRequest::FinalizeRestore(finalize) => {
                self.finalize_restore(request, finalize)
            }
            protocol::WorkspaceRequest::GetOperation(get) => self.get_operation(request, get),
            protocol::WorkspaceRequest::Search(search) => self.search(request, search),
            protocol::WorkspaceRequest::Aggregate(aggregate) => self.aggregate(request, aggregate),
            protocol::WorkspaceRequest::Catalog(catalog) => self.catalog(request, catalog),
            protocol::WorkspaceRequest::FindWorkspaces(find) => self.find_workspaces(request, find),
            protocol::WorkspaceRequest::ReadChanges(changes) => self.read_changes(request, changes),
        }
    }

    fn preflight(
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::WorkspacePreflightRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        if request.required_capabilities.iter().any(|required| {
            SUPPORTED_WORKSPACE_CAPABILITIES
                .binary_search(required)
                .is_err()
        }) {
            return Err(failure(
                protocol::ErrorCode::PreconditionFailed,
                "the current workspace owner does not support every required capability",
                false,
                None,
            ));
        }
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Preflight(protocol::WorkspacePreflightResult::new(
                rpc.route,
                SUPPORTED_WORKSPACE_CAPABILITIES,
            )),
            commit_version: None,
            replayed: false,
        })
    }

    fn create_workspace(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::CreateWorkspaceRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"create-workspace", 0);
        let context = self.write_context(rpc.route, step_id)?;
        let workbench = workbench_id(&request.workbench)?;
        let outcome = meta::create_visible_workspace(
            &self.store,
            context,
            &workbench,
            request.workspace_incarnation_id.into(),
        )
        .map_err(namespace_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Workspace(protocol::WorkspaceSummary {
                workbench: request.workbench.clone(),
                workspace_incarnation_id: outcome.workspace.incarnation_id.into(),
                workspace_revision: outcome.workspace.workspace_revision.get(),
                commit_head: None,
                commit_head_generation: None,
            }),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn get_workspace(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::GetWorkspaceRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let workspace = meta::get_workspace_at(
            &self.store,
            self.read_context(rpc.route)?,
            &workbench_id(&request.workbench)?,
        )
        .map_err(query_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Workspace(protocol::WorkspaceSummary {
                workbench: request.workbench.clone(),
                workspace_incarnation_id: workspace.workspace_incarnation_id.into(),
                workspace_revision: workspace.workspace_revision.get(),
                commit_head: workspace.commit_id.map(Into::into),
                commit_head_generation: workspace
                    .commit_head_generation
                    .map(types::Generation::get),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn get_path(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::GetPathRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let workbench = workbench_id(&request.target.workbench)?;
        let path = relative_path(&request.target.path)?;
        let resolved = if matches!(&request.view, protocol::WorkspaceReadView::Live) {
            let route = route_parts(rpc.route)?;
            meta::get_current_visible_workspace_path(
                &self.store,
                route.root_id,
                route.placement_generation,
                route.owner_epoch,
                &workbench,
                &path,
            )
        } else {
            let context = self.workspace_read_context(rpc.route, &workbench, &request.view)?;
            meta::get_visible_workspace_path_at(&self.store, context, &workbench, &path)
        }
        .map_err(namespace_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        let Some(entry) = resolved.entry else {
            return Err(not_found("path does not exist"));
        };
        let context = resolved.context;
        let workspace = resolved.workspace;
        if request.if_none_match == Some(entry.generation.get()) {
            return Ok(ExecutedRequest {
                result: protocol::WorkspaceResult::Path(protocol::PathReadResult {
                    not_modified: true,
                    metadata: None,
                    range: None,
                    blocks: Vec::new(),
                    next_cursor: None,
                }),
                commit_version: None,
                replayed: false,
            });
        }
        let metadata = Self::path_metadata(&request.target.workbench, workspace, path, entry)?;
        let (blocks, next_cursor) = match request.range {
            None => (Vec::new(), None),
            Some(range) => {
                let end = range
                    .offset
                    .checked_add(range.length)
                    .expect("protocol validation rejected range overflow");
                if end > metadata.descriptor.logical_size {
                    return Err(invalid_argument(
                        "requested byte range exceeds the artifact logical size",
                    ));
                }
                self.manifest_read_plan(
                    context,
                    metadata.artifact_revision_id.into(),
                    range,
                    request
                        .plan_page
                        .as_ref()
                        .expect("protocol validation requires ranged plan pagination"),
                )?
            }
        };
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Path(protocol::PathReadResult {
                not_modified: false,
                metadata: Some(metadata),
                range: request.range,
                blocks,
                next_cursor,
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn list_paths(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::ListPathsRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let workbench = workbench_id(&request.workbench)?;
        let context = self.workspace_read_context(rpc.route, &workbench, &request.view)?;
        if request
            .expected_read_version
            .is_some_and(|expected| expected != context.read_version.get())
        {
            return Err(failure(
                protocol::ErrorCode::PreconditionFailed,
                format!(
                    "list_paths expected read version {} does not match resolved read version {}",
                    request.expected_read_version.expect("checked as present"),
                    context.read_version.get()
                ),
                false,
                Some(protocol::ConflictKind::ReadVersion),
            ));
        }
        let workspace = meta::get_visible_workspace_at(&self.store, context, &workbench)
            .map_err(namespace_failure)?
            .ok_or_else(|| not_found("workbench does not exist"))?;
        let prefix = request.prefix.as_ref().map(relative_path).transpose()?;
        let marker = request
            .page
            .cursor
            .as_deref()
            .map(decode_path_cursor)
            .transpose()?;
        let wanted =
            usize::try_from(request.page.limit).expect("protocol page limit always fits usize");
        let page = meta::list_paths_at_visible_workspace(
            &self.store,
            context,
            &workspace,
            prefix.as_ref(),
            request.recursive,
            marker.as_ref(),
            wanted,
        )
        .map_err(namespace_failure)?;
        let next_cursor = page
            .next_marker
            .map(|marker| marker.as_str().as_bytes().to_vec());
        let mut entries = Vec::with_capacity(page.entries.len());
        for visible in page.entries {
            entries.push(match visible.entry {
                Some(entry) => protocol::PathListEntry::Artifact(Self::path_metadata(
                    &request.workbench,
                    workspace,
                    visible.path,
                    entry,
                )?),
                None => protocol::PathListEntry::Prefix(protocol::WorkspacePath {
                    workbench: request.workbench.clone(),
                    path: protocol::RelativePath::new(visible.path.as_str())
                        .map_err(|error| internal(error.to_string()))?,
                }),
            });
        }
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Paths(protocol::PathPage {
                entries,
                next_cursor,
                read_version: context.read_version.get(),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn remove_path(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::RemovePathRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"remove-path", 0);
        let outcome = meta::remove_path(
            &self.store,
            meta::RemovePathRequest {
                context: self.write_context(rpc.route, step_id)?,
                workbench_id: workbench_id(&request.target.workbench)?,
                path: relative_path(&request.target.path)?,
                expected_generation: types::Generation::new(request.expected_generation)
                    .map_err(|error| invalid_argument(error.to_string()))?,
            },
        )
        .map_err(remove_path_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Removed(protocol::RemovePathResult {
                removed: true,
                workspace_revision: outcome.workspace_revision.get(),
                removed_artifact_revision_id: Some(outcome.removed_artifact_revision_id.into()),
            }),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn get_snapshot(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::GetSnapshotRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let workbench_id = workbench_id(&request.workbench)?;
        let workspace = meta::get_visible_workspace_at(&self.store, context, &workbench_id)
            .map_err(namespace_failure)?
            .ok_or_else(|| not_found("workbench does not exist"))?;
        let snapshot = meta::get_snapshot_at(
            &self.store,
            context,
            &workbench_id,
            &snapshot_selector(&request.selector)?,
        )
        .map_err(snapshot_failure)?
        .ok_or_else(|| not_found("snapshot does not exist for the visible workbench"))?;
        let lease_clock = self
            .store
            .lease_clock_high_water()
            .map_err(engine_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Snapshot(snapshot_result(
                &request.workbench,
                workspace.incarnation_id,
                snapshot,
                lease_clock,
            )?),
            commit_version: None,
            replayed: false,
        })
    }

    fn mint_snapshot(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::MintSnapshotRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"snapshot-mint", 0);
        let context = self.write_context(rpc.route, step_id)?;
        let workbench_id = workbench_id(&request.workbench)?;
        let workspace = meta::get_visible_workspace_at(
            &self.store,
            read_context_from_write(context),
            &workbench_id,
        )
        .map_err(namespace_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        if workspace.incarnation_id
            != types::WorkspaceIncarnationId::from(request.workspace_incarnation_id)
        {
            return Err(conflict(
                protocol::ConflictKind::Workspace,
                "workspace incarnation does not match",
                None,
            ));
        }
        let outcome = meta::mint_snapshot(
            &self.store,
            context,
            &meta::MintSnapshotRequest {
                workbench_id,
                snapshot_id: types::SnapshotId::new(request.snapshot_id),
                alias: request
                    .alias
                    .as_ref()
                    .map(|alias| types::SnapshotAliasName::new(alias.as_str()))
                    .transpose()
                    .map_err(|error| invalid_argument(error.to_string()))?,
                lease_deadline_ms: request.lease_deadline_ms,
                annotation: request.annotation.clone(),
            },
        )
        .map_err(snapshot_failure)?;
        let lease_clock = self
            .store
            .lease_clock_high_water()
            .map_err(engine_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Snapshot(snapshot_result(
                &request.workbench,
                workspace.incarnation_id,
                outcome.snapshot,
                lease_clock,
            )?),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn renew_snapshot(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::RenewSnapshotRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"snapshot-renew", 0);
        let context = self.write_context(rpc.route, step_id)?;
        let workbench_id = workbench_id(&request.workbench)?;
        let workspace = meta::get_visible_workspace_at(
            &self.store,
            read_context_from_write(context),
            &workbench_id,
        )
        .map_err(namespace_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        let outcome = meta::renew_snapshot(
            &self.store,
            context,
            &meta::RenewSnapshotRequest {
                workbench_id,
                selector: snapshot_selector(&request.selector)?,
                lease_deadline_ms: request.lease_deadline_ms,
            },
        )
        .map_err(snapshot_failure)?;
        let lease_clock = self
            .store
            .lease_clock_high_water()
            .map_err(engine_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Snapshot(snapshot_result(
                &request.workbench,
                workspace.incarnation_id,
                outcome.snapshot,
                lease_clock,
            )?),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn retire_snapshot(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::RetireSnapshotRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"snapshot-retire", 0);
        let context = self.write_context(rpc.route, step_id)?;
        let workbench_id = workbench_id(&request.workbench)?;
        let workspace = meta::get_visible_workspace_at(
            &self.store,
            read_context_from_write(context),
            &workbench_id,
        )
        .map_err(namespace_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        let outcome = meta::retire_snapshot(
            &self.store,
            context,
            &meta::RetireSnapshotRequest {
                workbench_id,
                selector: snapshot_selector(&request.selector)?,
                retire_annotation: request.retire_annotation.clone(),
            },
        )
        .map_err(snapshot_failure)?;
        let lease_clock = self
            .store
            .lease_clock_high_water()
            .map_err(engine_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Snapshot(snapshot_result(
                &request.workbench,
                workspace.incarnation_id,
                outcome.snapshot,
                lease_clock,
            )?),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn list_snapshots(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::ListSnapshotsRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let page = meta::list_snapshots_at(
            &self.store,
            context,
            &workbench_id(&request.workbench)?,
            request.page.cursor.as_deref(),
            query_limit(request.page.limit),
        )
        .map_err(snapshot_list_failure)?;
        let lease_clock = self
            .store
            .lease_clock_high_water()
            .map_err(engine_failure)?;
        let snapshots = page
            .snapshots
            .into_iter()
            .map(|snapshot| {
                snapshot_result(
                    &request.workbench,
                    page.workspace_incarnation_id,
                    snapshot,
                    lease_clock,
                )
            })
            .collect::<Result<_, _>>()?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Snapshots(protocol::SnapshotPage {
                snapshots,
                next_cursor: page.next_cursor,
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn prepare_restore(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::PrepareRestoreRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let source_workbench = workbench_id(&request.source_workbench)?;
        let source = match &request.source {
            protocol::RestoreSource::Snapshot(protocol::SnapshotSelector::Id(snapshot_id)) => {
                meta::RestoreSourceSelector::Snapshot(types::SnapshotId::new(*snapshot_id))
            }
            protocol::RestoreSource::Snapshot(protocol::SnapshotSelector::Alias(_)) => {
                return Err(invalid_argument(
                    "restore preparation requires a concrete snapshot id",
                ));
            }
            protocol::RestoreSource::Commit(commit_id) => {
                meta::RestoreSourceSelector::Commit((*commit_id).into())
            }
        };
        let begin_id = derived_request_id(rpc.request_id, b"restore-begin", 0);
        let mut outcome = meta::begin_restore(
            &self.store,
            self.write_context(rpc.route, begin_id)?,
            &meta::BeginRestoreRequest {
                source_workbench_id: source_workbench,
                expected_source_workspace_incarnation_id: request
                    .source_workspace_incarnation_id
                    .into(),
                source,
                destination_workbench_id: workbench_id(&request.destination_workbench)?,
                destination_workspace_incarnation_id: request
                    .destination_workspace_incarnation_id
                    .into(),
                restore_manifest: meta::RestoreManifestDescriptor {
                    body_digest_uri: request.restore_manifest.body_digest.as_str().to_owned(),
                    logical_size: request.restore_manifest.logical_size,
                    content_type: request.restore_manifest.content_type.as_str().to_owned(),
                },
            },
        )
        .map_err(restore_failure)?;

        if matches!(
            outcome.operation.phase,
            types::RestorePhase::Cleaned | types::RestorePhase::Quarantined
        ) {
            return Err(restore_operation_status(&outcome.operation)?
                .failure
                .ok_or_else(|| internal("terminal restore status omitted its durable failure"))?);
        }

        if outcome.operation.phase == types::RestorePhase::Preparing {
            outcome = meta::start_restore_copy(
                &self.store,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-start-copy", 0),
                )?,
                meta::RestoreOperationRequest {
                    operation_id: outcome.operation.operation_id,
                },
            )
            .map_err(restore_failure)?;
        }

        let mut batch = 0_u64;
        while outcome.operation.phase == types::RestorePhase::Copying
            && !outcome.operation.source_eof
        {
            let before = outcome.operation.next_member_sequence;
            let copied = meta::copy_restore_batch(
                &self.store,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-copy", batch),
                )?,
                meta::CopyRestoreBatchRequest {
                    operation_id: outcome.operation.operation_id,
                    limit: meta::MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .map_err(restore_failure)?;
            outcome = copied.command;
            if !outcome.operation.source_eof && outcome.operation.next_member_sequence == before {
                return Err(internal("restore copier made no durable progress"));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("restore batch counter overflow"))?;
        }
        if outcome.operation.phase == types::RestorePhase::Copying && outcome.operation.source_eof {
            outcome = meta::seal_restore_source(
                &self.store,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-seal-source", 0),
                )?,
                meta::RestoreOperationRequest {
                    operation_id: outcome.operation.operation_id,
                },
            )
            .map_err(restore_failure)?;
        }
        let preparation = sealed_restore_preparation(&outcome.operation)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::RestorePrepared(preparation),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn finalize_restore(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::FinalizeRestoreRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let operation_id: types::OperationId = request.operation_id.into();
        let applied = meta::apply_restore_initialization(
            &self.store,
            self.write_context(
                rpc.route,
                derived_request_id(rpc.request_id, b"restore-apply-initialization", 0),
            )?,
            meta::RestoreOperationRequest { operation_id },
        )
        .map_err(restore_failure)?;
        if applied.operation.phase != types::RestorePhase::Ready {
            return Err(operation_terminal_failure(
                "restore initialization did not reach Ready",
            ));
        }
        let completed = meta::complete_restore(
            &self.store,
            self.write_context(
                rpc.route,
                derived_request_id(rpc.request_id, b"restore-complete", 0),
            )?,
            meta::RestoreOperationRequest { operation_id },
        )
        .map_err(restore_failure)?;
        let result = completed.result;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Restored(protocol::RestoreResult {
                operation_id: request.operation_id,
                destination: protocol::WorkspaceSummary {
                    workbench: protocol_workbench(
                        &completed.command.operation.destination_workbench_id,
                    )?,
                    workspace_incarnation_id: result.destination_workspace_incarnation_id.into(),
                    workspace_revision: result.destination_workspace_revision.get(),
                    commit_head: None,
                    commit_head_generation: None,
                },
                member_count: result.member_count,
                member_digest: protocol::Digest(result.member_digest),
                metadata_rows_copied: result.member_count,
                object_bytes_copied: 0,
            }),
            commit_version: Some(completed.command.commit_version.get()),
            replayed: completed.command.replayed,
        })
    }

    fn search(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::SearchRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let (scope, path_prefix) = query_scope(&request.scope)?;
        let query = meta::SearchRequest {
            scope,
            path_prefix,
            predicates: query_predicates(&request.predicates)?,
            projection: query_field_ids(&request.projection)?,
            sort: query_sort(&request.sort)?,
            facets: query_field_ids(&request.facets)?,
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::search_paths_at(&self.store, context, &query).map_err(query_failure)?;
        let mut hits = Vec::with_capacity(page.hits.len());
        for hit in page.hits {
            let workbench = protocol_workbench(&hit.workbench_id)?;
            let workspace = meta::get_visible_workspace_at(&self.store, context, &hit.workbench_id)
                .map_err(namespace_failure)?
                .ok_or_else(|| internal("query hit references an invisible workbench"))?;
            let visible =
                meta::get_path_at_visible_workspace(&self.store, context, &workspace, &hit.path)
                    .map_err(namespace_failure)?
                    .ok_or_else(|| internal("query hit references an invisible path"))?;
            if visible.generation != hit.generation
                || visible.body_digest_uri != hit.body_digest_uri
                || visible.logical_size != hit.logical_size
                || visible.content_type != hit.content_type
                || visible.producer != hit.producer
                || visible.manifest_id != hit.manifest_id
            {
                return Err(internal(
                    "query hit does not match its authoritative visible path",
                ));
            }
            hits.push(protocol::SearchHit {
                metadata: Self::path_metadata(&workbench, workspace, hit.path, visible)?,
                projection: protocol_field_values(hit.projection),
            });
        }
        let facets = page
            .facets
            .into_iter()
            .map(|facet| protocol::FacetResult {
                field_id: facet.field_id.as_str().to_owned(),
                buckets: facet
                    .buckets
                    .into_iter()
                    .map(|bucket| protocol::FacetBucket {
                        value: protocol_scalar(bucket.value),
                        count: bucket.count,
                    })
                    .collect(),
                distinct_count: facet.distinct_count,
                truncated: facet.truncated,
            })
            .collect();
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Search(protocol::SearchResult {
                hits,
                facets,
                next_cursor: page.next_cursor,
                read_version: page.read_version.get(),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn aggregate(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::AggregateRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let (scope, path_prefix) = query_scope(&request.scope)?;
        let query = meta::AggregateRequest {
            scope,
            path_prefix,
            predicates: query_predicates(&request.predicates)?,
            group_by: query_field_ids(&request.group_by)?,
            aggregates: request
                .aggregates
                .iter()
                .map(query_aggregate)
                .collect::<Result<_, _>>()?,
            sort: query_sort(&request.sort)?,
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::aggregate_paths_at(&self.store, context, &query).map_err(query_failure)?;
        let groups = page
            .groups
            .into_iter()
            .map(|group| protocol::AggregateGroup {
                keys: protocol_field_values(group.keys),
                values: protocol_field_values(group.values),
            })
            .collect();
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Aggregate(protocol::AggregateResult {
                groups,
                next_cursor: page.next_cursor,
                read_version: page.read_version.get(),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn catalog(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::CatalogRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let (scope, path_prefix) = query_scope(&request.scope)?;
        let query = meta::CatalogRequest {
            scope,
            path_prefix,
            field_prefix: request.field_prefix.clone(),
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::catalog_fields_at(&self.store, context, &query).map_err(query_failure)?;
        let fields = page
            .fields
            .into_iter()
            .map(|field| protocol::CatalogField {
                field_id: field.field_id.as_str().to_owned(),
                scalar_type: scalar_type_name(field.scalar_type).to_owned(),
                operators: field
                    .operators
                    .into_iter()
                    .map(protocol_query_operator)
                    .collect(),
                sortable: field.sortable,
                facetable: field.facetable,
                aggregatable: field.aggregatable,
            })
            .collect();
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Catalog(protocol::CatalogResult {
                fields,
                next_cursor: page.next_cursor,
                read_version: page.read_version.get(),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn find_workspaces(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::FindWorkspacesRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let query = meta::FindWorkspacesRequest {
            committed: if request.committed_only {
                meta::CommittedFilter::Committed
            } else {
                meta::CommittedFilter::Any
            },
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::find_workspaces_at(&self.store, context, &query).map_err(query_failure)?;
        let workspaces = page
            .workspaces
            .into_iter()
            .map(|workspace| {
                Ok(protocol::WorkspaceSummaryWithCommit {
                    workspace: protocol::WorkspaceSummary {
                        workbench: protocol_workbench(&workspace.workbench_id)?,
                        workspace_incarnation_id: workspace.workspace_incarnation_id.into(),
                        workspace_revision: workspace.workspace_revision.get(),
                        commit_head: workspace.commit_id.map(Into::into),
                        commit_head_generation: workspace
                            .commit_head_generation
                            .map(types::Generation::get),
                    },
                    commit: None,
                })
            })
            .collect::<Result<_, protocol::RpcFailure>>()?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Workspaces(protocol::FindWorkspacesResult {
                workspaces,
                next_cursor: page.next_cursor,
                read_version: page.read_version.get(),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn read_changes(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::ChangePageRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let scope = request
            .workbench
            .as_ref()
            .map(workbench_id)
            .transpose()?
            .map_or(meta::QueryScope::Root, meta::QueryScope::Workspace);
        let query = meta::ChangePageRequest {
            scope,
            after_commit_version: request
                .after_commit_version
                .map(types::CommitVersion::new)
                .transpose()
                .map_err(|error| invalid_argument(error.to_string()))?,
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::read_changes_at(&self.store, context, &query).map_err(query_failure)?;
        let changes = page
            .events
            .into_iter()
            .map(protocol_change_event)
            .collect::<Result<_, _>>()?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Changes(protocol::ChangePage {
                events: changes,
                next_cursor: page.next_cursor,
                read_version: page.read_version.get(),
            }),
            commit_version: None,
            replayed: false,
        })
    }

    fn begin_artifact_publish(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::BeginArtifactPublishRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"publish-begin", 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, step_id)? {
            return publish_operation_response(outcome);
        }
        let context = self.publication_context(rpc.route, step_id)?;
        let read = read_context_from_publication(context);
        let (dependency_owner_revision_ids, dependency_depth, dependency_digest) = self
            .publication_dependencies(
                read,
                request.artifact_revision_id.into(),
                &request.dependency_owner_revision_ids,
            )?;
        let workbench = workbench_id(&request.target.workbench)?;
        let path = relative_path(&request.target.path)?;
        let (authority, workspace_incarnation_id, claim) = match request.authority {
            protocol::PublicationAuthority::Visible => {
                let workspace = meta::get_visible_workspace_at(&self.store, read, &workbench)
                    .map_err(namespace_failure)?
                    .ok_or_else(|| not_found("workbench does not exist"))?;
                let current =
                    meta::get_path_at_visible_workspace(&self.store, read, &workspace, &path)
                        .map_err(namespace_failure)?;
                (
                    meta::PublishAuthority::Visible,
                    workspace.incarnation_id,
                    publish_claim(request.condition, current.as_ref())?,
                )
            }
            protocol::PublicationAuthority::CommitStaging {
                commit_operation_id,
            } => {
                let commit_operation_id: types::OperationId = commit_operation_id.into();
                let commit_key = meta::operation_key(
                    read.root_id,
                    types::OperationKind::BuildCommit,
                    commit_operation_id,
                );
                let payload = self
                    .store
                    .read_at(
                        read.root_id,
                        read.placement_generation,
                        read.owner_epoch,
                        meta::MetadataFamily::Operation,
                        &commit_key,
                        read.read_version,
                    )
                    .map_err(engine_failure)?
                    .ok_or_else(|| not_found("commit operation does not exist"))?;
                let commit = meta::BuildCommitOperationRecord::decode(&payload)
                    .map_err(|error| internal(format!("invalid commit operation: {error}")))?;
                let workspace = meta::get_visible_workspace_at(&self.store, read, &workbench)
                    .map_err(namespace_failure)?
                    .ok_or_else(|| not_found("workbench does not exist"))?;
                let current =
                    meta::get_path_at_visible_workspace(&self.store, read, &workspace, &path)
                        .map_err(namespace_failure)?;
                if commit.phase != types::BuildCommitPhase::Building
                    || commit.operation_id != commit_operation_id
                    || commit.workbench_id != workbench
                    || commit.source_workspace_incarnation_id != workspace.incarnation_id
                    || commit.tree_manifest_revision_id
                        != types::ArtifactRevisionId::from(request.artifact_revision_id)
                    || commit.commit_staged_run_manifest.is_some()
                    || path.as_str() != RUN_MANIFEST_PATH
                {
                    return Err(failure(
                        protocol::ErrorCode::PreconditionFailed,
                        "commit-staging publication does not match its frozen commit build",
                        false,
                        Some(protocol::ConflictKind::OperationState),
                    ));
                }
                (
                    meta::PublishAuthority::CommitStaging {
                        commit_operation_id,
                    },
                    workspace.incarnation_id,
                    publish_claim(request.condition, current.as_ref())?,
                )
            }
            protocol::PublicationAuthority::RestoreStaging {
                restore_operation_id,
            } => {
                let operation_id: types::OperationId = restore_operation_id.into();
                let restore = meta::get_restore(
                    &self.store,
                    write_context_from_publication(context),
                    operation_id,
                )
                .map_err(restore_failure)?
                .ok_or_else(|| not_found("restore operation does not exist"))?;
                if restore.phase != types::RestorePhase::SourceSealed
                    || restore.destination_workbench_id != workbench
                    || path.as_str() != meta::RESTORE_MANIFEST_PATH
                    || !matches!(request.condition, protocol::PublishCondition::CreateOnly)
                {
                    return Err(failure(
                        protocol::ErrorCode::PreconditionFailed,
                        "restore-staging publication does not match its sealed destination",
                        false,
                        Some(protocol::ConflictKind::OperationState),
                    ));
                }
                (
                    meta::PublishAuthority::RestoreStaging {
                        restore_operation_id: operation_id,
                    },
                    restore.destination_workspace_incarnation_id,
                    meta::PublishClaim::CreateOnly,
                )
            }
        };
        let mut operation = meta::PublishOperationRecord {
            operation_id: request.operation_id.into(),
            identity_digest: [0; types::SHA256_BYTES],
            initialization_digest: [0; types::SHA256_BYTES],
            initiating_owner_epoch: context.owner_epoch,
            activity_deadline_ms: publish_activity_deadline_ms(&self.store)?,
            authority,
            workbench_id: workbench,
            workspace_incarnation_id,
            path,
            artifact_revision_id: request.artifact_revision_id.into(),
            claim,
            phase: types::PublishPhase::Uploading,
            staged_object_count: request.staged_object_count,
            staged_object_seal: request.staged_object_seal.0,
            staged_object_cursor: 0,
            staged_object_rolling_digest: [0; types::SHA256_BYTES],
            uploaded_object_cursor: 0,
            uploaded_object_rolling_digest: [0; types::SHA256_BYTES],
            manifest_row_count: request.manifest_row_count,
            manifest_seal: request.manifest_seal.0,
            manifest_cursor: 0,
            manifest_rolling_digest: [0; types::SHA256_BYTES],
            manifest_last_position: None,
            dependency_count: u8::try_from(dependency_owner_revision_ids.len())
                .expect("protocol dependency bound fits u8"),
            dependency_depth,
            dependency_digest,
            cleanup_staged_object_cursor: 0,
            cleanup_manifest_cursor: 0,
            publication_absence_proof: None,
            result: None,
            terminal_error: None,
        };
        meta::seal_publish_operation(&mut operation);
        let outcome = meta::PublicationService::new(&self.store)
            .begin_publish(meta::BeginPublishRequest { context, operation })
            .map_err(publication_failure)?;
        publish_operation_response(outcome)
    }

    fn stage_artifact_objects(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::StageArtifactObjectsRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"publish-stage-objects", 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, step_id)? {
            return publish_operation_response(outcome);
        }
        let operation = self.heartbeat_publish_operation(
            rpc,
            request.token,
            b"publish-heartbeat-stage-objects",
        )?;
        let context = self.publication_context(rpc.route, step_id)?;
        let staged_objects = request
            .objects
            .iter()
            .map(|object| meta::StagedObjectRecord {
                artifact_revision_id: operation.artifact_revision_id,
                object_sequence: object.sequence,
                object_key: object.object_identity.as_str().to_owned(),
                multipart_upload_id: object.multipart_token.clone(),
                expected_length: object.expected_length,
                expected_digest_uri: object.expected_digest.as_str().to_owned(),
                provider_state: types::StagedProviderState::Planned,
                cleanup_state: types::StagedCleanupState::Owned,
            })
            .collect();
        let outcome = meta::PublicationService::new(&self.store)
            .stage_objects_batch(meta::StageObjectsBatchRequest {
                context,
                expected_operation: operation,
                staged_objects,
            })
            .map_err(publication_failure)?;
        publish_operation_response(outcome)
    }

    fn mark_artifact_objects_uploaded(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::MarkArtifactObjectsUploadedRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"publish-mark-uploaded", 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, step_id)? {
            return publish_operation_response(outcome);
        }
        let operation = self.heartbeat_publish_operation(
            rpc,
            request.token,
            b"publish-heartbeat-mark-uploaded",
        )?;
        let context = self.publication_context(rpc.route, step_id)?;
        let mut updates = Vec::with_capacity(request.objects.len());
        for proof in &request.objects {
            let key = meta::staged_object_key(
                context.root_id,
                operation.operation_id,
                u64::from(proof.sequence),
            );
            let payload = self
                .store
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::StagedObject,
                    &key,
                    context.read_version,
                )
                .map_err(engine_failure)?
                .ok_or_else(|| not_found("staged object does not exist"))?;
            let expected = meta::StagedObjectRecord::decode(&payload).map_err(|error| {
                internal(format!("invalid durable staged-object record: {error}"))
            })?;
            if expected.expected_length != proof.observed_length
                || expected.expected_digest_uri != proof.observed_digest.as_str()
            {
                return Err(failure(
                    protocol::ErrorCode::ObjectUnavailable,
                    "uploaded object proof does not match the sealed object",
                    false,
                    None,
                ));
            }
            let mut next = expected.clone();
            next.provider_state = types::StagedProviderState::Uploaded;
            updates.push(meta::StagedObjectUpdate { expected, next });
        }
        let outcome = meta::PublicationService::new(&self.store)
            .mark_objects_uploaded_batch(meta::MarkObjectsUploadedBatchRequest {
                context,
                expected_operation: operation,
                staged_object_updates: updates,
            })
            .map_err(publication_failure)?;
        publish_operation_response(outcome)
    }

    fn stage_artifact_manifest(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::StageArtifactManifestRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"publish-stage-manifest", 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, step_id)? {
            return publish_operation_response(outcome);
        }
        let operation = self.heartbeat_publish_operation(
            rpc,
            request.token,
            b"publish-heartbeat-stage-manifest",
        )?;
        let context = self.publication_context(rpc.route, step_id)?;
        let mut rows = Vec::with_capacity(request.rows.len());
        for row in &request.rows {
            rows.push(meta::ManifestRowInput {
                object_index: row.object_index,
                row: meta::ArtifactManifestRow {
                    physical_owner_revision_id: row.physical_owner_revision_id.into(),
                    physical_object_index: row.physical_object_index,
                    object_key: row.object_identity.as_str().to_owned(),
                    logical_offset: row.logical_offset,
                    offset: row.object_offset,
                    length: row.length,
                    digest_uri: row.digest.as_str().to_owned(),
                    append_segment: row.append_segment.map(|segment| meta::AppendSegment {
                        segment_sequence: segment.segment_sequence,
                        segment_offset: segment.segment_offset,
                    }),
                },
            });
        }
        let outcome = meta::PublicationService::new(&self.store)
            .stage_manifest_batch(meta::StageManifestBatchRequest {
                context,
                expected_operation: operation,
                manifest_rows: rows,
                dependency_owner_revision_ids: request
                    .dependency_owner_revision_ids
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect(),
            })
            .map_err(publication_failure)?;
        publish_operation_response(outcome)
    }

    fn complete_artifact_publish(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::CompleteArtifactPublishRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let transition_id = derived_request_id(rpc.request_id, b"publish-complete-transition", 0);
        let transition = if let Some(outcome) = self.replayed_publish(rpc.route, transition_id)? {
            outcome
        } else {
            let operation = self.heartbeat_publish_operation(
                rpc,
                request.token,
                b"publish-heartbeat-complete-transition",
            )?;
            let context = self.publication_context(rpc.route, transition_id)?;
            meta::PublicationService::new(&self.store)
                .transition_publish(meta::TransitionPublishRequest {
                    context,
                    expected_operation: operation,
                    transition: meta::PublishTransition::BeginFinalization,
                })
                .map_err(publication_failure)?
        };
        let finalize_id = derived_request_id(rpc.request_id, b"publish-complete-finalize", 0);
        if let Some(replayed) = self.replayed_publish(rpc.route, finalize_id)? {
            let result = replayed.operation.result.clone().ok_or_else(|| {
                internal("replayed publication finalization has no durable result")
            })?;
            return published_response(
                replayed.operation,
                result,
                replayed.commit_version.get(),
                true,
            );
        }
        let finalizing_token = protocol::OperationToken {
            operation_id: transition.operation.operation_id.into(),
            state_digest: publish_state_digest(&transition.operation)?,
        };
        let finalizing_operation = self.heartbeat_publish_operation(
            rpc,
            finalizing_token,
            b"publish-heartbeat-complete-finalize",
        )?;
        let context = self.publication_context(rpc.route, finalize_id)?;
        let dependency_owner_revision_ids = self.manifest_dependency_owners(
            read_context_from_publication(context),
            &finalizing_operation,
        )?;
        let artifact = meta::PublishedArtifact {
            logical_size: request.artifact.logical_size,
            body_digest_uri: request.artifact.body_digest.as_str().to_owned(),
            manifest_digest_uri: request.artifact.manifest_digest.as_str().to_owned(),
            content_type: request.artifact.content_type.as_str().to_owned(),
            producer: request.artifact.producer.clone(),
            manifest_id: request.artifact.manifest_identity.clone(),
            typed_index_projection: encode_index_fields(&request.artifact.index_fields)?,
        };
        let outcome = meta::PublicationService::new(&self.store)
            .finalize_publish(meta::FinalizePublishRequest {
                context,
                expected_operation: finalizing_operation,
                artifact,
                dependency_owner_revision_ids,
            })
            .map_err(publication_failure)?;
        published_response(
            outcome.operation,
            outcome.result,
            outcome.commit_version.get(),
            outcome.replayed,
        )
    }

    fn abort_artifact_publish(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::AbortArtifactPublishRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"publish-abort", 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, step_id)? {
            return publish_operation_response(outcome);
        }
        let context = self.publication_context(rpc.route, step_id)?;
        let operation = self.load_publish_operation(
            rpc.route,
            context.read_version,
            request.token.operation_id,
        )?;
        require_publish_token(&operation, request.token)?;
        let outcome = meta::PublicationService::new(&self.store)
            .transition_publish(meta::TransitionPublishRequest {
                context,
                expected_operation: operation,
                transition: meta::PublishTransition::BeginAbort {
                    terminal_error: meta::PublishTerminalError {
                        kind: meta::PublishTerminalErrorKind::AbortedByCaller,
                        message: request.reason.clone(),
                        evidence_digest: None,
                    },
                },
            })
            .map_err(publication_failure)?;
        publish_operation_response(outcome)
    }

    /// Operator reconciliation of one quarantined publication. The operator
    /// verifies provider-side object state out-of-band and presents a verdict
    /// bound to the exact quarantined operation payload through the token;
    /// this adapter only drives the durable metadata sweep and resolution.
    fn reconcile_quarantined_artifact_publish(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::ReconcileQuarantinedArtifactPublishRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let finish_id = derived_request_id(rpc.request_id, b"publish-reconcile-finish", 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, finish_id)? {
            return publish_operation_response(outcome);
        }
        let resolution = match request.resolution {
            protocol::QuarantineResolution::ProviderObjectsAbsent => {
                meta::QuarantineReconcileResolution::RevisionUnpublished
            }
            protocol::QuarantineResolution::RevisionPublished => {
                meta::QuarantineReconcileResolution::RevisionPublished
            }
        };
        let service = meta::PublicationService::new(&self.store);
        let read_version = self.store.current_read_version().map_err(engine_failure)?;
        let mut operation =
            self.load_publish_operation(rpc.route, read_version, request.token.operation_id)?;
        require_publish_token(&operation, request.token)?;
        if operation.phase != types::PublishPhase::Quarantined {
            return Err(conflict(
                protocol::ConflictKind::OperationState,
                "operation is not quarantined",
                None,
            ));
        }

        let mut batch: u64 = 0;
        while operation.cleanup_staged_object_cursor < operation.staged_object_cursor
            || operation.cleanup_manifest_cursor < operation.manifest_cursor
        {
            let before = (
                operation.cleanup_staged_object_cursor,
                operation.cleanup_manifest_cursor,
            );
            let step_id = derived_request_id(rpc.request_id, b"publish-reconcile-batch", batch);
            operation = if let Some(replayed) = self.replayed_publish(rpc.route, step_id)? {
                replayed.operation
            } else {
                let staged_object_rows = self.reconcile_staged_batch_rows(rpc.route, &operation)?;
                service
                    .reconcile_quarantined_publish_batch(
                        meta::ReconcileQuarantinedPublishBatchRequest {
                            context: self.publication_context(rpc.route, step_id)?,
                            expected_operation: operation,
                            resolution,
                            staged_object_rows,
                        },
                    )
                    .map_err(publication_failure)?
                    .operation
            };
            if (
                operation.cleanup_staged_object_cursor,
                operation.cleanup_manifest_cursor,
            ) == before
            {
                return Err(internal(
                    "quarantine reconciliation made no durable progress",
                ));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("quarantine reconciliation batch counter overflow"))?;
        }

        let outcome = service
            .finish_reconcile_quarantined_publish(meta::FinishReconcileQuarantinedPublishRequest {
                context: self.publication_context(rpc.route, finish_id)?,
                expected_operation: operation,
                resolution,
                reason: request.reason.clone(),
                operator_evidence_digest: request.evidence_digest.0,
            })
            .map_err(publication_failure)?;
        publish_operation_response(outcome)
    }

    /// Read the exact durable staged rows for the next reconciliation batch.
    /// Empty once the staged cursor is sealed and only manifest pages remain.
    fn reconcile_staged_batch_rows(
        &self,
        route: protocol::RootRoute,
        operation: &meta::PublishOperationRecord,
    ) -> Result<Vec<meta::StagedObjectRecord>, protocol::RpcFailure> {
        let remaining = operation
            .staged_object_cursor
            .saturating_sub(operation.cleanup_staged_object_cursor);
        if remaining == 0 {
            return Ok(Vec::new());
        }
        let route = route_parts(route)?;
        let read_version = self.store.current_read_version().map_err(engine_failure)?;
        let count = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(meta::MAX_PUBLICATION_BATCH_ROWS);
        let mut rows = Vec::with_capacity(count);
        for offset in 0..count {
            let sequence = operation.cleanup_staged_object_cursor
                + u32::try_from(offset).expect("bounded batch offset fits u32");
            let key =
                meta::staged_object_key(route.root_id, operation.operation_id, u64::from(sequence));
            let payload = self
                .store
                .read_at(
                    route.root_id,
                    route.placement_generation,
                    route.owner_epoch,
                    meta::MetadataFamily::StagedObject,
                    &key,
                    read_version,
                )
                .map_err(engine_failure)?
                .ok_or_else(|| internal(format!("durable staged object {sequence} is missing")))?;
            rows.push(
                meta::StagedObjectRecord::decode(&payload)
                    .map_err(|error| internal(format!("invalid durable staged object: {error}")))?,
            );
        }
        Ok(rows)
    }

    fn commit(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::CommitRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let service = meta::CommitService::new(&self.store);
        let operation_id: types::OperationId = request.operation_id.into();
        let begin_id = derived_request_id(rpc.request_id, b"commit-begin", 0);
        let mut outcome = if let Some(outcome) = self.replayed_build_commit(rpc.route, begin_id)? {
            outcome
        } else {
            let context = self.write_context(rpc.route, begin_id)?;
            let workbench = workbench_id(&request.workbench)?;
            service
                .begin_build(meta::BeginBuildCommitRequest {
                    context,
                    operation_id,
                    workbench_id: workbench,
                    expected_source_workspace_incarnation_id: request
                        .workspace_incarnation_id
                        .into(),
                    commit_id: request.commit_id.into(),
                    content_digest_uri: request.content_digest.as_str().to_owned(),
                    manifest_digest_uri: request.manifest_digest.as_str().to_owned(),
                    projection_input_digest: request.projection_input_digest.0,
                    tree_manifest_revision_id: request.tree_manifest_revision_id.into(),
                    replace: request.replace,
                    run_manifest_condition: commit_manifest_condition(
                        request.run_manifest_condition,
                    )?,
                    committed_at_unix_seconds: commit_time_unix_seconds(&self.store)?,
                    expected_head_generation: request
                        .expected_head_generation
                        .map(types::Generation::new)
                        .transpose()
                        .map_err(|error| invalid_argument(error.to_string()))?,
                    producer: request.producer.clone(),
                    lineage_projection: request.lineage_projection.clone(),
                    parent_commits: request.parents.iter().copied().map(Into::into).collect(),
                })
                .map_err(commit_failure)?
        };

        if !matches!(
            outcome.operation.phase,
            types::BuildCommitPhase::Building | types::BuildCommitPhase::Sealing
        ) {
            return Ok(ExecutedRequest {
                result: protocol::WorkspaceResult::Operation(build_commit_operation_status(
                    &outcome.operation,
                )?),
                commit_version: Some(outcome.commit_version.get()),
                replayed: outcome.replayed,
            });
        }

        // The first commit call durably freezes the expected head and source
        // read version. Object upload then publishes the canonical manifest
        // under CommitStaging, which atomically records its binding without making
        // the path visible. Only a later call may build and seal the commit.
        if outcome.operation.commit_staged_run_manifest.is_none() {
            return Ok(ExecutedRequest {
                result: protocol::WorkspaceResult::Operation(build_commit_operation_status(
                    &outcome.operation,
                )?),
                commit_version: Some(outcome.commit_version.get()),
                replayed: outcome.replayed,
            });
        }

        let mut batch = 0_u64;
        while !outcome.operation.members_complete {
            let before = outcome.operation.member_count;
            outcome = self.run_build_step(rpc, b"commit-members", batch, |context| {
                service.build_members(meta::BuildCommitStepRequest {
                    context,
                    operation_id,
                    limit: meta::MAX_COMMIT_MEMBER_BATCH_ROWS,
                })
            })?;
            if !outcome.operation.members_complete && outcome.operation.member_count == before {
                return Err(internal("commit member builder made no durable progress"));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("commit member batch counter overflow"))?;
        }

        batch = 0;
        while !outcome.operation.revisions_complete {
            let before = outcome.operation.revision_seal_count;
            outcome = self.run_build_step(rpc, b"commit-revisions", batch, |context| {
                service.seal_revisions(meta::BuildCommitStepRequest {
                    context,
                    operation_id,
                    limit: meta::MAX_COMMIT_REVISION_BATCH_ROWS,
                })
            })?;
            if !outcome.operation.revisions_complete
                && outcome.operation.revision_seal_count == before
            {
                return Err(internal("commit revision sealer made no durable progress"));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("commit revision batch counter overflow"))?;
        }

        batch = 0;
        while !outcome.operation.parents_complete {
            let before = outcome.operation.parent_cursor;
            outcome = self.run_build_step(rpc, b"commit-parents", batch, |context| {
                service.attach_parents(meta::BuildCommitStepRequest {
                    context,
                    operation_id,
                    limit: meta::MAX_COMMIT_PARENT_BATCH_ROWS,
                })
            })?;
            if !outcome.operation.parents_complete && outcome.operation.parent_cursor == before {
                return Err(internal("commit parent builder made no durable progress"));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("commit parent batch counter overflow"))?;
        }

        if outcome.operation.phase == types::BuildCommitPhase::Building {
            outcome = self.run_build_step(rpc, b"commit-begin-sealing", 0, |context| {
                service.begin_sealing(context, operation_id)
            })?;
        }
        if outcome.operation.phase == types::BuildCommitPhase::Sealing {
            outcome = self.run_build_step(rpc, b"commit-finalize", 0, |context| {
                service.finalize_build(context, operation_id)
            })?;
        }
        if outcome.operation.phase != types::BuildCommitPhase::Complete {
            return Err(failure(
                protocol::ErrorCode::OperationFailed,
                "commit operation did not reach Complete",
                false,
                Some(protocol::ConflictKind::OperationState),
            ));
        }
        let result = outcome
            .operation
            .result
            .ok_or_else(|| internal("complete commit operation has no result"))?;
        let status = build_commit_operation_status(&outcome.operation)?;
        if status.state != protocol::OperationState::Succeeded
            || status.result.as_ref().is_none_or(|operation_result| {
                !matches!(operation_result, protocol::OperationResult::Commit(commit) if commit.head_generation == result.head_generation.get())
            })
        {
            return Err(internal("complete commit operation has an inconsistent status"));
        }
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Operation(status),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn get_operation(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::GetOperationRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let operation_id: types::OperationId = request.operation_id.into();
        let mut found = Vec::new();
        for kind in [
            types::OperationKind::Publish,
            types::OperationKind::BuildCommit,
            types::OperationKind::Restore,
        ] {
            let key = meta::operation_key(context.root_id, kind, operation_id);
            if let Some(payload) = self
                .store
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::Operation,
                    &key,
                    context.read_version,
                )
                .map_err(engine_failure)?
            {
                found.push((kind, payload));
            }
        }
        if found.len() > 1 {
            return Err(internal(
                "operation identity is present under multiple lifecycle kinds",
            ));
        }
        let Some((kind, payload)) = found.pop() else {
            return Err(not_found("operation does not exist"));
        };
        let status = match kind {
            types::OperationKind::Publish => {
                let operation = meta::PublishOperationRecord::decode(&payload)
                    .map_err(|error| internal(format!("invalid publish operation: {error}")))?;
                publish_operation_status(&operation)?
            }
            types::OperationKind::BuildCommit => {
                let operation = meta::BuildCommitOperationRecord::decode(&payload)
                    .map_err(|error| internal(format!("invalid commit operation: {error}")))?;
                build_commit_operation_status(&operation)?
            }
            types::OperationKind::Restore => {
                let operation = meta::RestoreOperationRecord::decode(&payload)
                    .map_err(|error| internal(format!("invalid restore operation: {error}")))?;
                restore_operation_status(&operation)?
            }
            _ => unreachable!("only public operation kinds were scanned"),
        };
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Operation(status),
            commit_version: None,
            replayed: false,
        })
    }

    fn path_metadata(
        workbench: &protocol::WorkbenchName,
        workspace: meta::WorkspaceRecord,
        path: types::NormalizedRelativePath,
        entry: meta::PathEntry,
    ) -> Result<protocol::PathMetadata, protocol::RpcFailure> {
        Ok(protocol::PathMetadata {
            path: protocol::WorkspacePath {
                workbench: workbench.clone(),
                path: protocol::RelativePath::new(path.as_str())
                    .map_err(|error| internal(error.to_string()))?,
            },
            workspace_incarnation_id: workspace.incarnation_id.into(),
            workspace_revision: workspace.workspace_revision.get(),
            generation: entry.generation.get(),
            artifact_revision_id: entry.artifact_revision_id.into(),
            dependency_count: entry.dependency_count,
            dependency_depth: entry.dependency_depth,
            descriptor: protocol::ArtifactDescriptor {
                logical_size: entry.logical_size,
                body_digest: protocol::DigestUri::new(entry.body_digest_uri)
                    .map_err(|error| internal(error.to_string()))?,
                manifest_digest: protocol::DigestUri::new(entry.manifest_digest_uri)
                    .map_err(|error| internal(error.to_string()))?,
                content_type: protocol::ContentType::new(entry.content_type)
                    .map_err(|error| internal(error.to_string()))?,
                producer: entry.producer,
                manifest_identity: entry.manifest_id,
                index_fields: decode_index_fields(&entry.typed_index_projection)?,
            },
        })
    }

    fn manifest_read_plan(
        &self,
        context: meta::RootReadContext,
        revision_id: types::ArtifactRevisionId,
        range: protocol::ByteRange,
        page: &protocol::PageRequest,
    ) -> Result<(Vec<protocol::ArtifactManifestRow>, Option<Vec<u8>>), protocol::RpcFailure> {
        let prefix = meta::artifact_manifest_prefix(context.root_id, revision_id);
        let range_end = range
            .offset
            .checked_add(range.length)
            .expect("protocol validation rejected range overflow");
        let wanted = query_limit(page.limit);
        let (mut marker, mut logical_offset, mut previous_object_index) = match page
            .cursor
            .as_deref()
        {
            None => (None, 0_u64, None),
            Some(encoded) => {
                let object_index =
                    decode_manifest_plan_cursor(encoded, context.root_id, revision_id, range)?;
                let key = meta::artifact_manifest_key(context.root_id, revision_id, object_index);
                let payload = self
                    .store
                    .read_at(
                        context.root_id,
                        context.placement_generation,
                        context.owner_epoch,
                        meta::MetadataFamily::ArtifactManifest,
                        &key,
                        context.read_version,
                    )
                    .map_err(engine_failure)?
                    .ok_or_else(|| invalid_argument("manifest plan cursor row is absent"))?;
                let row = meta::ArtifactManifestRow::decode(&payload)
                    .map_err(|error| internal(format!("invalid artifact manifest row: {error}")))?;
                let row_end = row
                    .logical_offset
                    .checked_add(row.length)
                    .ok_or_else(|| internal("artifact manifest logical length overflow"))?;
                if row.logical_offset >= range_end
                    || row_end <= range.offset
                    || row_end >= range_end
                {
                    return Err(invalid_argument(
                        "manifest plan cursor is not a resumable range row",
                    ));
                }
                (Some(key), row_end, Some(object_index))
            }
        };
        let mut selected = Vec::with_capacity(wanted);
        'scan: loop {
            let rows = self
                .store
                .scan_prefix_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::ArtifactManifest,
                    &prefix,
                    context.read_version,
                    marker.as_deref(),
                    MANIFEST_SCAN_ROWS,
                )
                .map_err(engine_failure)?;
            if rows.is_empty() {
                break;
            }
            for item in &rows {
                let object_index =
                    meta::decode_artifact_manifest_key(context.root_id, revision_id, &item.key)
                        .ok_or_else(|| internal("malformed artifact manifest key"))?;
                if previous_object_index.is_some_and(|previous| previous >= object_index) {
                    return Err(internal(
                        "artifact manifest object indexes are not strictly increasing",
                    ));
                }
                let row = meta::ArtifactManifestRow::decode(&item.value)
                    .map_err(|error| internal(format!("invalid artifact manifest row: {error}")))?;
                if row.logical_offset != logical_offset {
                    return Err(internal(
                        "artifact manifest rows do not form one contiguous logical range",
                    ));
                }
                let row_end = row
                    .logical_offset
                    .checked_add(row.length)
                    .ok_or_else(|| internal("artifact manifest logical length overflow"))?;
                if row.logical_offset < range_end && row_end > range.offset {
                    selected.push(protocol::ArtifactManifestRow {
                        object_index,
                        physical_object_index: row.physical_object_index,
                        logical_offset: row.logical_offset,
                        physical_owner_revision_id: row.physical_owner_revision_id.into(),
                        object_identity: protocol::ObjectIdentity::new(row.object_key)
                            .map_err(|error| internal(error.to_string()))?,
                        object_offset: row.offset,
                        length: row.length,
                        digest: protocol::DigestUri::new(row.digest_uri)
                            .map_err(|error| internal(error.to_string()))?,
                        append_segment: row.append_segment.map(|segment| protocol::AppendSegment {
                            segment_sequence: segment.segment_sequence,
                            segment_offset: segment.segment_offset,
                        }),
                    });
                }
                logical_offset = row_end;
                previous_object_index = Some(object_index);
                if selected.len() == wanted || logical_offset >= range_end {
                    break 'scan;
                }
            }
            marker = rows.last().map(|item| item.key.clone());
            if rows.len() < MANIFEST_SCAN_ROWS {
                break;
            }
        }
        if selected.is_empty() || (logical_offset < range_end && selected.len() < wanted) {
            return Err(internal(
                "artifact manifest does not cover the requested logical range",
            ));
        }
        let next_cursor = (logical_offset < range_end).then(|| {
            let last = selected
                .last()
                .expect("non-terminal manifest page contains a selected row");
            encode_manifest_plan_cursor(context.root_id, revision_id, range, last.object_index)
        });
        Ok((selected, next_cursor))
    }

    fn publication_dependencies(
        &self,
        context: meta::RootReadContext,
        artifact_revision_id: types::ArtifactRevisionId,
        owners: &[protocol::ArtifactRevisionIdentity],
    ) -> Result<
        (
            Vec<types::ArtifactRevisionId>,
            u8,
            [u8; types::SHA256_BYTES],
        ),
        protocol::RpcFailure,
    > {
        let owners = owners
            .iter()
            .copied()
            .map(Into::into)
            .collect::<Vec<types::ArtifactRevisionId>>();
        if owners.binary_search(&artifact_revision_id).is_ok() {
            return Err(invalid_argument(
                "dependency closure contains the new artifact revision",
            ));
        }
        let digest = meta::dependency_owner_digest(&owners).map_err(publication_failure)?;
        if owners.is_empty() {
            return Ok((owners, 0, digest));
        }
        let mut maximum_owner_depth = 0_u8;
        for owner in &owners {
            let payload = self
                .store
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::ArtifactRevision,
                    &meta::artifact_revision_key(context.root_id, *owner),
                    context.read_version,
                )
                .map_err(engine_failure)?
                .ok_or_else(|| not_found("dependency owner revision does not exist"))?;
            let revision = meta::ArtifactRevisionRecord::decode(&payload)
                .map_err(|error| internal(format!("invalid dependency owner revision: {error}")))?;
            if revision.state != types::RevisionState::Available {
                return Err(failure(
                    protocol::ErrorCode::PreconditionFailed,
                    "dependency owner revision is not available",
                    false,
                    Some(protocol::ConflictKind::OperationState),
                ));
            }
            maximum_owner_depth = maximum_owner_depth.max(revision.dependency_depth);
        }
        let dependency_depth = maximum_owner_depth.checked_add(1).ok_or_else(|| {
            invalid_argument("artifact dependency closure exceeds the supported depth")
        })?;
        if dependency_depth > meta::MAX_REVISION_DEPENDENCY_DEPTH {
            return Err(invalid_argument(format!(
                "artifact dependency closure depth {dependency_depth} exceeds {}",
                meta::MAX_REVISION_DEPENDENCY_DEPTH
            )));
        }
        Ok((owners, dependency_depth, digest))
    }

    fn manifest_dependency_owners(
        &self,
        context: meta::RootReadContext,
        operation: &meta::PublishOperationRecord,
    ) -> Result<Vec<types::ArtifactRevisionId>, protocol::RpcFailure> {
        let prefix =
            meta::artifact_manifest_prefix(context.root_id, operation.artifact_revision_id);
        let mut marker = None;
        let mut owners = BTreeSet::new();
        loop {
            let rows = self
                .store
                .scan_prefix_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::ArtifactManifest,
                    &prefix,
                    context.read_version,
                    marker.as_deref(),
                    MANIFEST_SCAN_ROWS,
                )
                .map_err(engine_failure)?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let manifest = meta::ArtifactManifestRow::decode(&row.value)
                    .map_err(|error| internal(format!("invalid artifact manifest row: {error}")))?;
                if manifest.physical_owner_revision_id != operation.artifact_revision_id {
                    owners.insert(manifest.physical_owner_revision_id);
                }
            }
            marker = rows.last().map(|row| row.key.clone());
            if rows.len() < MANIFEST_SCAN_ROWS {
                break;
            }
        }
        let owners = owners.into_iter().collect::<Vec<_>>();
        meta::dependency_owner_digest(&owners).map_err(publication_failure)?;
        Ok(owners)
    }

    fn claim_mutation(
        &self,
        request: &protocol::WorkspaceRpcRequest,
    ) -> Result<bool, protocol::RpcFailure> {
        let encoded = protocol::encode_request(request)
            .map_err(|error| invalid_argument(error.to_string()))?;
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.server.rpc-request.v1\0");
        hasher.update(&encoded);
        let request_digest: [u8; types::SHA256_BYTES] = hasher.finalize().into();
        let request_id: types::RequestId = request.request_id.into();
        if let Some(existing) = self.lookup_request(request.route, request_id)? {
            if existing.deterministic_result == request_digest {
                return Ok(true);
            }
            return Err(failure(
                protocol::ErrorCode::RequestReplayMismatch,
                "request id was reused with different RPC inputs",
                false,
                None,
            ));
        }
        let route = route_parts(request.route)?;
        let command = meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: route.root_id,
            logical_shard_id: route.logical_shard_id,
            placement_generation: route.placement_generation,
            owner_epoch: route.owner_epoch,
            request_id,
            command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
            read_version: self.store.current_read_version().map_err(engine_failure)?,
            root_fence_action: meta::RootFenceAction::RequireActive,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: request_digest.to_vec(),
        }
        .seal();
        self.store.execute(&command).map_err(engine_failure)?;
        Ok(false)
    }

    fn read_context(
        &self,
        route: protocol::RootRoute,
    ) -> Result<meta::RootReadContext, protocol::RpcFailure> {
        let route = route_parts(route)?;
        meta::RootReadContext::current(
            &self.store,
            route.root_id,
            route.placement_generation,
            route.owner_epoch,
        )
        .map_err(namespace_failure)
    }

    fn workspace_read_context(
        &self,
        route: protocol::RootRoute,
        workbench: &types::WorkbenchId,
        view: &protocol::WorkspaceReadView,
    ) -> Result<meta::RootReadContext, protocol::RpcFailure> {
        let live = self.read_context(route)?;
        let protocol::WorkspaceReadView::Snapshot(selector) = view else {
            return Ok(live);
        };
        let snapshot =
            meta::get_snapshot_at(&self.store, live, workbench, &snapshot_selector(selector)?)
                .map_err(snapshot_failure)?
                .ok_or_else(|| not_found("snapshot does not exist for the visible workbench"))?;
        if snapshot.record.state != types::SnapshotState::Active {
            return Err(failure(
                protocol::ErrorCode::PreconditionFailed,
                "snapshot is not active",
                false,
                Some(protocol::ConflictKind::SnapshotLifecycle),
            ));
        }
        let lease_clock = self
            .store
            .lease_clock_high_water()
            .map_err(engine_failure)?;
        if snapshot.record.lease_deadline_ms <= lease_clock {
            return Err(failure(
                protocol::ErrorCode::SnapshotExpired,
                "snapshot lease has expired",
                false,
                Some(protocol::ConflictKind::SnapshotLifecycle),
            ));
        }
        Ok(meta::RootReadContext {
            read_version: snapshot.record.read_version,
            ..live
        })
    }

    fn write_context(
        &self,
        route: protocol::RootRoute,
        request_id: types::RequestId,
    ) -> Result<meta::RootWriteContext, protocol::RpcFailure> {
        let route = route_parts(route)?;
        let read_version = match self.lookup_request(route.protocol, request_id)? {
            Some(replay) => types::ReadVersion::new(
                replay
                    .commit_version
                    .get()
                    .checked_sub(1)
                    .ok_or_else(|| internal("dedupe commit version has no predecessor"))?,
            )
            .map_err(|error| internal(error.to_string()))?,
            None => self.store.current_read_version().map_err(engine_failure)?,
        };
        Ok(meta::RootWriteContext {
            root_id: route.root_id,
            logical_shard_id: route.logical_shard_id,
            placement_generation: route.placement_generation,
            owner_epoch: route.owner_epoch,
            request_id,
            read_version,
        })
    }

    fn publication_context(
        &self,
        route: protocol::RootRoute,
        request_id: types::RequestId,
    ) -> Result<meta::PublicationContext, protocol::RpcFailure> {
        let context = self.write_context(route, request_id)?;
        Ok(meta::PublicationContext {
            root_id: context.root_id,
            logical_shard_id: context.logical_shard_id,
            placement_generation: context.placement_generation,
            owner_epoch: context.owner_epoch,
            request_id: context.request_id,
            read_version: context.read_version,
        })
    }

    fn lookup_request(
        &self,
        route: protocol::RootRoute,
        request_id: types::RequestId,
    ) -> Result<Option<meta::CommandDedupeRecord>, protocol::RpcFailure> {
        let route = route_parts(route)?;
        self.store
            .lookup_request(
                route.root_id,
                route.placement_generation,
                route.owner_epoch,
                request_id,
            )
            .map_err(engine_failure)
    }

    fn load_publish_operation(
        &self,
        route: protocol::RootRoute,
        read_version: types::ReadVersion,
        operation_id: protocol::OperationIdentity,
    ) -> Result<meta::PublishOperationRecord, protocol::RpcFailure> {
        let route = route_parts(route)?;
        let operation_id: types::OperationId = operation_id.into();
        let key = meta::operation_key(route.root_id, types::OperationKind::Publish, operation_id);
        let payload = self
            .store
            .read_at(
                route.root_id,
                route.placement_generation,
                route.owner_epoch,
                meta::MetadataFamily::Operation,
                &key,
                read_version,
            )
            .map_err(engine_failure)?
            .ok_or_else(|| not_found("publish operation does not exist"))?;
        let operation = meta::PublishOperationRecord::decode(&payload)
            .map_err(|error| internal(format!("invalid durable publish operation: {error}")))?;
        if operation.operation_id != operation_id {
            return Err(internal("publish operation key and payload disagree"));
        }
        Ok(operation)
    }

    fn heartbeat_publish_operation(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        token: protocol::OperationToken,
        domain: &'static [u8],
    ) -> Result<meta::PublishOperationRecord, protocol::RpcFailure> {
        let heartbeat_id = derived_request_id(rpc.request_id, domain, 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, heartbeat_id)? {
            require_publish_token(&outcome.operation, token)?;
            return Ok(outcome.operation);
        }
        let heartbeat_context = self.publication_context(rpc.route, heartbeat_id)?;
        let operation = self.load_publish_operation(
            rpc.route,
            heartbeat_context.read_version,
            token.operation_id,
        )?;
        require_publish_token(&operation, token)?;
        let activity_deadline_ms = publish_activity_deadline_ms(&self.store)?;
        if activity_deadline_ms <= operation.activity_deadline_ms {
            return Ok(operation);
        }
        meta::PublicationService::new(&self.store)
            .heartbeat_publish(meta::HeartbeatPublishRequest {
                context: heartbeat_context,
                expected_operation: operation,
                activity_deadline_ms,
            })
            .map(|outcome| outcome.operation)
            .map_err(publication_failure)
    }

    fn replayed_publish(
        &self,
        route: protocol::RootRoute,
        request_id: types::RequestId,
    ) -> Result<Option<meta::PublishCommandOutcome>, protocol::RpcFailure> {
        self.lookup_request(route, request_id)?
            .map(|record| {
                let operation = meta::PublishOperationRecord::decode(&record.deterministic_result)
                    .map_err(|error| {
                        internal(format!("invalid replayed publish operation: {error}"))
                    })?;
                Ok(meta::PublishCommandOutcome {
                    commit_version: record.commit_version,
                    operation,
                    replayed: true,
                })
            })
            .transpose()
    }

    fn replayed_build_commit(
        &self,
        route: protocol::RootRoute,
        request_id: types::RequestId,
    ) -> Result<Option<meta::BuildCommitOutcome>, protocol::RpcFailure> {
        self.lookup_request(route, request_id)?
            .map(|record| {
                let operation =
                    meta::BuildCommitOperationRecord::decode(&record.deterministic_result)
                        .map_err(|error| {
                            internal(format!("invalid replayed commit operation: {error}"))
                        })?;
                Ok(meta::BuildCommitOutcome {
                    commit_version: record.commit_version,
                    operation,
                    replayed: true,
                })
            })
            .transpose()
    }

    fn run_build_step(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        label: &[u8],
        sequence: u64,
        run: impl FnOnce(meta::RootWriteContext) -> Result<meta::BuildCommitOutcome, meta::CommitError>,
    ) -> Result<meta::BuildCommitOutcome, protocol::RpcFailure> {
        let request_id = derived_request_id(rpc.request_id, label, sequence);
        if let Some(outcome) = self.replayed_build_commit(rpc.route, request_id)? {
            return Ok(outcome);
        }
        run(self.write_context(rpc.route, request_id)?).map_err(commit_failure)
    }
}

impl WorkspaceRequestExecutor for MetadataWorkspaceRequestExecutor {
    fn execute(
        &self,
        request: &protocol::WorkspaceRpcRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.execute_request(request)
    }
}

#[derive(Clone, Copy)]
struct RouteParts {
    protocol: protocol::RootRoute,
    root_id: types::RootId,
    logical_shard_id: types::LogicalShardId,
    placement_generation: types::PlacementGeneration,
    owner_epoch: types::OwnerEpoch,
}

fn route_parts(route: protocol::RootRoute) -> Result<RouteParts, protocol::RpcFailure> {
    Ok(RouteParts {
        protocol: route,
        root_id: route.root_id.into(),
        logical_shard_id: route.logical_shard_id.into(),
        placement_generation: types::PlacementGeneration::new(route.placement_generation)
            .map_err(|error| invalid_argument(error.to_string()))?,
        owner_epoch: types::OwnerEpoch::new(route.owner_epoch)
            .map_err(|error| invalid_argument(error.to_string()))?,
    })
}

fn workbench_id(
    value: &protocol::WorkbenchName,
) -> Result<types::WorkbenchId, protocol::RpcFailure> {
    types::WorkbenchId::new(value.as_str()).map_err(|error| invalid_argument(error.to_string()))
}

fn relative_path(
    value: &protocol::RelativePath,
) -> Result<types::NormalizedRelativePath, protocol::RpcFailure> {
    types::NormalizedRelativePath::new(value.as_str())
        .map_err(|error| invalid_argument(error.to_string()))
}

fn protocol_workbench(
    value: &types::WorkbenchId,
) -> Result<protocol::WorkbenchName, protocol::RpcFailure> {
    protocol::WorkbenchName::new(value.as_str()).map_err(|error| internal(error.to_string()))
}

fn snapshot_selector(
    selector: &protocol::SnapshotSelector,
) -> Result<meta::SnapshotSelector, protocol::RpcFailure> {
    match selector {
        protocol::SnapshotSelector::Id(snapshot_id) => {
            if *snapshot_id == 0 {
                return Err(invalid_argument(
                    "snapshot selector id must be greater than zero",
                ));
            }
            Ok(meta::SnapshotSelector::Id(types::SnapshotId::new(
                *snapshot_id,
            )))
        }
        protocol::SnapshotSelector::Alias(alias) => Ok(meta::SnapshotSelector::Alias(
            types::SnapshotAliasName::new(alias.as_str())
                .map_err(|error| invalid_argument(error.to_string()))?,
        )),
    }
}

fn snapshot_result(
    workbench: &protocol::WorkbenchName,
    workspace_incarnation_id: types::WorkspaceIncarnationId,
    snapshot: meta::ResolvedSnapshot,
    lease_clock_ms: u64,
) -> Result<protocol::SnapshotResult, protocol::RpcFailure> {
    let status = match snapshot.record.state {
        types::SnapshotState::Active if snapshot.record.lease_deadline_ms <= lease_clock_ms => {
            protocol::SnapshotStatus::Expired
        }
        types::SnapshotState::Active => protocol::SnapshotStatus::Alive,
        types::SnapshotState::ReapClaimed => protocol::SnapshotStatus::ReapClaimed,
        types::SnapshotState::Retired => protocol::SnapshotStatus::Retired,
        types::SnapshotState::Reaped => protocol::SnapshotStatus::Reaped,
    };
    Ok(protocol::SnapshotResult {
        snapshot_id: snapshot.snapshot_id.get(),
        workbench: workbench.clone(),
        workspace_incarnation_id: workspace_incarnation_id.into(),
        read_version: snapshot.record.read_version.get(),
        lease_deadline_ms: snapshot.record.lease_deadline_ms,
        alias: snapshot
            .record
            .alias
            .map(|alias| protocol::SnapshotAlias::new(alias.as_str()))
            .transpose()
            .map_err(|error| internal(error.to_string()))?,
        annotation: snapshot.record.annotation,
        retire_annotation: snapshot.record.retire_annotation,
        status,
        consumer_count: snapshot.record.consumer_count,
    })
}

fn query_scope(
    scope: &protocol::QueryScope,
) -> Result<(meta::QueryScope, Option<types::NormalizedRelativePath>), protocol::RpcFailure> {
    match scope {
        protocol::QueryScope::Workspace {
            workbench,
            path_prefix,
        } => Ok((
            meta::QueryScope::Workspace(workbench_id(workbench)?),
            path_prefix.as_ref().map(relative_path).transpose()?,
        )),
        protocol::QueryScope::Root { path_prefix } => Ok((
            meta::QueryScope::Root,
            path_prefix.as_ref().map(relative_path).transpose()?,
        )),
    }
}

fn query_field_id(value: &str) -> Result<meta::QueryFieldId, protocol::RpcFailure> {
    meta::QueryFieldId::new(value).map_err(|error| invalid_argument(error.to_string()))
}

fn query_field_ids(values: &[String]) -> Result<Vec<meta::QueryFieldId>, protocol::RpcFailure> {
    values.iter().map(|value| query_field_id(value)).collect()
}

fn query_predicates(
    predicates: &[protocol::QueryPredicate],
) -> Result<Vec<meta::QueryPredicate>, protocol::RpcFailure> {
    predicates
        .iter()
        .map(|predicate| {
            let operand = match &predicate.operand {
                protocol::QueryOperand::None => meta::QueryOperand::None,
                protocol::QueryOperand::Scalar(value) => {
                    meta::QueryOperand::Scalar(query_scalar(value)?)
                }
                protocol::QueryOperand::Set(values) => {
                    meta::QueryOperand::Set(values.iter().map(query_scalar).collect::<Result<
                        BTreeSet<_>,
                        _,
                    >>(
                    )?)
                }
            };
            Ok(meta::QueryPredicate {
                field_id: query_field_id(&predicate.field_id)?,
                operator: meta_query_operator(predicate.operator),
                operand,
            })
        })
        .collect()
}

fn query_sort(sort: &[protocol::SortField]) -> Result<Vec<meta::QuerySort>, protocol::RpcFailure> {
    sort.iter()
        .map(|sort| {
            Ok(meta::QuerySort {
                field_id: query_field_id(&sort.field_id)?,
                direction: match sort.direction {
                    protocol::SortDirection::Ascending => meta::QuerySortDirection::Ascending,
                    protocol::SortDirection::Descending => meta::QuerySortDirection::Descending,
                },
            })
        })
        .collect()
}

fn query_aggregate(
    aggregate: &protocol::AggregateSpec,
) -> Result<meta::AggregateSpec, protocol::RpcFailure> {
    Ok(meta::AggregateSpec {
        result_id: query_field_id(&aggregate.result_id)?,
        function: match aggregate.function {
            protocol::AggregateFunction::Count => meta::AggregateFunction::Count,
            protocol::AggregateFunction::Sum => meta::AggregateFunction::Sum,
            protocol::AggregateFunction::Average => meta::AggregateFunction::Average,
            protocol::AggregateFunction::Minimum => meta::AggregateFunction::Minimum,
            protocol::AggregateFunction::Maximum => meta::AggregateFunction::Maximum,
        },
        field_id: aggregate
            .field_id
            .as_deref()
            .map(query_field_id)
            .transpose()?,
    })
}

fn meta_query_operator(operator: protocol::QueryOperator) -> meta::QueryOperator {
    match operator {
        protocol::QueryOperator::Equal => meta::QueryOperator::Equal,
        protocol::QueryOperator::NotEqual => meta::QueryOperator::NotEqual,
        protocol::QueryOperator::In => meta::QueryOperator::In,
        protocol::QueryOperator::Less => meta::QueryOperator::Less,
        protocol::QueryOperator::LessOrEqual => meta::QueryOperator::LessOrEqual,
        protocol::QueryOperator::Greater => meta::QueryOperator::Greater,
        protocol::QueryOperator::GreaterOrEqual => meta::QueryOperator::GreaterOrEqual,
        protocol::QueryOperator::Prefix => meta::QueryOperator::Prefix,
        protocol::QueryOperator::Suffix => meta::QueryOperator::Suffix,
        protocol::QueryOperator::Contains => meta::QueryOperator::Contains,
        protocol::QueryOperator::Exists => meta::QueryOperator::Exists,
        protocol::QueryOperator::NotExists => meta::QueryOperator::NotExists,
    }
}

fn protocol_query_operator(operator: meta::QueryOperator) -> protocol::QueryOperator {
    match operator {
        meta::QueryOperator::Equal => protocol::QueryOperator::Equal,
        meta::QueryOperator::NotEqual => protocol::QueryOperator::NotEqual,
        meta::QueryOperator::In => protocol::QueryOperator::In,
        meta::QueryOperator::Less => protocol::QueryOperator::Less,
        meta::QueryOperator::LessOrEqual => protocol::QueryOperator::LessOrEqual,
        meta::QueryOperator::Greater => protocol::QueryOperator::Greater,
        meta::QueryOperator::GreaterOrEqual => protocol::QueryOperator::GreaterOrEqual,
        meta::QueryOperator::Prefix => protocol::QueryOperator::Prefix,
        meta::QueryOperator::Suffix => protocol::QueryOperator::Suffix,
        meta::QueryOperator::Contains => protocol::QueryOperator::Contains,
        meta::QueryOperator::Exists => protocol::QueryOperator::Exists,
        meta::QueryOperator::NotExists => protocol::QueryOperator::NotExists,
    }
}

fn query_scalar(value: &protocol::ScalarValue) -> Result<meta::QueryScalar, protocol::RpcFailure> {
    match value {
        protocol::ScalarValue::Null => Ok(meta::QueryScalar::Null),
        protocol::ScalarValue::Boolean(value) => Ok(meta::QueryScalar::Boolean(*value)),
        protocol::ScalarValue::Signed(value) => Ok(meta::QueryScalar::Signed(*value)),
        protocol::ScalarValue::Unsigned(value) => Ok(meta::QueryScalar::Unsigned(*value)),
        protocol::ScalarValue::Decimal(value) => {
            let value = value
                .parse::<f64>()
                .map_err(|_| invalid_argument("decimal query scalar is not an f64"))?;
            let value = meta::FiniteFloat::new(value)
                .map_err(|error| invalid_argument(error.to_string()))?;
            Ok(meta::QueryScalar::Float(value))
        }
        protocol::ScalarValue::Timestamp(value) => Ok(meta::QueryScalar::Timestamp(*value)),
        protocol::ScalarValue::String(value) => Ok(meta::QueryScalar::String(value.clone())),
        protocol::ScalarValue::Bytes(value) => Ok(meta::QueryScalar::Bytes(value.clone())),
    }
}

fn protocol_scalar(value: meta::QueryScalar) -> protocol::ScalarValue {
    match value {
        meta::QueryScalar::Null => protocol::ScalarValue::Null,
        meta::QueryScalar::Boolean(value) => protocol::ScalarValue::Boolean(value),
        meta::QueryScalar::Signed(value) => protocol::ScalarValue::Signed(value),
        meta::QueryScalar::Unsigned(value) => protocol::ScalarValue::Unsigned(value),
        meta::QueryScalar::Float(value) => protocol::ScalarValue::Decimal(value.get().to_string()),
        meta::QueryScalar::Timestamp(value) => protocol::ScalarValue::Timestamp(value),
        meta::QueryScalar::Bytes(value) => protocol::ScalarValue::Bytes(value),
        meta::QueryScalar::String(value) => protocol::ScalarValue::String(value),
    }
}

fn encode_index_fields(fields: &[protocol::FieldValue]) -> Result<Vec<u8>, protocol::RpcFailure> {
    let fields = fields
        .iter()
        .map(|field| {
            Ok((
                query_field_id(&field.field_id)?,
                query_scalar(&field.value)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, protocol::RpcFailure>>()?;
    meta::TypedProjection::new(fields)
        .and_then(|projection| projection.encode())
        .map_err(|error| invalid_argument(error.to_string()))
}

fn decode_index_fields(encoded: &[u8]) -> Result<Vec<protocol::FieldValue>, protocol::RpcFailure> {
    let projection = meta::TypedProjection::decode_stored(encoded)
        .map_err(|error| internal(format!("invalid durable typed projection: {error}")))?;
    Ok(projection
        .fields()
        .iter()
        .map(|(field_id, value)| protocol::FieldValue {
            field_id: field_id.as_str().to_owned(),
            value: protocol_scalar(value.clone()),
        })
        .collect())
}

fn protocol_field_values(
    values: impl IntoIterator<Item = (meta::QueryFieldId, meta::QueryScalar)>,
) -> Vec<protocol::FieldValue> {
    values
        .into_iter()
        .map(|(field_id, value)| protocol::FieldValue {
            field_id: field_id.as_str().to_owned(),
            value: protocol_scalar(value),
        })
        .collect()
}

fn scalar_type_name(scalar_type: meta::QueryScalarType) -> &'static str {
    match scalar_type {
        meta::QueryScalarType::Null => "null",
        meta::QueryScalarType::Boolean => "boolean",
        meta::QueryScalarType::Signed => "signed",
        meta::QueryScalarType::Unsigned => "unsigned",
        meta::QueryScalarType::Float => "float",
        meta::QueryScalarType::Timestamp => "timestamp",
        meta::QueryScalarType::Bytes => "bytes",
        meta::QueryScalarType::String => "string",
    }
}

fn query_limit(limit: u32) -> usize {
    usize::try_from(limit).expect("u32 query limit always fits usize")
}

fn protocol_change_event(
    change: meta::ChangeEvent,
) -> Result<protocol::ChangeEvent, protocol::RpcFailure> {
    let workbench = protocol_workbench(&change.workbench_id)?;
    let meta::ChangeEventRecord {
        kind,
        artifact_revision_id,
        commit_id,
        operation_id,
        path,
        ..
    } = change.event;
    let path = path
        .map(|path| {
            Ok(protocol::WorkspacePath {
                workbench: workbench.clone(),
                path: protocol::RelativePath::new(path.as_str())
                    .map_err(|error| internal(error.to_string()))?,
            })
        })
        .transpose()?;
    let kind = match kind {
        meta::ChangeEventKind::WorkspaceCreated => protocol::ChangeKind::WorkspaceCreated,
        meta::ChangeEventKind::ArtifactPublished => protocol::ChangeKind::ArtifactPublished,
        meta::ChangeEventKind::PathRemoved => protocol::ChangeKind::PathRemoved,
        meta::ChangeEventKind::WorkspaceRestored => protocol::ChangeKind::WorkspaceRestored,
        meta::ChangeEventKind::SnapshotMinted => protocol::ChangeKind::SnapshotMinted,
        meta::ChangeEventKind::SnapshotRenewed => protocol::ChangeKind::SnapshotRenewed,
        meta::ChangeEventKind::SnapshotRetired => protocol::ChangeKind::SnapshotRetired,
        meta::ChangeEventKind::SnapshotReapClaimed => protocol::ChangeKind::SnapshotReapClaimed,
        meta::ChangeEventKind::SnapshotReaped => protocol::ChangeKind::SnapshotReaped,
        meta::ChangeEventKind::SnapshotConsumerAttached => {
            protocol::ChangeKind::SnapshotConsumerAttached
        }
        meta::ChangeEventKind::SnapshotConsumerReleased => {
            protocol::ChangeKind::SnapshotConsumerReleased
        }
        meta::ChangeEventKind::CommitAdvanced => protocol::ChangeKind::CommitPublished,
        meta::ChangeEventKind::CommitRetired => protocol::ChangeKind::CommitRetired,
    };
    Ok(protocol::ChangeEvent {
        commit_version: change.commit_version.get(),
        event_sequence: change.sequence,
        kind,
        workbench: Some(workbench),
        path,
        artifact_revision_id: artifact_revision_id.map(Into::into),
        commit_id: commit_id.map(Into::into),
        operation_id: operation_id.map(Into::into),
    })
}

fn decode_path_cursor(
    cursor: &[u8],
) -> Result<types::NormalizedRelativePath, protocol::RpcFailure> {
    let value = std::str::from_utf8(cursor)
        .map_err(|_| invalid_argument("list_paths cursor is not UTF-8"))?;
    types::NormalizedRelativePath::new(value)
        .map_err(|error| invalid_argument(format!("invalid list_paths cursor: {error}")))
}

fn encode_manifest_plan_cursor(
    root_id: types::RootId,
    revision_id: types::ArtifactRevisionId,
    range: protocol::ByteRange,
    object_index: u64,
) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(MANIFEST_PLAN_CURSOR_BYTES);
    encoded.push(MANIFEST_PLAN_CURSOR_VERSION);
    encoded.extend_from_slice(root_id.as_bytes());
    encoded.extend_from_slice(revision_id.as_bytes());
    encoded.extend_from_slice(&range.offset.to_be_bytes());
    encoded.extend_from_slice(&range.length.to_be_bytes());
    encoded.extend_from_slice(&object_index.to_be_bytes());
    encoded
}

fn decode_manifest_plan_cursor(
    encoded: &[u8],
    expected_root_id: types::RootId,
    expected_revision_id: types::ArtifactRevisionId,
    expected_range: protocol::ByteRange,
) -> Result<u64, protocol::RpcFailure> {
    if encoded.len() != MANIFEST_PLAN_CURSOR_BYTES
        || encoded.first().copied() != Some(MANIFEST_PLAN_CURSOR_VERSION)
    {
        return Err(invalid_argument("invalid manifest plan cursor envelope"));
    }
    let root_end = 1 + types::FIXED_ID_BYTES;
    let revision_end = root_end + types::FIXED_ID_BYTES;
    if &encoded[1..root_end] != expected_root_id.as_bytes()
        || &encoded[root_end..revision_end] != expected_revision_id.as_bytes()
    {
        return Err(invalid_argument(
            "manifest plan cursor belongs to a different artifact",
        ));
    }
    let offset_end = revision_end + 8;
    let length_end = offset_end + 8;
    let object_end = length_end + 8;
    let offset = u64::from_be_bytes(
        encoded[revision_end..offset_end]
            .try_into()
            .expect("cursor offset has fixed width"),
    );
    let length = u64::from_be_bytes(
        encoded[offset_end..length_end]
            .try_into()
            .expect("cursor length has fixed width"),
    );
    if (offset, length) != (expected_range.offset, expected_range.length) {
        return Err(invalid_argument(
            "manifest plan cursor belongs to a different byte range",
        ));
    }
    Ok(u64::from_be_bytes(
        encoded[length_end..object_end]
            .try_into()
            .expect("cursor object index has fixed width"),
    ))
}

fn publish_activity_deadline_ms(
    store: &meta::AgentMetadataStore,
) -> Result<u64, protocol::RpcFailure> {
    let wall_clock_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| internal(format!("system clock is before Unix epoch: {error}")))?
            .as_millis(),
    )
    .map_err(|_| internal("system clock milliseconds exceed u64"))?;
    let lease_clock_ms = store.lease_clock_high_water().map_err(engine_failure)?;
    wall_clock_ms
        .max(lease_clock_ms)
        .checked_add(PUBLISH_ACTIVITY_LEASE_MS)
        .ok_or_else(|| internal("publish activity deadline overflows u64"))
}

fn commit_time_unix_seconds(store: &meta::AgentMetadataStore) -> Result<u64, protocol::RpcFailure> {
    let wall_clock_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| internal(format!("system clock is before Unix epoch: {error}")))?
            .as_millis(),
    )
    .map_err(|_| internal("system clock milliseconds exceed u64"))?;
    let lease_clock_ms = store.lease_clock_high_water().map_err(engine_failure)?;
    let seconds = wall_clock_ms.max(lease_clock_ms) / 1_000;
    if seconds == 0 {
        return Err(internal("commit time must be after the Unix epoch"));
    }
    Ok(seconds)
}

fn publish_claim(
    condition: protocol::PublishCondition,
    current: Option<&meta::PathEntry>,
) -> Result<meta::PublishClaim, protocol::RpcFailure> {
    match condition {
        protocol::PublishCondition::CreateOnly => Ok(meta::PublishClaim::CreateOnly),
        protocol::PublishCondition::ReplaceOnly {
            expected_generation,
        } => Ok(meta::PublishClaim::ReplaceOnly {
            expected_generation: types::Generation::new(expected_generation)
                .map_err(|error| invalid_argument(error.to_string()))?,
        }),
        protocol::PublishCondition::Append {
            expected_generation,
        } => match current {
            None if expected_generation.is_none() => Ok(meta::PublishClaim::CreateOnly),
            None => Err(not_found("append target does not exist")),
            Some(entry) => {
                let expected = expected_generation.unwrap_or_else(|| entry.generation.get());
                if expected != entry.generation.get() {
                    return Err(conflict(
                        protocol::ConflictKind::PathGeneration,
                        "append target generation does not match",
                        Some(entry.generation.get()),
                    ));
                }
                Ok(meta::PublishClaim::Append {
                    expected_generation: entry.generation,
                    base_revision_id: entry.artifact_revision_id,
                    append_offset: entry.logical_size,
                })
            }
        },
    }
}

fn commit_manifest_condition(
    condition: protocol::PublishCondition,
) -> Result<meta::CommitManifestCondition, protocol::RpcFailure> {
    match condition {
        protocol::PublishCondition::CreateOnly => Ok(meta::CommitManifestCondition::CreateOnly),
        protocol::PublishCondition::ReplaceOnly {
            expected_generation,
        } => Ok(meta::CommitManifestCondition::ReplaceOnly {
            expected_generation: types::Generation::new(expected_generation)
                .map_err(|error| invalid_argument(error.to_string()))?,
        }),
        protocol::PublishCondition::Append { .. } => Err(invalid_argument(
            "commit run-manifest publication does not support append",
        )),
    }
}

fn read_context_from_publication(context: meta::PublicationContext) -> meta::RootReadContext {
    meta::RootReadContext {
        root_id: context.root_id,
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        read_version: context.read_version,
    }
}

fn write_context_from_publication(context: meta::PublicationContext) -> meta::RootWriteContext {
    meta::RootWriteContext {
        root_id: context.root_id,
        logical_shard_id: context.logical_shard_id,
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        request_id: context.request_id,
        read_version: context.read_version,
    }
}

fn read_context_from_write(context: meta::RootWriteContext) -> meta::RootReadContext {
    meta::RootReadContext {
        root_id: context.root_id,
        placement_generation: context.placement_generation,
        owner_epoch: context.owner_epoch,
        read_version: context.read_version,
    }
}

fn derived_request_id(
    request_id: protocol::RequestIdentity,
    step: &[u8],
    sequence: u64,
) -> types::RequestId {
    let mut hasher = Sha256::new();
    hasher.update(b"nokv.server.metadata-step.v1\0");
    hasher.update(request_id.0);
    hasher.update((step.len() as u64).to_be_bytes());
    hasher.update(step);
    hasher.update(sequence.to_be_bytes());
    let digest: [u8; types::SHA256_BYTES] = hasher.finalize().into();
    let mut bytes = [0_u8; types::FIXED_ID_BYTES];
    bytes.copy_from_slice(&digest[..types::FIXED_ID_BYTES]);
    types::RequestId::from_bytes(bytes)
}

fn require_publish_token(
    operation: &meta::PublishOperationRecord,
    token: protocol::OperationToken,
) -> Result<(), protocol::RpcFailure> {
    if operation.operation_id != types::OperationId::from(token.operation_id) {
        return Err(invalid_argument(
            "operation token identity does not match the loaded operation",
        ));
    }
    if publish_state_digest(operation)? != token.state_digest {
        return Err(conflict(
            protocol::ConflictKind::OperationState,
            "operation token state digest is stale",
            None,
        ));
    }
    Ok(())
}

fn publish_state_digest(
    operation: &meta::PublishOperationRecord,
) -> Result<protocol::Digest, protocol::RpcFailure> {
    let encoded = operation
        .encode()
        .map_err(|error| internal(format!("cannot encode publish operation token: {error}")))?;
    Ok(protocol::Digest(Sha256::digest(encoded).into()))
}

fn build_state_digest(
    operation: &meta::BuildCommitOperationRecord,
) -> Result<protocol::Digest, protocol::RpcFailure> {
    let encoded = operation
        .encode()
        .map_err(|error| internal(format!("cannot encode commit operation token: {error}")))?;
    Ok(protocol::Digest(Sha256::digest(encoded).into()))
}

fn restore_state_digest(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::Digest, protocol::RpcFailure> {
    let encoded = operation
        .encode()
        .map_err(|error| internal(format!("cannot encode restore operation token: {error}")))?;
    Ok(protocol::Digest(Sha256::digest(encoded).into()))
}

fn publish_operation_response(
    outcome: meta::PublishCommandOutcome,
) -> Result<ExecutedRequest, protocol::RpcFailure> {
    Ok(ExecutedRequest {
        result: protocol::WorkspaceResult::Operation(publish_operation_status(&outcome.operation)?),
        commit_version: Some(outcome.commit_version.get()),
        replayed: outcome.replayed,
    })
}

fn published_response(
    operation: meta::PublishOperationRecord,
    result: meta::PublishResult,
    commit_version: u64,
    replayed: bool,
) -> Result<ExecutedRequest, protocol::RpcFailure> {
    let target = workspace_path(&operation.workbench_id, &operation.path)?;
    Ok(ExecutedRequest {
        result: protocol::WorkspaceResult::Published(protocol::PublishResult {
            operation_id: operation.operation_id.into(),
            target,
            workspace_revision: result.workspace_revision.get(),
            generation: result.path_generation.get(),
            artifact_revision_id: operation.artifact_revision_id.into(),
            logical_size: result.logical_size,
            body_digest: protocol::DigestUri::new(result.body_digest_uri)
                .map_err(|error| internal(error.to_string()))?,
        }),
        commit_version: Some(commit_version),
        replayed,
    })
}

fn publish_operation_status(
    operation: &meta::PublishOperationRecord,
) -> Result<protocol::OperationStatus, protocol::RpcFailure> {
    let total_rows = u64::from(operation.staged_object_count)
        .checked_mul(2)
        .and_then(|count| count.checked_add(u64::from(operation.manifest_row_count)))
        .ok_or_else(|| internal("publish progress total overflow"))?;
    let completed_rows = u64::from(operation.staged_object_cursor)
        .checked_add(u64::from(operation.uploaded_object_cursor))
        .and_then(|count| count.checked_add(u64::from(operation.manifest_cursor)))
        .ok_or_else(|| internal("publish progress counter overflow"))?;
    let (state, result, failure_body) = match operation.phase {
        types::PublishPhase::Uploading | types::PublishPhase::Finalizing => {
            (protocol::OperationState::Running, None, None)
        }
        types::PublishPhase::Published => {
            let published = operation
                .result
                .as_ref()
                .ok_or_else(|| internal("published operation does not contain a durable result"))?;
            (
                protocol::OperationState::Succeeded,
                Some(protocol::OperationResult::ArtifactPublish(
                    protocol::PublishResult {
                        operation_id: operation.operation_id.into(),
                        target: workspace_path(&operation.workbench_id, &operation.path)?,
                        workspace_revision: published.workspace_revision.get(),
                        generation: published.path_generation.get(),
                        artifact_revision_id: operation.artifact_revision_id.into(),
                        logical_size: published.logical_size,
                        body_digest: protocol::DigestUri::new(published.body_digest_uri.clone())
                            .map_err(|error| internal(error.to_string()))?,
                    },
                )),
                None,
            )
        }
        types::PublishPhase::Aborting | types::PublishPhase::Cleaning => {
            (protocol::OperationState::Aborting, None, None)
        }
        types::PublishPhase::Cleaned => (
            protocol::OperationState::Failed,
            None,
            Some(operation_terminal_failure(
                operation
                    .terminal_error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("artifact publication was aborted"),
            )),
        ),
        types::PublishPhase::Quarantined => (
            protocol::OperationState::Quarantined,
            None,
            Some(failure(
                protocol::ErrorCode::Quarantined,
                operation
                    .terminal_error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("artifact publication is quarantined"),
                false,
                Some(protocol::ConflictKind::OperationState),
            )),
        ),
    };
    Ok(protocol::OperationStatus {
        token: protocol::OperationToken {
            operation_id: operation.operation_id.into(),
            state_digest: publish_state_digest(operation)?,
        },
        kind: protocol::OperationKind::ArtifactPublish,
        commit_preparation: None,
        restore_preparation: None,
        state,
        progress: protocol::OperationProgress {
            completed_rows,
            total_rows: Some(total_rows),
            completed_bytes: 0,
            total_bytes: None,
        },
        result,
        failure: failure_body,
    })
}

fn build_commit_operation_status(
    operation: &meta::BuildCommitOperationRecord,
) -> Result<protocol::OperationStatus, protocol::RpcFailure> {
    let (state, result, failure_body) = match operation.phase {
        types::BuildCommitPhase::Building | types::BuildCommitPhase::Sealing => {
            (protocol::OperationState::Running, None, None)
        }
        types::BuildCommitPhase::Complete => {
            let complete = operation
                .result
                .ok_or_else(|| internal("complete commit operation has no result"))?;
            (
                protocol::OperationState::Succeeded,
                Some(protocol::OperationResult::Commit(protocol::CommitResult {
                    operation_id: operation.operation_id.into(),
                    commit_id: operation.commit_id.into(),
                    workbench: protocol::WorkbenchName::new(operation.workbench_id.as_str())
                        .map_err(|error| internal(error.to_string()))?,
                    head_generation: complete.head_generation.get(),
                    member_count: operation.member_count,
                    member_digest: protocol::Digest(operation.member_digest),
                })),
                None,
            )
        }
        types::BuildCommitPhase::Aborting | types::BuildCommitPhase::Cleaning => {
            (protocol::OperationState::Aborting, None, None)
        }
        types::BuildCommitPhase::Cleaned => (
            protocol::OperationState::Failed,
            None,
            Some(operation_terminal_failure(
                operation
                    .terminal_error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("commit construction was aborted"),
            )),
        ),
        types::BuildCommitPhase::Quarantined => (
            protocol::OperationState::Quarantined,
            None,
            Some(failure(
                protocol::ErrorCode::Quarantined,
                operation
                    .terminal_error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("commit construction is quarantined"),
                false,
                Some(protocol::ConflictKind::OperationState),
            )),
        ),
    };
    Ok(protocol::OperationStatus {
        token: protocol::OperationToken {
            operation_id: operation.operation_id.into(),
            state_digest: build_state_digest(operation)?,
        },
        kind: protocol::OperationKind::Commit,
        commit_preparation: Some(Box::new(protocol::CommitPreparation {
            request: Box::new(protocol::CommitRequest {
                operation_id: operation.operation_id.into(),
                workbench: protocol::WorkbenchName::new(operation.workbench_id.as_str())
                    .map_err(|error| internal(error.to_string()))?,
                workspace_incarnation_id: operation.source_workspace_incarnation_id.into(),
                commit_id: operation.commit_id.into(),
                content_digest: protocol::DigestUri::new(operation.content_digest_uri.clone())
                    .map_err(|error| internal(error.to_string()))?,
                manifest_digest: protocol::DigestUri::new(operation.manifest_digest_uri.clone())
                    .map_err(|error| internal(error.to_string()))?,
                projection_input_digest: protocol::Digest(operation.projection_input_digest),
                tree_manifest_revision_id: operation.tree_manifest_revision_id.into(),
                replace: operation.replace,
                run_manifest_condition: match operation.run_manifest_condition {
                    meta::CommitManifestCondition::CreateOnly => {
                        protocol::PublishCondition::CreateOnly
                    }
                    meta::CommitManifestCondition::ReplaceOnly {
                        expected_generation,
                    } => protocol::PublishCondition::ReplaceOnly {
                        expected_generation: expected_generation.get(),
                    },
                },
                expected_head_generation: operation
                    .expected_head
                    .map(|head| head.head_generation.get()),
                parents: operation
                    .parent_commits
                    .iter()
                    .copied()
                    .map(Into::into)
                    .collect(),
                producer: operation.producer.clone(),
                lineage_projection: operation.lineage_projection.clone(),
            }),
            committed_at_unix_seconds: operation.committed_at_unix_seconds,
            manifest: operation
                .commit_staged_run_manifest
                .as_ref()
                .map(|manifest| {
                    Ok(protocol::CommitManifestBinding {
                        workspace_incarnation_id: operation.source_workspace_incarnation_id.into(),
                        artifact_revision_id: operation.tree_manifest_revision_id.into(),
                        descriptor: protocol::ArtifactDescriptor {
                            logical_size: manifest.logical_size,
                            body_digest: protocol::DigestUri::new(manifest.body_digest_uri.clone())
                                .map_err(|error| internal(error.to_string()))?,
                            manifest_digest: protocol::DigestUri::new(
                                manifest.manifest_digest_uri.clone(),
                            )
                            .map_err(|error| internal(error.to_string()))?,
                            content_type: protocol::ContentType::new(manifest.content_type.clone())
                                .map_err(|error| internal(error.to_string()))?,
                            producer: None,
                            manifest_identity: None,
                            index_fields: Vec::new(),
                        },
                    })
                })
                .transpose()?,
        })),
        restore_preparation: None,
        state,
        progress: protocol::OperationProgress {
            completed_rows: operation.member_count,
            total_rows: operation.members_complete.then_some(operation.member_count),
            completed_bytes: 0,
            total_bytes: None,
        },
        result,
        failure: failure_body,
    })
}

fn restore_operation_preparation(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::RestoreOperationPreparation, protocol::RpcFailure> {
    let (source, source_snapshot_read_version) = match operation.source {
        meta::RestoreSource::Snapshot {
            snapshot_id,
            read_version,
        } => (
            protocol::RestoreSource::Snapshot(protocol::SnapshotSelector::Id(snapshot_id.get())),
            Some(read_version.get()),
        ),
        meta::RestoreSource::Commit { commit_id } => {
            (protocol::RestoreSource::Commit(commit_id.into()), None)
        }
    };
    let (sealed_member_count, sealed_member_digest) = match operation.member_seal {
        Some(digest) => (
            Some(operation.next_member_sequence),
            Some(protocol::Digest(digest)),
        ),
        None => (None, None),
    };
    Ok(protocol::RestoreOperationPreparation {
        request: protocol::PrepareRestoreRequest {
            source_workbench: protocol_workbench(&operation.source_workbench_id)?,
            source_workspace_incarnation_id: operation.source_workspace_incarnation_id.into(),
            source,
            destination_workbench: protocol_workbench(&operation.destination_workbench_id)?,
            destination_workspace_incarnation_id: operation
                .destination_workspace_incarnation_id
                .into(),
            restore_manifest: protocol::RestoreManifestDescriptor {
                body_digest: protocol::DigestUri::new(
                    operation.restore_manifest.body_digest_uri.clone(),
                )
                .map_err(|error| internal(error.to_string()))?,
                logical_size: operation.restore_manifest.logical_size,
                content_type: protocol::ContentType::new(
                    operation.restore_manifest.content_type.clone(),
                )
                .map_err(|error| internal(error.to_string()))?,
            },
        },
        source_snapshot_read_version,
        sealed_member_count,
        sealed_member_digest,
    })
}

fn sealed_restore_preparation(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::RestorePreparation, protocol::RpcFailure> {
    if !matches!(
        operation.phase,
        types::RestorePhase::SourceSealed
            | types::RestorePhase::Ready
            | types::RestorePhase::Complete
    ) {
        return Err(operation_terminal_failure(
            "restore preparation did not reach a sealed phase",
        ));
    }
    let member_digest = operation
        .member_seal
        .ok_or_else(|| internal("sealed restore has no member digest"))?;
    Ok(protocol::RestorePreparation {
        operation_id: operation.operation_id.into(),
        destination_workbench: protocol_workbench(&operation.destination_workbench_id)?,
        destination_workspace_incarnation_id: operation.destination_workspace_incarnation_id.into(),
        member_count: operation.next_member_sequence,
        member_digest: protocol::Digest(member_digest),
    })
}

fn restore_operation_status(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::OperationStatus, protocol::RpcFailure> {
    let (state, result, failure_body) = match operation.phase {
        types::RestorePhase::Preparing
        | types::RestorePhase::Copying
        | types::RestorePhase::SourceSealed
        | types::RestorePhase::Ready => (protocol::OperationState::Running, None, None),
        types::RestorePhase::Complete => {
            let complete = operation
                .result
                .as_ref()
                .ok_or_else(|| internal("complete restore operation has no result"))?;
            (
                protocol::OperationState::Succeeded,
                Some(protocol::OperationResult::Restore(
                    protocol::RestoreResult {
                        operation_id: operation.operation_id.into(),
                        destination: protocol::WorkspaceSummary {
                            workbench: protocol::WorkbenchName::new(
                                operation.destination_workbench_id.as_str(),
                            )
                            .map_err(|error| internal(error.to_string()))?,
                            workspace_incarnation_id: complete
                                .destination_workspace_incarnation_id
                                .into(),
                            workspace_revision: complete.destination_workspace_revision.get(),
                            commit_head: None,
                            commit_head_generation: None,
                        },
                        member_count: complete.member_count,
                        member_digest: protocol::Digest(complete.member_digest),
                        metadata_rows_copied: complete.member_count,
                        object_bytes_copied: 0,
                    },
                )),
                None,
            )
        }
        types::RestorePhase::Aborting | types::RestorePhase::Cleaning => {
            (protocol::OperationState::Aborting, None, None)
        }
        types::RestorePhase::Cleaned => (
            protocol::OperationState::Failed,
            None,
            Some(operation_terminal_failure(
                operation
                    .terminal_error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("restore was aborted"),
            )),
        ),
        types::RestorePhase::Quarantined => (
            protocol::OperationState::Quarantined,
            None,
            Some(failure(
                protocol::ErrorCode::Quarantined,
                operation
                    .terminal_error
                    .as_ref()
                    .map(|error| error.message.as_str())
                    .unwrap_or("restore is quarantined"),
                false,
                Some(protocol::ConflictKind::OperationState),
            )),
        ),
    };
    Ok(protocol::OperationStatus {
        token: protocol::OperationToken {
            operation_id: operation.operation_id.into(),
            state_digest: restore_state_digest(operation)?,
        },
        kind: protocol::OperationKind::Restore,
        commit_preparation: None,
        restore_preparation: Some(Box::new(restore_operation_preparation(operation)?)),
        state,
        progress: protocol::OperationProgress {
            completed_rows: operation.next_member_sequence,
            total_rows: operation
                .source_eof
                .then_some(operation.next_member_sequence),
            completed_bytes: 0,
            total_bytes: None,
        },
        result,
        failure: failure_body,
    })
}

fn workspace_path(
    workbench: &types::WorkbenchId,
    path: &types::NormalizedRelativePath,
) -> Result<protocol::WorkspacePath, protocol::RpcFailure> {
    Ok(protocol::WorkspacePath {
        workbench: protocol::WorkbenchName::new(workbench.as_str())
            .map_err(|error| internal(error.to_string()))?,
        path: protocol::RelativePath::new(path.as_str())
            .map_err(|error| internal(error.to_string()))?,
    })
}

fn namespace_failure(error: meta::NamespaceError) -> protocol::RpcFailure {
    match error {
        meta::NamespaceError::AlreadyExists { .. }
        | meta::NamespaceError::IncarnationAlreadyClaimed { .. } => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::Workspace),
        ),
        meta::NamespaceError::InvalidPageLimit { .. } | meta::NamespaceError::InvalidListMarker => {
            invalid_argument(error.to_string())
        }
        meta::NamespaceError::Engine(source) => engine_failure(source),
        _ => internal(error.to_string()),
    }
}

fn query_failure(error: meta::QueryError) -> protocol::RpcFailure {
    match error {
        meta::QueryError::Namespace(source) => namespace_failure(source),
        meta::QueryError::Engine(source) => engine_failure(source),
        meta::QueryError::CursorReadVersionMismatch { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::ReadVersion),
        ),
        meta::QueryError::CursorQueryMismatch
        | meta::QueryError::CursorAnchorMissing
        | meta::QueryError::AggregateOverflow { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            None,
        ),
        meta::QueryError::CorruptKey { .. }
        | meta::QueryError::Projection { .. }
        | meta::QueryError::WorkspaceCodec { .. }
        | meta::QueryError::PathCodec { .. }
        | meta::QueryError::CommitHeadCodec { .. } => internal(error.to_string()),
        meta::QueryError::InvalidLimit { .. }
        | meta::QueryError::BoundExceeded { .. }
        | meta::QueryError::InvalidPredicate { .. }
        | meta::QueryError::UnknownField { .. }
        | meta::QueryError::FieldTypeConflict { .. }
        | meta::QueryError::InvalidAggregate { .. }
        | meta::QueryError::InvalidFieldPrefix { .. }
        | meta::QueryError::CursorTooLarge { .. }
        | meta::QueryError::InvalidCursor { .. } => invalid_argument(error.to_string()),
    }
}

fn remove_path_failure(error: meta::RemovePathError) -> protocol::RpcFailure {
    match error {
        meta::RemovePathError::Engine(source) => engine_failure(source),
        meta::RemovePathError::WorkspaceNotFound
        | meta::RemovePathError::PathNotFound
        | meta::RemovePathError::RevisionNotFound { .. } => not_found(error.to_string()),
        meta::RemovePathError::GenerationMismatch { actual, .. } => conflict(
            protocol::ConflictKind::PathGeneration,
            error.to_string(),
            Some(actual),
        ),
        meta::RemovePathError::WorkspaceUnavailable => {
            conflict(protocol::ConflictKind::Workspace, error.to_string(), None)
        }
        meta::RemovePathError::ConcurrentMutation => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::PathGeneration),
        ),
        meta::RemovePathError::RequestInputMismatch => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::RemovePathError::RevisionUnavailable { .. }
        | meta::RemovePathError::ReservedManifest => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            None,
        ),
        meta::RemovePathError::RecordCodec(_)
        | meta::RemovePathError::QueryRecord(_)
        | meta::RemovePathError::WorkspaceRevisionOverflow
        | meta::RemovePathError::RevisionReferenceMissing
        | meta::RemovePathError::RevisionReferenceEpochAhead
        | meta::RemovePathError::ReferenceCountUnderflow
        | meta::RemovePathError::ReferenceEpochOverflow
        | meta::RemovePathError::CommitVersionOverflow
        | meta::RemovePathError::DeterministicResultMismatch { .. } => internal(error.to_string()),
    }
}

fn snapshot_failure(error: meta::SnapshotError) -> protocol::RpcFailure {
    match error {
        meta::SnapshotError::Engine(source) => engine_failure(source),
        meta::SnapshotError::WorkspaceMissing { .. }
        | meta::SnapshotError::SnapshotMissing { .. }
        | meta::SnapshotError::AliasMissing { .. }
        | meta::SnapshotError::ConsumerMissing { .. } => not_found(error.to_string()),
        meta::SnapshotError::SnapshotAlreadyExists { .. }
        | meta::SnapshotError::SnapshotIdAlreadyClaimed { .. } => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        meta::SnapshotError::RequestInputMismatch => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::SnapshotError::ConcurrentMutation => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        error @ meta::SnapshotError::SnapshotNotActive {
            state: types::SnapshotState::ReapClaimed,
            ..
        } => failure(
            protocol::ErrorCode::SnapshotExpired,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        error @ meta::SnapshotError::SnapshotNotActive {
            state: types::SnapshotState::Reaped,
            ..
        } => failure(
            protocol::ErrorCode::SnapshotReaped,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        meta::SnapshotError::SnapshotNotActive { .. }
        | meta::SnapshotError::SnapshotNotReapClaimed { .. }
        | meta::SnapshotError::LeaseDeadlineNotExtended { .. }
        | meta::SnapshotError::LeaseDeadlineNotFuture { .. }
        | meta::SnapshotError::SnapshotNotExpired { .. }
        | meta::SnapshotError::ForkRetentionActive { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        meta::SnapshotError::AliasProjectionMismatch
        | meta::SnapshotError::ConsumerCountOverflow
        | meta::SnapshotError::ConsumerEpochOverflow
        | meta::SnapshotError::AliasGenerationOverflow
        | meta::SnapshotError::DeterministicResultMismatch { .. }
        | meta::SnapshotError::WorkspaceCodec(_)
        | meta::SnapshotError::SnapshotCodec(_)
        | meta::SnapshotError::QueryRecord(_) => internal(error.to_string()),
    }
}

fn snapshot_list_failure(error: meta::SnapshotListError) -> protocol::RpcFailure {
    match error {
        meta::SnapshotListError::Engine(source) => engine_failure(source),
        meta::SnapshotListError::Namespace(source) => namespace_failure(source),
        meta::SnapshotListError::WorkspaceNotFound { .. } => not_found(error.to_string()),
        meta::SnapshotListError::InvalidLimit { .. }
        | meta::SnapshotListError::InvalidCursor { .. } => invalid_argument(error.to_string()),
        meta::SnapshotListError::CursorScopeMismatch
        | meta::SnapshotListError::CursorReadVersionMismatch { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        meta::SnapshotListError::CorruptSnapshotKey | meta::SnapshotListError::SnapshotCodec(_) => {
            internal(error.to_string())
        }
    }
}

fn restore_failure(error: meta::RestoreError) -> protocol::RpcFailure {
    match error {
        meta::RestoreError::Engine(source) => engine_failure(source),
        meta::RestoreError::SourceWorkspaceMissing { .. } => failure(
            protocol::ErrorCode::NotFound,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::Workspace),
        ),
        meta::RestoreError::SnapshotMissing { .. } => failure(
            protocol::ErrorCode::NotFound,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        meta::RestoreError::CommitMissing { .. }
        | meta::RestoreError::OperationMissing { .. }
        | meta::RestoreError::RevisionMissing { .. }
        | meta::RestoreError::RestoreManifestMissing => failure(
            protocol::ErrorCode::NotFound,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::RestoreError::DestinationExists { .. }
        | meta::RestoreError::DestinationIncarnationClaimed { .. }
        | meta::RestoreError::OperationIdentityCollision { .. } => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::Workspace),
        ),
        meta::RestoreError::SnapshotLeaseExpired { .. } => failure(
            protocol::ErrorCode::SnapshotExpired,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::SnapshotLifecycle),
        ),
        meta::RestoreError::RequestInputMismatch
        | meta::RestoreError::RestoreManifestBindingMismatch { .. } => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::RestoreError::ConcurrentMutation => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::RestoreError::InvalidBatchLimit { .. } => invalid_argument(error.to_string()),
        meta::RestoreError::SourceWorkspaceMismatch
        | meta::RestoreError::DestinationMarkerMismatch
        | meta::RestoreError::SnapshotNotActive { .. }
        | meta::RestoreError::SnapshotRetentionMismatch
        | meta::RestoreError::CommitNotSealed { .. }
        | meta::RestoreError::CommitRetentionMismatch
        | meta::RestoreError::InvalidPhase { .. }
        | meta::RestoreError::SourceAlreadyExhausted
        | meta::RestoreError::SourceClosureMismatch { .. }
        | meta::RestoreError::ReservedPathInSource
        | meta::RestoreError::RevisionUnavailable { .. }
        | meta::RestoreError::ManifestBindingMismatch
        | meta::RestoreError::ManifestRevisionMismatch => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::RestoreError::CorruptKey { .. }
        | meta::RestoreError::CorruptSourceMember { .. }
        | meta::RestoreError::RevisionReferenceMissing { .. }
        | meta::RestoreError::ReferenceEpochAhead { .. }
        | meta::RestoreError::ReferenceEpochOverflow { .. }
        | meta::RestoreError::ReferenceCountOverflow { .. }
        | meta::RestoreError::ReferenceCountUnderflow { .. }
        | meta::RestoreError::ConsumerEpochOverflow
        | meta::RestoreError::ConsumerCountOverflow
        | meta::RestoreError::ConsumerCountUnderflow
        | meta::RestoreError::WorkspaceRevisionOverflow
        | meta::RestoreError::CommitVersionOverflow
        | meta::RestoreError::DuplicateCommandKey { .. }
        | meta::RestoreError::DeterministicResultMismatch { .. }
        | meta::RestoreError::RecordCodec(_)
        | meta::RestoreError::CommitCodec(_)
        | meta::RestoreError::SnapshotCodec(_)
        | meta::RestoreError::RestoreCodec(_)
        | meta::RestoreError::QueryRecord(_) => internal(error.to_string()),
    }
}

fn publication_failure(error: meta::PublicationError) -> protocol::RpcFailure {
    match error {
        meta::PublicationError::Metadata(source) => engine_failure(source),
        meta::PublicationError::WorkspaceNotFound | meta::PublicationError::PathNotFound => {
            not_found(error.to_string())
        }
        meta::PublicationError::PathAlreadyExists => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::PathGeneration),
        ),
        meta::PublicationError::PathGenerationMismatch { actual, .. } => conflict(
            protocol::ConflictKind::PathGeneration,
            error.to_string(),
            Some(actual),
        ),
        meta::PublicationError::WorkspaceIncarnationMismatch
        | meta::PublicationError::WorkspaceUnavailable => {
            conflict(protocol::ConflictKind::Workspace, error.to_string(), None)
        }
        meta::PublicationError::InvalidOperationPhase { .. } => conflict(
            protocol::ConflictKind::OperationState,
            error.to_string(),
            None,
        ),
        meta::PublicationError::RevisionClaimHeld { .. } => conflict(
            protocol::ConflictKind::OperationState,
            error.to_string(),
            None,
        ),
        meta::PublicationError::ReconcileResolutionMismatch { .. }
        | meta::PublicationError::ReconcileManifestRowsRemain { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::PublicationError::OperationInputMismatch => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::PublicationError::OperationCodec(_)
        | meta::PublicationError::RecordCodec(_)
        | meta::PublicationError::InvalidOperationSeal { .. }
        | meta::PublicationError::EmptyBatch { .. }
        | meta::PublicationError::BatchTooLarge { .. }
        | meta::PublicationError::BatchExceedsPlannedCount { .. }
        | meta::PublicationError::StagedObjectCountMismatch { .. }
        | meta::PublicationError::StagedObjectSequenceMismatch { .. }
        | meta::PublicationError::StagedObjectRevisionMismatch
        | meta::PublicationError::InvalidStagedObjectTransition { .. }
        | meta::PublicationError::StagedObjectNotUploaded { .. }
        | meta::PublicationError::ManifestCountMismatch { .. }
        | meta::PublicationError::ManifestOrder
        | meta::PublicationError::ManifestPositionMismatch
        | meta::PublicationError::ManifestDigestMismatch
        | meta::PublicationError::ManifestOwnershipMismatch { .. }
        | meta::PublicationError::DuplicateStagedObjectKey { .. }
        | meta::PublicationError::DependencyCountMismatch { .. }
        | meta::PublicationError::DependencyOrder
        | meta::PublicationError::DependencyDigestMismatch
        | meta::PublicationError::DependencyDepthMismatch { .. } => {
            invalid_argument(error.to_string())
        }
        _ => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            None,
        ),
    }
}

fn commit_failure(error: meta::CommitError) -> protocol::RpcFailure {
    match error {
        meta::CommitError::Metadata(source) => engine_failure(source),
        meta::CommitError::Namespace(source) => namespace_failure(source),
        meta::CommitError::WorkspaceNotFound
        | meta::CommitError::OperationNotFound
        | meta::CommitError::CommitNotFound { .. }
        | meta::CommitError::RevisionNotFound { .. }
        | meta::CommitError::TagNotFound => not_found(error.to_string()),
        meta::CommitError::CommitAlreadyExists => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::CommitHead),
        ),
        meta::CommitError::HeadConflict => {
            conflict(protocol::ConflictKind::CommitHead, error.to_string(), None)
        }
        meta::CommitError::PhaseMismatch { .. } => conflict(
            protocol::ConflictKind::OperationState,
            error.to_string(),
            None,
        ),
        meta::CommitError::OperationInputMismatch => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::CommitError::InvalidBatchLimit { .. } => invalid_argument(error.to_string()),
        _ => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            None,
        ),
    }
}

fn engine_failure(error: meta::AgentMetadataError) -> protocol::RpcFailure {
    match error {
        meta::AgentMetadataError::OwnerEpochMismatch { .. }
        | meta::AgentMetadataError::PlacementMismatch
        | meta::AgentMetadataError::RootFenceMissing
        | meta::AgentMetadataError::RootFenceStateMismatch { .. } => failure(
            protocol::ErrorCode::NotOwner,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::RootPlacement),
        ),
        meta::AgentMetadataError::RequestIdReused => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::AgentMetadataError::PredicateFailed
        | meta::AgentMetadataError::WriteConflict
        | meta::AgentMetadataError::WriteReadVersionMismatch { .. } => {
            failure(protocol::ErrorCode::Conflict, error.to_string(), true, None)
        }
        meta::AgentMetadataError::ReadVersionInFuture { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            true,
            None,
        ),
        meta::AgentMetadataError::InvalidCommand { .. }
        | meta::AgentMetadataError::CommandDigestMismatch => invalid_argument(error.to_string()),
        _ => internal(error.to_string()),
    }
}

fn invalid_argument(message: impl Into<String>) -> protocol::RpcFailure {
    failure(protocol::ErrorCode::InvalidArgument, message, false, None)
}

fn not_found(message: impl Into<String>) -> protocol::RpcFailure {
    failure(protocol::ErrorCode::NotFound, message, false, None)
}

fn internal(message: impl Into<String>) -> protocol::RpcFailure {
    failure(protocol::ErrorCode::Internal, message, false, None)
}

fn operation_terminal_failure(message: &str) -> protocol::RpcFailure {
    failure(
        protocol::ErrorCode::OperationFailed,
        message,
        false,
        Some(protocol::ConflictKind::OperationState),
    )
}

fn conflict(
    kind: protocol::ConflictKind,
    message: impl Into<String>,
    current_generation: Option<u64>,
) -> protocol::RpcFailure {
    let mut failure = failure(protocol::ErrorCode::Conflict, message, false, Some(kind));
    failure.current_generation = current_generation;
    failure
}

fn failure(
    code: protocol::ErrorCode,
    message: impl Into<String>,
    retryable: bool,
    conflict: Option<protocol::ConflictKind>,
) -> protocol::RpcFailure {
    protocol::RpcFailure {
        code,
        message: bounded_message(message.into()),
        retryable,
        conflict,
        current_generation: None,
        route_hint: None,
    }
}

fn bounded_message(mut message: String) -> String {
    const MAX_BYTES: usize = 4_096;
    if message.is_empty() {
        return "request failed".to_owned();
    }
    if message.len() <= MAX_BYTES {
        return message;
    }
    let mut end = MAX_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    message.truncate(end);
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_zero_byte_projection_yields_empty_index_fields() {
        assert!(decode_index_fields(&[]).unwrap().is_empty());
        assert!(
            decode_index_fields(&meta::TypedProjection::empty().encode().unwrap())
                .unwrap()
                .is_empty()
        );
        assert!(decode_index_fields(&[0xff]).is_err());
    }

    fn root() -> types::RootId {
        types::RootId::from_bytes([1; types::FIXED_ID_BYTES])
    }

    fn shard() -> types::LogicalShardId {
        types::LogicalShardId::from_bytes([2; types::FIXED_ID_BYTES])
    }

    fn placement() -> types::PlacementGeneration {
        types::PlacementGeneration::new(3).unwrap()
    }

    fn owner(value: u64) -> types::OwnerEpoch {
        types::OwnerEpoch::new(value).unwrap()
    }

    fn request_id(value: u8) -> types::RequestId {
        types::RequestId::from_bytes([value; types::FIXED_ID_BYTES])
    }

    fn route(owner_epoch: u64) -> protocol::RootRoute {
        protocol::RootRoute {
            root_id: root().into(),
            logical_shard_id: shard().into(),
            placement_generation: placement().get(),
            owner_epoch,
        }
    }

    fn fence_command(
        store: &meta::AgentMetadataStore,
        request_id: types::RequestId,
        action: meta::RootFenceAction,
        owner_epoch: types::OwnerEpoch,
    ) -> meta::MetadataCommand {
        meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch,
            request_id,
            command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: action,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal()
    }

    fn ready_executor() -> (
        Arc<meta::AgentMetadataStore>,
        MetadataWorkspaceRequestExecutor,
    ) {
        let store = Arc::new(meta::AgentMetadataStore::open_memory(shard()).unwrap());
        store.advance_owner_epoch(None, owner(1)).unwrap();
        store
            .execute(&fence_command(
                &store,
                request_id(200),
                meta::RootFenceAction::Install,
                owner(1),
            ))
            .unwrap();
        store
            .execute(&fence_command(
                &store,
                request_id(201),
                meta::RootFenceAction::Transition {
                    expected: types::RootActivationState::Installing,
                    next: types::RootActivationState::Active,
                },
                owner(1),
            ))
            .unwrap();
        let executor = MetadataWorkspaceRequestExecutor::new(Arc::clone(&store));
        (store, executor)
    }

    #[test]
    fn preflight_uses_no_workspace_state_and_reports_the_exact_route() {
        let store = Arc::new(meta::AgentMetadataStore::open_memory(shard()).unwrap());
        let version_before = store.current_read_version().unwrap();
        let executor = MetadataWorkspaceRequestExecutor::new(Arc::clone(&store));
        let request = protocol::WorkspaceRpcRequest {
            route: route(41),
            request_id: protocol::RequestIdentity([91; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Preflight(
                protocol::WorkspacePreflightRequest::new([
                    protocol::WorkspaceCapability::RestoreV1,
                    protocol::WorkspaceCapability::QueryV1,
                ]),
            ),
        };

        let outcome = executor.execute(&request).unwrap();
        assert_eq!(outcome.commit_version, None);
        assert!(!outcome.replayed);
        let protocol::WorkspaceResult::Preflight(preflight) = outcome.result else {
            panic!("preflight returned the wrong result variant");
        };
        assert_eq!(preflight.route, route(41));
        assert_eq!(
            preflight.supported_capabilities,
            SUPPORTED_WORKSPACE_CAPABILITIES
        );
        assert_eq!(store.current_read_version().unwrap(), version_before);
    }

    fn create_request(
        request_fill: u8,
        workbench: &str,
        incarnation_fill: u8,
        owner_epoch: u64,
    ) -> protocol::WorkspaceRpcRequest {
        protocol::WorkspaceRpcRequest {
            route: route(owner_epoch),
            request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::CreateWorkspace(
                protocol::CreateWorkspaceRequest {
                    workbench: protocol::WorkbenchName::new(workbench).unwrap(),
                    workspace_incarnation_id: protocol::WorkspaceIdentity(
                        [incarnation_fill; types::FIXED_ID_BYTES],
                    ),
                },
            ),
        }
    }

    fn commit_request(request_fill: u8) -> protocol::WorkspaceRpcRequest {
        protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Commit(protocol::CommitRequest {
                operation_id: protocol::OperationIdentity([0x41; types::FIXED_ID_BYTES]),
                workbench: protocol::WorkbenchName::new("commit-time-test").unwrap(),
                workspace_incarnation_id: protocol::WorkspaceIdentity(
                    [0x42; types::FIXED_ID_BYTES],
                ),
                commit_id: protocol::CommitIdentity([0x43; types::SHA256_BYTES]),
                content_digest: protocol::DigestUri::new(format!(
                    "sha256:{}",
                    "44".repeat(types::SHA256_BYTES)
                ))
                .unwrap(),
                manifest_digest: protocol::DigestUri::new(format!(
                    "sha256:{}",
                    "45".repeat(types::SHA256_BYTES)
                ))
                .unwrap(),
                projection_input_digest: protocol::Digest([0x47; types::SHA256_BYTES]),
                tree_manifest_revision_id: protocol::ArtifactRevisionIdentity(
                    [0x46; types::FIXED_ID_BYTES],
                ),
                replace: false,
                run_manifest_condition: protocol::PublishCondition::CreateOnly,
                expected_head_generation: None,
                parents: Vec::new(),
                producer: None,
                lineage_projection: Vec::new(),
            }),
        }
    }

    fn replace_visible_workspace_incarnation(
        store: &meta::AgentMetadataStore,
        workbench: &str,
        previous_incarnation: types::WorkspaceIncarnationId,
        replacement_incarnation: types::WorkspaceIncarnationId,
    ) {
        let workbench = types::WorkbenchId::new(workbench).unwrap();
        let key = meta::workspace_current_key(root(), &workbench);
        let previous = meta::WorkspaceRecord {
            incarnation_id: previous_incarnation,
            workspace_revision: types::WorkspaceRevision::ZERO,
            state: types::WorkspaceState::Visible,
            owning_operation_id: None,
        }
        .encode()
        .unwrap();
        let replacement = meta::WorkspaceRecord {
            incarnation_id: replacement_incarnation,
            workspace_revision: types::WorkspaceRevision::ZERO,
            state: types::WorkspaceState::Visible,
            owning_operation_id: None,
        }
        .encode()
        .unwrap();
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(249),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates: vec![meta::CommandPredicate::Value {
                        family: meta::MetadataFamily::WorkspaceCurrent,
                        key: key.clone(),
                        expected: Some(previous),
                    }],
                    mutations: vec![meta::CommandMutation::Put {
                        family: meta::MetadataFamily::WorkspaceCurrent,
                        key: key.clone(),
                        value: replacement.clone(),
                    }],
                    history_projection: vec![meta::HistoryProjection {
                        family: meta::MetadataFamily::WorkspaceCurrent,
                        key,
                    }],
                    event_projection: Vec::new(),
                    deterministic_result: replacement,
                }
                .seal(),
            )
            .unwrap();
    }

    fn install_terminal_commit_and_newer_head(
        store: &meta::AgentMetadataStore,
        request: &protocol::CommitRequest,
    ) {
        let operation_id: types::OperationId = request.operation_id.into();
        let commit_id: types::CommitId = request.commit_id.into();
        let tree_revision_id: types::ArtifactRevisionId = request.tree_manifest_revision_id.into();
        let member_path = types::NormalizedRelativePath::new(RUN_MANIFEST_PATH).unwrap();
        let mut operation = meta::BuildCommitOperationRecord {
            operation_id,
            identity_digest: [0; types::SHA256_BYTES],
            initialization_digest: [0; types::SHA256_BYTES],
            workbench_id: types::WorkbenchId::new(request.workbench.as_str()).unwrap(),
            source_workspace_incarnation_id: request.workspace_incarnation_id.into(),
            source_read_version: store.current_read_version().unwrap(),
            commit_id,
            expected_head: None,
            content_digest_uri: request.content_digest.as_str().to_owned(),
            manifest_digest_uri: request.manifest_digest.as_str().to_owned(),
            projection_input_digest: request.projection_input_digest.0,
            tree_manifest_revision_id: tree_revision_id,
            replace: request.replace,
            run_manifest_condition: commit_manifest_condition(request.run_manifest_condition)
                .unwrap(),
            committed_at_unix_seconds: 1_700_000_123,
            commit_staged_run_manifest: Some(meta::CommitManifestBinding {
                logical_size: 128,
                body_digest_uri: format!("sha256:{}", "51".repeat(types::SHA256_BYTES)),
                manifest_digest_uri: format!("sha256:{}", "52".repeat(types::SHA256_BYTES)),
                content_type: "application/json".to_owned(),
            }),
            producer: request.producer.clone(),
            lineage_projection: request.lineage_projection.clone(),
            parent_commits: request.parents.iter().copied().map(Into::into).collect(),
            phase: types::BuildCommitPhase::Complete,
            member_cursor: Some(member_path),
            member_count: 1,
            member_digest: [0x53; types::SHA256_BYTES],
            members_complete: true,
            revision_ref_count: 1,
            revision_cursor: Some(tree_revision_id),
            revision_seal_count: 1,
            revision_digest: [0x54; types::SHA256_BYTES],
            revisions_complete: true,
            parent_cursor: 0,
            parent_digest: [0; types::SHA256_BYTES],
            parents_complete: true,
            cleanup_member_count: 0,
            cleanup_revision_count: 0,
            cleanup_parent_count: 0,
            history_hold_released: true,
            result: Some(meta::BuildCommitResult {
                commit_id,
                head_generation: types::Generation::new(1).unwrap(),
            }),
            terminal_error: None,
        };
        operation.seal_digests();
        let operation_key =
            meta::operation_key(root(), types::OperationKind::BuildCommit, operation_id);
        let head_key = meta::workbench_commit_head_key(
            root(),
            types::WorkspaceIncarnationId::from(request.workspace_incarnation_id),
        );
        let newer_head = meta::WorkbenchCommitHeadRecord {
            commit_id: types::CommitId::from_bytes([0x71; types::SHA256_BYTES]),
            head_generation: types::Generation::new(2).unwrap(),
        }
        .encode();
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(248),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates: vec![
                        meta::CommandPredicate::Value {
                            family: meta::MetadataFamily::Operation,
                            key: operation_key.clone(),
                            expected: None,
                        },
                        meta::CommandPredicate::Value {
                            family: meta::MetadataFamily::WorkbenchCommitHead,
                            key: head_key.clone(),
                            expected: None,
                        },
                    ],
                    mutations: vec![
                        meta::CommandMutation::Put {
                            family: meta::MetadataFamily::Operation,
                            key: operation_key.clone(),
                            value: operation.encode().unwrap(),
                        },
                        meta::CommandMutation::Put {
                            family: meta::MetadataFamily::WorkbenchCommitHead,
                            key: head_key.clone(),
                            value: newer_head,
                        },
                    ],
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: b"terminal-commit-and-new-head".to_vec(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn put_visible_path(
        store: &meta::AgentMetadataStore,
        workbench_incarnation: types::WorkspaceIncarnationId,
    ) {
        let revision = types::ArtifactRevisionId::from_bytes([9; types::FIXED_ID_BYTES]);
        let path = types::NormalizedRelativePath::new("outputs/result.bin").unwrap();
        let index_field = meta::QueryFieldId::new("agent.score").unwrap();
        let index_value = meta::QueryScalar::Unsigned(7);
        let projection = meta::TypedProjection::new(BTreeMap::from([(
            index_field.clone(),
            index_value.clone(),
        )]))
        .unwrap();
        let revision_record = meta::ArtifactRevisionRecord {
            logical_size: 0,
            body_digest_uri: "sha256:body".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            block_count: 0,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; types::SHA256_BYTES],
            content_type: "application/octet-stream".to_owned(),
            state: types::RevisionState::Available,
            reference_epoch: types::ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        };
        let path_record = meta::PathEntry {
            generation: types::Generation::new(1).unwrap(),
            artifact_revision_id: revision,
            body_digest_uri: "sha256:body".to_owned(),
            manifest_digest_uri: "sha256:manifest".to_owned(),
            logical_size: 0,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: Some("executor-test".to_owned()),
            manifest_id: Some("manifest-1".to_owned()),
            typed_index_projection: projection.encode().unwrap(),
        };
        let revision_key = meta::artifact_revision_key(root(), revision);
        let path_key = meta::path_current_key(root(), workbench_incarnation, &path);
        let reference_key =
            meta::path_revision_ref_key(root(), workbench_incarnation, &path, revision);
        let index_key = meta::secondary_index_key(
            root(),
            &index_field,
            &index_value,
            workbench_incarnation,
            &path,
        );
        let command = meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch: owner(1),
            request_id: request_id(202),
            command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
            read_version: store.current_read_version().unwrap(),
            root_fence_action: meta::RootFenceAction::RequireActive,
            predicates: vec![
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::ArtifactRevision,
                    key: revision_key.clone(),
                    expected: None,
                },
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::PathCurrent,
                    key: path_key.clone(),
                    expected: None,
                },
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::RevisionRef,
                    key: reference_key.clone(),
                    expected: None,
                },
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::SecondaryIndex,
                    key: index_key.clone(),
                    expected: None,
                },
            ],
            mutations: vec![
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::ArtifactRevision,
                    key: revision_key,
                    value: revision_record.encode().unwrap(),
                },
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::PathCurrent,
                    key: path_key,
                    value: path_record.encode().unwrap(),
                },
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::RevisionRef,
                    key: reference_key,
                    value: meta::RevisionRefRecord {
                        reference_epoch_at_add: types::ReferenceEpoch::new(1),
                    }
                    .encode()
                    .unwrap(),
                },
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::SecondaryIndex,
                    key: index_key,
                    value: meta::SecondaryIndexRecord {
                        path_generation: types::Generation::new(1).unwrap(),
                        compact_projection: projection,
                    }
                    .encode()
                    .unwrap(),
                },
            ],
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: Vec::new(),
        }
        .seal();
        store.execute(&command).unwrap();
    }

    fn corrupt_visible_path_revision(store: &meta::AgentMetadataStore) {
        let revision = types::ArtifactRevisionId::from_bytes([9; types::FIXED_ID_BYTES]);
        let key = meta::artifact_revision_key(root(), revision);
        let read_version = store.current_read_version().unwrap();
        let current = store
            .read_at(
                root(),
                placement(),
                owner(1),
                meta::MetadataFamily::ArtifactRevision,
                &key,
                read_version,
            )
            .unwrap()
            .expect("seeded revision exists");
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(203),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version,
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates: vec![meta::CommandPredicate::Value {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key: key.clone(),
                        expected: Some(current),
                    }],
                    mutations: vec![meta::CommandMutation::Put {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key: key.clone(),
                        value: vec![0xff],
                    }],
                    history_projection: vec![meta::HistoryProjection {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key,
                    }],
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn put_path_projection_rows(
        store: &meta::AgentMetadataStore,
        workbench_incarnation: types::WorkspaceIncarnationId,
        paths: &[&str],
    ) {
        let mut predicates = Vec::with_capacity(paths.len());
        let mut mutations = Vec::with_capacity(paths.len());
        for (index, raw_path) in paths.iter().enumerate() {
            let path = types::NormalizedRelativePath::new(*raw_path).unwrap();
            let key = meta::path_current_key(root(), workbench_incarnation, &path);
            let fill = u8::try_from(index + 1).unwrap();
            let body_hex = format!("{fill:02x}").repeat(32);
            let manifest_fill = fill.saturating_add(0x40);
            let manifest_hex = format!("{manifest_fill:02x}").repeat(32);
            let entry = meta::PathEntry {
                generation: types::Generation::new(u64::from(fill)).unwrap(),
                artifact_revision_id: types::ArtifactRevisionId::from_bytes(
                    [fill; types::FIXED_ID_BYTES],
                ),
                body_digest_uri: format!("sha256:{body_hex}"),
                manifest_digest_uri: format!("sha256:{manifest_hex}"),
                logical_size: u64::from(fill),
                dependency_count: 0,
                dependency_depth: 0,
                content_type: "application/octet-stream".to_owned(),
                producer: Some("list-prefix-test".to_owned()),
                manifest_id: None,
                typed_index_projection: meta::TypedProjection::empty().encode().unwrap(),
            };
            predicates.push(meta::CommandPredicate::Value {
                family: meta::MetadataFamily::PathCurrent,
                key: key.clone(),
                expected: None,
            });
            mutations.push(meta::CommandMutation::Put {
                family: meta::MetadataFamily::PathCurrent,
                key,
                value: entry.encode().unwrap(),
            });
        }
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(204),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates,
                    mutations,
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn put_ranged_path(
        store: &meta::AgentMetadataStore,
        workbench_incarnation: types::WorkspaceIncarnationId,
        row_count: u64,
    ) -> protocol::WorkspacePath {
        let revision = types::ArtifactRevisionId::from_bytes([8; types::FIXED_ID_BYTES]);
        let path = types::NormalizedRelativePath::new("outputs/ranged.bin").unwrap();
        let revision_record = meta::ArtifactRevisionRecord {
            logical_size: row_count,
            body_digest_uri: "sha256:ranged-body".to_owned(),
            manifest_digest_uri: "sha256:ranged-manifest".to_owned(),
            block_count: row_count,
            dependency_count: 0,
            dependency_depth: 0,
            dependency_digest: [0; types::SHA256_BYTES],
            content_type: "application/octet-stream".to_owned(),
            state: types::RevisionState::Available,
            reference_epoch: types::ReferenceEpoch::new(1),
            strong_reference_count: 1,
            last_zero_ref_version: None,
        };
        let path_record = meta::PathEntry {
            generation: types::Generation::new(1).unwrap(),
            artifact_revision_id: revision,
            body_digest_uri: "sha256:ranged-body".to_owned(),
            manifest_digest_uri: "sha256:ranged-manifest".to_owned(),
            logical_size: row_count,
            dependency_count: 0,
            dependency_depth: 0,
            content_type: "application/octet-stream".to_owned(),
            producer: Some("executor-range-test".to_owned()),
            manifest_id: Some("manifest-range".to_owned()),
            typed_index_projection: meta::TypedProjection::empty().encode().unwrap(),
        };
        let revision_key = meta::artifact_revision_key(root(), revision);
        let path_key = meta::path_current_key(root(), workbench_incarnation, &path);
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(202),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates: vec![
                        meta::CommandPredicate::Value {
                            family: meta::MetadataFamily::ArtifactRevision,
                            key: revision_key.clone(),
                            expected: None,
                        },
                        meta::CommandPredicate::Value {
                            family: meta::MetadataFamily::PathCurrent,
                            key: path_key.clone(),
                            expected: None,
                        },
                    ],
                    mutations: vec![
                        meta::CommandMutation::Put {
                            family: meta::MetadataFamily::ArtifactRevision,
                            key: revision_key,
                            value: revision_record.encode().unwrap(),
                        },
                        meta::CommandMutation::Put {
                            family: meta::MetadataFamily::PathCurrent,
                            key: path_key,
                            value: path_record.encode().unwrap(),
                        },
                    ],
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();

        for (batch, first) in (0..row_count).step_by(128).enumerate() {
            let end = first.saturating_add(128).min(row_count);
            let mut predicates = Vec::new();
            let mut mutations = Vec::new();
            for object_index in first..end {
                let key = meta::artifact_manifest_key(root(), revision, object_index);
                let row = meta::ArtifactManifestRow {
                    physical_owner_revision_id: revision,
                    physical_object_index: object_index,
                    object_key: meta::object_block_key(shard(), root(), revision, object_index),
                    logical_offset: object_index,
                    offset: 0,
                    length: 1,
                    digest_uri: format!("sha256:{object_index:064x}"),
                    append_segment: None,
                };
                predicates.push(meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::ArtifactManifest,
                    key: key.clone(),
                    expected: None,
                });
                mutations.push(meta::CommandMutation::Put {
                    family: meta::MetadataFamily::ArtifactManifest,
                    key,
                    value: row.encode().unwrap(),
                });
            }
            store
                .execute(
                    &meta::MetadataCommand {
                        schema_id: meta::SCHEMA_ID.to_owned(),
                        root_id: root(),
                        logical_shard_id: shard(),
                        placement_generation: placement(),
                        owner_epoch: owner(1),
                        request_id: request_id(
                            203_u8.checked_add(u8::try_from(batch).unwrap()).unwrap(),
                        ),
                        command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                        read_version: store.current_read_version().unwrap(),
                        root_fence_action: meta::RootFenceAction::RequireActive,
                        predicates,
                        mutations,
                        history_projection: Vec::new(),
                        event_projection: Vec::new(),
                        deterministic_result: Vec::new(),
                    }
                    .seal(),
                )
                .unwrap();
        }

        protocol::WorkspacePath {
            workbench: protocol::WorkbenchName::new("range-test").unwrap(),
            path: protocol::RelativePath::new(path.as_str()).unwrap(),
        }
    }

    #[test]
    fn derived_request_ids_are_step_and_sequence_separated() {
        let request = protocol::RequestIdentity([7; types::FIXED_ID_BYTES]);
        assert_ne!(
            derived_request_id(request, b"commit-members", 0),
            derived_request_id(request, b"commit-members", 1)
        );
        assert_ne!(
            derived_request_id(request, b"commit-members", 0),
            derived_request_id(request, b"commit-revisions", 0)
        );
    }

    #[test]
    fn sealed_restore_preparation_is_stable_through_ready_and_complete() {
        let destination_incarnation =
            types::WorkspaceIncarnationId::from_bytes([0x52; types::FIXED_ID_BYTES]);
        let member_digest = [0x33; types::SHA256_BYTES];
        let mut identity_digest = [0x11; types::SHA256_BYTES];
        identity_digest[..types::FIXED_ID_BYTES].fill(0x41);
        let mut operation = meta::RestoreOperationRecord {
            operation_id: types::OperationId::from_bytes([0x41; types::FIXED_ID_BYTES]),
            identity_digest,
            initialization_digest: Some([0x22; types::SHA256_BYTES]),
            source_workbench_id: types::WorkbenchId::new("restore-source").unwrap(),
            source_workspace_incarnation_id: types::WorkspaceIncarnationId::from_bytes(
                [0x42; types::FIXED_ID_BYTES],
            ),
            source: meta::RestoreSource::Snapshot {
                snapshot_id: types::SnapshotId::new(7),
                read_version: types::ReadVersion::new(1).unwrap(),
            },
            destination_workbench_id: types::WorkbenchId::new("restore-destination").unwrap(),
            destination_workspace_incarnation_id: destination_incarnation,
            restore_manifest: meta::RestoreManifestDescriptor {
                body_digest_uri: format!("sha256:{}", "00".repeat(32)),
                logical_size: 2,
                content_type: meta::RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            phase: types::RestorePhase::Ready,
            source_cursor: None,
            source_eof: true,
            next_member_sequence: 0,
            member_rolling_digest: member_digest,
            member_seal: Some(member_digest),
            cleanup_member_cursor: 0,
            result: None,
            terminal_error: None,
        };
        operation.validate().unwrap();
        let ready = sealed_restore_preparation(&operation).unwrap();

        operation.phase = types::RestorePhase::Complete;
        operation.result = Some(meta::RestoreResult {
            destination_workspace_incarnation_id: destination_incarnation,
            destination_workspace_revision: types::WorkspaceRevision::new(1),
            member_count: 0,
            member_digest,
        });
        operation.validate().unwrap();
        let complete = sealed_restore_preparation(&operation).unwrap();

        assert_eq!(ready, complete);
        assert_eq!(ready.operation_id.0, [0x41; types::FIXED_ID_BYTES]);
        assert_eq!(ready.destination_workbench.as_str(), "restore-destination");
        assert_eq!(ready.member_count, 0);
        assert_eq!(ready.member_digest, protocol::Digest(member_digest));
    }

    #[test]
    fn restore_not_found_failures_keep_typed_conflict_provenance() {
        let snapshot = restore_failure(meta::RestoreError::SnapshotMissing {
            snapshot_id: types::SnapshotId::new(7),
        });
        assert_eq!(snapshot.code, protocol::ErrorCode::NotFound);
        assert_eq!(
            snapshot.conflict,
            Some(protocol::ConflictKind::SnapshotLifecycle)
        );

        let workspace = restore_failure(meta::RestoreError::SourceWorkspaceMissing {
            workbench_id: types::WorkbenchId::new("source").unwrap(),
        });
        assert_eq!(workspace.code, protocol::ErrorCode::NotFound);
        assert_eq!(workspace.conflict, Some(protocol::ConflictKind::Workspace));

        let operation = restore_failure(meta::RestoreError::RestoreManifestMissing);
        assert_eq!(operation.code, protocol::ErrorCode::NotFound);
        assert_eq!(
            operation.conflict,
            Some(protocol::ConflictKind::OperationState)
        );

        let claimed = restore_failure(meta::RestoreError::DestinationIncarnationClaimed {
            incarnation_id: types::WorkspaceIncarnationId::from_bytes([9; types::FIXED_ID_BYTES]),
            workbench_id: types::WorkbenchId::new("existing").unwrap(),
        });
        assert_eq!(claimed.code, protocol::ErrorCode::AlreadyExists);
        assert_eq!(claimed.conflict, Some(protocol::ConflictKind::Workspace));
        assert!(!claimed.retryable);
    }

    #[test]
    fn create_crosses_real_fence_and_replays_exact_request() {
        let (_store, executor) = ready_executor();
        let request = create_request(10, "run-a", 11, 1);
        let created = executor.execute(&request).unwrap();
        assert!(!created.replayed);
        assert!(created.commit_version.is_some());

        let replayed = executor.execute(&request).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, created.result);

        let reused = create_request(10, "run-b", 12, 1);
        let failure = executor.execute(&reused).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::RequestReplayMismatch);
    }

    #[test]
    fn create_incarnation_conflict_keeps_rpc_identity_and_message_provenance() {
        let (_store, executor) = ready_executor();
        executor
            .execute(&create_request(10, "run-a", 11, 1))
            .unwrap();
        let claimed_incarnation = create_request(11, "run-b", 11, 1);
        let failure = executor.execute(&claimed_incarnation).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::AlreadyExists);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::Workspace));
        assert!(!failure.retryable);
        assert!(failure
            .message
            .contains("permanently claimed by workbench run-a"));
        assert!(!failure.message.contains("workbench run-b already exists"));
    }

    #[test]
    fn commit_prepare_replays_first_durable_time_after_clock_moves_forward() {
        let (store, executor) = ready_executor();
        executor
            .execute(&create_request(40, "commit-time-test", 0x42, 1))
            .unwrap();

        let first = executor.execute(&commit_request(41)).unwrap();
        let protocol::WorkspaceResult::Operation(first_status) = first.result else {
            panic!("commit prepare returned the wrong result variant");
        };
        let first_preparation = first_status
            .commit_preparation
            .clone()
            .expect("commit status carries durable preparation");
        assert_eq!(first_status.state, protocol::OperationState::Running);

        store
            .observe_lease_clock(
                root(),
                placement(),
                owner(1),
                first_preparation
                    .committed_at_unix_seconds
                    .checked_add(120)
                    .unwrap()
                    .checked_mul(1_000)
                    .unwrap(),
            )
            .unwrap();

        let replay = executor.execute(&commit_request(42)).unwrap();
        assert!(replay.replayed);
        let protocol::WorkspaceResult::Operation(replay_status) = replay.result else {
            panic!("commit replay returned the wrong result variant");
        };
        assert_eq!(replay_status.state, protocol::OperationState::Running);
        assert_eq!(replay_status.commit_preparation, Some(first_preparation));
    }

    #[test]
    fn commit_prepare_rejects_a_changed_projection_input_digest() {
        let (_store, executor) = ready_executor();
        executor
            .execute(&create_request(70, "commit-time-test", 0x42, 1))
            .unwrap();
        let first = executor.execute(&commit_request(71)).unwrap();
        let protocol::WorkspaceResult::Operation(first_status) = first.result else {
            panic!("commit prepare returned the wrong result variant");
        };
        assert_eq!(first_status.state, protocol::OperationState::Running);
        let first_preparation = first_status.commit_preparation.unwrap();
        assert!(first_preparation.manifest.is_none());

        let mut mismatch = commit_request(72);
        let protocol::WorkspaceRequest::Commit(request) = &mut mismatch.operation else {
            unreachable!("commit_request always creates a commit request");
        };
        request.projection_input_digest.0[0] ^= 0xff;
        let failure = executor.execute(&mismatch).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::RequestReplayMismatch);
        assert!(!failure.retryable);

        let status = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([73; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::GetOperation(
                    protocol::GetOperationRequest {
                        operation_id: first_preparation.request.operation_id,
                    },
                ),
            })
            .unwrap();
        let protocol::WorkspaceResult::Operation(status) = status.result else {
            panic!("get operation returned the wrong result variant");
        };
        assert_eq!(status.state, protocol::OperationState::Running);
        assert_eq!(status.commit_preparation, Some(first_preparation));
    }

    #[test]
    fn terminal_commit_replay_returns_the_original_result_after_the_head_advanced() {
        for replace in [false, true] {
            let (store, executor) = ready_executor();
            executor
                .execute(&create_request(60, "commit-time-test", 0x42, 1))
                .unwrap();
            let mut replay = commit_request(61);
            let exact_request = match &mut replay.operation {
                protocol::WorkspaceRequest::Commit(request) => {
                    request.replace = replace;
                    request.clone()
                }
                _ => unreachable!("commit_request always creates a commit request"),
            };
            install_terminal_commit_and_newer_head(&store, &exact_request);

            let outcome = executor.execute(&replay).unwrap();
            assert!(outcome.replayed);
            let protocol::WorkspaceResult::Operation(status) = outcome.result else {
                panic!("terminal commit replay returned the wrong result variant");
            };
            assert_eq!(status.state, protocol::OperationState::Succeeded);
            assert_eq!(
                status
                    .commit_preparation
                    .as_ref()
                    .map(|preparation| preparation.request.as_ref()),
                Some(&exact_request)
            );
            let Some(protocol::OperationResult::Commit(result)) = status.result else {
                panic!("terminal commit replay omitted its original result");
            };
            assert_eq!(result.commit_id, exact_request.commit_id);
            assert_eq!(result.head_generation, 1);
        }
    }

    #[test]
    fn commit_prepare_rejects_replay_after_the_workbench_name_is_rebound() {
        let (store, executor) = ready_executor();
        let original_incarnation =
            types::WorkspaceIncarnationId::from_bytes([0x42; types::FIXED_ID_BYTES]);
        let replacement_incarnation =
            types::WorkspaceIncarnationId::from_bytes([0x52; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(50, "commit-time-test", 0x42, 1))
            .unwrap();

        let first = executor.execute(&commit_request(51)).unwrap();
        let protocol::WorkspaceResult::Operation(first_status) = first.result else {
            panic!("commit prepare returned the wrong result variant");
        };
        let first_preparation = first_status
            .commit_preparation
            .clone()
            .expect("commit status carries durable preparation");
        assert_eq!(first_status.state, protocol::OperationState::Running);

        replace_visible_workspace_incarnation(
            &store,
            "commit-time-test",
            original_incarnation,
            replacement_incarnation,
        );
        let mut replay = commit_request(52);
        let protocol::WorkspaceRequest::Commit(commit) = &mut replay.operation else {
            unreachable!("commit_request always creates a commit request");
        };
        commit.workspace_incarnation_id = replacement_incarnation.into();

        let failure = executor.execute(&replay).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::RequestReplayMismatch);
        assert!(!failure.retryable);

        let status = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([53; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::GetOperation(
                    protocol::GetOperationRequest {
                        operation_id: protocol::OperationIdentity([0x41; types::FIXED_ID_BYTES]),
                    },
                ),
            })
            .unwrap();
        let protocol::WorkspaceResult::Operation(status) = status.result else {
            panic!("get operation returned the wrong result variant");
        };
        assert_eq!(status.state, protocol::OperationState::Running);
        assert_eq!(status.commit_preparation, Some(first_preparation));
    }

    #[test]
    fn metadata_commit_boundary_rejects_stale_owner_after_registry_admission() {
        let (store, executor) = ready_executor();
        store.advance_owner_epoch(Some(owner(1)), owner(2)).unwrap();

        let failure = executor
            .execute(&create_request(20, "stale-owner", 21, 1))
            .unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::NotOwner);
        assert!(failure.retryable);
        assert_eq!(
            failure.conflict,
            Some(protocol::ConflictKind::RootPlacement)
        );
    }

    #[test]
    fn live_get_and_list_read_real_metadata_records() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([31; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(30, "read-test", 31, 1))
            .unwrap();
        put_visible_path(&store, incarnation);
        // Metadata-only reads are authoritative from PathCurrent. Corrupting
        // the separate revision-lifetime row would have failed the old
        // per-entry fanout path, but must not affect stat/list shaping.
        corrupt_visible_path_revision(&store);

        let target = protocol::WorkspacePath {
            workbench: protocol::WorkbenchName::new("read-test").unwrap(),
            path: protocol::RelativePath::new("outputs/result.bin").unwrap(),
        };
        let get = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([32; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetPath(protocol::GetPathRequest {
                target,
                view: protocol::WorkspaceReadView::Live,
                range: None,
                plan_page: None,
                if_none_match: None,
            }),
        };
        let read = executor.execute(&get).unwrap();
        let protocol::WorkspaceResult::Path(path) = read.result else {
            panic!("get_path returned the wrong result variant");
        };
        let metadata = path.metadata.expect("live path has metadata");
        assert_eq!(metadata.generation, 1);
        assert_eq!(metadata.dependency_count, 0);
        assert_eq!(metadata.dependency_depth, 0);
        assert_eq!(metadata.descriptor.logical_size, 0);
        assert_eq!(
            metadata.descriptor.producer.as_deref(),
            Some("executor-test")
        );
        assert_eq!(
            metadata.descriptor.index_fields,
            vec![protocol::FieldValue {
                field_id: "agent.score".to_owned(),
                value: protocol::ScalarValue::Unsigned(7),
            }]
        );

        let list = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([33; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                workbench: protocol::WorkbenchName::new("read-test").unwrap(),
                prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                recursive: true,
                view: protocol::WorkspaceReadView::Live,
                expected_read_version: None,
                page: protocol::PageRequest {
                    cursor: None,
                    limit: 10,
                },
            }),
        };
        let listed = executor.execute(&list).unwrap();
        let protocol::WorkspaceResult::Paths(paths) = listed.result else {
            panic!("list_paths returned the wrong result variant");
        };
        assert_eq!(
            paths.entries,
            vec![protocol::PathListEntry::Artifact(metadata)]
        );
        assert!(paths.next_cursor.is_none());

        let stale_list = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([34; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                workbench: protocol::WorkbenchName::new("read-test").unwrap(),
                prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                recursive: true,
                view: protocol::WorkspaceReadView::Live,
                expected_read_version: Some(paths.read_version + 1),
                page: protocol::PageRequest {
                    cursor: None,
                    limit: 10,
                },
            }),
        };
        let failure = executor.execute(&stale_list).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::ReadVersion));
    }

    #[test]
    fn list_pushes_down_component_prefix_and_preserves_exact_prefix_order() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([35; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(35, "prefix-test", 35, 1))
            .unwrap();
        put_path_projection_rows(
            &store,
            incarnation,
            &[
                "outputs",
                "outputs/a/deep.bin",
                "outputs/a",
                "outputs/b",
                "outputs2/outside.bin",
            ],
        );

        let list = |request_fill: u8,
                    cursor: Option<Vec<u8>>,
                    expected_read_version,
                    limit,
                    recursive| {
            executor
                .execute(&protocol::WorkspaceRpcRequest {
                    route: route(1),
                    request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
                    operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                        workbench: protocol::WorkbenchName::new("prefix-test").unwrap(),
                        prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                        recursive,
                        view: protocol::WorkspaceReadView::Live,
                        expected_read_version,
                        page: protocol::PageRequest { cursor, limit },
                    }),
                })
                .unwrap()
        };
        let paths = |result: ExecutedRequest| {
            let protocol::WorkspaceResult::Paths(page) = result.result else {
                panic!("list_paths returned the wrong result variant");
            };
            page
        };

        let first = paths(list(36, None, None, 2, true));
        assert_eq!(
            first
                .entries
                .iter()
                .map(|entry| entry.path().path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/a/deep.bin", "outputs/a"]
        );
        let unfenced_continuation = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0xf0; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                    workbench: protocol::WorkbenchName::new("prefix-test").unwrap(),
                    prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                    recursive: true,
                    view: protocol::WorkspaceReadView::Live,
                    expected_read_version: None,
                    page: protocol::PageRequest {
                        cursor: first.next_cursor.clone(),
                        limit: 2,
                    },
                }),
            })
            .unwrap_err();
        assert_eq!(
            unfenced_continuation.code,
            protocol::ErrorCode::InvalidArgument
        );
        assert!(unfenced_continuation
            .message
            .contains("expected_read_version"));

        let second = paths(list(
            37,
            first.next_cursor.clone(),
            Some(first.read_version),
            2,
            true,
        ));
        assert_eq!(
            second
                .entries
                .iter()
                .map(|entry| entry.path().path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/b", "outputs"]
        );
        assert!(second.next_cursor.is_none());
        assert_eq!(second.read_version, first.read_version);

        let non_recursive = paths(list(38, None, None, 10, false));
        assert_eq!(
            non_recursive
                .entries
                .iter()
                .map(|entry| entry.path().path.as_str())
                .collect::<Vec<_>>(),
            ["outputs/a", "outputs/b", "outputs"]
        );
        assert!(non_recursive.next_cursor.is_none());

        let invalid_level = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([39; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                    workbench: protocol::WorkbenchName::new("prefix-test").unwrap(),
                    prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                    recursive: false,
                    view: protocol::WorkspaceReadView::Live,
                    expected_read_version: Some(first.read_version),
                    page: protocol::PageRequest {
                        cursor: Some(b"outputs/a/deep.bin".to_vec()),
                        limit: 2,
                    },
                }),
            })
            .unwrap_err();
        assert_eq!(invalid_level.code, protocol::ErrorCode::InvalidArgument);
        assert_eq!(
            invalid_level.message,
            "list_paths cursor does not belong to the requested listing level"
        );
    }

    #[test]
    fn non_recursive_limit_one_pages_coalesced_children_and_exact_prefix() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([36; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(60, "direct-page-test", 36, 1))
            .unwrap();
        put_path_projection_rows(
            &store,
            incarnation,
            &[
                "parent",
                "parent/a/deep",
                "parent/a",
                "parent/ab",
                "parent/b/deep",
            ],
        );

        let mut cursor = None;
        let mut listed = Vec::new();
        let mut kinds = Vec::new();
        let mut read_version = None;
        for request_fill in 61..=64 {
            let result = executor
                .execute(&protocol::WorkspaceRpcRequest {
                    route: route(1),
                    request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
                    operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                        workbench: protocol::WorkbenchName::new("direct-page-test").unwrap(),
                        prefix: Some(protocol::RelativePath::new("parent").unwrap()),
                        recursive: false,
                        view: protocol::WorkspaceReadView::Live,
                        expected_read_version: read_version,
                        page: protocol::PageRequest { cursor, limit: 1 },
                    }),
                })
                .unwrap();
            let protocol::WorkspaceResult::Paths(page) = result.result else {
                panic!("list_paths returned the wrong result variant");
            };
            read_version.get_or_insert(page.read_version);
            assert_eq!(Some(page.read_version), read_version);
            assert_eq!(page.entries.len(), 1);
            let entry = &page.entries[0];
            listed.push(entry.path().path.as_str().to_owned());
            kinds.push(matches!(entry, protocol::PathListEntry::Artifact(_)));
            cursor = page.next_cursor;
        }

        assert_eq!(listed, ["parent/a", "parent/ab", "parent/b", "parent"]);
        assert_eq!(kinds, [true, true, false, true]);
        assert!(cursor.is_none());
    }

    #[test]
    fn root_list_without_a_cursor_scans_the_visible_workspace() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([39; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(39, "root-list-test", 39, 1))
            .unwrap();
        put_path_projection_rows(&store, incarnation, &["alpha", "nested/value", "omega"]);

        let list = |request_fill: u8, recursive| {
            executor
                .execute(&protocol::WorkspaceRpcRequest {
                    route: route(1),
                    request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
                    operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                        workbench: protocol::WorkbenchName::new("root-list-test").unwrap(),
                        prefix: None,
                        recursive,
                        view: protocol::WorkspaceReadView::Live,
                        expected_read_version: None,
                        page: protocol::PageRequest {
                            cursor: None,
                            limit: 10,
                        },
                    }),
                })
                .unwrap()
        };
        let paths = |result: ExecutedRequest| {
            let protocol::WorkspaceResult::Paths(page) = result.result else {
                panic!("list_paths returned the wrong result variant");
            };
            page
        };

        let recursive = paths(list(40, true));
        assert_eq!(
            recursive
                .entries
                .iter()
                .map(|entry| entry.path().path.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "nested/value", "omega"]
        );
        assert!(recursive.next_cursor.is_none());

        let direct = paths(list(41, false));
        assert_eq!(
            direct
                .entries
                .iter()
                .map(|entry| entry.path().path.as_str())
                .collect::<Vec<_>>(),
            ["alpha", "nested", "omega"]
        );
        assert!(matches!(
            direct.entries[1],
            protocol::PathListEntry::Prefix(_)
        ));
        assert!(direct.next_cursor.is_none());
    }

    #[test]
    fn published_index_fields_cross_workspace_rpc_and_query_facade() {
        let (_store, executor) = ready_executor();
        executor
            .execute(&create_request(40, "query-test", 41, 1))
            .unwrap();
        let operation_id = protocol::OperationIdentity([0x51; types::FIXED_ID_BYTES]);
        let artifact_revision_id =
            protocol::ArtifactRevisionIdentity([0x52; types::FIXED_ID_BYTES]);
        let target = protocol::WorkspacePath {
            workbench: protocol::WorkbenchName::new("query-test").unwrap(),
            path: protocol::RelativePath::new("outputs/result.bin").unwrap(),
        };
        let index_fields = vec![protocol::FieldValue {
            field_id: "agent.score".to_owned(),
            value: protocol::ScalarValue::Unsigned(7),
        }];
        let seals = protocol::seal_artifact_publish_plan(artifact_revision_id, &[], &[]).unwrap();
        let begun = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x53; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::BeginArtifactPublish(
                    protocol::BeginArtifactPublishRequest {
                        operation_id,
                        artifact_revision_id,
                        target: target.clone(),
                        authority: protocol::PublicationAuthority::Visible,
                        condition: protocol::PublishCondition::CreateOnly,
                        staged_object_count: seals.staged_object_count,
                        staged_object_seal: seals.staged_object_seal,
                        manifest_row_count: seals.manifest_row_count,
                        manifest_seal: seals.manifest_seal,
                        dependency_owner_revision_ids: Vec::new(),
                    },
                ),
            })
            .unwrap();
        let protocol::WorkspaceResult::Operation(status) = begun.result else {
            panic!("begin artifact publication returned the wrong result variant");
        };
        assert_eq!(status.state, protocol::OperationState::Running);
        assert_eq!(status.progress.completed_rows, 0);
        assert_eq!(status.progress.total_rows, Some(0));

        let completed = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x54; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::CompleteArtifactPublish(
                    protocol::CompleteArtifactPublishRequest {
                        token: status.token,
                        artifact: protocol::ArtifactDescriptor {
                            logical_size: 0,
                            body_digest: protocol::sha256_digest_uri(protocol::Digest(
                                Sha256::digest([]).into(),
                            )),
                            manifest_digest: protocol::sha256_digest_uri(seals.manifest_seal),
                            content_type: protocol::ContentType::new("application/octet-stream")
                                .unwrap(),
                            producer: Some("executor-test".to_owned()),
                            manifest_identity: Some("manifest-1".to_owned()),
                            index_fields: index_fields.clone(),
                        },
                    },
                ),
            })
            .unwrap();
        let protocol::WorkspaceResult::Published(published) = completed.result else {
            panic!("complete artifact publication returned the wrong result variant");
        };
        assert_eq!(published.target, target);

        let search = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x55; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                scope: protocol::QueryScope::Root { path_prefix: None },
                predicates: vec![protocol::QueryPredicate {
                    field_id: "agent.score".to_owned(),
                    operator: protocol::QueryOperator::Equal,
                    operand: protocol::QueryOperand::Scalar(protocol::ScalarValue::Unsigned(7)),
                }],
                projection: vec!["agent.score".to_owned()],
                sort: vec![protocol::SortField {
                    field_id: "path".to_owned(),
                    direction: protocol::SortDirection::Ascending,
                }],
                facets: Vec::new(),
                page: protocol::PageRequest {
                    cursor: None,
                    limit: 10,
                },
            }),
        };
        let searched = executor.execute(&search).unwrap();
        let protocol::WorkspaceResult::Search(result) = searched.result else {
            panic!("search returned the wrong result variant");
        };
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].projection, index_fields);
        assert_eq!(
            result.hits[0].metadata.descriptor.index_fields,
            index_fields
        );
        assert_eq!(
            result.hits[0].metadata.path.path.as_str(),
            "outputs/result.bin"
        );
        assert!(result.next_cursor.is_none());
        assert!(result.read_version > 0);

        let catalog = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x56; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                scope: protocol::QueryScope::Workspace {
                    workbench: protocol::WorkbenchName::new("query-test").unwrap(),
                    path_prefix: None,
                },
                field_prefix: None,
                page: protocol::PageRequest {
                    cursor: None,
                    limit: 1,
                },
            }),
        };
        let first = executor.execute(&catalog).unwrap();
        let protocol::WorkspaceResult::Catalog(first_page) = first.result else {
            panic!("catalog returned the wrong result variant");
        };
        assert!(first_page.read_version > 0);
        let cursor = first_page
            .next_cursor
            .expect("one-row catalog page has a continuation");

        executor
            .execute(&create_request(0x57, "query-version-advance", 0x58, 1))
            .unwrap();
        let stale_catalog = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x59; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                scope: protocol::QueryScope::Workspace {
                    workbench: protocol::WorkbenchName::new("query-test").unwrap(),
                    path_prefix: None,
                },
                field_prefix: None,
                page: protocol::PageRequest {
                    cursor: Some(cursor),
                    limit: 1,
                },
            }),
        };
        let failure = executor.execute(&stale_catalog).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::ReadVersion));
    }

    #[test]
    fn reconcile_quarantined_publish_rpc_resolves_operation_and_frees_revision() {
        let (store, executor) = ready_executor();
        executor
            .execute(&create_request(0x70, "reconcile-test", 0x71, 1))
            .unwrap();
        let operation_identity = protocol::OperationIdentity([0x72; types::FIXED_ID_BYTES]);
        let revision_identity = protocol::ArtifactRevisionIdentity([0x73; types::FIXED_ID_BYTES]);
        let revision_id: types::ArtifactRevisionId = revision_identity.into();
        let object_key = meta::object_block_key(shard(), root(), revision_id, 0);
        let body_digest =
            protocol::sha256_digest_uri(protocol::Digest(Sha256::digest([0x61]).into()));
        let staged_objects = vec![protocol::StagedObject {
            sequence: 0,
            object_identity: protocol::ObjectIdentity::new(object_key.clone()).unwrap(),
            expected_length: 1,
            expected_digest: body_digest.clone(),
            multipart_token: None,
        }];
        let manifest_rows = vec![protocol::ArtifactManifestRow {
            object_index: 0,
            physical_object_index: 0,
            logical_offset: 0,
            physical_owner_revision_id: revision_identity,
            object_identity: protocol::ObjectIdentity::new(object_key).unwrap(),
            object_offset: 0,
            length: 1,
            digest: body_digest.clone(),
            append_segment: None,
        }];
        let seals = protocol::seal_artifact_publish_plan(
            revision_identity,
            &staged_objects,
            &manifest_rows,
        )
        .unwrap();
        let rpc = |fill: u8, operation: protocol::WorkspaceRequest| protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([fill; types::FIXED_ID_BYTES]),
            operation,
        };
        let operation_status = |result: ExecutedRequest| {
            let protocol::WorkspaceResult::Operation(status) = result.result else {
                panic!("publish request returned the wrong result variant");
            };
            status
        };
        let status = operation_status(
            executor
                .execute(&rpc(
                    0x74,
                    protocol::WorkspaceRequest::BeginArtifactPublish(
                        protocol::BeginArtifactPublishRequest {
                            operation_id: operation_identity,
                            artifact_revision_id: revision_identity,
                            target: protocol::WorkspacePath {
                                workbench: protocol::WorkbenchName::new("reconcile-test").unwrap(),
                                path: protocol::RelativePath::new("outputs/quarantine.bin")
                                    .unwrap(),
                            },
                            authority: protocol::PublicationAuthority::Visible,
                            condition: protocol::PublishCondition::CreateOnly,
                            staged_object_count: seals.staged_object_count,
                            staged_object_seal: seals.staged_object_seal,
                            manifest_row_count: seals.manifest_row_count,
                            manifest_seal: seals.manifest_seal,
                            dependency_owner_revision_ids: Vec::new(),
                        },
                    ),
                ))
                .unwrap(),
        );
        let status = operation_status(
            executor
                .execute(&rpc(
                    0x75,
                    protocol::WorkspaceRequest::StageArtifactObjects(
                        protocol::StageArtifactObjectsRequest {
                            token: status.token,
                            objects: staged_objects,
                        },
                    ),
                ))
                .unwrap(),
        );
        let status = operation_status(
            executor
                .execute(&rpc(
                    0x76,
                    protocol::WorkspaceRequest::MarkArtifactObjectsUploaded(
                        protocol::MarkArtifactObjectsUploadedRequest {
                            token: status.token,
                            objects: vec![protocol::ObjectUploadProof {
                                sequence: 0,
                                observed_length: 1,
                                observed_digest: body_digest,
                            }],
                        },
                    ),
                ))
                .unwrap(),
        );
        let status = operation_status(
            executor
                .execute(&rpc(
                    0x77,
                    protocol::WorkspaceRequest::StageArtifactManifest(
                        protocol::StageArtifactManifestRequest {
                            token: status.token,
                            rows: manifest_rows,
                            dependency_owner_revision_ids: Vec::new(),
                        },
                    ),
                ))
                .unwrap(),
        );
        let aborting_status = operation_status(
            executor
                .execute(&rpc(
                    0x78,
                    protocol::WorkspaceRequest::AbortArtifactPublish(
                        protocol::AbortArtifactPublishRequest {
                            token: status.token,
                            reason: "orchestrator cancelled the publication".to_owned(),
                        },
                    ),
                ))
                .unwrap(),
        );
        assert_eq!(aborting_status.state, protocol::OperationState::Aborting);

        // Drive the durable state machine into Quarantined the way lifecycle
        // cleanup does when the provider deletion outcome is ambiguous.
        let service = meta::PublicationService::new(&store);
        let meta_context = |fill: u8| meta::PublicationContext {
            root_id: root(),
            logical_shard_id: shard(),
            placement_generation: placement(),
            owner_epoch: owner(1),
            request_id: request_id(fill),
            read_version: store.current_read_version().unwrap(),
        };
        let load_operation = || {
            let key = meta::operation_key(
                root(),
                types::OperationKind::Publish,
                operation_identity.into(),
            );
            let payload = store
                .read_at(
                    root(),
                    placement(),
                    owner(1),
                    meta::MetadataFamily::Operation,
                    &key,
                    store.current_read_version().unwrap(),
                )
                .unwrap()
                .expect("publish operation row exists");
            meta::PublishOperationRecord::decode(&payload).unwrap()
        };
        let cleaning = service
            .transition_publish(meta::TransitionPublishRequest {
                context: meta_context(0xA0),
                expected_operation: load_operation(),
                transition: meta::PublishTransition::BeginCleaning,
            })
            .unwrap()
            .operation;
        service
            .transition_publish(meta::TransitionPublishRequest {
                context: meta_context(0xA1),
                expected_operation: cleaning,
                transition: meta::PublishTransition::Quarantine {
                    terminal_error: meta::PublishTerminalError {
                        kind: meta::PublishTerminalErrorKind::CleanupFailed,
                        message: "provider cleanup outcome is ambiguous".to_owned(),
                        evidence_digest: Some(Sha256::digest(b"ambiguous delete").into()),
                    },
                },
            })
            .unwrap();
        let claim_key = meta::artifact_revision_claim_key(root(), revision_id);
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(1),
                meta::MetadataFamily::ArtifactRevision,
                &claim_key,
                store.current_read_version().unwrap(),
            )
            .unwrap()
            .is_some());

        // The verdict binds to the exact quarantined payload: a token that
        // digests any earlier operation state is refused.
        let stale = executor
            .execute(&rpc(
                0x79,
                protocol::WorkspaceRequest::ReconcileQuarantinedArtifactPublish(
                    protocol::ReconcileQuarantinedArtifactPublishRequest {
                        token: aborting_status.token,
                        resolution: protocol::QuarantineResolution::ProviderObjectsAbsent,
                        reason: "stale operator view".to_owned(),
                        evidence_digest: protocol::Digest([3; types::SHA256_BYTES]),
                    },
                ),
            ))
            .unwrap_err();
        assert_eq!(stale.code, protocol::ErrorCode::Conflict);

        let quarantined_status = operation_status(
            executor
                .execute(&rpc(
                    0x7A,
                    protocol::WorkspaceRequest::GetOperation(protocol::GetOperationRequest {
                        operation_id: operation_identity,
                    }),
                ))
                .unwrap(),
        );
        assert_eq!(
            quarantined_status.state,
            protocol::OperationState::Quarantined
        );

        let reconcile = rpc(
            0x7B,
            protocol::WorkspaceRequest::ReconcileQuarantinedArtifactPublish(
                protocol::ReconcileQuarantinedArtifactPublishRequest {
                    token: quarantined_status.token,
                    resolution: protocol::QuarantineResolution::ProviderObjectsAbsent,
                    reason: "verified every staged key absent at the provider".to_owned(),
                    evidence_digest: protocol::Digest([7; types::SHA256_BYTES]),
                },
            ),
        );
        let resolved = executor.execute(&reconcile).unwrap();
        assert!(!resolved.replayed);
        let resolved_status = operation_status(resolved);
        assert_eq!(resolved_status.state, protocol::OperationState::Failed);
        let failure_body = resolved_status
            .failure
            .as_ref()
            .expect("reconciled operation reports its terminal failure");
        assert!(failure_body.message.contains("operator reconciliation"));

        // The same command released the revision claim; the staged ledger and
        // invisible manifest are durably gone.
        assert!(store
            .read_at(
                root(),
                placement(),
                owner(1),
                meta::MetadataFamily::ArtifactRevision,
                &claim_key,
                store.current_read_version().unwrap(),
            )
            .unwrap()
            .is_none());
        let resolved_operation = load_operation();
        assert_eq!(resolved_operation.phase, types::PublishPhase::Cleaned);
        assert_eq!(
            resolved_operation
                .terminal_error
                .as_ref()
                .expect("terminal error retained")
                .kind,
            meta::PublishTerminalErrorKind::OperatorReconciled
        );

        // An exact response-loss retry replays the stored resolution.
        let replayed = executor.execute(&reconcile).unwrap();
        assert!(replayed.replayed);
        assert_eq!(operation_status(replayed).token, resolved_status.token);

        // The revision identity is claimable again through the normal path.
        let retry_status = operation_status(
            executor
                .execute(&rpc(
                    0x7C,
                    protocol::WorkspaceRequest::BeginArtifactPublish(
                        protocol::BeginArtifactPublishRequest {
                            operation_id: protocol::OperationIdentity(
                                [0x7D; types::FIXED_ID_BYTES],
                            ),
                            artifact_revision_id: revision_identity,
                            target: protocol::WorkspacePath {
                                workbench: protocol::WorkbenchName::new("reconcile-test").unwrap(),
                                path: protocol::RelativePath::new("outputs/quarantine-retry.bin")
                                    .unwrap(),
                            },
                            authority: protocol::PublicationAuthority::Visible,
                            condition: protocol::PublishCondition::CreateOnly,
                            staged_object_count: seals.staged_object_count,
                            staged_object_seal: seals.staged_object_seal,
                            manifest_row_count: seals.manifest_row_count,
                            manifest_seal: seals.manifest_seal,
                            dependency_owner_revision_ids: Vec::new(),
                        },
                    ),
                ))
                .unwrap(),
        );
        assert_eq!(retry_status.state, protocol::OperationState::Running);
    }

    #[test]
    fn remove_path_crosses_one_atomic_metadata_command_and_replays() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([61; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(60, "remove-test", 61, 1))
            .unwrap();
        put_visible_path(&store, incarnation);
        let request = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([62; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::RemovePath(protocol::RemovePathRequest {
                target: protocol::WorkspacePath {
                    workbench: protocol::WorkbenchName::new("remove-test").unwrap(),
                    path: protocol::RelativePath::new("outputs/result.bin").unwrap(),
                },
                expected_generation: 1,
            }),
        };

        let removed = executor.execute(&request).unwrap();
        assert!(!removed.replayed);
        assert!(removed.commit_version.is_some());
        let protocol::WorkspaceResult::Removed(result) = &removed.result else {
            panic!("remove returned the wrong result variant");
        };
        assert!(result.removed);
        assert_eq!(result.workspace_revision, 1);
        assert_eq!(
            result.removed_artifact_revision_id,
            Some(types::ArtifactRevisionId::from_bytes([9; types::FIXED_ID_BYTES]).into())
        );

        let replayed = executor.execute(&request).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, removed.result);
        assert_eq!(replayed.commit_version, removed.commit_version);

        let reused = protocol::WorkspaceRpcRequest {
            operation: protocol::WorkspaceRequest::RemovePath(protocol::RemovePathRequest {
                target: protocol::WorkspacePath {
                    workbench: protocol::WorkbenchName::new("remove-test").unwrap(),
                    path: protocol::RelativePath::new("outputs/result.bin").unwrap(),
                },
                expected_generation: 2,
            }),
            ..request
        };
        let failure = executor.execute(&reused).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::RequestReplayMismatch);
    }

    #[test]
    fn snapshot_lifecycle_uses_visible_incarnation_and_lists_terminal_states() {
        let (_store, executor) = ready_executor();
        executor
            .execute(&create_request(70, "snapshot-test", 71, 1))
            .unwrap();
        let workbench = protocol::WorkbenchName::new("snapshot-test").unwrap();
        let incarnation = protocol::WorkspaceIdentity([71; types::FIXED_ID_BYTES]);
        let alias = protocol::SnapshotAlias::new("checkpoint").unwrap();
        let mint = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([72; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::MintSnapshot(protocol::MintSnapshotRequest {
                workbench: workbench.clone(),
                workspace_incarnation_id: incarnation,
                snapshot_id: 7,
                lease_deadline_ms: 1_000,
                alias: Some(alias.clone()),
                annotation: b"executor snapshot".to_vec(),
            }),
        };

        let minted = executor.execute(&mint).unwrap();
        assert!(!minted.replayed);
        let protocol::WorkspaceResult::Snapshot(minted_snapshot) = &minted.result else {
            panic!("mint returned the wrong result variant");
        };
        assert_eq!(minted_snapshot.snapshot_id, 7);
        assert_eq!(minted_snapshot.workbench, workbench);
        assert_eq!(minted_snapshot.workspace_incarnation_id, incarnation);
        assert_eq!(minted_snapshot.alias.as_ref(), Some(&alias));
        assert_eq!(minted_snapshot.status, protocol::SnapshotStatus::Alive);

        let point = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([77; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetSnapshot(protocol::GetSnapshotRequest {
                workbench: workbench.clone(),
                selector: protocol::SnapshotSelector::Alias(alias.clone()),
            }),
        };
        let pointed = executor.execute(&point).unwrap();
        assert_eq!(pointed.commit_version, None);
        assert!(!pointed.replayed);
        let protocol::WorkspaceResult::Snapshot(pointed_snapshot) = pointed.result else {
            panic!("point get returned the wrong result variant");
        };
        assert_eq!(pointed_snapshot.snapshot_id, 7);

        let replayed = executor.execute(&mint).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, minted.result);
        assert_eq!(replayed.commit_version, minted.commit_version);

        let renew = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([73; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::RenewSnapshot(protocol::RenewSnapshotRequest {
                workbench: workbench.clone(),
                selector: protocol::SnapshotSelector::Alias(alias.clone()),
                lease_deadline_ms: 2_000,
            }),
        };
        let renewed = executor.execute(&renew).unwrap();
        let protocol::WorkspaceResult::Snapshot(renewed_snapshot) = renewed.result else {
            panic!("renew returned the wrong result variant");
        };
        assert_eq!(renewed_snapshot.lease_deadline_ms, 2_000);
        assert_eq!(renewed_snapshot.status, protocol::SnapshotStatus::Alive);

        let retire = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([74; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::RetireSnapshot(
                protocol::RetireSnapshotRequest {
                    workbench: workbench.clone(),
                    selector: protocol::SnapshotSelector::Id(7),
                    retire_annotation: Some(br#"{"metadata":null,"reason":"done"}"#.to_vec()),
                },
            ),
        };
        let retired = executor.execute(&retire).unwrap();
        let protocol::WorkspaceResult::Snapshot(retired_snapshot) = retired.result else {
            panic!("retire returned the wrong result variant");
        };
        assert_eq!(retired_snapshot.status, protocol::SnapshotStatus::Retired);
        assert_eq!(
            retired_snapshot.retire_annotation.as_deref(),
            Some(br#"{"metadata":null,"reason":"done"}"#.as_slice())
        );

        let list = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([75; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::ListSnapshots(protocol::ListSnapshotsRequest {
                workbench: workbench.clone(),
                page: protocol::PageRequest {
                    cursor: None,
                    limit: 10,
                },
            }),
        };
        let listed = executor.execute(&list).unwrap();
        let protocol::WorkspaceResult::Snapshots(page) = listed.result else {
            panic!("list snapshots returned the wrong result variant");
        };
        assert_eq!(page.snapshots.len(), 1);
        assert_eq!(page.snapshots[0].snapshot_id, 7);
        assert_eq!(page.snapshots[0].status, protocol::SnapshotStatus::Retired);
        assert!(page.next_cursor.is_none());

        let wrong_incarnation = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([76; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::MintSnapshot(protocol::MintSnapshotRequest {
                workbench,
                workspace_incarnation_id: protocol::WorkspaceIdentity([99; types::FIXED_ID_BYTES]),
                snapshot_id: 8,
                lease_deadline_ms: 3_000,
                alias: None,
                annotation: Vec::new(),
            }),
        };
        let failure = executor.execute(&wrong_incarnation).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::Conflict);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::Workspace));
    }

    #[test]
    fn snapshot_alias_point_get_uses_alias_generation_not_numeric_id_order() {
        let (store, executor) = ready_executor();
        executor
            .execute(&create_request(80, "snapshot-alias-test", 81, 1))
            .unwrap();
        let workbench = protocol::WorkbenchName::new("snapshot-alias-test").unwrap();
        let incarnation = protocol::WorkspaceIdentity([81; types::FIXED_ID_BYTES]);
        let alias = protocol::SnapshotAlias::new("latest").unwrap();

        for (request_fill, snapshot_id) in [(82, 900), (83, 1)] {
            executor
                .execute(&protocol::WorkspaceRpcRequest {
                    route: route(1),
                    request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
                    operation: protocol::WorkspaceRequest::MintSnapshot(
                        protocol::MintSnapshotRequest {
                            workbench: workbench.clone(),
                            workspace_incarnation_id: incarnation,
                            snapshot_id,
                            lease_deadline_ms: 1_000,
                            alias: Some(alias.clone()),
                            annotation: Vec::new(),
                        },
                    ),
                })
                .unwrap();
        }

        let point = |request_fill, selector| protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetSnapshot(protocol::GetSnapshotRequest {
                workbench: workbench.clone(),
                selector,
            }),
        };
        let version_before = store.current_read_version().unwrap();
        let current = executor
            .execute(&point(84, protocol::SnapshotSelector::Alias(alias.clone())))
            .unwrap();
        assert_eq!(current.commit_version, None);
        assert_eq!(store.current_read_version().unwrap(), version_before);
        let protocol::WorkspaceResult::Snapshot(current) = current.result else {
            panic!("alias point get returned the wrong result variant");
        };
        assert_eq!(current.snapshot_id, 1);

        let old = executor
            .execute(&point(85, protocol::SnapshotSelector::Id(900)))
            .unwrap();
        let protocol::WorkspaceResult::Snapshot(old) = old.result else {
            panic!("id point get returned the wrong result variant");
        };
        assert_eq!(old.snapshot_id, 900);

        store
            .observe_lease_clock(root(), placement(), owner(1), 1_500)
            .unwrap();
        let expired = executor
            .execute(&point(86, protocol::SnapshotSelector::Alias(alias.clone())))
            .unwrap();
        let protocol::WorkspaceResult::Snapshot(expired) = expired.result else {
            panic!("expired point get returned the wrong result variant");
        };
        assert_eq!(expired.snapshot_id, 1);
        assert_eq!(expired.status, protocol::SnapshotStatus::Expired);

        let revived = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([87; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::RenewSnapshot(
                    protocol::RenewSnapshotRequest {
                        workbench,
                        selector: protocol::SnapshotSelector::Alias(alias),
                        lease_deadline_ms: 2_000,
                    },
                ),
            })
            .unwrap();
        let protocol::WorkspaceResult::Snapshot(revived) = revived.result else {
            panic!("renew returned the wrong result variant");
        };
        assert_eq!(revived.snapshot_id, 1);
        assert_eq!(revived.status, protocol::SnapshotStatus::Alive);
    }

    #[test]
    fn snapshot_terminal_states_keep_their_typed_failure_codes() {
        for (state, code) in [
            (
                types::SnapshotState::ReapClaimed,
                protocol::ErrorCode::SnapshotExpired,
            ),
            (
                types::SnapshotState::Retired,
                protocol::ErrorCode::PreconditionFailed,
            ),
            (
                types::SnapshotState::Reaped,
                protocol::ErrorCode::SnapshotReaped,
            ),
        ] {
            let failure = snapshot_failure(meta::SnapshotError::SnapshotNotActive {
                snapshot_id: types::SnapshotId::new(7),
                state,
            });
            assert_eq!(failure.code, code);
            assert_eq!(
                failure.conflict,
                Some(protocol::ConflictKind::SnapshotLifecycle)
            );
            assert!(!failure.retryable);
        }
    }

    #[test]
    fn ranged_manifest_plan_pages_across_engine_batches_without_truncation() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([51; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(50, "range-test", 51, 1))
            .unwrap();
        let target = put_ranged_path(&store, incarnation, 513);

        let metadata_only = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([52; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetPath(protocol::GetPathRequest {
                target: target.clone(),
                view: protocol::WorkspaceReadView::Live,
                range: None,
                plan_page: None,
                if_none_match: None,
            }),
        };
        let metadata_only = executor.execute(&metadata_only).unwrap();
        let protocol::WorkspaceResult::Path(metadata_only) = metadata_only.result else {
            panic!("metadata get returned the wrong result variant");
        };
        assert!(metadata_only.blocks.is_empty());
        assert!(metadata_only.next_cursor.is_none());
        assert_eq!(metadata_only.metadata.unwrap().descriptor.logical_size, 513);

        let range = protocol::ByteRange {
            offset: 0,
            length: 513,
        };
        let first = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([53; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetPath(protocol::GetPathRequest {
                target: target.clone(),
                view: protocol::WorkspaceReadView::Live,
                range: Some(range),
                plan_page: Some(protocol::PageRequest {
                    cursor: None,
                    limit: protocol::MAX_ARTIFACT_READ_PLAN_ROWS as u32,
                }),
                if_none_match: None,
            }),
        };
        let first = executor.execute(&first).unwrap();
        let protocol::WorkspaceResult::Path(first) = first.result else {
            panic!("first range page returned the wrong result variant");
        };
        assert_eq!(first.blocks.len(), protocol::MAX_ARTIFACT_READ_PLAN_ROWS);
        assert_eq!(first.blocks.first().unwrap().logical_offset, 0);
        assert_eq!(first.blocks.last().unwrap().logical_offset, 511);
        let cursor = first.next_cursor.expect("513 rows require a second page");

        let second = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([54; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetPath(protocol::GetPathRequest {
                target: target.clone(),
                view: protocol::WorkspaceReadView::Live,
                range: Some(range),
                plan_page: Some(protocol::PageRequest {
                    cursor: Some(cursor.clone()),
                    limit: protocol::MAX_ARTIFACT_READ_PLAN_ROWS as u32,
                }),
                if_none_match: None,
            }),
        };
        let second = executor.execute(&second).unwrap();
        let protocol::WorkspaceResult::Path(second) = second.result else {
            panic!("second range page returned the wrong result variant");
        };
        assert_eq!(second.blocks.len(), 1);
        assert_eq!(second.blocks[0].object_index, 512);
        assert_eq!(second.blocks[0].logical_offset, 512);
        assert!(second.next_cursor.is_none());

        let mismatched = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([55; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetPath(protocol::GetPathRequest {
                target,
                view: protocol::WorkspaceReadView::Live,
                range: Some(protocol::ByteRange {
                    offset: 1,
                    length: 512,
                }),
                plan_page: Some(protocol::PageRequest {
                    cursor: Some(cursor),
                    limit: protocol::MAX_ARTIFACT_READ_PLAN_ROWS as u32,
                }),
                if_none_match: None,
            }),
        };
        let failure = executor.execute(&mismatched).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::InvalidArgument);
    }
}
