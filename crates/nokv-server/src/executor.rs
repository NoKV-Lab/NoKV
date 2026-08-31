/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_meta::workspace as meta;
use nokv_protocol as protocol;
use nokv_types as types;
use sha2::{Digest as _, Sha256};

use crate::{ExecutedRequest, WorkspaceRequestExecutor};

const MANIFEST_SCAN_ROWS: usize = 256;
const MANIFEST_PLAN_CURSOR_VERSION: u8 = 1;
const MANIFEST_PLAN_CURSOR_BYTES: usize = 1 + types::FIXED_ID_BYTES * 2 + 8 * 3;
// Keep store-wide generation churn inside the replay-safe RPC boundary instead
// of spending the client's much smaller transport retry budget.
const MAX_INTERNAL_METADATA_ATTEMPTS: u32 = 8;
const PUBLISH_ACTIVITY_LEASE_MS: u64 = 30 * 60 * 1_000;
const RUN_MANIFEST_PATH: &str = "metadata/run_manifest.json";
const _: () = assert!(protocol::MAX_ARTIFACT_DEPENDENCY_OWNERS == meta::MAX_REVISION_DEPENDENCIES);
const _: () = assert!(MAX_INTERNAL_METADATA_ATTEMPTS > 0);
const _: () =
    assert!(protocol::MAX_ARTIFACT_DEPENDENCY_DEPTH == meta::MAX_REVISION_DEPENDENCY_DEPTH);
const _: () =
    assert!(protocol::PageRequest::MAX_LIMIT as usize == meta::MAX_VISIBLE_PATH_LIST_PAGE_SIZE);
const _: () = assert!(protocol::MAX_QUERY_PAGE_LIMIT as usize == meta::MAX_QUERY_PAGE_SIZE);
// Workspace RPC v2 already admits up to 64 descriptor fields. The durable
// projection planner has always accepted 60; preserve the wire contract and
// reject the legacy 61..=64 gap at DTO-to-domain conversion as main does.
const _: () =
    assert!(protocol::ArtifactDescriptor::MAX_INDEX_FIELDS >= meta::MAX_TYPED_PROJECTION_FIELDS);
const SUPPORTED_WORKSPACE_CAPABILITIES: [protocol::WorkspaceCapability; 10] = [
    protocol::WorkspaceCapability::ArtifactPublishV1,
    protocol::WorkspaceCapability::ArtifactRangeReadV1,
    protocol::WorkspaceCapability::ChangeFeedV1,
    protocol::WorkspaceCapability::CommitV1,
    protocol::WorkspaceCapability::GenericCustomIndexV1,
    protocol::WorkspaceCapability::QueryV1,
    protocol::WorkspaceCapability::RestoreV1,
    protocol::WorkspaceCapability::SnapshotLeaseV1,
    protocol::WorkspaceCapability::WorkspaceLifecycleV1,
    protocol::WorkspaceCapability::WorkspacePathV1,
];

#[cfg(feature = "restore-crash-test-support")]
#[derive(Clone, Copy, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestoreInitializationBarrierPhase {
    DestinationBuilding,
}

#[cfg(feature = "restore-crash-test-support")]
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct RestoreManifestPublicationEvidence {
    pub identity: protocol::RestoreManifestIdentity,
    pub workspace_incarnation_id: protocol::WorkspaceIdentity,
    pub body_digest_uri: String,
    pub manifest_digest_uri: String,
    pub logical_size: u64,
    pub content_type: String,
}

#[cfg(feature = "restore-crash-test-support")]
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct RestoreManifestBindingEvidence {
    pub expected: protocol::RestoreManifestIdentity,
    pub actual: RestoreManifestPublicationEvidence,
}

#[cfg(feature = "restore-crash-test-support")]
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct RestoreInitializationBarrierEvidence {
    pub route: protocol::RootRoute,
    pub operation_id: protocol::OperationIdentity,
    pub durable_read_version: u64,
    pub phase: RestoreInitializationBarrierPhase,
    pub initialization_digest: protocol::Digest,
    pub destination_workspace_incarnation_id: protocol::WorkspaceIdentity,
    pub destination_commit_id: protocol::CommitIdentity,
    pub run_manifest: RestoreManifestBindingEvidence,
    pub restore_manifest: RestoreManifestBindingEvidence,
    pub built_commit_members: u64,
    pub sealed_revisions: u64,
}

#[cfg(feature = "restore-crash-test-support")]
pub trait RestoreInitializationBarrier: Send + Sync {
    fn reached(
        &self,
        evidence: Result<RestoreInitializationBarrierEvidence, protocol::RpcFailure>,
    ) -> !;
}

#[cfg(feature = "restore-crash-test-support")]
#[derive(Clone)]
struct RestoreInitializationBarrierRegistration {
    target_operation_id: protocol::OperationIdentity,
    barrier: Arc<dyn RestoreInitializationBarrier>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct RestorePreparationKey {
    root_id: types::RootId,
    destination_workspace_incarnation_id: types::WorkspaceIncarnationId,
}

/// Serializes only competing drivers for one destination incarnation.
///
/// This owner-local gate is a scheduling optimization, not restore authority:
/// every caller resumes from the durable operation row, and owner failover
/// discards these gates while preserving the metadata CAS and epoch fences.
#[derive(Default)]
struct RestorePreparationCoordinator {
    gates: Mutex<BTreeMap<RestorePreparationKey, Weak<Mutex<()>>>>,
}

impl RestorePreparationCoordinator {
    fn gate(&self, key: RestorePreparationKey) -> Arc<Mutex<()>> {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        gates.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(key, Arc::downgrade(&gate));
        gate
    }
}

/// Storage-neutral protocol adapter over one authoritative metadata shard.
///
/// The adapter owns DTO conversion and exact RPC request binding only. Durable
/// lifecycle transitions remain in `nokv-meta`.
#[derive(Clone)]
pub struct MetadataWorkspaceRequestExecutor {
    meta: Arc<meta::MetaShard>,
    restore_preparations: Arc<RestorePreparationCoordinator>,
    #[cfg(feature = "restore-crash-test-support")]
    restore_initialization_barrier: Option<RestoreInitializationBarrierRegistration>,
}

impl MetadataWorkspaceRequestExecutor {
    pub fn new(meta: Arc<meta::MetaShard>) -> Self {
        Self {
            meta,
            restore_preparations: Arc::new(RestorePreparationCoordinator::default()),
            #[cfg(feature = "restore-crash-test-support")]
            restore_initialization_barrier: None,
        }
    }

    #[cfg(feature = "restore-crash-test-support")]
    pub fn with_restore_initialization_barrier(
        mut self,
        target_operation_id: protocol::OperationIdentity,
        barrier: Arc<dyn RestoreInitializationBarrier>,
    ) -> Self {
        self.restore_initialization_barrier = Some(RestoreInitializationBarrierRegistration {
            target_operation_id,
            barrier,
        });
        self
    }

    pub fn meta(&self) -> &Arc<meta::MetaShard> {
        &self.meta
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
            protocol::WorkspaceRequest::RenamePath(rename) => self.rename_path(request, rename),
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
            protocol::WorkspaceRequest::BindRestoreDestination(bind) => {
                self.bind_restore_destination(request, bind)
            }
            protocol::WorkspaceRequest::ReadRestoreSourceRunManifest(read) => {
                self.read_restore_source_run_manifest(request, read)
            }
            protocol::WorkspaceRequest::FinalizeRestore(finalize) => {
                self.finalize_restore(request, finalize)
            }
            protocol::WorkspaceRequest::GetOperation(get) => self.get_operation(request, get),
            protocol::WorkspaceRequest::BeginGenericIndexRegistration(begin) => {
                self.begin_generic_index_registration(request, begin)
            }
            protocol::WorkspaceRequest::AppendGenericIndexRows(append) => {
                self.append_generic_index_rows(request, append)
            }
            protocol::WorkspaceRequest::FinalizeGenericIndexRegistration(finalize) => {
                self.finalize_generic_index_registration(request, finalize)
            }
            protocol::WorkspaceRequest::AbortGenericIndexRegistration(abort) => {
                self.abort_generic_index_registration(request, abort)
            }
            protocol::WorkspaceRequest::GetGenericIndexRegistration(get) => {
                self.get_generic_index_registration(request, get)
            }
            protocol::WorkspaceRequest::Search(search) => self.search(request, search),
            protocol::WorkspaceRequest::Aggregate(aggregate) => self.aggregate(request, aggregate),
            protocol::WorkspaceRequest::Catalog(catalog) => self.catalog(request, catalog),
            protocol::WorkspaceRequest::FindWorkspaces(find) => self.find_workspaces(request, find),
            protocol::WorkspaceRequest::ReadChanges(changes) => self.read_changes(request, changes),
        }
    }

    fn execute_request_with_internal_retry(
        &self,
        request: &protocol::WorkspaceRpcRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        retry_internal_metadata_conflicts(|| self.execute_request(request))
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
            &self.meta,
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
            &self.meta,
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
        let resolved = match (&request.view, request.expected_read_version) {
            // An unfenced live read resolves the workspace and path under one
            // ownership/fence validation at the current version; it does not
            // pay for a separate read-context sample it never compares.
            (protocol::WorkspaceReadView::Live, None) => {
                let route = route_parts(rpc.route)?;
                meta::get_current_visible_workspace_path(
                    &self.meta,
                    route.root_id,
                    route.placement_generation,
                    route.owner_epoch,
                    &workbench,
                    &path,
                )
            }
            (view, expected_read_version) => {
                let context = self.workspace_read_context(rpc.route, &workbench, view)?;
                if let Some(expected) = expected_read_version {
                    if expected != context.read_version.get() {
                        return Err(failure(
                            protocol::ErrorCode::PreconditionFailed,
                            format!(
                                "get_path expected read version {expected} does not match resolved read version {}",
                                context.read_version.get()
                            ),
                            false,
                            Some(protocol::ConflictKind::ReadVersion),
                        ));
                    }
                }
                meta::get_visible_workspace_path_at(&self.meta, context, &workbench, &path)
            }
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
        let workspace = meta::get_visible_workspace_at(&self.meta, context, &workbench)
            .map_err(namespace_failure)?;
        // The marker comparison and path scan share this exact RootReadContext.
        if let Some(fence) = &request.workspace_continuation_fence {
            let matches = workspace.as_ref().is_some_and(|workspace| {
                workspace.incarnation_id
                    == types::WorkspaceIncarnationId::from(fence.workspace_incarnation_id)
                    && workspace.workspace_revision.get() == fence.workspace_revision
            });
            if !matches {
                return Err(failure(
                    protocol::ErrorCode::PreconditionFailed,
                    "list_paths workspace continuation fence no longer matches the visible workbench",
                    false,
                    Some(protocol::ConflictKind::Workspace),
                ));
            }
        }
        let workspace = workspace.ok_or_else(|| not_found("workbench does not exist"))?;
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
            &self.meta,
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
            &self.meta,
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

    fn rename_path(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::RenamePathRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let step_id = derived_request_id(rpc.request_id, b"rename-path", 0);
        let workbench_id = workbench_id(&request.source.workbench)?;
        let outcome = meta::rename_path(
            &self.meta,
            meta::RenamePathRequest {
                context: self.write_context(rpc.route, step_id)?,
                workbench_id,
                source: relative_path(&request.source.path)?,
                destination: relative_path(&request.destination.path)?,
                expected_generation: types::Generation::new(request.expected_generation)
                    .map_err(|error| invalid_argument(error.to_string()))?,
            },
        )
        .map_err(rename_path_failure)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Renamed(protocol::RenamePathResult {
                source: request.source.clone(),
                destination: request.destination.clone(),
                workspace_revision: outcome.workspace_revision.get(),
                generation: outcome.generation.get(),
                artifact_revision_id: outcome.artifact_revision_id.into(),
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
        let workspace = meta::get_visible_workspace_at(&self.meta, context, &workbench_id)
            .map_err(namespace_failure)?
            .ok_or_else(|| not_found("workbench does not exist"))?;
        let snapshot = meta::get_snapshot_at(
            &self.meta,
            context,
            &workbench_id,
            &snapshot_selector(&request.selector)?,
        )
        .map_err(snapshot_failure)?
        .ok_or_else(|| not_found("snapshot does not exist for the visible workbench"))?;
        let lease_clock = self.meta.lease_clock_high_water().map_err(meta_failure)?;
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
            &self.meta,
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
            &self.meta,
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
        let lease_clock = self.meta.lease_clock_high_water().map_err(meta_failure)?;
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
            &self.meta,
            read_context_from_write(context),
            &workbench_id,
        )
        .map_err(namespace_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        let outcome = meta::renew_snapshot(
            &self.meta,
            context,
            &meta::RenewSnapshotRequest {
                workbench_id,
                selector: snapshot_selector(&request.selector)?,
                lease_deadline_ms: request.lease_deadline_ms,
            },
        )
        .map_err(snapshot_failure)?;
        let lease_clock = self.meta.lease_clock_high_water().map_err(meta_failure)?;
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
            &self.meta,
            read_context_from_write(context),
            &workbench_id,
        )
        .map_err(namespace_failure)?
        .ok_or_else(|| not_found("workbench does not exist"))?;
        let outcome = meta::retire_snapshot(
            &self.meta,
            context,
            &meta::RetireSnapshotRequest {
                workbench_id,
                selector: snapshot_selector(&request.selector)?,
                retire_annotation: request.retire_annotation.clone(),
            },
        )
        .map_err(snapshot_failure)?;
        let lease_clock = self.meta.lease_clock_high_water().map_err(meta_failure)?;
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
            &self.meta,
            context,
            &workbench_id(&request.workbench)?,
            request.page.cursor.as_deref(),
            query_limit(request.page.limit),
        )
        .map_err(snapshot_list_failure)?;
        let lease_clock = self.meta.lease_clock_high_water().map_err(meta_failure)?;
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
        let gate = self.restore_preparations.gate(RestorePreparationKey {
            root_id: rpc.route.root_id.into(),
            destination_workspace_incarnation_id: request
                .destination_workspace_incarnation_id
                .into(),
        });
        let _driver = gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retry_restore_preparation_progress(|| self.prepare_restore_once(rpc, request))
    }

    fn prepare_restore_once(
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
        let destination_workbench = workbench_id(&request.destination_workbench)?;
        let source_workspace_incarnation_id = request.source_workspace_incarnation_id.into();
        let destination_workspace_incarnation_id =
            request.destination_workspace_incarnation_id.into();
        let route = route_parts(rpc.route)?;
        let expected_operation_id = meta::restore_operation_id(
            route.root_id,
            &source_workbench,
            source_workspace_incarnation_id,
            source,
            &destination_workbench,
            destination_workspace_incarnation_id,
        )
        .map_err(restore_failure)?;
        if types::OperationId::from(request.operation_id) != expected_operation_id {
            return Err(invalid_argument(
                "restore operation identity does not match the deterministic source/destination selector",
            ));
        }
        self.claim_mutation(rpc)?;
        let begin_id = derived_request_id(rpc.request_id, b"restore-begin", 0);
        let mut outcome = meta::begin_restore(
            &self.meta,
            self.write_context(rpc.route, begin_id)?,
            &meta::BeginRestoreRequest {
                operation_id: request.operation_id.into(),
                source_workbench_id: source_workbench,
                expected_source_workspace_incarnation_id: source_workspace_incarnation_id,
                source,
                destination_workbench_id: destination_workbench,
                destination_workspace_incarnation_id,
                destination_restore_manifest_identity: restore_manifest_identity(
                    request.destination_restore_manifest_identity,
                ),
                destination_committed_at_unix_seconds: commit_time_unix_seconds(&self.meta)?,
                restore_manifest: meta::RestoreManifestDescriptor {
                    body_digest_uri: request.restore_manifest.body_digest.as_str().to_owned(),
                    logical_size: request.restore_manifest.logical_size,
                    content_type: request.restore_manifest.content_type.as_str().to_owned(),
                },
            },
        )
        .map_err(restore_failure)?;
        let durable = meta::get_restore(
            &self.meta,
            self.write_context(
                rpc.route,
                derived_request_id(rpc.request_id, b"restore-prepare-load", 0),
            )?,
            outcome.operation.operation_id,
        )
        .map_err(restore_failure)?
        .ok_or_else(|| internal("begun restore operation is not durably readable"))?;
        restore_replay_lineage_matches(&outcome.operation, &durable)?;
        outcome.operation = durable;
        if let Some(failure) = restore_terminal_failure(&outcome.operation) {
            return Err(failure);
        }

        if outcome.operation.phase == types::RestorePhase::Preparing {
            let started = meta::start_restore_copy(
                &self.meta,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-start-copy", 0),
                )?,
                meta::RestoreOperationRequest {
                    operation_id: outcome.operation.operation_id,
                },
            )
            .map_err(restore_preparation_step_failure)?;
            if let Some(failure) = restore_terminal_failure(&started.operation) {
                return Err(failure);
            }
            if !restore_step_advances(
                &outcome.operation,
                &started.operation,
                started.replayed,
                "restore copy start",
            )? {
                return Err(internal("restore copy start replay did not reach Copying"));
            }
            outcome = started;
        }

        while outcome.operation.phase == types::RestorePhase::Copying
            && !outcome.operation.source_eof
        {
            let copied = meta::copy_restore_batch(
                &self.meta,
                self.write_context(
                    rpc.route,
                    restore_copy_request_id(rpc.request_id, outcome.operation.next_member_sequence),
                )?,
                meta::CopyRestoreBatchRequest {
                    operation_id: outcome.operation.operation_id,
                    limit: meta::MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .map_err(restore_preparation_step_failure)?;
            if let Some(failure) = restore_terminal_failure(&copied.command.operation) {
                return Err(failure);
            }
            if copied.command.operation.phase != types::RestorePhase::Copying
                || copied.source_eof != copied.command.operation.source_eof
            {
                return Err(internal(
                    "restore copy batch returned an inconsistent phase or EOF projection",
                ));
            }
            let advances = restore_step_advances(
                &outcome.operation,
                &copied.command.operation,
                copied.command.replayed,
                "restore copier",
            )?;
            if !advances {
                // The copy request id tracks the durable cursor, so a replay
                // that does not move the cursor can never make progress on a
                // later attempt either.
                return Err(internal("restore copier made no durable progress"));
            }
            outcome = copied.command;
        }
        if outcome.operation.phase == types::RestorePhase::Copying && outcome.operation.source_eof {
            let sealed = meta::seal_restore_source(
                &self.meta,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-seal-source", 0),
                )?,
                meta::RestoreOperationRequest {
                    operation_id: outcome.operation.operation_id,
                },
            )
            .map_err(restore_preparation_step_failure)?;
            if let Some(failure) = restore_terminal_failure(&sealed.operation) {
                return Err(failure);
            }
            if !restore_step_advances(
                &outcome.operation,
                &sealed.operation,
                sealed.replayed,
                "restore source sealer",
            )? {
                return Err(internal(
                    "restore source-seal replay did not reach SourceSealed",
                ));
            }
            outcome = sealed;
        }
        let preparation = sealed_restore_preparation(&outcome.operation)?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::RestorePrepared(preparation),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn bind_restore_destination(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::BindRestoreDestinationRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let operation_id: types::OperationId = request.operation_id.into();
        let outcome = meta::bind_restore_destination(
            &self.meta,
            self.write_context(
                rpc.route,
                derived_request_id(rpc.request_id, b"restore-bind-destination", 0),
            )?,
            &meta::BindRestoreDestinationRequest {
                operation_id,
                binding: meta::RestoreDestinationBinding {
                    destination_commit_id: request.destination_commit_id.into(),
                    effective_content_digest_uri: request
                        .effective_content_digest
                        .as_str()
                        .to_owned(),
                    destination_projection_input_digest: request
                        .destination_run_manifest_projection_input_digest
                        .0,
                    run_manifest_identity: restore_manifest_identity(
                        request.destination_run_manifest_identity,
                    ),
                    restore_manifest_identity: restore_manifest_identity(
                        request.destination_restore_manifest_identity,
                    ),
                    // Object-first publication installs the two actual
                    // descriptors later. The bind RPC only freezes intent.
                    manifests: None,
                },
            },
        )
        .map_err(restore_failure)?;
        if let Some(failure) = restore_terminal_failure(&outcome.operation) {
            return Err(failure);
        }
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::RestorePrepared(sealed_restore_preparation(
                &outcome.operation,
            )?),
            commit_version: Some(outcome.commit_version.get()),
            replayed: outcome.replayed,
        })
    }

    fn read_restore_source_run_manifest(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::ReadRestoreSourceRunManifestRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.write_context(
            rpc.route,
            derived_request_id(rpc.request_id, b"restore-read-source-run-manifest", 0),
        )?;
        let retained = meta::read_restore_source_run_manifest(
            &self.meta,
            context,
            request.operation_id.into(),
        )
        .map_err(restore_failure)?;
        let provenance = restore_commit_provenance(&retained.operation)?;
        if retained.source_commit_id != provenance.source_commit.commit_id
            || retained.path_entry.artifact_revision_id
                != provenance.source_commit.tree_manifest_revision_id
        {
            return Err(internal(
                "restore-held source run manifest does not match its frozen base commit",
            ));
        }
        let workbench = protocol_workbench(&retained.operation.source_workbench_id)?;
        let path = types::NormalizedRelativePath::new(RUN_MANIFEST_PATH)
            .expect("canonical run manifest path is normalized");
        let artifact_revision_id = retained.path_entry.artifact_revision_id;
        let metadata = Self::path_metadata(
            &workbench,
            meta::WorkspaceRecord {
                incarnation_id: retained.operation.source_workspace_incarnation_id,
                // Commit-owned members are not one live workspace revision.
                workspace_revision: types::WorkspaceRevision::ZERO,
                state: types::WorkspaceState::Visible,
                owning_operation_id: None,
            },
            path,
            retained.path_entry,
        )?;
        let (blocks, next_cursor) = match request.range {
            None => (Vec::new(), None),
            Some(range) => {
                let end = range
                    .offset
                    .checked_add(range.length)
                    .expect("protocol validation rejected range overflow");
                if end > metadata.descriptor.logical_size {
                    return Err(invalid_argument(
                        "requested byte range exceeds the source run manifest logical size",
                    ));
                }
                self.manifest_read_plan(
                    read_context_from_write(context),
                    artifact_revision_id,
                    range,
                    request
                        .plan_page
                        .as_ref()
                        .expect("protocol validation requires ranged plan pagination"),
                )?
            }
        };
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::RestoreSourceRunManifest(protocol::PathReadResult {
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

    fn finalize_restore(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::FinalizeRestoreRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let operation_id: types::OperationId = request.operation_id.into();
        let load_context = self.write_context(
            rpc.route,
            derived_request_id(rpc.request_id, b"restore-finalize-load", 0),
        )?;
        #[cfg(feature = "restore-crash-test-support")]
        let mut durable_read_version = load_context.read_version.get();
        let mut operation = meta::get_restore(&self.meta, load_context, operation_id)
            .map_err(restore_failure)?
            .ok_or_else(|| not_found("restore operation does not exist"))?;
        if let Some(failure) = restore_terminal_failure(&operation) {
            return Err(failure);
        }
        if operation.phase == types::RestorePhase::Complete {
            return restored_response(&operation, None, true);
        }
        if operation.phase == types::RestorePhase::SourceSealed {
            let initialized = meta::apply_restore_initialization(
                &self.meta,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-apply-initialization", 0),
                )?,
                meta::RestoreOperationRequest { operation_id },
            )
            .map_err(restore_failure)?;
            if let Some(failure) = restore_terminal_failure(&initialized.operation) {
                return Err(failure);
            }
            if initialized.operation.phase != types::RestorePhase::DestinationBuilding
                || !restore_step_advances(
                    &operation,
                    &initialized.operation,
                    initialized.replayed,
                    "restore destination initialization",
                )?
            {
                return Err(internal(
                    "restore initialization did not reach DestinationBuilding",
                ));
            }
            #[cfg(feature = "restore-crash-test-support")]
            {
                let initialization_read_version =
                    types::ReadVersion::new(initialized.commit_version.get()).map_err(|error| {
                        internal(format!(
                            "restore initialization commit version is not readable: {error}"
                        ))
                    })?;
                let mut readback_context = self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-initialization-readback", 0),
                )?;
                readback_context.read_version = initialization_read_version;
                let durable = meta::get_restore(&self.meta, readback_context, operation_id)
                    .map_err(restore_failure)?;
                operation =
                    authoritative_restore_initialization_readback(&initialized.operation, durable)?;
                durable_read_version = initialized.commit_version.get();
            }
            #[cfg(not(feature = "restore-crash-test-support"))]
            {
                operation = initialized.operation;
            }
        }

        #[cfg(feature = "restore-crash-test-support")]
        if operation.phase == types::RestorePhase::DestinationBuilding {
            if let Some(registration) = &self.restore_initialization_barrier {
                let operation_id = protocol::OperationIdentity::from(operation.operation_id);
                if operation_id == registration.target_operation_id {
                    let evidence = restore_initialization_barrier_evidence(
                        rpc.route,
                        durable_read_version,
                        &operation,
                    );
                    registration.barrier.reached(evidence);
                }
            }
        }

        let mut batch = 0_u64;
        while operation.phase == types::RestorePhase::DestinationBuilding {
            let before = restore_commit_member_progress(&operation)?;
            let built = meta::build_restore_commit_members(
                &self.meta,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-build-commit-members", batch),
                )?,
                meta::RestoreClosureBatchRequest {
                    operation_id,
                    limit: meta::MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .map_err(restore_failure)?;
            if let Some(failure) = restore_terminal_failure(&built.command.operation) {
                return Err(failure);
            }
            if !matches!(
                built.command.operation.phase,
                types::RestorePhase::DestinationBuilding | types::RestorePhase::DestinationSealing
            ) || built.members_complete
                != (built.command.operation.phase == types::RestorePhase::DestinationSealing)
            {
                return Err(internal(
                    "restore destination commit-member completion projection is inconsistent",
                ));
            }
            let advances = restore_step_advances(
                &operation,
                &built.command.operation,
                built.command.replayed,
                "restore destination commit-member builder",
            )?;
            if advances {
                let after = restore_commit_member_progress(&built.command.operation)?;
                if built.command.operation.phase == types::RestorePhase::DestinationBuilding
                    && (built.built_members == 0 || after <= before)
                {
                    return Err(internal(
                        "restore destination commit-member builder made no durable progress",
                    ));
                }
                operation = built.command.operation;
            } else if !built.command.replayed {
                return Err(internal(
                    "restore destination commit-member builder did not advance",
                ));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("restore commit-member batch counter overflow"))?;
        }

        batch = 0;
        while operation.phase == types::RestorePhase::DestinationSealing {
            let before = restore_revision_seal_progress(&operation)?;
            let sealed = meta::seal_restore_commit_revisions(
                &self.meta,
                self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"restore-seal-commit-revisions", batch),
                )?,
                meta::RestoreClosureBatchRequest {
                    operation_id,
                    limit: meta::MAX_RESTORE_BATCH_MEMBERS,
                },
            )
            .map_err(restore_failure)?;
            if let Some(failure) = restore_terminal_failure(&sealed.command.operation) {
                return Err(failure);
            }
            if !matches!(
                sealed.command.operation.phase,
                types::RestorePhase::DestinationSealing | types::RestorePhase::Ready
            ) || sealed.ready != (sealed.command.operation.phase == types::RestorePhase::Ready)
            {
                return Err(internal(
                    "restore destination revision completion projection is inconsistent",
                ));
            }
            let advances = restore_step_advances(
                &operation,
                &sealed.command.operation,
                sealed.command.replayed,
                "restore destination revision sealer",
            )?;
            if advances {
                let after = restore_revision_seal_progress(&sealed.command.operation)?;
                if sealed.command.operation.phase == types::RestorePhase::DestinationSealing
                    && (sealed.sealed_revisions == 0 || after <= before)
                {
                    return Err(internal(
                        "restore destination revision sealer made no durable progress",
                    ));
                }
                operation = sealed.command.operation;
            } else if !sealed.command.replayed {
                return Err(internal(
                    "restore destination revision sealer did not advance",
                ));
            }
            batch = batch
                .checked_add(1)
                .ok_or_else(|| internal("restore revision-seal batch counter overflow"))?;
        }

        if operation.phase != types::RestorePhase::Ready {
            return Err(operation_terminal_failure(
                "restore finalization did not reach Ready",
            ));
        }
        let completed = match meta::complete_restore(
            &self.meta,
            self.write_context(
                rpc.route,
                derived_request_id(rpc.request_id, b"restore-complete", 0),
            )?,
            meta::RestoreOperationRequest { operation_id },
        ) {
            Ok(completed) => completed,
            Err(error) => {
                // Another exact finalizer may have completed the shared
                // restore while this request was between its last durable
                // step and completion. Converge on the durable terminal row
                // instead of surfacing a phase conflict for finished work.
                if let Some(replayed) = self.completed_restore_replay(rpc, operation_id)? {
                    return Ok(replayed);
                }
                return Err(restore_failure(error));
            }
        };
        restored_response(
            &completed.command.operation,
            Some(completed.command.commit_version.get()),
            completed.command.replayed,
        )
    }

    fn completed_restore_replay(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        operation_id: types::OperationId,
    ) -> Result<Option<ExecutedRequest>, protocol::RpcFailure> {
        let context = self.write_context(
            rpc.route,
            derived_request_id(rpc.request_id, b"restore-terminal-replay", 0),
        )?;
        let Some(operation) =
            meta::get_restore(&self.meta, context, operation_id).map_err(restore_failure)?
        else {
            return Ok(None);
        };
        if operation.phase != types::RestorePhase::Complete {
            return Ok(None);
        }
        restored_response(&operation, None, true).map(Some)
    }

    fn begin_generic_index_registration(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::BeginGenericIndexRegistrationRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let outcome = meta::GenericIndexRegistrationService::new(&self.meta)
            .begin(meta::BeginGenericIndexRegistrationRequest {
                context: self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"generic-index-begin", 0),
                )?,
                operation_id: request.operation_id.into(),
                generation_id: request.generation_id.into(),
                workbench_id: workbench_id(&request.workbench)?,
                expected_workspace_incarnation_id: request.workspace_incarnation_id.into(),
                index_path: request.index_path.as_ref().map(relative_path).transpose()?,
                expected_current_generation: request
                    .expected_current_generation
                    .map(types::Generation::new)
                    .transpose()
                    .map_err(|error| invalid_argument(error.to_string()))?,
                capabilities: request
                    .capabilities
                    .iter()
                    .map(meta_generic_index_capability)
                    .collect::<Result<_, _>>()?,
                declared_row_count: request.declared_row_count,
            })
            .map_err(generic_index_failure)?;
        generic_index_registration_response(request.operation_id, outcome)
    }

    fn append_generic_index_rows(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::AppendGenericIndexRowsRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let outcome = meta::GenericIndexRegistrationService::new(&self.meta)
            .append(meta::AppendGenericIndexRowsRequest {
                context: self.write_context(
                    rpc.route,
                    derived_request_id(
                        rpc.request_id,
                        b"generic-index-append",
                        request.first_sequence,
                    ),
                )?,
                operation_id: request.operation_id.into(),
                first_sequence: request.first_sequence,
                rows: request
                    .rows
                    .iter()
                    .map(meta_generic_index_row)
                    .collect::<Result<_, _>>()?,
            })
            .map_err(generic_index_failure)?;
        let result = protocol::GenericIndexAppendResult {
            registration: generic_index_registration_status(
                request.operation_id,
                &outcome.command.operation,
            )?,
            receipt: protocol::GenericIndexAppendReceipt {
                first_sequence: outcome.receipt.first_sequence,
                row_count: outcome.receipt.row_count,
                commit_version: outcome.receipt.commit_version.get(),
                input_digest: protocol::Digest(outcome.receipt.input_digest),
                resulting_row_count: outcome.receipt.resulting_row_count,
                resulting_row_digest: protocol::Digest(outcome.receipt.resulting_row_digest),
            },
        };
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::GenericIndexAppend(result),
            commit_version: Some(outcome.command.commit_version.get()),
            replayed: outcome.command.replayed,
        })
    }

    fn finalize_generic_index_registration(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::FinalizeGenericIndexRegistrationRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let outcome = meta::GenericIndexRegistrationService::new(&self.meta)
            .finalize(meta::FinalizeGenericIndexRegistrationRequest {
                context: self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"generic-index-finalize", 0),
                )?,
                operation_id: request.operation_id.into(),
            })
            .map_err(generic_index_failure)?;
        generic_index_registration_response(request.operation_id, outcome)
    }

    fn abort_generic_index_registration(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::AbortGenericIndexRegistrationRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        self.claim_mutation(rpc)?;
        let outcome = meta::GenericIndexRegistrationService::new(&self.meta)
            .abort(meta::AbortGenericIndexRegistrationRequest {
                context: self.write_context(
                    rpc.route,
                    derived_request_id(rpc.request_id, b"generic-index-abort", 0),
                )?,
                operation_id: request.operation_id.into(),
                limit: request.limit as usize,
            })
            .map_err(generic_index_failure)?;
        let result = protocol::GenericIndexAbortResult {
            registration: generic_index_registration_status(
                request.operation_id,
                &outcome.command.operation,
            )?,
            removed_rows: u32::try_from(outcome.removed_rows)
                .map_err(|_| internal("generic index removed-row count exceeds u32"))?,
            removed_receipts: u32::try_from(outcome.removed_receipts)
                .map_err(|_| internal("generic index removed-receipt count exceeds u32"))?,
            cleanup_complete: outcome.cleanup_complete,
        };
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::GenericIndexAbort(result),
            commit_version: Some(outcome.command.commit_version.get()),
            replayed: outcome.command.replayed,
        })
    }

    fn get_generic_index_registration(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::GetGenericIndexRegistrationRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let operation = meta::GenericIndexRegistrationService::new(&self.meta)
            .get(self.read_context(rpc.route)?, request.operation_id.into())
            .map_err(generic_index_failure)?
            .ok_or_else(|| not_found("Generic index registration does not exist"))?;
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::GenericIndexRegistration(
                generic_index_registration_status(request.operation_id, &operation)?,
            ),
            commit_version: None,
            replayed: false,
        })
    }

    fn search(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        request: &protocol::SearchRequest,
    ) -> Result<ExecutedRequest, protocol::RpcFailure> {
        let context = self.read_context(rpc.route)?;
        let (scope, path_prefix) = query_scope(&request.scope)?;
        let profile = query_profile(&request.profile)?;
        let query = meta::SearchRequest {
            profile: profile.clone(),
            scope,
            path_prefix,
            predicates: query_predicates(&request.predicates)?,
            projection: query_field_ids(&request.projection)?,
            sort: query_sort(&request.sort)?,
            facets: query_field_ids(&request.facets)?,
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::search_paths_at(&self.meta, context, &query).map_err(query_failure)?;
        let mut hits = Vec::with_capacity(page.hits.len() + page.namespace_hits.len());
        match profile {
            meta::QueryProfile::ArtifactV1 => {
                if !page.namespace_hits.is_empty() {
                    return Err(internal("artifact query returned generic namespace rows"));
                }
                for hit in page.hits {
                    let workbench = protocol_workbench(&hit.workbench_id)?;
                    let workspace =
                        meta::get_visible_workspace_at(&self.meta, context, &hit.workbench_id)
                            .map_err(namespace_failure)?
                            .ok_or_else(|| {
                                internal("query hit references an invisible workbench")
                            })?;
                    let visible = meta::get_path_at_visible_workspace(
                        &self.meta, context, &workspace, &hit.path,
                    )
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
                    hits.push(protocol::SearchRow::Artifact(protocol::SearchHit {
                        metadata: Self::path_metadata(&workbench, workspace, hit.path, visible)?,
                        projection: protocol_field_values(hit.projection),
                    }));
                }
            }
            meta::QueryProfile::GenericNamespaceV1 { .. } => {
                if !page.hits.is_empty() {
                    return Err(internal("generic namespace query returned artifact rows"));
                }
                for hit in page.namespace_hits {
                    let artifact = hit
                        .artifact
                        .map(|artifact| {
                            Ok(protocol::GenericNamespaceArtifact {
                                generation: artifact.generation.get(),
                                logical_size: artifact.logical_size,
                                body_digest: protocol::DigestUri::new(artifact.body_digest_uri)
                                    .map_err(|error| internal(error.to_string()))?,
                                content_type: protocol::ContentType::new(artifact.content_type)
                                    .map_err(|error| internal(error.to_string()))?,
                                producer: artifact.producer,
                                manifest_identity: artifact.manifest_id,
                            })
                        })
                        .transpose()?;
                    hits.push(protocol::SearchRow::GenericNamespace(
                        protocol::GenericNamespaceHit {
                            workbench: protocol_workbench(&hit.workbench_id)?,
                            relative_path: hit
                                .relative_path
                                .map(|path| {
                                    protocol::RelativePath::new(path.as_str())
                                        .map_err(|error| internal(error.to_string()))
                                })
                                .transpose()?,
                            kind: match hit.kind {
                                meta::GenericNamespaceKind::Directory => {
                                    protocol::GenericNamespaceKind::Directory
                                }
                                meta::GenericNamespaceKind::Artifact => {
                                    protocol::GenericNamespaceKind::Artifact
                                }
                            },
                            artifact,
                            projection: protocol_field_values(hit.projection),
                            indexed_values: hit
                                .indexed_values
                                .into_iter()
                                .map(|(field_id, values)| protocol::GenericIndexFieldValues {
                                    field_id: field_id.as_str().to_owned(),
                                    values: values.into_iter().map(protocol_scalar).collect(),
                                })
                                .collect(),
                        },
                    ));
                }
            }
        }
        let facets = protocol_facets(page.facets);
        Ok(ExecutedRequest {
            result: protocol::WorkspaceResult::Search(protocol::SearchResult {
                hits,
                match_count: page.match_count,
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
            profile: query_profile(&request.profile)?,
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
        let page = meta::aggregate_paths_at(&self.meta, context, &query).map_err(query_failure)?;
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
                input_match_count: page.input_match_count,
                row_count: page.row_count,
                group_count: page.group_count,
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
            profile: query_profile(&request.profile)?,
            scope,
            path_prefix,
            path_match: match request.path_match {
                protocol::CatalogPathMatch::Prefix => meta::CatalogPathMatch::Prefix,
                protocol::CatalogPathMatch::Exact => meta::CatalogPathMatch::Exact,
            },
            field_prefix: request.field_prefix.clone(),
            include_facets: request.include_facets,
            cursor: request.page.cursor.clone(),
            limit: query_limit(request.page.limit),
        };
        let page = meta::catalog_fields_at(&self.meta, context, &query).map_err(query_failure)?;
        let fields = page
            .fields
            .into_iter()
            .map(|field| protocol::CatalogField {
                field_id: field.field_id.as_str().to_owned(),
                scalar_type: scalar_type_name(field.scalar_type).to_owned(),
                // The wire contract carries the observed scalar-type list only
                // for Generic custom fields; built-in and ArtifactV1 fields
                // summarize through `scalar_type` alone.
                scalar_types: if field.generic_custom {
                    field
                        .scalar_types
                        .into_iter()
                        .map(scalar_type_name)
                        .map(str::to_owned)
                        .collect()
                } else {
                    Vec::new()
                },
                generic_custom: field.generic_custom,
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
                facets: protocol_facets(page.facets),
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
        let page = meta::find_workspaces_at(&self.meta, context, &query).map_err(query_failure)?;
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
        let page = meta::read_changes_at(&self.meta, context, &query).map_err(query_failure)?;
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
                let workspace = meta::get_visible_workspace_at(&self.meta, read, &workbench)
                    .map_err(namespace_failure)?
                    .ok_or_else(|| not_found("workbench does not exist"))?;
                let current =
                    meta::get_path_at_visible_workspace(&self.meta, read, &workspace, &path)
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
                    .meta
                    .read_at(
                        read.root_id,
                        read.placement_generation,
                        read.owner_epoch,
                        meta::MetadataFamily::Operation,
                        &commit_key,
                        read.read_version,
                    )
                    .map_err(meta_failure)?
                    .ok_or_else(|| not_found("commit operation does not exist"))?;
                let commit = meta::BuildCommitOperationRecord::decode(&payload)
                    .map_err(|error| internal(format!("invalid commit operation: {error}")))?;
                let workspace = meta::get_visible_workspace_at(&self.meta, read, &workbench)
                    .map_err(namespace_failure)?
                    .ok_or_else(|| not_found("workbench does not exist"))?;
                let current =
                    meta::get_path_at_visible_workspace(&self.meta, read, &workspace, &path)
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
                    &self.meta,
                    write_context_from_publication(context),
                    operation_id,
                )
                .map_err(restore_failure)?
                .ok_or_else(|| not_found("restore operation does not exist"))?;
                let provenance = restore_commit_provenance(&restore)?;
                let binding = provenance.destination_binding.as_ref().ok_or_else(|| {
                    failure(
                        protocol::ErrorCode::PreconditionFailed,
                        "restore-staging publication requires late-bound destination authority",
                        false,
                        Some(protocol::ConflictKind::OperationState),
                    )
                })?;
                let expected_identity = match path.as_str() {
                    RUN_MANIFEST_PATH => binding.run_manifest_identity,
                    meta::RESTORE_MANIFEST_PATH => binding.restore_manifest_identity,
                    _ => {
                        return Err(failure(
                            protocol::ErrorCode::PreconditionFailed,
                            "restore-staging publication is not a destination-owned manifest",
                            false,
                            Some(protocol::ConflictKind::OperationState),
                        ))
                    }
                };
                let existing_publish = self.find_publish_operation(
                    rpc.route,
                    read.read_version,
                    request.operation_id,
                )?;
                let terminal_replay = existing_publish.as_ref().is_some_and(|operation| {
                    operation.phase == types::PublishPhase::Published
                        && operation.authority
                            == meta::PublishAuthority::RestoreStaging {
                                restore_operation_id: operation_id,
                            }
                });
                let fresh_admission = restore.phase == types::RestorePhase::SourceSealed
                    && binding.manifests.is_none();
                if !fresh_admission && !terminal_replay
                    || restore.destination_workbench_id != workbench
                    || types::OperationId::from(request.operation_id)
                        != expected_identity.publication_operation_id
                    || types::ArtifactRevisionId::from(request.artifact_revision_id)
                        != expected_identity.artifact_revision_id
                    || !matches!(request.condition, protocol::PublishCondition::CreateOnly)
                {
                    return Err(failure(
                        protocol::ErrorCode::PreconditionFailed,
                        "restore-staging publication does not match its sealed destination authority",
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
            activity_deadline_ms: publish_activity_deadline_ms(&self.meta)?,
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
        let outcome = meta::PublicationService::new(&self.meta)
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
        let outcome = meta::PublicationService::new(&self.meta)
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
                .meta
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::StagedObject,
                    &key,
                    context.read_version,
                )
                .map_err(meta_failure)?
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
        let outcome = meta::PublicationService::new(&self.meta)
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
        let outcome = meta::PublicationService::new(&self.meta)
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
        let finalizing_operation =
            if let Some(outcome) = self.replayed_publish(rpc.route, transition_id)? {
                outcome.operation
            } else {
                let operation = self.heartbeat_publish_operation(
                    rpc,
                    request.token,
                    b"publish-heartbeat-complete-transition",
                )?;
                match operation.phase {
                    types::PublishPhase::Uploading => {
                        let context = self.publication_context(rpc.route, transition_id)?;
                        meta::PublicationService::new(&self.meta)
                            .transition_publish(meta::TransitionPublishRequest {
                                context,
                                expected_operation: operation,
                                transition: meta::PublishTransition::BeginFinalization,
                            })
                            .map_err(publication_failure)?
                            .operation
                    }
                    types::PublishPhase::Finalizing => operation,
                    phase => {
                        return Err(failure(
                            protocol::ErrorCode::PreconditionFailed,
                            format!(
                            "artifact publication is {phase:?}, expected Uploading or Finalizing"
                        ),
                            false,
                            Some(protocol::ConflictKind::OperationState),
                        ));
                    }
                }
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
            operation_id: finalizing_operation.operation_id.into(),
            state_digest: publish_state_digest(&finalizing_operation)?,
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
        let outcome = meta::PublicationService::new(&self.meta)
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
        let outcome = meta::PublicationService::new(&self.meta)
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
        let service = meta::PublicationService::new(&self.meta);
        let read_version = self.meta.current_read_version().map_err(meta_failure)?;
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
        let read_version = self.meta.current_read_version().map_err(meta_failure)?;
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
                .meta
                .read_at(
                    route.root_id,
                    route.placement_generation,
                    route.owner_epoch,
                    meta::MetadataFamily::StagedObject,
                    &key,
                    read_version,
                )
                .map_err(meta_failure)?
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
        let service = meta::CommitService::new(&self.meta);
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
                    committed_at_unix_seconds: commit_time_unix_seconds(&self.meta)?,
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
            return build_commit_response(outcome);
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
            if !matches!(
                outcome.operation.phase,
                types::BuildCommitPhase::Building | types::BuildCommitPhase::Sealing
            ) {
                return build_commit_response(outcome);
            }
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
            if !matches!(
                outcome.operation.phase,
                types::BuildCommitPhase::Building | types::BuildCommitPhase::Sealing
            ) {
                return build_commit_response(outcome);
            }
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
            if !matches!(
                outcome.operation.phase,
                types::BuildCommitPhase::Building | types::BuildCommitPhase::Sealing
            ) {
                return build_commit_response(outcome);
            }
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
                .meta
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::Operation,
                    &key,
                    context.read_version,
                )
                .map_err(meta_failure)?
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
                    .meta
                    .read_at(
                        context.root_id,
                        context.placement_generation,
                        context.owner_epoch,
                        meta::MetadataFamily::ArtifactManifest,
                        &key,
                        context.read_version,
                    )
                    .map_err(meta_failure)?
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
                .meta
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
                .map_err(meta_failure)?;
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
                .meta
                .read_at(
                    context.root_id,
                    context.placement_generation,
                    context.owner_epoch,
                    meta::MetadataFamily::ArtifactRevision,
                    &meta::artifact_revision_key(context.root_id, *owner),
                    context.read_version,
                )
                .map_err(meta_failure)?
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
                .meta
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
                .map_err(meta_failure)?;
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
        let encoded =
            protocol::encode_request(&protocol::RpcRequest::Workspace(Box::new(request.clone())))
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
            object_namespace_id: Some(route.object_namespace_id),
            placement_generation: route.placement_generation,
            owner_epoch: route.owner_epoch,
            request_id,
            command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
            read_version: self.meta.current_read_version().map_err(meta_failure)?,
            root_fence_action: meta::RootFenceAction::RequireActive,
            predicates: Vec::new(),
            mutations: Vec::new(),
            history_projection: Vec::new(),
            event_projection: Vec::new(),
            deterministic_result: request_digest.to_vec(),
        }
        .seal();
        self.meta.execute(&command).map_err(meta_failure)?;
        Ok(false)
    }

    fn read_context(
        &self,
        route: protocol::RootRoute,
    ) -> Result<meta::RootReadContext, protocol::RpcFailure> {
        let route = route_parts(route)?;
        meta::RootReadContext::current(
            &self.meta,
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
            meta::get_snapshot_at(&self.meta, live, workbench, &snapshot_selector(selector)?)
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
        let lease_clock = self.meta.lease_clock_high_water().map_err(meta_failure)?;
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
            None => self.meta.current_read_version().map_err(meta_failure)?,
        };
        Ok(meta::RootWriteContext {
            root_id: route.root_id,
            logical_shard_id: route.logical_shard_id,
            object_namespace_id: route.object_namespace_id,
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
            object_namespace_id: context.object_namespace_id,
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
        self.meta
            .lookup_request(
                route.root_id,
                route.placement_generation,
                route.owner_epoch,
                request_id,
            )
            .map_err(meta_failure)
    }

    fn load_publish_operation(
        &self,
        route: protocol::RootRoute,
        read_version: types::ReadVersion,
        operation_id: protocol::OperationIdentity,
    ) -> Result<meta::PublishOperationRecord, protocol::RpcFailure> {
        self.find_publish_operation(route, read_version, operation_id)?
            .ok_or_else(|| not_found("publish operation does not exist"))
    }

    fn find_publish_operation(
        &self,
        route: protocol::RootRoute,
        read_version: types::ReadVersion,
        operation_id: protocol::OperationIdentity,
    ) -> Result<Option<meta::PublishOperationRecord>, protocol::RpcFailure> {
        let route = route_parts(route)?;
        let operation_id: types::OperationId = operation_id.into();
        let key = meta::operation_key(route.root_id, types::OperationKind::Publish, operation_id);
        let Some(payload) = self
            .meta
            .read_at(
                route.root_id,
                route.placement_generation,
                route.owner_epoch,
                meta::MetadataFamily::Operation,
                &key,
                read_version,
            )
            .map_err(meta_failure)?
        else {
            return Ok(None);
        };
        let operation = meta::PublishOperationRecord::decode(&payload)
            .map_err(|error| internal(format!("invalid durable publish operation: {error}")))?;
        if operation.operation_id != operation_id {
            return Err(internal("publish operation key and payload disagree"));
        }
        Ok(Some(operation))
    }

    fn heartbeat_publish_operation(
        &self,
        rpc: &protocol::WorkspaceRpcRequest,
        token: protocol::OperationToken,
        domain: &'static [u8],
    ) -> Result<meta::PublishOperationRecord, protocol::RpcFailure> {
        let heartbeat_id = derived_request_id(rpc.request_id, domain, 0);
        if let Some(outcome) = self.replayed_publish(rpc.route, heartbeat_id)? {
            // `claim_mutation` has already exact-bound this outer request id to
            // the complete encoded RPC, including the caller's pre-heartbeat
            // token. The durable heartbeat result necessarily contains the
            // post-heartbeat operation, so rechecking its digest against that
            // earlier token would make a response-safe retry reject itself.
            if outcome.operation.operation_id != types::OperationId::from(token.operation_id) {
                return Err(invalid_argument(
                    "replayed publish heartbeat belongs to another operation",
                ));
            }
            return Ok(outcome.operation);
        }
        let heartbeat_context = self.publication_context(rpc.route, heartbeat_id)?;
        let operation = self.load_publish_operation(
            rpc.route,
            heartbeat_context.read_version,
            token.operation_id,
        )?;
        require_publish_token(&operation, token)?;
        let activity_deadline_ms = publish_activity_deadline_ms(&self.meta)?;
        if activity_deadline_ms <= operation.activity_deadline_ms {
            return Ok(operation);
        }
        meta::PublicationService::new(&self.meta)
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
        self.execute_request_with_internal_retry(request)
    }
}

#[derive(Clone, Copy)]
struct RouteParts {
    protocol: protocol::RootRoute,
    root_id: types::RootId,
    logical_shard_id: types::LogicalShardId,
    object_namespace_id: types::ObjectNamespaceId,
    placement_generation: types::PlacementGeneration,
    owner_epoch: types::OwnerEpoch,
}

fn route_parts(route: protocol::RootRoute) -> Result<RouteParts, protocol::RpcFailure> {
    Ok(RouteParts {
        protocol: route,
        root_id: route.root_id.into(),
        logical_shard_id: route.logical_shard_id.into(),
        object_namespace_id: route.object_namespace_id.into(),
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

fn meta_generic_index_capability(
    capability: &protocol::GenericIndexFieldCapability,
) -> Result<meta::GenericIndexFieldCapability, protocol::RpcFailure> {
    Ok(meta::GenericIndexFieldCapability {
        field: query_field_id(&capability.field_id)?,
        operators: capability
            .operators
            .iter()
            .copied()
            .map(meta_generic_index_operator)
            .collect(),
        sortable: capability.sortable,
        facetable: capability.facetable,
    })
}

fn meta_generic_index_operator(operator: protocol::QueryOperator) -> meta::GenericIndexOperator {
    match operator {
        protocol::QueryOperator::Equal => meta::GenericIndexOperator::Equal,
        protocol::QueryOperator::NotEqual => meta::GenericIndexOperator::NotEqual,
        protocol::QueryOperator::In => meta::GenericIndexOperator::In,
        protocol::QueryOperator::Greater => meta::GenericIndexOperator::Greater,
        protocol::QueryOperator::GreaterOrEqual => meta::GenericIndexOperator::GreaterOrEqual,
        protocol::QueryOperator::Less => meta::GenericIndexOperator::Less,
        protocol::QueryOperator::LessOrEqual => meta::GenericIndexOperator::LessOrEqual,
        protocol::QueryOperator::Prefix => meta::GenericIndexOperator::Prefix,
        protocol::QueryOperator::Suffix => meta::GenericIndexOperator::Suffix,
        protocol::QueryOperator::Contains => meta::GenericIndexOperator::Contains,
        protocol::QueryOperator::Exists => meta::GenericIndexOperator::Exists,
        protocol::QueryOperator::NotExists => meta::GenericIndexOperator::NotExists,
    }
}

fn meta_generic_index_row(
    row: &protocol::GenericIndexRow,
) -> Result<meta::GenericIndexRowInput, protocol::RpcFailure> {
    Ok(meta::GenericIndexRowInput {
        relative_path: row.relative_path.as_ref().map(relative_path).transpose()?,
        values: row
            .values
            .iter()
            .map(|values| {
                Ok(meta::GenericIndexFieldValues {
                    field: query_field_id(&values.field_id)?,
                    values: values
                        .values
                        .iter()
                        .map(query_scalar)
                        .collect::<Result<_, _>>()?,
                })
            })
            .collect::<Result<_, protocol::RpcFailure>>()?,
    })
}

fn generic_index_registration_status(
    operation_id: protocol::OperationIdentity,
    operation: &meta::GenericIndexRegistrationOperationRecord,
) -> Result<protocol::GenericIndexRegistrationStatus, protocol::RpcFailure> {
    Ok(protocol::GenericIndexRegistrationStatus {
        operation_id,
        generation_id: operation.generation_id.into(),
        workspace_incarnation_id: operation.workspace_incarnation_id.into(),
        index_path: operation
            .index_path
            .as_ref()
            .map(|path| protocol::RelativePath::new(path.as_str()))
            .transpose()
            .map_err(|error| internal(error.to_string()))?,
        source_read_version: operation.source_read_version.get(),
        last_transition_version: operation.last_transition_version.get(),
        expected_current_generation: operation
            .expected_current_generation
            .map(types::Generation::get),
        capability_digest: protocol::Digest(operation.capability_digest),
        declared_row_count: operation.declared_row_count,
        appended_row_count: operation.appended_row_count,
        row_digest: protocol::Digest(operation.rolling_row_digest),
        phase: match operation.phase {
            types::GenericIndexRegistrationPhase::Preparing => {
                protocol::GenericIndexRegistrationPhase::Preparing
            }
            types::GenericIndexRegistrationPhase::Appending => {
                protocol::GenericIndexRegistrationPhase::Appending
            }
            types::GenericIndexRegistrationPhase::Sealing => {
                protocol::GenericIndexRegistrationPhase::Sealing
            }
            types::GenericIndexRegistrationPhase::Publishing => {
                protocol::GenericIndexRegistrationPhase::Publishing
            }
            types::GenericIndexRegistrationPhase::Complete => {
                protocol::GenericIndexRegistrationPhase::Complete
            }
            types::GenericIndexRegistrationPhase::Aborting => {
                protocol::GenericIndexRegistrationPhase::Aborting
            }
            types::GenericIndexRegistrationPhase::Cleaning => {
                protocol::GenericIndexRegistrationPhase::Cleaning
            }
            types::GenericIndexRegistrationPhase::Cleaned => {
                protocol::GenericIndexRegistrationPhase::Cleaned
            }
            types::GenericIndexRegistrationPhase::Quarantined => {
                protocol::GenericIndexRegistrationPhase::Quarantined
            }
        },
        published_pointer_generation: operation
            .published_pointer_generation
            .map(types::Generation::get),
        terminal_error: operation.terminal_error.clone(),
    })
}

fn generic_index_registration_response(
    operation_id: protocol::OperationIdentity,
    outcome: meta::GenericIndexRegistrationOutcome,
) -> Result<ExecutedRequest, protocol::RpcFailure> {
    Ok(ExecutedRequest {
        result: protocol::WorkspaceResult::GenericIndexRegistration(
            generic_index_registration_status(operation_id, &outcome.operation)?,
        ),
        commit_version: Some(outcome.commit_version.get()),
        replayed: outcome.replayed,
    })
}

fn query_profile(
    profile: &protocol::QueryProfile,
) -> Result<meta::QueryProfile, protocol::RpcFailure> {
    match profile {
        protocol::QueryProfile::ArtifactV1 => Ok(meta::QueryProfile::ArtifactV1),
        protocol::QueryProfile::GenericCustomIndexV1 {
            presentation_path_root,
        } => Ok(meta::QueryProfile::GenericNamespaceV1 {
            presentation_path_root: meta::PresentationPathRoot::new(presentation_path_root.clone())
                .map_err(|error| invalid_argument(error.to_string()))?,
        }),
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

fn protocol_facets(facets: Vec<meta::FacetResult>) -> Vec<protocol::FacetResult> {
    facets
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
        .collect()
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

fn publish_activity_deadline_ms(meta: &meta::MetaShard) -> Result<u64, protocol::RpcFailure> {
    let wall_clock_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| internal(format!("system clock is before Unix epoch: {error}")))?
            .as_millis(),
    )
    .map_err(|_| internal("system clock milliseconds exceed u64"))?;
    let lease_clock_ms = meta.lease_clock_high_water().map_err(meta_failure)?;
    wall_clock_ms
        .max(lease_clock_ms)
        .checked_add(PUBLISH_ACTIVITY_LEASE_MS)
        .ok_or_else(|| internal("publish activity deadline overflows u64"))
}

fn commit_time_unix_seconds(meta: &meta::MetaShard) -> Result<u64, protocol::RpcFailure> {
    let wall_clock_ms = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| internal(format!("system clock is before Unix epoch: {error}")))?
            .as_millis(),
    )
    .map_err(|_| internal("system clock milliseconds exceed u64"))?;
    let lease_clock_ms = meta.lease_clock_high_water().map_err(meta_failure)?;
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
        object_namespace_id: context.object_namespace_id,
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

fn restore_copy_request_id(
    request_id: protocol::RequestIdentity,
    next_member_sequence: u64,
) -> types::RequestId {
    derived_request_id(request_id, b"restore-copy", next_member_sequence)
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

fn build_commit_response(
    outcome: meta::BuildCommitOutcome,
) -> Result<ExecutedRequest, protocol::RpcFailure> {
    let terminal_message = || {
        outcome
            .operation
            .terminal_error
            .as_ref()
            .map(|error| error.message.as_str())
            .unwrap_or("commit construction was aborted")
    };
    match outcome.operation.phase {
        types::BuildCommitPhase::Aborting
        | types::BuildCommitPhase::Cleaning
        | types::BuildCommitPhase::Cleaned => {
            return Err(operation_terminal_failure(terminal_message()));
        }
        types::BuildCommitPhase::Quarantined => {
            return Err(operation_quarantined_failure(terminal_message()));
        }
        types::BuildCommitPhase::Building
        | types::BuildCommitPhase::Sealing
        | types::BuildCommitPhase::Complete => {}
    }
    Ok(ExecutedRequest {
        result: protocol::WorkspaceResult::Operation(build_commit_operation_status(
            &outcome.operation,
        )?),
        commit_version: Some(outcome.commit_version.get()),
        replayed: outcome.replayed,
    })
}

fn restore_terminal_failure(
    operation: &meta::RestoreOperationRecord,
) -> Option<protocol::RpcFailure> {
    let message = operation
        .terminal_error
        .as_ref()
        .map(|error| error.message.as_str())
        .unwrap_or("restore was aborted");
    match operation.phase {
        types::RestorePhase::Aborting
        | types::RestorePhase::Cleaning
        | types::RestorePhase::Cleaned => Some(operation_terminal_failure(message)),
        types::RestorePhase::Quarantined => Some(operation_quarantined_failure(message)),
        types::RestorePhase::Preparing
        | types::RestorePhase::Copying
        | types::RestorePhase::SourceSealed
        | types::RestorePhase::DestinationBuilding
        | types::RestorePhase::DestinationSealing
        | types::RestorePhase::Ready
        | types::RestorePhase::Complete => None,
    }
}

fn restore_manifest_identity(
    identity: protocol::RestoreManifestIdentity,
) -> meta::RestoreManifestIdentity {
    meta::RestoreManifestIdentity {
        publication_operation_id: identity.publication_operation_id.into(),
        artifact_revision_id: identity.artifact_revision_id.into(),
    }
}

fn protocol_restore_manifest_identity(
    identity: meta::RestoreManifestIdentity,
) -> protocol::RestoreManifestIdentity {
    protocol::RestoreManifestIdentity {
        publication_operation_id: identity.publication_operation_id.into(),
        artifact_revision_id: identity.artifact_revision_id.into(),
    }
}

fn restore_commit_provenance(
    operation: &meta::RestoreOperationRecord,
) -> Result<&meta::RestoreCommitProvenanceV5, protocol::RpcFailure> {
    let meta::RestoreCommitProvenance::V5(provenance) = &operation.commit_provenance else {
        return Err(internal(
            "legacy restore operation has no v5 commit provenance",
        ));
    };
    Ok(provenance)
}

#[cfg(feature = "restore-crash-test-support")]
fn restore_initialization_barrier_evidence(
    route: protocol::RootRoute,
    durable_read_version: u64,
    operation: &meta::RestoreOperationRecord,
) -> Result<RestoreInitializationBarrierEvidence, protocol::RpcFailure> {
    if durable_read_version == 0 {
        return Err(internal(
            "restore initialization barrier requires a durable read version",
        ));
    }
    operation.validate().map_err(|error| {
        internal(format!(
            "invalid restore initialization barrier state: {error}"
        ))
    })?;
    if operation.phase != types::RestorePhase::DestinationBuilding {
        return Err(internal(
            "restore initialization barrier requires DestinationBuilding",
        ));
    }
    let initialization_digest = operation.initialization_digest.ok_or_else(|| {
        internal("restore initialization barrier requires an initialization digest")
    })?;
    let provenance = restore_commit_provenance(operation)?;
    let binding = provenance
        .destination_binding
        .as_ref()
        .ok_or_else(|| internal("restore initialization barrier requires a destination binding"))?;
    let manifests = binding.manifests.as_ref().ok_or_else(|| {
        internal("restore initialization barrier requires both actual destination manifests")
    })?;
    if !restore_closure_is_pristine(&provenance.closure) {
        return Err(internal(
            "restore initialization barrier requires zero destination closure progress",
        ));
    }
    if binding.run_manifest_identity == binding.restore_manifest_identity {
        return Err(internal(
            "restore initialization barrier requires distinct manifest publication identities",
        ));
    }
    if operation.destination_restore_manifest_identity != Some(binding.restore_manifest_identity) {
        return Err(internal(
            "restore initialization barrier restore-manifest reservation drifted",
        ));
    }
    if manifests.restore_manifest.body_digest_uri != operation.restore_manifest.body_digest_uri
        || manifests.restore_manifest.logical_size != operation.restore_manifest.logical_size
        || manifests.restore_manifest.content_type != operation.restore_manifest.content_type
    {
        return Err(internal(
            "restore initialization barrier restore-manifest descriptor drifted",
        ));
    }

    let run_manifest = restore_manifest_barrier_evidence(
        operation.destination_workspace_incarnation_id,
        binding.run_manifest_identity,
        &manifests.run_manifest,
        "run manifest",
    )?;
    let restore_manifest = restore_manifest_barrier_evidence(
        operation.destination_workspace_incarnation_id,
        binding.restore_manifest_identity,
        &manifests.restore_manifest,
        "restore manifest",
    )?;

    Ok(RestoreInitializationBarrierEvidence {
        route,
        operation_id: operation.operation_id.into(),
        durable_read_version,
        phase: RestoreInitializationBarrierPhase::DestinationBuilding,
        initialization_digest: protocol::Digest(initialization_digest),
        destination_workspace_incarnation_id: operation.destination_workspace_incarnation_id.into(),
        destination_commit_id: binding.destination_commit_id.into(),
        run_manifest,
        restore_manifest,
        built_commit_members: provenance.closure.member_count,
        sealed_revisions: provenance.closure.revision_seal_count,
    })
}

#[cfg(feature = "restore-crash-test-support")]
fn authoritative_restore_initialization_readback(
    command_outcome: &meta::RestoreOperationRecord,
    durable: Option<meta::RestoreOperationRecord>,
) -> Result<meta::RestoreOperationRecord, protocol::RpcFailure> {
    let durable = durable
        .ok_or_else(|| internal("restore initialization is not durably readable after commit"))?;
    command_outcome.validate().map_err(|error| {
        internal(format!(
            "invalid restore initialization command outcome: {error}"
        ))
    })?;
    durable.validate().map_err(|error| {
        internal(format!(
            "invalid restore initialization post-commit readback: {error}"
        ))
    })?;
    if &durable != command_outcome {
        return Err(internal(
            "restore initialization post-commit readback does not exactly match the command outcome",
        ));
    }
    Ok(durable)
}

#[cfg(feature = "restore-crash-test-support")]
fn restore_manifest_barrier_evidence(
    destination_workspace_incarnation_id: types::WorkspaceIncarnationId,
    expected: meta::RestoreManifestIdentity,
    actual: &meta::RestoreManifestPublication,
    label: &str,
) -> Result<RestoreManifestBindingEvidence, protocol::RpcFailure> {
    let actual_identity = meta::RestoreManifestIdentity {
        publication_operation_id: actual.publication_operation_id,
        artifact_revision_id: actual.artifact_revision_id,
    };
    if actual_identity != expected
        || actual.workspace_incarnation_id != destination_workspace_incarnation_id
    {
        return Err(internal(format!(
            "restore initialization barrier {label} actual binding drifted"
        )));
    }
    Ok(RestoreManifestBindingEvidence {
        expected: protocol_restore_manifest_identity(expected),
        actual: RestoreManifestPublicationEvidence {
            identity: protocol_restore_manifest_identity(actual_identity),
            workspace_incarnation_id: actual.workspace_incarnation_id.into(),
            body_digest_uri: actual.body_digest_uri.clone(),
            manifest_digest_uri: actual.manifest_digest_uri.clone(),
            logical_size: actual.logical_size,
            content_type: actual.content_type.clone(),
        },
    })
}

#[cfg(feature = "restore-crash-test-support")]
fn restore_closure_is_pristine(closure: &meta::RestoreCommitClosureProgress) -> bool {
    closure.member_cursor.is_none()
        && closure.member_count == 0
        && closure.member_digest == [0; types::SHA256_BYTES]
        && closure.member_seal.is_none()
        && closure.revision_ref_count == 0
        && closure.revision_cursor.is_none()
        && closure.revision_seal_count == 0
        && closure.revision_digest == [0; types::SHA256_BYTES]
        && closure.revision_seal.is_none()
        && closure.parent_seal.is_none()
        && closure.cleanup_member_count == 0
        && closure.cleanup_revision_count == 0
}

fn restore_active_position(
    operation: &meta::RestoreOperationRecord,
) -> Result<(u8, u64, bool), protocol::RpcFailure> {
    let position = match operation.phase {
        types::RestorePhase::Preparing => (0, 0, false),
        types::RestorePhase::Copying => (1, operation.source_member_count, operation.source_eof),
        types::RestorePhase::SourceSealed => (2, 0, false),
        types::RestorePhase::DestinationBuilding => (
            3,
            restore_commit_provenance(operation)?.closure.member_count,
            false,
        ),
        types::RestorePhase::DestinationSealing => (
            4,
            restore_commit_provenance(operation)?
                .closure
                .revision_seal_count,
            false,
        ),
        types::RestorePhase::Ready => (5, 0, false),
        types::RestorePhase::Complete => (6, 0, false),
        types::RestorePhase::Aborting
        | types::RestorePhase::Cleaning
        | types::RestorePhase::Cleaned
        | types::RestorePhase::Quarantined => {
            return Err(operation_terminal_failure(
                "terminal restore state cannot be ordered as active progress",
            ));
        }
    };
    Ok(position)
}

fn optional_restore_seal_matches(
    first: Option<[u8; types::SHA256_BYTES]>,
    second: Option<[u8; types::SHA256_BYTES]>,
) -> bool {
    !matches!((first, second), (Some(first), Some(second)) if first != second)
}

fn restore_replay_lineage_matches(
    current: &meta::RestoreOperationRecord,
    candidate: &meta::RestoreOperationRecord,
) -> Result<(), protocol::RpcFailure> {
    current
        .validate()
        .map_err(|error| internal(format!("invalid durable restore state: {error}")))?;
    candidate
        .validate()
        .map_err(|error| internal(format!("invalid replayed restore state: {error}")))?;
    let current_provenance = restore_commit_provenance(current)?;
    let candidate_provenance = restore_commit_provenance(candidate)?;
    let bindings_match = match (
        current_provenance.destination_binding.as_ref(),
        candidate_provenance.destination_binding.as_ref(),
    ) {
        (Some(current), Some(candidate)) => {
            current.destination_commit_id == candidate.destination_commit_id
                && current.effective_content_digest_uri == candidate.effective_content_digest_uri
                && current.destination_projection_input_digest
                    == candidate.destination_projection_input_digest
                && current.run_manifest_identity == candidate.run_manifest_identity
                && current.restore_manifest_identity == candidate.restore_manifest_identity
                && !matches!(
                    (&current.manifests, &candidate.manifests),
                    (Some(current), Some(candidate)) if current != candidate
                )
        }
        _ => true,
    };
    let immutable_identity_matches = current.operation_id == candidate.operation_id
        && current.identity_digest == candidate.identity_digest
        && current.source_workbench_id == candidate.source_workbench_id
        && current.source_workspace_incarnation_id == candidate.source_workspace_incarnation_id
        && current.source == candidate.source
        && current.destination_workbench_id == candidate.destination_workbench_id
        && current.destination_workspace_incarnation_id
            == candidate.destination_workspace_incarnation_id
        && current.destination_restore_manifest_identity
            == candidate.destination_restore_manifest_identity
        && current.restore_manifest == candidate.restore_manifest
        && current_provenance.source_commit == candidate_provenance.source_commit
        && current_provenance.destination_committed_at_unix_seconds
            == candidate_provenance.destination_committed_at_unix_seconds
        && current_provenance.closure.parent_digest == candidate_provenance.closure.parent_digest
        && bindings_match;
    let known_seals_match =
        optional_restore_seal_matches(current.source_member_seal, candidate.source_member_seal)
            && optional_restore_seal_matches(current.member_seal, candidate.member_seal)
            && optional_restore_seal_matches(
                current_provenance.closure.member_seal,
                candidate_provenance.closure.member_seal,
            )
            && optional_restore_seal_matches(
                current_provenance.closure.revision_seal,
                candidate_provenance.closure.revision_seal,
            )
            && optional_restore_seal_matches(
                current_provenance.closure.parent_seal,
                candidate_provenance.closure.parent_seal,
            );
    let initialization_matches = !matches!(
        (current.initialization_digest, candidate.initialization_digest),
        (Some(current), Some(candidate)) if current != candidate
    );
    let result_matches = !matches!(
        (&current.result, &candidate.result),
        (Some(current), Some(candidate)) if current != candidate
    );
    if !immutable_identity_matches
        || !known_seals_match
        || !initialization_matches
        || !result_matches
    {
        return Err(internal(
            "replayed restore step does not match the durable operation lineage",
        ));
    }
    Ok(())
}

/// Returns true only when `candidate` may replace the caller's current durable
/// projection. Exact-request replay can yield an older command receipt; that
/// receipt is valid evidence but must never move local progress backwards.
fn restore_step_advances(
    current: &meta::RestoreOperationRecord,
    candidate: &meta::RestoreOperationRecord,
    replayed: bool,
    step: &'static str,
) -> Result<bool, protocol::RpcFailure> {
    restore_replay_lineage_matches(current, candidate)?;
    let current_provenance = restore_commit_provenance(current)?;
    let candidate_provenance = restore_commit_provenance(candidate)?;
    let progress_not_after =
        |earlier: &meta::RestoreOperationRecord,
         earlier_provenance: &meta::RestoreCommitProvenanceV5,
         later: &meta::RestoreOperationRecord,
         later_provenance: &meta::RestoreCommitProvenanceV5| {
            earlier.source_member_count <= later.source_member_count
                && earlier.next_member_sequence <= later.next_member_sequence
                && earlier_provenance.closure.member_count <= later_provenance.closure.member_count
                && earlier_provenance.closure.revision_ref_count
                    <= later_provenance.closure.revision_ref_count
                && earlier_provenance.closure.revision_seal_count
                    <= later_provenance.closure.revision_seal_count
                && (earlier.source_member_count != later.source_member_count
                    || (earlier.source_cursor == later.source_cursor
                        && earlier.source_member_rolling_digest
                            == later.source_member_rolling_digest))
                && (earlier.next_member_sequence != later.next_member_sequence
                    || earlier.member_rolling_digest == later.member_rolling_digest)
                && (earlier_provenance.closure.member_count
                    != later_provenance.closure.member_count
                    || (earlier_provenance.closure.member_cursor
                        == later_provenance.closure.member_cursor
                        && earlier_provenance.closure.member_digest
                            == later_provenance.closure.member_digest))
                && (earlier_provenance.closure.revision_seal_count
                    != later_provenance.closure.revision_seal_count
                    || (earlier_provenance.closure.revision_cursor
                        == later_provenance.closure.revision_cursor
                        && earlier_provenance.closure.revision_digest
                            == later_provenance.closure.revision_digest))
        };
    match restore_active_position(candidate)?.cmp(&restore_active_position(current)?) {
        std::cmp::Ordering::Less
            if replayed
                && progress_not_after(
                    candidate,
                    candidate_provenance,
                    current,
                    current_provenance,
                ) =>
        {
            Ok(false)
        }
        std::cmp::Ordering::Equal if replayed && candidate == current => Ok(false),
        std::cmp::Ordering::Greater
            if progress_not_after(current, current_provenance, candidate, candidate_provenance) =>
        {
            Ok(true)
        }
        std::cmp::Ordering::Less if replayed => Err(internal(format!(
            "{step} replay is older in phase but not a monotonic durable prefix"
        ))),
        std::cmp::Ordering::Greater => Err(internal(format!(
            "{step} advanced phase with non-monotonic restore progress"
        ))),
        std::cmp::Ordering::Less => Err(internal(format!(
            "{step} returned a non-replayed restore state older than durable progress"
        ))),
        std::cmp::Ordering::Equal => {
            Err(internal(format!("{step} made no durable restore progress")))
        }
    }
}

fn restore_commit_member_progress(
    operation: &meta::RestoreOperationRecord,
) -> Result<u64, protocol::RpcFailure> {
    Ok(restore_commit_provenance(operation)?.closure.member_count)
}

fn restore_revision_seal_progress(
    operation: &meta::RestoreOperationRecord,
) -> Result<u64, protocol::RpcFailure> {
    Ok(restore_commit_provenance(operation)?
        .closure
        .revision_seal_count)
}

fn protocol_restore_result(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::RestoreResult, protocol::RpcFailure> {
    let complete = operation
        .result
        .as_ref()
        .ok_or_else(|| internal("complete restore operation has no result"))?;
    let commit_receipt = operation
        .destination_commit_receipt()
        .map_err(|error| internal(format!("invalid terminal restore receipt: {error}")))?;
    Ok(protocol::RestoreResult {
        operation_id: operation.operation_id.into(),
        destination: protocol::WorkspaceSummary {
            workbench: protocol::WorkbenchName::new(operation.destination_workbench_id.as_str())
                .map_err(|error| internal(error.to_string()))?,
            workspace_incarnation_id: complete.destination_workspace_incarnation_id.into(),
            workspace_revision: complete.destination_workspace_revision.get(),
            commit_head: Some(commit_receipt.destination_commit_id.into()),
            commit_head_generation: Some(commit_receipt.destination_head_generation.get()),
        },
        member_count: complete.member_count,
        member_digest: protocol::Digest(complete.member_digest),
        metadata_rows_copied: complete.member_count,
        object_bytes_copied: 0,
    })
}

fn restored_response(
    operation: &meta::RestoreOperationRecord,
    commit_version: Option<u64>,
    replayed: bool,
) -> Result<ExecutedRequest, protocol::RpcFailure> {
    Ok(ExecutedRequest {
        result: protocol::WorkspaceResult::Restored(protocol_restore_result(operation)?),
        commit_version,
        replayed,
    })
}

fn protocol_restore_source_commit(
    source: &meta::RestoreSourceCommitSeal,
) -> Result<protocol::RestoreSourceCommitBinding, protocol::RpcFailure> {
    Ok(protocol::RestoreSourceCommitBinding {
        commit_id: source.commit_id.into(),
        content_digest: protocol::DigestUri::new(source.content_digest_uri.clone())
            .map_err(|error| internal(error.to_string()))?,
        manifest_digest: protocol::DigestUri::new(source.manifest_digest_uri.clone())
            .map_err(|error| internal(error.to_string()))?,
        tree_manifest_revision_id: source.tree_manifest_revision_id.into(),
        member_count: source.member_count,
        member_digest: protocol::Digest(source.member_digest),
    })
}

fn protocol_restore_manifest_binding(
    manifest: &meta::RestoreManifestPublication,
) -> Result<protocol::RestoreManifestBinding, protocol::RpcFailure> {
    Ok(protocol::RestoreManifestBinding {
        publication_operation_id: manifest.publication_operation_id.into(),
        workspace_incarnation_id: manifest.workspace_incarnation_id.into(),
        artifact_revision_id: manifest.artifact_revision_id.into(),
        descriptor: protocol::ArtifactDescriptor {
            logical_size: manifest.logical_size,
            body_digest: protocol::DigestUri::new(manifest.body_digest_uri.clone())
                .map_err(|error| internal(error.to_string()))?,
            manifest_digest: protocol::DigestUri::new(manifest.manifest_digest_uri.clone())
                .map_err(|error| internal(error.to_string()))?,
            content_type: protocol::ContentType::new(manifest.content_type.clone())
                .map_err(|error| internal(error.to_string()))?,
            producer: None,
            manifest_identity: None,
            index_fields: Vec::new(),
        },
    })
}

fn protocol_restore_destination_binding(
    binding: &meta::RestoreDestinationBinding,
) -> Result<protocol::RestoreDestinationBinding, protocol::RpcFailure> {
    Ok(protocol::RestoreDestinationBinding {
        destination_commit_id: binding.destination_commit_id.into(),
        effective_content_digest: protocol::DigestUri::new(
            binding.effective_content_digest_uri.clone(),
        )
        .map_err(|error| internal(error.to_string()))?,
        destination_run_manifest_projection_input_digest: protocol::Digest(
            binding.destination_projection_input_digest,
        ),
        destination_run_manifest_identity: protocol_restore_manifest_identity(
            binding.run_manifest_identity,
        ),
        destination_restore_manifest_identity: protocol_restore_manifest_identity(
            binding.restore_manifest_identity,
        ),
        destination_manifests: binding
            .manifests
            .as_ref()
            .map(|manifests| {
                Ok(protocol::RestoreDestinationManifestBindings {
                    run_manifest: protocol_restore_manifest_binding(&manifests.run_manifest)?,
                    restore_manifest: protocol_restore_manifest_binding(
                        &manifests.restore_manifest,
                    )?,
                })
            })
            .transpose()?,
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
    let provenance = restore_commit_provenance(operation)?;
    let sealed_source =
        match (
            operation.source_member_seal,
            operation.member_seal,
            operation.source_matches_base_commit,
        ) {
            (None, None, None) => None,
            (Some(source_digest), Some(materialized_digest), Some(source_matches_base_commit)) => {
                Some((
                    operation.source_member_count,
                    protocol::Digest(source_digest),
                    operation.next_member_sequence,
                    protocol::Digest(materialized_digest),
                    source_matches_base_commit,
                ))
            }
            _ => return Err(internal(
                "restore source and materialized seals are only valid as one complete projection",
            )),
        };
    let (
        source_member_count,
        source_member_digest,
        materialized_member_count,
        materialized_member_digest,
        source_matches_base_commit,
    ) = match sealed_source {
        Some((source_count, source_digest, materialized_count, materialized_digest, matches)) => (
            Some(source_count),
            Some(source_digest),
            Some(materialized_count),
            Some(materialized_digest),
            Some(matches),
        ),
        None => (None, None, None, None, None),
    };
    let destination_restore_manifest_identity = operation
        .destination_restore_manifest_identity
        .ok_or_else(|| internal("v5 restore has no restore-manifest publication identity"))?;
    Ok(protocol::RestoreOperationPreparation {
        request: protocol::PrepareRestoreRequest {
            operation_id: operation.operation_id.into(),
            source_workbench: protocol_workbench(&operation.source_workbench_id)?,
            source_workspace_incarnation_id: operation.source_workspace_incarnation_id.into(),
            source,
            destination_workbench: protocol_workbench(&operation.destination_workbench_id)?,
            destination_workspace_incarnation_id: operation
                .destination_workspace_incarnation_id
                .into(),
            destination_restore_manifest_identity: protocol_restore_manifest_identity(
                destination_restore_manifest_identity,
            ),
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
        source_commit: protocol_restore_source_commit(&provenance.source_commit)?,
        destination_committed_at_unix_seconds: provenance.destination_committed_at_unix_seconds,
        source_member_count,
        source_member_digest,
        materialized_member_count,
        materialized_member_digest,
        source_matches_base_commit,
        destination_binding: provenance
            .destination_binding
            .as_ref()
            .map(protocol_restore_destination_binding)
            .transpose()?
            .map(Box::new),
    })
}

fn sealed_restore_preparation(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::RestorePreparation, protocol::RpcFailure> {
    if !matches!(
        operation.phase,
        types::RestorePhase::SourceSealed
            | types::RestorePhase::DestinationBuilding
            | types::RestorePhase::DestinationSealing
            | types::RestorePhase::Ready
            | types::RestorePhase::Complete
    ) {
        return Err(operation_terminal_failure(
            "restore preparation did not reach a sealed phase",
        ));
    }
    if !operation.source_eof {
        return Err(internal("sealed restore source has not reached EOF"));
    }
    let provenance = restore_commit_provenance(operation)?;
    let source_member_digest = operation
        .source_member_seal
        .ok_or_else(|| internal("sealed restore has no source member digest"))?;
    let materialized_member_digest = operation
        .member_seal
        .ok_or_else(|| internal("sealed restore has no materialized member digest"))?;
    let source_matches_base_commit = operation
        .source_matches_base_commit
        .ok_or_else(|| internal("sealed restore has no base-commit comparison"))?;
    Ok(protocol::RestorePreparation {
        operation_id: operation.operation_id.into(),
        destination_workbench: protocol_workbench(&operation.destination_workbench_id)?,
        destination_workspace_incarnation_id: operation.destination_workspace_incarnation_id.into(),
        source_commit: protocol_restore_source_commit(&provenance.source_commit)?,
        destination_committed_at_unix_seconds: provenance.destination_committed_at_unix_seconds,
        source_member_count: operation.source_member_count,
        source_member_digest: protocol::Digest(source_member_digest),
        materialized_member_count: operation.next_member_sequence,
        materialized_member_digest: protocol::Digest(materialized_member_digest),
        source_matches_base_commit,
        destination_binding: provenance
            .destination_binding
            .as_ref()
            .map(protocol_restore_destination_binding)
            .transpose()?
            .map(Box::new),
    })
}

fn restore_operation_status(
    operation: &meta::RestoreOperationRecord,
) -> Result<protocol::OperationStatus, protocol::RpcFailure> {
    let (state, result, failure_body) = match operation.phase {
        types::RestorePhase::Preparing
        | types::RestorePhase::Copying
        | types::RestorePhase::SourceSealed
        | types::RestorePhase::DestinationBuilding
        | types::RestorePhase::DestinationSealing
        | types::RestorePhase::Ready => (protocol::OperationState::Running, None, None),
        types::RestorePhase::Complete => (
            protocol::OperationState::Succeeded,
            Some(protocol::OperationResult::Restore(protocol_restore_result(
                operation,
            )?)),
            None,
        ),
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
        meta::NamespaceError::Meta(source) => meta_failure(source),
        _ => internal(error.to_string()),
    }
}

fn generic_index_failure(error: meta::GenericIndexError) -> protocol::RpcFailure {
    match error {
        meta::GenericIndexError::Meta(source) => meta_failure(source),
        meta::GenericIndexError::Namespace(source) => namespace_failure(source),
        meta::GenericIndexError::WorkspaceMissing
        | meta::GenericIndexError::RegistrationRootMissing
        | meta::GenericIndexError::OperationMissing
        | meta::GenericIndexError::GenerationMissing
        | meta::GenericIndexError::RowPathMissing { .. } => not_found(error.to_string()),
        meta::GenericIndexError::GenerationAlreadyExists => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::GenericIndexError::OperationInputMismatch => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::GenericIndexError::WorkspaceIncarnationMismatch => {
            conflict(protocol::ConflictKind::Workspace, error.to_string(), None)
        }
        meta::GenericIndexError::CurrentPointerConflict => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::GenericIndexError::RegistrationRootIsArtifact
        | meta::GenericIndexError::GenerationMismatch
        | meta::GenericIndexError::InvalidPhase { .. }
        | meta::GenericIndexError::AppendSequenceMismatch { .. }
        | meta::GenericIndexError::AppendExceedsDeclaredRows => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::GenericIndexError::InvalidBatchLimit { .. }
        | meta::GenericIndexError::EmptyAppendBatch
        | meta::GenericIndexError::RowsNotStrictlyOrdered
        | meta::GenericIndexError::UndeclaredField { .. }
        | meta::GenericIndexError::PathJoinInvalid { .. } => invalid_argument(error.to_string()),
        meta::GenericIndexError::ResourceExhausted { .. } => failure(
            protocol::ErrorCode::ResourceExhausted,
            error.to_string(),
            false,
            None,
        ),
        meta::GenericIndexError::Record(_)
        | meta::GenericIndexError::WorkspaceCodec(_)
        | meta::GenericIndexError::HoldCodec(_)
        | meta::GenericIndexError::CurrentPointerMissing
        | meta::GenericIndexError::CurrentReferenceMissing
        | meta::GenericIndexError::RegistrationReferenceMissing
        | meta::GenericIndexError::HistoryHoldMissing
        | meta::GenericIndexError::HistoryHoldMismatch
        | meta::GenericIndexError::CounterOverflow { .. }
        | meta::GenericIndexError::ReplayResultMismatch
        | meta::GenericIndexError::CorruptKey { .. } => internal(error.to_string()),
    }
}

fn query_failure(error: meta::QueryError) -> protocol::RpcFailure {
    match error {
        meta::QueryError::Namespace(source) => namespace_failure(source),
        meta::QueryError::Meta(source) => meta_failure(source),
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
        | meta::QueryError::CommitHeadCodec { .. }
        | meta::QueryError::GenericIndexCodec { .. }
        | meta::QueryError::GenericIndexClosureMismatch { .. }
        | meta::QueryError::GenericIndexCapabilityConflict { .. } => internal(error.to_string()),
        meta::QueryError::InvalidLimit { .. }
        | meta::QueryError::BoundExceeded { .. }
        | meta::QueryError::InvalidPredicate { .. }
        | meta::QueryError::UnknownField { .. }
        | meta::QueryError::FieldTypeConflict { .. }
        | meta::QueryError::InvalidAggregate { .. }
        | meta::QueryError::InvalidFieldPrefix { .. }
        | meta::QueryError::InvalidPresentationPathRoot { .. }
        | meta::QueryError::ExactCatalogRequiresPath
        | meta::QueryError::CursorTooLarge { .. }
        | meta::QueryError::InvalidCursor { .. } => invalid_argument(error.to_string()),
    }
}

fn remove_path_failure(error: meta::RemovePathError) -> protocol::RpcFailure {
    match error {
        meta::RemovePathError::Meta(source) => meta_failure(source),
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

fn rename_path_failure(error: meta::RenamePathError) -> protocol::RpcFailure {
    match error {
        meta::RenamePathError::Meta(source) => meta_failure(source),
        meta::RenamePathError::WorkspaceNotFound
        | meta::RenamePathError::SourceNotFound
        | meta::RenamePathError::RevisionNotFound { .. } => not_found(error.to_string()),
        meta::RenamePathError::DestinationAlreadyExists => failure(
            protocol::ErrorCode::AlreadyExists,
            error.to_string(),
            false,
            Some(protocol::ConflictKind::PathGeneration),
        ),
        meta::RenamePathError::GenerationMismatch { actual, .. } => conflict(
            protocol::ConflictKind::PathGeneration,
            error.to_string(),
            Some(actual),
        ),
        meta::RenamePathError::WorkspaceUnavailable => {
            conflict(protocol::ConflictKind::Workspace, error.to_string(), None)
        }
        meta::RenamePathError::ConcurrentMutation => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::PathGeneration),
        ),
        meta::RenamePathError::RequestInputMismatch => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::RenamePathError::SamePath => invalid_argument(error.to_string()),
        meta::RenamePathError::RevisionUnavailable { .. }
        | meta::RenamePathError::ReservedManifest => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            false,
            None,
        ),
        meta::RenamePathError::RecordCodec(_)
        | meta::RenamePathError::QueryRecord(_)
        | meta::RenamePathError::WorkspaceRevisionOverflow
        | meta::RenamePathError::RevisionReferenceMissing
        | meta::RenamePathError::RevisionReferenceEpochAhead
        | meta::RenamePathError::DeterministicResultMismatch { .. } => internal(error.to_string()),
    }
}

fn snapshot_failure(error: meta::SnapshotError) -> protocol::RpcFailure {
    match error {
        meta::SnapshotError::Meta(source) => meta_failure(source),
        meta::SnapshotError::WorkspaceMissing { .. }
        | meta::SnapshotError::SnapshotMissing { .. }
        | meta::SnapshotError::AliasMissing { .. }
        | meta::SnapshotError::ConsumerMissing { .. } => not_found(error.to_string()),
        meta::SnapshotError::WorkspaceNotCommitted { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            "snapshot requires a committed workbench head",
            false,
            Some(protocol::ConflictKind::CommitHead),
        ),
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
        meta::SnapshotError::SourceCommitMissing
        | meta::SnapshotError::SourceCommitUnavailable { .. }
        | meta::SnapshotError::SourceCommitBindingMismatch
        | meta::SnapshotError::SourceCommitConsumerMissing
        | meta::SnapshotError::SourceCommitConsumerMismatch
        | meta::SnapshotError::CommitConsumerCountOverflow
        | meta::SnapshotError::CommitConsumerCountUnderflow
        | meta::SnapshotError::CommitConsumerEpochOverflow
        | meta::SnapshotError::CommitVersionOverflow
        | meta::SnapshotError::CommitCodec(_) => {
            internal("snapshot source commit metadata is inconsistent")
        }
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
        meta::SnapshotListError::Meta(source) => meta_failure(source),
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
        meta::RestoreError::Meta(source) => meta_failure(source),
        meta::RestoreError::GenericIndex(meta::GenericIndexError::Meta(source)) => {
            meta_failure(source)
        }
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
        meta::RestoreError::SnapshotCommitProvenanceMissing { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            "legacy snapshot has no sealed commit provenance; commit the source workbench and mint a new snapshot",
            false,
            Some(protocol::ConflictKind::OperationState),
        ),
        meta::RestoreError::RequestInputMismatch
        | meta::RestoreError::RestoreManifestBindingMismatch { .. }
        | meta::RestoreError::DestinationBindingMismatch { .. } => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            "restore request is bound to different durable inputs",
            false,
            None,
        ),
        meta::RestoreError::OperationIdentityMismatch { .. } => invalid_argument(
            "restore operation identity does not match the deterministic source/destination selector",
        ),
        meta::RestoreError::ConcurrentMutation
        | meta::RestoreError::PublicationCleanupPending { .. } => failure(
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
        | meta::RestoreError::ManifestRevisionMismatch
        | meta::RestoreError::GenericIndex(_) => failure(
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
        | meta::RestoreError::PublishCodec(_)
        | meta::RestoreError::QueryRecord(_) => internal(error.to_string()),
    }
}

fn restore_preparation_step_failure(error: meta::RestoreError) -> protocol::RpcFailure {
    match error {
        meta::RestoreError::InvalidPhase { .. } | meta::RestoreError::SourceAlreadyExhausted => {
            failure(
                protocol::ErrorCode::Conflict,
                error.to_string(),
                true,
                Some(protocol::ConflictKind::OperationState),
            )
        }
        other => restore_failure(other),
    }
}

fn publication_failure(error: meta::PublicationError) -> protocol::RpcFailure {
    match error {
        meta::PublicationError::Meta(source) => meta_failure(source),
        meta::PublicationError::ConcurrentMutation => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::OperationState),
        ),
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
        meta::CommitError::Meta(source) => meta_failure(source),
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

fn meta_failure(error: meta::MetaError) -> protocol::RpcFailure {
    match error {
        error @ meta::MetaError::Store {
            source: nokv_meta_store::StoreError::Unavailable(_),
            ..
        } => failure(protocol::ErrorCode::Internal, error.to_string(), true, None),
        error @ meta::MetaError::Store {
            source: nokv_meta_store::StoreError::OutcomeUnknown { .. },
            ..
        }
        | error @ meta::MetaError::Store {
            source: nokv_meta_store::StoreError::Fenced { .. },
            ..
        } => failure(
            protocol::ErrorCode::NotOwner,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::RootPlacement),
        ),
        error @ meta::MetaError::Store {
            source: nokv_meta_store::StoreError::LimitExceeded { .. },
            ..
        } => failure(
            protocol::ErrorCode::ResourceExhausted,
            error.to_string(),
            false,
            None,
        ),
        meta::MetaError::OwnerEpochMismatch { .. }
        | meta::MetaError::PlacementMismatch
        | meta::MetaError::RootFenceMissing
        | meta::MetaError::RootFenceStateMismatch { .. } => failure(
            protocol::ErrorCode::NotOwner,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::RootPlacement),
        ),
        meta::MetaError::RequestIdReused => failure(
            protocol::ErrorCode::RequestReplayMismatch,
            error.to_string(),
            false,
            None,
        ),
        meta::MetaError::PredicateFailed => {
            failure(protocol::ErrorCode::Conflict, error.to_string(), true, None)
        }
        meta::MetaError::WriteConflict | meta::MetaError::WriteReadVersionMismatch { .. } => {
            failure(
                protocol::ErrorCode::Conflict,
                error.to_string(),
                true,
                Some(protocol::ConflictKind::ReadVersion),
            )
        }
        meta::MetaError::ReadVersionInFuture { .. } => failure(
            protocol::ErrorCode::PreconditionFailed,
            error.to_string(),
            true,
            None,
        ),
        meta::MetaError::ReadStabilityExhausted { .. } => failure(
            protocol::ErrorCode::Conflict,
            error.to_string(),
            true,
            Some(protocol::ConflictKind::ReadVersion),
        ),
        meta::MetaError::InvalidCommand { .. } | meta::MetaError::CommandDigestMismatch => {
            invalid_argument(error.to_string())
        }
        _ => internal(error.to_string()),
    }
}

fn internal_metadata_conflict(failure: &protocol::RpcFailure) -> bool {
    failure.code == protocol::ErrorCode::Conflict
        && failure.retryable
        && failure.conflict == Some(protocol::ConflictKind::ReadVersion)
}

fn retry_internal_metadata_conflicts<T>(
    mut execute: impl FnMut() -> Result<T, protocol::RpcFailure>,
) -> Result<T, protocol::RpcFailure> {
    for attempt in 1..=MAX_INTERNAL_METADATA_ATTEMPTS {
        match execute() {
            Err(failure)
                if internal_metadata_conflict(&failure)
                    && attempt < MAX_INTERNAL_METADATA_ATTEMPTS => {}
            result => return result,
        }
    }
    unreachable!("internal metadata retry bound is non-zero")
}

fn retry_restore_preparation_progress<T>(
    mut execute: impl FnMut() -> Result<T, protocol::RpcFailure>,
) -> Result<T, protocol::RpcFailure> {
    for attempt in 1..=MAX_INTERNAL_METADATA_ATTEMPTS {
        match execute() {
            Err(failure)
                if failure.code == protocol::ErrorCode::Conflict
                    && failure.retryable
                    && failure.conflict == Some(protocol::ConflictKind::OperationState)
                    && attempt < MAX_INTERNAL_METADATA_ATTEMPTS => {}
            result => return result,
        }
    }
    unreachable!("internal restore progress retry bound is non-zero")
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

fn operation_quarantined_failure(message: &str) -> protocol::RpcFailure {
    failure(
        protocol::ErrorCode::Quarantined,
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
            object_namespace_id: types::ObjectNamespaceId::from_bytes([10; types::FIXED_ID_BYTES])
                .into(),
            placement_generation: placement().get(),
            owner_epoch,
        }
    }

    fn generic_query_profile() -> protocol::QueryProfile {
        protocol::QueryProfile::GenericCustomIndexV1 {
            presentation_path_root: "/agents".to_owned(),
        }
    }

    fn fence_command(
        store: &meta::MetaShard,
        request_id: types::RequestId,
        action: meta::RootFenceAction,
        owner_epoch: types::OwnerEpoch,
    ) -> meta::MetadataCommand {
        meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                [10; types::FIXED_ID_BYTES],
            )),
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

    fn ready_executor() -> (Arc<meta::MetaShard>, MetadataWorkspaceRequestExecutor) {
        let store = crate::test_support::meta_shard(shard());
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

    fn install_snapshot_committed_head(
        store: &meta::MetaShard,
        workspace_incarnation_id: types::WorkspaceIncarnationId,
        identity_fill: u8,
    ) {
        let commit_id = types::CommitId::from_bytes([identity_fill; types::SHA256_BYTES]);
        let commit = meta::CommitRecord {
            source_workspace_incarnation_id: workspace_incarnation_id,
            content_digest_uri: format!("sha256:{}", "11".repeat(types::SHA256_BYTES)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(types::SHA256_BYTES)),
            tree_manifest_revision_id: types::ArtifactRevisionId::from_bytes(
                [identity_fill.wrapping_add(1); types::FIXED_ID_BYTES],
            ),
            tree_digest_uri: format!("sha256:{}", "44".repeat(types::SHA256_BYTES)),
            member_count: 0,
            member_digest: [0; types::SHA256_BYTES],
            unique_revision_count: 1,
            revision_digest: [0x55; types::SHA256_BYTES],
            parent_commits: Vec::new(),
            parent_digest: [0; types::SHA256_BYTES],
            generic_index_count: 0,
            generic_index_digest: [0; types::SHA256_BYTES],
            producer: Some("snapshot-executor-test".to_owned()),
            lineage_projection: Vec::new(),
            consumer_count: 1,
            consumer_epoch: types::ConsumerEpoch::new(1),
            last_zero_consumer_version: None,
            state: types::CommitState::Sealed,
        };
        let head = meta::WorkbenchCommitHeadRecord {
            commit_id,
            head_generation: types::Generation::new(1).unwrap(),
        };
        let commit_key = meta::commit_key(root(), commit_id);
        let consumer_key =
            meta::workbench_head_commit_consumer_key(root(), commit_id, workspace_incarnation_id);
        let head_key = meta::workbench_commit_head_key(root(), workspace_incarnation_id);
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(identity_fill.wrapping_add(64)),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version: store.current_read_version().unwrap(),
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates: vec![
                        meta::CommandPredicate::Value {
                            family: meta::MetadataFamily::Commit,
                            key: commit_key.clone(),
                            expected: None,
                        },
                        meta::CommandPredicate::Value {
                            family: meta::MetadataFamily::CommitConsumer,
                            key: consumer_key.clone(),
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
                            family: meta::MetadataFamily::Commit,
                            key: commit_key,
                            value: commit.encode().unwrap(),
                        },
                        meta::CommandMutation::Put {
                            family: meta::MetadataFamily::CommitConsumer,
                            key: consumer_key,
                            value: meta::CommitConsumerRecord {
                                consumer_epoch_at_add: types::ConsumerEpoch::new(1),
                            }
                            .encode(),
                        },
                        meta::CommandMutation::Put {
                            family: meta::MetadataFamily::WorkbenchCommitHead,
                            key: head_key,
                            value: head.encode(),
                        },
                    ],
                    history_projection: Vec::new(),
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    #[test]
    fn preflight_uses_no_workspace_state_and_reports_the_exact_route() {
        let store = crate::test_support::meta_shard(shard());
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

    #[test]
    fn generic_index_zero_row_registration_replays_and_populates_declared_catalog() {
        let (_store, executor) = ready_executor();
        let workspace_incarnation_id = protocol::WorkspaceIdentity([0x31; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(0x30, "generic-index", 0x31, 1))
            .unwrap();
        let operation_id = protocol::OperationIdentity([0x32; types::FIXED_ID_BYTES]);
        let begin = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x33; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::BeginGenericIndexRegistration(
                protocol::BeginGenericIndexRegistrationRequest {
                    operation_id,
                    generation_id: protocol::GenericIndexGenerationIdentity(
                        [0x34; types::FIXED_ID_BYTES],
                    ),
                    workbench: protocol::WorkbenchName::new("generic-index").unwrap(),
                    workspace_incarnation_id,
                    index_path: None,
                    expected_current_generation: None,
                    capabilities: vec![protocol::GenericIndexFieldCapability {
                        field_id: "experiment.labels".to_owned(),
                        operators: vec![
                            protocol::QueryOperator::Equal,
                            protocol::QueryOperator::Exists,
                        ],
                        sortable: true,
                        facetable: true,
                    }],
                    declared_row_count: 0,
                },
            ),
        };
        let begun = executor.execute(&begin).unwrap();
        let begun_replay = executor.execute(&begin).unwrap();
        assert!(!begun.replayed);
        assert!(begun_replay.replayed);
        assert_eq!(begun_replay.commit_version, begun.commit_version);

        let finalize = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x35; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::FinalizeGenericIndexRegistration(
                protocol::FinalizeGenericIndexRegistrationRequest { operation_id },
            ),
        };
        let finalized = executor.execute(&finalize).unwrap();
        let finalized_replay = executor.execute(&finalize).unwrap();
        assert!(finalized_replay.replayed);
        assert_eq!(finalized_replay.commit_version, finalized.commit_version);
        let protocol::WorkspaceResult::GenericIndexRegistration(status) = finalized.result else {
            panic!("finalize returned the wrong result variant");
        };
        assert_eq!(
            status.phase,
            protocol::GenericIndexRegistrationPhase::Complete
        );
        assert_eq!(status.published_pointer_generation, Some(1));

        let catalog = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x36; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Workspace {
                        workbench: protocol::WorkbenchName::new("generic-index").unwrap(),
                        path_prefix: None,
                    },
                    path_match: protocol::CatalogPathMatch::Prefix,
                    field_prefix: Some("experiment".to_owned()),
                    include_facets: false,
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 10,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Catalog(catalog) = catalog.result else {
            panic!("catalog returned the wrong result variant");
        };
        assert_eq!(catalog.fields.len(), 1);
        assert_eq!(catalog.fields[0].field_id, "experiment.labels");
        assert!(catalog.fields[0].generic_custom);
        assert!(catalog.fields[0].scalar_types.is_empty());
    }

    #[test]
    fn generic_index_pointer_aba_conflict_is_a_nonretryable_operation_conflict() {
        let failure = generic_index_failure(meta::GenericIndexError::CurrentPointerConflict);

        assert_eq!(failure.code, protocol::ErrorCode::Conflict);
        assert!(!failure.retryable);
        assert_eq!(
            failure.conflict,
            Some(protocol::ConflictKind::OperationState)
        );
    }

    #[test]
    fn generic_index_append_replays_and_search_preserves_repeated_values() {
        let (_store, executor) = ready_executor();
        let workspace_incarnation_id = protocol::WorkspaceIdentity([0x41; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(0x40, "generic-values", 0x41, 1))
            .unwrap();
        let operation_id = protocol::OperationIdentity([0x42; types::FIXED_ID_BYTES]);
        executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x43; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::BeginGenericIndexRegistration(
                    protocol::BeginGenericIndexRegistrationRequest {
                        operation_id,
                        generation_id: protocol::GenericIndexGenerationIdentity(
                            [0x44; types::FIXED_ID_BYTES],
                        ),
                        workbench: protocol::WorkbenchName::new("generic-values").unwrap(),
                        workspace_incarnation_id,
                        index_path: None,
                        expected_current_generation: None,
                        capabilities: vec![protocol::GenericIndexFieldCapability {
                            field_id: "experiment.labels".to_owned(),
                            operators: vec![protocol::QueryOperator::Equal],
                            sortable: true,
                            facetable: true,
                        }],
                        declared_row_count: 1,
                    },
                ),
            })
            .unwrap();
        let append = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x45; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::AppendGenericIndexRows(
                protocol::AppendGenericIndexRowsRequest {
                    operation_id,
                    first_sequence: 0,
                    rows: vec![protocol::GenericIndexRow {
                        relative_path: Some(protocol::RelativePath::new("metadata").unwrap()),
                        values: vec![protocol::GenericIndexFieldValues {
                            field_id: "experiment.labels".to_owned(),
                            values: vec![
                                protocol::ScalarValue::String("alpha".to_owned()),
                                protocol::ScalarValue::Unsigned(7),
                                protocol::ScalarValue::String("alpha".to_owned()),
                            ],
                        }],
                    }],
                },
            ),
        };
        let appended = executor.execute(&append).unwrap();
        let appended_replay = executor.execute(&append).unwrap();
        assert!(appended_replay.replayed);
        assert_eq!(appended_replay.commit_version, appended.commit_version);
        executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x46; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::FinalizeGenericIndexRegistration(
                    protocol::FinalizeGenericIndexRegistrationRequest { operation_id },
                ),
            })
            .unwrap();

        let searched = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x47; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Workspace {
                        workbench: protocol::WorkbenchName::new("generic-values").unwrap(),
                        path_prefix: None,
                    },
                    predicates: vec![protocol::QueryPredicate {
                        field_id: "experiment.labels".to_owned(),
                        operator: protocol::QueryOperator::Equal,
                        operand: protocol::QueryOperand::Scalar(protocol::ScalarValue::String(
                            "alpha".to_owned(),
                        )),
                    }],
                    projection: vec!["experiment.labels".to_owned()],
                    sort: Vec::new(),
                    facets: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 10,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Search(searched) = searched.result else {
            panic!("search returned the wrong result variant");
        };
        assert_eq!(searched.hits.len(), 1);
        let protocol::SearchRow::GenericNamespace(hit) = &searched.hits[0] else {
            panic!("Generic custom-index search returned an ArtifactV1 row");
        };
        assert_eq!(
            hit.indexed_values,
            vec![protocol::GenericIndexFieldValues {
                field_id: "experiment.labels".to_owned(),
                values: vec![
                    protocol::ScalarValue::String("alpha".to_owned()),
                    protocol::ScalarValue::Unsigned(7),
                    protocol::ScalarValue::String("alpha".to_owned()),
                ],
            }]
        );
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

    fn terminal_build_operation(
        phase: types::BuildCommitPhase,
    ) -> meta::BuildCommitOperationRecord {
        let mut operation = meta::BuildCommitOperationRecord {
            operation_id: types::OperationId::from_bytes([0x71; types::FIXED_ID_BYTES]),
            identity_digest: [0; types::SHA256_BYTES],
            initialization_digest: [0; types::SHA256_BYTES],
            workbench_id: types::WorkbenchId::new("terminal-commit").unwrap(),
            source_workspace_incarnation_id: types::WorkspaceIncarnationId::from_bytes(
                [0x72; types::FIXED_ID_BYTES],
            ),
            source_read_version: types::ReadVersion::new(1).unwrap(),
            commit_id: types::CommitId::from_bytes([0x73; types::SHA256_BYTES]),
            expected_head: None,
            content_digest_uri: format!("sha256:{}", "74".repeat(types::SHA256_BYTES)),
            manifest_digest_uri: format!("sha256:{}", "75".repeat(types::SHA256_BYTES)),
            projection_input_digest: [0x76; types::SHA256_BYTES],
            tree_manifest_revision_id: types::ArtifactRevisionId::from_bytes(
                [0x77; types::FIXED_ID_BYTES],
            ),
            replace: false,
            run_manifest_condition: meta::CommitManifestCondition::CreateOnly,
            committed_at_unix_seconds: 1,
            commit_staged_run_manifest: None,
            producer: None,
            lineage_projection: Vec::new(),
            parent_commits: Vec::new(),
            phase,
            member_cursor: None,
            member_count: 0,
            member_digest: [0; types::SHA256_BYTES],
            path_members_complete: false,
            generic_index_cursor: None,
            generic_index_count: 0,
            generic_index_digest: [0; types::SHA256_BYTES],
            generic_indexes_complete: false,
            generic_index_ref_cursor: None,
            generic_index_ref_count: 0,
            generic_index_ref_digest: [0; types::SHA256_BYTES],
            generic_index_refs_complete: false,
            members_complete: false,
            revision_ref_count: 0,
            revision_cursor: None,
            revision_seal_count: 0,
            revision_digest: [0; types::SHA256_BYTES],
            revisions_complete: false,
            parent_cursor: 0,
            parent_digest: [0; types::SHA256_BYTES],
            parents_complete: false,
            cleanup_member_count: 0,
            cleanup_generic_index_count: 0,
            cleanup_revision_count: 0,
            cleanup_parent_count: 0,
            history_hold_released: false,
            result: None,
            terminal_error: Some(meta::CommitOperationTerminalError {
                kind: meta::CommitOperationErrorKind::InvariantViolation,
                message: "serving transaction capacity cannot admit one commit member step"
                    .to_owned(),
            }),
        };
        operation.seal_digests();
        operation
    }

    #[test]
    fn commit_capacity_abort_is_reported_as_a_terminal_rpc_failure() {
        let operation = terminal_build_operation(types::BuildCommitPhase::Aborting);
        let failure = build_commit_response(meta::BuildCommitOutcome {
            commit_version: types::CommitVersion::new(2).unwrap(),
            operation: operation.clone(),
            replayed: false,
        })
        .unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::OperationFailed);
        assert_eq!(
            failure.conflict,
            Some(protocol::ConflictKind::OperationState)
        );
        assert!(!failure.retryable);
        assert_eq!(
            failure.message,
            operation.terminal_error.as_ref().unwrap().message
        );

        let mut quarantined = operation;
        quarantined.phase = types::BuildCommitPhase::Quarantined;
        let failure = build_commit_response(meta::BuildCommitOutcome {
            commit_version: types::CommitVersion::new(3).unwrap(),
            operation: quarantined,
            replayed: true,
        })
        .unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::Quarantined);
        assert_eq!(
            failure.conflict,
            Some(protocol::ConflictKind::OperationState)
        );
        assert!(!failure.retryable);
    }

    fn replace_visible_workspace_marker(
        store: &meta::MetaShard,
        workbench: &str,
        previous_incarnation: types::WorkspaceIncarnationId,
        previous_revision: types::WorkspaceRevision,
        replacement_incarnation: types::WorkspaceIncarnationId,
        replacement_revision: types::WorkspaceRevision,
        request_sequence: u8,
    ) {
        let workbench = types::WorkbenchId::new(workbench).unwrap();
        let key = meta::workspace_current_key(root(), &workbench);
        let previous = meta::WorkspaceRecord {
            incarnation_id: previous_incarnation,
            workspace_revision: previous_revision,
            state: types::WorkspaceState::Visible,
            owning_operation_id: None,
        }
        .encode()
        .unwrap();
        let replacement = meta::WorkspaceRecord {
            incarnation_id: replacement_incarnation,
            workspace_revision: replacement_revision,
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
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(request_sequence),
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
        store: &meta::MetaShard,
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
            path_members_complete: true,
            generic_index_cursor: None,
            generic_index_count: 0,
            generic_index_digest: [0; types::SHA256_BYTES],
            generic_indexes_complete: true,
            generic_index_ref_cursor: None,
            generic_index_ref_count: 0,
            generic_index_ref_digest: [0; types::SHA256_BYTES],
            generic_index_refs_complete: true,
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
            cleanup_generic_index_count: 0,
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
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
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
        store: &meta::MetaShard,
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
        let path_digest = meta::path_index_digest(&path);
        let index_generation = types::PathIndexGenerationId::from_bytes([1; types::FIXED_ID_BYTES]);
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
            index_generation,
            path_digest,
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
        let locator_key = meta::path_index_locator_key(
            root(),
            workbench_incarnation,
            path_digest,
            index_generation,
        );
        let index_key = meta::secondary_index_key(
            root(),
            &index_field,
            &index_value,
            workbench_incarnation,
            path_digest,
            index_generation,
        );
        let command = meta::MetadataCommand {
            schema_id: meta::SCHEMA_ID.to_owned(),
            root_id: root(),
            logical_shard_id: shard(),
            object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                [10; types::FIXED_ID_BYTES],
            )),
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
                    family: meta::MetadataFamily::PathIndexLocator,
                    key: locator_key.clone(),
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
                    family: meta::MetadataFamily::PathIndexLocator,
                    key: locator_key,
                    value: meta::PathIndexLocatorRecord {
                        state: meta::PathIndexLocatorState::Published,
                        path,
                    }
                    .encode()
                    .unwrap(),
                },
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::SecondaryIndex,
                    key: index_key,
                    value: meta::SecondaryIndexRecord {
                        path_digest,
                        index_generation,
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

    fn put_shared_revision_paths(
        store: &meta::MetaShard,
        workbench_incarnation: types::WorkspaceIncarnationId,
        total_paths: usize,
    ) {
        assert!(total_paths > 0);
        put_visible_path(store, workbench_incarnation);
        if total_paths == 1 {
            return;
        }

        let revision = types::ArtifactRevisionId::from_bytes([9; types::FIXED_ID_BYTES]);
        let revision_key = meta::artifact_revision_key(root(), revision);
        let read_version = store.current_read_version().unwrap();
        let revision_payload = store
            .read_at(
                root(),
                placement(),
                owner(1),
                meta::MetadataFamily::ArtifactRevision,
                &revision_key,
                read_version,
            )
            .unwrap()
            .expect("seeded revision exists");
        let mut revision_record = meta::ArtifactRevisionRecord::decode(&revision_payload).unwrap();
        revision_record.strong_reference_count = u64::try_from(total_paths).unwrap();
        revision_record.reference_epoch = types::ReferenceEpoch::new(2);

        let mut predicates = vec![meta::CommandPredicate::Value {
            family: meta::MetadataFamily::ArtifactRevision,
            key: revision_key.clone(),
            expected: Some(revision_payload),
        }];
        let mut mutations = vec![meta::CommandMutation::Put {
            family: meta::MetadataFamily::ArtifactRevision,
            key: revision_key.clone(),
            value: revision_record.encode().unwrap(),
        }];
        for index in 1..total_paths {
            let path = types::NormalizedRelativePath::new(format!("outputs/shared-{index:03}.bin"))
                .unwrap();
            let path_digest = meta::path_index_digest(&path);
            let index_generation = types::PathIndexGenerationId::from_bytes(
                u128::try_from(index + 1).unwrap().to_be_bytes(),
            );
            let path_key = meta::path_current_key(root(), workbench_incarnation, &path);
            let locator_key = meta::path_index_locator_key(
                root(),
                workbench_incarnation,
                path_digest,
                index_generation,
            );
            let reference_key =
                meta::path_revision_ref_key(root(), workbench_incarnation, &path, revision);
            let path_record = meta::PathEntry {
                generation: types::Generation::new(1).unwrap(),
                index_generation,
                path_digest,
                artifact_revision_id: revision,
                body_digest_uri: "sha256:body".to_owned(),
                manifest_digest_uri: "sha256:manifest".to_owned(),
                logical_size: 0,
                dependency_count: 0,
                dependency_depth: 0,
                content_type: "application/octet-stream".to_owned(),
                producer: Some("executor-test".to_owned()),
                manifest_id: Some("manifest-1".to_owned()),
                typed_index_projection: meta::TypedProjection::empty().encode().unwrap(),
            };
            predicates.extend([
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::PathCurrent,
                    key: path_key.clone(),
                    expected: None,
                },
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::PathIndexLocator,
                    key: locator_key.clone(),
                    expected: None,
                },
                meta::CommandPredicate::Value {
                    family: meta::MetadataFamily::RevisionRef,
                    key: reference_key.clone(),
                    expected: None,
                },
            ]);
            mutations.extend([
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::PathCurrent,
                    key: path_key,
                    value: path_record.encode().unwrap(),
                },
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::PathIndexLocator,
                    key: locator_key,
                    value: meta::PathIndexLocatorRecord {
                        state: meta::PathIndexLocatorState::Published,
                        path,
                    }
                    .encode()
                    .unwrap(),
                },
                meta::CommandMutation::Put {
                    family: meta::MetadataFamily::RevisionRef,
                    key: reference_key,
                    value: meta::RevisionRefRecord {
                        reference_epoch_at_add: types::ReferenceEpoch::new(2),
                    }
                    .encode()
                    .unwrap(),
                },
            ]);
        }
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(203),
                    command_digest: types::CommandDigest::from_bytes([0; types::SHA256_BYTES]),
                    read_version,
                    root_fence_action: meta::RootFenceAction::RequireActive,
                    predicates,
                    mutations,
                    history_projection: vec![meta::HistoryProjection {
                        family: meta::MetadataFamily::ArtifactRevision,
                        key: revision_key,
                    }],
                    event_projection: Vec::new(),
                    deterministic_result: Vec::new(),
                }
                .seal(),
            )
            .unwrap();
    }

    fn corrupt_visible_path_revision(store: &meta::MetaShard) {
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
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
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
        store: &meta::MetaShard,
        workbench_incarnation: types::WorkspaceIncarnationId,
        paths: &[&str],
    ) {
        let mut predicates = Vec::with_capacity(paths.len() * 2);
        let mut mutations = Vec::with_capacity(paths.len() * 2);
        for (index, raw_path) in paths.iter().enumerate() {
            let path = types::NormalizedRelativePath::new(*raw_path).unwrap();
            let key = meta::path_current_key(root(), workbench_incarnation, &path);
            let fill = u8::try_from(index + 1).unwrap();
            let path_digest = meta::path_index_digest(&path);
            let index_generation =
                types::PathIndexGenerationId::from_bytes([fill; types::FIXED_ID_BYTES]);
            let locator_key = meta::path_index_locator_key(
                root(),
                workbench_incarnation,
                path_digest,
                index_generation,
            );
            let body_hex = format!("{fill:02x}").repeat(32);
            let manifest_fill = fill.saturating_add(0x40);
            let manifest_hex = format!("{manifest_fill:02x}").repeat(32);
            let entry = meta::PathEntry {
                generation: types::Generation::new(u64::from(fill)).unwrap(),
                index_generation,
                path_digest,
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
            predicates.push(meta::CommandPredicate::Value {
                family: meta::MetadataFamily::PathIndexLocator,
                key: locator_key.clone(),
                expected: None,
            });
            mutations.push(meta::CommandMutation::Put {
                family: meta::MetadataFamily::PathCurrent,
                key,
                value: entry.encode().unwrap(),
            });
            mutations.push(meta::CommandMutation::Put {
                family: meta::MetadataFamily::PathIndexLocator,
                key: locator_key,
                value: meta::PathIndexLocatorRecord {
                    state: meta::PathIndexLocatorState::Published,
                    path,
                }
                .encode()
                .unwrap(),
            });
        }
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
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
        store: &meta::MetaShard,
        workbench_incarnation: types::WorkspaceIncarnationId,
        row_count: u64,
    ) -> protocol::WorkspacePath {
        let revision = types::ArtifactRevisionId::from_bytes([8; types::FIXED_ID_BYTES]);
        let path = types::NormalizedRelativePath::new("outputs/ranged.bin").unwrap();
        let path_digest = meta::path_index_digest(&path);
        let index_generation = types::PathIndexGenerationId::from_bytes([8; types::FIXED_ID_BYTES]);
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
            index_generation,
            path_digest,
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
        let locator_key = meta::path_index_locator_key(
            root(),
            workbench_incarnation,
            path_digest,
            index_generation,
        );
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
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
                            family: meta::MetadataFamily::PathIndexLocator,
                            key: locator_key.clone(),
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
                            family: meta::MetadataFamily::PathIndexLocator,
                            key: locator_key,
                            value: meta::PathIndexLocatorRecord {
                                state: meta::PathIndexLocatorState::Published,
                                path: path.clone(),
                            }
                            .encode()
                            .unwrap(),
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
                        object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                            [10; types::FIXED_ID_BYTES],
                        )),
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

    fn restore_destination_manifests() -> meta::RestoreDestinationManifests {
        let destination_incarnation =
            types::WorkspaceIncarnationId::from_bytes([0x52; types::FIXED_ID_BYTES]);
        meta::RestoreDestinationManifests {
            run_manifest: meta::RestoreManifestPublication {
                publication_operation_id: types::OperationId::from_bytes(
                    [0x54; types::FIXED_ID_BYTES],
                ),
                workspace_incarnation_id: destination_incarnation,
                artifact_revision_id: types::ArtifactRevisionId::from_bytes(
                    [0x64; types::FIXED_ID_BYTES],
                ),
                body_digest_uri: format!("sha256:{}", "77".repeat(types::SHA256_BYTES)),
                manifest_digest_uri: format!("sha256:{}", "78".repeat(types::SHA256_BYTES)),
                logical_size: 1,
                content_type: meta::RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            restore_manifest: meta::RestoreManifestPublication {
                publication_operation_id: types::OperationId::from_bytes(
                    [0x55; types::FIXED_ID_BYTES],
                ),
                workspace_incarnation_id: destination_incarnation,
                artifact_revision_id: types::ArtifactRevisionId::from_bytes(
                    [0x65; types::FIXED_ID_BYTES],
                ),
                body_digest_uri: format!("sha256:{}", "00".repeat(types::SHA256_BYTES)),
                manifest_digest_uri: format!("sha256:{}", "79".repeat(types::SHA256_BYTES)),
                logical_size: 2,
                content_type: meta::RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
        }
    }

    fn restore_destination_binding(
        manifests: Option<meta::RestoreDestinationManifests>,
    ) -> meta::RestoreDestinationBinding {
        meta::RestoreDestinationBinding {
            destination_commit_id: types::CommitId::from_bytes([0x63; types::SHA256_BYTES]),
            effective_content_digest_uri: format!("sha256:{}", "11".repeat(types::SHA256_BYTES)),
            destination_projection_input_digest: [0x66; types::SHA256_BYTES],
            run_manifest_identity: meta::RestoreManifestIdentity {
                publication_operation_id: types::OperationId::from_bytes(
                    [0x54; types::FIXED_ID_BYTES],
                ),
                artifact_revision_id: types::ArtifactRevisionId::from_bytes(
                    [0x64; types::FIXED_ID_BYTES],
                ),
            },
            restore_manifest_identity: meta::RestoreManifestIdentity {
                publication_operation_id: types::OperationId::from_bytes(
                    [0x55; types::FIXED_ID_BYTES],
                ),
                artifact_revision_id: types::ArtifactRevisionId::from_bytes(
                    [0x65; types::FIXED_ID_BYTES],
                ),
            },
            manifests,
        }
    }

    #[test]
    fn restore_copy_request_identity_tracks_the_durable_cursor() {
        let request = protocol::RequestIdentity([8; types::FIXED_ID_BYTES]);
        let first = restore_copy_request_id(request, 0);
        let exact_retry = restore_copy_request_id(request, 0);
        let advanced = restore_copy_request_id(
            request,
            u64::try_from(meta::MAX_RESTORE_BATCH_MEMBERS).unwrap(),
        );

        assert_eq!(exact_retry, first);
        assert_ne!(advanced, first);
    }

    fn source_sealed_restore_operation() -> meta::RestoreOperationRecord {
        let destination_incarnation =
            types::WorkspaceIncarnationId::from_bytes([0x52; types::FIXED_ID_BYTES]);
        let mut identity_digest = [0x11; types::SHA256_BYTES];
        identity_digest[..types::FIXED_ID_BYTES].fill(0x41);
        let source_commit_id = types::CommitId::from_bytes([0x61; types::SHA256_BYTES]);
        let operation = meta::RestoreOperationRecord {
            operation_id: types::OperationId::from_bytes([0x41; types::FIXED_ID_BYTES]),
            identity_digest,
            initialization_digest: None,
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
            destination_restore_manifest_identity: Some(meta::RestoreManifestIdentity {
                publication_operation_id: types::OperationId::from_bytes(
                    [0x55; types::FIXED_ID_BYTES],
                ),
                artifact_revision_id: types::ArtifactRevisionId::from_bytes(
                    [0x65; types::FIXED_ID_BYTES],
                ),
            }),
            restore_manifest: meta::RestoreManifestDescriptor {
                body_digest_uri: format!("sha256:{}", "00".repeat(32)),
                logical_size: 2,
                content_type: meta::RESTORE_MANIFEST_CONTENT_TYPE.to_owned(),
            },
            commit_provenance: meta::RestoreCommitProvenance::V5(Box::new(
                meta::RestoreCommitProvenanceV5 {
                    source_commit: meta::RestoreSourceCommitSeal {
                        commit_id: source_commit_id,
                        content_digest_uri: format!("sha256:{}", "11".repeat(types::SHA256_BYTES)),
                        manifest_digest_uri: format!("sha256:{}", "12".repeat(types::SHA256_BYTES)),
                        tree_manifest_revision_id: types::ArtifactRevisionId::from_bytes(
                            [0x62; types::FIXED_ID_BYTES],
                        ),
                        member_count: 2,
                        member_digest: [0x70; types::SHA256_BYTES],
                        unique_revision_count: 1,
                        revision_digest: [0x73; types::SHA256_BYTES],
                        parent_digest: [0; types::SHA256_BYTES],
                        generic_index_count: 0,
                        generic_index_digest: [0; types::SHA256_BYTES],
                    },
                    destination_committed_at_unix_seconds: 1,
                    destination_binding: None,
                    closure: meta::RestoreCommitClosureProgress {
                        member_cursor: None,
                        member_count: 0,
                        member_digest: [0; types::SHA256_BYTES],
                        path_members_complete: false,
                        generic_index_cursor: None,
                        generic_index_count: 0,
                        generic_index_digest: [0; types::SHA256_BYTES],
                        generic_indexes_complete: false,
                        member_seal: None,
                        revision_ref_count: 0,
                        revision_cursor: None,
                        revision_seal_count: 0,
                        revision_digest: [0; types::SHA256_BYTES],
                        revision_seal: None,
                        parent_digest: meta::advance_commit_parent_rolling_digest(
                            [0; types::SHA256_BYTES],
                            0,
                            source_commit_id,
                        ),
                        parent_seal: None,
                        cleanup_member_count: 0,
                        cleanup_generic_index_count: 0,
                        cleanup_revision_count: 0,
                    },
                    destination_head_generation: None,
                },
            )),
            phase: types::RestorePhase::SourceSealed,
            source_cursor: Some(types::NormalizedRelativePath::new("outputs/result").unwrap()),
            source_paths_eof: true,
            source_generic_index_cursor: None,
            source_generic_index_count: 0,
            source_generic_index_rolling_digest: [0; types::SHA256_BYTES],
            source_generic_index_seal: Some([0; types::SHA256_BYTES]),
            source_generic_indexes_match_base_commit: Some(true),
            source_eof: true,
            source_member_count: 2,
            source_member_rolling_digest: [0x70; types::SHA256_BYTES],
            source_member_seal: Some([0x70; types::SHA256_BYTES]),
            source_matches_base_commit: Some(true),
            next_member_sequence: 1,
            member_rolling_digest: [0x71; types::SHA256_BYTES],
            member_seal: Some([0x71; types::SHA256_BYTES]),
            cleanup_member_cursor: 0,
            cleanup_generic_index_cursor: 0,
            result: None,
            terminal_error: None,
        };
        operation.validate().unwrap();
        operation
    }

    fn destination_building_restore_operation() -> meta::RestoreOperationRecord {
        let mut operation = bound_source_sealed_restore_operation();
        operation.initialization_digest = Some([0x67; types::SHA256_BYTES]);
        operation.phase = types::RestorePhase::DestinationBuilding;
        let meta::RestoreCommitProvenance::V5(provenance) = &mut operation.commit_provenance else {
            unreachable!();
        };
        provenance.destination_binding = Some(restore_destination_binding(Some(
            restore_destination_manifests(),
        )));
        provenance.closure.member_cursor =
            Some(types::NormalizedRelativePath::new("outputs/result").unwrap());
        provenance.closure.member_count = 3;
        provenance.closure.member_digest = [0x74; types::SHA256_BYTES];
        provenance.closure.revision_ref_count = 2;
        operation.validate().unwrap();
        operation
    }

    #[cfg(feature = "restore-crash-test-support")]
    fn pristine_destination_building_restore_operation() -> meta::RestoreOperationRecord {
        let mut operation = bound_source_sealed_restore_operation();
        operation.initialization_digest = Some([0x67; types::SHA256_BYTES]);
        operation.phase = types::RestorePhase::DestinationBuilding;
        let meta::RestoreCommitProvenance::V5(provenance) = &mut operation.commit_provenance else {
            unreachable!();
        };
        provenance.destination_binding = Some(restore_destination_binding(Some(
            restore_destination_manifests(),
        )));
        operation.validate().unwrap();
        operation
    }

    #[cfg(feature = "restore-crash-test-support")]
    #[test]
    fn restore_initialization_barrier_requires_two_actual_manifests_and_zero_closure_progress() {
        let operation = pristine_destination_building_restore_operation();
        let evidence = restore_initialization_barrier_evidence(route(1), 91, &operation).unwrap();

        assert_eq!(
            evidence.phase,
            RestoreInitializationBarrierPhase::DestinationBuilding
        );
        assert_eq!(evidence.operation_id, operation.operation_id.into());
        assert_eq!(evidence.durable_read_version, 91);
        assert_eq!(evidence.built_commit_members, 0);
        assert_eq!(evidence.sealed_revisions, 0);
        assert_eq!(
            evidence.run_manifest.expected,
            evidence.run_manifest.actual.identity
        );
        assert_eq!(
            evidence.restore_manifest.expected,
            evidence.restore_manifest.actual.identity
        );
        assert_ne!(
            evidence.run_manifest.actual.identity,
            evidence.restore_manifest.actual.identity
        );

        let mut progressed = operation.clone();
        let meta::RestoreCommitProvenance::V5(provenance) = &mut progressed.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.member_cursor =
            Some(types::NormalizedRelativePath::new("outputs/result").unwrap());
        provenance.closure.member_count = 1;
        provenance.closure.member_digest = [0x44; types::SHA256_BYTES];
        assert!(restore_initialization_barrier_evidence(route(1), 92, &progressed).is_err());

        let mut missing_actual = operation;
        let meta::RestoreCommitProvenance::V5(provenance) = &mut missing_actual.commit_provenance
        else {
            unreachable!();
        };
        provenance.destination_binding.as_mut().unwrap().manifests = None;
        assert!(restore_initialization_barrier_evidence(route(1), 93, &missing_actual).is_err());
    }

    #[cfg(feature = "restore-crash-test-support")]
    #[test]
    fn restore_initialization_barrier_requires_an_exact_post_commit_readback() {
        let committed = pristine_destination_building_restore_operation();
        let durable =
            authoritative_restore_initialization_readback(&committed, Some(committed.clone()))
                .unwrap();
        assert_eq!(durable, committed);

        let mut drifted = committed.clone();
        drifted.initialization_digest = Some([0x68; types::SHA256_BYTES]);
        drifted.validate().unwrap();
        let mismatch =
            authoritative_restore_initialization_readback(&committed, Some(drifted)).unwrap_err();
        assert!(mismatch.message.contains("post-commit readback"));

        let missing = authoritative_restore_initialization_readback(&committed, None).unwrap_err();
        assert!(missing.message.contains("durably readable"));
    }

    #[cfg(feature = "restore-crash-test-support")]
    #[test]
    fn every_concurrent_destination_building_request_stops_at_the_shared_barrier() {
        struct OneShotTestBarrier {
            state: std::sync::atomic::AtomicU8,
            arrivals: std::sync::mpsc::Sender<(&'static str, RestoreInitializationBarrierEvidence)>,
            winner_release: Arc<std::sync::Barrier>,
        }

        impl RestoreInitializationBarrier for OneShotTestBarrier {
            fn reached(
                &self,
                evidence: Result<RestoreInitializationBarrierEvidence, protocol::RpcFailure>,
            ) -> ! {
                let evidence = evidence.expect("the exact target evidence must be valid");
                match self.state.compare_exchange(
                    0,
                    1,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                ) {
                    Ok(_) => {
                        self.arrivals.send(("firing", evidence)).unwrap();
                        self.winner_release.wait();
                        panic!("simulate the winner terminating the owner");
                    }
                    Err(1) => {
                        self.arrivals.send(("parked", evidence)).unwrap();
                        loop {
                            std::thread::park();
                        }
                    }
                    Err(state) => panic!("unexpected one-shot barrier state {state}"),
                }
            }
        }

        let (store, executor) = ready_executor();
        let operation = pristine_destination_building_restore_operation();
        install_restore_operation(&store, &operation, 0xe0, None);
        let (arrival_tx, arrival_rx) = std::sync::mpsc::channel();
        let winner_release = Arc::new(std::sync::Barrier::new(2));
        let executor = executor.with_restore_initialization_barrier(
            operation.operation_id.into(),
            Arc::new(OneShotTestBarrier {
                state: std::sync::atomic::AtomicU8::new(0),
                arrivals: arrival_tx,
                winner_release: Arc::clone(&winner_release),
            }),
        );
        let request =
            protocol::WorkspaceRequest::FinalizeRestore(protocol::FinalizeRestoreRequest {
                operation_id: operation.operation_id.into(),
            });
        let first_executor = executor.clone();
        let first_request = restore_rpc(0xe1, request.clone());
        let first = std::thread::spawn(move || first_executor.execute(&first_request));
        let (first_state, first_evidence) = arrival_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("first exact target must enter Firing");
        assert_eq!(first_state, "firing");

        let second_executor = executor.clone();
        let second_request = restore_rpc(0xe2, request);
        let second = std::thread::spawn(move || second_executor.execute(&second_request));
        let (second_state, second_evidence) = arrival_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("second exact target must park after Firing");
        assert_eq!(second_state, "parked");
        assert_eq!(first_evidence.operation_id, second_evidence.operation_id);
        assert_eq!(first_evidence.built_commit_members, 0);
        assert_eq!(second_evidence.built_commit_members, 0);

        let persisted = meta::get_restore(
            &store,
            meta::RootWriteContext::current(
                &store,
                root(),
                shard(),
                types::ObjectNamespaceId::from_bytes([10; types::FIXED_ID_BYTES]),
                placement(),
                owner(1),
                request_id(0xe3),
            )
            .unwrap(),
            operation.operation_id,
        )
        .unwrap()
        .unwrap();
        let provenance = restore_commit_provenance(&persisted).unwrap();
        assert!(restore_closure_is_pristine(&provenance.closure));

        assert!(!first.is_finished());
        assert!(!second.is_finished());
        winner_release.wait();
        assert!(first.join().is_err());
        assert!(!second.is_finished());
        drop(second);
    }

    #[cfg(feature = "restore-crash-test-support")]
    #[test]
    fn invalid_exact_target_evidence_enters_the_divergent_failure_path() {
        struct InvalidEvidenceBarrier {
            arrived: std::sync::mpsc::Sender<protocol::RpcFailure>,
        }

        impl RestoreInitializationBarrier for InvalidEvidenceBarrier {
            fn reached(
                &self,
                evidence: Result<RestoreInitializationBarrierEvidence, protocol::RpcFailure>,
            ) -> ! {
                self.arrived
                    .send(evidence.expect_err("progressed closure must be rejected"))
                    .unwrap();
                panic!("simulate the fault owner exiting 87");
            }
        }

        let (store, executor) = ready_executor();
        let operation = destination_building_restore_operation();
        install_restore_operation(&store, &operation, 0xed, None);
        let (arrival_tx, arrival_rx) = std::sync::mpsc::channel();
        let executor = executor.with_restore_initialization_barrier(
            operation.operation_id.into(),
            Arc::new(InvalidEvidenceBarrier {
                arrived: arrival_tx,
            }),
        );
        let request = restore_rpc(
            0xee,
            protocol::WorkspaceRequest::FinalizeRestore(protocol::FinalizeRestoreRequest {
                operation_id: operation.operation_id.into(),
            }),
        );
        let worker = std::thread::spawn(move || executor.execute(&request));
        let failure = arrival_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .expect("invalid exact target evidence must enter the barrier failure path");
        assert!(failure
            .message
            .contains("zero destination closure progress"));
        assert!(worker.join().is_err());
    }

    #[cfg(feature = "restore-crash-test-support")]
    #[test]
    fn non_target_destination_building_resume_bypasses_the_strict_barrier() {
        struct OtherOperationBarrier;

        impl RestoreInitializationBarrier for OtherOperationBarrier {
            fn reached(
                &self,
                _evidence: Result<RestoreInitializationBarrierEvidence, protocol::RpcFailure>,
            ) -> ! {
                panic!("a non-target restore must not reach the crash barrier");
            }
        }

        let (store, executor) = ready_executor();
        let source_workbench = protocol::WorkbenchName::new("restore-source-live").unwrap();
        let source_workspace_incarnation_id =
            protocol::WorkspaceIdentity([0x91; types::FIXED_ID_BYTES]);
        executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x80; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::CreateWorkspace(
                    protocol::CreateWorkspaceRequest {
                        workbench: source_workbench.clone(),
                        workspace_incarnation_id: source_workspace_incarnation_id,
                    },
                ),
            })
            .unwrap();

        let commit_operation_id = protocol::OperationIdentity([0x92; types::FIXED_ID_BYTES]);
        let source_commit_id = protocol::CommitIdentity([0x93; types::SHA256_BYTES]);
        let source_run_manifest_revision =
            protocol::ArtifactRevisionIdentity([0x94; types::FIXED_ID_BYTES]);
        let source_run_manifest_body =
            protocol::sha256_digest_uri(protocol::Digest(Sha256::digest([0x7b]).into()));
        let source_content_digest =
            protocol::DigestUri::new(format!("sha256:{}", "aa".repeat(types::SHA256_BYTES)))
                .unwrap();
        let commit_operation = protocol::WorkspaceRequest::Commit(protocol::CommitRequest {
            operation_id: commit_operation_id,
            workbench: source_workbench.clone(),
            workspace_incarnation_id: source_workspace_incarnation_id,
            commit_id: source_commit_id,
            content_digest: source_content_digest.clone(),
            manifest_digest: source_run_manifest_body,
            projection_input_digest: protocol::Digest([0x96; types::SHA256_BYTES]),
            tree_manifest_revision_id: source_run_manifest_revision,
            replace: false,
            run_manifest_condition: protocol::PublishCondition::CreateOnly,
            expected_head_generation: None,
            parents: Vec::new(),
            producer: None,
            lineage_projection: Vec::new(),
        });
        let first_commit = executor
            .execute(&restore_rpc(0x81, commit_operation.clone()))
            .unwrap();
        let protocol::WorkspaceResult::Operation(first_commit) = first_commit.result else {
            panic!("initial commit request returned the wrong result variant");
        };
        assert_eq!(first_commit.state, protocol::OperationState::Running);
        publish_one_byte_artifact(
            &executor,
            0x82,
            protocol::OperationIdentity([0x95; types::FIXED_ID_BYTES]),
            source_run_manifest_revision,
            source_workbench.as_str(),
            RUN_MANIFEST_PATH,
            protocol::PublicationAuthority::CommitStaging {
                commit_operation_id,
            },
            0x7b,
        );
        let completed_commit = executor
            .execute(&restore_rpc(0x87, commit_operation))
            .unwrap();
        let protocol::WorkspaceResult::Operation(completed_commit) = completed_commit.result else {
            panic!("completed commit returned the wrong result variant");
        };
        assert_eq!(completed_commit.state, protocol::OperationState::Succeeded);

        let snapshot_id = 7;
        executor
            .execute(&restore_rpc(
                0x88,
                protocol::WorkspaceRequest::MintSnapshot(protocol::MintSnapshotRequest {
                    workbench: source_workbench.clone(),
                    workspace_incarnation_id: source_workspace_incarnation_id,
                    snapshot_id,
                    lease_deadline_ms: 1_000_000,
                    alias: None,
                    annotation: Vec::new(),
                }),
            ))
            .unwrap();

        let destination_workbench =
            protocol::WorkbenchName::new("restore-destination-live").unwrap();
        let destination_workspace_incarnation_id =
            protocol::WorkspaceIdentity([0x97; types::FIXED_ID_BYTES]);
        let restore_operation_id: protocol::OperationIdentity = meta::restore_operation_id(
            root(),
            &types::WorkbenchId::new(source_workbench.as_str()).unwrap(),
            source_workspace_incarnation_id.into(),
            meta::RestoreSourceSelector::Snapshot(types::SnapshotId::new(snapshot_id)),
            &types::WorkbenchId::new(destination_workbench.as_str()).unwrap(),
            destination_workspace_incarnation_id.into(),
        )
        .unwrap()
        .into();
        let destination_restore_manifest_identity = protocol::RestoreManifestIdentity {
            publication_operation_id: protocol::OperationIdentity([0x98; types::FIXED_ID_BYTES]),
            artifact_revision_id: protocol::ArtifactRevisionIdentity([0x99; types::FIXED_ID_BYTES]),
        };
        let restore_manifest_body =
            protocol::sha256_digest_uri(protocol::Digest(Sha256::digest([0x7d]).into()));
        executor
            .execute(&restore_rpc(
                0x89,
                protocol::WorkspaceRequest::PrepareRestore(protocol::PrepareRestoreRequest {
                    operation_id: restore_operation_id,
                    source_workbench: source_workbench.clone(),
                    source_workspace_incarnation_id,
                    source: protocol::RestoreSource::Snapshot(protocol::SnapshotSelector::Id(
                        snapshot_id,
                    )),
                    destination_workbench: destination_workbench.clone(),
                    destination_workspace_incarnation_id,
                    destination_restore_manifest_identity,
                    restore_manifest: protocol::RestoreManifestDescriptor {
                        body_digest: restore_manifest_body,
                        logical_size: 1,
                        content_type: protocol::ContentType::new("application/json").unwrap(),
                    },
                }),
            ))
            .unwrap();

        let destination_run_manifest_identity = protocol::RestoreManifestIdentity {
            publication_operation_id: protocol::OperationIdentity([0x9a; types::FIXED_ID_BYTES]),
            artifact_revision_id: protocol::ArtifactRevisionIdentity([0x9b; types::FIXED_ID_BYTES]),
        };
        let bind_request = protocol::BindRestoreDestinationRequest {
            operation_id: restore_operation_id,
            destination_commit_id: protocol::CommitIdentity([0x9c; types::SHA256_BYTES]),
            effective_content_digest: source_content_digest,
            destination_run_manifest_projection_input_digest: protocol::Digest(
                [0x9d; types::SHA256_BYTES],
            ),
            destination_run_manifest_identity,
            destination_restore_manifest_identity,
        };
        executor
            .execute(&restore_rpc(
                0x8a,
                protocol::WorkspaceRequest::BindRestoreDestination(bind_request.clone()),
            ))
            .unwrap();
        publish_one_byte_artifact(
            &executor,
            0xa0,
            destination_run_manifest_identity.publication_operation_id,
            destination_run_manifest_identity.artifact_revision_id,
            destination_workbench.as_str(),
            RUN_MANIFEST_PATH,
            protocol::PublicationAuthority::RestoreStaging {
                restore_operation_id,
            },
            0x7c,
        );
        publish_one_byte_artifact(
            &executor,
            0xa5,
            destination_restore_manifest_identity.publication_operation_id,
            destination_restore_manifest_identity.artifact_revision_id,
            destination_workbench.as_str(),
            meta::RESTORE_MANIFEST_PATH,
            protocol::PublicationAuthority::RestoreStaging {
                restore_operation_id,
            },
            0x7d,
        );
        let initialized = meta::apply_restore_initialization(
            &store,
            meta::RootWriteContext::current(
                &store,
                root(),
                shard(),
                types::ObjectNamespaceId::from_bytes([10; types::FIXED_ID_BYTES]),
                placement(),
                owner(1),
                request_id(0xaa),
            )
            .unwrap(),
            meta::RestoreOperationRequest {
                operation_id: restore_operation_id.into(),
            },
        )
        .unwrap();
        assert_eq!(
            initialized.operation.phase,
            types::RestorePhase::DestinationBuilding
        );
        assert!(restore_closure_is_pristine(
            &restore_commit_provenance(&initialized.operation)
                .unwrap()
                .closure
        ));

        let replayed_run_manifest = publish_one_byte_artifact(
            &executor,
            0xcb,
            destination_run_manifest_identity.publication_operation_id,
            destination_run_manifest_identity.artifact_revision_id,
            destination_workbench.as_str(),
            RUN_MANIFEST_PATH,
            protocol::PublicationAuthority::RestoreStaging {
                restore_operation_id,
            },
            0x7c,
        );
        assert_eq!(replayed_run_manifest.logical_size, 1);

        let bind_replay = executor
            .execute(&restore_rpc(
                0xd1,
                protocol::WorkspaceRequest::BindRestoreDestination(bind_request.clone()),
            ))
            .expect("exact bind replay must resume DestinationBuilding");
        assert!(bind_replay.replayed);
        let protocol::WorkspaceResult::RestorePrepared(replayed_preparation) = bind_replay.result
        else {
            panic!("exact bind replay returned the wrong result variant");
        };
        assert_eq!(
            replayed_preparation.destination_binding,
            sealed_restore_preparation(&initialized.operation)
                .unwrap()
                .destination_binding
        );

        let mut mismatched_bind = bind_request;
        mismatched_bind.destination_commit_id =
            protocol::CommitIdentity([0xfe; types::SHA256_BYTES]);
        let mismatch = executor
            .execute(&restore_rpc(
                0xd2,
                protocol::WorkspaceRequest::BindRestoreDestination(mismatched_bind),
            ))
            .expect_err("mismatched bind replay must fail closed");
        assert_eq!(mismatch.code, protocol::ErrorCode::RequestReplayMismatch);

        let executor = executor.with_restore_initialization_barrier(
            protocol::OperationIdentity([0xff; types::FIXED_ID_BYTES]),
            Arc::new(OtherOperationBarrier),
        );
        let finalize = restore_rpc(
            0xab,
            protocol::WorkspaceRequest::FinalizeRestore(protocol::FinalizeRestoreRequest {
                operation_id: restore_operation_id,
            }),
        );
        let first = executor.execute(&finalize).unwrap();
        let protocol::WorkspaceResult::Restored(first_result) = first.result else {
            panic!("non-target finalize returned the wrong result variant");
        };
        assert!(!first.replayed);
        let replay = executor.execute(&finalize).unwrap();
        let protocol::WorkspaceResult::Restored(replay_result) = replay.result else {
            panic!("non-target replay returned the wrong result variant");
        };
        assert!(replay.replayed);
        assert_eq!(replay_result, first_result);

        let persisted = meta::get_restore(
            &store,
            meta::RootWriteContext::current(
                &store,
                root(),
                shard(),
                types::ObjectNamespaceId::from_bytes([10; types::FIXED_ID_BYTES]),
                placement(),
                owner(1),
                request_id(0xac),
            )
            .unwrap(),
            restore_operation_id.into(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(persisted.phase, types::RestorePhase::Complete);
        assert_eq!(
            persisted
                .destination_commit_receipt()
                .unwrap()
                .destination_commit_id,
            types::CommitId::from_bytes([0x9c; types::SHA256_BYTES])
        );
    }

    fn bound_source_sealed_restore_operation() -> meta::RestoreOperationRecord {
        let mut operation = source_sealed_restore_operation();
        let meta::RestoreCommitProvenance::V5(provenance) = &mut operation.commit_provenance else {
            unreachable!();
        };
        provenance.destination_binding = Some(restore_destination_binding(None));
        operation.validate().unwrap();
        operation
    }

    fn destination_sealing_restore_operation() -> meta::RestoreOperationRecord {
        let mut operation = destination_building_restore_operation();
        operation.phase = types::RestorePhase::DestinationSealing;
        let meta::RestoreCommitProvenance::V5(provenance) = &mut operation.commit_provenance else {
            unreachable!();
        };
        provenance.closure.path_members_complete = true;
        provenance.closure.generic_indexes_complete = true;
        provenance.closure.member_seal = Some([0x74; types::SHA256_BYTES]);
        provenance.closure.revision_cursor = Some(types::ArtifactRevisionId::from_bytes(
            [0x65; types::FIXED_ID_BYTES],
        ));
        provenance.closure.revision_seal_count = 2;
        provenance.closure.revision_digest = [0x75; types::SHA256_BYTES];
        operation.validate().unwrap();
        operation
    }

    fn ready_restore_operation() -> meta::RestoreOperationRecord {
        let mut operation = destination_sealing_restore_operation();
        operation.phase = types::RestorePhase::Ready;
        let meta::RestoreCommitProvenance::V5(provenance) = &mut operation.commit_provenance else {
            unreachable!();
        };
        provenance.closure.revision_seal = Some([0x75; types::SHA256_BYTES]);
        provenance.closure.parent_seal = Some(provenance.closure.parent_digest);
        operation.validate().unwrap();
        operation
    }

    fn copying_restore_operation(
        source_member_count: u64,
        materialized_member_count: u64,
        source_eof: bool,
        digest_fill: u8,
    ) -> meta::RestoreOperationRecord {
        let mut operation = source_sealed_restore_operation();
        operation.phase = types::RestorePhase::Copying;
        operation.source_eof = source_eof;
        operation.source_paths_eof = source_eof;
        operation.source_generic_index_seal = source_eof.then_some([0; types::SHA256_BYTES]);
        operation.source_generic_indexes_match_base_commit = source_eof.then_some(true);
        operation.source_member_count = source_member_count;
        operation.source_cursor = (source_member_count > 0).then(|| {
            types::NormalizedRelativePath::new(format!("input/{source_member_count:04}")).unwrap()
        });
        operation.source_member_rolling_digest = if source_member_count == 0 {
            [0; types::SHA256_BYTES]
        } else {
            [digest_fill; types::SHA256_BYTES]
        };
        operation.source_member_seal = None;
        operation.source_matches_base_commit = None;
        operation.next_member_sequence = materialized_member_count;
        operation.member_rolling_digest = if materialized_member_count == 0 {
            [0; types::SHA256_BYTES]
        } else {
            [0x71; types::SHA256_BYTES]
        };
        operation.member_seal = None;
        operation.validate().unwrap();
        operation
    }

    fn complete_restore_operation() -> meta::RestoreOperationRecord {
        let ready = ready_restore_operation();
        ready
            .apply(
                types::RestorePhase::Ready,
                meta::RestoreTransition::Complete {
                    result: meta::RestoreResult {
                        destination_workspace_incarnation_id: ready
                            .destination_workspace_incarnation_id,
                        destination_workspace_revision: types::WorkspaceRevision::new(1),
                        member_count: 1,
                        member_digest: [0x71; types::SHA256_BYTES],
                    },
                    destination_head_generation: types::Generation::new(1).unwrap(),
                },
            )
            .unwrap()
    }

    fn install_restore_operation(
        store: &meta::MetaShard,
        operation: &meta::RestoreOperationRecord,
        request_fill: u8,
        later_live_head: Option<meta::WorkbenchCommitHeadRecord>,
    ) {
        let operation_key = meta::operation_key(
            root(),
            types::OperationKind::Restore,
            operation.operation_id,
        );
        let mut predicates = vec![meta::CommandPredicate::Value {
            family: meta::MetadataFamily::Operation,
            key: operation_key.clone(),
            expected: None,
        }];
        let mut mutations = vec![meta::CommandMutation::Put {
            family: meta::MetadataFamily::Operation,
            key: operation_key,
            value: operation.encode().unwrap(),
        }];
        if let Some(head) = later_live_head {
            let head_key = meta::workbench_commit_head_key(
                root(),
                operation.destination_workspace_incarnation_id,
            );
            predicates.push(meta::CommandPredicate::Value {
                family: meta::MetadataFamily::WorkbenchCommitHead,
                key: head_key.clone(),
                expected: None,
            });
            mutations.push(meta::CommandMutation::Put {
                family: meta::MetadataFamily::WorkbenchCommitHead,
                key: head_key,
                value: head.encode(),
            });
        }
        store
            .execute(
                &meta::MetadataCommand {
                    schema_id: meta::SCHEMA_ID.to_owned(),
                    root_id: root(),
                    logical_shard_id: shard(),
                    object_namespace_id: Some(types::ObjectNamespaceId::from_bytes(
                        [10; types::FIXED_ID_BYTES],
                    )),
                    placement_generation: placement(),
                    owner_epoch: owner(1),
                    request_id: request_id(request_fill),
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

    fn restore_rpc(
        request_fill: u8,
        operation: protocol::WorkspaceRequest,
    ) -> protocol::WorkspaceRpcRequest {
        protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
            operation,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_one_byte_artifact(
        executor: &MetadataWorkspaceRequestExecutor,
        first_request_fill: u8,
        operation_id: protocol::OperationIdentity,
        artifact_revision_id: protocol::ArtifactRevisionIdentity,
        workbench: &str,
        path: &str,
        authority: protocol::PublicationAuthority,
        byte: u8,
    ) -> protocol::ArtifactDescriptor {
        let revision_id: types::ArtifactRevisionId = artifact_revision_id.into();
        let object_key = meta::object_block_key(shard(), root(), revision_id, 0);
        let body_digest =
            protocol::sha256_digest_uri(protocol::Digest(Sha256::digest([byte]).into()));
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
            physical_owner_revision_id: artifact_revision_id,
            object_identity: protocol::ObjectIdentity::new(object_key).unwrap(),
            object_offset: 0,
            length: 1,
            digest: body_digest.clone(),
            append_segment: None,
        }];
        let seals = protocol::seal_artifact_publish_plan(
            artifact_revision_id,
            &staged_objects,
            &manifest_rows,
        )
        .unwrap();
        let artifact = protocol::ArtifactDescriptor {
            logical_size: 1,
            body_digest: body_digest.clone(),
            manifest_digest: protocol::sha256_digest_uri(seals.manifest_seal),
            content_type: protocol::ContentType::new("application/json").unwrap(),
            producer: None,
            manifest_identity: None,
            index_fields: Vec::new(),
        };
        let target = protocol::WorkspacePath {
            workbench: protocol::WorkbenchName::new(workbench).unwrap(),
            path: protocol::RelativePath::new(path).unwrap(),
        };
        let operation_status = |result: ExecutedRequest| {
            let protocol::WorkspaceResult::Operation(status) = result.result else {
                panic!("artifact publication returned the wrong result variant");
            };
            status
        };
        let status = operation_status(
            executor
                .execute(&restore_rpc(
                    first_request_fill,
                    protocol::WorkspaceRequest::BeginArtifactPublish(
                        protocol::BeginArtifactPublishRequest {
                            operation_id,
                            artifact_revision_id,
                            target: target.clone(),
                            authority,
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
        if status.state == protocol::OperationState::Succeeded {
            let Some(protocol::OperationResult::ArtifactPublish(result)) = status.result else {
                panic!("terminal artifact replay omitted its durable result");
            };
            assert_eq!(result.operation_id, operation_id);
            assert_eq!(result.target, target);
            assert_eq!(result.artifact_revision_id, artifact_revision_id);
            assert_eq!(result.logical_size, artifact.logical_size);
            assert_eq!(result.body_digest, artifact.body_digest);
            return artifact;
        }
        assert_eq!(status.state, protocol::OperationState::Running);
        let status = operation_status(
            executor
                .execute(&restore_rpc(
                    first_request_fill.wrapping_add(1),
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
                .execute(&restore_rpc(
                    first_request_fill.wrapping_add(2),
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
                .execute(&restore_rpc(
                    first_request_fill.wrapping_add(3),
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
        let completed = executor
            .execute(&restore_rpc(
                first_request_fill.wrapping_add(4),
                protocol::WorkspaceRequest::CompleteArtifactPublish(
                    protocol::CompleteArtifactPublishRequest {
                        token: status.token,
                        artifact: artifact.clone(),
                    },
                ),
            ))
            .unwrap();
        let protocol::WorkspaceResult::Published(published) = completed.result else {
            panic!("artifact finalization returned the wrong result variant");
        };
        assert_eq!(published.target, target);
        artifact
    }

    #[test]
    fn sealed_restore_preparation_replays_through_destination_construction() {
        let pre_bind = source_sealed_restore_operation();
        let source_sealed = bound_source_sealed_restore_operation();
        let destination_building = destination_building_restore_operation();
        let destination_sealing = destination_sealing_restore_operation();
        let ready = ready_restore_operation();
        let complete = ready
            .apply(
                types::RestorePhase::Ready,
                meta::RestoreTransition::Complete {
                    result: meta::RestoreResult {
                        destination_workspace_incarnation_id: ready
                            .destination_workspace_incarnation_id,
                        destination_workspace_revision: types::WorkspaceRevision::new(1),
                        member_count: 1,
                        member_digest: [0x71; types::SHA256_BYTES],
                    },
                    destination_head_generation: types::Generation::new(1).unwrap(),
                },
            )
            .unwrap();

        let expected = sealed_restore_preparation(&source_sealed).unwrap();
        for operation in [
            &destination_building,
            &destination_sealing,
            &ready,
            &complete,
        ] {
            let projected = sealed_restore_preparation(operation).unwrap();
            assert!(projected
                .destination_binding
                .as_ref()
                .and_then(|binding| binding.destination_manifests.as_ref())
                .is_some());
            let mut without_actual_manifests = projected;
            without_actual_manifests
                .destination_binding
                .as_mut()
                .unwrap()
                .destination_manifests = None;
            assert_eq!(without_actual_manifests, expected);
        }
        assert_eq!(expected.operation_id.0, [0x41; types::FIXED_ID_BYTES]);
        assert_eq!(
            expected.destination_workbench.as_str(),
            "restore-destination"
        );
        assert_eq!(expected.source_member_count, 2);
        assert_eq!(
            expected.source_member_digest,
            protocol::Digest([0x70; types::SHA256_BYTES])
        );
        assert_eq!(expected.materialized_member_count, 1);
        assert_eq!(
            expected.materialized_member_digest,
            protocol::Digest([0x71; types::SHA256_BYTES])
        );
        assert!(expected.source_matches_base_commit);
        assert!(expected.destination_binding.is_some());

        let pre_bind = sealed_restore_preparation(&pre_bind).unwrap();
        assert!(pre_bind.destination_binding.is_none());
        assert_eq!(pre_bind.source_member_digest, expected.source_member_digest);
        assert_eq!(
            pre_bind.materialized_member_digest,
            expected.materialized_member_digest
        );
        assert_eq!(
            pre_bind.destination_committed_at_unix_seconds,
            expected.destination_committed_at_unix_seconds
        );
    }

    #[test]
    fn destination_restore_construction_is_running_and_nonterminal() {
        for operation in [
            destination_building_restore_operation(),
            destination_sealing_restore_operation(),
        ] {
            assert!(restore_terminal_failure(&operation).is_none());
            let status = restore_operation_status(&operation).unwrap();
            assert_eq!(status.state, protocol::OperationState::Running);
            assert!(status.result.is_none());
            assert!(status.failure.is_none());
            assert_eq!(status.progress.completed_rows, 1);
            assert_eq!(status.progress.total_rows, Some(1));
        }
    }

    #[test]
    fn whole_rpc_restore_replay_never_regresses_copy_build_or_seal_progress() {
        let copy_first = copying_restore_operation(1, 1, false, 0x81);
        let copy_later = copying_restore_operation(2, 1, false, 0x82);
        assert!(!restore_step_advances(&copy_later, &copy_first, true, "copy").unwrap());
        assert!(restore_step_advances(&copy_first, &copy_later, true, "copy").unwrap());
        assert!(restore_step_advances(&copy_later, &copy_first, false, "copy").is_err());

        let build_first = destination_building_restore_operation();
        let mut build_later = build_first.clone();
        let meta::RestoreCommitProvenance::V5(provenance) = &mut build_later.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.member_cursor =
            Some(types::NormalizedRelativePath::new("zzzz/final").unwrap());
        provenance.closure.member_count = 4;
        provenance.closure.member_digest = [0x76; types::SHA256_BYTES];
        provenance.closure.revision_ref_count = 3;
        build_later.validate().unwrap();
        assert!(!restore_step_advances(&build_later, &build_first, true, "build").unwrap());

        let sealing = destination_sealing_restore_operation();
        assert!(restore_step_advances(&build_first, &sealing, true, "build boundary").unwrap());
        assert!(!restore_step_advances(&sealing, &build_first, true, "build boundary").unwrap());

        let mut seal_first = sealing.clone();
        let meta::RestoreCommitProvenance::V5(provenance) = &mut seal_first.commit_provenance
        else {
            unreachable!();
        };
        provenance.closure.revision_cursor = Some(types::ArtifactRevisionId::from_bytes(
            [0x64; types::FIXED_ID_BYTES],
        ));
        provenance.closure.revision_seal_count = 1;
        provenance.closure.revision_digest = [0x76; types::SHA256_BYTES];
        seal_first.validate().unwrap();
        assert!(!restore_step_advances(&sealing, &seal_first, true, "seal").unwrap());

        let ready = ready_restore_operation();
        assert!(restore_step_advances(&sealing, &ready, true, "seal boundary").unwrap());
        assert!(!restore_step_advances(&ready, &sealing, true, "seal boundary").unwrap());

        let mut incompatible = copy_first.clone();
        let meta::RestoreCommitProvenance::V5(provenance) = &mut incompatible.commit_provenance
        else {
            unreachable!();
        };
        provenance.source_commit.manifest_digest_uri =
            format!("sha256:{}", "99".repeat(types::SHA256_BYTES));
        incompatible.validate().unwrap();
        assert!(restore_step_advances(&copy_first, &incompatible, true, "copy").is_err());
    }

    #[test]
    fn restored_response_uses_the_terminal_commit_receipt_only() {
        let complete = complete_restore_operation();
        let response = restored_response(&complete, Some(77), true).unwrap();
        let protocol::WorkspaceResult::Restored(restored) = response.result else {
            panic!("expected restored response");
        };
        let receipt = complete.destination_commit_receipt().unwrap();
        assert_eq!(
            restored.destination.commit_head,
            Some(receipt.destination_commit_id.into())
        );
        assert_eq!(
            restored.destination.commit_head_generation,
            Some(receipt.destination_head_generation.get())
        );
        assert_ne!(
            restored.destination.commit_head,
            Some(types::CommitId::from_bytes([0x99; types::SHA256_BYTES]).into())
        );
        assert_eq!(restored.member_count, 1);
        assert_eq!(restored.metadata_rows_copied, 1);
        assert_eq!(response.commit_version, Some(77));
        assert!(response.replayed);
    }

    #[test]
    fn restore_dispatcher_rejects_missing_and_wrong_phase_held_reads() {
        let (store, executor) = ready_executor();
        let missing = executor
            .execute(&restore_rpc(
                0xd0,
                protocol::WorkspaceRequest::ReadRestoreSourceRunManifest(
                    protocol::ReadRestoreSourceRunManifestRequest {
                        operation_id: protocol::OperationIdentity([0xd1; types::FIXED_ID_BYTES]),
                        range: None,
                        plan_page: None,
                    },
                ),
            ))
            .unwrap_err();
        assert_eq!(missing.code, protocol::ErrorCode::NotFound);

        let copying = copying_restore_operation(1, 1, false, 0x81);
        install_restore_operation(&store, &copying, 0xd2, None);
        let wrong_phase = executor
            .execute(&restore_rpc(
                0xd3,
                protocol::WorkspaceRequest::ReadRestoreSourceRunManifest(
                    protocol::ReadRestoreSourceRunManifestRequest {
                        operation_id: copying.operation_id.into(),
                        range: None,
                        plan_page: None,
                    },
                ),
            ))
            .unwrap_err();
        assert_eq!(wrong_phase.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(
            wrong_phase.conflict,
            Some(protocol::ConflictKind::OperationState)
        );
    }

    #[test]
    fn finalize_dispatcher_replays_receipt_after_live_head_changes_and_rejects_abort() {
        let (store, executor) = ready_executor();
        let complete = complete_restore_operation();
        let later_head = meta::WorkbenchCommitHeadRecord {
            commit_id: types::CommitId::from_bytes([0x99; types::SHA256_BYTES]),
            head_generation: types::Generation::new(7).unwrap(),
        };
        install_restore_operation(&store, &complete, 0xd4, Some(later_head));
        let finalize = restore_rpc(
            0xd5,
            protocol::WorkspaceRequest::FinalizeRestore(protocol::FinalizeRestoreRequest {
                operation_id: complete.operation_id.into(),
            }),
        );
        for _ in 0..2 {
            let response = executor.execute(&finalize).unwrap();
            assert!(response.replayed);
            let protocol::WorkspaceResult::Restored(restored) = response.result else {
                panic!("finalize returned the wrong result variant");
            };
            let receipt = complete.destination_commit_receipt().unwrap();
            assert_eq!(
                restored.destination.commit_head,
                Some(receipt.destination_commit_id.into())
            );
            assert_eq!(
                restored.destination.commit_head_generation,
                Some(receipt.destination_head_generation.get())
            );
        }

        let (store, executor) = ready_executor();
        let ready = ready_restore_operation();
        let aborting = ready
            .apply(
                types::RestorePhase::Ready,
                meta::RestoreTransition::BeginAbort {
                    terminal_error: meta::RestoreTerminalError {
                        kind: meta::RestoreTerminalErrorKind::AbortedByCaller,
                        message: "restore was aborted concurrently".to_owned(),
                        evidence_digest: None,
                    },
                },
            )
            .unwrap();
        install_restore_operation(&store, &aborting, 0xd6, None);
        let failure = executor
            .execute(&restore_rpc(
                0xd7,
                protocol::WorkspaceRequest::FinalizeRestore(protocol::FinalizeRestoreRequest {
                    operation_id: aborting.operation_id.into(),
                }),
            ))
            .unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::OperationFailed);
        assert_eq!(failure.message, "restore was aborted concurrently");
    }

    #[test]
    fn restore_capacity_failure_is_stable_while_cleanup_advances() {
        let ready = ready_restore_operation();
        let terminal = meta::RestoreTerminalError {
            kind: meta::RestoreTerminalErrorKind::InvariantViolation,
            message: "serving transaction capacity cannot admit one restore member".to_owned(),
            evidence_digest: None,
        };
        let aborting = ready
            .apply(
                types::RestorePhase::Ready,
                meta::RestoreTransition::BeginAbort {
                    terminal_error: terminal.clone(),
                },
            )
            .unwrap();
        let mut cleaning = aborting
            .apply(
                types::RestorePhase::Aborting,
                meta::RestoreTransition::BeginCleaning,
            )
            .unwrap();
        cleaning.cleanup_member_cursor = cleaning.next_member_sequence;
        let meta::RestoreCommitProvenance::V5(provenance) = &mut cleaning.commit_provenance else {
            unreachable!();
        };
        provenance.closure.cleanup_member_count = provenance.closure.member_count;
        provenance.closure.cleanup_revision_count = provenance.closure.revision_ref_count;
        cleaning.validate().unwrap();
        let cleaned = cleaning
            .apply(
                types::RestorePhase::Cleaning,
                meta::RestoreTransition::FinishCleanup,
            )
            .unwrap();

        for operation in [&aborting, &cleaning, &cleaned] {
            let failure = restore_terminal_failure(operation).unwrap();
            assert_eq!(failure.code, protocol::ErrorCode::OperationFailed);
            assert_eq!(failure.message, terminal.message);
            assert!(!failure.retryable);
        }

        let quarantined = cleaning
            .apply(
                types::RestorePhase::Cleaning,
                meta::RestoreTransition::Quarantine {
                    terminal_error: meta::RestoreTerminalError {
                        kind: meta::RestoreTerminalErrorKind::CleanupFailed,
                        message: "restore cleanup requires operator repair".to_owned(),
                        evidence_digest: None,
                    },
                },
            )
            .unwrap();
        let failure = restore_terminal_failure(&quarantined).unwrap();
        assert_eq!(failure.code, protocol::ErrorCode::Quarantined);
        assert!(!failure.retryable);
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

        let cleanup_pending = restore_failure(meta::RestoreError::PublicationCleanupPending {
            operation_id: types::OperationId::from_bytes([0x91; types::FIXED_ID_BYTES]),
            phase: types::PublishPhase::Cleaning,
        });
        assert_eq!(cleanup_pending.code, protocol::ErrorCode::Conflict);
        assert_eq!(
            cleanup_pending.conflict,
            Some(protocol::ConflictKind::OperationState)
        );
        assert!(cleanup_pending.retryable);
    }

    #[test]
    fn legacy_snapshot_restore_requires_new_commit_provenance() {
        let failure = restore_failure(meta::RestoreError::SnapshotCommitProvenanceMissing {
            snapshot_id: types::SnapshotId::new(9_876_543),
        });

        assert_eq!(failure.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(
            failure.conflict,
            Some(protocol::ConflictKind::OperationState)
        );
        assert!(!failure.retryable);
        assert_eq!(
            failure.message,
            "legacy snapshot has no sealed commit provenance; commit the source workbench and mint a new snapshot"
        );
        assert!(!failure.message.contains("9876543"));
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

        replace_visible_workspace_marker(
            &store,
            "commit-time-test",
            original_incarnation,
            types::WorkspaceRevision::ZERO,
            replacement_incarnation,
            types::WorkspaceRevision::ZERO,
            249,
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
    fn unstable_metadata_read_maps_to_a_retryable_read_version_conflict() {
        let failure = meta_failure(meta::MetaError::ReadStabilityExhausted { attempts: 4 });
        assert_eq!(failure.code, protocol::ErrorCode::Conflict);
        assert!(failure.retryable);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::ReadVersion));
        assert!(internal_metadata_conflict(&failure));
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
                target: target.clone(),
                view: protocol::WorkspaceReadView::Live,
                expected_read_version: None,
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
                workspace_continuation_fence: None,
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

        let stale_get = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([35; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::GetPath(protocol::GetPathRequest {
                target: target.clone(),
                view: protocol::WorkspaceReadView::Live,
                expected_read_version: Some(paths.read_version + 1),
                range: None,
                plan_page: None,
                if_none_match: None,
            }),
        };
        let failure = executor.execute(&stale_get).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::ReadVersion));

        let stale_list = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([34; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                workbench: protocol::WorkbenchName::new("read-test").unwrap(),
                prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                recursive: true,
                view: protocol::WorkspaceReadView::Live,
                expected_read_version: Some(paths.read_version + 1),
                workspace_continuation_fence: None,
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
                        workspace_continuation_fence: None,
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
                    workspace_continuation_fence: None,
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
                    workspace_continuation_fence: None,
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
    fn live_list_workspace_fence_ignores_unrelated_writes_but_rejects_target_changes() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([0x35; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(0x35, "fenced-list", 0x35, 1))
            .unwrap();
        put_path_projection_rows(&store, incarnation, &["outputs/a", "outputs/b"]);
        let fence = protocol::WorkspaceContinuationFence {
            workspace_incarnation_id: protocol::WorkspaceIdentity([0x35; types::FIXED_ID_BYTES]),
            workspace_revision: 0,
        };
        let list = |request_fill: u8,
                    cursor: Option<Vec<u8>>,
                    fence: protocol::WorkspaceContinuationFence| {
            executor.execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::ListPaths(protocol::ListPathsRequest {
                    workbench: protocol::WorkbenchName::new("fenced-list").unwrap(),
                    prefix: Some(protocol::RelativePath::new("outputs").unwrap()),
                    recursive: true,
                    view: protocol::WorkspaceReadView::Live,
                    expected_read_version: None,
                    workspace_continuation_fence: Some(fence),
                    page: protocol::PageRequest { cursor, limit: 1 },
                }),
            })
        };

        let first = list(0x36, None, fence.clone()).unwrap();
        let protocol::WorkspaceResult::Paths(first) = first.result else {
            panic!("list_paths returned the wrong result variant");
        };
        let first_cursor = first.next_cursor.clone();
        executor
            .execute(&create_request(0x37, "unrelated", 0x37, 1))
            .unwrap();
        let second = list(0x38, first_cursor.clone(), fence.clone()).unwrap();
        let protocol::WorkspaceResult::Paths(second) = second.result else {
            panic!("list_paths returned the wrong result variant");
        };
        assert_eq!(second.entries[0].path().path.as_str(), "outputs/b");
        assert!(second.read_version > first.read_version);

        replace_visible_workspace_marker(
            &store,
            "fenced-list",
            incarnation,
            types::WorkspaceRevision::ZERO,
            incarnation,
            types::WorkspaceRevision::new(1),
            250,
        );
        let stale_revision = list(0x39, first_cursor, fence.clone()).unwrap_err();
        assert_eq!(stale_revision.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(
            stale_revision.conflict,
            Some(protocol::ConflictKind::Workspace)
        );

        let replacement = types::WorkspaceIncarnationId::from_bytes([0x45; types::FIXED_ID_BYTES]);
        replace_visible_workspace_marker(
            &store,
            "fenced-list",
            incarnation,
            types::WorkspaceRevision::new(1),
            replacement,
            types::WorkspaceRevision::ZERO,
            251,
        );
        let rebound = list(0x3a, None, fence).unwrap_err();
        assert_eq!(rebound.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(rebound.conflict, Some(protocol::ConflictKind::Workspace));
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
                        workspace_continuation_fence: None,
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
                        workspace_continuation_fence: None,
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
            path: protocol::RelativePath::new("outputs/dir/result.bin").unwrap(),
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
                profile: protocol::QueryProfile::ArtifactV1,
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
        let protocol::SearchRow::Artifact(hit) = &result.hits[0] else {
            panic!("ArtifactV1 search returned a generic namespace row");
        };
        assert_eq!(hit.projection, index_fields);
        assert_eq!(hit.metadata.descriptor.index_fields, index_fields);
        assert_eq!(hit.metadata.path.path.as_str(), "outputs/dir/result.bin");
        assert!(result.next_cursor.is_none());
        assert!(result.read_version > 0);

        let generic_search = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x5a; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                profile: generic_query_profile(),
                scope: protocol::QueryScope::Root { path_prefix: None },
                predicates: Vec::new(),
                projection: vec![
                    "path".to_owned(),
                    "name".to_owned(),
                    "kind".to_owned(),
                    "size_bytes".to_owned(),
                    "body.content_type".to_owned(),
                    "body.producer".to_owned(),
                    "body.manifest_id".to_owned(),
                ],
                sort: vec![protocol::SortField {
                    field_id: "path".to_owned(),
                    direction: protocol::SortDirection::Ascending,
                }],
                facets: vec!["kind".to_owned(), "body.producer".to_owned()],
                page: protocol::PageRequest {
                    cursor: None,
                    limit: 100,
                },
            }),
        };
        let generic = executor.execute(&generic_search).unwrap();
        let protocol::WorkspaceResult::Search(generic) = generic.result else {
            panic!("generic search returned the wrong result variant");
        };
        assert_eq!(generic.match_count, 8);
        assert_eq!(generic.hits.len(), 8);
        let generic_hits = generic
            .hits
            .iter()
            .map(|row| match row {
                protocol::SearchRow::GenericNamespace(hit) => hit,
                protocol::SearchRow::Artifact(_) => {
                    panic!("GenericNamespaceV1 search returned an ArtifactV1 row")
                }
            })
            .collect::<Vec<_>>();
        let paths = generic_hits
            .iter()
            .map(|hit| {
                hit.projection
                    .iter()
                    .find(|field| field.field_id == "path")
                    .map(|field| field.value.clone())
                    .expect("every generic row projects its canonical path")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            [
                "/agents/query-test",
                "/agents/query-test/input",
                "/agents/query-test/logs",
                "/agents/query-test/metadata",
                "/agents/query-test/outputs",
                "/agents/query-test/outputs/dir",
                "/agents/query-test/outputs/dir/result.bin",
                "/agents/query-test/scripts",
            ]
            .into_iter()
            .map(|path| protocol::ScalarValue::String(path.to_owned()))
            .collect::<Vec<_>>()
        );
        assert_eq!(
            generic_hits
                .iter()
                .filter(|hit| hit.kind == protocol::GenericNamespaceKind::Directory)
                .count(),
            7
        );
        let artifact = generic_hits
            .iter()
            .find(|hit| hit.kind == protocol::GenericNamespaceKind::Artifact)
            .expect("the published artifact must remain visible");
        assert_eq!(
            artifact
                .relative_path
                .as_ref()
                .map(protocol::RelativePath::as_str),
            Some("outputs/dir/result.bin")
        );
        let artifact_projection = artifact
            .projection
            .iter()
            .map(|field| (field.field_id.as_str(), &field.value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            artifact_projection.get("body.producer"),
            Some(&&protocol::ScalarValue::String("executor-test".to_owned()))
        );
        assert_eq!(
            artifact_projection.get("body.manifest_id"),
            Some(&&protocol::ScalarValue::String("manifest-1".to_owned()))
        );
        assert_eq!(generic.facets.len(), 2);
        let kind_facet = generic
            .facets
            .iter()
            .find(|facet| facet.field_id == "kind")
            .expect("kind facet must be returned");
        assert_eq!(kind_facet.distinct_count, 2);
        assert_eq!(
            kind_facet
                .buckets
                .iter()
                .map(|bucket| bucket.count)
                .sum::<u64>(),
            8
        );

        let search_size = |request_fill: u8,
                           profile: protocol::QueryProfile,
                           field_id: &str,
                           operator: protocol::QueryOperator,
                           value: &str| {
            executor.execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([request_fill; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                    profile,
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: vec![protocol::QueryPredicate {
                        field_id: field_id.to_owned(),
                        operator,
                        operand: protocol::QueryOperand::Scalar(protocol::ScalarValue::String(
                            value.to_owned(),
                        )),
                    }],
                    projection: vec!["path".to_owned()],
                    sort: Vec::new(),
                    facets: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
        };
        for (request_fill, operator, value, expected_matches) in [
            (0x9d, protocol::QueryOperator::Prefix, "/agents", 8),
            (
                0x9e,
                protocol::QueryOperator::Contains,
                "agents/query-test",
                8,
            ),
            (
                0x9f,
                protocol::QueryOperator::Suffix,
                "agents/query-test",
                1,
            ),
        ] {
            let searched = search_size(
                request_fill,
                generic_query_profile(),
                "path",
                operator,
                value,
            )
            .unwrap();
            let protocol::WorkspaceResult::Search(searched) = searched.result else {
                panic!("presentation-path search returned the wrong result variant");
            };
            assert_eq!(searched.match_count, expected_matches);
            assert_eq!(searched.hits.len(), expected_matches as usize);
        }
        for (request_fill, operator) in [
            (0xa1, protocol::QueryOperator::Equal),
            (0xa2, protocol::QueryOperator::GreaterOrEqual),
        ] {
            let searched = search_size(
                request_fill,
                generic_query_profile(),
                "size_bytes",
                operator,
                "0",
            )
            .unwrap();
            let protocol::WorkspaceResult::Search(searched) = searched.result else {
                panic!("numeric-string search returned the wrong result variant");
            };
            assert_eq!(searched.match_count, 8);
            assert_eq!(searched.hits.len(), 8);
        }
        for (request_fill, invalid) in [(0xa3, "NaN"), (0xa4, "inf"), (0xa5, ""), (0xa6, "nope")] {
            let failure = search_size(
                request_fill,
                generic_query_profile(),
                "size_bytes",
                protocol::QueryOperator::Equal,
                invalid,
            )
            .unwrap_err();
            assert_eq!(failure.code, protocol::ErrorCode::InvalidArgument);
        }
        let native_string_number = search_size(
            0xa7,
            protocol::QueryProfile::ArtifactV1,
            "logical_size",
            protocol::QueryOperator::Equal,
            "0",
        )
        .unwrap_err();
        assert_eq!(
            native_string_number.code,
            protocol::ErrorCode::InvalidArgument
        );

        let generic_count = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5b; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Aggregate(protocol::AggregateRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    group_by: Vec::new(),
                    aggregates: vec![protocol::AggregateSpec {
                        function: protocol::AggregateFunction::Count,
                        field_id: None,
                        result_id: "rows".to_owned(),
                    }],
                    sort: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Aggregate(generic_count) = generic_count.result else {
            panic!("generic aggregate returned the wrong result variant");
        };
        assert_eq!(generic_count.input_match_count, 8);
        assert_eq!(generic_count.row_count, 8);
        assert_eq!(generic_count.group_count, 1);
        assert_eq!(
            generic_count.groups[0].values,
            vec![protocol::FieldValue {
                field_id: "rows".to_owned(),
                value: protocol::ScalarValue::Unsigned(8),
            }]
        );

        let grouped = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5e; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Aggregate(protocol::AggregateRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    group_by: vec!["body.producer".to_owned()],
                    aggregates: vec![
                        protocol::AggregateSpec {
                            function: protocol::AggregateFunction::Count,
                            field_id: None,
                            result_id: "rows".to_owned(),
                        },
                        protocol::AggregateSpec {
                            function: protocol::AggregateFunction::Sum,
                            field_id: Some("size_bytes".to_owned()),
                            result_id: "bytes".to_owned(),
                        },
                    ],
                    sort: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Aggregate(grouped) = grouped.result else {
            panic!("grouped generic aggregate returned the wrong result variant");
        };
        assert_eq!(grouped.input_match_count, 8);
        assert_eq!(grouped.row_count, 1);
        assert_eq!(grouped.group_count, 1);
        assert_eq!(
            grouped.groups[0].keys,
            vec![protocol::FieldValue {
                field_id: "body.producer".to_owned(),
                value: protocol::ScalarValue::String("executor-test".to_owned()),
            }]
        );
        assert!(grouped.groups[0]
            .values
            .iter()
            .any(|field| field.field_id == "bytes"
                && matches!(field.value, protocol::ScalarValue::Decimal(_))));

        let grouped_paths = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0xa8; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Aggregate(protocol::AggregateRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    group_by: vec!["path".to_owned()],
                    aggregates: vec![protocol::AggregateSpec {
                        function: protocol::AggregateFunction::Count,
                        field_id: None,
                        result_id: "rows".to_owned(),
                    }],
                    sort: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Aggregate(grouped_paths) = grouped_paths.result else {
            panic!("path-grouped generic aggregate returned the wrong result variant");
        };
        assert_eq!(grouped_paths.input_match_count, 8);
        assert_eq!(grouped_paths.row_count, 8);
        assert_eq!(grouped_paths.group_count, 8);
        assert!(grouped_paths.groups.iter().any(|group| {
            group.keys
                == vec![protocol::FieldValue {
                    field_id: "path".to_owned(),
                    value: protocol::ScalarValue::String(
                        "/agents/query-test/outputs/dir/result.bin".to_owned(),
                    ),
                }]
        }));

        let generic_catalog = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5c; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    path_match: protocol::CatalogPathMatch::Prefix,
                    field_prefix: None,
                    include_facets: true,
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Catalog(generic_catalog) = generic_catalog.result else {
            panic!("generic catalog returned the wrong result variant");
        };
        let generic_fields = generic_catalog
            .fields
            .iter()
            .map(|field| field.field_id.as_str())
            .collect::<BTreeSet<_>>();
        for builtin in [
            "path",
            "name",
            "kind",
            "size_bytes",
            "body.content_type",
            "body.producer",
            "body.manifest_id",
        ] {
            assert!(generic_fields.contains(builtin), "missing {builtin}");
        }
        for native in [
            "workbench_id",
            "generation",
            "logical_size",
            "body_digest_uri",
            "content_type",
            "producer",
            "manifest_id",
        ] {
            assert!(
                !generic_fields.contains(native),
                "leaked native field {native}"
            );
        }

        let empty_prefix_catalog = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0xa9; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    path_match: protocol::CatalogPathMatch::Prefix,
                    field_prefix: Some(String::new()),
                    include_facets: true,
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Catalog(empty_prefix_catalog) = empty_prefix_catalog.result
        else {
            panic!("empty-prefix generic catalog returned the wrong result variant");
        };
        assert_eq!(empty_prefix_catalog.fields, generic_catalog.fields);
        assert_eq!(empty_prefix_catalog.facets, generic_catalog.facets);

        let exact_catalog = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5d; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Workspace {
                        workbench: protocol::WorkbenchName::new("query-test").unwrap(),
                        path_prefix: Some(
                            protocol::RelativePath::new("outputs/dir/result.bin").unwrap(),
                        ),
                    },
                    path_match: protocol::CatalogPathMatch::Exact,
                    field_prefix: None,
                    include_facets: true,
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Catalog(exact_catalog) = exact_catalog.result else {
            panic!("exact generic catalog returned the wrong result variant");
        };
        assert!(exact_catalog.fields.is_empty());
        assert!(exact_catalog.facets.is_empty());

        let cursor_seed = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5f; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                    profile: generic_query_profile(),
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    projection: vec!["path".to_owned()],
                    sort: vec![protocol::SortField {
                        field_id: "path".to_owned(),
                        direction: protocol::SortDirection::Ascending,
                    }],
                    facets: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 1,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Search(cursor_seed) = cursor_seed.result else {
            panic!("cursor seed returned the wrong result variant");
        };
        let cursor = cursor_seed
            .next_cursor
            .expect("one-row generic search must return a continuation");
        let presentation_root_drift = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0xaa; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                    profile: protocol::QueryProfile::GenericCustomIndexV1 {
                        presentation_path_root: "/other-agents".to_owned(),
                    },
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    projection: vec!["path".to_owned()],
                    sort: vec![protocol::SortField {
                        field_id: "path".to_owned(),
                        direction: protocol::SortDirection::Ascending,
                    }],
                    facets: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: Some(cursor.clone()),
                        limit: 1,
                    },
                }),
            })
            .unwrap_err();
        assert_eq!(
            presentation_root_drift.code,
            protocol::ErrorCode::PreconditionFailed
        );
        assert_eq!(presentation_root_drift.conflict, None);

        let profile_drift = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x60; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Search(protocol::SearchRequest {
                    profile: protocol::QueryProfile::ArtifactV1,
                    scope: protocol::QueryScope::Root { path_prefix: None },
                    predicates: Vec::new(),
                    projection: vec!["path".to_owned()],
                    sort: vec![protocol::SortField {
                        field_id: "path".to_owned(),
                        direction: protocol::SortDirection::Ascending,
                    }],
                    facets: Vec::new(),
                    page: protocol::PageRequest {
                        cursor: Some(cursor),
                        limit: 1,
                    },
                }),
            })
            .unwrap_err();
        assert_eq!(profile_drift.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(profile_drift.conflict, None);

        let catalog = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x56; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                profile: protocol::QueryProfile::ArtifactV1,
                scope: protocol::QueryScope::Workspace {
                    workbench: protocol::WorkbenchName::new("query-test").unwrap(),
                    path_prefix: None,
                },
                path_match: protocol::CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: false,
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

        // An ArtifactV1 catalog that discovers a custom index field must stay
        // wire-encodable: only Generic custom fields carry a per-field
        // scalar-type list, so built-in and ArtifactV1 fields project none.
        let full_catalog = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5b; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                    profile: protocol::QueryProfile::ArtifactV1,
                    scope: protocol::QueryScope::Workspace {
                        workbench: protocol::WorkbenchName::new("query-test").unwrap(),
                        path_prefix: None,
                    },
                    path_match: protocol::CatalogPathMatch::Prefix,
                    field_prefix: None,
                    include_facets: false,
                    page: protocol::PageRequest {
                        cursor: None,
                        limit: 100,
                    },
                }),
            })
            .unwrap();
        let protocol::WorkspaceResult::Catalog(full_page) = full_catalog.result else {
            panic!("full catalog returned the wrong result variant");
        };
        assert!(full_page
            .fields
            .iter()
            .any(|field| field.field_id == "agent.score"));
        assert!(full_page
            .fields
            .iter()
            .all(|field| !field.generic_custom && field.scalar_types.is_empty()));
        protocol::encode_response(&protocol::RpcResponse::Workspace(Box::new(
            protocol::WorkspaceRpcResponse {
                route: route(1),
                request_id: protocol::RequestIdentity([0x5b; types::FIXED_ID_BYTES]),
                commit_version: None,
                replayed: false,
                outcome: protocol::WorkspaceRpcOutcome::Success(Box::new(
                    protocol::WorkspaceResult::Catalog(full_page),
                )),
            },
        )))
        .expect("ArtifactV1 catalog with custom index fields must encode on the wire");

        executor
            .execute(&create_request(0x57, "query-version-advance", 0x58, 1))
            .unwrap();
        let stale_catalog = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x59; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::Catalog(protocol::CatalogRequest {
                profile: protocol::QueryProfile::ArtifactV1,
                scope: protocol::QueryScope::Workspace {
                    workbench: protocol::WorkbenchName::new("query-test").unwrap(),
                    path_prefix: None,
                },
                path_match: protocol::CatalogPathMatch::Prefix,
                field_prefix: None,
                include_facets: false,
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
    fn exact_publish_stage_retry_resumes_after_its_heartbeat_committed() {
        let (store, executor) = ready_executor();
        executor
            .execute(&create_request(0x60, "heartbeat-retry", 0x61, 1))
            .unwrap();
        let operation_id = protocol::OperationIdentity([0x62; types::FIXED_ID_BYTES]);
        let artifact_revision_id =
            protocol::ArtifactRevisionIdentity([0x63; types::FIXED_ID_BYTES]);
        let seals = protocol::seal_artifact_publish_plan(artifact_revision_id, &[], &[]).unwrap();
        let begun = executor
            .execute(&protocol::WorkspaceRpcRequest {
                route: route(1),
                request_id: protocol::RequestIdentity([0x64; types::FIXED_ID_BYTES]),
                operation: protocol::WorkspaceRequest::BeginArtifactPublish(
                    protocol::BeginArtifactPublishRequest {
                        operation_id,
                        artifact_revision_id,
                        target: protocol::WorkspacePath {
                            workbench: protocol::WorkbenchName::new("heartbeat-retry").unwrap(),
                            path: protocol::RelativePath::new("outputs/result.bin").unwrap(),
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
            })
            .unwrap();
        let protocol::WorkspaceResult::Operation(status) = begun.result else {
            panic!("begin artifact publication returned the wrong result variant");
        };

        let complete = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([0x65; types::FIXED_ID_BYTES]),
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
                        producer: Some("heartbeat-retry-test".to_owned()),
                        manifest_identity: None,
                        index_fields: Vec::new(),
                    },
                },
            ),
        };
        assert!(!executor.claim_mutation(&complete).unwrap());

        // Model a response-safe partial attempt: the internal heartbeat is
        // durable, but the following publication transition lost an unrelated
        // store-wide read-version race before it could commit.
        let before = executor
            .load_publish_operation(
                complete.route,
                store.current_read_version().unwrap(),
                operation_id,
            )
            .unwrap();
        require_publish_token(&before, status.token).unwrap();
        let heartbeat_id = derived_request_id(
            complete.request_id,
            b"publish-heartbeat-complete-transition",
            0,
        );
        let heartbeated = meta::PublicationService::new(&store)
            .heartbeat_publish(meta::HeartbeatPublishRequest {
                context: executor
                    .publication_context(complete.route, heartbeat_id)
                    .unwrap(),
                expected_operation: before.clone(),
                activity_deadline_ms: before.activity_deadline_ms.checked_add(1).unwrap(),
            })
            .unwrap()
            .operation;
        assert_ne!(
            publish_state_digest(&heartbeated).unwrap(),
            status.token.state_digest
        );

        // The outer request id is the authority for this exact retry. Reusing
        // it with different inputs remains rejected before heartbeat replay.
        let mut mismatched = complete.clone();
        let protocol::WorkspaceRequest::CompleteArtifactPublish(request) =
            &mut mismatched.operation
        else {
            unreachable!();
        };
        request.token.state_digest = protocol::Digest([0x66; types::SHA256_BYTES]);
        let mismatch = executor.claim_mutation(&mismatched).unwrap_err();
        assert_eq!(mismatch.code, protocol::ErrorCode::RequestReplayMismatch);

        assert!(executor.claim_mutation(&complete).unwrap());
        let resumed = executor
            .heartbeat_publish_operation(
                &complete,
                status.token,
                b"publish-heartbeat-complete-transition",
            )
            .unwrap();
        assert_eq!(resumed.encode().unwrap(), heartbeated.encode().unwrap());
    }

    #[test]
    fn internal_retry_classification_excludes_business_conflicts() {
        let transient = meta_failure(meta::MetaError::WriteReadVersionMismatch {
            requested: 10,
            current: 11,
        });
        assert!(internal_metadata_conflict(&transient));

        let lost_race = meta_failure(meta::MetaError::WriteConflict);
        assert!(internal_metadata_conflict(&lost_race));

        let predicate = meta_failure(meta::MetaError::PredicateFailed);
        assert!(!internal_metadata_conflict(&predicate));

        for state in [
            nokv_meta_store::UnknownCommit::Settled,
            nokv_meta_store::UnknownCommit::MayCommit,
            nokv_meta_store::UnknownCommit::Poisoned,
        ] {
            let unknown = meta_failure(meta::MetaError::Store {
                operation: "commit",
                source: nokv_meta_store::StoreError::OutcomeUnknown {
                    state,
                    reason: "injected unknown outcome".to_owned(),
                },
            });
            assert!(!internal_metadata_conflict(&unknown));
            assert_eq!(unknown.code, protocol::ErrorCode::NotOwner);
            assert_eq!(
                unknown.conflict,
                Some(protocol::ConflictKind::RootPlacement)
            );
            assert!(unknown.retryable);
            assert!(unknown.message.contains(&state.to_string()));
        }

        let known_not_applied = meta_failure(meta::MetaError::Store {
            operation: "commit",
            source: nokv_meta_store::StoreError::Unavailable(
                "injected definitely-not-applied outcome".to_owned(),
            ),
        });
        assert_eq!(known_not_applied.code, protocol::ErrorCode::Internal);
        assert!(known_not_applied.retryable);

        let fenced = meta_failure(meta::MetaError::Store {
            operation: "commit",
            source: nokv_meta_store::StoreError::Fenced {
                expected_owner_epoch: 7,
                expected_session_generation: 9,
            },
        });
        assert!(!internal_metadata_conflict(&fenced));
        assert_eq!(fenced.code, protocol::ErrorCode::NotOwner);
        assert_eq!(fenced.conflict, Some(protocol::ConflictKind::RootPlacement));
        assert!(fenced.retryable);
        assert!(fenced.message.contains("7/9"));

        let limit = meta_failure(meta::MetaError::Store {
            operation: "commit",
            source: nokv_meta_store::StoreError::LimitExceeded {
                kind: nokv_meta_store::LimitKind::TransactionBytes,
                actual: 1_000_001,
                maximum: 1_000_000,
            },
        });
        assert_eq!(limit.code, protocol::ErrorCode::ResourceExhausted);
        assert!(!limit.retryable);
        assert!(limit.message.contains("transaction bytes"));
        assert!(limit.message.contains("1000001"));
        assert!(limit.message.contains("1000000"));

        let internal_store_failure = meta_failure(meta::MetaError::Store {
            operation: "commit",
            source: nokv_meta_store::StoreError::InvalidRequest("invalid key range".to_owned()),
        });
        assert_eq!(internal_store_failure.code, protocol::ErrorCode::Internal);
        assert!(!internal_store_failure.retryable);

        let path = conflict(
            protocol::ConflictKind::PathGeneration,
            "path generation mismatch",
            Some(1),
        );
        assert!(!internal_metadata_conflict(&path));

        let operation = conflict(
            protocol::ConflictKind::OperationState,
            "operation token state digest is stale",
            None,
        );
        assert!(!internal_metadata_conflict(&operation));
    }

    #[test]
    fn internal_metadata_retry_converges_and_preserves_terminal_conflicts() {
        let transient = || {
            meta_failure(meta::MetaError::WriteReadVersionMismatch {
                requested: 10,
                current: 11,
            })
        };
        let mut attempts = 0;
        let converged = retry_internal_metadata_conflicts(|| {
            attempts += 1;
            if attempts < 3 {
                Err(transient())
            } else {
                Ok("applied")
            }
        })
        .unwrap();
        assert_eq!(converged, "applied");
        assert_eq!(attempts, 3);

        let business_conflict = conflict(
            protocol::ConflictKind::PathGeneration,
            "path already exists",
            Some(1),
        );
        let mut business_attempts = 0;
        let returned = retry_internal_metadata_conflicts::<()>(|| {
            business_attempts += 1;
            Err(business_conflict.clone())
        })
        .unwrap_err();
        assert_eq!(returned, business_conflict);
        assert_eq!(business_attempts, 1);

        let mut exhausted_attempts = 0;
        let exhausted = retry_internal_metadata_conflicts::<()>(|| {
            exhausted_attempts += 1;
            Err(transient())
        })
        .unwrap_err();
        assert!(internal_metadata_conflict(&exhausted));
        assert_eq!(exhausted_attempts, MAX_INTERNAL_METADATA_ATTEMPTS);
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
            object_namespace_id: types::ObjectNamespaceId::from_bytes([10; types::FIXED_ID_BYTES]),
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
    fn rename_path_crosses_one_atomic_metadata_command_and_replays() {
        let (store, executor) = ready_executor();
        let incarnation = types::WorkspaceIncarnationId::from_bytes([63; types::FIXED_ID_BYTES]);
        executor
            .execute(&create_request(63, "rename-test", 63, 1))
            .unwrap();
        put_visible_path(&store, incarnation);
        let source = protocol::WorkspacePath {
            workbench: protocol::WorkbenchName::new("rename-test").unwrap(),
            path: protocol::RelativePath::new("outputs/result.bin").unwrap(),
        };
        let destination = protocol::WorkspacePath {
            workbench: source.workbench.clone(),
            path: protocol::RelativePath::new("outputs/moved.bin").unwrap(),
        };
        let request = protocol::WorkspaceRpcRequest {
            route: route(1),
            request_id: protocol::RequestIdentity([64; types::FIXED_ID_BYTES]),
            operation: protocol::WorkspaceRequest::RenamePath(protocol::RenamePathRequest {
                source: source.clone(),
                destination: destination.clone(),
                expected_generation: 1,
            }),
        };

        let renamed = executor.execute(&request).unwrap();
        assert!(!renamed.replayed);
        let protocol::WorkspaceResult::Renamed(result) = &renamed.result else {
            panic!("rename returned the wrong result variant");
        };
        assert_eq!(result.source, source);
        assert_eq!(result.destination, destination);
        assert_eq!(result.workspace_revision, 1);
        assert_eq!(result.generation, 1);
        assert_eq!(
            result.artifact_revision_id,
            types::ArtifactRevisionId::from_bytes([9; types::FIXED_ID_BYTES]).into()
        );

        let replayed = executor.execute(&request).unwrap();
        assert!(replayed.replayed);
        assert_eq!(replayed.result, renamed.result);
        assert_eq!(replayed.commit_version, renamed.commit_version);

        let context =
            meta::RootReadContext::current(&store, root(), placement(), owner(1)).unwrap();
        assert_eq!(
            meta::get_visible_path_at(
                &store,
                context,
                &types::WorkbenchId::new("rename-test").unwrap(),
                &types::NormalizedRelativePath::new("outputs/result.bin").unwrap(),
            )
            .unwrap(),
            None
        );
        assert!(meta::get_visible_path_at(
            &store,
            context,
            &types::WorkbenchId::new("rename-test").unwrap(),
            &types::NormalizedRelativePath::new("outputs/moved.bin").unwrap(),
        )
        .unwrap()
        .is_some());

        let reused = protocol::WorkspaceRpcRequest {
            operation: protocol::WorkspaceRequest::RenamePath(protocol::RenamePathRequest {
                source,
                destination: protocol::WorkspacePath {
                    workbench: protocol::WorkbenchName::new("rename-test").unwrap(),
                    path: protocol::RelativePath::new("outputs/other.bin").unwrap(),
                },
                expected_generation: 1,
            }),
            ..request
        };
        let failure = executor.execute(&reused).unwrap_err();
        assert_eq!(failure.code, protocol::ErrorCode::RequestReplayMismatch);
    }

    #[test]
    fn rename_destination_conflict_is_a_typed_already_exists_failure() {
        let failure = rename_path_failure(meta::RenamePathError::DestinationAlreadyExists);
        assert_eq!(failure.code, protocol::ErrorCode::AlreadyExists);
        assert_eq!(
            failure.conflict,
            Some(protocol::ConflictKind::PathGeneration)
        );
        assert!(!failure.retryable);
    }

    #[test]
    fn snapshot_lifecycle_uses_visible_incarnation_and_lists_terminal_states() {
        let (store, executor) = ready_executor();
        executor
            .execute(&create_request(70, "snapshot-test", 71, 1))
            .unwrap();
        install_snapshot_committed_head(
            &store,
            types::WorkspaceIncarnationId::from_bytes([71; types::FIXED_ID_BYTES]),
            0xD0,
        );
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

    struct CommittedSnapshotSource {
        workbench: protocol::WorkbenchName,
        workspace_incarnation_id: protocol::WorkspaceIdentity,
        snapshot_id: u64,
    }

    /// Creates one Workbench through the executor, commits it with a one-byte
    /// run manifest, optionally seeds shared-revision paths, and mints one
    /// snapshot so it is a valid committed restore source. Request identities
    /// use `fill..fill + 12`.
    fn seed_committed_snapshot_source(
        store: &Arc<meta::MetaShard>,
        executor: &MetadataWorkspaceRequestExecutor,
        name: &str,
        fill: u8,
        snapshot_id: u64,
        shared_paths: usize,
    ) -> CommittedSnapshotSource {
        let workbench = protocol::WorkbenchName::new(name).unwrap();
        let workspace_incarnation_id = protocol::WorkspaceIdentity([fill; types::FIXED_ID_BYTES]);
        executor
            .execute(&restore_rpc(
                fill,
                protocol::WorkspaceRequest::CreateWorkspace(protocol::CreateWorkspaceRequest {
                    workbench: workbench.clone(),
                    workspace_incarnation_id,
                }),
            ))
            .unwrap();
        let commit_operation_id =
            protocol::OperationIdentity([fill.wrapping_add(1); types::FIXED_ID_BYTES]);
        let run_manifest_revision =
            protocol::ArtifactRevisionIdentity([fill.wrapping_add(2); types::FIXED_ID_BYTES]);
        let commit_operation = protocol::WorkspaceRequest::Commit(protocol::CommitRequest {
            operation_id: commit_operation_id,
            workbench: workbench.clone(),
            workspace_incarnation_id,
            commit_id: protocol::CommitIdentity([fill.wrapping_add(3); types::SHA256_BYTES]),
            content_digest: protocol::DigestUri::new(format!(
                "sha256:{}",
                "aa".repeat(types::SHA256_BYTES)
            ))
            .unwrap(),
            manifest_digest: protocol::sha256_digest_uri(protocol::Digest(
                Sha256::digest([0x7b]).into(),
            )),
            projection_input_digest: protocol::Digest([fill.wrapping_add(4); types::SHA256_BYTES]),
            tree_manifest_revision_id: run_manifest_revision,
            replace: false,
            run_manifest_condition: protocol::PublishCondition::CreateOnly,
            expected_head_generation: None,
            parents: Vec::new(),
            producer: None,
            lineage_projection: Vec::new(),
        });
        let begun = executor
            .execute(&restore_rpc(fill.wrapping_add(1), commit_operation.clone()))
            .unwrap();
        let protocol::WorkspaceResult::Operation(begun) = begun.result else {
            panic!("initial commit request returned the wrong result variant");
        };
        assert_eq!(begun.state, protocol::OperationState::Running);
        publish_one_byte_artifact(
            executor,
            fill.wrapping_add(2),
            protocol::OperationIdentity([fill.wrapping_add(5); types::FIXED_ID_BYTES]),
            run_manifest_revision,
            name,
            RUN_MANIFEST_PATH,
            protocol::PublicationAuthority::CommitStaging {
                commit_operation_id,
            },
            0x7b,
        );
        let committed = executor
            .execute(&restore_rpc(fill.wrapping_add(10), commit_operation))
            .unwrap();
        let protocol::WorkspaceResult::Operation(committed) = committed.result else {
            panic!("completed commit returned the wrong result variant");
        };
        assert_eq!(committed.state, protocol::OperationState::Succeeded);
        if shared_paths > 0 {
            put_shared_revision_paths(store, workspace_incarnation_id.into(), shared_paths);
        }
        executor
            .execute(&restore_rpc(
                fill.wrapping_add(11),
                protocol::WorkspaceRequest::MintSnapshot(protocol::MintSnapshotRequest {
                    workbench: workbench.clone(),
                    workspace_incarnation_id,
                    snapshot_id,
                    lease_deadline_ms: 10_000,
                    alias: None,
                    annotation: Vec::new(),
                }),
            ))
            .unwrap();
        CommittedSnapshotSource {
            workbench,
            workspace_incarnation_id,
            snapshot_id,
        }
    }

    fn fork_prepare_request(
        source: &CommittedSnapshotSource,
        destination: &str,
        destination_incarnation_fill: u8,
    ) -> (
        protocol::PrepareRestoreRequest,
        protocol::RestoreManifestIdentity,
    ) {
        let destination_workbench = protocol::WorkbenchName::new(destination).unwrap();
        let destination_workspace_incarnation_id =
            protocol::WorkspaceIdentity([destination_incarnation_fill; types::FIXED_ID_BYTES]);
        let operation_id: protocol::OperationIdentity = meta::restore_operation_id(
            root(),
            &types::WorkbenchId::new(source.workbench.as_str()).unwrap(),
            source.workspace_incarnation_id.into(),
            meta::RestoreSourceSelector::Snapshot(types::SnapshotId::new(source.snapshot_id)),
            &types::WorkbenchId::new(destination).unwrap(),
            destination_workspace_incarnation_id.into(),
        )
        .unwrap()
        .into();
        let destination_restore_manifest_identity = protocol::RestoreManifestIdentity {
            publication_operation_id: protocol::OperationIdentity(
                [destination_incarnation_fill.wrapping_add(1); types::FIXED_ID_BYTES],
            ),
            artifact_revision_id: protocol::ArtifactRevisionIdentity(
                [destination_incarnation_fill.wrapping_add(2); types::FIXED_ID_BYTES],
            ),
        };
        (
            protocol::PrepareRestoreRequest {
                operation_id,
                source_workbench: source.workbench.clone(),
                source_workspace_incarnation_id: source.workspace_incarnation_id,
                source: protocol::RestoreSource::Snapshot(protocol::SnapshotSelector::Id(
                    source.snapshot_id,
                )),
                destination_workbench,
                destination_workspace_incarnation_id,
                destination_restore_manifest_identity,
                restore_manifest: protocol::RestoreManifestDescriptor {
                    body_digest: protocol::sha256_digest_uri(protocol::Digest(
                        Sha256::digest([0x7d]).into(),
                    )),
                    logical_size: 1,
                    content_type: protocol::ContentType::new("application/json").unwrap(),
                },
            },
            destination_restore_manifest_identity,
        )
    }

    #[test]
    fn restore_preparation_coordinator_is_keyed_and_recovers_a_failed_driver() {
        let coordinator = Arc::new(RestorePreparationCoordinator::default());
        let first_key = RestorePreparationKey {
            root_id: root(),
            destination_workspace_incarnation_id: types::WorkspaceIncarnationId::from_bytes(
                [0x91; types::FIXED_ID_BYTES],
            ),
        };
        let second_key = RestorePreparationKey {
            root_id: root(),
            destination_workspace_incarnation_id: types::WorkspaceIncarnationId::from_bytes(
                [0x92; types::FIXED_ID_BYTES],
            ),
        };
        let first = coordinator.gate(first_key);
        let same = coordinator.gate(first_key);
        let second = coordinator.gate(second_key);
        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &second));

        let first_guard = first.lock().unwrap();
        assert!(same.try_lock().is_err());
        let second_guard = second
            .try_lock()
            .expect("a different destination must not share the driver gate");
        drop(second_guard);
        drop(first_guard);

        let failed_driver = Arc::clone(&same);
        let failure = std::thread::spawn(move || {
            let _driver = failed_driver.lock().unwrap();
            panic!("injected restore preparation driver failure");
        })
        .join();
        assert!(failure.is_err());
        let recovered = coordinator.gate(first_key);
        let _replacement_driver = recovered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }

    /// Exact concurrent `PrepareRestore` callers, whether they share one
    /// request identity or each carry their own, must converge on one durable
    /// operation, one source hold, and one identical sealed preparation.
    fn assert_concurrent_fork_preparations_converge(request_fills: &[u8]) {
        assert!(!request_fills.is_empty());
        let (store, executor) = ready_executor();
        let source = seed_committed_snapshot_source(&store, &executor, "fork-source", 0x30, 7, 64);
        let (request, _) = fork_prepare_request(&source, "fork-destination", 0x50);
        let prepare = |request_fill| {
            restore_rpc(
                request_fill,
                protocol::WorkspaceRequest::PrepareRestore(request.clone()),
            )
        };
        let requests = request_fills
            .iter()
            .copied()
            .map(prepare)
            .collect::<Vec<_>>();
        let barrier = Arc::new(std::sync::Barrier::new(requests.len()));
        let outcomes = std::thread::scope(|scope| {
            let handles = requests
                .into_iter()
                .map(|request| {
                    let executor = executor.clone();
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        executor.execute(&request)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut outcomes = outcomes.into_iter().map(Result::unwrap);
        let first_outcome = outcomes.next().unwrap();
        for outcome in outcomes {
            assert_eq!(first_outcome.result, outcome.result);
        }
        let exact_replay = executor.execute(&prepare(request_fills[0])).unwrap();
        assert_eq!(exact_replay.result, first_outcome.result);
        assert!(exact_replay.replayed);
        let protocol::WorkspaceResult::RestorePrepared(prepared) = first_outcome.result else {
            panic!("prepare restore returned the wrong result variant");
        };
        assert_eq!(prepared.destination_workbench.as_str(), "fork-destination");
        // The source closure seals the committed run manifest plus the shared
        // paths; only the shared paths materialize into the destination.
        assert_eq!(prepared.source_member_count, 65);
        assert_eq!(prepared.materialized_member_count, 64);
        assert!(prepared.destination_binding.is_none());

        let snapshot = executor
            .execute(&restore_rpc(
                0xc0,
                protocol::WorkspaceRequest::GetSnapshot(protocol::GetSnapshotRequest {
                    workbench: source.workbench,
                    selector: protocol::SnapshotSelector::Id(source.snapshot_id),
                }),
            ))
            .unwrap();
        let protocol::WorkspaceResult::Snapshot(snapshot) = snapshot.result else {
            panic!("snapshot lookup returned the wrong result variant");
        };
        assert_eq!(snapshot.consumer_count, 1);
    }

    #[test]
    fn concurrent_different_request_ids_drive_one_fork_preparation_to_completion() {
        assert_concurrent_fork_preparations_converge(&[
            0x95, 0x96, 0x97, 0x98, 0x99, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2,
            0xa3, 0xa4,
        ]);
    }

    #[test]
    fn concurrent_same_request_id_joins_one_fork_preparation_to_completion() {
        assert_concurrent_fork_preparations_converge(&[0x95, 0x95, 0x95, 0x95, 0x95]);
    }

    /// Exact concurrent finalizers of one prepared, bound, and fully published
    /// restore must all observe the same terminal `Restored` result, and the
    /// exact manifest publications plus finalize must replay afterwards.
    #[test]
    fn completed_fork_concurrent_finalize_replays_exact_manifest_publication() {
        let (store, executor) = ready_executor();
        let source =
            seed_committed_snapshot_source(&store, &executor, "fork-replay-source", 0x30, 11, 3);
        let (request, destination_restore_manifest_identity) =
            fork_prepare_request(&source, "fork-replay-destination", 0x50);
        let restore_operation_id = request.operation_id;
        let destination = request.destination_workbench.clone();
        let prepared = executor
            .execute(&restore_rpc(
                0x60,
                protocol::WorkspaceRequest::PrepareRestore(request),
            ))
            .unwrap();
        let protocol::WorkspaceResult::RestorePrepared(prepared) = prepared.result else {
            panic!("prepare restore returned the wrong result variant");
        };
        assert_eq!(prepared.operation_id, restore_operation_id);

        let destination_run_manifest_identity = protocol::RestoreManifestIdentity {
            publication_operation_id: protocol::OperationIdentity([0x61; types::FIXED_ID_BYTES]),
            artifact_revision_id: protocol::ArtifactRevisionIdentity([0x62; types::FIXED_ID_BYTES]),
        };
        let bind_request = protocol::BindRestoreDestinationRequest {
            operation_id: restore_operation_id,
            destination_commit_id: protocol::CommitIdentity([0x63; types::SHA256_BYTES]),
            // The shared paths were published after the source commit, so the
            // snapshot is a dirty materialization and must not reuse the clean
            // source content digest.
            effective_content_digest: protocol::DigestUri::new(format!(
                "sha256:{}",
                "bb".repeat(types::SHA256_BYTES)
            ))
            .unwrap(),
            destination_run_manifest_projection_input_digest: protocol::Digest(
                [0x64; types::SHA256_BYTES],
            ),
            destination_run_manifest_identity,
            destination_restore_manifest_identity,
        };
        executor
            .execute(&restore_rpc(
                0x65,
                protocol::WorkspaceRequest::BindRestoreDestination(bind_request),
            ))
            .unwrap();
        let publish_manifests = |first_fill: u8| {
            publish_one_byte_artifact(
                &executor,
                first_fill,
                destination_run_manifest_identity.publication_operation_id,
                destination_run_manifest_identity.artifact_revision_id,
                destination.as_str(),
                RUN_MANIFEST_PATH,
                protocol::PublicationAuthority::RestoreStaging {
                    restore_operation_id,
                },
                0x7c,
            );
            publish_one_byte_artifact(
                &executor,
                first_fill.wrapping_add(8),
                destination_restore_manifest_identity.publication_operation_id,
                destination_restore_manifest_identity.artifact_revision_id,
                destination.as_str(),
                meta::RESTORE_MANIFEST_PATH,
                protocol::PublicationAuthority::RestoreStaging {
                    restore_operation_id,
                },
                0x7d,
            )
        };
        publish_manifests(0x70);

        let finalize = |request_fill: u8| {
            restore_rpc(
                request_fill,
                protocol::WorkspaceRequest::FinalizeRestore(protocol::FinalizeRestoreRequest {
                    operation_id: restore_operation_id,
                }),
            )
        };
        let fills = [0xb0_u8, 0xb1, 0xb2, 0xb3];
        let barrier = Arc::new(std::sync::Barrier::new(fills.len()));
        let outcomes = std::thread::scope(|scope| {
            let handles = fills
                .iter()
                .map(|fill| {
                    let executor = executor.clone();
                    let barrier = Arc::clone(&barrier);
                    let request = finalize(*fill);
                    scope.spawn(move || {
                        barrier.wait();
                        executor.execute(&request)
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let outcomes = outcomes
            .into_iter()
            .map(|outcome| outcome.expect("every exact concurrent finalizer must converge"))
            .collect::<Vec<_>>();
        let protocol::WorkspaceResult::Restored(first) = &outcomes[0].result else {
            panic!("finalize returned the wrong result variant");
        };
        for outcome in &outcomes[1..] {
            assert_eq!(&outcomes[0].result, &outcome.result);
        }
        assert_eq!(first.operation_id, restore_operation_id);
        assert_eq!(first.destination.workbench, destination);
        assert!(first.destination.commit_head.is_some());

        // The exact manifest publications and finalize replay after the
        // restore is Complete without disturbing the terminal state.
        let replayed_manifest = publish_manifests(0x70);
        assert_eq!(replayed_manifest.logical_size, 1);
        let replay = executor.execute(&finalize(0xb0)).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.result, outcomes[0].result);
        let fresh_finalizer = executor.execute(&finalize(0xb9)).unwrap();
        assert!(fresh_finalizer.replayed);
        assert_eq!(fresh_finalizer.result, outcomes[0].result);
    }

    #[test]
    fn snapshot_alias_point_get_uses_alias_generation_not_numeric_id_order() {
        let (store, executor) = ready_executor();
        executor
            .execute(&create_request(80, "snapshot-alias-test", 81, 1))
            .unwrap();
        install_snapshot_committed_head(
            &store,
            types::WorkspaceIncarnationId::from_bytes([81; types::FIXED_ID_BYTES]),
            0xD1,
        );
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
    fn snapshot_requires_a_committed_head_without_leaking_workbench_identity() {
        let failure = snapshot_failure(meta::SnapshotError::WorkspaceNotCommitted {
            workbench_id: types::WorkbenchId::new("private-agent-workbench").unwrap(),
        });

        assert_eq!(failure.code, protocol::ErrorCode::PreconditionFailed);
        assert_eq!(failure.conflict, Some(protocol::ConflictKind::CommitHead));
        assert!(!failure.retryable);
        assert_eq!(
            failure.message,
            "snapshot requires a committed workbench head"
        );
        assert!(!failure.message.contains("private-agent-workbench"));
    }

    #[test]
    fn snapshot_source_commit_corruption_is_sanitized() {
        let failures = [
            meta::SnapshotError::SourceCommitMissing,
            meta::SnapshotError::SourceCommitUnavailable {
                state: types::CommitState::Retiring,
            },
            meta::SnapshotError::SourceCommitBindingMismatch,
            meta::SnapshotError::SourceCommitConsumerMissing,
            meta::SnapshotError::SourceCommitConsumerMismatch,
            meta::SnapshotError::CommitConsumerCountOverflow,
            meta::SnapshotError::CommitConsumerCountUnderflow,
            meta::SnapshotError::CommitConsumerEpochOverflow,
            meta::SnapshotError::CommitVersionOverflow,
            meta::SnapshotError::CommitCodec(meta::CommitRecordError::EmptyField {
                field: "physical/key/private-agent",
            }),
        ];

        for error in failures {
            let failure = snapshot_failure(error);
            assert_eq!(failure.code, protocol::ErrorCode::Internal);
            assert_eq!(
                failure.message,
                "snapshot source commit metadata is inconsistent"
            );
            assert!(!failure.retryable);
            assert!(failure.conflict.is_none());
            assert!(!failure.message.contains("private-agent"));
            assert!(!failure.message.contains("Retiring"));
        }
    }

    #[test]
    fn ranged_manifest_plan_pages_across_meta_batches_without_truncation() {
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
                expected_read_version: None,
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
                expected_read_version: None,
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
                expected_read_version: None,
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
                expected_read_version: None,
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
