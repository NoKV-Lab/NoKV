/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reusable high-level Workbench commit and restore lifecycle orchestration.
//!
//! The SDK owns admission, recover-before-read, protocol CAS composition, and
//! exact destination binding. Canonical Agent presentation bytes are supplied
//! through [`WorkbenchProjection`], keeping this crate independent of the
//! transport-free Agent facade.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_object::ArtifactObjectStore;
use nokv_protocol as wire;
use nokv_types::WorkbenchId;
use sha2::{Digest as _, Sha256};

use crate::{
    ClientError, CommitRecoveryRequest, CommitWorkflowError, CommitWorkflowIdentities,
    CommitWorkflowOptions, CommitWorkflowOutcome, CommitWorkflowRequest, RestoreDestinationPlan,
    RestoreManifestIdentities, RestoreManifestPublication, RestoreRecoveryRequest,
    RestoreWorkflowError, RestoreWorkflowIdentities, RestoreWorkflowOptions,
    RestoreWorkflowRequest, RouteResolver, RpcTransport, WorkspaceClient,
};

const RUN_MANIFEST_PATH: &str = "metadata/run_manifest.json";
const RESTORE_MANIFEST_PATH: &str = "metadata/restore_manifest.json";
const JSON_CONTENT_TYPE: &str = "application/json";

static WORKSPACE_ID_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
struct RestoreInvocation {
    request: WorkbenchRestoreRequest,
    identities: RestoreWorkflowIdentities,
    destination_restore_manifest_identity: wire::RestoreManifestIdentity,
    workflow_request: RestoreWorkflowRequest,
    canonical_restore_manifest: Vec<u8>,
    snapshot_id: u64,
}

/// Client-owned input for one canonical Workbench commit lifecycle.
#[derive(Clone, Debug, PartialEq)]
pub struct WorkbenchCommitRequest {
    pub workbench_id: WorkbenchId,
    pub canonical_manifest: Vec<u8>,
    pub workbench_path: String,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    pub stable_commit_id: [u8; 32],
    pub replace: bool,
}

/// Client-owned snapshot selector used by the restore lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkbenchSnapshotSelector {
    Id(u64),
    Name(String),
}

/// Client-owned input for one Workbench snapshot restore lifecycle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchRestoreRequest {
    pub source_workbench_id: WorkbenchId,
    pub source_workbench_path: String,
    pub selector: WorkbenchSnapshotSelector,
    pub destination_workbench_id: WorkbenchId,
    pub destination_workbench_path: String,
}

/// Inputs to the canonical run-manifest projection.
#[derive(Clone, Copy, Debug)]
pub struct RunManifestProjectionContext<'a> {
    pub workbench_id: &'a WorkbenchId,
    pub workbench_path: &'a str,
    pub content_digest_uri: &'a str,
    pub canonical_manifest: &'a [u8],
    pub manifest_digest_uri: &'a str,
    pub commit_identity: [u8; 32],
}

/// Inputs to the canonical restore-manifest projection.
#[derive(Clone, Copy, Debug)]
pub struct RestoreManifestProjectionContext<'a> {
    pub operation_id: [u8; 16],
    pub source_workbench_id: &'a WorkbenchId,
    pub source_path: &'a str,
    pub destination_workbench_id: &'a WorkbenchId,
    pub destination_path: &'a str,
    pub snapshot_id: u64,
}

/// Inputs used to rebuild the destination run manifest during restore.
#[derive(Clone, Copy, Debug)]
pub struct RestoredRunManifestProjectionContext<'a> {
    pub source_run_manifest: &'a [u8],
    pub destination_workbench_id: &'a WorkbenchId,
    pub destination_workbench_path: &'a str,
    pub effective_content_digest_uri: &'a str,
    pub destination_commit_identity: [u8; 32],
    pub destination_committed_at_unix_seconds: u64,
}

/// Canonical fields verified from one run-manifest projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedWorkbenchRunManifest {
    pub workbench_id: WorkbenchId,
    pub workbench_path: String,
    pub content_digest_uri: String,
    pub manifest_digest_uri: String,
    pub commit_identity: [u8; 32],
    pub canonical_manifest: Vec<u8>,
    pub canonical_envelope: Vec<u8>,
    pub envelope_digest_uri: String,
}

/// Canonical fields verified from one restore-manifest projection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedWorkbenchRestoreManifest {
    pub operation_id: [u8; 16],
    pub source_workbench_id: WorkbenchId,
    pub source_path: String,
    pub destination_workbench_id: WorkbenchId,
    pub destination_path: String,
    pub snapshot_id: u64,
    pub canonical_envelope: Vec<u8>,
    pub envelope_digest_uri: String,
}

/// Injection boundary for Agent-owned canonical presentation projections.
///
/// Implementations are pure: they receive typed immutable inputs and return
/// canonical bytes or verified fields. They must not perform SDK, metadata, or
/// object I/O.
pub trait WorkbenchProjection {
    type Error: std::error::Error + Send + Sync + 'static;

    fn build_run_manifest(
        &self,
        context: RunManifestProjectionContext<'_>,
        committed_at_unix_seconds: u64,
    ) -> Result<Vec<u8>, Self::Error>;

    fn run_manifest_projection_input_digest(
        &self,
        context: RunManifestProjectionContext<'_>,
    ) -> [u8; 32];

    fn verify_run_manifest(
        &self,
        bytes: &[u8],
    ) -> Result<VerifiedWorkbenchRunManifest, Self::Error>;

    fn build_restore_manifest(
        &self,
        context: RestoreManifestProjectionContext<'_>,
    ) -> Result<Vec<u8>, Self::Error>;

    fn verify_restore_manifest(
        &self,
        bytes: &[u8],
    ) -> Result<VerifiedWorkbenchRestoreManifest, Self::Error>;

    fn restore_effective_content_digest_uri(
        &self,
        source_content_digest_uri: &str,
        source_matches_base_commit: bool,
        materialized_member_digest: [u8; 32],
    ) -> Result<String, Self::Error>;

    fn workbench_commit_identity(
        &self,
        workbench_id: &WorkbenchId,
        content_digest_uri: &str,
        manifest_digest_uri: &str,
    ) -> [u8; 32];

    fn build_restored_run_manifest(
        &self,
        context: RestoredRunManifestProjectionContext<'_>,
    ) -> Result<Vec<u8>, Self::Error>;
}

/// Limits applied by the high-level Workbench lifecycle facade.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkbenchLifecycleOptions {
    max_artifact_bytes: usize,
}

impl WorkbenchLifecycleOptions {
    pub fn new(max_artifact_bytes: usize) -> Result<Self, ClientError> {
        if max_artifact_bytes == 0 {
            return Err(ClientError::InvalidOptions(
                "Workbench lifecycle artifact limit must be greater than zero".to_owned(),
            ));
        }
        Ok(Self { max_artifact_bytes })
    }
}

/// Typed commit result. `commit_head_generation` is the commit-head CAS
/// generation and is deliberately distinct from the run-manifest path
/// generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchCommitOutcome {
    pub commit_id: [u8; 32],
    pub commit_head_generation: u64,
    pub manifest_size_bytes: u64,
    pub envelope_digest_uri: String,
    pub tree_digest_uri: String,
    pub idempotent_replay: bool,
}

/// Typed restore result returned after atomic destination publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchRestoreOutcome {
    pub operation_id: [u8; 16],
    pub snapshot_id: u64,
    pub source_snapshot_read_version: u64,
    pub destination_workspace_revision: u64,
    pub idempotent_replay: bool,
}

/// Result of exact workbench admission. The typed summary is returned so a
/// caller can bind its following SDK operation to the observed incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkbenchAdmission {
    pub workspace: wire::WorkspaceSummary,
    pub created: bool,
}

/// High-level lifecycle failure before Agent error-envelope shaping.
#[derive(Debug)]
pub enum WorkbenchLifecycleError<ProjectionError> {
    Client(ClientError),
    RestoreWorkflow(Box<RestoreWorkflowError>),
    SnapshotLookup(ClientError),
    ProtocolInput(wire::ProtocolError),
    ProtocolMismatch(String),
    ProjectionInvalid {
        context: &'static str,
        source: ProjectionError,
    },
    Conflict(String),
    ResourceExhausted {
        artifact: &'static str,
        actual_bytes: usize,
        max_bytes: usize,
    },
}

impl<ProjectionError: fmt::Display> fmt::Display for WorkbenchLifecycleError<ProjectionError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) | Self::SnapshotLookup(error) => error.fmt(formatter),
            Self::RestoreWorkflow(error) => error.fmt(formatter),
            Self::ProtocolInput(error) => error.fmt(formatter),
            Self::ProtocolMismatch(message) | Self::Conflict(message) => {
                formatter.write_str(message)
            }
            Self::ProjectionInvalid { context, source } => {
                write!(formatter, "{context}: {source}")
            }
            Self::ResourceExhausted {
                artifact,
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "{artifact} is {actual_bytes} bytes, maximum is {max_bytes}"
            ),
        }
    }
}

impl<ProjectionError> std::error::Error for WorkbenchLifecycleError<ProjectionError>
where
    ProjectionError: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Client(error) | Self::SnapshotLookup(error) => Some(error),
            Self::RestoreWorkflow(error) => Some(error),
            Self::ProtocolInput(error) => Some(error),
            Self::ProjectionInvalid { source, .. } => Some(source),
            Self::ProtocolMismatch(_) | Self::Conflict(_) | Self::ResourceExhausted { .. } => None,
        }
    }
}

impl<ProjectionError> From<ClientError> for WorkbenchLifecycleError<ProjectionError> {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

impl<ProjectionError> From<wire::ProtocolError> for WorkbenchLifecycleError<ProjectionError> {
    fn from(error: wire::ProtocolError) -> Self {
        Self::ProtocolInput(error)
    }
}

impl<ProjectionError> From<RestoreWorkflowError> for WorkbenchLifecycleError<ProjectionError> {
    fn from(error: RestoreWorkflowError) -> Self {
        Self::RestoreWorkflow(Box::new(error))
    }
}

/// Reusable Workbench lifecycle facade over one rooted workspace client and
/// its admitted immutable-object store.
pub struct WorkbenchLifecycleFacade<'a, Transport, Resolver, Projection> {
    client: &'a WorkspaceClient<Transport, Resolver>,
    objects: &'a dyn ArtifactObjectStore,
    options: WorkbenchLifecycleOptions,
    projection: Projection,
}

impl<'a, Transport, Resolver, Projection>
    WorkbenchLifecycleFacade<'a, Transport, Resolver, Projection>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
    Projection: WorkbenchProjection,
{
    pub fn new(
        client: &'a WorkspaceClient<Transport, Resolver>,
        objects: &'a dyn ArtifactObjectStore,
        options: WorkbenchLifecycleOptions,
        projection: Projection,
    ) -> Self {
        Self {
            client,
            objects,
            options,
            projection,
        }
    }

    /// Publish or exactly replay one canonical Workbench commit.
    ///
    /// The deterministic operation is recovered before any mutable workspace
    /// or path read. Only a proven operation absence enters fresh admission.
    pub fn commit(
        &self,
        request: WorkbenchCommitRequest,
    ) -> Result<WorkbenchCommitOutcome, WorkbenchLifecycleError<Projection::Error>> {
        self.validate_commit_request(&request)?;
        let projection_input_digest = self.commit_projection_input_digest(&request);
        let commit_id = wire::CommitIdentity(request.stable_commit_id);
        let identities = CommitWorkflowIdentities::derive(self.client.root_id(), commit_id);
        let workbench = workbench_name(&request.workbench_id)?;
        let manifest_target = reserved_manifest_target(workbench.clone(), RUN_MANIFEST_PATH)?;
        let manifest_content_type = json_content_type()?;
        let content_digest = wire::DigestUri::new(request.content_digest_uri.clone())?;
        let manifest_digest = wire::DigestUri::new(request.manifest_digest_uri.clone())?;

        let recovered = self.client.commit_workflow(
            self.objects,
            CommitWorkflowOptions {
                identities,
                request: CommitWorkflowRequest::Recover(CommitRecoveryRequest {
                    operation_id: identities.operation_id,
                    workbench: workbench.clone(),
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
                self.build_commit_manifest(&request, committed_at_unix_seconds)
            },
        );
        match recovered {
            Ok(workflow) => return Ok(commit_outcome(workflow)),
            Err(CommitWorkflowError::Lookup(error))
                if client_rpc_code(&error) == Some(wire::ErrorCode::NotFound) => {}
            Err(CommitWorkflowError::Lookup(error) | CommitWorkflowError::Client(error)) => {
                return Err(WorkbenchLifecycleError::Client(error));
            }
            Err(CommitWorkflowError::BuildManifest(error)) => return Err(error),
        }

        let workspace = self.admit_workbench(&request.workbench_id)?.workspace;
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
            return Err(WorkbenchLifecycleError::Conflict(
                "workbench already has a different commit head; set replace=true".to_owned(),
            ));
        }
        if manifest_matches {
            return Err(protocol_mismatch(
                "an uncommitted head cannot already expose the requested canonical run manifest",
            ));
        }
        let run_manifest_condition = match current_manifest.as_ref() {
            None => wire::PublishCondition::CreateOnly,
            Some(projection) if request.replace => wire::PublishCondition::ReplaceOnly {
                expected_generation: projection.metadata.generation,
            },
            Some(_) => {
                return Err(WorkbenchLifecycleError::Conflict(
                    "metadata/run_manifest.json already contains a different v1 envelope"
                        .to_owned(),
                ));
            }
        };
        let mut parents = workspace.commit_head.into_iter().collect::<Vec<_>>();
        parents.sort_unstable();
        let fresh = wire::CommitRequest {
            operation_id: identities.operation_id,
            workbench: workspace.workbench,
            workspace_incarnation_id: workspace.workspace_incarnation_id,
            commit_id,
            content_digest,
            manifest_digest,
            projection_input_digest,
            tree_manifest_revision_id: identities.tree_manifest_revision_id,
            replace: request.replace,
            run_manifest_condition,
            expected_head_generation: workspace.commit_head_generation,
            parents,
            producer: None,
            lineage_projection: Vec::new(),
        };
        let workflow = self.client.commit_workflow(
            self.objects,
            CommitWorkflowOptions {
                identities,
                request: CommitWorkflowRequest::Fresh(fresh),
                manifest_target,
                manifest_content_type,
            },
            |committed_at_unix_seconds| {
                self.build_commit_manifest(&request, committed_at_unix_seconds)
            },
        );
        match workflow {
            Ok(workflow) => Ok(commit_outcome(workflow)),
            Err(CommitWorkflowError::Lookup(error) | CommitWorkflowError::Client(error)) => {
                Err(WorkbenchLifecycleError::Client(error))
            }
            Err(CommitWorkflowError::BuildManifest(error)) => Err(error),
        }
    }

    /// Admit one workbench without upsert ambiguity. An absent/create race is
    /// resolved by observing the winner's incarnation.
    pub fn admit_workbench(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<WorkbenchAdmission, WorkbenchLifecycleError<Projection::Error>> {
        if let Some(workspace) = self.optional_workspace(workbench_id)? {
            return Ok(WorkbenchAdmission {
                workspace,
                created: false,
            });
        }
        let incarnation = wire::WorkspaceIdentity(self.fresh_workspace_identity(workbench_id));
        let created = self.client.create_workspace(
            self.client.new_request_id(),
            wire::CreateWorkspaceRequest {
                workbench: workbench_name(workbench_id)?,
                workspace_incarnation_id: incarnation,
            },
        );
        let replayed = match created {
            Ok(call) => call.replayed,
            Err(error) if client_rpc_code(&error) == Some(wire::ErrorCode::AlreadyExists) => {
                return Ok(WorkbenchAdmission {
                    workspace: self.workspace(workbench_id)?,
                    created: false,
                });
            }
            Err(error) => return Err(WorkbenchLifecycleError::Client(error)),
        };
        let workspace = self.workspace(workbench_id)?;
        if workspace.workspace_incarnation_id != incarnation {
            return Err(protocol_mismatch(
                "created workbench resolved to a different incarnation",
            ));
        }
        Ok(WorkbenchAdmission {
            workspace,
            created: !replayed,
        })
    }

    /// Restore one concrete snapshot into an absent destination, or exactly
    /// replay a destination whose durable restore manifest is already visible.
    pub fn restore(
        &self,
        request: WorkbenchRestoreRequest,
    ) -> Result<WorkbenchRestoreOutcome, WorkbenchLifecycleError<Projection::Error>> {
        let destination_workbench = workbench_name(&request.destination_workbench_id)?;
        let destination = self.optional_workspace(&request.destination_workbench_id)?;
        let invocation = match destination {
            Some(destination) => {
                let projection =
                    self.read_restore_manifest(&destination, &request.destination_workbench_id)?;
                let verified = projection.verified;
                if verified.source_workbench_id != request.source_workbench_id
                    || verified.source_path != request.source_workbench_path
                    || verified.destination_workbench_id != request.destination_workbench_id
                    || verified.destination_path != request.destination_workbench_path
                    || !restore_selector_matches_durable_snapshot(
                        &request.selector,
                        verified.snapshot_id,
                    )
                {
                    return Err(WorkbenchLifecycleError::Conflict(
                        "existing destination restore manifest belongs to different provenance"
                            .to_owned(),
                    ));
                }
                let identities = RestoreWorkflowIdentities {
                    operation_id: wire::OperationIdentity(verified.operation_id),
                    destination_workspace_incarnation_id: destination.workspace_incarnation_id,
                };
                let manifest_identities = identities
                    .manifest_identities(self.client.root_id(), &verified.envelope_digest_uri);
                let destination_restore_manifest_identity =
                    restore_manifest_identity(manifest_identities);
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
                    destination_workbench: destination.workbench,
                    destination_workspace_incarnation_id: destination.workspace_incarnation_id,
                    destination_restore_manifest_identity,
                    restore_manifest: wire::RestoreManifestDescriptor {
                        body_digest: projection.metadata.descriptor.body_digest,
                        logical_size: projection.metadata.descriptor.logical_size,
                        content_type: projection.metadata.descriptor.content_type,
                    },
                };
                RestoreInvocation {
                    request: request.clone(),
                    identities,
                    destination_restore_manifest_identity,
                    workflow_request: RestoreWorkflowRequest::Recover(recovery),
                    canonical_restore_manifest: verified.canonical_envelope,
                    snapshot_id: verified.snapshot_id,
                }
            }
            None => {
                let source_workspace = self.workspace(&request.source_workbench_id)?;
                let snapshot = self.snapshot(&request.source_workbench_id, &request.selector)?;
                if snapshot.workspace_incarnation_id != source_workspace.workspace_incarnation_id {
                    return Err(protocol_mismatch(
                        "snapshot resolved to a different source workspace incarnation",
                    ));
                }
                let identities = RestoreWorkflowIdentities::derive_snapshot(
                    self.client.root_id(),
                    &source_workspace.workbench,
                    source_workspace.workspace_incarnation_id,
                    snapshot.snapshot_id,
                    &destination_workbench,
                );
                let canonical_manifest = self
                    .projection
                    .build_restore_manifest(RestoreManifestProjectionContext {
                        operation_id: identities.operation_id.0,
                        source_workbench_id: &request.source_workbench_id,
                        source_path: &request.source_workbench_path,
                        destination_workbench_id: &request.destination_workbench_id,
                        destination_path: &request.destination_workbench_path,
                        snapshot_id: snapshot.snapshot_id,
                    })
                    .map_err(|source| WorkbenchLifecycleError::ProjectionInvalid {
                        context: "facade supplied invalid restore provenance",
                        source,
                    })?;
                let verified = self
                    .projection
                    .verify_restore_manifest(&canonical_manifest)
                    .map_err(|source| WorkbenchLifecycleError::ProjectionInvalid {
                        context: "canonical restore manifest is invalid",
                        source,
                    })?;
                self.ensure_artifact_size("artifact", canonical_manifest.len())?;
                let manifest_identities = identities
                    .manifest_identities(self.client.root_id(), &verified.envelope_digest_uri);
                let destination_restore_manifest_identity =
                    restore_manifest_identity(manifest_identities);
                let prepare = wire::PrepareRestoreRequest {
                    operation_id: identities.operation_id,
                    source_workbench: source_workspace.workbench,
                    source_workspace_incarnation_id: source_workspace.workspace_incarnation_id,
                    source: wire::RestoreSource::Snapshot(wire::SnapshotSelector::Id(
                        snapshot.snapshot_id,
                    )),
                    destination_workbench: destination_workbench.clone(),
                    destination_workspace_incarnation_id: identities
                        .destination_workspace_incarnation_id,
                    destination_restore_manifest_identity,
                    restore_manifest: wire::RestoreManifestDescriptor {
                        body_digest: wire::DigestUri::new(verified.envelope_digest_uri.clone())?,
                        logical_size: canonical_manifest.len() as u64,
                        content_type: json_content_type()?,
                    },
                };
                RestoreInvocation {
                    request: request.clone(),
                    identities,
                    destination_restore_manifest_identity,
                    workflow_request: RestoreWorkflowRequest::Fresh(prepare),
                    canonical_restore_manifest: canonical_manifest,
                    snapshot_id: snapshot.snapshot_id,
                }
            }
        };

        self.drive_restore(invocation)
    }

    fn drive_restore(
        &self,
        invocation: RestoreInvocation,
    ) -> Result<WorkbenchRestoreOutcome, WorkbenchLifecycleError<Projection::Error>> {
        let root_id = self.client.root_id();
        let source_workbench_id = invocation.request.source_workbench_id;
        let source_workbench_path = invocation.request.source_workbench_path;
        let destination_workbench_id = invocation.request.destination_workbench_id;
        let destination_workbench_path = invocation.request.destination_workbench_path;
        let identities = invocation.identities;
        let workflow_request = invocation.workflow_request;
        let destination_restore_manifest_identity =
            invocation.destination_restore_manifest_identity;
        let canonical_restore_manifest = invocation.canonical_restore_manifest;
        let snapshot_id = invocation.snapshot_id;
        let max_artifact_bytes = self.options.max_artifact_bytes;
        let workflow = self
            .client
            .restore_workflow(
                self.objects,
                RestoreWorkflowOptions {
                    identities,
                    request: workflow_request,
                },
                move |preparation, source_run_manifest| {
                    build_restore_destination_plan(
                        root_id,
                        preparation,
                        source_run_manifest,
                        &source_workbench_id,
                        &source_workbench_path,
                        &destination_workbench_id,
                        &destination_workbench_path,
                        snapshot_id,
                        destination_restore_manifest_identity,
                        &canonical_restore_manifest,
                        max_artifact_bytes,
                        &self.projection,
                    )
                },
            )
            .map_err(WorkbenchLifecycleError::from)?;
        let source_snapshot_read_version =
            workflow.source_snapshot_read_version.ok_or_else(|| {
                protocol_mismatch("snapshot restore operation omitted its durable read version")
            })?;
        Ok(WorkbenchRestoreOutcome {
            operation_id: identities.operation_id.0,
            snapshot_id,
            source_snapshot_read_version,
            destination_workspace_revision: workflow.result.destination.workspace_revision,
            idempotent_replay: workflow.replayed,
        })
    }

    fn build_commit_manifest(
        &self,
        request: &WorkbenchCommitRequest,
        committed_at_unix_seconds: u64,
    ) -> Result<Vec<u8>, WorkbenchLifecycleError<Projection::Error>> {
        let bytes = self
            .projection
            .build_run_manifest(
                run_manifest_projection_context(request),
                committed_at_unix_seconds,
            )
            .map_err(|source| WorkbenchLifecycleError::ProjectionInvalid {
                context: "commit request cannot form a canonical run manifest",
                source,
            })?;
        self.ensure_artifact_size("artifact", bytes.len())?;
        Ok(bytes)
    }

    fn validate_commit_request(
        &self,
        request: &WorkbenchCommitRequest,
    ) -> Result<(), WorkbenchLifecycleError<Projection::Error>> {
        self.projection
            .build_run_manifest(run_manifest_projection_context(request), 1)
            .map(|_| ())
            .map_err(|source| WorkbenchLifecycleError::ProjectionInvalid {
                context: "commit request cannot form a canonical run manifest",
                source,
            })
    }

    fn commit_projection_input_digest(&self, request: &WorkbenchCommitRequest) -> wire::Digest {
        wire::Digest(
            self.projection
                .run_manifest_projection_input_digest(run_manifest_projection_context(request)),
        )
    }

    fn ensure_artifact_size(
        &self,
        artifact: &'static str,
        actual_bytes: usize,
    ) -> Result<(), WorkbenchLifecycleError<Projection::Error>> {
        if actual_bytes > self.options.max_artifact_bytes {
            return Err(WorkbenchLifecycleError::ResourceExhausted {
                artifact,
                actual_bytes,
                max_bytes: self.options.max_artifact_bytes,
            });
        }
        Ok(())
    }

    fn workspace(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<wire::WorkspaceSummary, WorkbenchLifecycleError<Projection::Error>> {
        self.client
            .get_workspace(wire::GetWorkspaceRequest {
                workbench: workbench_name(workbench_id)?,
            })
            .map(|call| call.value)
            .map_err(WorkbenchLifecycleError::Client)
    }

    fn optional_workspace(
        &self,
        workbench_id: &WorkbenchId,
    ) -> Result<Option<wire::WorkspaceSummary>, WorkbenchLifecycleError<Projection::Error>> {
        match self.client.get_workspace(wire::GetWorkspaceRequest {
            workbench: workbench_name(workbench_id)?,
        }) {
            Ok(call) => Ok(Some(call.value)),
            Err(error) if client_rpc_code(&error) == Some(wire::ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(WorkbenchLifecycleError::Client(error)),
        }
    }

    fn fresh_workspace_identity(&self, workbench_id: &WorkbenchId) -> [u8; 16] {
        let sequence = WORKSPACE_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut hasher = Sha256::new();
        // Frozen identity domain retained from the original CLI adapter. The
        // orchestration owner moved packages; existing identity bytes did not.
        hasher.update(b"nokv.cli.workspace-incarnation\0");
        hasher.update(self.client.root_id().0);
        hasher.update(std::process::id().to_be_bytes());
        hasher.update(sequence.to_be_bytes());
        hasher.update(now.to_be_bytes());
        hash_length_prefixed(&mut hasher, workbench_id.as_bytes());
        digest_prefix(hasher.finalize().into())
    }

    fn snapshot(
        &self,
        workbench_id: &WorkbenchId,
        selector: &WorkbenchSnapshotSelector,
    ) -> Result<wire::SnapshotResult, WorkbenchLifecycleError<Projection::Error>> {
        let wire_selector = snapshot_selector(selector)?;
        let result = self
            .client
            .get_snapshot(wire::GetSnapshotRequest {
                workbench: workbench_name(workbench_id)?,
                selector: wire_selector.clone(),
            })
            .map_err(WorkbenchLifecycleError::SnapshotLookup)?
            .value;
        if result.workbench.as_str() != workbench_id.as_str()
            || matches!(wire_selector, wire::SnapshotSelector::Id(id) if id != result.snapshot_id)
            || matches!(
                (&wire_selector, &result.alias),
                (wire::SnapshotSelector::Alias(expected), Some(actual)) if expected != actual
            )
            || matches!(
                (&wire_selector, &result.alias),
                (wire::SnapshotSelector::Alias(_), None)
            )
        {
            return Err(protocol_mismatch(
                "snapshot lookup returned a different workbench or selector",
            ));
        }
        Ok(result)
    }

    fn read_run_manifest(
        &self,
        workspace: &wire::WorkspaceSummary,
        workbench_id: &WorkbenchId,
    ) -> Result<Option<RunManifestProjection>, WorkbenchLifecycleError<Projection::Error>> {
        let target = reserved_manifest_target(workspace.workbench.clone(), RUN_MANIFEST_PATH)?;
        let Some(metadata) = self.path_metadata(target.clone())? else {
            if workspace.commit_head.is_some() {
                return Err(protocol_mismatch(
                    "committed workbench has no metadata/run_manifest.json",
                ));
            }
            return Ok(None);
        };
        self.ensure_artifact_size(
            "run manifest",
            usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX),
        )?;
        let body = self
            .client
            .read_artifact(self.objects, None, target, wire::WorkspaceReadView::Live)
            .map_err(WorkbenchLifecycleError::Client)?;
        let verified = self
            .projection
            .verify_run_manifest(&body.bytes)
            .map_err(|source| WorkbenchLifecycleError::ProjectionInvalid {
                context: "run manifest violates the v1 projection",
                source,
            })?;
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
        Ok(Some(RunManifestProjection { metadata, verified }))
    }

    fn read_restore_manifest(
        &self,
        workspace: &wire::WorkspaceSummary,
        workbench_id: &WorkbenchId,
    ) -> Result<RestoreManifestProjection, WorkbenchLifecycleError<Projection::Error>> {
        let target = reserved_manifest_target(workspace.workbench.clone(), RESTORE_MANIFEST_PATH)?;
        let Some(metadata) = self.path_metadata(target.clone())? else {
            return Err(WorkbenchLifecycleError::Conflict(
                "restore destination already exists without metadata/restore_manifest.json"
                    .to_owned(),
            ));
        };
        self.ensure_artifact_size(
            "restore manifest",
            usize::try_from(metadata.descriptor.logical_size).unwrap_or(usize::MAX),
        )?;
        let body = self
            .client
            .read_artifact(self.objects, None, target, wire::WorkspaceReadView::Live)
            .map_err(WorkbenchLifecycleError::Client)?;
        let verified = self
            .projection
            .verify_restore_manifest(&body.bytes)
            .map_err(|source| WorkbenchLifecycleError::ProjectionInvalid {
                context: "restore manifest violates the v1 projection",
                source,
            })?;
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

    fn path_metadata(
        &self,
        target: wire::WorkspacePath,
    ) -> Result<Option<wire::PathMetadata>, WorkbenchLifecycleError<Projection::Error>> {
        match self.client.get_path(wire::GetPathRequest {
            target: target.clone(),
            view: wire::WorkspaceReadView::Live,
            expected_read_version: None,
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
            Err(error) if client_rpc_code(&error) == Some(wire::ErrorCode::NotFound) => Ok(None),
            Err(error) => Err(WorkbenchLifecycleError::Client(error)),
        }
    }
}

struct RunManifestProjection {
    metadata: wire::PathMetadata,
    verified: VerifiedWorkbenchRunManifest,
}

struct RestoreManifestProjection {
    metadata: wire::PathMetadata,
    verified: VerifiedWorkbenchRestoreManifest,
}

fn run_manifest_projection_context(
    request: &WorkbenchCommitRequest,
) -> RunManifestProjectionContext<'_> {
    RunManifestProjectionContext {
        workbench_id: &request.workbench_id,
        workbench_path: &request.workbench_path,
        content_digest_uri: &request.content_digest_uri,
        canonical_manifest: &request.canonical_manifest,
        manifest_digest_uri: &request.manifest_digest_uri,
        commit_identity: request.stable_commit_id,
    }
}

fn run_manifest_matches_request(
    projection: &RunManifestProjection,
    request: &WorkbenchCommitRequest,
) -> bool {
    projection.verified.workbench_id == request.workbench_id
        && projection.verified.workbench_path == request.workbench_path
        && projection.verified.content_digest_uri == request.content_digest_uri
        && projection.verified.manifest_digest_uri == request.manifest_digest_uri
        && projection.verified.commit_identity == request.stable_commit_id
        && projection.verified.canonical_manifest == request.canonical_manifest
}

fn commit_outcome(workflow: CommitWorkflowOutcome) -> WorkbenchCommitOutcome {
    WorkbenchCommitOutcome {
        commit_id: workflow.result.commit_id.0,
        commit_head_generation: workflow.result.head_generation,
        manifest_size_bytes: workflow.manifest.descriptor.logical_size,
        envelope_digest_uri: workflow.manifest.descriptor.body_digest.as_str().to_owned(),
        tree_digest_uri: format!("sha256:{}", lowercase_hex(&workflow.result.member_digest.0)),
        idempotent_replay: workflow.replayed,
    }
}

fn restore_manifest_identity(
    identities: RestoreManifestIdentities,
) -> wire::RestoreManifestIdentity {
    wire::RestoreManifestIdentity {
        publication_operation_id: identities.publish_operation_id,
        artifact_revision_id: identities.revision_id,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_restore_destination_plan<Projection: WorkbenchProjection>(
    root_id: wire::RootIdentity,
    preparation: &wire::RestorePreparation,
    source_run_manifest: &[u8],
    source_workbench_id: &WorkbenchId,
    source_workbench_path: &str,
    destination_workbench_id: &WorkbenchId,
    destination_workbench_path: &str,
    snapshot_id: u64,
    destination_restore_manifest_identity: wire::RestoreManifestIdentity,
    canonical_restore_manifest: &[u8],
    max_artifact_bytes: usize,
    projection: &Projection,
) -> Result<RestoreDestinationPlan, ClientError> {
    let source = projection
        .verify_run_manifest(source_run_manifest)
        .map_err(|error| {
            ClientError::ResponseMismatch(format!(
                "restore-held source run manifest violates the canonical v1 projection: {error}"
            ))
        })?;
    if source.workbench_id != *source_workbench_id
        || source.workbench_path != source_workbench_path
        || source.content_digest_uri != preparation.source_commit.content_digest.as_str()
        || source.manifest_digest_uri != preparation.source_commit.manifest_digest.as_str()
        || source.commit_identity != preparation.source_commit.commit_id.0
    {
        return Err(ClientError::ResponseMismatch(
            "restore-held source run manifest differs from its durable commit binding".to_owned(),
        ));
    }

    let restore = projection
        .verify_restore_manifest(canonical_restore_manifest)
        .map_err(|error| {
            ClientError::ResponseMismatch(format!(
                "destination restore manifest violates the canonical v1 projection: {error}"
            ))
        })?;
    if restore.operation_id != preparation.operation_id.0
        || restore.source_workbench_id != *source_workbench_id
        || restore.source_path != source_workbench_path
        || restore.destination_workbench_id != *destination_workbench_id
        || restore.destination_path != destination_workbench_path
        || restore.snapshot_id != snapshot_id
        || preparation.destination_workbench.as_str() != destination_workbench_id.as_str()
    {
        return Err(ClientError::ResponseMismatch(
            "destination restore manifest differs from the durable restore provenance".to_owned(),
        ));
    }
    let expected_restore_manifest_identity = restore_manifest_identity(
        RestoreWorkflowIdentities {
            operation_id: preparation.operation_id,
            destination_workspace_incarnation_id: preparation.destination_workspace_incarnation_id,
        }
        .manifest_identities(root_id, &restore.envelope_digest_uri),
    );
    if destination_restore_manifest_identity != expected_restore_manifest_identity {
        return Err(ClientError::ResponseMismatch(
            "destination restore manifest identity differs from its operation and canonical envelope"
                .to_owned(),
        ));
    }

    let effective_content_digest = projection
        .restore_effective_content_digest_uri(
            preparation.source_commit.content_digest.as_str(),
            preparation.source_matches_base_commit,
            preparation.materialized_member_digest.0,
        )
        .map_err(|error| {
            ClientError::ResponseMismatch(format!(
                "durable restore content commitment is invalid: {error}"
            ))
        })?;
    let destination_commit_identity = projection.workbench_commit_identity(
        destination_workbench_id,
        &effective_content_digest,
        &source.manifest_digest_uri,
    );
    let destination_commit_id = wire::CommitIdentity(destination_commit_identity);
    let destination_run_manifest_identities =
        CommitWorkflowIdentities::derive(root_id, destination_commit_id);
    let destination_run_manifest_identity = wire::RestoreManifestIdentity {
        publication_operation_id: destination_run_manifest_identities.manifest_publish_operation_id,
        artifact_revision_id: destination_run_manifest_identities.tree_manifest_revision_id,
    };
    let destination_run_manifest = projection
        .build_restored_run_manifest(RestoredRunManifestProjectionContext {
            source_run_manifest,
            destination_workbench_id,
            destination_workbench_path,
            effective_content_digest_uri: &effective_content_digest,
            destination_commit_identity,
            destination_committed_at_unix_seconds: preparation
                .destination_committed_at_unix_seconds,
        })
        .map_err(|error| {
            ClientError::ResponseMismatch(format!(
                "destination run manifest projection could not be rebuilt: {error}"
            ))
        })?;
    if destination_run_manifest.len() > max_artifact_bytes
        || canonical_restore_manifest.len() > max_artifact_bytes
    {
        return Err(ClientError::InvalidOptions(format!(
            "restore-owned manifest exceeds the {max_artifact_bytes}-byte artifact limit"
        )));
    }
    let destination_run_manifest_projection_input_digest = wire::Digest(
        projection.run_manifest_projection_input_digest(RunManifestProjectionContext {
            workbench_id: destination_workbench_id,
            workbench_path: destination_workbench_path,
            content_digest_uri: &effective_content_digest,
            canonical_manifest: &source.canonical_manifest,
            manifest_digest_uri: &source.manifest_digest_uri,
            commit_identity: destination_commit_identity,
        }),
    );
    let content_type = json_content_type()?;
    let run_manifest_target =
        reserved_manifest_target(preparation.destination_workbench.clone(), RUN_MANIFEST_PATH)?;
    let restore_manifest_target = reserved_manifest_target(
        preparation.destination_workbench.clone(),
        RESTORE_MANIFEST_PATH,
    )?;

    Ok(RestoreDestinationPlan {
        binding: wire::BindRestoreDestinationRequest {
            operation_id: preparation.operation_id,
            destination_commit_id,
            effective_content_digest: wire::DigestUri::new(effective_content_digest)?,
            destination_run_manifest_projection_input_digest,
            destination_run_manifest_identity,
            destination_restore_manifest_identity,
        },
        run_manifest: RestoreManifestPublication {
            identity: destination_run_manifest_identity,
            target: run_manifest_target,
            content_type: content_type.clone(),
            bytes: destination_run_manifest,
        },
        restore_manifest: RestoreManifestPublication {
            identity: destination_restore_manifest_identity,
            target: restore_manifest_target,
            content_type,
            bytes: canonical_restore_manifest.to_vec(),
        },
    })
}

fn workbench_name(workbench_id: &WorkbenchId) -> Result<wire::WorkbenchName, wire::ProtocolError> {
    wire::WorkbenchName::new(workbench_id.as_str().to_owned())
}

fn snapshot_selector(
    selector: &WorkbenchSnapshotSelector,
) -> Result<wire::SnapshotSelector, wire::ProtocolError> {
    match selector {
        WorkbenchSnapshotSelector::Id(snapshot_id) => Ok(wire::SnapshotSelector::Id(*snapshot_id)),
        WorkbenchSnapshotSelector::Name(name) => {
            wire::SnapshotAlias::new(name.clone()).map(wire::SnapshotSelector::Alias)
        }
    }
}

/// Restore operation identity stores the point-resolved numeric snapshot id.
/// An alias is presentation input and must not be resolved again during cold
/// destination recovery: it may have expired or been repointed since Begin.
fn restore_selector_matches_durable_snapshot(
    selector: &WorkbenchSnapshotSelector,
    snapshot_id: u64,
) -> bool {
    match selector {
        WorkbenchSnapshotSelector::Id(requested) => *requested == snapshot_id,
        WorkbenchSnapshotSelector::Name(_) => true,
    }
}

fn reserved_manifest_target(
    workbench: wire::WorkbenchName,
    path: &'static str,
) -> Result<wire::WorkspacePath, wire::ProtocolError> {
    Ok(wire::WorkspacePath {
        workbench,
        path: wire::RelativePath::new(path.to_owned())?,
    })
}

fn json_content_type() -> Result<wire::ContentType, wire::ProtocolError> {
    wire::ContentType::new(JSON_CONTENT_TYPE.to_owned())
}

fn protocol_mismatch<ProjectionError>(
    message: impl Into<String>,
) -> WorkbenchLifecycleError<ProjectionError> {
    WorkbenchLifecycleError::ProtocolMismatch(message.into())
}

fn client_rpc_failure(error: &ClientError) -> Option<&wire::RpcFailure> {
    match error {
        ClientError::Rpc(failure) => Some(failure),
        ClientError::ArtifactPublishFailed { source, .. } => client_rpc_failure(source),
        ClientError::RetryExhausted { last_error, .. } => client_rpc_failure(last_error),
        _ => None,
    }
}

fn client_rpc_code(error: &ClientError) -> Option<wire::ErrorCode> {
    client_rpc_failure(error).map(|failure| failure.code)
}

fn digest_prefix(digest: [u8; 32]) -> [u8; 16] {
    digest[..16]
        .try_into()
        .expect("SHA-256 prefix has fixed width")
}

fn hash_length_prefixed(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn lowercase_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

// The end-to-end Agent projection fixtures remain in the `nokv` adapter tests;
// client-owned callback/state-machine tests live in this module below.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_options_reject_zero_artifact_limit() {
        assert!(matches!(
            WorkbenchLifecycleOptions::new(0),
            Err(ClientError::InvalidOptions(_))
        ));
        assert_eq!(
            WorkbenchLifecycleOptions::new(1).unwrap(),
            WorkbenchLifecycleOptions {
                max_artifact_bytes: 1
            }
        );
    }

    #[test]
    fn run_manifest_context_binds_every_client_owned_input() {
        let request = WorkbenchCommitRequest {
            workbench_id: WorkbenchId::new("run-42").unwrap(),
            canonical_manifest: br#"{"model":"viking"}"#.to_vec(),
            workbench_path: "/agents/test/wb/run-42".to_owned(),
            content_digest_uri: format!("sha256:{}", "11".repeat(32)),
            manifest_digest_uri: format!("sha256:{}", "22".repeat(32)),
            stable_commit_id: [0x33; 32],
            replace: true,
        };
        let context = run_manifest_projection_context(&request);
        assert_eq!(context.workbench_id, &request.workbench_id);
        assert_eq!(context.workbench_path, request.workbench_path);
        assert_eq!(context.content_digest_uri, request.content_digest_uri);
        assert_eq!(context.canonical_manifest, request.canonical_manifest);
        assert_eq!(context.manifest_digest_uri, request.manifest_digest_uri);
        assert_eq!(context.commit_identity, request.stable_commit_id);
    }

    #[test]
    fn restore_alias_recovery_converges_on_durable_numeric_snapshot() {
        assert!(restore_selector_matches_durable_snapshot(
            &WorkbenchSnapshotSelector::Name("original-alias".to_owned()),
            7,
        ));
        assert!(restore_selector_matches_durable_snapshot(
            &WorkbenchSnapshotSelector::Name("repointed-alias".to_owned()),
            7,
        ));
        assert!(restore_selector_matches_durable_snapshot(
            &WorkbenchSnapshotSelector::Id(7),
            7,
        ));
        assert!(!restore_selector_matches_durable_snapshot(
            &WorkbenchSnapshotSelector::Id(8),
            7,
        ));
    }
}
