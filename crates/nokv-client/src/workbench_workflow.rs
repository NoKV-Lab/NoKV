/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Durable Workbench commit and restore workflows.
//!
//! This module owns SDK-side phase orchestration and response validation. The
//! caller supplies storage-neutral protocol inputs and canonical manifest
//! bytes; metadata state and object publication remain behind their existing
//! protocol and object-store boundaries.

use std::fmt;

use nokv_object::ArtifactObjectStore;
use nokv_protocol::{
    sha256_digest_uri, ArtifactRevisionIdentity, BindRestoreDestinationRequest, CommitIdentity,
    CommitManifestBinding, CommitPreparation, CommitRequest, CommitResult, ContentType, Digest,
    DigestUri, ErrorCode, FinalizeRestoreRequest, GetOperationRequest, OperationIdentity,
    OperationKind, OperationResult, OperationState, OperationStatus, PrepareRestoreRequest,
    PublicationAuthority, PublishCondition, PublishResult, RestoreDestinationBinding,
    RestoreDestinationManifestBindings, RestoreManifestDescriptor, RestoreManifestIdentity,
    RestoreOperationPreparation, RestorePreparation, RestoreResult, RestoreSource, RootIdentity,
    WorkbenchName, WorkspaceIdentity, WorkspacePath,
};
use sha2::{Digest as _, Sha256};

use crate::{
    ArtifactPublishOptions, ArtifactReadOutcome, ClientCall, ClientError, RouteResolver,
    RpcTransport, WorkspaceClient,
};

const COMMIT_OPERATION_DOMAIN: &[u8] = b"nokv.cli.commit-operation\0";
const COMMIT_MANIFEST_OPERATION_DOMAIN: &[u8] = b"nokv.cli.commit-manifest-operation.v2\0";
const COMMIT_MANIFEST_REVISION_DOMAIN: &[u8] = b"nokv.cli.commit-manifest-revision.v2\0";
const RESTORE_OPERATION_DOMAIN: &[u8] = b"nokv.restore.operation.v2\0";
const RESTORE_DESTINATION_INCARNATION_DOMAIN: &[u8] =
    b"nokv.cli.restore-destination-incarnation.v2\0";
// A commit restore is a different operation from a snapshot restore of the
// same frozen state, so it gets its own domains. Keeping the snapshot domains
// untouched leaves every restore already in flight with the identity it was
// started under.
const RESTORE_COMMIT_OPERATION_DOMAIN: &[u8] = b"nokv.restore.commit-operation.v1\0";
const RESTORE_COMMIT_DESTINATION_INCARNATION_DOMAIN: &[u8] =
    b"nokv.restore.commit-destination-incarnation.v1\0";
const RESTORE_MANIFEST_OPERATION_DOMAIN: &[u8] = b"nokv.cli.restore-manifest-operation\0";
const RESTORE_MANIFEST_REVISION_DOMAIN: &[u8] = b"nokv.cli.restore-manifest-revision\0";

/// Frozen SDK identities for one Workbench commit workflow.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitWorkflowIdentities {
    pub operation_id: OperationIdentity,
    pub manifest_publish_operation_id: OperationIdentity,
    pub tree_manifest_revision_id: ArtifactRevisionIdentity,
}

impl CommitWorkflowIdentities {
    /// Derives the exact historical CLI identities. The `nokv.cli.*` domain
    /// names are retained as frozen byte contracts even though ownership now
    /// lives in the SDK.
    pub fn derive(root_id: RootIdentity, commit_id: CommitIdentity) -> Self {
        Self {
            operation_id: OperationIdentity(stable_fixed_identity(
                COMMIT_OPERATION_DOMAIN,
                root_id,
                &[&commit_id.0],
            )),
            manifest_publish_operation_id: OperationIdentity(stable_fixed_identity(
                COMMIT_MANIFEST_OPERATION_DOMAIN,
                root_id,
                &[&commit_id.0],
            )),
            tree_manifest_revision_id: ArtifactRevisionIdentity(stable_fixed_identity(
                COMMIT_MANIFEST_REVISION_DOMAIN,
                root_id,
                &[&commit_id.0],
            )),
        }
    }
}

/// Frozen identities known before a restore manifest is materialized.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreWorkflowIdentities {
    pub operation_id: OperationIdentity,
    pub destination_workspace_incarnation_id: WorkspaceIdentity,
}

impl RestoreWorkflowIdentities {
    /// Derives the v2 snapshot restore and destination identities. The
    /// operation binds both workbench names, both workspace incarnations, and
    /// the concrete numeric snapshot selector.
    pub fn derive(
        root_id: RootIdentity,
        source_workbench: &WorkbenchName,
        source_workspace_incarnation_id: WorkspaceIdentity,
        source: crate::WorkbenchRestoreSource,
        destination_workbench: &WorkbenchName,
    ) -> Self {
        match source {
            crate::WorkbenchRestoreSource::Snapshot { snapshot_id } => Self::derive_snapshot(
                root_id,
                source_workbench,
                source_workspace_incarnation_id,
                snapshot_id,
                destination_workbench,
            ),
            crate::WorkbenchRestoreSource::Commit { commit_id } => Self::derive_commit(
                root_id,
                source_workbench,
                source_workspace_incarnation_id,
                commit_id,
                destination_workbench,
            ),
        }
    }

    /// Derives the v2 snapshot restore and destination identities. The
    /// operation binds both workbench names, both workspace incarnations, and
    /// the concrete numeric snapshot selector.
    fn derive_snapshot(
        root_id: RootIdentity,
        source_workbench: &WorkbenchName,
        source_workspace_incarnation_id: WorkspaceIdentity,
        snapshot_id: u64,
        destination_workbench: &WorkbenchName,
    ) -> Self {
        let destination_workspace_incarnation_id = WorkspaceIdentity(stable_fixed_identity(
            RESTORE_DESTINATION_INCARNATION_DOMAIN,
            root_id,
            &[
                source_workbench.as_str().as_bytes(),
                &source_workspace_incarnation_id.0,
                &snapshot_id.to_be_bytes(),
                destination_workbench.as_str().as_bytes(),
            ],
        ));
        let operation_id = restore_operation_identity(
            root_id,
            source_workbench,
            source_workspace_incarnation_id,
            &snapshot_id.to_be_bytes(),
            destination_workbench,
            destination_workspace_incarnation_id,
            RESTORE_OPERATION_DOMAIN,
        );
        Self {
            operation_id,
            destination_workspace_incarnation_id,
        }
    }

    /// Derives the commit restore identities under their own domains, so a
    /// commit restore can never be mistaken for a snapshot restore of the
    /// same frozen state.
    fn derive_commit(
        root_id: RootIdentity,
        source_workbench: &WorkbenchName,
        source_workspace_incarnation_id: WorkspaceIdentity,
        commit_id: [u8; 32],
        destination_workbench: &WorkbenchName,
    ) -> Self {
        let destination_workspace_incarnation_id = WorkspaceIdentity(stable_fixed_identity(
            RESTORE_COMMIT_DESTINATION_INCARNATION_DOMAIN,
            root_id,
            &[
                source_workbench.as_str().as_bytes(),
                &source_workspace_incarnation_id.0,
                &commit_id,
                destination_workbench.as_str().as_bytes(),
            ],
        ));
        let operation_id = restore_operation_identity(
            root_id,
            source_workbench,
            source_workspace_incarnation_id,
            &commit_id,
            destination_workbench,
            destination_workspace_incarnation_id,
            RESTORE_COMMIT_OPERATION_DOMAIN,
        );
        Self {
            operation_id,
            destination_workspace_incarnation_id,
        }
    }

    pub fn manifest_identities(
        self,
        root_id: RootIdentity,
        envelope_digest_uri: &str,
    ) -> RestoreManifestIdentities {
        RestoreManifestIdentities {
            publish_operation_id: OperationIdentity(stable_fixed_identity(
                RESTORE_MANIFEST_OPERATION_DOMAIN,
                root_id,
                &[&self.operation_id.0, envelope_digest_uri.as_bytes()],
            )),
            revision_id: ArtifactRevisionIdentity(stable_fixed_identity(
                RESTORE_MANIFEST_REVISION_DOMAIN,
                root_id,
                &[&self.operation_id.0, envelope_digest_uri.as_bytes()],
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RestoreManifestIdentities {
    pub publish_operation_id: OperationIdentity,
    pub revision_id: ArtifactRevisionIdentity,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWorkflowOptions {
    pub identities: CommitWorkflowIdentities,
    pub request: CommitWorkflowRequest,
    pub manifest_target: WorkspacePath,
    pub manifest_content_type: ContentType,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommitWorkflowRequest {
    Fresh(CommitRequest),
    Recover(CommitRecoveryRequest),
}

/// Caller-known immutable inputs used to authenticate a durable commit before
/// its source incarnation, head, and manifest claim are reconstructed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitRecoveryRequest {
    pub operation_id: OperationIdentity,
    pub workbench: WorkbenchName,
    pub commit_id: CommitIdentity,
    pub content_digest: DigestUri,
    pub manifest_digest: DigestUri,
    pub projection_input_digest: Digest,
    pub tree_manifest_revision_id: ArtifactRevisionIdentity,
    pub replace: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitWorkflowOutcome {
    pub result: CommitResult,
    pub preparation: CommitPreparation,
    pub manifest: CommitManifestBinding,
    pub replayed: bool,
}

#[derive(Debug)]
pub enum CommitWorkflowError<ManifestError> {
    Lookup(ClientError),
    Client(ClientError),
    BuildManifest(ManifestError),
}

impl<ManifestError: fmt::Display> fmt::Display for CommitWorkflowError<ManifestError> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Lookup(error) => error.fmt(formatter),
            Self::Client(error) => error.fmt(formatter),
            Self::BuildManifest(error) => {
                write!(formatter, "commit manifest build failed: {error}")
            }
        }
    }
}

impl<ManifestError> std::error::Error for CommitWorkflowError<ManifestError>
where
    ManifestError: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Lookup(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::BuildManifest(error) => Some(error),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreWorkflowOptions {
    pub identities: RestoreWorkflowIdentities,
    pub request: RestoreWorkflowRequest,
}

/// One destination-owned canonical manifest supplied by the Agent projection
/// boundary after metadata has frozen the source closure and timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreManifestPublication {
    pub identity: RestoreManifestIdentity,
    pub target: WorkspacePath,
    pub content_type: ContentType,
    pub bytes: Vec<u8>,
}

/// Storage-neutral late-bind intent and the two immutable objects that realize
/// it. The SDK validates this plan against durable restore state before any
/// binding or object publication is attempted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreDestinationPlan {
    pub binding: BindRestoreDestinationRequest,
    pub run_manifest: RestoreManifestPublication,
    pub restore_manifest: RestoreManifestPublication,
}

/// Start mode for one restore workflow. `Recover` is used only when a durable
/// destination manifest already proves the operation identity but the original
/// source incarnation is no longer available from the live namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreWorkflowRequest {
    Fresh(PrepareRestoreRequest),
    Recover(RestoreRecoveryRequest),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreRecoveryRequest {
    pub source_workbench: WorkbenchName,
    pub source: RestoreSource,
    pub destination_workbench: WorkbenchName,
    pub destination_workspace_incarnation_id: WorkspaceIdentity,
    pub destination_restore_manifest_identity: RestoreManifestIdentity,
    pub restore_manifest: RestoreManifestDescriptor,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreWorkflowOutcome {
    pub result: RestoreResult,
    pub source_snapshot_read_version: Option<u64>,
    pub destination_manifests: RestoreDestinationManifestBindings,
    pub replayed: bool,
}

#[derive(Debug)]
pub enum RestoreWorkflowError {
    Lookup(ClientError),
    Prepare(ClientError),
    Bind(ClientError),
    Publish(ClientError),
    Finalize(ClientError),
    ReadSourceManifest(ClientError),
    Validation(ClientError),
}

impl RestoreWorkflowError {
    pub fn client_error(&self) -> &ClientError {
        match self {
            Self::Lookup(error)
            | Self::Prepare(error)
            | Self::Bind(error)
            | Self::Publish(error)
            | Self::Finalize(error)
            | Self::ReadSourceManifest(error)
            | Self::Validation(error) => error,
        }
    }

    pub fn into_client_error(self) -> ClientError {
        match self {
            Self::Lookup(error)
            | Self::Prepare(error)
            | Self::Bind(error)
            | Self::Publish(error)
            | Self::Finalize(error)
            | Self::ReadSourceManifest(error)
            | Self::Validation(error) => error,
        }
    }
}

impl fmt::Display for RestoreWorkflowError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.client_error().fmt(formatter)
    }
}

impl std::error::Error for RestoreWorkflowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.client_error())
    }
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// Drives one complete durable commit workflow. The manifest builder is
    /// invoked exactly once and only with the timestamp frozen by metadata.
    pub fn commit_workflow<ManifestError>(
        &self,
        store: &dyn ArtifactObjectStore,
        options: CommitWorkflowOptions,
        build_manifest: impl FnOnce(u64) -> Result<Vec<u8>, ManifestError>,
    ) -> Result<CommitWorkflowOutcome, CommitWorkflowError<ManifestError>> {
        let io = ClientWorkflowIo {
            client: self,
            store,
        };
        drive_commit_workflow(&io, options, build_manifest)
    }

    /// Drives one complete same-root restore workflow, including terminal
    /// replay validation and exact live-manifest verification.
    pub fn restore_workflow(
        &self,
        store: &dyn ArtifactObjectStore,
        options: RestoreWorkflowOptions,
        build_destination: impl FnOnce(
            &RestorePreparation,
            &[u8],
        ) -> Result<RestoreDestinationPlan, ClientError>,
    ) -> Result<RestoreWorkflowOutcome, RestoreWorkflowError> {
        let io = ClientWorkflowIo {
            client: self,
            store,
        };
        drive_restore_workflow(&io, options, build_destination)
    }
}

trait WorkflowIo {
    fn submit_commit(
        &self,
        request: CommitRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError>;

    fn get_operation(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ClientCall<OperationStatus>, ClientError>;

    fn publish_manifest(
        &self,
        options: ArtifactPublishOptions,
        bytes: &[u8],
    ) -> Result<ClientCall<PublishResult>, ClientError>;

    fn prepare_restore(
        &self,
        request: PrepareRestoreRequest,
    ) -> Result<ClientCall<RestorePreparation>, ClientError>;

    fn bind_restore_destination(
        &self,
        request: BindRestoreDestinationRequest,
    ) -> Result<ClientCall<RestorePreparation>, ClientError>;

    fn read_restore_source_run_manifest(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ArtifactReadOutcome, ClientError>;

    fn finalize_restore(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ClientCall<RestoreResult>, ClientError>;
}

struct ClientWorkflowIo<'a, Transport, Resolver> {
    client: &'a WorkspaceClient<Transport, Resolver>,
    store: &'a dyn ArtifactObjectStore,
}

impl<Transport, Resolver> WorkflowIo for ClientWorkflowIo<'_, Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    fn submit_commit(
        &self,
        request: CommitRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.client.commit(self.client.new_request_id(), request)
    }

    fn get_operation(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.client
            .get_operation(GetOperationRequest { operation_id })
    }

    fn publish_manifest(
        &self,
        options: ArtifactPublishOptions,
        bytes: &[u8],
    ) -> Result<ClientCall<PublishResult>, ClientError> {
        self.client
            .publish_artifact(self.store, options, bytes)
            .map(|outcome| outcome.publication)
    }

    fn prepare_restore(
        &self,
        request: PrepareRestoreRequest,
    ) -> Result<ClientCall<RestorePreparation>, ClientError> {
        self.client
            .prepare_restore(self.client.new_request_id(), request)
    }

    fn bind_restore_destination(
        &self,
        request: BindRestoreDestinationRequest,
    ) -> Result<ClientCall<RestorePreparation>, ClientError> {
        self.client
            .bind_restore_destination(self.client.new_request_id(), request)
    }

    fn read_restore_source_run_manifest(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ArtifactReadOutcome, ClientError> {
        self.client
            .read_restore_source_run_manifest_artifact(self.store, operation_id)
    }

    fn finalize_restore(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<ClientCall<RestoreResult>, ClientError> {
        self.client.finalize_restore(
            self.client.new_request_id(),
            FinalizeRestoreRequest { operation_id },
        )
    }
}

fn drive_commit_workflow<ManifestError>(
    io: &impl WorkflowIo,
    options: CommitWorkflowOptions,
    build_manifest: impl FnOnce(u64) -> Result<Vec<u8>, ManifestError>,
) -> Result<CommitWorkflowOutcome, CommitWorkflowError<ManifestError>> {
    validate_commit_options(&options).map_err(CommitWorkflowError::Client)?;
    let existing = match io.get_operation(options.identities.operation_id) {
        Ok(existing) => Some(existing),
        Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => match &options.request {
            CommitWorkflowRequest::Fresh(_) => None,
            CommitWorkflowRequest::Recover(_) => {
                return Err(CommitWorkflowError::Lookup(error));
            }
        },
        Err(error) => return Err(CommitWorkflowError::Lookup(error)),
    };
    let (exact_request, observed_preparation) = match existing {
        Some(existing) => {
            let preparation = commit_preparation(&existing.value, options.identities.operation_id)
                .map_err(CommitWorkflowError::Client)?;
            match &options.request {
                CommitWorkflowRequest::Fresh(request)
                    if preparation.request.as_ref() != request =>
                {
                    return Err(CommitWorkflowError::Client(ClientError::ResponseMismatch(
                        "durable commit request differs from the fresh request".to_owned(),
                    )));
                }
                CommitWorkflowRequest::Recover(request)
                    if !commit_recovery_matches(request, &preparation.request) =>
                {
                    return Err(CommitWorkflowError::Client(ClientError::ResponseMismatch(
                        "durable commit request differs from the recovery identity".to_owned(),
                    )));
                }
                _ => {}
            }
            ((*preparation.request).clone(), Some(preparation))
        }
        None => match &options.request {
            CommitWorkflowRequest::Fresh(request) => (request.clone(), None),
            CommitWorkflowRequest::Recover(_) => unreachable!("recovery absence returned above"),
        },
    };

    // A GET only recovers the durable dynamic inputs. Every success path is
    // authenticated by submitting the complete original DTO back to metadata.
    let prepared = submit_exact_commit_request_after_conflict(&exact_request, |request| {
        io.submit_commit(request)
    })
    .map_err(CommitWorkflowError::Client)?;
    let preparation = exact_commit_preparation(&prepared.value, &exact_request)
        .map_err(CommitWorkflowError::Client)?;
    if observed_preparation.as_ref().is_some_and(|observed| {
        !commit_preparation_progresses_monotonically(observed, &preparation)
    }) {
        return Err(CommitWorkflowError::Client(ClientError::ResponseMismatch(
            "commit preparation changed between lookup and exact replay".to_owned(),
        )));
    }

    match prepared.value.state {
        OperationState::Succeeded => {
            let result = terminal_commit_result(&prepared.value, &exact_request)
                .map_err(CommitWorkflowError::Client)?;
            let manifest_bytes = build_manifest(preparation.committed_at_unix_seconds)
                .map_err(CommitWorkflowError::BuildManifest)?;
            let manifest = validate_commit_manifest(
                io,
                &preparation,
                options.identities.manifest_publish_operation_id,
                &options.manifest_target,
                &options.manifest_content_type,
                &manifest_bytes,
            )
            .map_err(CommitWorkflowError::Client)?;
            return Ok(CommitWorkflowOutcome {
                result,
                preparation,
                manifest,
                replayed: true,
            });
        }
        OperationState::Running => {
            validate_running_commit(&prepared.value).map_err(CommitWorkflowError::Client)?
        }
        OperationState::Failed | OperationState::Quarantined => {
            return Err(CommitWorkflowError::Client(terminal_failure(
                &prepared.value,
                "commit preparation",
            )))
        }
        OperationState::Aborting => {
            return Err(CommitWorkflowError::Client(ClientError::ResponseMismatch(
                "commit preparation returned an aborting operation".to_owned(),
            )))
        }
    }

    let manifest_bytes = build_manifest(preparation.committed_at_unix_seconds)
        .map_err(CommitWorkflowError::BuildManifest)?;
    let publication_replayed = if preparation.manifest.is_some() {
        false
    } else {
        let publication = io
            .publish_manifest(
                ArtifactPublishOptions::new(
                    options.identities.manifest_publish_operation_id,
                    options.identities.tree_manifest_revision_id,
                    options.manifest_target.clone(),
                    exact_request.run_manifest_condition,
                    options.manifest_content_type.clone(),
                )
                .with_authority(PublicationAuthority::CommitStaging {
                    commit_operation_id: options.identities.operation_id,
                }),
                &manifest_bytes,
            )
            .map_err(CommitWorkflowError::Client)?;
        validate_publication(
            &publication.value,
            options.identities.manifest_publish_operation_id,
            &options.manifest_target,
            options.identities.tree_manifest_revision_id,
        )
        .map_err(CommitWorkflowError::Client)?;
        publication.replayed
    };

    let finalized = io
        .submit_commit(exact_request.clone())
        .map_err(CommitWorkflowError::Client)?;
    let completed_preparation = exact_commit_preparation(&finalized.value, &exact_request)
        .map_err(CommitWorkflowError::Client)?;
    if !commit_preparation_progresses_monotonically(&preparation, &completed_preparation) {
        return Err(CommitWorkflowError::Client(ClientError::ResponseMismatch(
            "commit preparation changed while the operation was running".to_owned(),
        )));
    }
    let result = terminal_commit_result(&finalized.value, &exact_request)
        .map_err(CommitWorkflowError::Client)?;
    let manifest = validate_commit_manifest(
        io,
        &completed_preparation,
        options.identities.manifest_publish_operation_id,
        &options.manifest_target,
        &options.manifest_content_type,
        &manifest_bytes,
    )
    .map_err(CommitWorkflowError::Client)?;

    Ok(CommitWorkflowOutcome {
        result,
        preparation: completed_preparation,
        manifest,
        replayed: prepared.replayed || publication_replayed || finalized.replayed,
    })
}

/// Terminal outcome for a restore whose durable status is `Succeeded`, built
/// from the receipt only. The exact preparation was already authenticated by
/// resubmitting the complete prepare DTO.
fn succeeded_restore_outcome(
    status: &OperationStatus,
    durable: &RestoreOperationPreparation,
    prepared: &RestorePreparation,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
    replayed: bool,
) -> Result<RestoreWorkflowOutcome, RestoreWorkflowError> {
    let result = terminal_restore_result(status, exact_request, options)
        .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_result(&result, prepared, exact_request, options)
        .map_err(RestoreWorkflowError::Validation)?;
    let destination_manifests = terminal_destination_manifests(
        durable,
        &result,
        exact_request,
        options.identities.operation_id,
    )
    .map_err(RestoreWorkflowError::Validation)?;
    Ok(RestoreWorkflowOutcome {
        result,
        source_snapshot_read_version: durable.source_snapshot_read_version,
        destination_manifests,
        replayed,
    })
}

/// A concurrent exact caller may complete the shared restore between this
/// caller's status check and its next construction step, at which point the
/// engine rejects further construction against the terminal row. Converge on
/// the durable `Succeeded` receipt instead of surfacing that phase conflict;
/// any other state leaves the original step error to the caller.
fn concurrently_completed_restore(
    io: &impl WorkflowIo,
    prepared: &RestorePreparation,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<Option<RestoreWorkflowOutcome>, RestoreWorkflowError> {
    let Ok(status) = io.get_operation(options.identities.operation_id) else {
        return Ok(None);
    };
    if status.value.state != OperationState::Succeeded {
        return Ok(None);
    }
    let durable = exact_restore_operation_preparation(
        &status.value,
        options.identities.operation_id,
        exact_request,
    )
    .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_seal(&durable, prepared).map_err(RestoreWorkflowError::Validation)?;
    succeeded_restore_outcome(
        &status.value,
        &durable,
        prepared,
        exact_request,
        options,
        true,
    )
    .map(Some)
}

fn drive_restore_workflow(
    io: &impl WorkflowIo,
    options: RestoreWorkflowOptions,
    build_destination: impl FnOnce(
        &RestorePreparation,
        &[u8],
    ) -> Result<RestoreDestinationPlan, ClientError>,
) -> Result<RestoreWorkflowOutcome, RestoreWorkflowError> {
    validate_restore_options(&options).map_err(RestoreWorkflowError::Validation)?;
    let mut observed_terminal_failure = None;
    let observed = match io.get_operation(options.identities.operation_id) {
        Ok(status) => {
            let preparation =
                restore_operation_preparation(&status.value, options.identities.operation_id)
                    .and_then(|preparation| {
                        validate_restore_recovery_request(&preparation.request, &options)?;
                        Ok(preparation)
                    })
                    .map_err(RestoreWorkflowError::Validation)?;
            if matches!(
                status.value.state,
                OperationState::Failed | OperationState::Quarantined
            ) {
                match (&status.value.result, &status.value.failure) {
                    (None, Some(failure)) => observed_terminal_failure = Some(failure.clone()),
                    _ => {
                        return Err(RestoreWorkflowError::Validation(
                            ClientError::ResponseMismatch(
                                "terminal restore lookup returned an invalid failure shape"
                                    .to_owned(),
                            ),
                        ));
                    }
                }
            }
            Some(preparation)
        }
        Err(error) if error.rpc_code() == Some(ErrorCode::NotFound) => match &options.request {
            RestoreWorkflowRequest::Fresh(_) => None,
            RestoreWorkflowRequest::Recover(_) => {
                return Err(RestoreWorkflowError::Lookup(ClientError::ResponseMismatch(
                    "durable restore manifest refers to a missing operation".to_owned(),
                )));
            }
        },
        Err(error) => return Err(RestoreWorkflowError::Lookup(error)),
    };
    let exact_request = match &observed {
        Some(preparation) => preparation.request.clone(),
        None => match &options.request {
            RestoreWorkflowRequest::Fresh(request) => request.clone(),
            RestoreWorkflowRequest::Recover(_) => unreachable!("recovery requires an operation"),
        },
    };

    // GET only reconstructs durable state. Every replay is authenticated by
    // resubmitting the complete exact prepare DTO before any terminal result is
    // accepted.
    let prepared = match io.prepare_restore(exact_request.clone()) {
        Ok(_) if observed_terminal_failure.is_some() => {
            return Err(RestoreWorkflowError::Validation(
                ClientError::ResponseMismatch(
                    "terminal restore replay unexpectedly returned a preparation".to_owned(),
                ),
            ));
        }
        Ok(prepared) => prepared,
        Err(error) => {
            let Some(expected) = observed_terminal_failure else {
                return Err(RestoreWorkflowError::Prepare(error));
            };
            if error.rpc_failure() != Some(&expected) {
                return Err(RestoreWorkflowError::Validation(
                    ClientError::ResponseMismatch(
                        "terminal restore replay returned a different durable failure".to_owned(),
                    ),
                ));
            }
            return Err(RestoreWorkflowError::Prepare(ClientError::Rpc(expected)));
        }
    };
    validate_restore_preparation(&prepared.value, &exact_request, &options)
        .map_err(RestoreWorkflowError::Validation)?;
    let status = io
        .get_operation(options.identities.operation_id)
        .map_err(RestoreWorkflowError::Lookup)?;
    let durable = exact_restore_operation_preparation(
        &status.value,
        options.identities.operation_id,
        &exact_request,
    )
    .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_progress(observed.as_ref(), &durable)
        .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_seal(&durable, &prepared.value).map_err(RestoreWorkflowError::Validation)?;
    match status.value.state {
        OperationState::Succeeded => {
            return succeeded_restore_outcome(
                &status.value,
                &durable,
                &prepared.value,
                &exact_request,
                &options,
                observed.is_some() || prepared.replayed,
            );
        }
        OperationState::Running => {
            validate_running_restore(&status.value).map_err(RestoreWorkflowError::Validation)?;
        }
        OperationState::Failed | OperationState::Quarantined => {
            return Err(RestoreWorkflowError::Validation(terminal_failure(
                &status.value,
                "restore operation",
            )));
        }
        OperationState::Aborting => {
            return Err(RestoreWorkflowError::Validation(
                ClientError::ResponseMismatch("restore operation is aborting".to_owned()),
            ));
        }
    }

    let source_run_manifest =
        match io.read_restore_source_run_manifest(options.identities.operation_id) {
            Ok(manifest) => manifest,
            Err(error) => {
                if let Some(outcome) =
                    concurrently_completed_restore(io, &prepared.value, &exact_request, &options)?
                {
                    return Ok(outcome);
                }
                return Err(RestoreWorkflowError::ReadSourceManifest(error));
            }
        };
    validate_source_run_manifest(&source_run_manifest, &durable)
        .map_err(RestoreWorkflowError::Validation)?;
    let plan = build_destination(&prepared.value, &source_run_manifest.bytes)
        .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_destination_plan(&plan, &prepared.value, &exact_request, &options)
        .map_err(RestoreWorkflowError::Validation)?;
    if let Some(binding) = durable.destination_binding.as_deref() {
        validate_exact_destination_binding(binding, &plan.binding)
            .map_err(RestoreWorkflowError::Validation)?;
    }

    // Bind is always resubmitted exactly, including after response loss or
    // process recovery. The metadata operation owns idempotency; the client
    // never invents replacement identities.
    let bound = match io.bind_restore_destination(plan.binding.clone()) {
        Ok(bound) => bound,
        Err(error) => {
            if let Some(outcome) =
                concurrently_completed_restore(io, &prepared.value, &exact_request, &options)?
            {
                return Ok(outcome);
            }
            return Err(RestoreWorkflowError::Bind(error));
        }
    };
    validate_restore_preparation(&bound.value, &exact_request, &options)
        .map_err(RestoreWorkflowError::Validation)?;
    validate_bound_preparation(&bound.value, &prepared.value, &plan.binding)
        .map_err(RestoreWorkflowError::Validation)?;

    let bound_status = io
        .get_operation(options.identities.operation_id)
        .map_err(RestoreWorkflowError::Lookup)?;
    let bound_durable = exact_restore_operation_preparation(
        &bound_status.value,
        options.identities.operation_id,
        &exact_request,
    )
    .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_seal(&bound_durable, &prepared.value)
        .map_err(RestoreWorkflowError::Validation)?;
    let durable_binding = bound_durable
        .destination_binding
        .as_deref()
        .ok_or_else(|| {
            RestoreWorkflowError::Validation(ClientError::ResponseMismatch(
                "restore destination bind response was not durable".to_owned(),
            ))
        })?;
    validate_exact_destination_binding(durable_binding, &plan.binding)
        .map_err(RestoreWorkflowError::Validation)?;

    if bound_status.value.state == OperationState::Succeeded {
        let result = terminal_restore_result(&bound_status.value, &exact_request, &options)
            .map_err(RestoreWorkflowError::Validation)?;
        validate_restore_result(&result, &prepared.value, &exact_request, &options)
            .map_err(RestoreWorkflowError::Validation)?;
        let destination_manifests = terminal_destination_manifests(
            &bound_durable,
            &result,
            &exact_request,
            options.identities.operation_id,
        )
        .map_err(RestoreWorkflowError::Validation)?;
        validate_restore_manifest_binding_matches_plan(&destination_manifests, &plan)
            .map_err(RestoreWorkflowError::Validation)?;
        return Ok(RestoreWorkflowOutcome {
            result,
            source_snapshot_read_version: bound_durable.source_snapshot_read_version,
            destination_manifests,
            replayed: true,
        });
    }
    validate_running_restore(&bound_status.value).map_err(RestoreWorkflowError::Validation)?;

    let run_publication = io
        .publish_manifest(
            ArtifactPublishOptions::new(
                plan.run_manifest.identity.publication_operation_id,
                plan.run_manifest.identity.artifact_revision_id,
                plan.run_manifest.target.clone(),
                PublishCondition::CreateOnly,
                plan.run_manifest.content_type.clone(),
            )
            .with_authority(PublicationAuthority::RestoreStaging {
                restore_operation_id: options.identities.operation_id,
            }),
            &plan.run_manifest.bytes,
        )
        .map_err(RestoreWorkflowError::Publish)?;
    validate_restore_publication(&run_publication.value, &plan.run_manifest)
        .map_err(RestoreWorkflowError::Validation)?;

    let restore_publication = io
        .publish_manifest(
            ArtifactPublishOptions::new(
                plan.restore_manifest.identity.publication_operation_id,
                plan.restore_manifest.identity.artifact_revision_id,
                plan.restore_manifest.target.clone(),
                PublishCondition::CreateOnly,
                plan.restore_manifest.content_type.clone(),
            )
            .with_authority(PublicationAuthority::RestoreStaging {
                restore_operation_id: options.identities.operation_id,
            }),
            &plan.restore_manifest.bytes,
        )
        .map_err(RestoreWorkflowError::Publish)?;
    validate_restore_publication(&restore_publication.value, &plan.restore_manifest)
        .map_err(RestoreWorkflowError::Validation)?;

    let finalized = io
        .finalize_restore(options.identities.operation_id)
        .map_err(RestoreWorkflowError::Finalize)?;
    validate_restore_result(&finalized.value, &prepared.value, &exact_request, &options)
        .map_err(RestoreWorkflowError::Validation)?;

    // Re-read the terminal receipt rather than deriving it from a later live
    // head. This also authenticates the final two-manifest binding after a
    // finalize response-loss replay.
    let terminal = io
        .get_operation(options.identities.operation_id)
        .map_err(RestoreWorkflowError::Lookup)?;
    let terminal_result = terminal_restore_result(&terminal.value, &exact_request, &options)
        .map_err(RestoreWorkflowError::Validation)?;
    if terminal_result != finalized.value {
        return Err(RestoreWorkflowError::Validation(
            ClientError::ResponseMismatch(
                "finalize response differs from the durable terminal restore receipt".to_owned(),
            ),
        ));
    }
    let terminal_preparation = exact_restore_operation_preparation(
        &terminal.value,
        options.identities.operation_id,
        &exact_request,
    )
    .map_err(RestoreWorkflowError::Validation)?;
    let destination_manifests = terminal_destination_manifests(
        &terminal_preparation,
        &terminal_result,
        &exact_request,
        options.identities.operation_id,
    )
    .map_err(RestoreWorkflowError::Validation)?;
    validate_restore_manifest_binding_matches_plan(&destination_manifests, &plan)
        .map_err(RestoreWorkflowError::Validation)?;
    Ok(RestoreWorkflowOutcome {
        result: terminal_result,
        source_snapshot_read_version: terminal_preparation.source_snapshot_read_version,
        destination_manifests,
        replayed: prepared.replayed
            || bound.replayed
            || run_publication.replayed
            || restore_publication.replayed
            || finalized.replayed
            || terminal.replayed,
    })
}

fn validate_commit_options(options: &CommitWorkflowOptions) -> Result<(), ClientError> {
    let (operation_id, tree_revision_id, workbench) = match &options.request {
        CommitWorkflowRequest::Fresh(request) => (
            request.operation_id,
            request.tree_manifest_revision_id,
            &request.workbench,
        ),
        CommitWorkflowRequest::Recover(request) => (
            request.operation_id,
            request.tree_manifest_revision_id,
            &request.workbench,
        ),
    };
    if operation_id != options.identities.operation_id
        || tree_revision_id != options.identities.tree_manifest_revision_id
        || &options.manifest_target.workbench != workbench
        || options.manifest_target.path.as_str() != "metadata/run_manifest.json"
        || options.manifest_content_type.as_str() != "application/json"
    {
        return Err(ClientError::InvalidOptions(
            "commit workflow identities and manifest target must match the immutable request"
                .to_owned(),
        ));
    }
    Ok(())
}

fn commit_preparation(
    status: &OperationStatus,
    operation_id: OperationIdentity,
) -> Result<CommitPreparation, ClientError> {
    if status.token.operation_id != operation_id || status.kind != OperationKind::Commit {
        return Err(ClientError::ResponseMismatch(
            "commit operation status does not match its requested identity and kind".to_owned(),
        ));
    }
    let preparation = status
        .commit_preparation
        .as_deref()
        .cloned()
        .ok_or_else(|| {
            ClientError::ResponseMismatch(
                "commit operation status omitted its durable preparation".to_owned(),
            )
        })?;
    if preparation.request.operation_id != operation_id {
        return Err(ClientError::ResponseMismatch(
            "commit preparation carries a different operation identity".to_owned(),
        ));
    }
    Ok(preparation)
}

fn exact_commit_preparation(
    status: &OperationStatus,
    request: &CommitRequest,
) -> Result<CommitPreparation, ClientError> {
    let preparation = commit_preparation(status, request.operation_id)?;
    if preparation.request.as_ref() != request {
        return Err(ClientError::ResponseMismatch(
            "commit preparation does not match the exact submitted request".to_owned(),
        ));
    }
    Ok(preparation)
}

fn commit_recovery_matches(recovery: &CommitRecoveryRequest, request: &CommitRequest) -> bool {
    recovery.operation_id == request.operation_id
        && recovery.workbench == request.workbench
        && recovery.commit_id == request.commit_id
        && recovery.content_digest == request.content_digest
        && recovery.manifest_digest == request.manifest_digest
        && recovery.projection_input_digest == request.projection_input_digest
        && recovery.tree_manifest_revision_id == request.tree_manifest_revision_id
        && recovery.replace == request.replace
}

fn commit_preparation_progresses_monotonically(
    before: &CommitPreparation,
    after: &CommitPreparation,
) -> bool {
    before.request == after.request
        && before.committed_at_unix_seconds == after.committed_at_unix_seconds
        && (before.manifest.is_none() || before.manifest == after.manifest)
}

fn validate_commit_manifest(
    io: &impl WorkflowIo,
    preparation: &CommitPreparation,
    publish_operation_id: OperationIdentity,
    target: &WorkspacePath,
    content_type: &ContentType,
    bytes: &[u8],
) -> Result<CommitManifestBinding, ClientError> {
    let binding = preparation.manifest.as_ref().ok_or_else(|| {
        ClientError::ResponseMismatch(
            "succeeded commit omitted its immutable run-manifest binding".to_owned(),
        )
    })?;
    let expected_size = u64::try_from(bytes.len()).map_err(|_| {
        ClientError::ResponseMismatch("commit run manifest length exceeds u64".to_owned())
    })?;
    let expected_digest: [u8; 32] = Sha256::digest(bytes).into();
    let expected_digest = sha256_digest_uri(Digest(expected_digest));
    if binding.workspace_incarnation_id != preparation.request.workspace_incarnation_id
        || binding.artifact_revision_id != preparation.request.tree_manifest_revision_id
        || binding.descriptor.logical_size != expected_size
        || binding.descriptor.body_digest != expected_digest
        || &binding.descriptor.content_type != content_type
        || binding.descriptor.producer.is_some()
        || binding.descriptor.manifest_identity.is_some()
        || !binding.descriptor.index_fields.is_empty()
    {
        return Err(ClientError::ResponseMismatch(
            "commit-owned run-manifest binding does not match the exact canonical envelope"
                .to_owned(),
        ));
    }

    let publication = io.get_operation(publish_operation_id)?;
    let published = match (
        publication.value.token.operation_id,
        publication.value.kind,
        publication.value.state,
        publication.value.result.as_ref(),
        publication.value.failure.as_ref(),
    ) {
        (
            operation_id,
            OperationKind::ArtifactPublish,
            OperationState::Succeeded,
            Some(OperationResult::ArtifactPublish(result)),
            None,
        ) if operation_id == publish_operation_id => result,
        _ => {
            return Err(ClientError::ResponseMismatch(
                "commit manifest publication operation is not the exact durable success".to_owned(),
            ));
        }
    };
    if published.operation_id != publish_operation_id
        || &published.target != target
        || published.artifact_revision_id != binding.artifact_revision_id
        || published.logical_size != binding.descriptor.logical_size
        || published.body_digest != binding.descriptor.body_digest
    {
        return Err(ClientError::ResponseMismatch(
            "commit manifest publication result differs from its immutable binding".to_owned(),
        ));
    }
    Ok(binding.clone())
}

fn validate_running_commit(status: &OperationStatus) -> Result<(), ClientError> {
    if status.state != OperationState::Running
        || status.result.is_some()
        || status.failure.is_some()
    {
        return Err(ClientError::ResponseMismatch(
            "commit preparation did not return a running build operation".to_owned(),
        ));
    }
    Ok(())
}

fn terminal_commit_result(
    status: &OperationStatus,
    request: &CommitRequest,
) -> Result<CommitResult, ClientError> {
    let result = match (&status.state, &status.result, &status.failure) {
        (OperationState::Succeeded, Some(OperationResult::Commit(result)), None) => result.clone(),
        (OperationState::Failed | OperationState::Quarantined, None, Some(failure)) => {
            return Err(ClientError::Rpc(failure.clone()))
        }
        _ => {
            return Err(ClientError::ResponseMismatch(
                "commit did not return a terminal commit result".to_owned(),
            ))
        }
    };
    if result.operation_id != request.operation_id
        || result.commit_id != request.commit_id
        || result.workbench != request.workbench
    {
        return Err(ClientError::ResponseMismatch(
            "commit result does not match its immutable request".to_owned(),
        ));
    }
    Ok(result)
}

fn validate_restore_options(options: &RestoreWorkflowOptions) -> Result<(), ClientError> {
    let destination_incarnation = match &options.request {
        RestoreWorkflowRequest::Fresh(request) => {
            if request.operation_id != options.identities.operation_id {
                return Err(ClientError::InvalidOptions(
                    "restore workflow operation identity does not match its preparation request"
                        .to_owned(),
                ));
            }
            request.destination_workspace_incarnation_id
        }
        RestoreWorkflowRequest::Recover(request) => {
            if matches!(
                request.source,
                RestoreSource::Snapshot(nokv_protocol::SnapshotSelector::Alias(_))
            ) {
                return Err(ClientError::InvalidOptions(
                    "restore recovery requires a concrete durable source selector".to_owned(),
                ));
            }
            request.destination_workspace_incarnation_id
        }
    };
    if destination_incarnation != options.identities.destination_workspace_incarnation_id {
        return Err(ClientError::InvalidOptions(
            "restore workflow destination incarnation does not match its deterministic identity"
                .to_owned(),
        ));
    }
    Ok(())
}

fn restore_operation_preparation(
    status: &OperationStatus,
    operation_id: OperationIdentity,
) -> Result<RestoreOperationPreparation, ClientError> {
    if status.token.operation_id != operation_id || status.kind != OperationKind::Restore {
        return Err(ClientError::ResponseMismatch(
            "restore operation status does not match its requested identity and kind".to_owned(),
        ));
    }
    status
        .restore_preparation
        .as_deref()
        .cloned()
        .ok_or_else(|| {
            ClientError::ResponseMismatch(
                "restore operation status omitted its durable preparation".to_owned(),
            )
        })
}

fn exact_restore_operation_preparation(
    status: &OperationStatus,
    operation_id: OperationIdentity,
    exact_request: &PrepareRestoreRequest,
) -> Result<RestoreOperationPreparation, ClientError> {
    let preparation = restore_operation_preparation(status, operation_id)?;
    if &preparation.request != exact_request {
        return Err(ClientError::ResponseMismatch(
            "restore operation preparation does not match the exact submitted request".to_owned(),
        ));
    }
    Ok(preparation)
}

fn validate_restore_recovery_request(
    durable: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<(), ClientError> {
    let matches = match &options.request {
        RestoreWorkflowRequest::Fresh(request) => durable == request,
        RestoreWorkflowRequest::Recover(request) => {
            durable.source_workbench == request.source_workbench
                && durable.source == request.source
                && durable.destination_workbench == request.destination_workbench
                && durable.destination_workspace_incarnation_id
                    == request.destination_workspace_incarnation_id
                && durable.destination_restore_manifest_identity
                    == request.destination_restore_manifest_identity
                && durable.restore_manifest == request.restore_manifest
        }
    };
    if !matches {
        return Err(ClientError::ResponseMismatch(
            "durable restore preparation does not match the requested provenance".to_owned(),
        ));
    }
    Ok(())
}

fn validate_running_restore(status: &OperationStatus) -> Result<(), ClientError> {
    if status.state != OperationState::Running
        || status.result.is_some()
        || status.failure.is_some()
    {
        return Err(ClientError::ResponseMismatch(
            "running restore status has an invalid operation shape".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_preparation(
    preparation: &RestorePreparation,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<(), ClientError> {
    if preparation.operation_id != options.identities.operation_id
        || preparation.operation_id != exact_request.operation_id
        || preparation.destination_workbench != exact_request.destination_workbench
        || preparation.destination_workspace_incarnation_id
            != options.identities.destination_workspace_incarnation_id
    {
        return Err(ClientError::ResponseMismatch(
            "restore preparation returned a different deterministic identity".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_progress(
    previous: Option<&RestoreOperationPreparation>,
    current: &RestoreOperationPreparation,
) -> Result<(), ClientError> {
    let Some(previous) = previous else {
        return Ok(());
    };
    if previous.request != current.request
        || previous.source_snapshot_read_version != current.source_snapshot_read_version
        || previous.source_commit != current.source_commit
        || previous.destination_committed_at_unix_seconds
            != current.destination_committed_at_unix_seconds
    {
        return Err(ClientError::ResponseMismatch(
            "restore immutable preparation changed during exact replay".to_owned(),
        ));
    }
    let previous_seal = (
        previous.source_member_count,
        previous.source_member_digest,
        previous.materialized_member_count,
        previous.materialized_member_digest,
        previous.source_matches_base_commit,
    );
    let current_seal = (
        current.source_member_count,
        current.source_member_digest,
        current.materialized_member_count,
        current.materialized_member_digest,
        current.source_matches_base_commit,
    );
    if previous.source_member_count.is_some() && previous_seal != current_seal {
        return Err(ClientError::ResponseMismatch(
            "restore source closure changed during exact replay".to_owned(),
        ));
    }
    if let Some(previous_binding) = previous.destination_binding.as_deref() {
        let current_binding = current.destination_binding.as_deref().ok_or_else(|| {
            ClientError::ResponseMismatch(
                "restore destination binding disappeared during exact replay".to_owned(),
            )
        })?;
        validate_destination_binding_progress(previous_binding, current_binding)?;
    }
    Ok(())
}

fn validate_destination_binding_progress(
    previous: &RestoreDestinationBinding,
    current: &RestoreDestinationBinding,
) -> Result<(), ClientError> {
    if previous.destination_commit_id != current.destination_commit_id
        || previous.effective_content_digest != current.effective_content_digest
        || previous.destination_run_manifest_projection_input_digest
            != current.destination_run_manifest_projection_input_digest
        || previous.destination_run_manifest_identity != current.destination_run_manifest_identity
        || previous.destination_restore_manifest_identity
            != current.destination_restore_manifest_identity
        || previous
            .destination_manifests
            .as_ref()
            .is_some_and(|manifests| current.destination_manifests.as_ref() != Some(manifests))
    {
        return Err(ClientError::ResponseMismatch(
            "restore destination binding changed during exact replay".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_seal(
    durable: &RestoreOperationPreparation,
    preparation: &RestorePreparation,
) -> Result<(), ClientError> {
    if durable.source_commit != preparation.source_commit
        || durable.destination_committed_at_unix_seconds
            != preparation.destination_committed_at_unix_seconds
        || durable.source_member_count != Some(preparation.source_member_count)
        || durable.source_member_digest != Some(preparation.source_member_digest)
        || durable.materialized_member_count != Some(preparation.materialized_member_count)
        || durable.materialized_member_digest != Some(preparation.materialized_member_digest)
        || durable.source_matches_base_commit != Some(preparation.source_matches_base_commit)
    {
        return Err(ClientError::ResponseMismatch(
            "restore raw/materialized source seals do not match the prepare response".to_owned(),
        ));
    }
    match (
        preparation.destination_binding.as_deref(),
        durable.destination_binding.as_deref(),
    ) {
        (Some(compact), Some(exact)) => validate_destination_binding_progress(compact, exact)?,
        (None, _) => {}
        (Some(_), None) => {
            return Err(ClientError::ResponseMismatch(
                "restore prepare response binding is absent from durable state".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_bound_preparation(
    bound: &RestorePreparation,
    initial: &RestorePreparation,
    request: &BindRestoreDestinationRequest,
) -> Result<(), ClientError> {
    if bound.operation_id != initial.operation_id
        || bound.destination_workbench != initial.destination_workbench
        || bound.destination_workspace_incarnation_id
            != initial.destination_workspace_incarnation_id
        || bound.source_commit != initial.source_commit
        || bound.destination_committed_at_unix_seconds
            != initial.destination_committed_at_unix_seconds
        || bound.source_member_count != initial.source_member_count
        || bound.source_member_digest != initial.source_member_digest
        || bound.materialized_member_count != initial.materialized_member_count
        || bound.materialized_member_digest != initial.materialized_member_digest
        || bound.source_matches_base_commit != initial.source_matches_base_commit
    {
        return Err(ClientError::ResponseMismatch(
            "destination bind changed the frozen restore preparation".to_owned(),
        ));
    }
    let binding = bound.destination_binding.as_deref().ok_or_else(|| {
        ClientError::ResponseMismatch(
            "destination bind response omitted its exact durable binding".to_owned(),
        )
    })?;
    validate_exact_destination_binding(binding, request)
}

fn validate_source_run_manifest(
    source: &ArtifactReadOutcome,
    durable: &RestoreOperationPreparation,
) -> Result<(), ClientError> {
    if source.metadata.path.workbench != durable.request.source_workbench
        || source.metadata.path.path.as_str() != "metadata/run_manifest.json"
        || source.metadata.workspace_incarnation_id
            != durable.request.source_workspace_incarnation_id
        || source.metadata.artifact_revision_id != durable.source_commit.tree_manifest_revision_id
        || source.metadata.descriptor.content_type.as_str() != "application/json"
        || source.metadata.descriptor.logical_size
            != u64::try_from(source.bytes.len()).unwrap_or(u64::MAX)
    {
        return Err(ClientError::ResponseMismatch(
            "restore-held source run manifest does not match its exact source commit".to_owned(),
        ));
    }
    let digest: [u8; 32] = Sha256::digest(&source.bytes).into();
    if sha256_digest_uri(Digest(digest)) != source.metadata.descriptor.body_digest {
        return Err(ClientError::ArtifactIntegrity(
            "restore-held source run manifest body digest mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_destination_plan(
    plan: &RestoreDestinationPlan,
    preparation: &RestorePreparation,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<(), ClientError> {
    if plan.binding.operation_id != options.identities.operation_id
        || plan.binding.operation_id != exact_request.operation_id
        || plan.binding.destination_restore_manifest_identity
            != exact_request.destination_restore_manifest_identity
        || plan.binding.destination_run_manifest_identity != plan.run_manifest.identity
        || plan.binding.destination_restore_manifest_identity != plan.restore_manifest.identity
    {
        return Err(ClientError::InvalidOptions(
            "restore destination plan identities do not match the durable prepare request"
                .to_owned(),
        ));
    }
    if plan.binding.destination_commit_id == preparation.source_commit.commit_id
        || preparation.source_matches_base_commit
            != (plan.binding.effective_content_digest == preparation.source_commit.content_digest)
        || plan
            .binding
            .destination_run_manifest_projection_input_digest
            == Digest([0; 32])
    {
        return Err(ClientError::InvalidOptions(
            "restore destination commit or effective content binding is invalid".to_owned(),
        ));
    }
    if plan.run_manifest.identity == plan.restore_manifest.identity
        || plan.run_manifest.identity.publication_operation_id
            == plan.restore_manifest.identity.publication_operation_id
        || plan.run_manifest.identity.artifact_revision_id
            == plan.restore_manifest.identity.artifact_revision_id
        || plan.run_manifest.identity.publication_operation_id == exact_request.operation_id
        || plan.restore_manifest.identity.publication_operation_id == exact_request.operation_id
        || plan.run_manifest.target == plan.restore_manifest.target
        || plan.run_manifest.target.workbench != exact_request.destination_workbench
        || plan.restore_manifest.target.workbench != exact_request.destination_workbench
        || plan.run_manifest.target.path.as_str() != "metadata/run_manifest.json"
        || plan.restore_manifest.target.path.as_str() != "metadata/restore_manifest.json"
    {
        return Err(ClientError::InvalidOptions(
            "restore destination manifests require distinct destination-owned identities and canonical targets"
                .to_owned(),
        ));
    }
    validate_manifest_publication(&plan.run_manifest, None)?;
    validate_manifest_publication(
        &plan.restore_manifest,
        Some(&exact_request.restore_manifest),
    )?;
    Ok(())
}

fn validate_manifest_publication(
    publication: &RestoreManifestPublication,
    descriptor: Option<&RestoreManifestDescriptor>,
) -> Result<(), ClientError> {
    if publication.content_type.as_str() != "application/json" || publication.bytes.is_empty() {
        return Err(ClientError::InvalidOptions(
            "restore-owned manifests must be non-empty canonical JSON".to_owned(),
        ));
    }
    let logical_size = u64::try_from(publication.bytes.len()).unwrap_or(u64::MAX);
    let digest: [u8; 32] = Sha256::digest(&publication.bytes).into();
    let body_digest = sha256_digest_uri(Digest(digest));
    if descriptor.is_some_and(|descriptor| {
        descriptor.logical_size != logical_size
            || descriptor.body_digest != body_digest
            || descriptor.content_type != publication.content_type
    }) {
        return Err(ClientError::InvalidOptions(
            "restore manifest bytes do not match the durable prepare descriptor".to_owned(),
        ));
    }
    Ok(())
}

fn validate_exact_destination_binding(
    actual: &RestoreDestinationBinding,
    expected: &BindRestoreDestinationRequest,
) -> Result<(), ClientError> {
    if actual.destination_commit_id != expected.destination_commit_id
        || actual.effective_content_digest != expected.effective_content_digest
        || actual.destination_run_manifest_projection_input_digest
            != expected.destination_run_manifest_projection_input_digest
        || actual.destination_run_manifest_identity != expected.destination_run_manifest_identity
        || actual.destination_restore_manifest_identity
            != expected.destination_restore_manifest_identity
    {
        return Err(ClientError::ResponseMismatch(
            "durable restore destination binding differs from the exact late-bind intent"
                .to_owned(),
        ));
    }
    Ok(())
}

fn terminal_restore_result(
    status: &OperationStatus,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<RestoreResult, ClientError> {
    if status.token.operation_id != options.identities.operation_id
        || status.kind != OperationKind::Restore
    {
        return Err(ClientError::ResponseMismatch(
            "restore operation status does not match its requested identity and kind".to_owned(),
        ));
    }
    let result = match (&status.state, &status.result, &status.failure) {
        (OperationState::Succeeded, Some(OperationResult::Restore(result)), None) => result.clone(),
        (OperationState::Failed | OperationState::Quarantined, None, Some(failure)) => {
            return Err(ClientError::Rpc(failure.clone()))
        }
        _ => {
            return Err(ClientError::ResponseMismatch(
                "restore operation returned an invalid terminal shape".to_owned(),
            ))
        }
    };
    validate_restore_destination(&result, exact_request, options)?;
    Ok(result)
}

fn validate_restore_result(
    result: &RestoreResult,
    preparation: &RestorePreparation,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<(), ClientError> {
    validate_restore_destination(result, exact_request, options)?;
    if result.member_count != preparation.materialized_member_count
        || result.member_digest != preparation.materialized_member_digest
    {
        return Err(ClientError::ResponseMismatch(
            "finalized restore does not match its sealed preparation".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_destination(
    result: &RestoreResult,
    exact_request: &PrepareRestoreRequest,
    options: &RestoreWorkflowOptions,
) -> Result<(), ClientError> {
    if result.operation_id != options.identities.operation_id
        || result.destination.workbench != exact_request.destination_workbench
        || result.destination.workspace_incarnation_id
            != options.identities.destination_workspace_incarnation_id
        || result.destination.commit_head_generation != Some(1)
    {
        return Err(ClientError::ResponseMismatch(
            "restore result does not match its requested destination".to_owned(),
        ));
    }
    Ok(())
}

fn validate_publication(
    result: &PublishResult,
    operation_id: OperationIdentity,
    target: &WorkspacePath,
    revision_id: ArtifactRevisionIdentity,
) -> Result<(), ClientError> {
    if result.operation_id != operation_id
        || &result.target != target
        || result.artifact_revision_id != revision_id
    {
        return Err(ClientError::ResponseMismatch(
            "manifest publication returned a different immutable identity".to_owned(),
        ));
    }
    Ok(())
}

fn validate_restore_publication(
    result: &PublishResult,
    expected: &RestoreManifestPublication,
) -> Result<(), ClientError> {
    let digest: [u8; 32] = Sha256::digest(&expected.bytes).into();
    if result.operation_id != expected.identity.publication_operation_id
        || result.target != expected.target
        || result.artifact_revision_id != expected.identity.artifact_revision_id
        || result.logical_size != u64::try_from(expected.bytes.len()).unwrap_or(u64::MAX)
        || result.body_digest != sha256_digest_uri(Digest(digest))
    {
        return Err(ClientError::ResponseMismatch(
            "staged manifest publication returned a different immutable identity".to_owned(),
        ));
    }
    Ok(())
}

fn terminal_destination_manifests(
    durable: &RestoreOperationPreparation,
    result: &RestoreResult,
    exact_request: &PrepareRestoreRequest,
    operation_id: OperationIdentity,
) -> Result<RestoreDestinationManifestBindings, ClientError> {
    let binding = durable.destination_binding.as_deref().ok_or_else(|| {
        ClientError::ResponseMismatch(
            "terminal restore omitted its destination commit binding".to_owned(),
        )
    })?;
    let manifests = binding.destination_manifests.as_ref().ok_or_else(|| {
        ClientError::ResponseMismatch(
            "terminal restore omitted its two destination manifest bindings".to_owned(),
        )
    })?;
    if result.destination.commit_head != Some(binding.destination_commit_id)
        || binding.destination_restore_manifest_identity
            != exact_request.destination_restore_manifest_identity
        || manifests.run_manifest.publication_operation_id
            != binding
                .destination_run_manifest_identity
                .publication_operation_id
        || manifests.run_manifest.artifact_revision_id
            != binding
                .destination_run_manifest_identity
                .artifact_revision_id
        || manifests.restore_manifest.publication_operation_id
            != binding
                .destination_restore_manifest_identity
                .publication_operation_id
        || manifests.restore_manifest.artifact_revision_id
            != binding
                .destination_restore_manifest_identity
                .artifact_revision_id
        || manifests.run_manifest.workspace_incarnation_id
            != exact_request.destination_workspace_incarnation_id
        || manifests.restore_manifest.workspace_incarnation_id
            != exact_request.destination_workspace_incarnation_id
        || manifests.restore_manifest.descriptor.body_digest
            != exact_request.restore_manifest.body_digest
        || manifests.restore_manifest.descriptor.logical_size
            != exact_request.restore_manifest.logical_size
        || manifests.restore_manifest.descriptor.content_type
            != exact_request.restore_manifest.content_type
        || binding
            .destination_run_manifest_identity
            .publication_operation_id
            == operation_id
        || binding
            .destination_restore_manifest_identity
            .publication_operation_id
            == operation_id
    {
        return Err(ClientError::ResponseMismatch(
            "terminal restore receipt and destination manifest bindings disagree".to_owned(),
        ));
    }
    Ok(manifests.clone())
}

fn validate_restore_manifest_binding_matches_plan(
    manifests: &RestoreDestinationManifestBindings,
    plan: &RestoreDestinationPlan,
) -> Result<(), ClientError> {
    let run_digest: [u8; 32] = Sha256::digest(&plan.run_manifest.bytes).into();
    let restore_digest: [u8; 32] = Sha256::digest(&plan.restore_manifest.bytes).into();
    if manifests.run_manifest.descriptor.body_digest != sha256_digest_uri(Digest(run_digest))
        || manifests.run_manifest.descriptor.logical_size
            != u64::try_from(plan.run_manifest.bytes.len()).unwrap_or(u64::MAX)
        || manifests.run_manifest.descriptor.content_type != plan.run_manifest.content_type
        || manifests.restore_manifest.descriptor.body_digest
            != sha256_digest_uri(Digest(restore_digest))
        || manifests.restore_manifest.descriptor.logical_size
            != u64::try_from(plan.restore_manifest.bytes.len()).unwrap_or(u64::MAX)
        || manifests.restore_manifest.descriptor.content_type != plan.restore_manifest.content_type
    {
        return Err(ClientError::ResponseMismatch(
            "terminal restore manifest descriptors differ from the exact published plan".to_owned(),
        ));
    }
    Ok(())
}

fn terminal_failure(status: &OperationStatus, context: &str) -> ClientError {
    match (&status.result, &status.failure) {
        (None, Some(failure)) => ClientError::Rpc(failure.clone()),
        _ => ClientError::ResponseMismatch(format!(
            "{context} returned an invalid failed operation shape"
        )),
    }
}

fn submit_exact_commit_request_after_conflict<T>(
    request: &CommitRequest,
    mut submit: impl FnMut(CommitRequest) -> Result<T, ClientError>,
) -> Result<T, ClientError> {
    match submit(request.clone()) {
        Err(error) if error.rpc_code() == Some(ErrorCode::Conflict) => submit(request.clone()),
        outcome => outcome,
    }
}

fn restore_operation_identity(
    root_id: RootIdentity,
    source_workbench: &WorkbenchName,
    source_incarnation: WorkspaceIdentity,
    source_bytes: &[u8],
    destination: &WorkbenchName,
    destination_incarnation: WorkspaceIdentity,
    domain: &[u8],
) -> OperationIdentity {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(root_id.0);
    hash_len32(&mut hasher, source_workbench.as_str().as_bytes());
    hasher.update(source_incarnation.0);
    hasher.update([1]);
    hasher.update(source_bytes);
    hash_len32(&mut hasher, destination.as_str().as_bytes());
    hasher.update(destination_incarnation.0);
    let digest: [u8; 32] = hasher.finalize().into();
    OperationIdentity(digest_prefix(digest))
}

fn hash_len32(hasher: &mut Sha256, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("validated protocol string length fits u32");
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
}

fn stable_fixed_identity(domain: &[u8], root_id: RootIdentity, pieces: &[&[u8]]) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(root_id.0);
    for piece in pieces {
        hash_len64(&mut hasher, piece);
    }
    digest_prefix(hasher.finalize().into())
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::Mutex;

    use nokv_object::ArtifactReadStats;
    use nokv_protocol::{
        ArtifactDescriptor, ConflictKind, DigestUri, OperationProgress, OperationToken,
        PathMetadata, RelativePath, RestoreManifestDescriptor, RestoreSource, RpcFailure,
        SnapshotSelector, WorkspaceSummary,
    };

    use super::*;

    #[derive(Default)]
    struct FakeWorkflowState {
        get_operations: VecDeque<Result<ClientCall<OperationStatus>, ClientError>>,
        commits: VecDeque<Result<ClientCall<OperationStatus>, ClientError>>,
        preparations: VecDeque<Result<ClientCall<RestorePreparation>, ClientError>>,
        bindings: VecDeque<Result<ClientCall<RestorePreparation>, ClientError>>,
        source_manifests: VecDeque<Result<ArtifactReadOutcome, ClientError>>,
        finalizations: VecDeque<Result<ClientCall<RestoreResult>, ClientError>>,
        commit_requests: Vec<CommitRequest>,
        prepare_requests: Vec<PrepareRestoreRequest>,
        bind_requests: Vec<BindRestoreDestinationRequest>,
        finalize_requests: Vec<OperationIdentity>,
        publications: Vec<(ArtifactPublishOptions, Vec<u8>)>,
        publication_replays: VecDeque<bool>,
        manifest: Option<ArtifactReadOutcome>,
        manifest_workspace_incarnation_id: Option<WorkspaceIdentity>,
        manifest_reads: usize,
    }

    #[derive(Default)]
    struct FakeWorkflowIo {
        state: Mutex<FakeWorkflowState>,
    }

    impl FakeWorkflowIo {
        fn state(&self) -> std::sync::MutexGuard<'_, FakeWorkflowState> {
            self.state.lock().unwrap()
        }
    }

    impl WorkflowIo for FakeWorkflowIo {
        fn submit_commit(
            &self,
            request: CommitRequest,
        ) -> Result<ClientCall<OperationStatus>, ClientError> {
            let mut state = self.state();
            state.commit_requests.push(request);
            state
                .commits
                .pop_front()
                .expect("test must script every commit response")
        }

        fn get_operation(
            &self,
            _operation_id: OperationIdentity,
        ) -> Result<ClientCall<OperationStatus>, ClientError> {
            self.state()
                .get_operations
                .pop_front()
                .expect("test must script every operation lookup")
        }

        fn publish_manifest(
            &self,
            options: ArtifactPublishOptions,
            bytes: &[u8],
        ) -> Result<ClientCall<PublishResult>, ClientError> {
            let body_digest = digest_uri(bytes);
            let mut state = self.state();
            let metadata = manifest_metadata(
                options.target.clone(),
                options.artifact_revision_id,
                state
                    .manifest_workspace_incarnation_id
                    .expect("test must script the live manifest incarnation"),
                options.content_type.clone(),
                bytes,
            );
            let result = PublishResult {
                operation_id: options.operation_id,
                target: options.target.clone(),
                workspace_revision: 2,
                generation: 1,
                artifact_revision_id: options.artifact_revision_id,
                logical_size: bytes.len() as u64,
                body_digest,
            };
            state.publications.push((options, bytes.to_vec()));
            state.manifest = Some(ArtifactReadOutcome {
                metadata,
                bytes: bytes.to_vec(),
                stats: ArtifactReadStats::default(),
            });
            let replayed = state.publication_replays.pop_front().unwrap_or(false);
            Ok(call(result, replayed))
        }

        fn read_restore_source_run_manifest(
            &self,
            _operation_id: OperationIdentity,
        ) -> Result<ArtifactReadOutcome, ClientError> {
            let mut state = self.state();
            state.manifest_reads += 1;
            state
                .source_manifests
                .pop_front()
                .expect("test must script every restore-held source manifest read")
        }

        fn prepare_restore(
            &self,
            request: PrepareRestoreRequest,
        ) -> Result<ClientCall<RestorePreparation>, ClientError> {
            let mut state = self.state();
            state.prepare_requests.push(request);
            state
                .preparations
                .pop_front()
                .expect("test must script every restore preparation")
        }

        fn bind_restore_destination(
            &self,
            request: BindRestoreDestinationRequest,
        ) -> Result<ClientCall<RestorePreparation>, ClientError> {
            let mut state = self.state();
            state.bind_requests.push(request);
            state
                .bindings
                .pop_front()
                .expect("test must script every restore destination bind")
        }

        fn finalize_restore(
            &self,
            operation_id: OperationIdentity,
        ) -> Result<ClientCall<RestoreResult>, ClientError> {
            let mut state = self.state();
            state.finalize_requests.push(operation_id);
            state
                .finalizations
                .pop_front()
                .expect("test must script every restore finalization")
        }
    }

    fn call<T>(value: T, replayed: bool) -> ClientCall<T> {
        ClientCall {
            value,
            commit_version: Some(7),
            replayed,
        }
    }

    fn rpc_error(code: ErrorCode) -> ClientError {
        ClientError::Rpc(RpcFailure {
            code,
            message: "injected workflow response".to_owned(),
            retryable: false,
            conflict: (code == ErrorCode::Conflict).then_some(ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        })
    }

    fn digest_uri(bytes: &[u8]) -> DigestUri {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        sha256_digest_uri(Digest(digest))
    }

    fn progress() -> OperationProgress {
        OperationProgress {
            completed_rows: 0,
            total_rows: None,
            completed_bytes: 0,
            total_bytes: None,
        }
    }

    fn commit_fixture() -> (CommitWorkflowOptions, CommitPreparation, CommitResult) {
        let root = RootIdentity([1; 16]);
        let commit_id = CommitIdentity([3; 32]);
        let identities = CommitWorkflowIdentities::derive(root, commit_id);
        let workbench = WorkbenchName::new("commit-run").unwrap();
        let target = WorkspacePath {
            workbench: workbench.clone(),
            path: RelativePath::new("metadata/run_manifest.json").unwrap(),
        };
        let request = CommitRequest {
            operation_id: identities.operation_id,
            workbench: workbench.clone(),
            workspace_incarnation_id: WorkspaceIdentity([4; 16]),
            commit_id,
            content_digest: DigestUri::new(format!("sha256:{}", "05".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "06".repeat(32))).unwrap(),
            projection_input_digest: Digest([0x0a; 32]),
            tree_manifest_revision_id: identities.tree_manifest_revision_id,
            replace: false,
            run_manifest_condition: PublishCondition::CreateOnly,
            expected_head_generation: None,
            parents: Vec::new(),
            producer: None,
            lineage_projection: Vec::new(),
        };
        let preparation = CommitPreparation {
            request: Box::new(request.clone()),
            committed_at_unix_seconds: 1_700_000_123,
            manifest: None,
        };
        let result = CommitResult {
            operation_id: identities.operation_id,
            commit_id,
            workbench,
            head_generation: 1,
            member_count: 3,
            member_digest: Digest([7; 32]),
        };
        (
            CommitWorkflowOptions {
                identities,
                request: CommitWorkflowRequest::Fresh(request),
                manifest_target: target,
                manifest_content_type: ContentType::new("application/json").unwrap(),
            },
            preparation,
            result,
        )
    }

    fn commit_status(
        options: &CommitWorkflowOptions,
        preparation: CommitPreparation,
        result: Option<CommitResult>,
    ) -> OperationStatus {
        OperationStatus {
            token: OperationToken {
                operation_id: options.identities.operation_id,
                state_digest: Digest([8; 32]),
            },
            kind: OperationKind::Commit,
            commit_preparation: Some(Box::new(preparation)),
            restore_preparation: None,
            state: if result.is_some() {
                OperationState::Succeeded
            } else {
                OperationState::Running
            },
            progress: progress(),
            result: result.map(OperationResult::Commit),
            failure: None,
        }
    }

    fn fresh_commit_request(options: &CommitWorkflowOptions) -> CommitRequest {
        match &options.request {
            CommitWorkflowRequest::Fresh(request) => request.clone(),
            CommitWorkflowRequest::Recover(_) => panic!("fixture starts as a fresh commit"),
        }
    }

    fn recovery_request(request: &CommitRequest) -> CommitRecoveryRequest {
        CommitRecoveryRequest {
            operation_id: request.operation_id,
            workbench: request.workbench.clone(),
            commit_id: request.commit_id,
            content_digest: request.content_digest.clone(),
            manifest_digest: request.manifest_digest.clone(),
            projection_input_digest: request.projection_input_digest,
            tree_manifest_revision_id: request.tree_manifest_revision_id,
            replace: request.replace,
        }
    }

    fn commit_manifest_binding(
        options: &CommitWorkflowOptions,
        request: &CommitRequest,
        bytes: &[u8],
    ) -> CommitManifestBinding {
        CommitManifestBinding {
            workspace_incarnation_id: request.workspace_incarnation_id,
            artifact_revision_id: options.identities.tree_manifest_revision_id,
            descriptor: ArtifactDescriptor {
                logical_size: bytes.len() as u64,
                body_digest: digest_uri(bytes),
                manifest_digest: digest_uri(b"commit-manifest-plan"),
                content_type: options.manifest_content_type.clone(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        }
    }

    fn publish_status(
        options: &CommitWorkflowOptions,
        binding: &CommitManifestBinding,
    ) -> OperationStatus {
        OperationStatus {
            token: OperationToken {
                operation_id: options.identities.manifest_publish_operation_id,
                state_digest: Digest([0x18; 32]),
            },
            kind: OperationKind::ArtifactPublish,
            commit_preparation: None,
            restore_preparation: None,
            state: OperationState::Succeeded,
            progress: progress(),
            result: Some(OperationResult::ArtifactPublish(PublishResult {
                operation_id: options.identities.manifest_publish_operation_id,
                target: options.manifest_target.clone(),
                workspace_revision: 2,
                generation: 1,
                artifact_revision_id: binding.artifact_revision_id,
                logical_size: binding.descriptor.logical_size,
                body_digest: binding.descriptor.body_digest.clone(),
            })),
            failure: None,
        }
    }

    fn restore_fixture() -> (
        RestoreWorkflowOptions,
        RestorePreparation,
        RestoreDestinationPlan,
        RestoreResult,
    ) {
        let root = RootIdentity([1; 16]);
        let source_incarnation = WorkspaceIdentity([2; 16]);
        let source_workbench = WorkbenchName::new("source-run").unwrap();
        let destination = WorkbenchName::new("restored-run").unwrap();
        let identities = RestoreWorkflowIdentities::derive(
            root,
            &source_workbench,
            source_incarnation,
            crate::WorkbenchRestoreSource::Snapshot { snapshot_id: 7 },
            &destination,
        );
        let restore_bytes = br#"{"restore":true}"#.to_vec();
        let body_digest = digest_uri(&restore_bytes);
        let restore_identities = identities.manifest_identities(root, body_digest.as_str());
        let restore_identity = RestoreManifestIdentity {
            publication_operation_id: restore_identities.publish_operation_id,
            artifact_revision_id: restore_identities.revision_id,
        };
        let prepare_request = PrepareRestoreRequest {
            operation_id: identities.operation_id,
            source_workbench,
            source_workspace_incarnation_id: source_incarnation,
            source: RestoreSource::Snapshot(SnapshotSelector::Id(7)),
            destination_workbench: destination.clone(),
            destination_workspace_incarnation_id: identities.destination_workspace_incarnation_id,
            destination_restore_manifest_identity: restore_identity,
            restore_manifest: RestoreManifestDescriptor {
                body_digest,
                logical_size: restore_bytes.len() as u64,
                content_type: ContentType::new("application/json").unwrap(),
            },
        };
        let source_commit = nokv_protocol::RestoreSourceCommitBinding {
            commit_id: CommitIdentity([0x30; 32]),
            content_digest: DigestUri::new(format!("sha256:{}", "31".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "32".repeat(32))).unwrap(),
            tree_manifest_revision_id: ArtifactRevisionIdentity([0x33; 16]),
            member_count: 5,
            member_digest: Digest([9; 32]),
        };
        let preparation = RestorePreparation {
            operation_id: identities.operation_id,
            destination_workbench: destination.clone(),
            destination_workspace_incarnation_id: identities.destination_workspace_incarnation_id,
            source_commit: source_commit.clone(),
            destination_committed_at_unix_seconds: 1_700_000_456,
            source_member_count: 5,
            source_member_digest: Digest([9; 32]),
            materialized_member_count: 4,
            materialized_member_digest: Digest([0x0b; 32]),
            source_matches_base_commit: true,
            destination_binding: None,
        };
        let run_identity = RestoreManifestIdentity {
            publication_operation_id: OperationIdentity([0x42; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([0x43; 16]),
        };
        let plan = RestoreDestinationPlan {
            binding: BindRestoreDestinationRequest {
                operation_id: identities.operation_id,
                destination_commit_id: CommitIdentity([0x40; 32]),
                effective_content_digest: source_commit.content_digest.clone(),
                destination_run_manifest_projection_input_digest: Digest([0x41; 32]),
                destination_run_manifest_identity: run_identity,
                destination_restore_manifest_identity: restore_identity,
            },
            run_manifest: RestoreManifestPublication {
                identity: run_identity,
                target: WorkspacePath {
                    workbench: destination.clone(),
                    path: RelativePath::new("metadata/run_manifest.json").unwrap(),
                },
                content_type: ContentType::new("application/json").unwrap(),
                bytes: br#"{"run":"destination"}"#.to_vec(),
            },
            restore_manifest: RestoreManifestPublication {
                identity: restore_identity,
                target: WorkspacePath {
                    workbench: destination.clone(),
                    path: RelativePath::new("metadata/restore_manifest.json").unwrap(),
                },
                content_type: ContentType::new("application/json").unwrap(),
                bytes: restore_bytes,
            },
        };
        let result = RestoreResult {
            operation_id: identities.operation_id,
            destination: WorkspaceSummary {
                workbench: destination,
                workspace_incarnation_id: identities.destination_workspace_incarnation_id,
                workspace_revision: 1,
                commit_head: Some(plan.binding.destination_commit_id),
                commit_head_generation: Some(1),
            },
            member_count: preparation.materialized_member_count,
            member_digest: preparation.materialized_member_digest,
            metadata_rows_copied: preparation.materialized_member_count,
            object_bytes_copied: 0,
        };
        (
            RestoreWorkflowOptions {
                identities,
                request: RestoreWorkflowRequest::Fresh(prepare_request),
            },
            preparation,
            plan,
            result,
        )
    }

    fn restore_request(options: &RestoreWorkflowOptions) -> PrepareRestoreRequest {
        match &options.request {
            RestoreWorkflowRequest::Fresh(request) => request.clone(),
            RestoreWorkflowRequest::Recover(_) => panic!("fixture starts as fresh restore"),
        }
    }

    fn source_run_manifest(
        options: &RestoreWorkflowOptions,
        preparation: &RestorePreparation,
    ) -> ArtifactReadOutcome {
        let request = restore_request(options);
        let bytes = br#"{"source":"run"}"#.to_vec();
        ArtifactReadOutcome {
            metadata: manifest_metadata(
                WorkspacePath {
                    workbench: request.source_workbench,
                    path: RelativePath::new("metadata/run_manifest.json").unwrap(),
                },
                preparation.source_commit.tree_manifest_revision_id,
                request.source_workspace_incarnation_id,
                ContentType::new("application/json").unwrap(),
                &bytes,
            ),
            bytes,
            stats: ArtifactReadStats::default(),
        }
    }

    fn destination_manifest_binding(
        publication: &RestoreManifestPublication,
        workspace_incarnation_id: WorkspaceIdentity,
    ) -> nokv_protocol::RestoreManifestBinding {
        nokv_protocol::RestoreManifestBinding {
            publication_operation_id: publication.identity.publication_operation_id,
            workspace_incarnation_id,
            artifact_revision_id: publication.identity.artifact_revision_id,
            descriptor: ArtifactDescriptor {
                logical_size: publication.bytes.len() as u64,
                body_digest: digest_uri(&publication.bytes),
                manifest_digest: digest_uri(b"restore-owned-manifest-plan"),
                content_type: publication.content_type.clone(),
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        }
    }

    fn response_binding(
        options: &RestoreWorkflowOptions,
        plan: &RestoreDestinationPlan,
        terminal: bool,
    ) -> RestoreDestinationBinding {
        RestoreDestinationBinding {
            destination_commit_id: plan.binding.destination_commit_id,
            effective_content_digest: plan.binding.effective_content_digest.clone(),
            destination_run_manifest_projection_input_digest: plan
                .binding
                .destination_run_manifest_projection_input_digest,
            destination_run_manifest_identity: plan.binding.destination_run_manifest_identity,
            destination_restore_manifest_identity: plan
                .binding
                .destination_restore_manifest_identity,
            destination_manifests: terminal.then(|| RestoreDestinationManifestBindings {
                run_manifest: destination_manifest_binding(
                    &plan.run_manifest,
                    options.identities.destination_workspace_incarnation_id,
                ),
                restore_manifest: destination_manifest_binding(
                    &plan.restore_manifest,
                    options.identities.destination_workspace_incarnation_id,
                ),
            }),
        }
    }

    fn bound_preparation(
        options: &RestoreWorkflowOptions,
        preparation: &RestorePreparation,
        plan: &RestoreDestinationPlan,
        terminal: bool,
    ) -> RestorePreparation {
        let mut bound = preparation.clone();
        bound.destination_binding = Some(Box::new(response_binding(options, plan, terminal)));
        bound
    }

    fn restore_operation_preparation(
        options: &RestoreWorkflowOptions,
        preparation: &RestorePreparation,
        binding: Option<RestoreDestinationBinding>,
    ) -> RestoreOperationPreparation {
        RestoreOperationPreparation {
            request: restore_request(options),
            source_snapshot_read_version: Some(17),
            source_commit: preparation.source_commit.clone(),
            destination_committed_at_unix_seconds: preparation
                .destination_committed_at_unix_seconds,
            source_member_count: Some(preparation.source_member_count),
            source_member_digest: Some(preparation.source_member_digest),
            materialized_member_count: Some(preparation.materialized_member_count),
            materialized_member_digest: Some(preparation.materialized_member_digest),
            source_matches_base_commit: Some(preparation.source_matches_base_commit),
            destination_binding: binding.map(Box::new),
        }
    }

    fn restore_status(
        options: &RestoreWorkflowOptions,
        preparation: &RestorePreparation,
        plan: &RestoreDestinationPlan,
        result: RestoreResult,
    ) -> OperationStatus {
        OperationStatus {
            token: OperationToken {
                operation_id: options.identities.operation_id,
                state_digest: Digest([10; 32]),
            },
            kind: OperationKind::Restore,
            commit_preparation: None,
            restore_preparation: Some(Box::new(restore_operation_preparation(
                options,
                preparation,
                Some(response_binding(options, plan, true)),
            ))),
            state: OperationState::Succeeded,
            progress: progress(),
            result: Some(OperationResult::Restore(result)),
            failure: None,
        }
    }

    fn running_restore_status(
        options: &RestoreWorkflowOptions,
        preparation: &RestorePreparation,
        binding: Option<RestoreDestinationBinding>,
    ) -> OperationStatus {
        OperationStatus {
            token: OperationToken {
                operation_id: options.identities.operation_id,
                state_digest: Digest([10; 32]),
            },
            kind: OperationKind::Restore,
            commit_preparation: None,
            restore_preparation: Some(Box::new(restore_operation_preparation(
                options,
                preparation,
                binding,
            ))),
            state: OperationState::Running,
            progress: progress(),
            result: None,
            failure: None,
        }
    }

    fn manifest_metadata(
        target: WorkspacePath,
        revision_id: ArtifactRevisionIdentity,
        workspace_incarnation_id: WorkspaceIdentity,
        content_type: ContentType,
        bytes: &[u8],
    ) -> PathMetadata {
        PathMetadata {
            path: target,
            workspace_incarnation_id,
            workspace_revision: 2,
            generation: 1,
            artifact_revision_id: revision_id,
            dependency_count: 0,
            dependency_depth: 0,
            descriptor: ArtifactDescriptor {
                logical_size: bytes.len() as u64,
                body_digest: digest_uri(bytes),
                manifest_digest: digest_uri(b"manifest-plan"),
                content_type,
                producer: None,
                manifest_identity: None,
                index_fields: Vec::new(),
            },
        }
    }

    fn script_fresh_restore_success(
        io: &FakeWorkflowIo,
        options: &RestoreWorkflowOptions,
        preparation: &RestorePreparation,
        plan: &RestoreDestinationPlan,
        result: &RestoreResult,
        publication_replays: impl IntoIterator<Item = bool>,
    ) {
        let mut state = io.state();
        state
            .get_operations
            .push_back(Err(rpc_error(ErrorCode::NotFound)));
        state
            .preparations
            .push_back(Ok(call(preparation.clone(), false)));
        state.get_operations.push_back(Ok(call(
            running_restore_status(options, preparation, None),
            false,
        )));
        state
            .source_manifests
            .push_back(Ok(source_run_manifest(options, preparation)));
        state.bindings.push_back(Ok(call(
            bound_preparation(options, preparation, plan, false),
            false,
        )));
        state.get_operations.push_back(Ok(call(
            running_restore_status(
                options,
                preparation,
                Some(response_binding(options, plan, false)),
            ),
            false,
        )));
        state
            .finalizations
            .push_back(Ok(call(result.clone(), false)));
        state.get_operations.push_back(Ok(call(
            restore_status(options, preparation, plan, result.clone()),
            false,
        )));
        state.publication_replays.extend(publication_replays);
        state.manifest_workspace_incarnation_id =
            Some(options.identities.destination_workspace_incarnation_id);
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn workflow_identity_domains_have_frozen_golden_bytes() {
        let root = RootIdentity([1; 16]);
        let commit = CommitWorkflowIdentities::derive(root, CommitIdentity([3; 32]));
        assert_eq!(
            hex(&commit.operation_id.0),
            "6c69f78792b72f1d86353313a9edc4e4"
        );
        assert_eq!(
            hex(&commit.manifest_publish_operation_id.0),
            "fc654ed78a4774bcdd44cc7381978188"
        );
        assert_eq!(
            hex(&commit.tree_manifest_revision_id.0),
            "d879f0983c8c4549e90b7026bcc52621"
        );

        let restore = RestoreWorkflowIdentities::derive(
            root,
            &WorkbenchName::new("source-run").unwrap(),
            WorkspaceIdentity([2; 16]),
            crate::WorkbenchRestoreSource::Snapshot { snapshot_id: 7 },
            &WorkbenchName::new("restored-run").unwrap(),
        );
        assert_eq!(
            hex(&restore.operation_id.0),
            "9b21eabe08d0356d55168ef162156ee6"
        );
        assert_eq!(
            hex(&restore.destination_workspace_incarnation_id.0),
            "d6671c1122c9f8739e03fec77520aaed"
        );
        let manifest = restore.manifest_identities(root, &format!("sha256:{}", "ab".repeat(32)));
        assert_eq!(
            hex(&manifest.publish_operation_id.0),
            "f813bc0aceb0d0ecfb02ef0c9bd1dd98"
        );
        assert_eq!(
            hex(&manifest.revision_id.0),
            "80eaf4e236db23c62e3d070517fe5301"
        );
    }

    #[test]
    fn commit_conflict_resubmits_the_exact_dto_once() {
        let (options, _, _) = commit_fixture();
        let request = fresh_commit_request(&options);
        let mut submitted = Vec::new();
        let value = submit_exact_commit_request_after_conflict(&request, |candidate| {
            submitted.push(candidate);
            if submitted.len() == 1 {
                Err(rpc_error(ErrorCode::Conflict))
            } else {
                Ok(7_u8)
            }
        })
        .unwrap();
        assert_eq!(value, 7);
        assert_eq!(submitted, vec![request.clone(), request]);
    }

    #[test]
    fn commit_conflict_then_replay_mismatch_stops_without_lookup_fallback() {
        let (options, _, _) = commit_fixture();
        let request = fresh_commit_request(&options);
        let mut submitted = Vec::new();
        let error = submit_exact_commit_request_after_conflict(&request, |candidate| {
            submitted.push(candidate);
            if submitted.len() == 1 {
                Err::<(), _>(rpc_error(ErrorCode::Conflict))
            } else {
                Err::<(), _>(rpc_error(ErrorCode::RequestReplayMismatch))
            }
        })
        .unwrap_err();
        assert_eq!(error.rpc_code(), Some(ErrorCode::RequestReplayMismatch));
        assert_eq!(submitted, vec![request.clone(), request]);
    }

    #[test]
    fn initial_commit_replay_mismatch_is_not_retried() {
        let (options, _, _) = commit_fixture();
        let request = fresh_commit_request(&options);
        let mut submitted = Vec::new();
        let error = submit_exact_commit_request_after_conflict(&request, |candidate| {
            submitted.push(candidate);
            Err::<(), _>(rpc_error(ErrorCode::RequestReplayMismatch))
        })
        .unwrap_err();
        assert_eq!(error.rpc_code(), Some(ErrorCode::RequestReplayMismatch));
        assert_eq!(submitted, vec![request]);
    }

    #[test]
    fn commit_workflow_uses_durable_time_and_owns_staging_and_finalize() {
        let (options, preparation, result) = commit_fixture();
        let request = fresh_commit_request(&options);
        let bytes = b"manifest:1700000123".to_vec();
        let mut completed_preparation = preparation.clone();
        completed_preparation.manifest = Some(commit_manifest_binding(&options, &request, &bytes));
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.manifest_workspace_incarnation_id = Some(request.workspace_incarnation_id);
            state
                .get_operations
                .push_back(Err(rpc_error(ErrorCode::NotFound)));
            state.get_operations.push_back(Ok(call(
                publish_status(&options, completed_preparation.manifest.as_ref().unwrap()),
                false,
            )));
            state.commits.push_back(Err(rpc_error(ErrorCode::Conflict)));
            state.commits.push_back(Ok(call(
                commit_status(&options, preparation.clone(), None),
                true,
            )));
            state.commits.push_back(Ok(call(
                commit_status(
                    &options,
                    completed_preparation.clone(),
                    Some(result.clone()),
                ),
                false,
            )));
        }
        let outcome = drive_commit_workflow(&io, options.clone(), |durable_time| {
            Ok::<_, Infallible>(format!("manifest:{durable_time}").into_bytes())
        })
        .unwrap();

        assert_eq!(outcome.result, result);
        assert_eq!(outcome.preparation, completed_preparation);
        assert!(outcome.replayed);
        let state = io.state();
        assert_eq!(
            state.commit_requests,
            vec![request.clone(), request.clone(), request]
        );
        assert_eq!(state.publications.len(), 1);
        assert_eq!(state.publications[0].1, b"manifest:1700000123".to_vec());
        assert_eq!(
            state.publications[0].0.authority,
            PublicationAuthority::CommitStaging {
                commit_operation_id: options.identities.operation_id
            }
        );
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn terminal_commit_replay_resubmits_the_durable_exact_request_without_live_path() {
        let (mut options, mut preparation, mut result) = commit_fixture();
        preparation.request.expected_head_generation = Some(7);
        preparation.request.parents = vec![CommitIdentity([1; 32])];
        preparation.request.replace = true;
        preparation.request.run_manifest_condition = PublishCondition::ReplaceOnly {
            expected_generation: 4,
        };
        result.head_generation = 8;
        let bytes = b"manifest:1700000123".to_vec();
        preparation.manifest = Some(commit_manifest_binding(
            &options,
            &preparation.request,
            &bytes,
        ));
        options.request = CommitWorkflowRequest::Recover(CommitRecoveryRequest {
            operation_id: preparation.request.operation_id,
            workbench: preparation.request.workbench.clone(),
            commit_id: preparation.request.commit_id,
            content_digest: preparation.request.content_digest.clone(),
            manifest_digest: preparation.request.manifest_digest.clone(),
            projection_input_digest: preparation.request.projection_input_digest,
            tree_manifest_revision_id: preparation.request.tree_manifest_revision_id,
            replace: preparation.request.replace,
        });
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.get_operations.push_back(Ok(call(
                commit_status(&options, preparation.clone(), Some(result.clone())),
                false,
            )));
            state.commits.push_back(Ok(call(
                commit_status(&options, preparation.clone(), Some(result.clone())),
                true,
            )));
            state.get_operations.push_back(Ok(call(
                publish_status(&options, preparation.manifest.as_ref().unwrap()),
                false,
            )));
        }
        let outcome = drive_commit_workflow(&io, options.clone(), |durable_time| {
            Ok::<_, Infallible>(format!("manifest:{durable_time}").into_bytes())
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert!(outcome.replayed);
        let state = io.state();
        assert_eq!(state.commit_requests.len(), 1);
        assert_eq!(
            state.commit_requests[0].expected_head_generation,
            preparation.request.expected_head_generation
        );
        assert_eq!(
            state.commit_requests[0].parents,
            preparation.request.parents
        );
        assert_eq!(
            state.commit_requests[0].workspace_incarnation_id,
            preparation.request.workspace_incarnation_id
        );
        assert_eq!(
            state.commit_requests[0].run_manifest_condition,
            preparation.request.run_manifest_condition
        );
        assert!(state.publications.is_empty());
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn running_commit_recovery_rejects_a_different_projection_before_publish() {
        let (mut options, preparation, _) = commit_fixture();
        let request = fresh_commit_request(&options);
        let mut recovery = recovery_request(&request);
        recovery.projection_input_digest = Digest([0xee; 32]);
        options.request = CommitWorkflowRequest::Recover(recovery);
        let io = FakeWorkflowIo::default();
        io.state()
            .get_operations
            .push_back(Ok(call(commit_status(&options, preparation, None), false)));

        let error = drive_commit_workflow(&io, options, |_| -> Result<Vec<u8>, Infallible> {
            panic!("projection mismatch must fail before rebuilding manifest bytes")
        })
        .unwrap_err();
        assert!(matches!(
            error,
            CommitWorkflowError::Client(ClientError::ResponseMismatch(_))
        ));
        let state = io.state();
        assert!(state.commit_requests.is_empty());
        assert!(state.publications.is_empty());
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn running_commit_recovery_with_the_exact_projection_completes() {
        let (mut options, preparation, result) = commit_fixture();
        let request = fresh_commit_request(&options);
        options.request = CommitWorkflowRequest::Recover(recovery_request(&request));
        let bytes = b"manifest:1700000123".to_vec();
        let mut completed_preparation = preparation.clone();
        completed_preparation.manifest = Some(commit_manifest_binding(&options, &request, &bytes));
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.manifest_workspace_incarnation_id = Some(request.workspace_incarnation_id);
            state.get_operations.push_back(Ok(call(
                commit_status(&options, preparation.clone(), None),
                false,
            )));
            state.get_operations.push_back(Ok(call(
                publish_status(&options, completed_preparation.manifest.as_ref().unwrap()),
                false,
            )));
            state
                .commits
                .push_back(Ok(call(commit_status(&options, preparation, None), true)));
            state.commits.push_back(Ok(call(
                commit_status(
                    &options,
                    completed_preparation.clone(),
                    Some(result.clone()),
                ),
                false,
            )));
        }

        let outcome = drive_commit_workflow(&io, options.clone(), |durable_time| {
            Ok::<_, Infallible>(format!("manifest:{durable_time}").into_bytes())
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert!(outcome.replayed);
        let state = io.state();
        assert_eq!(state.commit_requests, vec![request.clone(), request]);
        assert_eq!(state.publications.len(), 1);
        assert_eq!(state.publications[0].1, bytes);
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn commit_manifest_binding_rejects_wrong_incarnation_and_rebuilt_bytes() {
        let (options, mut preparation, _) = commit_fixture();
        let request = fresh_commit_request(&options);
        let bytes = b"manifest:1700000123".to_vec();
        preparation.manifest = Some(commit_manifest_binding(&options, &request, &bytes));

        let mut wrong_incarnation = preparation.clone();
        wrong_incarnation
            .manifest
            .as_mut()
            .unwrap()
            .workspace_incarnation_id = WorkspaceIdentity([0xee; 16]);
        let error = validate_commit_manifest(
            &FakeWorkflowIo::default(),
            &wrong_incarnation,
            options.identities.manifest_publish_operation_id,
            &options.manifest_target,
            &options.manifest_content_type,
            &bytes,
        )
        .unwrap_err();
        assert!(matches!(error, ClientError::ResponseMismatch(_)));

        let error = validate_commit_manifest(
            &FakeWorkflowIo::default(),
            &preparation,
            options.identities.manifest_publish_operation_id,
            &options.manifest_target,
            &options.manifest_content_type,
            b"different canonical bytes",
        )
        .unwrap_err();
        assert!(matches!(error, ClientError::ResponseMismatch(_)));
    }

    #[test]
    fn commit_manifest_publication_status_is_validated_field_by_field() {
        let (options, mut preparation, _) = commit_fixture();
        let request = fresh_commit_request(&options);
        let bytes = b"manifest:1700000123".to_vec();
        preparation.manifest = Some(commit_manifest_binding(&options, &request, &bytes));
        let binding = preparation.manifest.as_ref().unwrap();
        let valid = publish_status(&options, binding);
        let mut cases = Vec::new();

        let mut wrong_token = valid.clone();
        wrong_token.token.operation_id = OperationIdentity([0xe1; 16]);
        cases.push(wrong_token);

        let mut wrong_kind = valid.clone();
        wrong_kind.kind = OperationKind::Commit;
        cases.push(wrong_kind);

        let mut wrong_state = valid.clone();
        wrong_state.state = OperationState::Running;
        cases.push(wrong_state);

        let mut wrong_result_operation = valid.clone();
        let Some(OperationResult::ArtifactPublish(result)) = wrong_result_operation.result.as_mut()
        else {
            unreachable!("publish fixture always carries a publish result");
        };
        result.operation_id = OperationIdentity([0xe2; 16]);
        cases.push(wrong_result_operation);

        let mut wrong_target = valid.clone();
        let Some(OperationResult::ArtifactPublish(result)) = wrong_target.result.as_mut() else {
            unreachable!("publish fixture always carries a publish result");
        };
        result.target.path = RelativePath::new("metadata/other.json").unwrap();
        cases.push(wrong_target);

        let mut wrong_revision = valid.clone();
        let Some(OperationResult::ArtifactPublish(result)) = wrong_revision.result.as_mut() else {
            unreachable!("publish fixture always carries a publish result");
        };
        result.artifact_revision_id = ArtifactRevisionIdentity([0xe3; 16]);
        cases.push(wrong_revision);

        let mut wrong_size = valid.clone();
        let Some(OperationResult::ArtifactPublish(result)) = wrong_size.result.as_mut() else {
            unreachable!("publish fixture always carries a publish result");
        };
        result.logical_size += 1;
        cases.push(wrong_size);

        let mut wrong_digest = valid;
        let Some(OperationResult::ArtifactPublish(result)) = wrong_digest.result.as_mut() else {
            unreachable!("publish fixture always carries a publish result");
        };
        result.body_digest = digest_uri(b"different published body");
        cases.push(wrong_digest);

        for status in cases {
            let io = FakeWorkflowIo::default();
            io.state().get_operations.push_back(Ok(call(status, false)));
            let error = validate_commit_manifest(
                &io,
                &preparation,
                options.identities.manifest_publish_operation_id,
                &options.manifest_target,
                &options.manifest_content_type,
                &bytes,
            )
            .unwrap_err();
            assert!(matches!(error, ClientError::ResponseMismatch(_)));
        }
    }

    #[test]
    fn terminal_commit_recovery_rejects_different_caller_input_before_submit() {
        let (mut options, mut preparation, result) = commit_fixture();
        let bytes = b"manifest:1700000123".to_vec();
        let request = fresh_commit_request(&options);
        preparation.manifest = Some(commit_manifest_binding(&options, &request, &bytes));
        options.request = CommitWorkflowRequest::Recover(CommitRecoveryRequest {
            operation_id: request.operation_id,
            workbench: request.workbench,
            commit_id: request.commit_id,
            content_digest: request.content_digest,
            manifest_digest: request.manifest_digest,
            projection_input_digest: request.projection_input_digest,
            tree_manifest_revision_id: request.tree_manifest_revision_id,
            replace: !request.replace,
        });
        let io = FakeWorkflowIo::default();
        io.state().get_operations.push_back(Ok(call(
            commit_status(&options, preparation, Some(result)),
            false,
        )));

        let error =
            drive_commit_workflow(&io, options, |_| Ok::<_, Infallible>(Vec::new())).unwrap_err();
        assert!(matches!(
            error,
            CommitWorkflowError::Client(ClientError::ResponseMismatch(_))
        ));
        let state = io.state();
        assert!(state.commit_requests.is_empty());
        assert!(state.publications.is_empty());
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn terminal_commit_initialization_mismatch_cannot_fall_back_to_get_success() {
        let (options, mut preparation, result) = commit_fixture();
        let request = fresh_commit_request(&options);
        let bytes = b"manifest:1700000123".to_vec();
        preparation.manifest = Some(commit_manifest_binding(&options, &request, &bytes));
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.get_operations.push_back(Ok(call(
                commit_status(&options, preparation, Some(result)),
                false,
            )));
            state
                .commits
                .push_back(Err(rpc_error(ErrorCode::RequestReplayMismatch)));
        }

        let error =
            drive_commit_workflow(&io, options, |_| Ok::<_, Infallible>(Vec::new())).unwrap_err();
        let code = match error {
            CommitWorkflowError::Lookup(error) | CommitWorkflowError::Client(error) => {
                error.rpc_code()
            }
            CommitWorkflowError::BuildManifest(error) => match error {},
        };
        assert_eq!(code, Some(ErrorCode::RequestReplayMismatch));
        let state = io.state();
        assert_eq!(state.commit_requests.len(), 1);
        assert!(state.publications.is_empty());
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn restore_workflow_owns_prepare_staging_finalize_and_result_validation() {
        let (options, preparation, plan, result) = restore_fixture();
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state
                .get_operations
                .push_back(Err(rpc_error(ErrorCode::NotFound)));
            state
                .preparations
                .push_back(Ok(call(preparation.clone(), false)));
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, None),
                false,
            )));
            state
                .source_manifests
                .push_back(Ok(source_run_manifest(&options, &preparation)));
            state.bindings.push_back(Ok(call(
                bound_preparation(&options, &preparation, &plan, false),
                false,
            )));
            state.get_operations.push_back(Ok(call(
                running_restore_status(
                    &options,
                    &preparation,
                    Some(response_binding(&options, &plan, false)),
                ),
                false,
            )));
            state
                .finalizations
                .push_back(Ok(call(result.clone(), false)));
            state.get_operations.push_back(Ok(call(
                restore_status(&options, &preparation, &plan, result.clone()),
                false,
            )));
            state.manifest_workspace_incarnation_id =
                Some(options.identities.destination_workspace_incarnation_id);
        }
        let expected_plan = plan.clone();
        let outcome = drive_restore_workflow(&io, options.clone(), move |actual, bytes| {
            assert_eq!(actual, &preparation);
            assert_eq!(bytes, br#"{"source":"run"}"#);
            Ok(expected_plan)
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert!(!outcome.replayed);
        let state = io.state();
        assert_eq!(state.prepare_requests, vec![restore_request(&options)]);
        assert_eq!(
            state.finalize_requests,
            vec![options.identities.operation_id]
        );
        assert_eq!(state.bind_requests, vec![plan.binding]);
        assert_eq!(state.publications.len(), 2);
        assert_eq!(
            state.publications[0].0.authority,
            PublicationAuthority::RestoreStaging {
                restore_operation_id: options.identities.operation_id
            }
        );
        assert_eq!(state.manifest_reads, 1);
    }

    #[test]
    fn run_only_restore_only_and_both_published_recover_with_exact_identities() {
        for publication_replays in [[true, false], [false, true], [true, true]] {
            let (options, preparation, plan, result) = restore_fixture();
            let io = FakeWorkflowIo::default();
            script_fresh_restore_success(
                &io,
                &options,
                &preparation,
                &plan,
                &result,
                publication_replays,
            );

            let expected = plan.clone();
            let outcome =
                drive_restore_workflow(&io, options.clone(), move |_, _| Ok(expected)).unwrap();
            assert!(outcome.replayed);
            let state = io.state();
            assert_eq!(state.bind_requests, vec![plan.binding.clone()]);
            assert_eq!(state.publications.len(), 2);
            assert_eq!(
                state.publications[0].0.operation_id,
                plan.run_manifest.identity.publication_operation_id
            );
            assert_eq!(
                state.publications[0].0.artifact_revision_id,
                plan.run_manifest.identity.artifact_revision_id
            );
            assert_eq!(
                state.publications[1].0.operation_id,
                plan.restore_manifest.identity.publication_operation_id
            );
            assert_eq!(
                state.publications[1].0.artifact_revision_id,
                plan.restore_manifest.identity.artifact_revision_id
            );
        }
    }

    #[test]
    fn dirty_snapshot_restore_exact_binds_a_distinct_effective_digest() {
        let (options, mut preparation, mut plan, result) = restore_fixture();
        preparation.source_member_digest = Digest([0xa1; 32]);
        preparation.source_matches_base_commit = false;
        plan.binding.effective_content_digest =
            DigestUri::new(format!("sha256:{}", "a2".repeat(32))).unwrap();
        let io = FakeWorkflowIo::default();
        script_fresh_restore_success(&io, &options, &preparation, &plan, &result, [false, false]);

        let expected = plan.clone();
        let outcome = drive_restore_workflow(&io, options, move |actual, _| {
            assert!(!actual.source_matches_base_commit);
            assert_ne!(
                expected.binding.effective_content_digest,
                actual.source_commit.content_digest
            );
            Ok(expected)
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert_eq!(io.state().bind_requests, vec![plan.binding]);
    }

    #[test]
    fn pre_bind_recovery_reconstructs_plan_from_durable_source_hold() {
        let (mut options, preparation, plan, result) = restore_fixture();
        let exact = restore_request(&options);
        options.request = RestoreWorkflowRequest::Recover(RestoreRecoveryRequest {
            source_workbench: exact.source_workbench.clone(),
            source: exact.source.clone(),
            destination_workbench: exact.destination_workbench.clone(),
            destination_workspace_incarnation_id: exact.destination_workspace_incarnation_id,
            destination_restore_manifest_identity: exact.destination_restore_manifest_identity,
            restore_manifest: exact.restore_manifest.clone(),
        });
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            let unbound = running_restore_status(
                &RestoreWorkflowOptions {
                    identities: options.identities,
                    request: RestoreWorkflowRequest::Fresh(exact.clone()),
                },
                &preparation,
                None,
            );
            state
                .get_operations
                .push_back(Ok(call(unbound.clone(), false)));
            state
                .preparations
                .push_back(Ok(call(preparation.clone(), true)));
            state.get_operations.push_back(Ok(call(unbound, false)));
            state.source_manifests.push_back(Ok(source_run_manifest(
                &RestoreWorkflowOptions {
                    identities: options.identities,
                    request: RestoreWorkflowRequest::Fresh(exact.clone()),
                },
                &preparation,
            )));
            state.bindings.push_back(Ok(call(
                bound_preparation(&options, &preparation, &plan, false),
                true,
            )));
            let fresh_options = RestoreWorkflowOptions {
                identities: options.identities,
                request: RestoreWorkflowRequest::Fresh(exact.clone()),
            };
            state.get_operations.push_back(Ok(call(
                running_restore_status(
                    &fresh_options,
                    &preparation,
                    Some(response_binding(&options, &plan, false)),
                ),
                false,
            )));
            state
                .finalizations
                .push_back(Ok(call(result.clone(), false)));
            state.get_operations.push_back(Ok(call(
                restore_status(&fresh_options, &preparation, &plan, result.clone()),
                false,
            )));
            state.manifest_workspace_incarnation_id =
                Some(options.identities.destination_workspace_incarnation_id);
        }

        let expected = plan.clone();
        let outcome = drive_restore_workflow(&io, options, move |actual, bytes| {
            assert_eq!(actual.destination_committed_at_unix_seconds, 1_700_000_456);
            assert_eq!(bytes, br#"{"source":"run"}"#);
            Ok(expected)
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert_eq!(io.state().bind_requests, vec![plan.binding]);
    }

    #[test]
    fn existing_destination_bind_mismatch_stops_before_bind_or_publication() {
        let (options, preparation, plan, _) = restore_fixture();
        let mut conflicting = response_binding(&options, &plan, false);
        conflicting.destination_commit_id = CommitIdentity([0xee; 32]);
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, Some(conflicting.clone())),
                false,
            )));
            state
                .preparations
                .push_back(Ok(call(preparation.clone(), true)));
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, Some(conflicting)),
                false,
            )));
            state
                .source_manifests
                .push_back(Ok(source_run_manifest(&options, &preparation)));
        }
        let expected = plan.clone();
        assert!(matches!(
            drive_restore_workflow(&io, options, move |_, _| Ok(expected)),
            Err(RestoreWorkflowError::Validation(
                ClientError::ResponseMismatch(_)
            ))
        ));
        let state = io.state();
        assert!(state.bind_requests.is_empty());
        assert!(state.publications.is_empty());
    }

    #[test]
    fn manifest_or_projection_failure_writes_no_bind_or_object() {
        for callback_error in [false, true] {
            let (options, preparation, mut plan, _) = restore_fixture();
            let io = FakeWorkflowIo::default();
            {
                let mut state = io.state();
                state
                    .get_operations
                    .push_back(Err(rpc_error(ErrorCode::NotFound)));
                state
                    .preparations
                    .push_back(Ok(call(preparation.clone(), false)));
                state.get_operations.push_back(Ok(call(
                    running_restore_status(&options, &preparation, None),
                    false,
                )));
                state
                    .source_manifests
                    .push_back(Ok(source_run_manifest(&options, &preparation)));
            }
            if !callback_error {
                plan.restore_manifest.bytes.push(b' ');
            }
            let outcome = drive_restore_workflow(&io, options, move |_, _| {
                if callback_error {
                    Err(ClientError::InvalidOptions(
                        "injected projection failure".to_owned(),
                    ))
                } else {
                    Ok(plan)
                }
            });
            assert!(matches!(outcome, Err(RestoreWorkflowError::Validation(_))));
            let state = io.state();
            assert!(state.bind_requests.is_empty());
            assert!(state.publications.is_empty());
        }
    }

    #[test]
    fn terminal_restore_replay_uses_receipt_without_source_or_projection() {
        let (options, preparation, plan, result) = restore_fixture();
        let terminal = restore_status(&options, &preparation, &plan, result.clone());
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.preparations.push_back(Ok(call(
                bound_preparation(&options, &preparation, &plan, true),
                true,
            )));
            state
                .get_operations
                .push_back(Ok(call(terminal.clone(), false)));
            state.get_operations.push_back(Ok(call(terminal, false)));
        }
        let outcome = drive_restore_workflow(&io, options.clone(), |_, _| {
            panic!("terminal replay must not rebuild a destination projection")
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert!(outcome.replayed);
        let state = io.state();
        assert_eq!(state.prepare_requests, vec![restore_request(&options)]);
        assert!(state.finalize_requests.is_empty());
        assert!(state.publications.is_empty());
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn concurrent_completion_before_source_manifest_read_converges_on_the_receipt() {
        // Another exact caller completes the shared restore between this
        // caller's Running status check and its source-manifest read. The
        // engine rejects the read against the terminal row; the workflow must
        // converge on the durable Succeeded receipt instead of failing.
        let (options, preparation, plan, result) = restore_fixture();
        let terminal = restore_status(&options, &preparation, &plan, result.clone());
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state
                .get_operations
                .push_back(Err(rpc_error(ErrorCode::NotFound)));
            state
                .preparations
                .push_back(Ok(call(preparation.clone(), false)));
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, None),
                false,
            )));
            state
                .source_manifests
                .push_back(Err(rpc_error(ErrorCode::PreconditionFailed)));
            state.get_operations.push_back(Ok(call(terminal, false)));
        }
        let outcome = drive_restore_workflow(&io, options.clone(), |_, _| {
            panic!("a concurrently completed restore must not rebuild a projection")
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert!(outcome.replayed);
        let state = io.state();
        assert!(state.bind_requests.is_empty());
        assert!(state.finalize_requests.is_empty());
        assert!(state.publications.is_empty());
    }

    #[test]
    fn concurrent_completion_before_bind_converges_on_the_receipt() {
        let (options, preparation, plan, result) = restore_fixture();
        let terminal = restore_status(&options, &preparation, &plan, result.clone());
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state
                .get_operations
                .push_back(Err(rpc_error(ErrorCode::NotFound)));
            state
                .preparations
                .push_back(Ok(call(preparation.clone(), false)));
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, None),
                false,
            )));
            state
                .source_manifests
                .push_back(Ok(source_run_manifest(&options, &preparation)));
            state
                .bindings
                .push_back(Err(rpc_error(ErrorCode::PreconditionFailed)));
            state.get_operations.push_back(Ok(call(terminal, false)));
        }
        let expected_plan = plan.clone();
        let outcome =
            drive_restore_workflow(&io, options.clone(), move |_, _| Ok(expected_plan)).unwrap();
        assert_eq!(outcome.result, result);
        assert!(outcome.replayed);
        let state = io.state();
        assert_eq!(state.bind_requests, vec![plan.binding]);
        assert!(state.finalize_requests.is_empty());
        assert!(state.publications.is_empty());
    }

    #[test]
    fn step_failure_without_completion_keeps_the_original_error() {
        let (options, preparation, _plan, _result) = restore_fixture();
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state
                .get_operations
                .push_back(Err(rpc_error(ErrorCode::NotFound)));
            state
                .preparations
                .push_back(Ok(call(preparation.clone(), false)));
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, None),
                false,
            )));
            state
                .source_manifests
                .push_back(Err(rpc_error(ErrorCode::ObjectUnavailable)));
            state.get_operations.push_back(Ok(call(
                running_restore_status(&options, &preparation, None),
                false,
            )));
        }
        let error = drive_restore_workflow(&io, options, |_, _| {
            panic!("a failed source-manifest read must not build a projection")
        })
        .unwrap_err();
        assert!(matches!(
            error,
            RestoreWorkflowError::ReadSourceManifest(ref inner)
                if inner.rpc_code() == Some(ErrorCode::ObjectUnavailable)
        ));
    }

    #[test]
    fn terminal_restore_initialization_mismatch_stops_after_lookup_authentication() {
        let (options, preparation, plan, result) = restore_fixture();
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state.get_operations.push_back(Ok(call(
                restore_status(&options, &preparation, &plan, result),
                false,
            )));
            state
                .preparations
                .push_back(Err(rpc_error(ErrorCode::RequestReplayMismatch)));
        }

        let error = drive_restore_workflow(&io, options.clone(), |_, _| {
            panic!("prepare mismatch must stop before projection")
        })
        .unwrap_err();
        assert_eq!(
            error.client_error().rpc_code(),
            Some(ErrorCode::RequestReplayMismatch)
        );
        let state = io.state();
        assert_eq!(state.prepare_requests, vec![restore_request(&options)]);
        assert!(state.finalize_requests.is_empty());
        assert!(state.publications.is_empty());
        assert_eq!(state.manifest_reads, 0);
    }

    #[test]
    fn terminal_restore_recovery_reconstructs_the_exact_source_incarnation_from_status() {
        let (mut options, preparation, plan, result) = restore_fixture();
        let exact_request = restore_request(&options);
        let terminal = restore_status(&options, &preparation, &plan, result.clone());
        options.request = RestoreWorkflowRequest::Recover(RestoreRecoveryRequest {
            source_workbench: exact_request.source_workbench.clone(),
            source: exact_request.source.clone(),
            destination_workbench: exact_request.destination_workbench.clone(),
            destination_workspace_incarnation_id: exact_request
                .destination_workspace_incarnation_id,
            destination_restore_manifest_identity: exact_request
                .destination_restore_manifest_identity,
            restore_manifest: exact_request.restore_manifest.clone(),
        });
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state
                .get_operations
                .push_back(Ok(call(terminal.clone(), false)));
            state.preparations.push_back(Ok(call(
                bound_preparation(&options, &preparation, &plan, true),
                true,
            )));
            state.get_operations.push_back(Ok(call(terminal, false)));
        }

        let outcome = drive_restore_workflow(&io, options, |_, _| {
            panic!("terminal recovery must not read or rebuild the source projection")
        })
        .unwrap();
        assert_eq!(outcome.result, result);
        assert_eq!(outcome.source_snapshot_read_version, Some(17));
        let state = io.state();
        assert_eq!(state.prepare_requests, vec![exact_request]);
        assert!(state.publications.is_empty());
        assert!(state.finalize_requests.is_empty());
    }

    #[test]
    fn failed_and_quarantined_restore_statuses_are_identity_and_preparation_bound() {
        let (options, preparation, _, _) = restore_fixture();
        let mut cases = Vec::new();

        let mut wrong_token = running_restore_status(&options, &preparation, None);
        wrong_token.state = OperationState::Failed;
        wrong_token.token.operation_id = OperationIdentity([0xee; 16]);
        wrong_token.failure = Some(RpcFailure {
            code: ErrorCode::OperationFailed,
            message: "failed".to_owned(),
            retryable: false,
            conflict: Some(ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        });
        cases.push(wrong_token);

        let mut wrong_kind = running_restore_status(&options, &preparation, None);
        wrong_kind.state = OperationState::Quarantined;
        wrong_kind.kind = OperationKind::Commit;
        wrong_kind.failure = Some(RpcFailure {
            code: ErrorCode::Quarantined,
            message: "quarantined".to_owned(),
            retryable: false,
            conflict: Some(ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        });
        cases.push(wrong_kind);

        let mut missing_preparation = running_restore_status(&options, &preparation, None);
        missing_preparation.state = OperationState::Failed;
        missing_preparation.restore_preparation = None;
        missing_preparation.failure = Some(RpcFailure {
            code: ErrorCode::OperationFailed,
            message: "failed".to_owned(),
            retryable: false,
            conflict: Some(ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        });
        cases.push(missing_preparation);

        for status in cases {
            let io = FakeWorkflowIo::default();
            io.state().get_operations.push_back(Ok(call(status, false)));
            assert!(matches!(
                drive_restore_workflow(&io, options.clone(), |_, _| {
                    panic!("invalid terminal status must stop before projection")
                }),
                Err(RestoreWorkflowError::Validation(
                    ClientError::ResponseMismatch(_)
                ))
            ));
            assert!(io.state().prepare_requests.is_empty());
        }
    }

    #[test]
    fn terminal_restore_exact_prepare_must_replay_the_same_durable_failure() {
        let (options, preparation, _, _) = restore_fixture();
        for (state_kind, code, message) in [
            (OperationState::Failed, ErrorCode::OperationFailed, "failed"),
            (
                OperationState::Quarantined,
                ErrorCode::Quarantined,
                "quarantined",
            ),
        ] {
            let failure = RpcFailure {
                code,
                message: message.to_owned(),
                retryable: false,
                conflict: Some(ConflictKind::OperationState),
                current_generation: None,
                route_hint: None,
            };
            let mut status = running_restore_status(&options, &preparation, None);
            status.state = state_kind;
            status.failure = Some(failure.clone());
            let io = FakeWorkflowIo::default();
            {
                let mut scripted = io.state();
                scripted.get_operations.push_back(Ok(call(status, false)));
                scripted
                    .preparations
                    .push_back(Err(ClientError::Rpc(failure.clone())));
            }
            let error = drive_restore_workflow(&io, options.clone(), |_, _| {
                panic!("durable failure must stop before projection")
            })
            .unwrap_err();
            assert!(matches!(
                error,
                RestoreWorkflowError::Prepare(ClientError::Rpc(actual)) if actual == failure
            ));
            assert_eq!(io.state().prepare_requests, vec![restore_request(&options)]);
        }

        let expected = RpcFailure {
            code: ErrorCode::Quarantined,
            message: "durable quarantine".to_owned(),
            retryable: false,
            conflict: Some(ConflictKind::OperationState),
            current_generation: None,
            route_hint: None,
        };
        let mut status = running_restore_status(&options, &preparation, None);
        status.state = OperationState::Quarantined;
        status.failure = Some(expected);
        let io = FakeWorkflowIo::default();
        {
            let mut scripted = io.state();
            scripted.get_operations.push_back(Ok(call(status, false)));
            scripted
                .preparations
                .push_back(Err(rpc_error(ErrorCode::Quarantined)));
        }
        assert!(matches!(
            drive_restore_workflow(&io, options, |_, _| {
                panic!("durable failure must stop before projection")
            }),
            Err(RestoreWorkflowError::Validation(
                ClientError::ResponseMismatch(_)
            ))
        ));
    }

    #[test]
    fn terminal_restore_rejects_manifest_binding_from_another_incarnation() {
        let (options, preparation, plan, result) = restore_fixture();
        let mut terminal = restore_status(&options, &preparation, &plan, result);
        terminal
            .restore_preparation
            .as_deref_mut()
            .unwrap()
            .destination_binding
            .as_deref_mut()
            .unwrap()
            .destination_manifests
            .as_mut()
            .unwrap()
            .run_manifest
            .workspace_incarnation_id = WorkspaceIdentity([0xff; 16]);
        let io = FakeWorkflowIo::default();
        {
            let mut state = io.state();
            state
                .get_operations
                .push_back(Ok(call(terminal.clone(), false)));
            state.preparations.push_back(Ok(call(
                bound_preparation(&options, &preparation, &plan, true),
                true,
            )));
            state.get_operations.push_back(Ok(call(terminal, false)));
        }

        assert!(matches!(
            drive_restore_workflow(&io, options, |_, _| {
                panic!("terminal replay must not rebuild projection")
            }),
            Err(RestoreWorkflowError::Validation(
                ClientError::ResponseMismatch(_)
            ))
        ));
    }
}
