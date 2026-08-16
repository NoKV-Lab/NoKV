/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_protocol::{
    decode_response, encode_request, AbortArtifactPublishRequest, AggregateRequest,
    AggregateResult, BeginArtifactPublishRequest, BindRestoreDestinationRequest, CatalogRequest,
    CatalogResult, ChangePage, ChangePageRequest, CommitRequest, CompleteArtifactPublishRequest,
    CreateWorkspaceRequest, ErrorCode, FinalizeRestoreRequest, FindWorkspacesRequest,
    FindWorkspacesResult, GetOperationRequest, GetPathRequest, GetSnapshotRequest,
    GetWorkspaceRequest, ListPathsRequest, ListSnapshotsRequest, LogicalShardIdentity,
    MarkArtifactObjectsUploadedRequest, MintSnapshotRequest, OperationStatus, PathPage,
    PathReadResult, PrepareRestoreRequest, PublishResult, ReadRestoreSourceRunManifestRequest,
    RemovePathRequest, RemovePathResult, RenewSnapshotRequest, RequestIdentity, RestorePreparation,
    RestoreResult, RetireSnapshotRequest, RootIdentity, RootRoute, SearchRequest, SearchResult,
    SnapshotPage, SnapshotResult, StageArtifactManifestRequest, StageArtifactObjectsRequest,
    WorkspaceCapability, WorkspacePreflightRequest, WorkspacePreflightResult, WorkspaceRequest,
    WorkspaceResult, WorkspaceRpcOutcome, WorkspaceRpcRequest, WorkspaceSummary,
};
use sha2::{Digest as _, Sha256};

use crate::{ClientError, ResolvedRoute, RouteResolver, RpcTransport};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientOptions {
    pub max_attempts: u32,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

impl ClientOptions {
    fn validate(self) -> Result<(), ClientError> {
        if !(1..=16).contains(&self.max_attempts) {
            return Err(ClientError::InvalidOptions(
                "max_attempts must be between 1 and 16".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientCall<T> {
    pub value: T,
    pub commit_version: Option<u64>,
    pub replayed: bool,
}

impl<T> ClientCall<T> {
    pub(crate) fn map<U>(
        self,
        map: impl FnOnce(T) -> Result<U, ClientError>,
    ) -> Result<ClientCall<U>, ClientError> {
        Ok(ClientCall {
            value: map(self.value)?,
            commit_version: self.commit_version,
            replayed: self.replayed,
        })
    }
}

pub struct WorkspaceClient<Transport, Resolver> {
    root_id: RootIdentity,
    transport: Transport,
    routes: Resolver,
    options: ClientOptions,
    request_seed: [u8; 16],
    request_sequence: Arc<AtomicU64>,
}

impl<Transport, Resolver> Clone for WorkspaceClient<Transport, Resolver>
where
    Transport: Clone,
    Resolver: Clone,
{
    fn clone(&self) -> Self {
        Self {
            root_id: self.root_id,
            transport: self.transport.clone(),
            routes: self.routes.clone(),
            options: self.options,
            request_seed: self.request_seed,
            request_sequence: Arc::clone(&self.request_sequence),
        }
    }
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    pub fn new(
        root_id: RootIdentity,
        transport: Transport,
        routes: Resolver,
        options: ClientOptions,
    ) -> Result<Self, ClientError> {
        options.validate()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.workspace.sdk.request-seed\0");
        hasher.update(root_id.0);
        hasher.update(now.to_be_bytes());
        hasher.update(std::process::id().to_be_bytes());
        let digest = hasher.finalize();
        let mut request_seed = [0_u8; 16];
        request_seed.copy_from_slice(&digest[..16]);
        Ok(Self {
            root_id,
            transport,
            routes,
            options,
            request_seed,
            request_sequence: Arc::new(AtomicU64::new(0)),
        })
    }

    pub const fn root_id(&self) -> RootIdentity {
        self.root_id
    }

    pub(crate) const fn max_attempts(&self) -> u32 {
        self.options.max_attempts
    }

    /// Allocates a process-local unique request id suitable for exact replay.
    pub fn new_request_id(&self) -> RequestIdentity {
        let sequence = self.request_sequence.fetch_add(1, Ordering::Relaxed);
        let mut hasher = Sha256::new();
        hasher.update(b"nokv.workspace.sdk.request\0");
        hasher.update(self.request_seed);
        hasher.update(sequence.to_be_bytes());
        let digest = hasher.finalize();
        let mut request_id = [0_u8; 16];
        request_id.copy_from_slice(&digest[..16]);
        RequestIdentity(request_id)
    }

    /// Executes one operation. All retries reuse `request_id` exactly.
    pub fn execute(
        &self,
        request_id: RequestIdentity,
        operation: WorkspaceRequest,
    ) -> Result<ClientCall<WorkspaceResult>, ClientError> {
        self.execute_with_expected_shard(request_id, operation, None)
    }

    pub(crate) fn resolve_artifact_route(&self) -> Result<RootRoute, ClientError> {
        let resolved = self.routes.resolve(self.root_id, false)?;
        validate_resolved_route(resolved, self.root_id, None)?;
        Ok(resolved.route)
    }

    pub(crate) fn execute_on_logical_shard(
        &self,
        request_id: RequestIdentity,
        operation: WorkspaceRequest,
        logical_shard_id: LogicalShardIdentity,
    ) -> Result<ClientCall<WorkspaceResult>, ClientError> {
        self.execute_with_expected_shard(request_id, operation, Some(logical_shard_id))
    }

    fn execute_with_expected_shard(
        &self,
        request_id: RequestIdentity,
        operation: WorkspaceRequest,
        expected_shard: Option<LogicalShardIdentity>,
    ) -> Result<ClientCall<WorkspaceResult>, ClientError> {
        let mut resolved = self.routes.resolve(self.root_id, false)?;
        validate_resolved_route(resolved, self.root_id, expected_shard)?;

        for attempt in 1..=self.options.max_attempts {
            let request = WorkspaceRpcRequest {
                route: resolved.route,
                request_id,
                operation: operation.clone(),
            };
            let encoded = encode_request(&request)?;
            let response_bytes = match self.transport.round_trip(resolved.endpoint, &encoded) {
                Ok(response) => response,
                Err(error) => {
                    let retryable = error.retryable();
                    if retryable && attempt < self.options.max_attempts {
                        continue;
                    }
                    return exhausted_or_last(
                        attempt,
                        self.options.max_attempts,
                        ClientError::Transport(error),
                    );
                }
            };
            let response = decode_response(&response_bytes)?;
            if response.request_id != request_id {
                return Err(ClientError::ResponseMismatch(
                    "request identity does not match".to_owned(),
                ));
            }
            if response.route.root_id != self.root_id {
                return Err(ClientError::ResponseMismatch(
                    "response belongs to another root".to_owned(),
                ));
            }

            match response.outcome {
                WorkspaceRpcOutcome::Success(result) => {
                    if response.route != resolved.route {
                        return Err(ClientError::ResponseMismatch(
                            "successful response route differs from request route".to_owned(),
                        ));
                    }
                    if let WorkspaceResult::Preflight(preflight) = result.as_ref() {
                        if preflight.route != resolved.route {
                            return Err(ClientError::ResponseMismatch(
                                "preflight result route differs from the resolved route".to_owned(),
                            ));
                        }
                    }
                    return Ok(ClientCall {
                        value: *result,
                        commit_version: response.commit_version,
                        replayed: response.replayed,
                    });
                }
                WorkspaceRpcOutcome::Failure(failure) => {
                    let should_refresh = failure.code == ErrorCode::NotOwner;
                    let retryable = failure.retryable || should_refresh;
                    if retryable && attempt < self.options.max_attempts {
                        if should_refresh {
                            if let Some(hint) = failure.route_hint {
                                validate_route(hint, self.root_id, expected_shard)?;
                                validate_route_continuity(hint, resolved.route, "NotOwner hint")?;
                            }
                            let refreshed = self.routes.resolve(self.root_id, true)?;
                            validate_resolved_route(refreshed, self.root_id, expected_shard)?;
                            validate_route_continuity(
                                refreshed.route,
                                resolved.route,
                                "refreshed route",
                            )?;
                            if let Some(hint) = failure.route_hint {
                                validate_refreshed_route(refreshed.route, hint)?;
                            }
                            if refreshed == resolved {
                                return Err(ClientError::InvalidRoute(
                                    "NotOwner refresh returned the unchanged route and endpoint"
                                        .to_owned(),
                                ));
                            }
                            resolved = refreshed;
                        }
                        continue;
                    }
                    return exhausted_or_last(
                        attempt,
                        self.options.max_attempts,
                        ClientError::Rpc(failure),
                    );
                }
            }
        }

        unreachable!("validated max_attempts is non-zero")
    }

    pub fn execute_read(
        &self,
        operation: WorkspaceRequest,
    ) -> Result<ClientCall<WorkspaceResult>, ClientError> {
        self.execute(self.new_request_id(), operation)
    }

    /// Verifies the exact wire/capability schemas, current route, and every
    /// capability required by the caller before any business operation.
    pub fn preflight(
        &self,
        required_capabilities: impl IntoIterator<Item = WorkspaceCapability>,
    ) -> Result<ClientCall<WorkspacePreflightResult>, ClientError> {
        let request = WorkspacePreflightRequest::new(required_capabilities);
        let required_capabilities = request.required_capabilities.clone();
        let call = self
            .execute_read(WorkspaceRequest::Preflight(request))?
            .map(expect_preflight)?;
        if call.value.route.root_id != self.root_id {
            return Err(ClientError::ResponseMismatch(
                "preflight result belongs to another root".to_owned(),
            ));
        }
        let missing = required_capabilities
            .into_iter()
            .filter(|capability| !call.value.supports(*capability))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(ClientError::MissingCapabilities(missing));
        }
        Ok(call)
    }

    pub fn create_workspace(
        &self,
        request_id: RequestIdentity,
        request: CreateWorkspaceRequest,
    ) -> Result<ClientCall<WorkspaceSummary>, ClientError> {
        self.execute(request_id, WorkspaceRequest::CreateWorkspace(request))?
            .map(expect_workspace)
    }

    pub fn get_workspace(
        &self,
        request: GetWorkspaceRequest,
    ) -> Result<ClientCall<WorkspaceSummary>, ClientError> {
        self.execute_read(WorkspaceRequest::GetWorkspace(request))?
            .map(expect_workspace)
    }

    pub fn get_path(
        &self,
        request: GetPathRequest,
    ) -> Result<ClientCall<PathReadResult>, ClientError> {
        self.execute_read(WorkspaceRequest::GetPath(request))?
            .map(expect_path)
    }

    pub fn list_paths(
        &self,
        request: ListPathsRequest,
    ) -> Result<ClientCall<PathPage>, ClientError> {
        self.execute_read(WorkspaceRequest::ListPaths(request))?
            .map(expect_paths)
    }

    pub fn remove_path(
        &self,
        request_id: RequestIdentity,
        request: RemovePathRequest,
    ) -> Result<ClientCall<RemovePathResult>, ClientError> {
        self.execute(request_id, WorkspaceRequest::RemovePath(request))?
            .map(expect_removed)
    }

    pub fn begin_artifact_publish(
        &self,
        request_id: RequestIdentity,
        request: BeginArtifactPublishRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute(request_id, WorkspaceRequest::BeginArtifactPublish(request))?
            .map(expect_operation)
    }

    pub fn stage_artifact_objects(
        &self,
        request_id: RequestIdentity,
        request: StageArtifactObjectsRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute(request_id, WorkspaceRequest::StageArtifactObjects(request))?
            .map(expect_operation)
    }

    pub fn mark_artifact_objects_uploaded(
        &self,
        request_id: RequestIdentity,
        request: MarkArtifactObjectsUploadedRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::MarkArtifactObjectsUploaded(request),
        )?
        .map(expect_operation)
    }

    pub fn stage_artifact_manifest(
        &self,
        request_id: RequestIdentity,
        request: StageArtifactManifestRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute(request_id, WorkspaceRequest::StageArtifactManifest(request))?
            .map(expect_operation)
    }

    pub fn complete_artifact_publish(
        &self,
        request_id: RequestIdentity,
        request: CompleteArtifactPublishRequest,
    ) -> Result<ClientCall<PublishResult>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::CompleteArtifactPublish(request),
        )?
        .map(expect_published)
    }

    pub fn abort_artifact_publish(
        &self,
        request_id: RequestIdentity,
        request: AbortArtifactPublishRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute(request_id, WorkspaceRequest::AbortArtifactPublish(request))?
            .map(expect_operation)
    }

    pub fn commit(
        &self,
        request_id: RequestIdentity,
        request: CommitRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute(request_id, WorkspaceRequest::Commit(request))?
            .map(expect_operation)
    }

    pub fn mint_snapshot(
        &self,
        request_id: RequestIdentity,
        request: MintSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.execute(request_id, WorkspaceRequest::MintSnapshot(request))?
            .map(expect_snapshot)
    }

    pub fn get_snapshot(
        &self,
        request: GetSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.execute_read(WorkspaceRequest::GetSnapshot(request))?
            .map(expect_snapshot)
    }

    pub fn renew_snapshot(
        &self,
        request_id: RequestIdentity,
        request: RenewSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.execute(request_id, WorkspaceRequest::RenewSnapshot(request))?
            .map(expect_snapshot)
    }

    pub fn retire_snapshot(
        &self,
        request_id: RequestIdentity,
        request: RetireSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.execute(request_id, WorkspaceRequest::RetireSnapshot(request))?
            .map(expect_snapshot)
    }

    pub fn list_snapshots(
        &self,
        request: ListSnapshotsRequest,
    ) -> Result<ClientCall<SnapshotPage>, ClientError> {
        self.execute_read(WorkspaceRequest::ListSnapshots(request))?
            .map(expect_snapshots)
    }

    pub fn prepare_restore(
        &self,
        request_id: RequestIdentity,
        request: PrepareRestoreRequest,
    ) -> Result<ClientCall<RestorePreparation>, ClientError> {
        self.execute(request_id, WorkspaceRequest::PrepareRestore(request))?
            .map(expect_restore_prepared)
    }

    pub fn bind_restore_destination(
        &self,
        request_id: RequestIdentity,
        request: BindRestoreDestinationRequest,
    ) -> Result<ClientCall<RestorePreparation>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::BindRestoreDestination(request),
        )?
        .map(expect_restore_prepared)
    }

    pub fn read_restore_source_run_manifest(
        &self,
        request: ReadRestoreSourceRunManifestRequest,
    ) -> Result<ClientCall<PathReadResult>, ClientError> {
        self.execute_read(WorkspaceRequest::ReadRestoreSourceRunManifest(request))?
            .map(expect_restore_source_run_manifest)
    }

    pub fn finalize_restore(
        &self,
        request_id: RequestIdentity,
        request: FinalizeRestoreRequest,
    ) -> Result<ClientCall<RestoreResult>, ClientError> {
        self.execute(request_id, WorkspaceRequest::FinalizeRestore(request))?
            .map(expect_restored)
    }

    pub fn get_operation(
        &self,
        request: GetOperationRequest,
    ) -> Result<ClientCall<OperationStatus>, ClientError> {
        self.execute_read(WorkspaceRequest::GetOperation(request))?
            .map(expect_operation)
    }

    pub fn search(&self, request: SearchRequest) -> Result<ClientCall<SearchResult>, ClientError> {
        self.execute_read(WorkspaceRequest::Search(request))?
            .map(expect_search)
    }

    pub fn aggregate(
        &self,
        request: AggregateRequest,
    ) -> Result<ClientCall<AggregateResult>, ClientError> {
        self.execute_read(WorkspaceRequest::Aggregate(request))?
            .map(expect_aggregate)
    }

    pub fn catalog(
        &self,
        request: CatalogRequest,
    ) -> Result<ClientCall<CatalogResult>, ClientError> {
        self.execute_read(WorkspaceRequest::Catalog(request))?
            .map(expect_catalog)
    }

    pub fn find_workspaces(
        &self,
        request: FindWorkspacesRequest,
    ) -> Result<ClientCall<FindWorkspacesResult>, ClientError> {
        self.execute_read(WorkspaceRequest::FindWorkspaces(request))?
            .map(expect_workspaces)
    }

    pub fn read_changes(
        &self,
        request: ChangePageRequest,
    ) -> Result<ClientCall<ChangePage>, ClientError> {
        self.execute_read(WorkspaceRequest::ReadChanges(request))?
            .map(expect_changes)
    }
}

fn validate_route(
    route: RootRoute,
    root_id: RootIdentity,
    expected_shard: Option<LogicalShardIdentity>,
) -> Result<(), ClientError> {
    route
        .validate()
        .map_err(|error| ClientError::InvalidRoute(error.to_string()))?;
    if route.root_id != root_id {
        return Err(ClientError::InvalidRoute(
            "resolved route belongs to another root".to_owned(),
        ));
    }
    if expected_shard.is_some_and(|expected| route.logical_shard_id != expected) {
        return Err(ClientError::InvalidRoute(
            "root logical shard changed during one artifact lifecycle".to_owned(),
        ));
    }
    Ok(())
}

fn validate_resolved_route(
    resolved: ResolvedRoute,
    root_id: RootIdentity,
    expected_shard: Option<LogicalShardIdentity>,
) -> Result<(), ClientError> {
    validate_route(resolved.route, root_id, expected_shard)
}

fn validate_refreshed_route(refreshed: RootRoute, hint: RootRoute) -> Result<(), ClientError> {
    if refreshed.root_id != hint.root_id || refreshed.logical_shard_id != hint.logical_shard_id {
        return Err(ClientError::InvalidRoute(
            "refreshed route does not match the NotOwner placement hint".to_owned(),
        ));
    }
    if refreshed.placement_generation < hint.placement_generation
        || (refreshed.placement_generation == hint.placement_generation
            && refreshed.owner_epoch < hint.owner_epoch)
    {
        return Err(ClientError::InvalidRoute(
            "refreshed route is older than the NotOwner placement hint".to_owned(),
        ));
    }
    Ok(())
}

fn validate_route_continuity(
    candidate: RootRoute,
    current: RootRoute,
    source: &str,
) -> Result<(), ClientError> {
    if candidate.root_id != current.root_id
        || candidate.logical_shard_id != current.logical_shard_id
    {
        return Err(ClientError::InvalidRoute(format!(
            "{source} changes immutable root or logical-shard identity"
        )));
    }
    if candidate.object_namespace_id != current.object_namespace_id {
        return Err(ClientError::InvalidRoute(format!(
            "{source} changes immutable object namespace identity"
        )));
    }
    Ok(())
}

fn exhausted_or_last<T>(
    attempts: u32,
    max_attempts: u32,
    error: ClientError,
) -> Result<T, ClientError> {
    if error.retryable() && attempts == max_attempts {
        Err(ClientError::RetryExhausted {
            attempts,
            last_error: Box::new(error),
        })
    } else {
        Err(error)
    }
}

macro_rules! expect_result {
    ($name:ident, $variant:ident, $output:ty, $expected:literal) => {
        fn $name(result: WorkspaceResult) -> Result<$output, ClientError> {
            match result {
                WorkspaceResult::$variant(value) => Ok(value),
                _ => Err(ClientError::ResponseMismatch(
                    concat!("expected ", $expected, " result").to_owned(),
                )),
            }
        }
    };
}

expect_result!(
    expect_preflight,
    Preflight,
    WorkspacePreflightResult,
    "preflight"
);
expect_result!(expect_workspace, Workspace, WorkspaceSummary, "workspace");
expect_result!(expect_path, Path, PathReadResult, "path");
expect_result!(expect_paths, Paths, PathPage, "paths");
expect_result!(expect_removed, Removed, RemovePathResult, "removed");
expect_result!(expect_operation, Operation, OperationStatus, "operation");
expect_result!(expect_published, Published, PublishResult, "published");
expect_result!(expect_snapshot, Snapshot, SnapshotResult, "snapshot");
expect_result!(expect_snapshots, Snapshots, SnapshotPage, "snapshots");
expect_result!(
    expect_restore_prepared,
    RestorePrepared,
    RestorePreparation,
    "restore_prepared"
);
expect_result!(
    expect_restore_source_run_manifest,
    RestoreSourceRunManifest,
    PathReadResult,
    "restore_source_run_manifest"
);
expect_result!(expect_restored, Restored, RestoreResult, "restored");
expect_result!(expect_search, Search, SearchResult, "search");
expect_result!(expect_aggregate, Aggregate, AggregateResult, "aggregate");
expect_result!(expect_catalog, Catalog, CatalogResult, "catalog");
expect_result!(
    expect_workspaces,
    Workspaces,
    FindWorkspacesResult,
    "workspaces"
);
expect_result!(expect_changes, Changes, ChangePage, "changes");

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use nokv_protocol::{
        decode_request, encode_response, ArtifactDescriptor, ArtifactRevisionIdentity,
        BindRestoreDestinationRequest, CommitIdentity, ConflictKind, ContentType, Digest,
        DigestUri, ErrorCode, LogicalShardIdentity, OperationIdentity, PathMetadata,
        PathReadResult, ReadRestoreSourceRunManifestRequest, RelativePath,
        RestoreDestinationBinding, RestoreManifestIdentity, RestorePreparation,
        RestoreSourceCommitBinding, RpcFailure, WorkbenchName, WorkspaceIdentity, WorkspacePath,
        WorkspaceRpcResponse,
    };

    use super::*;
    use crate::StaticRouteResolver;

    #[derive(Debug)]
    struct ScriptedTransport {
        responses: Mutex<VecDeque<WorkspaceRpcResponse>>,
        requests: Mutex<Vec<WorkspaceRpcRequest>>,
        endpoints: Mutex<Vec<SocketAddr>>,
    }

    impl ScriptedTransport {
        fn new(responses: Vec<WorkspaceRpcResponse>) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                requests: Mutex::new(Vec::new()),
                endpoints: Mutex::new(Vec::new()),
            }
        }
    }

    impl RpcTransport for ScriptedTransport {
        fn round_trip(
            &self,
            endpoint: SocketAddr,
            request: &[u8],
        ) -> Result<Vec<u8>, crate::TransportError> {
            let request = decode_request(request)
                .map_err(|error| crate::TransportError::new(error.to_string(), false))?;
            self.endpoints.lock().unwrap().push(endpoint);
            self.requests.lock().unwrap().push(request);
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("script contains one response per request");
            encode_response(&response)
                .map_err(|error| crate::TransportError::new(error.to_string(), false))
        }
    }

    #[derive(Debug)]
    struct LossyRestoreReadTransport {
        requests: Mutex<Vec<WorkspaceRpcRequest>>,
        result: PathReadResult,
    }

    impl RpcTransport for LossyRestoreReadTransport {
        fn round_trip(
            &self,
            _endpoint: SocketAddr,
            request: &[u8],
        ) -> Result<Vec<u8>, crate::TransportError> {
            let request = decode_request(request)
                .map_err(|error| crate::TransportError::new(error.to_string(), false))?;
            let mut requests = self.requests.lock().unwrap();
            requests.push(request.clone());
            if requests.len() == 1 {
                return Err(crate::TransportError::new(
                    "injected restore-held read response loss",
                    true,
                ));
            }
            encode_response(&WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version: None,
                replayed: true,
                outcome: WorkspaceRpcOutcome::Success(Box::new(
                    WorkspaceResult::RestoreSourceRunManifest(self.result.clone()),
                )),
            })
            .map_err(|error| crate::TransportError::new(error.to_string(), false))
        }
    }

    #[derive(Clone, Debug)]
    struct ScriptedResolver {
        initial: ResolvedRoute,
        refreshed: ResolvedRoute,
        refreshes: Arc<Mutex<Vec<bool>>>,
    }

    impl RouteResolver for ScriptedResolver {
        fn resolve(
            &self,
            root_id: RootIdentity,
            refresh: bool,
        ) -> Result<ResolvedRoute, ClientError> {
            self.refreshes.lock().unwrap().push(refresh);
            let resolved = if refresh {
                self.refreshed
            } else {
                self.initial
            };
            if resolved.route.root_id != root_id {
                return Err(ClientError::InvalidRoute(
                    "scripted route belongs to another root".to_owned(),
                ));
            }
            Ok(resolved)
        }
    }

    fn route(owner_epoch: u64) -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: nokv_protocol::ObjectNamespaceIdentity([8; 16]),
            placement_generation: 3,
            owner_epoch,
        }
    }

    fn resolved(owner_epoch: u64, port: u16) -> ResolvedRoute {
        ResolvedRoute::new(route(owner_epoch), SocketAddr::from(([127, 0, 0, 1], port))).unwrap()
    }

    fn workspace_result() -> WorkspaceResult {
        WorkspaceResult::Workspace(WorkspaceSummary {
            workbench: nokv_protocol::WorkbenchName::new("run-42").unwrap(),
            workspace_incarnation_id: nokv_protocol::WorkspaceIdentity([5; 16]),
            workspace_revision: 0,
            commit_head: None,
            commit_head_generation: None,
        })
    }

    fn create_request() -> CreateWorkspaceRequest {
        CreateWorkspaceRequest {
            workbench: nokv_protocol::WorkbenchName::new("run-42").unwrap(),
            workspace_incarnation_id: nokv_protocol::WorkspaceIdentity([5; 16]),
        }
    }

    #[test]
    fn typed_call_uses_exact_route_and_request_id() {
        let request_id = RequestIdentity([8; 16]);
        let response = WorkspaceRpcResponse {
            route: route(7),
            request_id,
            commit_version: Some(11),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(workspace_result())),
        };
        let transport = ScriptedTransport::new(vec![response]);
        let resolver = StaticRouteResolver::new(route(7), resolved(7, 4107).endpoint).unwrap();
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();

        let result = client
            .create_workspace(request_id, create_request())
            .unwrap();
        assert_eq!(result.commit_version, Some(11));
        assert_eq!(result.value.workspace_revision, 0);
        let requests = client.transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].request_id, request_id);
        assert_eq!(requests[0].route, route(7));
        assert_eq!(
            *client.transport.endpoints.lock().unwrap(),
            vec![resolved(7, 4107).endpoint]
        );
    }

    #[test]
    fn restore_held_source_read_response_loss_reuses_the_exact_request() {
        let operation_id = OperationIdentity([0x44; 16]);
        let result = PathReadResult {
            not_modified: false,
            metadata: Some(PathMetadata {
                path: WorkspacePath {
                    workbench: WorkbenchName::new("source-run").unwrap(),
                    path: RelativePath::new("metadata/run_manifest.json").unwrap(),
                },
                workspace_incarnation_id: WorkspaceIdentity([0x45; 16]),
                workspace_revision: 9,
                generation: 1,
                artifact_revision_id: ArtifactRevisionIdentity([0x46; 16]),
                dependency_count: 0,
                dependency_depth: 0,
                descriptor: ArtifactDescriptor {
                    logical_size: 12,
                    body_digest: DigestUri::new(format!("sha256:{}", "47".repeat(32))).unwrap(),
                    manifest_digest: DigestUri::new(format!("sha256:{}", "48".repeat(32))).unwrap(),
                    content_type: ContentType::new("application/json").unwrap(),
                    producer: None,
                    manifest_identity: None,
                    index_fields: Vec::new(),
                },
            }),
            range: None,
            blocks: Vec::new(),
            next_cursor: None,
        };
        let transport = LossyRestoreReadTransport {
            requests: Mutex::new(Vec::new()),
            result: result.clone(),
        };
        let resolver = StaticRouteResolver::new(route(7), resolved(7, 4107).endpoint).unwrap();
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();

        let call = client
            .read_restore_source_run_manifest(ReadRestoreSourceRunManifestRequest {
                operation_id,
                range: None,
                plan_page: None,
            })
            .unwrap();
        assert!(call.replayed);
        assert_eq!(call.value, result);
        let requests = client.transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].request_id, requests[1].request_id);
        assert_eq!(requests[0].operation, requests[1].operation);
        assert!(matches!(
            &requests[0].operation,
            WorkspaceRequest::ReadRestoreSourceRunManifest(request)
                if request.operation_id == operation_id
        ));
    }

    #[test]
    fn typed_restore_destination_bind_maps_the_exact_preparation() {
        let request_id = RequestIdentity([0x50; 16]);
        let operation_id = OperationIdentity([0x51; 16]);
        let run_identity = RestoreManifestIdentity {
            publication_operation_id: OperationIdentity([0x52; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([0x53; 16]),
        };
        let restore_identity = RestoreManifestIdentity {
            publication_operation_id: OperationIdentity([0x54; 16]),
            artifact_revision_id: ArtifactRevisionIdentity([0x55; 16]),
        };
        let source_commit = RestoreSourceCommitBinding {
            commit_id: CommitIdentity([0x56; 32]),
            content_digest: DigestUri::new(format!("sha256:{}", "57".repeat(32))).unwrap(),
            manifest_digest: DigestUri::new(format!("sha256:{}", "58".repeat(32))).unwrap(),
            tree_manifest_revision_id: ArtifactRevisionIdentity([0x59; 16]),
            member_count: 2,
            member_digest: Digest([0x5a; 32]),
        };
        let bind = BindRestoreDestinationRequest {
            operation_id,
            destination_commit_id: CommitIdentity([0x5b; 32]),
            effective_content_digest: source_commit.content_digest.clone(),
            destination_run_manifest_projection_input_digest: Digest([0x5c; 32]),
            destination_run_manifest_identity: run_identity,
            destination_restore_manifest_identity: restore_identity,
        };
        let preparation = RestorePreparation {
            operation_id,
            destination_workbench: WorkbenchName::new("destination-run").unwrap(),
            destination_workspace_incarnation_id: WorkspaceIdentity([0x5d; 16]),
            source_commit: source_commit.clone(),
            destination_committed_at_unix_seconds: 1_700_000_999,
            source_member_count: 2,
            source_member_digest: source_commit.member_digest,
            materialized_member_count: 1,
            materialized_member_digest: Digest([0x5e; 32]),
            source_matches_base_commit: true,
            destination_binding: Some(Box::new(RestoreDestinationBinding {
                destination_commit_id: bind.destination_commit_id,
                effective_content_digest: bind.effective_content_digest.clone(),
                destination_run_manifest_projection_input_digest: bind
                    .destination_run_manifest_projection_input_digest,
                destination_run_manifest_identity: run_identity,
                destination_restore_manifest_identity: restore_identity,
                destination_manifests: None,
            })),
        };
        let response = WorkspaceRpcResponse {
            route: route(7),
            request_id,
            commit_version: Some(23),
            replayed: true,
            outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::RestorePrepared(
                preparation.clone(),
            ))),
        };
        let transport = ScriptedTransport::new(vec![response]);
        let resolver = StaticRouteResolver::new(route(7), resolved(7, 4107).endpoint).unwrap();
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();

        let call = client
            .bind_restore_destination(request_id, bind.clone())
            .unwrap();
        assert_eq!(call.value, preparation);
        assert!(call.replayed);
        let requests = client.transport.requests.lock().unwrap();
        assert!(matches!(
            &requests[0].operation,
            WorkspaceRequest::BindRestoreDestination(actual) if actual == &bind
        ));
    }

    #[test]
    fn not_owner_hint_forces_resolver_refresh_and_new_endpoint() {
        let request_id = RequestIdentity([9; 16]);
        let first = WorkspaceRpcResponse {
            route: route(7),
            request_id,
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                message: "owner changed".to_owned(),
                retryable: true,
                conflict: Some(ConflictKind::RootPlacement),
                current_generation: None,
                route_hint: Some(route(8)),
            }),
        };
        let second = WorkspaceRpcResponse {
            route: route(8),
            request_id,
            commit_version: Some(12),
            replayed: true,
            outcome: WorkspaceRpcOutcome::Success(Box::new(workspace_result())),
        };
        let transport = ScriptedTransport::new(vec![first, second]);
        let refreshes = Arc::new(Mutex::new(Vec::new()));
        let resolver = ScriptedResolver {
            initial: resolved(7, 4107),
            refreshed: resolved(8, 4108),
            refreshes: Arc::clone(&refreshes),
        };
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();

        let result = client
            .create_workspace(request_id, create_request())
            .unwrap();
        assert!(result.replayed);
        let requests = client.transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].request_id, requests[1].request_id);
        assert_eq!(requests[0].route.owner_epoch, 7);
        assert_eq!(requests[1].route.owner_epoch, 8);
        assert_eq!(
            *client.transport.endpoints.lock().unwrap(),
            vec![resolved(7, 4107).endpoint, resolved(8, 4108).endpoint]
        );
        assert_eq!(*refreshes.lock().unwrap(), vec![false, true]);
    }

    #[test]
    fn not_owner_refresh_cannot_change_the_root_object_namespace() {
        let request_id = RequestIdentity([10; 16]);
        let mut drifted = route(8);
        drifted.object_namespace_id = nokv_protocol::ObjectNamespaceIdentity([9; 16]);
        let first = WorkspaceRpcResponse {
            route: route(7),
            request_id,
            commit_version: None,
            replayed: false,
            outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                message: "owner changed".to_owned(),
                retryable: true,
                conflict: Some(ConflictKind::RootPlacement),
                current_generation: None,
                route_hint: Some(drifted),
            }),
        };
        let second = WorkspaceRpcResponse {
            route: drifted,
            request_id,
            commit_version: Some(12),
            replayed: true,
            outcome: WorkspaceRpcOutcome::Success(Box::new(workspace_result())),
        };
        let transport = ScriptedTransport::new(vec![first, second]);
        let refreshes = Arc::new(Mutex::new(Vec::new()));
        let resolver = ScriptedResolver {
            initial: resolved(7, 4107),
            refreshed: ResolvedRoute::new(drifted, SocketAddr::from(([127, 0, 0, 1], 4108)))
                .unwrap(),
            refreshes: Arc::clone(&refreshes),
        };
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();

        let error = client
            .create_workspace(request_id, create_request())
            .expect_err("object namespace identity is immutable for one root");

        assert!(matches!(error, ClientError::InvalidRoute(message)
            if message.contains("object namespace")));
        assert_eq!(client.transport.requests.lock().unwrap().len(), 1);
        assert_eq!(*refreshes.lock().unwrap(), vec![false]);
    }

    #[test]
    fn response_request_mismatch_fails_closed() {
        let response = WorkspaceRpcResponse {
            route: route(7),
            request_id: RequestIdentity([4; 16]),
            commit_version: Some(11),
            replayed: false,
            outcome: WorkspaceRpcOutcome::Success(Box::new(workspace_result())),
        };
        let transport = ScriptedTransport::new(vec![response]);
        let resolver = StaticRouteResolver::new(route(7), resolved(7, 4107).endpoint).unwrap();
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();

        assert!(matches!(
            client.create_workspace(RequestIdentity([8; 16]), create_request()),
            Err(ClientError::ResponseMismatch(_))
        ));
    }

    #[test]
    fn typed_preflight_canonicalizes_requirements_and_verifies_the_route() {
        let transport = ScriptedTransport::new(Vec::new());
        let resolver = StaticRouteResolver::new(route(7), resolved(7, 4107).endpoint).unwrap();
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();
        let request_id = client.new_request_id();
        client.request_sequence.store(0, Ordering::Relaxed);
        client
            .transport
            .responses
            .lock()
            .unwrap()
            .push_back(WorkspaceRpcResponse {
                route: route(7),
                request_id,
                commit_version: None,
                replayed: false,
                outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Preflight(
                    WorkspacePreflightResult::new(route(7), WorkspaceCapability::ALL),
                ))),
            });

        let preflight = client
            .preflight([
                WorkspaceCapability::RestoreV1,
                WorkspaceCapability::QueryV1,
                WorkspaceCapability::RestoreV1,
            ])
            .unwrap();
        assert_eq!(preflight.value.route, route(7));
        assert!(preflight.value.supports(WorkspaceCapability::RestoreV1));
        let requests = client.transport.requests.lock().unwrap();
        let WorkspaceRequest::Preflight(request) = &requests[0].operation else {
            panic!("typed preflight must emit the preflight operation");
        };
        assert_eq!(
            request.required_capabilities,
            vec![WorkspaceCapability::QueryV1, WorkspaceCapability::RestoreV1]
        );
    }

    #[test]
    fn typed_preflight_fails_closed_when_a_required_capability_is_missing() {
        let transport = ScriptedTransport::new(Vec::new());
        let resolver = StaticRouteResolver::new(route(7), resolved(7, 4107).endpoint).unwrap();
        let client = WorkspaceClient::new(
            route(7).root_id,
            transport,
            resolver,
            ClientOptions::default(),
        )
        .unwrap();
        let request_id = client.new_request_id();
        client.request_sequence.store(0, Ordering::Relaxed);
        client
            .transport
            .responses
            .lock()
            .unwrap()
            .push_back(WorkspaceRpcResponse {
                route: route(7),
                request_id,
                commit_version: None,
                replayed: false,
                outcome: WorkspaceRpcOutcome::Success(Box::new(WorkspaceResult::Preflight(
                    WorkspacePreflightResult::new(route(7), [WorkspaceCapability::QueryV1]),
                ))),
            });

        assert_eq!(
            client.preflight([WorkspaceCapability::RestoreV1]),
            Err(ClientError::MissingCapabilities(vec![
                WorkspaceCapability::RestoreV1
            ]))
        );
    }
}
