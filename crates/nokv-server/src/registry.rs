/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use nokv_protocol::{
    ConflictKind, ErrorCode, RootIdentity, RootRoute, RpcFailure, WorkspaceRpcOutcome,
    WorkspaceRpcRequest, WorkspaceRpcResponse,
};

use crate::{ExecutedRequest, ServerError, WorkspaceRequestExecutor};

struct RootOwner {
    route: RootRoute,
    executor: Arc<dyn WorkspaceRequestExecutor>,
}

#[derive(Default)]
pub struct RootOwnerRegistry {
    owners: RwLock<BTreeMap<RootIdentity, RootOwner>>,
}

impl RootOwnerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Installs or advances one current owner. Placement generation and owner
    /// epoch can never move backwards.
    pub fn install(
        &self,
        route: RootRoute,
        executor: Arc<dyn WorkspaceRequestExecutor>,
    ) -> Result<(), ServerError> {
        route
            .validate()
            .map_err(|error| ServerError::InvalidRoute(error.to_string()))?;
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        if let Some(current) = owners.get(&route.root_id) {
            if route.logical_shard_id != current.route.logical_shard_id {
                return Err(ServerError::RouteRollback(
                    "logical shard identity cannot change through local owner install".to_owned(),
                ));
            }
            if route.placement_generation < current.route.placement_generation
                || (route.placement_generation == current.route.placement_generation
                    && route.owner_epoch < current.route.owner_epoch)
            {
                return Err(ServerError::RouteRollback(format!(
                    "current placement/owner is {}/{}, attempted {}/{}",
                    current.route.placement_generation,
                    current.route.owner_epoch,
                    route.placement_generation,
                    route.owner_epoch
                )));
            }
        }
        owners.insert(route.root_id, RootOwner { route, executor });
        Ok(())
    }

    pub fn remove(&self, route: RootRoute) -> Result<bool, ServerError> {
        route
            .validate()
            .map_err(|error| ServerError::InvalidRoute(error.to_string()))?;
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let matches = owners
            .get(&route.root_id)
            .is_some_and(|owner| owner.route == route);
        if matches {
            owners.remove(&route.root_id);
        }
        Ok(matches)
    }

    pub fn installed_root_count(&self) -> Result<usize, ServerError> {
        self.owners
            .read()
            .map(|owners| owners.len())
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
    }

    pub fn contains_exact(&self, route: RootRoute) -> Result<bool, ServerError> {
        route
            .validate()
            .map_err(|error| ServerError::InvalidRoute(error.to_string()))?;
        self.owners
            .read()
            .map(|owners| {
                owners
                    .get(&route.root_id)
                    .is_some_and(|owner| owner.route == route)
            })
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
    }

    pub fn dispatch(
        &self,
        request: WorkspaceRpcRequest,
    ) -> Result<WorkspaceRpcResponse, ServerError> {
        let owners = self
            .owners
            .read()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let Some(owner) = owners.get(&request.route.root_id) else {
            return Ok(not_owner_response(
                request,
                None,
                "this server does not own the requested root",
            ));
        };
        if owner.route != request.route {
            return Ok(not_owner_response(
                request,
                Some(owner.route),
                "the requested root route is stale",
            ));
        }
        let route = owner.route;
        let executor = Arc::clone(&owner.executor);
        let request_id = request.request_id;
        drop(owners);
        let outcome = executor.execute(&request);
        Ok(match outcome {
            Ok(ExecutedRequest {
                result,
                commit_version,
                replayed,
            }) => WorkspaceRpcResponse {
                route,
                request_id,
                commit_version,
                replayed,
                outcome: WorkspaceRpcOutcome::Success(Box::new(result)),
            },
            Err(failure) => WorkspaceRpcResponse {
                route,
                request_id,
                commit_version: None,
                replayed: false,
                outcome: WorkspaceRpcOutcome::Failure(failure),
            },
        })
    }
}

fn not_owner_response(
    request: WorkspaceRpcRequest,
    route_hint: Option<RootRoute>,
    message: &str,
) -> WorkspaceRpcResponse {
    WorkspaceRpcResponse {
        route: route_hint.unwrap_or(request.route),
        request_id: request.request_id,
        commit_version: None,
        replayed: false,
        outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
            code: ErrorCode::NotOwner,
            message: message.to_owned(),
            retryable: true,
            conflict: Some(ConflictKind::RootPlacement),
            current_generation: None,
            route_hint,
        }),
    }
}

#[cfg(test)]
mod tests {
    use nokv_protocol::{
        CreateWorkspaceRequest, LogicalShardIdentity, RequestIdentity, WorkbenchName,
        WorkspaceIdentity, WorkspaceRequest, WorkspaceResult, WorkspaceSummary,
    };

    use super::*;

    struct EchoExecutor;

    impl WorkspaceRequestExecutor for EchoExecutor {
        fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            match &request.operation {
                WorkspaceRequest::CreateWorkspace(create) => Ok(ExecutedRequest {
                    result: WorkspaceResult::Workspace(WorkspaceSummary {
                        workbench: create.workbench.clone(),
                        workspace_incarnation_id: create.workspace_incarnation_id,
                        workspace_revision: 0,
                        commit_head: None,
                        commit_head_generation: None,
                    }),
                    commit_version: Some(9),
                    replayed: false,
                }),
                WorkspaceRequest::Preflight(_) => Ok(ExecutedRequest {
                    result: WorkspaceResult::Preflight(
                        nokv_protocol::WorkspacePreflightResult::new(
                            request.route,
                            nokv_protocol::WorkspaceCapability::ALL,
                        ),
                    ),
                    commit_version: None,
                    replayed: false,
                }),
                _ => panic!("test executor received an unexpected request"),
            }
        }
    }

    fn route(owner_epoch: u64) -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            placement_generation: 3,
            owner_epoch,
        }
    }

    fn request(route: RootRoute) -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route,
            request_id: RequestIdentity([4; 16]),
            operation: WorkspaceRequest::CreateWorkspace(CreateWorkspaceRequest {
                workbench: WorkbenchName::new("run-42").unwrap(),
                workspace_incarnation_id: WorkspaceIdentity([5; 16]),
            }),
        }
    }

    fn preflight_request(route: RootRoute) -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route,
            request_id: RequestIdentity([6; 16]),
            operation: WorkspaceRequest::Preflight(nokv_protocol::WorkspacePreflightRequest::new(
                [
                    nokv_protocol::WorkspaceCapability::QueryV1,
                    nokv_protocol::WorkspaceCapability::RestoreV1,
                ],
            )),
        }
    }

    #[test]
    fn exact_installed_route_dispatches() {
        let registry = RootOwnerRegistry::new();
        registry.install(route(7), Arc::new(EchoExecutor)).unwrap();
        let response = registry.dispatch(request(route(7))).unwrap();
        assert_eq!(response.route, route(7));
        assert_eq!(response.commit_version, Some(9));
        assert!(matches!(response.outcome, WorkspaceRpcOutcome::Success(_)));
    }

    #[test]
    fn preflight_dispatches_only_through_the_exact_installed_route() {
        let registry = RootOwnerRegistry::new();
        registry.install(route(8), Arc::new(EchoExecutor)).unwrap();

        let stale = registry.dispatch(preflight_request(route(7))).unwrap();
        let WorkspaceRpcOutcome::Failure(failure) = stale.outcome else {
            panic!("stale preflight route must fail");
        };
        assert_eq!(failure.code, ErrorCode::NotOwner);
        assert_eq!(failure.route_hint, Some(route(8)));

        let current = registry.dispatch(preflight_request(route(8))).unwrap();
        let WorkspaceRpcOutcome::Success(result) = current.outcome else {
            panic!("exact preflight route must dispatch");
        };
        let WorkspaceResult::Preflight(preflight) = *result else {
            panic!("preflight returned the wrong result variant");
        };
        assert_eq!(preflight.route, route(8));
    }

    #[test]
    fn stale_owner_is_rejected_with_current_route() {
        let registry = RootOwnerRegistry::new();
        registry.install(route(8), Arc::new(EchoExecutor)).unwrap();
        let response = registry.dispatch(request(route(7))).unwrap();
        let WorkspaceRpcOutcome::Failure(failure) = response.outcome else {
            panic!("stale route must fail");
        };
        assert_eq!(failure.code, ErrorCode::NotOwner);
        assert_eq!(failure.route_hint, Some(route(8)));
    }

    #[test]
    fn install_rejects_owner_rollback() {
        let registry = RootOwnerRegistry::new();
        registry.install(route(8), Arc::new(EchoExecutor)).unwrap();
        assert!(matches!(
            registry.install(route(7), Arc::new(EchoExecutor)),
            Err(ServerError::RouteRollback(_))
        ));
    }

    #[test]
    fn exact_remove_does_not_remove_a_newer_owner() {
        let registry = RootOwnerRegistry::new();
        registry.install(route(8), Arc::new(EchoExecutor)).unwrap();
        assert!(!registry.remove(route(7)).unwrap());
        assert_eq!(registry.installed_root_count().unwrap(), 1);
        assert!(registry.remove(route(8)).unwrap());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }
}
