/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, RwLock};

use nokv_protocol::{
    ConflictKind, ErrorCode, RootIdentity, RootRoute, RpcFailure, WorkspaceRpcOutcome,
    WorkspaceRpcRequest, WorkspaceRpcResponse,
};

use crate::{ExecutedRequest, OwnerLossSignal, ServerError, WorkspaceRequestExecutor};

#[derive(Default)]
struct ShardRuntimeState {
    closed: bool,
    in_flight: usize,
}

#[derive(Default)]
struct ShardRuntime {
    state: Mutex<ShardRuntimeState>,
    drained: Condvar,
}

impl ShardRuntime {
    fn enter(self: &Arc<Self>) -> Option<ResponsePermit> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return None;
        }
        state.in_flight += 1;
        Some(ResponsePermit {
            runtime: Arc::clone(self),
        })
    }

    fn close_admission(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
    }

    fn wait_until_drained(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.in_flight != 0 {
            state = self
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct ResponsePermit {
    runtime: Arc<ShardRuntime>,
}

impl Drop for ResponsePermit {
    fn drop(&mut self) {
        let mut state = self
            .runtime
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        debug_assert!(state.in_flight > 0);
        state.in_flight -= 1;
        if state.closed && state.in_flight == 0 {
            self.runtime.drained.notify_all();
        }
    }
}

pub(crate) struct GuardedResponse {
    response: WorkspaceRpcResponse,
    _permit: Option<ResponsePermit>,
}

impl GuardedResponse {
    pub(crate) fn response(&self) -> &WorkspaceRpcResponse {
        &self.response
    }

    #[cfg(test)]
    fn into_response(self) -> WorkspaceRpcResponse {
        self.response
    }
}

struct RootOwner {
    route: RootRoute,
    executor: Arc<dyn WorkspaceRequestExecutor>,
    runtime: Arc<ShardRuntime>,
}

#[derive(Default)]
pub struct RootOwnerRegistry {
    owners: RwLock<BTreeMap<RootIdentity, RootOwner>>,
    runtimes: RwLock<BTreeMap<nokv_protocol::LogicalShardIdentity, Arc<ShardRuntime>>>,
    owner_loss: OwnerLossSignal,
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
        if self.owner_loss.is_lost() {
            return Err(ServerError::InvalidRoute(
                "owner registry is fail-closed".to_owned(),
            ));
        }
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
        let runtime = {
            let mut runtimes = self.runtimes.write().map_err(|_| {
                ServerError::InvalidRoute("shard runtime registry lock is poisoned".to_owned())
            })?;
            Arc::clone(runtimes.entry(route.logical_shard_id).or_default())
        };
        owners.insert(
            route.root_id,
            RootOwner {
                route,
                executor,
                runtime,
            },
        );
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

    pub(crate) fn owner_loss_signal(&self) -> OwnerLossSignal {
        self.owner_loss.clone()
    }

    /// Stop admission for one complete logical shard and wait until every
    /// response admitted before the fence has finished delivery. The shared
    /// owner-loss signal also stops new work across this server process; the
    /// affected shard is the unit whose routes and response permits are
    /// synchronously fenced here.
    pub(crate) fn fail_closed_shard(
        &self,
        logical_shard_id: nokv_protocol::LogicalShardIdentity,
    ) -> Result<(), ServerError> {
        self.owner_loss.fail_closed();
        let runtime = {
            let mut runtimes = self.runtimes.write().map_err(|_| {
                ServerError::InvalidRoute("shard runtime registry lock is poisoned".to_owned())
            })?;
            Arc::clone(runtimes.entry(logical_shard_id).or_default())
        };
        self.fail_closed_runtime(logical_shard_id, &runtime)
    }

    fn fail_closed_runtime(
        &self,
        logical_shard_id: nokv_protocol::LogicalShardIdentity,
        runtime: &ShardRuntime,
    ) -> Result<(), ServerError> {
        runtime.close_admission();
        self.owner_loss.fail_closed();
        let route_removal = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
            .map(|mut owners| {
                owners.retain(|_, owner| owner.route.logical_shard_id != logical_shard_id);
            });
        runtime.wait_until_drained();
        route_removal
    }

    #[cfg(test)]
    pub(crate) fn dispatch(
        &self,
        request: WorkspaceRpcRequest,
    ) -> Result<WorkspaceRpcResponse, ServerError> {
        self.dispatch_guarded(request)
            .map(GuardedResponse::into_response)
    }

    pub(crate) fn dispatch_guarded(
        &self,
        request: WorkspaceRpcRequest,
    ) -> Result<GuardedResponse, ServerError> {
        if self.owner_loss.is_lost() {
            return Ok(GuardedResponse {
                response: not_owner_response(
                    request,
                    None,
                    "the server owner scope is fail-closed",
                ),
                _permit: None,
            });
        }
        let owners = self
            .owners
            .read()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let Some(owner) = owners.get(&request.route.root_id) else {
            return Ok(GuardedResponse {
                response: not_owner_response(
                    request,
                    None,
                    "this server does not own the requested root",
                ),
                _permit: None,
            });
        };
        if owner.route != request.route {
            return Ok(GuardedResponse {
                response: not_owner_response(
                    request,
                    Some(owner.route),
                    "the requested root route is stale",
                ),
                _permit: None,
            });
        }
        let route = owner.route;
        let executor = Arc::clone(&owner.executor);
        let runtime = Arc::clone(&owner.runtime);
        let Some(permit) = runtime.enter() else {
            return Ok(GuardedResponse {
                response: not_owner_response(
                    request,
                    None,
                    "the requested logical shard is fail-closed",
                ),
                _permit: None,
            });
        };
        let request_id = request.request_id;
        drop(owners);
        let outcome = executor.execute(&request);
        let fail_closed = matches!(
            &outcome,
            Err(RpcFailure {
                code: ErrorCode::NotOwner,
                conflict: Some(ConflictKind::RootPlacement),
                ..
            })
        );
        let response = match outcome {
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
        };
        if fail_closed {
            drop(permit);
            self.fail_closed_runtime(route.logical_shard_id, &runtime)?;
            return Ok(GuardedResponse {
                response,
                _permit: None,
            });
        }
        Ok(GuardedResponse {
            response,
            _permit: Some(permit),
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use nokv_meta::workspace as meta;
    use nokv_meta_holt::{HoltStore, TreeBinding};
    use nokv_meta_store::{
        Commit, ReadBatch, ReadSnapshot, StoreError, StoreProfile, TxnStore, UnknownCommit,
        WriteTxn,
    };
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

    struct PoisonOnceExecutor {
        calls: AtomicUsize,
    }

    struct SignalledPoisonExecutor {
        calls: AtomicUsize,
        poison_started: Mutex<mpsc::Sender<()>>,
    }

    impl WorkspaceRequestExecutor for SignalledPoisonExecutor {
        fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => EchoExecutor.execute(request),
                1 => {
                    self.poison_started.lock().unwrap().send(()).unwrap();
                    Err(RpcFailure {
                        code: ErrorCode::NotOwner,
                        message: "metadata store outcome is unknown and the shard is poisoned"
                            .to_owned(),
                        retryable: true,
                        conflict: Some(ConflictKind::RootPlacement),
                        current_generation: None,
                        route_hint: None,
                    })
                }
                _ => panic!("fail-closed shard dispatched another request"),
            }
        }
    }

    struct FaultingCommitStore {
        inner: Arc<dyn TxnStore>,
        failure: Mutex<Option<StoreError>>,
        reads: AtomicUsize,
        commits: AtomicUsize,
    }

    impl TxnStore for FaultingCommitStore {
        fn profile(&self) -> StoreProfile {
            self.inner.profile()
        }

        fn read(&self, batch: ReadBatch) -> Result<ReadSnapshot, StoreError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.inner.read(batch)
        }

        fn commit(&self, txn: WriteTxn) -> Result<Commit, StoreError> {
            self.commits.fetch_add(1, Ordering::SeqCst);
            if let Some(failure) = self.failure.lock().unwrap().take() {
                return Err(failure);
            }
            self.inner.commit(txn)
        }

        fn ready(&self) -> Result<(), StoreError> {
            self.inner.ready()
        }
    }

    impl WorkspaceRequestExecutor for PoisonOnceExecutor {
        fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst);
            if call == 0 {
                return Err(RpcFailure {
                    code: ErrorCode::NotOwner,
                    message: "metadata store outcome is unknown and the shard is poisoned"
                        .to_owned(),
                    retryable: true,
                    conflict: Some(ConflictKind::RootPlacement),
                    current_generation: None,
                    route_hint: None,
                });
            }
            EchoExecutor.execute(request)
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

    fn active_meta_with_commit_failure(
        installed: RootRoute,
        failure: StoreError,
    ) -> (Arc<meta::MetaShard>, Arc<FaultingCommitStore>) {
        let catalog = meta::keyspaces()
            .iter()
            .map(|definition| TreeBinding::new(definition.id, definition.name));
        let base: Arc<dyn TxnStore> = Arc::new(
            HoltStore::memory(catalog, meta::store_limits())
                .expect("create in-memory metadata store"),
        );
        let shard_id = nokv_types::LogicalShardId::from(installed.logical_shard_id);
        let initializing = meta::MetaShard::initialize(Arc::clone(&base), shard_id)
            .expect("initialize metadata shard");
        let owner_epoch = nokv_types::OwnerEpoch::new(installed.owner_epoch).unwrap();
        initializing
            .advance_owner_epoch(None, owner_epoch)
            .expect("install owner epoch");
        for (request_byte, action) in [
            (9, meta::RootFenceAction::Install),
            (
                10,
                meta::RootFenceAction::Transition {
                    expected: nokv_types::RootActivationState::Installing,
                    next: nokv_types::RootActivationState::Active,
                },
            ),
        ] {
            initializing
                .execute(
                    &meta::MetadataCommand {
                        schema_id: meta::SCHEMA_ID.to_owned(),
                        root_id: installed.root_id.into(),
                        logical_shard_id: shard_id,
                        placement_generation: nokv_types::PlacementGeneration::new(
                            installed.placement_generation,
                        )
                        .unwrap(),
                        owner_epoch,
                        request_id: nokv_types::RequestId::from_bytes([request_byte; 16]),
                        command_digest: nokv_types::CommandDigest::from_bytes(
                            [0; nokv_types::SHA256_BYTES],
                        ),
                        read_version: initializing.current_read_version().unwrap(),
                        root_fence_action: action,
                        predicates: Vec::new(),
                        mutations: Vec::new(),
                        history_projection: Vec::new(),
                        event_projection: Vec::new(),
                        deterministic_result: Vec::new(),
                    }
                    .seal(),
                )
                .expect("activate root fence");
        }
        drop(initializing);
        let faulting = Arc::new(FaultingCommitStore {
            inner: base,
            failure: Mutex::new(Some(failure)),
            reads: AtomicUsize::new(0),
            commits: AtomicUsize::new(0),
        });
        let store: Arc<dyn TxnStore> = faulting.clone();
        let meta = Arc::new(meta::MetaShard::open(store, shard_id).expect("reopen metadata shard"));
        (meta, faulting)
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

    #[test]
    fn poisoned_outcome_fail_closes_every_route_before_a_second_dispatch() {
        let registry = RootOwnerRegistry::new();
        let executor = Arc::new(PoisonOnceExecutor {
            calls: AtomicUsize::new(0),
        });
        let first = route(8);
        let second = RootRoute {
            root_id: RootIdentity([7; 16]),
            ..first
        };
        registry.install(first, executor.clone()).unwrap();
        registry.install(second, executor.clone()).unwrap();

        let response = registry.dispatch(request(first)).unwrap();
        assert!(matches!(
            response.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                conflict: Some(ConflictKind::RootPlacement),
                ..
            })
        ));
        assert_eq!(registry.installed_root_count().unwrap(), 0);

        let response = registry.dispatch(request(second)).unwrap();
        assert!(matches!(
            response.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn shard_poison_stops_new_dispatch_across_the_process_owner_scope() {
        let registry = RootOwnerRegistry::new();
        let poisoned_executor = Arc::new(PoisonOnceExecutor {
            calls: AtomicUsize::new(0),
        });
        let healthy_executor = Arc::new(PoisonOnceExecutor {
            calls: AtomicUsize::new(0),
        });
        let poisoned = route(8);
        let healthy = RootRoute {
            root_id: RootIdentity([8; 16]),
            logical_shard_id: LogicalShardIdentity([9; 16]),
            ..poisoned
        };
        registry
            .install(poisoned, poisoned_executor.clone())
            .unwrap();
        registry.install(healthy, healthy_executor.clone()).unwrap();

        let failure = registry.dispatch(request(poisoned)).unwrap();
        assert!(matches!(
            failure.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                conflict: Some(ConflictKind::RootPlacement),
                ..
            })
        ));
        let rejected = registry.dispatch(request(healthy)).unwrap();
        assert!(matches!(
            rejected.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        assert_eq!(healthy_executor.calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn install_and_dispatch_race_cannot_reopen_a_fail_closed_registry() {
        let registry = Arc::new(RootOwnerRegistry::new());
        let poisoned_executor = Arc::new(PoisonOnceExecutor {
            calls: AtomicUsize::new(0),
        });
        let installed = route(8);
        registry
            .install(installed, poisoned_executor.clone())
            .unwrap();

        let registry_for_install = Arc::clone(&registry);
        let (start_tx, start_rx) = mpsc::channel();
        let concurrent = RootRoute {
            root_id: RootIdentity([10; 16]),
            logical_shard_id: LogicalShardIdentity([11; 16]),
            ..installed
        };
        let installer = std::thread::spawn(move || {
            start_rx.recv().unwrap();
            registry_for_install.install(concurrent, Arc::new(EchoExecutor))
        });
        start_tx.send(()).unwrap();
        let poison = registry.dispatch(request(installed)).unwrap();
        assert!(matches!(
            poison.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));

        let install = installer.join().unwrap();
        if install.is_ok() {
            let rejected = registry.dispatch(request(concurrent)).unwrap();
            assert!(matches!(
                rejected.outcome,
                WorkspaceRpcOutcome::Failure(RpcFailure {
                    code: ErrorCode::NotOwner,
                    ..
                })
            ));
        }
        assert!(registry.owner_loss_signal().is_lost());
        assert!(registry
            .install(
                RootRoute {
                    root_id: RootIdentity([12; 16]),
                    ..concurrent
                },
                Arc::new(EchoExecutor),
            )
            .is_err());
    }

    #[test]
    fn poison_fence_waits_for_an_admitted_response_to_finish_delivery() {
        let registry = Arc::new(RootOwnerRegistry::new());
        let (poison_started_tx, poison_started_rx) = mpsc::channel();
        let executor = Arc::new(SignalledPoisonExecutor {
            calls: AtomicUsize::new(0),
            poison_started: Mutex::new(poison_started_tx),
        });
        let installed = route(8);
        registry.install(installed, executor.clone()).unwrap();

        let admitted = registry.dispatch_guarded(request(installed)).unwrap();
        assert!(matches!(
            admitted.response().outcome,
            WorkspaceRpcOutcome::Success(_)
        ));

        let registry_for_poison = Arc::clone(&registry);
        let (completed_tx, completed_rx) = mpsc::channel();
        let poison = std::thread::spawn(move || {
            completed_tx
                .send(registry_for_poison.dispatch(request(installed)))
                .unwrap();
        });
        poison_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !registry.owner_loss_signal().is_lost() && Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert!(registry.owner_loss_signal().is_lost());
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        let rejected = registry.dispatch(request(installed)).unwrap();
        assert!(matches!(
            rejected.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        assert_eq!(executor.calls.load(Ordering::SeqCst), 2);

        let registry_for_repeated_fence = Arc::clone(&registry);
        let (repeated_started_tx, repeated_started_rx) = mpsc::channel();
        let (repeated_tx, repeated_rx) = mpsc::channel();
        let repeated_fence = std::thread::spawn(move || {
            repeated_started_tx.send(()).unwrap();
            repeated_tx
                .send(registry_for_repeated_fence.fail_closed_shard(installed.logical_shard_id))
                .unwrap();
        });
        repeated_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            repeated_rx.recv_timeout(Duration::from_millis(20)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(admitted);
        let poisoned = completed_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert!(matches!(
            poisoned.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                conflict: Some(ConflictKind::RootPlacement),
                ..
            })
        ));
        poison.join().unwrap();
        repeated_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        repeated_fence.join().unwrap();
    }

    #[test]
    fn definitely_not_applied_failure_remains_request_local() {
        let registry = RootOwnerRegistry::new();
        let installed = route(8);
        let (meta, store) = active_meta_with_commit_failure(
            installed,
            StoreError::Unavailable("injected definitely-not-applied outcome".to_owned()),
        );
        registry
            .install(
                installed,
                Arc::new(crate::MetadataWorkspaceRequestExecutor::new(meta)),
            )
            .unwrap();

        let first = registry.dispatch(request(installed)).unwrap();
        assert!(matches!(
            first.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::Internal,
                retryable: true,
                ..
            })
        ));
        assert_eq!(registry.installed_root_count().unwrap(), 1);
        assert!(!registry.owner_loss_signal().is_lost());
        assert_eq!(store.commits.load(Ordering::SeqCst), 1);

        let second = registry.dispatch(request(installed)).unwrap();
        assert!(matches!(second.outcome, WorkspaceRpcOutcome::Success(_)));
        assert!(store.commits.load(Ordering::SeqCst) > 1);
        assert_eq!(registry.installed_root_count().unwrap(), 1);
    }

    #[test]
    fn settled_and_may_commit_fail_close_before_follow_up_physical_work() {
        for (state, state_message) in [
            (UnknownCommit::Settled, "settled"),
            (UnknownCommit::MayCommit, "may still commit"),
        ] {
            let registry = RootOwnerRegistry::new();
            let installed = route(8);
            let (meta, store) = active_meta_with_commit_failure(
                installed,
                StoreError::OutcomeUnknown {
                    state,
                    reason: "injected acknowledgement loss".to_owned(),
                },
            );
            registry
                .install(
                    installed,
                    Arc::new(crate::MetadataWorkspaceRequestExecutor::new(meta)),
                )
                .unwrap();

            let first = registry.dispatch(request(installed)).unwrap();
            let reads_after_unknown = store.reads.load(Ordering::SeqCst);
            let second = registry.dispatch(request(installed)).unwrap();

            assert_eq!(store.commits.load(Ordering::SeqCst), 1, "{state:?}");
            assert_eq!(
                store.reads.load(Ordering::SeqCst),
                reads_after_unknown,
                "{state:?}"
            );
            assert!(matches!(
                &first.outcome,
                WorkspaceRpcOutcome::Failure(RpcFailure {
                    code: ErrorCode::NotOwner,
                    conflict: Some(ConflictKind::RootPlacement),
                    ..
                })
            ));
            let WorkspaceRpcOutcome::Failure(first_failure) = &first.outcome else {
                unreachable!("first outcome was already checked as failure")
            };
            assert!(first_failure.message.contains(state_message));
            assert!(matches!(
                second.outcome,
                WorkspaceRpcOutcome::Failure(RpcFailure {
                    code: ErrorCode::NotOwner,
                    ..
                })
            ));
            assert_eq!(registry.installed_root_count().unwrap(), 0);
            assert!(registry.owner_loss_signal().is_lost());
        }
    }

    #[test]
    fn real_poisoned_store_error_removes_the_route_before_any_follow_up_read() {
        let registry = RootOwnerRegistry::new();
        let installed = route(8);
        let (meta, store) = active_meta_with_commit_failure(
            installed,
            StoreError::OutcomeUnknown {
                state: UnknownCommit::Poisoned,
                reason: "injected acknowledgement loss".to_owned(),
            },
        );
        registry
            .install(
                installed,
                Arc::new(crate::MetadataWorkspaceRequestExecutor::new(meta)),
            )
            .unwrap();

        let first = registry.dispatch(request(installed)).unwrap();
        assert!(matches!(
            first.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                conflict: Some(ConflictKind::RootPlacement),
                ..
            })
        ));
        assert_eq!(store.commits.load(Ordering::SeqCst), 1);
        let reads_after_poison = store.reads.load(Ordering::SeqCst);

        let second = registry.dispatch(request(installed)).unwrap();
        assert!(matches!(
            second.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        assert_eq!(store.commits.load(Ordering::SeqCst), 1);
        assert_eq!(store.reads.load(Ordering::SeqCst), reads_after_poison);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        assert!(registry.owner_loss_signal().is_lost());
    }
}
