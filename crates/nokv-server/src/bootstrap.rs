/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::Arc;

use nokv_control::{
    ControlError, ControlStore, LogicalShardLease, LogicalShardRecord, MetadataAuthorityRecord,
    NodeId, OwnerEpoch, OwnerIncarnationId, OwnerLeaseModel, OwnerServingAdmission,
    RecoveryPublication, RootId, RootLayoutGeneration, RootLayoutProfile, RootPartitionId,
    RootPlacement, RootPlacementLifecycle,
};
use nokv_meta::workspace as meta;
use nokv_protocol::{LogicalShardIdentity, RootIdentity, RootRoute};
use nokv_types::{CommandDigest, RequestId, RootActivationState, SHA256_BYTES};

use crate::{
    AdmissionCode, LifecycleTransition, MetadataWorkspaceRequestExecutor, OpenIntent,
    ResolvedRuntime, RootOwnerRegistry, RuntimeDescriptor, RuntimeLifecycleValidationError,
    ServerError, WorkspaceRequestExecutor,
};

use crate::registry::{
    OwnerAdmissionWritePermit, OwnerCandidateRuntimeValidator, OwnerCandidateToken,
    OwnerReleaseState, ShardBootstrapReservation,
};

const OWNER_LEASE_EXPIRY_ADMISSION_QUALIFIED: bool = false;
const OWNER_LEASE_EXPIRY_ADMISSION_NOT_QUALIFIED: &str =
    "owner_lease_expiry_admission_not_qualified";

struct BootstrapRuntimeBinding {
    runtime: ResolvedRuntime,
    store: Arc<meta::AgentMetadataStore>,
    lease: LogicalShardLease,
}

impl OwnerCandidateRuntimeValidator for BootstrapRuntimeBinding {
    fn validate(&self) -> bool {
        inspect_runtime_binding(&self.runtime, &self.store).is_ok()
    }

    fn poison(&self) {
        self.runtime.poison_lifecycle();
    }

    fn persist_release_receipt(&self) -> Result<(), ServerError> {
        persist_release_receipt(&self.runtime, &self.lease)
    }
}

/// Exact control-plane admission used by one root-owner bootstrap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnerAdmission {
    Acquire {
        owner: NodeId,
        owner_incarnation_id: OwnerIncarnationId,
        endpoint: String,
        expected_previous_epoch: Option<OwnerEpoch>,
    },
    Resume {
        lease: LogicalShardLease,
    },
    ResumePreparedOrAcquireSuccessor {
        lease: LogicalShardLease,
        endpoint: String,
    },
}

#[derive(Clone)]
pub struct RootOwnerBootstrapRequest {
    pub root_id: RootId,
    pub runtime: ResolvedRuntime,
    pub open_intent: OpenIntent,
    pub admission: OwnerAdmission,
    pub install_request_id: RequestId,
    pub activate_request_id: RequestId,
    pub recovery: RecoveryPublication,
}

/// Release-only recovery capability returned when bootstrap cannot prove that
/// its newly acquired exact owner lease reached a terminal control state.
/// It deliberately exposes no acquire, resume, provider-open, or route API.
pub struct PendingOwnerRelease {
    control: Arc<dyn ControlStore>,
    runtime: ResolvedRuntime,
    lease: LogicalShardLease,
    guard: std::sync::Mutex<PendingOwnerReleaseGuard>,
}

enum PendingOwnerReleaseGuard {
    Reservation(ShardBootstrapReservation),
    Candidate {
        registry: Arc<RootOwnerRegistry>,
        candidate: OwnerCandidateToken,
    },
    Terminal(nokv_control::OwnerReleaseOutcome),
}

enum PendingOwnerReleaseAttempt {
    Terminal(nokv_control::OwnerReleaseOutcome),
    Unknown,
    Control(ControlError),
}

impl PendingOwnerRelease {
    pub fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub fn retry(&self) -> Result<nokv_control::OwnerReleaseOutcome, ServerError> {
        match self.attempt()? {
            PendingOwnerReleaseAttempt::Terminal(outcome) => Ok(outcome),
            PendingOwnerReleaseAttempt::Unknown => {
                Ok(nokv_control::OwnerReleaseOutcome::OutcomeUnknown)
            }
            PendingOwnerReleaseAttempt::Control(error) => Err(ServerError::Control(error)),
        }
    }

    fn attempt(&self) -> Result<PendingOwnerReleaseAttempt, ServerError> {
        let mut guard = self.guard.lock().map_err(|_| {
            ServerError::InvalidRoute("pending owner release capability is poisoned".to_owned())
        })?;
        if let PendingOwnerReleaseGuard::Terminal(outcome) = &*guard {
            return Ok(PendingOwnerReleaseAttempt::Terminal(outcome.clone()));
        }

        let outcome = match &mut *guard {
            PendingOwnerReleaseGuard::Reservation(reservation) => {
                if !reservation.is_current()? {
                    return Err(ServerError::InvalidRoute(
                        "owner release reservation is no longer current".to_owned(),
                    ));
                }
                persist_release_receipt(&self.runtime, &self.lease)?;
                self.control.release_owner(&self.lease)
            }
            PendingOwnerReleaseGuard::Candidate {
                registry,
                candidate,
            } => {
                let mut admission = candidate.write_admission()?;
                match candidate.release_state()? {
                    OwnerReleaseState::Active => {
                        if !registry
                            .close_pending_for_release_with_admission(candidate, &mut admission)?
                        {
                            return Err(ServerError::InvalidRoute(
                                "owner release candidate is no longer the exact pending route"
                                    .to_owned(),
                            ));
                        }
                    }
                    OwnerReleaseState::ReleasePending => {
                        if !registry
                            .contains_release_tombstone_with_admission(candidate, &admission)?
                        {
                            return Err(ServerError::InvalidRoute(
                                "owner release candidate tombstone is no longer current".to_owned(),
                            ));
                        }
                        candidate.persist_release_receipt()?;
                    }
                    OwnerReleaseState::Released(record) | OwnerReleaseState::Superseded(record) => {
                        return Ok(PendingOwnerReleaseAttempt::Terminal(
                            nokv_control::OwnerReleaseOutcome::AlreadyReleased(record),
                        ));
                    }
                }
                let outcome = self.control.release_owner(&self.lease);
                if let Ok(outcome) = &outcome {
                    let state = candidate.finish_release(outcome.clone())?;
                    if matches!(
                        state,
                        OwnerReleaseState::Released(_) | OwnerReleaseState::Superseded(_)
                    ) {
                        let _ =
                            registry.remove_candidate_with_admission(candidate, &mut admission)?;
                    }
                }
                outcome
            }
            PendingOwnerReleaseGuard::Terminal(_) => unreachable!("terminal guard returned above"),
        };

        match outcome {
            Ok(nokv_control::OwnerReleaseOutcome::OutcomeUnknown) => {
                Ok(PendingOwnerReleaseAttempt::Unknown)
            }
            Ok(outcome) => {
                *guard = PendingOwnerReleaseGuard::Terminal(outcome.clone());
                Ok(PendingOwnerReleaseAttempt::Terminal(outcome))
            }
            Err(error) => Ok(PendingOwnerReleaseAttempt::Control(error)),
        }
    }
}

impl fmt::Debug for PendingOwnerRelease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PendingOwnerRelease")
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

/// Control-backed ownership token retained by the serving runtime.
///
/// A failed renewal immediately removes the exact local route before exposing
/// the lease failure to the caller. The runtime can then stop accepting work
/// without relying on a stale in-memory route.
#[derive(Clone)]
pub struct ControlBackedRootOwner {
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    route: RootRoute,
    candidate: OwnerCandidateToken,
    lease: LogicalShardLease,
    admission: OwnerServingAdmission,
    runtime: ResolvedRuntime,
    store: Arc<meta::AgentMetadataStore>,
}

impl ControlBackedRootOwner {
    pub fn route(&self) -> RootRoute {
        self.route
    }

    pub fn lease(&self) -> &LogicalShardLease {
        &self.lease
    }

    pub fn is_for_registry(&self, registry: &Arc<RootOwnerRegistry>) -> bool {
        Arc::ptr_eq(&self.registry, registry)
    }

    pub(crate) fn is_active_candidate(&self) -> Result<bool, ServerError> {
        if let Err(error) = inspect_runtime_binding(&self.runtime, &self.store) {
            self.registry.terminate_candidate(&self.candidate)?;
            let mut admission = self.candidate.write_admission()?;
            return match self.reconcile_release(&mut admission, Some(error.to_string())) {
                Ok(_) => Err(error),
                Err(release) => Err(release),
            };
        }
        self.registry.contains_candidate(&self.candidate)
    }

    pub(crate) fn candidate_token(&self) -> OwnerCandidateToken {
        self.candidate.clone()
    }

    /// Renew the exact owner session. Any failure permanently closes local
    /// admission and reconciles only the exact control-plane release.
    pub fn renew_or_uninstall(&self) -> Result<LogicalShardRecord, ServerError> {
        if self.candidate.release_state()? != OwnerReleaseState::Active {
            return match self.release() {
                Ok(_) => Err(ServerError::InvalidRoute(
                    "owner candidate is terminal and cannot renew".to_owned(),
                )),
                Err(error) => Err(error),
            };
        }
        let mut admission = self.candidate.write_admission()?;
        if self.candidate.release_state()? != OwnerReleaseState::Active {
            return match self.reconcile_release(&mut admission, None) {
                Ok(_) => Err(ServerError::InvalidRoute(
                    "owner candidate is terminal and cannot renew".to_owned(),
                )),
                Err(error) => Err(error),
            };
        }
        if admission.is_terminal()? {
            return Err(self.close_after_failure(
                &mut admission,
                ServerError::InvalidRoute(
                    "owner candidate is terminal and can only retry exact release".to_owned(),
                ),
            ));
        }
        if !self
            .registry
            .contains_candidate_with_admission(&self.candidate, &admission)?
        {
            return Err(ServerError::InvalidRoute(
                "owner candidate is no longer the active local route".to_owned(),
            ));
        }
        if let Err(primary) = inspect_runtime_binding(&self.runtime, &self.store) {
            return Err(self.close_after_failure(&mut admission, primary));
        }
        let record = match self.control.renew_owner(&self.lease, &self.admission) {
            Ok(record) => record,
            Err(error) => {
                return Err(self.close_after_failure(&mut admission, ServerError::Control(error)));
            }
        };
        if let Err(primary) = inspect_runtime_binding(&self.runtime, &self.store) {
            return Err(self.close_after_failure(&mut admission, primary));
        }
        Ok(record)
    }

    /// Stop local admission and release the exact control-plane owner session.
    pub fn release(&self) -> Result<LogicalShardRecord, ServerError> {
        let mut admission = self.candidate.write_admission()?;
        if self.candidate.release_state()? == OwnerReleaseState::Active {
            if !self
                .registry
                .contains_candidate_with_admission(&self.candidate, &admission)?
            {
                return Err(ServerError::InvalidRoute(
                    "owner candidate is no longer the active local route".to_owned(),
                ));
            }
            if let Err(error) = persist_release_receipt(&self.runtime, &self.lease) {
                self.candidate.flag_terminal()?;
                return Err(error);
            }
            self.candidate.flag_terminal()?;
            self.candidate.begin_release()?;
            self.candidate.poison_runtime();
            if !self
                .registry
                .deactivate_candidate_with_admission(&self.candidate, &mut admission)?
            {
                return Err(ServerError::InvalidRoute(
                    "owner candidate changed before control release".to_owned(),
                ));
            }
        }
        self.reconcile_release(&mut admission, None)
    }

    fn close_after_failure(
        &self,
        admission: &mut OwnerAdmissionWritePermit<'_>,
        primary: ServerError,
    ) -> ServerError {
        let primary_message = primary.to_string();
        if let Err(receipt) = persist_release_receipt(&self.runtime, &self.lease) {
            let terminal = self.candidate.flag_terminal();
            return match terminal {
                Ok(()) => ServerError::BootstrapRollback {
                    primary: primary_message,
                    rollback: format!(
                        "persist release-only receipt before local close: {receipt}"
                    ),
                },
                Err(terminal) => ServerError::BootstrapRollback {
                    primary: primary_message,
                    rollback: format!(
                        "persist release-only receipt: {receipt}; flag candidate terminal: {terminal}"
                    ),
                },
            };
        }
        let mut rollback = Vec::new();
        if let Err(error) = self.candidate.flag_terminal() {
            rollback.push(format!("flag candidate terminal: {error}"));
        }
        if let Err(error) = self.candidate.begin_release() {
            rollback.push(format!("retain exact release capability: {error}"));
        }
        self.candidate.poison_runtime();
        if let Err(error) = self
            .registry
            .deactivate_candidate_with_admission(&self.candidate, admission)
        {
            rollback.push(format!("deactivate candidate: {error}"));
        }
        match self.reconcile_release(admission, Some(primary_message.clone())) {
            Ok(_) if rollback.is_empty() => primary,
            Ok(_) => ServerError::BootstrapRollback {
                primary: primary_message,
                rollback: rollback.join("; "),
            },
            Err(error @ ServerError::OwnerReleasePending { .. })
            | Err(error @ ServerError::OwnerReleaseRetryable { .. }) => error,
            Err(error) => {
                rollback.push(format!("reconcile exact owner release: {error}"));
                ServerError::BootstrapRollback {
                    primary: primary_message,
                    rollback: rollback.join("; "),
                }
            }
        }
    }

    fn reconcile_release(
        &self,
        admission: &mut OwnerAdmissionWritePermit<'_>,
        primary: Option<String>,
    ) -> Result<LogicalShardRecord, ServerError> {
        match self.candidate.release_state()? {
            OwnerReleaseState::Released(record) | OwnerReleaseState::Superseded(record) => {
                let _ = self
                    .registry
                    .remove_candidate_with_admission(&self.candidate, admission)?;
                return Ok(record);
            }
            OwnerReleaseState::Active => {
                return Err(ServerError::InvalidRoute(
                    "owner release was attempted before local admission closed".to_owned(),
                ));
            }
            OwnerReleaseState::ReleasePending => {}
        }

        // The closed registry tombstone is part of the release capability.
        // A lease alone cannot distinguish an old process candidate from a
        // later exact-resume candidate carrying the same control token.
        if !self
            .registry
            .contains_release_tombstone_with_admission(&self.candidate, admission)?
        {
            return Err(ServerError::InvalidRoute(
                "exact owner release tombstone is no longer current".to_owned(),
            ));
        }

        let outcome = match self.control.release_owner(&self.lease) {
            Ok(outcome) => outcome,
            Err(error) => {
                return Err(ServerError::OwnerReleaseRetryable {
                    primary,
                    lease: Box::new(self.lease.clone()),
                    error: Box::new(error),
                });
            }
        };
        let state = self.candidate.finish_release(outcome)?;
        let record = match state {
            OwnerReleaseState::Released(record) | OwnerReleaseState::Superseded(record) => record,
            OwnerReleaseState::ReleasePending => {
                return Err(ServerError::OwnerReleasePending {
                    primary,
                    lease: Box::new(self.lease.clone()),
                });
            }
            OwnerReleaseState::Active => {
                return Err(ServerError::InvalidRoute(
                    "owner release returned to Active unexpectedly".to_owned(),
                ));
            }
        };
        let _ = self
            .registry
            .remove_candidate_with_admission(&self.candidate, admission)?;
        Ok(record)
    }
}

/// Fully activated owner state returned to the server runtime.
pub struct BootstrappedRootOwner {
    pub route: RootRoute,
    pub lease: LogicalShardLease,
    pub serving_record: LogicalShardRecord,
    store: Arc<meta::AgentMetadataStore>,
    #[cfg(test)]
    executor: Arc<MetadataWorkspaceRequestExecutor>,
    pub ownership: ControlBackedRootOwner,
    runtime: ResolvedRuntime,
}

impl BootstrappedRootOwner {
    pub fn runtime_descriptor(&self) -> &RuntimeDescriptor {
        self.runtime.descriptor()
    }

    pub fn lifecycle_runner(
        &self,
        owner_loss: crate::OwnerLossSignal,
        objects: Arc<dyn crate::LifecycleObjectDeleter>,
        options: crate::LifecycleRunnerOptions,
    ) -> Result<crate::LifecycleRunner, crate::LifecycleError> {
        crate::LifecycleRunner::new_control_backed(
            Arc::clone(&self.store),
            Arc::clone(&self.ownership.registry),
            &self.ownership,
            owner_loss,
            objects,
            options,
        )
    }
}

/// Validate one requested owner-admission transition before any control,
/// provider, journal, locator, or registry side effect.
///
/// All six acquisition and resume transitions are currently fail-closed with a
/// stable [`AdmissionCode`]. They remain unqualified until bootstrap can durably
/// bind a planned exact owner incarnation before its control-plane CAS and can
/// reconcile every unknown acquisition outcome after process restart.
pub fn bootstrap_root_owner(
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    request: RootOwnerBootstrapRequest,
) -> Result<BootstrappedRootOwner, ServerError> {
    let transition = lifecycle_transition(request.open_intent, &request.admission)?;
    reject_unqualified_owner_admission_transition(transition)?;
    bootstrap_root_owner_after_admission(control, registry, request, transition)
}

fn bootstrap_root_owner_after_admission(
    control: Arc<dyn ControlStore>,
    registry: Arc<RootOwnerRegistry>,
    request: RootOwnerBootstrapRequest,
    transition: LifecycleTransition,
) -> Result<BootstrappedRootOwner, ServerError> {
    if request.install_request_id == request.activate_request_id {
        return Err(ServerError::InvalidBootstrap(
            "install and activate request ids must differ".to_owned(),
        ));
    }
    validate_owner_lease_model_before_control_read_v1(control.owner_lease_model())?;
    request
        .runtime
        .descriptor()
        .classify_bootstrap(request.open_intent, transition)
        .map_err(|error| ServerError::InvalidBootstrap(error.to_string()))?;
    request
        .runtime
        .preflight_owner_release()
        .map_err(ServerError::OwnerReleaseReceipt)?;
    let placement = control
        .get_root_placement(&request.root_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("root placement does not exist".to_owned()))?;
    validate_serving_placement(&placement)?;
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("logical shard does not exist".to_owned()))?;
    let authority = control
        .get_metadata_authority(&placement.logical_shard_id)?
        .ok_or_else(|| {
            ServerError::InvalidBootstrap(
                "logical shard has no metadata authority; implicit store adoption is forbidden"
                    .to_owned(),
            )
        })?;
    if authority.logical_shard_id != placement.logical_shard_id
        || shard.logical_shard_id != placement.logical_shard_id
    {
        return Err(ServerError::InvalidBootstrap(
            "placement, logical shard, and metadata authority identities differ".to_owned(),
        ));
    }
    let expected_store_identity = request
        .runtime
        .descriptor()
        .validate_authority(&authority)?;
    validate_migration_admission(&authority)?;
    let serving_admission = OwnerServingAdmission::stable(placement.clone(), authority.clone())?;
    let bootstrap_reservation = registry
        .reserve_logical_shard_bootstrap(LogicalShardIdentity::from(placement.logical_shard_id))?;
    reject_unrecovered_shared_frontier(control.as_ref(), &placement, &request.recovery)?;
    validate_requested_lease_authority(&request.admission, &authority)?;
    validate_runtime(&request.runtime)?;

    let allow_skipped_owner_epochs = matches!(
        transition,
        LifecycleTransition::SuccessorReopen
            | LifecycleTransition::PreparedSuccessorCreate
            | LifecycleTransition::PreparedResumeOrSuccessor
    );
    let runtime = request.runtime.clone();
    // Descriptor, placement, authority, migration, frontier, local reservation,
    // provider offer, locator, journal, and lifecycle admission are pure
    // preflights above. The exact control owner is the first durable mutation:
    // no provider create or reopen may run before this CAS or exact renewal.
    let (lease, lease_provenance) =
        admit_owner(control.as_ref(), &serving_admission, &request.admission)?;
    let route = root_route(&placement, &lease);
    if lease.authority != authority.fence() {
        return Err(rollback_bootstrap(
            ServerError::InvalidBootstrap(
                "admitted owner lease carries a different metadata authority".to_owned(),
            ),
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            None,
            lease_provenance,
        ));
    }

    let store = match runtime.open_store(request.open_intent, expected_store_identity) {
        Ok(store) => Arc::new(store),
        Err(error) => {
            let error = match error {
                crate::runtime_registry::RuntimeOpenError::Runtime(error) => {
                    ServerError::InvalidBootstrap(error.to_string())
                }
                crate::runtime_registry::RuntimeOpenError::Metadata(error) => {
                    ServerError::Metadata(error)
                }
            };
            return Err(rollback_bootstrap(
                error,
                &control,
                &registry,
                &runtime,
                bootstrap_reservation,
                &lease,
                None,
                lease_provenance,
            ));
        }
    };
    if store.metadata_store_identity() != expected_store_identity {
        return Err(rollback_bootstrap(
            ServerError::InvalidBootstrap(
                "opened metadata store identity differs from the admitted authority".to_owned(),
            ),
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            None,
            lease_provenance,
        ));
    }
    match store.metadata_authority_state() {
        Ok(meta::MetadataAuthorityState::Active) => {}
        Ok(state) => {
            return Err(rollback_bootstrap(
                ServerError::InvalidBootstrap(format!(
                    "metadata store authority state is {state:?}, expected Active"
                )),
                &control,
                &registry,
                &runtime,
                bootstrap_reservation,
                &lease,
                None,
                lease_provenance,
            ));
        }
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::Metadata(error),
                &control,
                &registry,
                &runtime,
                bootstrap_reservation,
                &lease,
                None,
                lease_provenance,
            ));
        }
    }
    if let Err(error) = inspect_runtime_binding(&runtime, &store) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            None,
            lease_provenance,
        ));
    }

    if let Err(error) = activate_root_fence(
        &store,
        &placement,
        &lease,
        request.install_request_id,
        request.activate_request_id,
        allow_skipped_owner_epochs,
    ) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            None,
            lease_provenance,
        ));
    }
    if let Err(error) = inspect_runtime_binding(&runtime, &store) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            None,
            lease_provenance,
        ));
    }

    let executor = Arc::new(MetadataWorkspaceRequestExecutor::new(Arc::clone(&store)));
    if let Err(error) = inspect_runtime_binding(&runtime, &store) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            None,
            lease_provenance,
        ));
    }
    let installed_executor: Arc<dyn WorkspaceRequestExecutor> = executor.clone();
    let candidate_runtime: Arc<dyn OwnerCandidateRuntimeValidator> =
        Arc::new(BootstrapRuntimeBinding {
            runtime: runtime.clone(),
            store: Arc::clone(&store),
            lease: lease.clone(),
        });
    let pending_owner = match registry.install_pending(
        &bootstrap_reservation,
        route,
        installed_executor,
        candidate_runtime,
    ) {
        Ok(token) => token,
        Err(error) => {
            return Err(rollback_bootstrap(
                error,
                &control,
                &registry,
                &runtime,
                bootstrap_reservation,
                &lease,
                None,
                lease_provenance,
            ));
        }
    };
    if let Err(error) = inspect_runtime_binding(&runtime, &store) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            Some(pending_owner),
            lease_provenance,
        ));
    }
    if let Err(error) = control.renew_owner(&lease, &serving_admission) {
        return Err(rollback_bootstrap(
            ServerError::Control(error),
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            Some(pending_owner),
            lease_provenance,
        ));
    }
    if let Err(error) = inspect_runtime_binding(&runtime, &store) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            Some(pending_owner),
            lease_provenance,
        ));
    }
    let serving_record = match control.mark_serving(&lease, &serving_admission, request.recovery) {
        Ok(record) => record,
        Err(error) => {
            return Err(rollback_bootstrap(
                ServerError::Control(error),
                &control,
                &registry,
                &runtime,
                bootstrap_reservation,
                &lease,
                Some(pending_owner),
                lease_provenance,
            ));
        }
    };
    if let Err(error) = inspect_runtime_binding(&runtime, &store) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            Some(pending_owner),
            lease_provenance,
        ));
    }
    if let Err(error) = registry.activate(&pending_owner) {
        return Err(rollback_bootstrap(
            error,
            &control,
            &registry,
            &runtime,
            bootstrap_reservation,
            &lease,
            Some(pending_owner),
            lease_provenance,
        ));
    }
    let ownership = ControlBackedRootOwner {
        control,
        registry,
        route,
        candidate: pending_owner,
        lease: lease.clone(),
        admission: serving_admission,
        runtime: runtime.clone(),
        store: Arc::clone(&store),
    };
    Ok(BootstrappedRootOwner {
        route,
        lease,
        serving_record,
        store,
        #[cfg(test)]
        executor,
        ownership,
        runtime,
    })
}

/// Reject an owner-session lifetime model before the caller reads any durable
/// control record.
///
/// `ControlStore::owner_lease_model` is a local capability declaration. It
/// must not perform backend I/O. CLI and embedding entry points call this
/// preflight immediately after constructing the control store, while the
/// bootstrap path repeats it defensively before any owner mutation.
pub fn validate_owner_lease_model_before_control_read_v1(
    model: OwnerLeaseModel,
) -> Result<(), ServerError> {
    match model {
        OwnerLeaseModel::NonExpiring => Ok(()),
        OwnerLeaseModel::FiniteAuthoritativeTtl
            if OWNER_LEASE_EXPIRY_ADMISSION_QUALIFIED =>
        {
            Ok(())
        }
        OwnerLeaseModel::FiniteAuthoritativeTtl => Err(ServerError::InvalidBootstrap(format!(
            "{OWNER_LEASE_EXPIRY_ADMISSION_NOT_QUALIFIED}: finite owner leases require authoritative monotonic validity, bounded irreversible cut points, and independent per-owner supervision"
        ))),
    }
}

fn lifecycle_transition(
    open_intent: OpenIntent,
    admission: &OwnerAdmission,
) -> Result<LifecycleTransition, ServerError> {
    match (open_intent, admission) {
        (
            OpenIntent::CreateFresh,
            OwnerAdmission::Acquire {
                expected_previous_epoch: None,
                ..
            },
        ) => Ok(LifecycleTransition::FreshCreate),
        (OpenIntent::ReopenExisting, OwnerAdmission::Resume { .. }) => {
            Ok(LifecycleTransition::ExactResume)
        }
        (
            OpenIntent::ReopenExisting,
            OwnerAdmission::Acquire {
                expected_previous_epoch: Some(_),
                ..
            },
        ) => Ok(LifecycleTransition::SuccessorReopen),
        (
            OpenIntent::ReconcilePreparedCreate,
            OwnerAdmission::Acquire {
                expected_previous_epoch: None,
                ..
            },
        ) => Ok(LifecycleTransition::PreparedFirstCreate),
        (
            OpenIntent::ReconcilePreparedCreate,
            OwnerAdmission::Acquire {
                expected_previous_epoch: Some(_),
                ..
            },
        ) => Ok(LifecycleTransition::PreparedSuccessorCreate),
        (
            OpenIntent::ReconcilePreparedCreate,
            OwnerAdmission::ResumePreparedOrAcquireSuccessor { .. },
        ) => Ok(LifecycleTransition::PreparedResumeOrSuccessor),
        _ => Err(ServerError::InvalidBootstrap(
            "metadata open intent and owner admission do not form a valid lifecycle transition"
                .to_owned(),
        )),
    }
}

pub fn validate_owner_admission_transition_v1(
    transition: LifecycleTransition,
) -> Result<(), AdmissionCode> {
    match transition {
        LifecycleTransition::FreshCreate
        | LifecycleTransition::SuccessorReopen
        | LifecycleTransition::PreparedFirstCreate
        | LifecycleTransition::PreparedSuccessorCreate => {
            Err(AdmissionCode::PlannedOwnerAdmissionNotQualifiedV1)
        }
        LifecycleTransition::ExactResume => Err(AdmissionCode::ExactResumeNotQualifiedV1),
        LifecycleTransition::PreparedResumeOrSuccessor => {
            Err(AdmissionCode::PreparedResumeOrSuccessorNotQualifiedV1)
        }
    }
}

fn reject_unqualified_owner_admission_transition(
    transition: LifecycleTransition,
) -> Result<(), ServerError> {
    validate_owner_admission_transition_v1(transition)
        .map_err(|code| ServerError::InvalidBootstrap(code.to_string()))
}

fn validate_migration_admission(authority: &MetadataAuthorityRecord) -> Result<(), ServerError> {
    let Some(migration) = authority.migration.as_ref() else {
        return Ok(());
    };
    Err(ServerError::InvalidBootstrap(format!(
        "metadata authority migration is {:?}; copy, catch-up, cutover coordination, and aborted-state reconciliation are not qualified, so owner Serving is refused",
        migration.phase
    )))
}

fn validate_requested_lease_authority(
    admission: &OwnerAdmission,
    authority: &MetadataAuthorityRecord,
) -> Result<(), ServerError> {
    let lease = match admission {
        OwnerAdmission::Resume { lease }
        | OwnerAdmission::ResumePreparedOrAcquireSuccessor { lease, .. } => lease,
        OwnerAdmission::Acquire { .. } => return Ok(()),
    };
    if lease.authority != authority.fence() {
        return Err(ServerError::InvalidBootstrap(
            "resumed lease carries a stale metadata authority fence".to_owned(),
        ));
    }
    Ok(())
}

fn reject_unrecovered_shared_frontier(
    control: &dyn ControlStore,
    placement: &RootPlacement,
    requested: &RecoveryPublication,
) -> Result<(), ServerError> {
    let shard = control
        .get_logical_shard(&placement.logical_shard_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("logical shard does not exist".to_owned()))?;
    let persisted_nonempty =
        shard.durable_lsn != 0 || shard.checkpoint.is_some() || shard.log.is_some();
    let requested_nonempty =
        requested.durable_lsn != 0 || requested.checkpoint.is_some() || requested.log.is_some();
    if persisted_nonempty || requested_nonempty {
        return Err(ServerError::InvalidBootstrap(format!(
            "logical shard {:?} has an unverified shared recovery frontier (persisted LSN {}, requested LSN {}); checkpoint/log install, replay, and fsck are not implemented, so Serving is refused",
            placement.logical_shard_id, shard.durable_lsn, requested.durable_lsn
        )));
    }
    Ok(())
}

fn validate_serving_placement(placement: &RootPlacement) -> Result<(), ServerError> {
    if placement.lifecycle != RootPlacementLifecycle::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "root placement is {:?}, expected Active",
            placement.lifecycle
        )));
    }
    if placement.layout_profile != RootLayoutProfile::SingleShardRoot {
        return Err(ServerError::InvalidBootstrap(format!(
            "root layout {:?} is not qualified by this runtime",
            placement.layout_profile
        )));
    }
    let supported_generation =
        RootLayoutGeneration::new(1).expect("supported root layout generation is non-zero");
    if placement.layout_generation != supported_generation {
        return Err(ServerError::InvalidBootstrap(format!(
            "root layout generation {} is not qualified by this runtime",
            placement.layout_generation
        )));
    }
    if placement.partition_id != RootPartitionId::SINGLE_SHARD {
        return Err(ServerError::InvalidBootstrap(
            "SingleShardRoot must use the reserved SINGLE_SHARD partition id".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapLeaseProvenance {
    /// This bootstrap advanced the durable owner epoch and must release the
    /// exact lease on every later failure. The skipped epoch remains durable.
    AcquiredByBootstrap,
    /// The caller supplied an existing exact owner session. Bootstrap renews
    /// but never releases that caller-owned session when local opening fails.
    ResumedExistingSession,
}

fn admit_owner(
    control: &dyn ControlStore,
    serving_admission: &OwnerServingAdmission,
    admission: &OwnerAdmission,
) -> Result<(LogicalShardLease, BootstrapLeaseProvenance), ServerError> {
    match admission {
        OwnerAdmission::Acquire {
            owner,
            owner_incarnation_id,
            endpoint,
            expected_previous_epoch,
        } => {
            let lease = match expected_previous_epoch {
                None => control.acquire_owner(
                    serving_admission,
                    owner.clone(),
                    *owner_incarnation_id,
                    endpoint.clone(),
                )?,
                Some(expected) => control.acquire_successor(
                    serving_admission,
                    *expected,
                    owner.clone(),
                    *owner_incarnation_id,
                    endpoint.clone(),
                )?,
            };
            Ok((lease, BootstrapLeaseProvenance::AcquiredByBootstrap))
        }
        OwnerAdmission::Resume { .. } => Err(ServerError::InvalidBootstrap(
            AdmissionCode::ExactResumeNotQualifiedV1.to_string(),
        )),
        OwnerAdmission::ResumePreparedOrAcquireSuccessor { .. } => {
            Err(ServerError::InvalidBootstrap(
                AdmissionCode::PreparedResumeOrSuccessorNotQualifiedV1.to_string(),
            ))
        }
    }
}

fn validate_runtime(runtime: &ResolvedRuntime) -> Result<(), ServerError> {
    match inspect_runtime(runtime) {
        Ok(()) => Ok(()),
        Err(error) => {
            runtime.poison_lifecycle();
            Err(error)
        }
    }
}

fn inspect_runtime(runtime: &ResolvedRuntime) -> Result<(), ServerError> {
    runtime
        .validate_provider_binding()
        .map_err(|error| ServerError::InvalidBootstrap(error.to_string()))?;
    runtime
        .validate_lifecycle()
        .map_err(lifecycle_validation_error)?;
    runtime
        .validate_provider_binding()
        .map_err(|error| ServerError::InvalidBootstrap(error.to_string()))
}

fn inspect_runtime_binding(
    runtime: &ResolvedRuntime,
    store: &meta::AgentMetadataStore,
) -> Result<(), ServerError> {
    inspect_runtime(runtime)?;
    store
        .validate_provider_runtime()
        .map_err(ServerError::Metadata)?;
    inspect_runtime(runtime)
}

fn lifecycle_validation_error(error: RuntimeLifecycleValidationError) -> ServerError {
    ServerError::InvalidBootstrap(error.to_string())
}

fn root_route(placement: &RootPlacement, lease: &LogicalShardLease) -> RootRoute {
    RootRoute {
        root_id: RootIdentity::from(placement.root_id),
        logical_shard_id: LogicalShardIdentity::from(placement.logical_shard_id),
        placement_generation: placement.placement_generation.get(),
        owner_epoch: lease.owner_epoch.get(),
    }
}

fn activate_root_fence(
    store: &meta::AgentMetadataStore,
    placement: &RootPlacement,
    lease: &LogicalShardLease,
    install_request_id: RequestId,
    activate_request_id: RequestId,
    allow_skipped_owner_epochs: bool,
) -> Result<(), ServerError> {
    let current_owner = store.current_owner_epoch()?;
    if current_owner != Some(lease.owner_epoch) {
        let expected_previous = if allow_skipped_owner_epochs {
            if current_owner.is_some_and(|current| current.get() >= lease.owner_epoch.get()) {
                return Err(ServerError::InvalidBootstrap(format!(
                    "metadata owner epoch {} is not below explicit successor lease epoch {}",
                    display_epoch(current_owner),
                    lease.owner_epoch
                )));
            }
            current_owner
        } else {
            predecessor(lease.owner_epoch)
        };
        if current_owner != expected_previous {
            return Err(ServerError::InvalidBootstrap(format!(
                "metadata owner epoch is {}, lease requires predecessor {} or exact epoch {}",
                display_epoch(current_owner),
                display_epoch(expected_previous),
                lease.owner_epoch
            )));
        }
        store.advance_owner_epoch(expected_previous, lease.owner_epoch)?;
    }

    match store.root_fence(placement.root_id)? {
        None => execute_fence_command(
            store,
            placement,
            lease,
            install_request_id,
            meta::RootFenceAction::Install {
                layout_profile: placement.layout_profile,
                layout_generation: placement.layout_generation,
                partition_id: placement.partition_id,
            },
            b"nokv.root-fence.install.v1".to_vec(),
        )?,
        Some(fence) => validate_existing_fence(placement, fence)?,
    }

    let fence = store.root_fence(placement.root_id)?.ok_or_else(|| {
        ServerError::InvalidBootstrap("root fence install produced no durable fence".to_owned())
    })?;
    validate_existing_fence(placement, fence)?;
    if fence.activation_state == RootActivationState::Installing {
        execute_fence_command(
            store,
            placement,
            lease,
            activate_request_id,
            meta::RootFenceAction::Transition {
                expected: RootActivationState::Installing,
                next: RootActivationState::Active,
            },
            b"nokv.root-fence.activate.v1".to_vec(),
        )?;
    }

    let active = store
        .root_fence(placement.root_id)?
        .ok_or_else(|| ServerError::InvalidBootstrap("active root fence disappeared".to_owned()))?;
    validate_existing_fence(placement, active)?;
    if active.activation_state != RootActivationState::Active {
        return Err(ServerError::InvalidBootstrap(format!(
            "root fence is {:?}, expected Active",
            active.activation_state
        )));
    }
    Ok(())
}

fn validate_existing_fence(
    placement: &RootPlacement,
    fence: meta::RootFence,
) -> Result<(), ServerError> {
    if fence.logical_shard_id != placement.logical_shard_id
        || fence.placement_generation != placement.placement_generation
        || fence.layout_profile != placement.layout_profile
        || fence.layout_generation != placement.layout_generation
        || fence.partition_id != placement.partition_id
    {
        return Err(ServerError::InvalidBootstrap(
            "root fence placement does not match the control-plane placement".to_owned(),
        ));
    }
    if matches!(
        fence.activation_state,
        RootActivationState::Draining | RootActivationState::Fenced
    ) {
        return Err(ServerError::InvalidBootstrap(format!(
            "root fence is {:?}",
            fence.activation_state
        )));
    }
    Ok(())
}

fn execute_fence_command(
    store: &meta::AgentMetadataStore,
    placement: &RootPlacement,
    lease: &LogicalShardLease,
    request_id: RequestId,
    action: meta::RootFenceAction,
    deterministic_result: Vec<u8>,
) -> Result<(), ServerError> {
    let command = meta::MetadataCommand {
        schema_id: meta::SCHEMA_ID.to_owned(),
        root_id: placement.root_id,
        logical_shard_id: placement.logical_shard_id,
        placement_generation: placement.placement_generation,
        owner_epoch: lease.owner_epoch,
        request_id,
        command_digest: CommandDigest::from_bytes([0; SHA256_BYTES]),
        read_version: store.current_read_version()?,
        root_fence_action: action,
        predicates: Vec::new(),
        mutations: Vec::new(),
        history_projection: Vec::new(),
        event_projection: Vec::new(),
        deterministic_result,
    }
    .seal();
    store.execute(&command)?;
    Ok(())
}

fn predecessor(epoch: OwnerEpoch) -> Option<OwnerEpoch> {
    let previous = epoch.get() - 1;
    (previous != 0).then(|| OwnerEpoch::new(previous).expect("nonzero predecessor is valid"))
}

fn display_epoch(epoch: Option<OwnerEpoch>) -> String {
    epoch.map_or_else(|| "epoch-zero".to_owned(), |epoch| epoch.get().to_string())
}

fn persist_release_receipt(
    runtime: &ResolvedRuntime,
    lease: &LogicalShardLease,
) -> Result<(), ServerError> {
    runtime
        .persist_owner_releasing(lease)
        .map_err(ServerError::OwnerReleaseReceipt)
}

#[allow(
    clippy::too_many_arguments,
    reason = "rollback keeps every exact ownership capability explicit at the failure site"
)]
fn rollback_bootstrap(
    primary: ServerError,
    control: &Arc<dyn ControlStore>,
    registry: &Arc<RootOwnerRegistry>,
    runtime: &ResolvedRuntime,
    bootstrap_reservation: ShardBootstrapReservation,
    lease: &LogicalShardLease,
    pending_owner: Option<OwnerCandidateToken>,
    lease_provenance: BootstrapLeaseProvenance,
) -> ServerError {
    let primary_message = primary.to_string();
    let mut rollback = Vec::new();

    if lease_provenance == BootstrapLeaseProvenance::ResumedExistingSession {
        if let Some(pending_owner) = pending_owner {
            if let Err(error) = registry.remove_pending(&pending_owner) {
                rollback.push(format!("remove pending registry candidate: {error}"));
            }
        }
        return if rollback.is_empty() {
            primary
        } else {
            ServerError::BootstrapRollback {
                primary: primary_message,
                rollback: rollback.join("; "),
            }
        };
    }

    let guard = match pending_owner {
        Some(candidate) => PendingOwnerReleaseGuard::Candidate {
            registry: Arc::clone(registry),
            candidate,
        },
        None => {
            if let Ok(false) = bootstrap_reservation.is_current() {
                return ServerError::BootstrapRollback {
                    primary: primary_message,
                    rollback: "owner release reservation is no longer current".to_owned(),
                };
            }
            PendingOwnerReleaseGuard::Reservation(bootstrap_reservation)
        }
    };
    let pending = PendingOwnerRelease {
        control: Arc::clone(control),
        runtime: runtime.clone(),
        lease: lease.clone(),
        guard: std::sync::Mutex::new(guard),
    };
    match pending.attempt() {
        Ok(PendingOwnerReleaseAttempt::Terminal(_)) => {
            if rollback.is_empty() {
                primary
            } else {
                ServerError::BootstrapRollback {
                    primary: primary_message,
                    rollback: rollback.join("; "),
                }
            }
        }
        Ok(PendingOwnerReleaseAttempt::Unknown) => ServerError::BootstrapOwnerReleasePending {
            primary: primary_message,
            rollback: (!rollback.is_empty()).then(|| rollback.join("; ")),
            pending: Box::new(pending),
        },
        Ok(PendingOwnerReleaseAttempt::Control(error)) => {
            ServerError::BootstrapOwnerReleaseRetryable {
                primary: primary_message,
                rollback: (!rollback.is_empty()).then(|| rollback.join("; ")),
                pending: Box::new(pending),
                error: Box::new(error),
            }
        }
        Err(ServerError::OwnerReleaseReceipt(error)) => {
            ServerError::BootstrapOwnerReleaseReceiptRejected {
                primary: primary_message,
                rollback: (!rollback.is_empty()).then(|| rollback.join("; ")),
                pending: Box::new(pending),
                error,
            }
        }
        Err(error) => ServerError::BootstrapRollback {
            primary: primary_message,
            rollback: format!("reconcile exact owner release: {error}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::Duration;

    use nokv_control::{
        ConsistencyDomainId, InMemoryControlStore, LogRef, LogSegmentRef, LogicalShardState,
        MetadataAuthorityId, MetadataAuthorityRevision, MetadataMigration, MetadataMigrationPhase,
        MetadataProviderProfileId, OperationId, PlacementGeneration, RootPlacement,
    };
    use nokv_types::LogicalShardId;
    use tempfile::TempDir;

    use super::*;
    use crate::{ServerOptions, WorkspaceServer};

    fn characterize_bootstrap_after_admission(
        control: Arc<dyn ControlStore>,
        registry: Arc<RootOwnerRegistry>,
        request: RootOwnerBootstrapRequest,
    ) -> Result<BootstrappedRootOwner, ServerError> {
        let transition = lifecycle_transition(request.open_intent, &request.admission)?;
        bootstrap_root_owner_after_admission(control, registry, request, transition)
    }

    const TEST_TRANSITIONS: [LifecycleTransition; 6] = [
        LifecycleTransition::FreshCreate,
        LifecycleTransition::ExactResume,
        LifecycleTransition::SuccessorReopen,
        LifecycleTransition::PreparedFirstCreate,
        LifecycleTransition::PreparedSuccessorCreate,
        LifecycleTransition::PreparedResumeOrSuccessor,
    ];

    #[derive(Default)]
    struct TestRuntimeServices {
        reject_provider: AtomicBool,
        reject_commit_receipt_resolution: AtomicBool,
        reject_release_receipt: AtomicBool,
        poisoned: AtomicBool,
        commit_receipt: Arc<crate::runtime_registry::RecordingCommitReceiptStoreV1>,
        create_delegate_calls: AtomicUsize,
        reopen_delegate_calls: AtomicUsize,
        recovery_reopen_delegate_calls: AtomicUsize,
        release_receipt_calls: AtomicUsize,
    }

    struct AlwaysValidCandidateRuntime {
        poison_calls: Arc<AtomicUsize>,
    }

    impl OwnerCandidateRuntimeValidator for AlwaysValidCandidateRuntime {
        fn validate(&self) -> bool {
            true
        }

        fn poison(&self) {
            self.poison_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl nokv_meta::built_in_holt::HoltRuntimeGuard for TestRuntimeServices {
        fn bind_store(
            &self,
            _identity: &nokv_meta::built_in_holt::HoltStoreObjectIdentity,
        ) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            Ok(())
        }

        fn validate_runtime(&self) -> Result<(), nokv_meta::built_in_holt::HoltRuntimeGuardError> {
            if self.reject_provider.load(Ordering::Acquire) {
                Err(nokv_meta::built_in_holt::HoltRuntimeGuardError::Rejected)
            } else {
                Ok(())
            }
        }

        fn poison(&self) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    impl meta::MetadataCommitReceiptStoreV1 for TestRuntimeServices {
        fn commit_receipt_qualification_v1(&self) -> meta::MetadataCommitReceiptQualificationV1 {
            self.commit_receipt.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.commit_receipt.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: meta::MetadataStoreIdentity,
        ) -> Result<meta::MetadataCommitReceiptStateV1, meta::MetadataCommitReceiptErrorV1>
        {
            self.commit_receipt.load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: meta::MetadataCommitReceiptPersistCommandV1,
        ) -> meta::MetadataCommitReceiptPersistOutcomeV1 {
            self.commit_receipt.persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: meta::MetadataCommitReceiptResolveCommandV1,
        ) -> meta::MetadataCommitReceiptResolveOutcomeV1 {
            self.commit_receipt.reject_resolve(
                self.reject_commit_receipt_resolution
                    .load(Ordering::Acquire),
            );
            self.commit_receipt.resolve_pending_commit_v1(command)
        }

        fn poison_commit_receipt_v1(
            &self,
            command: meta::MetadataCommitReceiptPoisonCommandV1,
        ) -> meta::MetadataCommitReceiptPoisonOutcomeV1 {
            self.commit_receipt.poison_commit_receipt_v1(command)
        }
    }

    impl crate::RuntimeLifecycleValidator for TestRuntimeServices {
        fn validate(&self) -> Result<(), crate::RuntimeLifecycleValidationError> {
            Ok(())
        }

        fn poison(&self) {
            self.poisoned.store(true, Ordering::Release);
        }
    }

    impl crate::OwnerReleaseReceipt for TestRuntimeServices {
        type Binding = ();

        fn owner_release_binding(&self) -> Result<Self::Binding, crate::OwnerReleaseReceiptError> {
            Ok(())
        }

        fn preflight_owner_release_at_binding(
            &self,
            _expected: &Self::Binding,
        ) -> Result<(), crate::OwnerReleaseReceiptError> {
            if self.reject_release_receipt.load(Ordering::Acquire) {
                Err(crate::OwnerReleaseReceiptError::PersistenceRejectedV1)
            } else {
                Ok(())
            }
        }

        fn persist_owner_releasing_at_binding(
            &self,
            _expected: &Self::Binding,
            _lease: &LogicalShardLease,
        ) -> Result<(), crate::OwnerReleaseReceiptError> {
            self.release_receipt_calls.fetch_add(1, Ordering::SeqCst);
            if self.reject_release_receipt.load(Ordering::Acquire) {
                Err(crate::OwnerReleaseReceiptError::PersistenceRejectedV1)
            } else {
                Ok(())
            }
        }
    }

    #[derive(Clone, PartialEq, Eq)]
    struct TestRuntimeInstallationIdentity {
        canonical_locator: PathBuf,
        services_address: usize,
    }

    struct TestExternalOwnerRuntimeBundle {
        provider: Arc<dyn meta::MetadataCommitRecoveryFenceFactoryV1>,
        services: Arc<TestRuntimeServices>,
        installation_identity: TestRuntimeInstallationIdentity,
    }

    impl TestExternalOwnerRuntimeBundle {
        fn new(path: PathBuf, services: Arc<TestRuntimeServices>) -> Self {
            let name = path.file_name().unwrap();
            let parent = path.parent().unwrap();
            let canonical_locator = std::fs::canonicalize(parent).unwrap().join(name);
            let guard: Arc<dyn nokv_meta::built_in_holt::HoltRuntimeGuard> = services.clone();
            let provider =
                nokv_meta::built_in_holt::file_provider_factory_v1(&canonical_locator, guard);
            let installation_identity = TestRuntimeInstallationIdentity {
                canonical_locator,
                services_address: Arc::as_ptr(&services) as *const () as usize,
            };
            Self {
                provider,
                services,
                installation_identity,
            }
        }
    }

    impl nokv_meta::provider::v1::MetadataProviderFactoryV1 for TestExternalOwnerRuntimeBundle {
        fn contract_offer(
            &self,
            schema: &nokv_meta::provider::v1::ProviderSchemaV1,
        ) -> Result<
            nokv_meta::provider::v1::ProviderContractOfferV1,
            nokv_meta::provider::v1::ProviderError,
        > {
            self.provider.contract_offer(schema)
        }

        fn create(
            &self,
            request: &nokv_meta::provider::v1::ProviderCreateRequestV1,
        ) -> Result<
            Arc<dyn nokv_meta::provider::v1::MetadataProvider>,
            nokv_meta::provider::v1::ProviderError,
        > {
            self.provider.create(request)
        }

        fn reopen(
            &self,
            request: &nokv_meta::provider::v1::ProviderReopenRequestV1,
        ) -> Result<
            Arc<dyn nokv_meta::provider::v1::MetadataProvider>,
            nokv_meta::provider::v1::ProviderError,
        > {
            self.provider.reopen(request)
        }
    }

    impl meta::MetadataCommitRecoveryFenceFactoryV1 for TestExternalOwnerRuntimeBundle {
        fn old_dispatch_exclusion_installation_v1(
            &self,
        ) -> meta::MetadataOldDispatchExclusionInstallationV1 {
            self.provider.old_dispatch_exclusion_installation_v1()
        }

        fn reopen_pending_with_old_dispatch_excluded_v1(
            &self,
            command: meta::MetadataPendingRecoveryOpenCommandV1,
        ) -> meta::MetadataPendingRecoveryOpenOutcomeV1 {
            self.provider
                .reopen_pending_with_old_dispatch_excluded_v1(command)
        }
    }

    impl crate::RuntimeProviderFactory for TestExternalOwnerRuntimeBundle {
        type InstallationIdentity = TestRuntimeInstallationIdentity;

        fn binding_snapshot(
            &self,
            schema: &nokv_meta::provider::v1::ProviderSchemaV1,
        ) -> Result<
            crate::RuntimeProviderBinding<Self::InstallationIdentity>,
            nokv_meta::provider::v1::ProviderError,
        > {
            Ok(
                crate::RuntimeProviderBinding::with_recovery_fence_installation(
                    self.provider.contract_offer(schema)?,
                    self.installation_identity.clone(),
                    self.provider.old_dispatch_exclusion_installation_v1(),
                ),
            )
        }

        fn create_at_binding(
            &self,
            expected_binding: &crate::RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &nokv_meta::provider::v1::ProviderCreateRequestV1,
        ) -> Result<
            Arc<dyn nokv_meta::provider::v1::MetadataProvider>,
            nokv_meta::provider::v1::ProviderError,
        > {
            if &self.binding_snapshot(request.schema())? != expected_binding {
                return Err(nokv_meta::provider::v1::ProviderError::authority_mismatch(
                    nokv_meta::provider::v1::ProviderOperationV1::Create,
                ));
            }
            self.services
                .create_delegate_calls
                .fetch_add(1, Ordering::SeqCst);
            self.provider.create(request)
        }

        fn reopen_at_binding(
            &self,
            expected_binding: &crate::RuntimeProviderBinding<Self::InstallationIdentity>,
            request: &nokv_meta::provider::v1::ProviderReopenRequestV1,
        ) -> Result<
            Arc<dyn nokv_meta::provider::v1::MetadataProvider>,
            nokv_meta::provider::v1::ProviderError,
        > {
            if &self.binding_snapshot(request.schema())? != expected_binding {
                return Err(nokv_meta::provider::v1::ProviderError::authority_mismatch(
                    nokv_meta::provider::v1::ProviderOperationV1::Reopen,
                ));
            }
            self.services
                .reopen_delegate_calls
                .fetch_add(1, Ordering::SeqCst);
            self.provider.reopen(request)
        }

        fn reopen_pending_with_old_dispatch_excluded_at_binding_v1(
            &self,
            expected_binding: &crate::RuntimeProviderBinding<Self::InstallationIdentity>,
            command: meta::MetadataPendingRecoveryOpenCommandV1,
        ) -> meta::MetadataPendingRecoveryOpenOutcomeV1 {
            if !self
                .binding_snapshot(command.schema())
                .is_ok_and(|current| &current == expected_binding)
                || command.expected_installation() != expected_binding.recovery_fence_installation()
            {
                return command.reject_before_execution(
                    meta::MetadataPendingRecoveryOpenNotDispatchedV1::InvalidBinding,
                );
            }
            self.services
                .recovery_reopen_delegate_calls
                .fetch_add(1, Ordering::SeqCst);
            self.provider
                .reopen_pending_with_old_dispatch_excluded_v1(command)
        }
    }

    impl meta::MetadataCommitReceiptStoreV1 for TestExternalOwnerRuntimeBundle {
        fn commit_receipt_qualification_v1(&self) -> meta::MetadataCommitReceiptQualificationV1 {
            self.services.commit_receipt_qualification_v1()
        }

        fn frozen_runtime_bundle_digest_v1(&self) -> [u8; SHA256_BYTES] {
            self.services.frozen_runtime_bundle_digest_v1()
        }

        fn load_commit_receipt_v1(
            &self,
            store_identity: meta::MetadataStoreIdentity,
        ) -> Result<meta::MetadataCommitReceiptStateV1, meta::MetadataCommitReceiptErrorV1>
        {
            self.services.load_commit_receipt_v1(store_identity)
        }

        fn persist_pending_commit_v1(
            &self,
            command: meta::MetadataCommitReceiptPersistCommandV1,
        ) -> meta::MetadataCommitReceiptPersistOutcomeV1 {
            self.services.persist_pending_commit_v1(command)
        }

        fn resolve_pending_commit_v1(
            &self,
            command: meta::MetadataCommitReceiptResolveCommandV1,
        ) -> meta::MetadataCommitReceiptResolveOutcomeV1 {
            self.services.resolve_pending_commit_v1(command)
        }

        fn poison_commit_receipt_v1(
            &self,
            command: meta::MetadataCommitReceiptPoisonCommandV1,
        ) -> meta::MetadataCommitReceiptPoisonOutcomeV1 {
            self.services.poison_commit_receipt_v1(command)
        }
    }

    impl crate::RuntimeLifecycleValidator for TestExternalOwnerRuntimeBundle {
        fn validate(&self) -> Result<(), crate::RuntimeLifecycleValidationError> {
            crate::RuntimeLifecycleValidator::validate(self.services.as_ref())
        }

        fn poison(&self) {
            crate::RuntimeLifecycleValidator::poison(self.services.as_ref());
        }
    }

    impl crate::OwnerReleaseReceipt for TestExternalOwnerRuntimeBundle {
        type Binding = <TestRuntimeServices as crate::OwnerReleaseReceipt>::Binding;

        fn owner_release_binding(&self) -> Result<Self::Binding, crate::OwnerReleaseReceiptError> {
            crate::OwnerReleaseReceipt::owner_release_binding(self.services.as_ref())
        }

        fn preflight_owner_release_at_binding(
            &self,
            expected: &Self::Binding,
        ) -> Result<(), crate::OwnerReleaseReceiptError> {
            crate::OwnerReleaseReceipt::preflight_owner_release_at_binding(
                self.services.as_ref(),
                expected,
            )
        }

        fn persist_owner_releasing_at_binding(
            &self,
            expected: &Self::Binding,
            lease: &LogicalShardLease,
        ) -> Result<(), crate::OwnerReleaseReceiptError> {
            crate::OwnerReleaseReceipt::persist_owner_releasing_at_binding(
                self.services.as_ref(),
                expected,
                lease,
            )
        }
    }

    fn test_descriptor() -> RuntimeDescriptor {
        test_descriptor_with("external-test-runtime-v1", 0x5a)
    }

    fn test_descriptor_with(profile: &str, fingerprint: u8) -> RuntimeDescriptor {
        let provider = nokv_meta::built_in_holt::memory_provider_factory_v1();
        let schema = meta::canonical_provider_schema_v1();
        RuntimeDescriptor::new(
            MetadataProviderProfileId::new(profile).unwrap(),
            [fingerprint; SHA256_BYTES],
            provider.contract_offer(&schema).unwrap(),
            crate::LifecycleCapabilities::new(
                crate::OwnerReceiptMode::ExternalOwnerJournal,
                &TEST_TRANSITIONS,
            ),
            crate::RuntimeConsistencyDomain::ShardLocal,
        )
        .unwrap()
    }

    fn file_runtime(path: PathBuf) -> ResolvedRuntime {
        file_runtime_with_descriptor(path, test_descriptor())
    }

    fn file_runtime_with_descriptor(
        path: PathBuf,
        descriptor: RuntimeDescriptor,
    ) -> ResolvedRuntime {
        let services = Arc::new(TestRuntimeServices::default());
        file_runtime_with_services(path, descriptor, services)
    }

    fn file_runtime_with_services(
        path: PathBuf,
        descriptor: RuntimeDescriptor,
        services: Arc<TestRuntimeServices>,
    ) -> ResolvedRuntime {
        let bundle = TestExternalOwnerRuntimeBundle::new(path, services);
        ResolvedRuntime::external_owner_journal(descriptor, bundle).unwrap()
    }

    struct TestRegistryRuntimeFactory {
        descriptor: RuntimeDescriptor,
        resolved: ResolvedRuntime,
    }

    impl crate::RuntimeFactory for TestRegistryRuntimeFactory {
        fn descriptor(&self) -> RuntimeDescriptor {
            self.descriptor.clone()
        }

        fn resolve(&self) -> Result<ResolvedRuntime, crate::RuntimeFactoryError> {
            Ok(self.resolved.clone())
        }
    }

    fn root() -> RootId {
        RootId::from_bytes([1; nokv_types::FIXED_ID_BYTES])
    }

    fn second_root() -> RootId {
        RootId::from_bytes([3; nokv_types::FIXED_ID_BYTES])
    }

    fn shard() -> nokv_types::LogicalShardId {
        nokv_types::LogicalShardId::from_bytes([2; nokv_types::FIXED_ID_BYTES])
    }

    fn request_id(fill: u8) -> RequestId {
        RequestId::from_bytes([fill; nokv_types::FIXED_ID_BYTES])
    }

    fn incarnation(fill: u8) -> OwnerIncarnationId {
        OwnerIncarnationId::from_bytes([fill; nokv_types::FIXED_ID_BYTES])
    }

    fn holt_runtime() -> RuntimeDescriptor {
        test_descriptor()
    }

    fn empty_recovery() -> RecoveryPublication {
        RecoveryPublication {
            checkpoint: None,
            log: None,
            durable_lsn: 0,
        }
    }

    fn log_recovery(lsn: u64) -> RecoveryPublication {
        let digest = format!("state-{lsn}");
        RecoveryPublication {
            checkpoint: None,
            log: Some(LogRef {
                segments: vec![LogSegmentRef {
                    segment_key: format!("logs/{lsn}-{lsn}"),
                    first_lsn: lsn,
                    last_lsn: lsn,
                    digest: digest.clone(),
                }],
                durable_lsn: lsn,
                digest,
            }),
            durable_lsn: lsn,
        }
    }

    fn active_control() -> Arc<InMemoryControlStore> {
        active_control_with_descriptor(&holt_runtime())
    }

    fn active_control_with_descriptor(descriptor: &RuntimeDescriptor) -> Arc<InMemoryControlStore> {
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        control
            .create_metadata_authority(descriptor.initial_authority(shard()))
            .unwrap();
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id: root(),
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                logical_shard_id: shard(),
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        control
            .compare_and_set_root_placement(
                &provisioning,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..provisioning
                },
            )
            .unwrap();
        control
    }

    fn serving_admission(control: &InMemoryControlStore) -> OwnerServingAdmission {
        let placement = control.get_root_placement(&root()).unwrap().unwrap();
        let authority = control.get_metadata_authority(&shard()).unwrap().unwrap();
        OwnerServingAdmission::stable(placement, authority).unwrap()
    }

    fn migration(
        authority: &MetadataAuthorityRecord,
        phase: MetadataMigrationPhase,
    ) -> MetadataMigration {
        let mut target = authority.active.clone();
        target.authority_id = MetadataAuthorityId::from_bytes([0x91; 16]);
        target.provider_profile_id = MetadataProviderProfileId::new("migration-target").unwrap();
        target.profile_fingerprint = [0x92; SHA256_BYTES];
        target.consistency_domain_id = ConsistencyDomainId::from_bytes([0x93; 16]);
        MetadataMigration {
            migration_id: OperationId::from_bytes([0x94; 16]),
            source: authority.active.clone(),
            target,
            phase,
            source_frontier: None,
            target_frontier: None,
            cutover_frontier: None,
            source_quiesce_receipt: None,
            target_activation_token: None,
        }
    }

    #[test]
    fn holt_local_profile_derivation_has_a_frozen_canonical_golden() {
        let runtime = crate::holt_runtime_descriptor().unwrap();
        let authority = runtime.initial_authority(shard());
        assert_eq!(
            authority.active.provider_profile_id.as_str(),
            "holt-local-v1"
        );
        assert_eq!(
            authority.active.profile_fingerprint,
            [
                0xeb, 0x56, 0xd2, 0x2c, 0x7e, 0xa7, 0x93, 0x17, 0x91, 0xe6, 0x7b, 0x6d, 0xf3, 0x60,
                0xae, 0x57, 0xe8, 0x16, 0x37, 0xe3, 0xa8, 0x37, 0x67, 0x87, 0x70, 0xf5, 0xe8, 0x8a,
                0x4b, 0xf5, 0x6d, 0xb2,
            ]
        );
        assert_eq!(
            authority.active.authority_id.as_bytes(),
            &[
                0xfc, 0xb0, 0x16, 0x1f, 0x11, 0x42, 0x7e, 0x92, 0x58, 0x19, 0x0f, 0x59, 0x72, 0x0f,
                0x56, 0x52,
            ]
        );
        assert_eq!(
            authority.active.consistency_domain_id.as_bytes(),
            &[
                0xcf, 0x97, 0xe7, 0x9c, 0x29, 0xb2, 0x4c, 0xa2, 0xf2, 0x7f, 0xbf, 0xb2, 0x61, 0xec,
                0xb0, 0xff,
            ]
        );
        assert_eq!(
            authority.active.contract_digest,
            meta::workspace_metadata_contract_digest()
        );
        assert_ne!(
            authority.active.authority_id,
            runtime
                .initial_authority(nokv_types::LogicalShardId::from_bytes(
                    [3; nokv_types::FIXED_ID_BYTES]
                ))
                .active
                .authority_id
        );
    }

    fn acquire_request(path: PathBuf) -> RootOwnerBootstrapRequest {
        RootOwnerBootstrapRequest {
            root_id: root(),
            runtime: file_runtime(path),
            open_intent: OpenIntent::CreateFresh,
            admission: OwnerAdmission::Acquire {
                owner: NodeId::new("node-a").unwrap(),
                owner_incarnation_id: incarnation(1),
                endpoint: "127.0.0.1:9010".to_owned(),
                expected_previous_epoch: None,
            },
            install_request_id: request_id(3),
            activate_request_id: request_id(4),
            recovery: empty_recovery(),
        }
    }

    #[test]
    fn selector_and_open_mismatches_fail_before_owner_or_store_side_effects() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());

        let mut wrong_selector = acquire_request(temporary.path().join("selector"));
        wrong_selector.runtime = file_runtime_with_descriptor(
            temporary.path().join("selector"),
            test_descriptor_with("other-test-runtime-v1", 0x7c),
        );
        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            wrong_selector,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("selects profile"));

        let mut wrong_open = acquire_request(temporary.path().join("open"));
        wrong_open.open_intent = OpenIntent::ReopenExisting;
        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            wrong_open,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("lifecycle transition"));

        let shard_record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(shard_record.owner.is_none());
        assert_eq!(shard_record.lease_id, 0);
        assert!(!temporary.path().join("selector").exists());
        assert!(!temporary.path().join("open").exists());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn every_migration_phase_is_unconditionally_rejected() {
        for phase in [
            MetadataMigrationPhase::Preparing,
            MetadataMigrationPhase::Copying,
            MetadataMigrationPhase::CatchingUp,
            MetadataMigrationPhase::Quiescing,
            MetadataMigrationPhase::ReadyToCutover,
            MetadataMigrationPhase::CutoverComplete,
            MetadataMigrationPhase::Aborted,
        ] {
            let mut authority = holt_runtime().initial_authority(shard());
            authority.migration = Some(migration(&authority, phase));
            let error = validate_migration_admission(&authority).unwrap_err();
            assert!(error.to_string().contains(&format!("{phase:?}")));
        }
    }

    #[test]
    fn migration_rejection_has_no_owner_store_or_registry_side_effects() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let current = control.get_metadata_authority(&shard()).unwrap().unwrap();
        let mut preparing = current.clone();
        preparing.record_revision =
            MetadataAuthorityRevision::new(current.record_revision.get() + 1).unwrap();
        preparing.migration = Some(migration(&current, MetadataMigrationPhase::Preparing));
        control
            .compare_and_set_metadata_authority(&current, preparing)
            .unwrap();
        let registry = Arc::new(RootOwnerRegistry::new());

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("Preparing"));
        let shard_record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(shard_record.owner.is_none());
        assert!(shard_record.owner_epoch.is_none());
        assert_eq!(shard_record.lease_id, 0);
        assert!(!database.exists());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn successor_transition_is_generic_and_built_in_holt_remains_unavailable() {
        let successor = OwnerAdmission::Acquire {
            owner: NodeId::new("node-b").unwrap(),
            owner_incarnation_id: incarnation(2),
            endpoint: "127.0.0.1:9020".to_owned(),
            expected_previous_epoch: Some(OwnerEpoch::new(7).unwrap()),
        };
        let transition = lifecycle_transition(OpenIntent::ReopenExisting, &successor).unwrap();
        assert_eq!(transition, LifecycleTransition::SuccessorReopen);
        test_descriptor()
            .classify_bootstrap(OpenIntent::ReopenExisting, transition)
            .unwrap();
        assert!(crate::holt_runtime_descriptor()
            .unwrap()
            .classify_bootstrap(OpenIntent::ReopenExisting, transition)
            .is_err());
    }

    #[cfg(feature = "foundationdb-provider")]
    #[test]
    fn foundationdb_bootstrap_is_not_qualified_before_control_or_runtime_side_effects() {
        let temporary = TempDir::new().unwrap();
        let cluster_file = temporary.path().join("fdb.cluster");
        std::fs::write(&cluster_file, "nokv:0123456789abcdef@127.0.0.1:4500\n").unwrap();
        let config = crate::FoundationDbRuntimeConfig::from_cluster_file(
            cluster_file,
            "openviking/metadata",
            crate::FoundationDbTransactionPolicy::default(),
        )
        .unwrap();
        let runtime_factory = crate::foundationdb_runtime_factory(&config).unwrap();
        let profile_id = runtime_factory.descriptor().profile_id().clone();
        let runtime_registry = crate::RuntimeRegistry::new(vec![runtime_factory]).unwrap();
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        let registry = Arc::new(RootOwnerRegistry::new());
        let error = runtime_registry.resolve(&profile_id).unwrap_err();
        assert!(error.to_string().contains("not qualified"));
        let shard_record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(shard_record.owner.is_none());
        assert_eq!(shard_record.lease_id, 0);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    fn as_control(control: &Arc<InMemoryControlStore>) -> Arc<dyn ControlStore> {
        control.clone()
    }

    struct AcquireBarrierControl {
        inner: Arc<InMemoryControlStore>,
        acquire_barrier: Option<Barrier>,
        reject_mark_serving: bool,
        release_faults: Mutex<VecDeque<ReleaseFault>>,
    }

    enum ReleaseFault {
        FailBeforeMutation,
        OutcomeUnknownBeforeMutation,
        CommitBeforeUnknownResponse,
    }

    impl AcquireBarrierControl {
        fn new(inner: Arc<InMemoryControlStore>, participants: usize) -> Self {
            Self {
                inner,
                acquire_barrier: Some(Barrier::new(participants)),
                reject_mark_serving: false,
                release_faults: Mutex::new(VecDeque::new()),
            }
        }

        fn rejecting_mark_serving(inner: Arc<InMemoryControlStore>) -> Self {
            Self {
                inner,
                acquire_barrier: None,
                reject_mark_serving: true,
                release_faults: Mutex::new(VecDeque::new()),
            }
        }

        fn release_faulting(
            inner: Arc<InMemoryControlStore>,
            faults: impl IntoIterator<Item = ReleaseFault>,
        ) -> Self {
            Self {
                inner,
                acquire_barrier: None,
                reject_mark_serving: false,
                release_faults: Mutex::new(faults.into_iter().collect()),
            }
        }
    }

    impl ControlStore for AcquireBarrierControl {
        fn owner_lease_model(&self) -> OwnerLeaseModel {
            self.inner.owner_lease_model()
        }

        fn prepare_owner_admission(
            &self,
            command: nokv_control::PrepareOwnerAdmissionCommandV1,
        ) -> nokv_control::PrepareOwnerAdmissionOutcomeV1 {
            self.inner.prepare_owner_admission(command)
        }

        fn commit_owner_admission(
            &self,
            command: nokv_control::CommitOwnerAdmissionCommandV1,
        ) -> nokv_control::CommitOwnerAdmissionOutcomeV1 {
            self.inner.commit_owner_admission(command)
        }

        fn abort_owner_admission(
            &self,
            command: nokv_control::AbortOwnerAdmissionCommandV1,
        ) -> nokv_control::AbortOwnerAdmissionOutcomeV1 {
            self.inner.abort_owner_admission(command)
        }

        fn terminate_owner_admission(
            &self,
            command: nokv_control::TerminateOwnerAdmissionCommandV1,
        ) -> nokv_control::TerminateOwnerAdmissionOutcomeV1 {
            self.inner.terminate_owner_admission(command)
        }

        fn reconcile_owner_admission(
            &self,
            command: nokv_control::ReconcileOwnerAdmissionCommandV1,
        ) -> nokv_control::ReconcileOwnerAdmissionOutcomeV1 {
            self.inner.reconcile_owner_admission(command)
        }

        fn publish_owner_serving(
            &self,
            command: nokv_control::PublishOwnerServingCommandV1,
        ) -> nokv_control::PublishOwnerServingOutcomeV1 {
            if self.reject_mark_serving {
                let claimed = command.claim_execution();
                return claimed.complete(nokv_control::PublishOwnerServingResultV1::NotDispatched(
                    nokv_control::PublishOwnerServingNotDispatchedV1::BackendUnavailableBeforeEffect,
                ));
            }
            self.inner.publish_owner_serving(command)
        }

        fn renew_owner_session(
            &self,
            command: nokv_control::RenewOwnerSessionCommandV1,
        ) -> nokv_control::RenewOwnerSessionOutcomeV1 {
            self.inner.renew_owner_session(command)
        }

        fn provision_fresh_root(
            &self,
            initial_placement: RootPlacement,
            initial_authority: MetadataAuthorityRecord,
        ) -> Result<nokv_control::FreshRootProvisioningOutcome, ControlError> {
            self.inner
                .provision_fresh_root(initial_placement, initial_authority)
        }

        fn create_root_placement(
            &self,
            placement: RootPlacement,
        ) -> Result<RootPlacement, ControlError> {
            self.inner.create_root_placement(placement)
        }

        fn get_root_placement(
            &self,
            root_id: &RootId,
        ) -> Result<Option<RootPlacement>, ControlError> {
            self.inner.get_root_placement(root_id)
        }

        fn list_root_placements(&self) -> Result<Vec<RootPlacement>, ControlError> {
            self.inner.list_root_placements()
        }

        fn compare_and_set_root_placement(
            &self,
            expected: &RootPlacement,
            next: RootPlacement,
        ) -> Result<RootPlacement, ControlError> {
            self.inner.compare_and_set_root_placement(expected, next)
        }

        fn create_logical_shard(
            &self,
            logical_shard_id: LogicalShardId,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.create_logical_shard(logical_shard_id)
        }

        fn get_logical_shard(
            &self,
            logical_shard_id: &LogicalShardId,
        ) -> Result<Option<LogicalShardRecord>, ControlError> {
            self.inner.get_logical_shard(logical_shard_id)
        }

        fn list_logical_shards(&self) -> Result<Vec<LogicalShardRecord>, ControlError> {
            self.inner.list_logical_shards()
        }

        fn create_metadata_authority(
            &self,
            authority: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, ControlError> {
            self.inner.create_metadata_authority(authority)
        }

        fn get_metadata_authority(
            &self,
            logical_shard_id: &LogicalShardId,
        ) -> Result<Option<MetadataAuthorityRecord>, ControlError> {
            self.inner.get_metadata_authority(logical_shard_id)
        }

        fn compare_and_set_metadata_authority(
            &self,
            expected: &MetadataAuthorityRecord,
            next: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, ControlError> {
            self.inner
                .compare_and_set_metadata_authority(expected, next)
        }

        fn acquire_owner(
            &self,
            admission: &OwnerServingAdmission,
            owner: NodeId,
            owner_incarnation_id: OwnerIncarnationId,
            endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            if let Some(barrier) = self.acquire_barrier.as_ref() {
                barrier.wait();
            }
            self.inner
                .acquire_owner(admission, owner, owner_incarnation_id, endpoint)
        }

        fn acquire_successor(
            &self,
            admission: &OwnerServingAdmission,
            expected_owner_epoch: OwnerEpoch,
            owner: NodeId,
            owner_incarnation_id: OwnerIncarnationId,
            endpoint: String,
        ) -> Result<LogicalShardLease, ControlError> {
            self.inner.acquire_successor(
                admission,
                expected_owner_epoch,
                owner,
                owner_incarnation_id,
                endpoint,
            )
        }

        fn renew_owner(
            &self,
            lease: &LogicalShardLease,
            admission: &OwnerServingAdmission,
        ) -> Result<LogicalShardRecord, ControlError> {
            self.inner.renew_owner(lease, admission)
        }

        fn mark_serving(
            &self,
            lease: &LogicalShardLease,
            admission: &OwnerServingAdmission,
            publication: RecoveryPublication,
        ) -> Result<LogicalShardRecord, ControlError> {
            if self.reject_mark_serving {
                return Err(ControlError::StaleLease(lease.clone()));
            }
            self.inner.mark_serving(lease, admission, publication)
        }

        fn release_owner(
            &self,
            lease: &LogicalShardLease,
        ) -> Result<nokv_control::OwnerReleaseOutcome, ControlError> {
            match self.release_faults.lock().unwrap().pop_front() {
                Some(ReleaseFault::FailBeforeMutation) => Err(ControlError::Backend(
                    "injected pre-mutation release failure".to_owned(),
                )),
                Some(ReleaseFault::OutcomeUnknownBeforeMutation) => {
                    Ok(nokv_control::OwnerReleaseOutcome::OutcomeUnknown)
                }
                Some(ReleaseFault::CommitBeforeUnknownResponse) => {
                    self.inner.release_owner(lease)?;
                    Ok(nokv_control::OwnerReleaseOutcome::OutcomeUnknown)
                }
                None => self.inner.release_owner(lease),
            }
        }
    }

    struct LayoutRejectControl {
        placement: RootPlacement,
    }

    impl ControlStore for LayoutRejectControl {
        fn owner_lease_model(&self) -> OwnerLeaseModel {
            OwnerLeaseModel::NonExpiring
        }

        fn prepare_owner_admission(
            &self,
            _: nokv_control::PrepareOwnerAdmissionCommandV1,
        ) -> nokv_control::PrepareOwnerAdmissionOutcomeV1 {
            panic!("layout rejection must precede planned owner preparation")
        }

        fn commit_owner_admission(
            &self,
            _: nokv_control::CommitOwnerAdmissionCommandV1,
        ) -> nokv_control::CommitOwnerAdmissionOutcomeV1 {
            panic!("layout rejection must precede planned owner commit")
        }

        fn abort_owner_admission(
            &self,
            _: nokv_control::AbortOwnerAdmissionCommandV1,
        ) -> nokv_control::AbortOwnerAdmissionOutcomeV1 {
            panic!("layout rejection must precede planned owner abort")
        }

        fn terminate_owner_admission(
            &self,
            _: nokv_control::TerminateOwnerAdmissionCommandV1,
        ) -> nokv_control::TerminateOwnerAdmissionOutcomeV1 {
            panic!("layout rejection must precede planned owner termination")
        }

        fn reconcile_owner_admission(
            &self,
            _: nokv_control::ReconcileOwnerAdmissionCommandV1,
        ) -> nokv_control::ReconcileOwnerAdmissionOutcomeV1 {
            panic!("layout rejection must precede planned owner reconciliation")
        }

        fn publish_owner_serving(
            &self,
            _: nokv_control::PublishOwnerServingCommandV1,
        ) -> nokv_control::PublishOwnerServingOutcomeV1 {
            panic!("layout rejection must precede serving publication")
        }

        fn renew_owner_session(
            &self,
            _: nokv_control::RenewOwnerSessionCommandV1,
        ) -> nokv_control::RenewOwnerSessionOutcomeV1 {
            panic!("layout rejection must precede owner-session renewal")
        }

        fn provision_fresh_root(
            &self,
            _: RootPlacement,
            _: MetadataAuthorityRecord,
        ) -> Result<nokv_control::FreshRootProvisioningOutcome, nokv_control::ControlError>
        {
            panic!("layout rejection must precede control mutation")
        }

        fn create_root_placement(
            &self,
            _: RootPlacement,
        ) -> Result<RootPlacement, nokv_control::ControlError> {
            panic!("layout rejection must precede control mutation")
        }

        fn get_root_placement(
            &self,
            root_id: &RootId,
        ) -> Result<Option<RootPlacement>, nokv_control::ControlError> {
            assert_eq!(root_id, &self.placement.root_id);
            Ok(Some(self.placement.clone()))
        }

        fn list_root_placements(&self) -> Result<Vec<RootPlacement>, nokv_control::ControlError> {
            panic!("layout rejection must precede control enumeration")
        }

        fn compare_and_set_root_placement(
            &self,
            _: &RootPlacement,
            _: RootPlacement,
        ) -> Result<RootPlacement, nokv_control::ControlError> {
            panic!("layout rejection must precede control mutation")
        }

        fn create_logical_shard(
            &self,
            _: LogicalShardId,
        ) -> Result<LogicalShardRecord, nokv_control::ControlError> {
            panic!("layout rejection must precede shard access")
        }

        fn get_logical_shard(
            &self,
            _: &LogicalShardId,
        ) -> Result<Option<LogicalShardRecord>, nokv_control::ControlError> {
            panic!("layout rejection must precede shard access")
        }

        fn list_logical_shards(
            &self,
        ) -> Result<Vec<LogicalShardRecord>, nokv_control::ControlError> {
            panic!("layout rejection must precede shard access")
        }

        fn create_metadata_authority(
            &self,
            _: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, nokv_control::ControlError> {
            panic!("layout rejection must precede authority access")
        }

        fn get_metadata_authority(
            &self,
            _: &LogicalShardId,
        ) -> Result<Option<MetadataAuthorityRecord>, nokv_control::ControlError> {
            panic!("layout rejection must precede authority access")
        }

        fn compare_and_set_metadata_authority(
            &self,
            _: &MetadataAuthorityRecord,
            _: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, nokv_control::ControlError> {
            panic!("layout rejection must precede authority access")
        }

        fn acquire_owner(
            &self,
            _: &OwnerServingAdmission,
            _: NodeId,
            _: OwnerIncarnationId,
            _: String,
        ) -> Result<LogicalShardLease, nokv_control::ControlError> {
            panic!("layout rejection must precede owner acquisition")
        }

        fn acquire_successor(
            &self,
            _: &OwnerServingAdmission,
            _: OwnerEpoch,
            _: NodeId,
            _: OwnerIncarnationId,
            _: String,
        ) -> Result<LogicalShardLease, nokv_control::ControlError> {
            panic!("layout rejection must precede owner acquisition")
        }

        fn renew_owner(
            &self,
            _: &LogicalShardLease,
            _: &OwnerServingAdmission,
        ) -> Result<LogicalShardRecord, nokv_control::ControlError> {
            panic!("layout rejection must precede owner admission")
        }

        fn mark_serving(
            &self,
            _: &LogicalShardLease,
            _: &OwnerServingAdmission,
            _: RecoveryPublication,
        ) -> Result<LogicalShardRecord, nokv_control::ControlError> {
            panic!("layout rejection must precede serving publication")
        }

        fn release_owner(
            &self,
            _: &LogicalShardLease,
        ) -> Result<nokv_control::OwnerReleaseOutcome, nokv_control::ControlError> {
            panic!("layout rejection must precede owner mutation")
        }
    }

    struct PanicOnControlCallStore {
        lease_model: OwnerLeaseModel,
        unexpected_control_calls: AtomicUsize,
    }

    impl PanicOnControlCallStore {
        fn finite() -> Self {
            Self {
                lease_model: OwnerLeaseModel::FiniteAuthoritativeTtl,
                unexpected_control_calls: AtomicUsize::new(0),
            }
        }

        fn non_expiring() -> Self {
            Self {
                lease_model: OwnerLeaseModel::NonExpiring,
                unexpected_control_calls: AtomicUsize::new(0),
            }
        }

        fn unexpected_control_call(&self, operation: &str) -> ! {
            self.unexpected_control_calls.fetch_add(1, Ordering::SeqCst);
            panic!("admission gate must precede control operation {operation}")
        }
    }

    impl ControlStore for PanicOnControlCallStore {
        fn owner_lease_model(&self) -> OwnerLeaseModel {
            self.lease_model
        }

        fn prepare_owner_admission(
            &self,
            _: nokv_control::PrepareOwnerAdmissionCommandV1,
        ) -> nokv_control::PrepareOwnerAdmissionOutcomeV1 {
            self.unexpected_control_call("prepare_owner_admission")
        }

        fn commit_owner_admission(
            &self,
            _: nokv_control::CommitOwnerAdmissionCommandV1,
        ) -> nokv_control::CommitOwnerAdmissionOutcomeV1 {
            self.unexpected_control_call("commit_owner_admission")
        }

        fn abort_owner_admission(
            &self,
            _: nokv_control::AbortOwnerAdmissionCommandV1,
        ) -> nokv_control::AbortOwnerAdmissionOutcomeV1 {
            self.unexpected_control_call("abort_owner_admission")
        }

        fn terminate_owner_admission(
            &self,
            _: nokv_control::TerminateOwnerAdmissionCommandV1,
        ) -> nokv_control::TerminateOwnerAdmissionOutcomeV1 {
            self.unexpected_control_call("terminate_owner_admission")
        }

        fn reconcile_owner_admission(
            &self,
            _: nokv_control::ReconcileOwnerAdmissionCommandV1,
        ) -> nokv_control::ReconcileOwnerAdmissionOutcomeV1 {
            self.unexpected_control_call("reconcile_owner_admission")
        }

        fn publish_owner_serving(
            &self,
            _: nokv_control::PublishOwnerServingCommandV1,
        ) -> nokv_control::PublishOwnerServingOutcomeV1 {
            self.unexpected_control_call("publish_owner_serving")
        }

        fn renew_owner_session(
            &self,
            _: nokv_control::RenewOwnerSessionCommandV1,
        ) -> nokv_control::RenewOwnerSessionOutcomeV1 {
            self.unexpected_control_call("renew_owner_session")
        }

        fn provision_fresh_root(
            &self,
            _: RootPlacement,
            _: MetadataAuthorityRecord,
        ) -> Result<nokv_control::FreshRootProvisioningOutcome, nokv_control::ControlError>
        {
            self.unexpected_control_call("provision_fresh_root")
        }

        fn create_root_placement(
            &self,
            _: RootPlacement,
        ) -> Result<RootPlacement, nokv_control::ControlError> {
            self.unexpected_control_call("create_root_placement")
        }

        fn get_root_placement(
            &self,
            _: &RootId,
        ) -> Result<Option<RootPlacement>, nokv_control::ControlError> {
            self.unexpected_control_call("get_root_placement")
        }

        fn list_root_placements(&self) -> Result<Vec<RootPlacement>, nokv_control::ControlError> {
            self.unexpected_control_call("list_root_placements")
        }

        fn compare_and_set_root_placement(
            &self,
            _: &RootPlacement,
            _: RootPlacement,
        ) -> Result<RootPlacement, nokv_control::ControlError> {
            self.unexpected_control_call("compare_and_set_root_placement")
        }

        fn create_logical_shard(
            &self,
            _: LogicalShardId,
        ) -> Result<LogicalShardRecord, nokv_control::ControlError> {
            self.unexpected_control_call("create_logical_shard")
        }

        fn get_logical_shard(
            &self,
            _: &LogicalShardId,
        ) -> Result<Option<LogicalShardRecord>, nokv_control::ControlError> {
            self.unexpected_control_call("get_logical_shard")
        }

        fn list_logical_shards(
            &self,
        ) -> Result<Vec<LogicalShardRecord>, nokv_control::ControlError> {
            self.unexpected_control_call("list_logical_shards")
        }

        fn create_metadata_authority(
            &self,
            _: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, nokv_control::ControlError> {
            self.unexpected_control_call("create_metadata_authority")
        }

        fn get_metadata_authority(
            &self,
            _: &LogicalShardId,
        ) -> Result<Option<MetadataAuthorityRecord>, nokv_control::ControlError> {
            self.unexpected_control_call("get_metadata_authority")
        }

        fn compare_and_set_metadata_authority(
            &self,
            _: &MetadataAuthorityRecord,
            _: MetadataAuthorityRecord,
        ) -> Result<MetadataAuthorityRecord, nokv_control::ControlError> {
            self.unexpected_control_call("compare_and_set_metadata_authority")
        }

        fn acquire_owner(
            &self,
            _: &OwnerServingAdmission,
            _: NodeId,
            _: OwnerIncarnationId,
            _: String,
        ) -> Result<LogicalShardLease, nokv_control::ControlError> {
            self.unexpected_control_call("acquire_owner")
        }

        fn acquire_successor(
            &self,
            _: &OwnerServingAdmission,
            _: OwnerEpoch,
            _: NodeId,
            _: OwnerIncarnationId,
            _: String,
        ) -> Result<LogicalShardLease, nokv_control::ControlError> {
            self.unexpected_control_call("acquire_successor")
        }

        fn renew_owner(
            &self,
            _: &LogicalShardLease,
            _: &OwnerServingAdmission,
        ) -> Result<LogicalShardRecord, nokv_control::ControlError> {
            self.unexpected_control_call("renew_owner")
        }

        fn mark_serving(
            &self,
            _: &LogicalShardLease,
            _: &OwnerServingAdmission,
            _: RecoveryPublication,
        ) -> Result<LogicalShardRecord, nokv_control::ControlError> {
            self.unexpected_control_call("mark_serving")
        }

        fn release_owner(
            &self,
            _: &LogicalShardLease,
        ) -> Result<nokv_control::OwnerReleaseOutcome, nokv_control::ControlError> {
            self.unexpected_control_call("release_owner")
        }
    }

    #[test]
    fn finite_owner_lease_is_rejected_before_control_records_store_or_registry_access() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = Arc::new(PanicOnControlCallStore::finite());
        let registry = Arc::new(RootOwnerRegistry::new());

        let error = characterize_bootstrap_after_admission(
            control.clone(),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .err()
        .unwrap();

        assert!(error
            .to_string()
            .contains(OWNER_LEASE_EXPIRY_ADMISSION_NOT_QUALIFIED));
        assert_eq!(control.unexpected_control_calls.load(Ordering::SeqCst), 0);
        assert!(!database.exists());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn unsupported_layout_is_rejected_before_owner_acquire_or_store_open() {
        let temporary = TempDir::new().unwrap();
        let base = RootPlacement {
            root_id: root(),
            layout_profile: RootLayoutProfile::SingleShardRoot,
            layout_generation: RootLayoutGeneration::new(1).unwrap(),
            partition_id: RootPartitionId::SINGLE_SHARD,
            logical_shard_id: shard(),
            placement_generation: PlacementGeneration::new(2).unwrap(),
            lifecycle: RootPlacementLifecycle::Active,
        };
        let cases = [
            (
                "profile",
                RootPlacement {
                    layout_profile: RootLayoutProfile::PartitionedRoot,
                    partition_id: RootPartitionId::from_bytes([0x33; 16]),
                    ..base.clone()
                },
                "not qualified",
            ),
            (
                "generation",
                RootPlacement {
                    layout_generation: RootLayoutGeneration::new(2).unwrap(),
                    ..base.clone()
                },
                "generation 2 is not qualified",
            ),
            (
                "partition",
                RootPlacement {
                    partition_id: RootPartitionId::from_bytes([0x44; 16]),
                    ..base
                },
                "SINGLE_SHARD",
            ),
        ];

        for (name, placement, expected) in cases {
            let database = temporary.path().join(name);
            let control: Arc<dyn ControlStore> = Arc::new(LayoutRejectControl { placement });
            let error = characterize_bootstrap_after_admission(
                control,
                Arc::new(RootOwnerRegistry::new()),
                acquire_request(database.clone()),
            )
            .err()
            .unwrap();

            assert!(error.to_string().contains(expected));
            assert!(!database.exists());
        }
    }

    #[test]
    fn competing_fresh_bootstraps_admit_exactly_one_before_provider_create() {
        let temporary = TempDir::new().unwrap();
        let database_a = temporary.path().join("metadata-a");
        let database_b = temporary.path().join("metadata-b");
        let inner_control = active_control();
        let control: Arc<dyn ControlStore> =
            Arc::new(AcquireBarrierControl::new(Arc::clone(&inner_control), 2));
        let services_a = Arc::new(TestRuntimeServices::default());
        let services_b = Arc::new(TestRuntimeServices::default());

        let mut request_a = acquire_request(database_a.clone());
        request_a.runtime = file_runtime_with_services(
            database_a.clone(),
            test_descriptor(),
            Arc::clone(&services_a),
        );
        let mut request_b = acquire_request(database_b.clone());
        request_b.runtime = file_runtime_with_services(
            database_b.clone(),
            test_descriptor(),
            Arc::clone(&services_b),
        );
        request_b.admission = OwnerAdmission::Acquire {
            owner: NodeId::new("node-b").unwrap(),
            owner_incarnation_id: incarnation(2),
            endpoint: "127.0.0.1:9020".to_owned(),
            expected_previous_epoch: None,
        };
        request_b.install_request_id = request_id(0x51);
        request_b.activate_request_id = request_id(0x52);

        let control_a = Arc::clone(&control);
        let thread_a = std::thread::spawn(move || {
            characterize_bootstrap_after_admission(
                control_a,
                Arc::new(RootOwnerRegistry::new()),
                request_a,
            )
        });
        let control_b = Arc::clone(&control);
        let thread_b = std::thread::spawn(move || {
            characterize_bootstrap_after_admission(
                control_b,
                Arc::new(RootOwnerRegistry::new()),
                request_b,
            )
        });
        let result_a = thread_a.join().unwrap();
        let result_b = thread_b.join().unwrap();

        let (winner, loser_error, winner_database, loser_database, winner_services, loser_services) =
            match (result_a, result_b) {
                (Ok(winner), Err(loser_error)) => (
                    winner,
                    loser_error,
                    &database_a,
                    &database_b,
                    &services_a,
                    &services_b,
                ),
                (Err(loser_error), Ok(winner)) => (
                    winner,
                    loser_error,
                    &database_b,
                    &database_a,
                    &services_b,
                    &services_a,
                ),
                (Ok(_), Ok(_)) => panic!("one owner CAS must lose"),
                (Err(first), Err(second)) => {
                    panic!("one owner CAS must win: first={first}; second={second}")
                }
            };

        assert!(matches!(
            loser_error,
            ServerError::Control(ControlError::LogicalShardAlreadyOwned { .. })
        ));
        assert_eq!(
            winner_services.create_delegate_calls.load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            loser_services.create_delegate_calls.load(Ordering::SeqCst),
            0
        );
        assert!(winner_database.exists());
        assert!(!loser_database.exists());
        winner.ownership.release().unwrap();
    }

    #[test]
    fn live_owner_successor_loser_never_reopens_provider_or_creates_locator() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let first = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_request(temporary.path().join("active-metadata")),
        )
        .unwrap();
        let loser_database = temporary.path().join("loser-metadata");
        let loser_services = Arc::new(TestRuntimeServices::default());

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    loser_database.clone(),
                    test_descriptor(),
                    Arc::clone(&loser_services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    owner_incarnation_id: incarnation(2),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: Some(first.lease.owner_epoch),
                },
                install_request_id: request_id(0x53),
                activate_request_id: request_id(0x54),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(matches!(
            error,
            ServerError::Control(ControlError::PreviousOwnerSessionLive { .. })
        ));
        assert_eq!(
            loser_services.reopen_delegate_calls.load(Ordering::SeqCst),
            0
        );
        assert!(!loser_database.exists());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.owner_epoch, Some(first.lease.owner_epoch));
        assert_eq!(record.state, LogicalShardState::Serving);
        first.ownership.release().unwrap();
    }

    #[test]
    fn exact_resume_is_rejected_before_stale_lease_or_provider_inspection() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let authority = control.get_metadata_authority(&shard()).unwrap().unwrap();
        let mut stale_lease = LogicalShardLease {
            logical_shard_id: shard(),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            owner_incarnation_id: incarnation(1),
            lease_id: 1,
            authority: authority.fence(),
        };
        stale_lease.authority.authority_id =
            MetadataAuthorityId::from_bytes([0xa7; nokv_types::FIXED_ID_BYTES]);
        let services = Arc::new(TestRuntimeServices::default());

        let error = super::bootstrap_root_owner(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    database.clone(),
                    test_descriptor(),
                    Arc::clone(&services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Resume { lease: stale_lease },
                install_request_id: request_id(0x55),
                activate_request_id: request_id(0x56),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(matches!(
            error,
            ServerError::InvalidBootstrap(ref code)
                if code == &AdmissionCode::ExactResumeNotQualifiedV1.to_string()
        ));
        assert_eq!(services.reopen_delegate_calls.load(Ordering::SeqCst), 0);
        assert!(!database.exists());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(record.owner.is_none());
        assert!(record.owner_epoch.is_none());
    }

    #[test]
    fn public_bootstrap_rejects_all_unrecoverable_owner_transitions_without_side_effects() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("must-not-exist");
        let services = Arc::new(TestRuntimeServices::default());
        let runtime =
            file_runtime_with_services(database.clone(), test_descriptor(), Arc::clone(&services));
        let control = Arc::new(PanicOnControlCallStore::non_expiring());
        let registry = Arc::new(RootOwnerRegistry::new());
        let authority = test_descriptor().initial_authority(shard()).fence();
        let lease = LogicalShardLease {
            logical_shard_id: shard(),
            owner: NodeId::new("node-a").unwrap(),
            owner_epoch: OwnerEpoch::new(1).unwrap(),
            owner_incarnation_id: incarnation(1),
            lease_id: 7,
            authority,
        };
        let cases = [
            (
                OpenIntent::CreateFresh,
                OwnerAdmission::Acquire {
                    owner: NodeId::new("node-a").unwrap(),
                    owner_incarnation_id: incarnation(1),
                    endpoint: "127.0.0.1:9010".to_owned(),
                    expected_previous_epoch: None,
                },
                AdmissionCode::PlannedOwnerAdmissionNotQualifiedV1,
            ),
            (
                OpenIntent::ReopenExisting,
                OwnerAdmission::Acquire {
                    owner: NodeId::new("node-a").unwrap(),
                    owner_incarnation_id: incarnation(2),
                    endpoint: "127.0.0.1:9010".to_owned(),
                    expected_previous_epoch: Some(OwnerEpoch::new(1).unwrap()),
                },
                AdmissionCode::PlannedOwnerAdmissionNotQualifiedV1,
            ),
            (
                OpenIntent::ReconcilePreparedCreate,
                OwnerAdmission::Acquire {
                    owner: NodeId::new("node-a").unwrap(),
                    owner_incarnation_id: incarnation(3),
                    endpoint: "127.0.0.1:9010".to_owned(),
                    expected_previous_epoch: None,
                },
                AdmissionCode::PlannedOwnerAdmissionNotQualifiedV1,
            ),
            (
                OpenIntent::ReconcilePreparedCreate,
                OwnerAdmission::Acquire {
                    owner: NodeId::new("node-a").unwrap(),
                    owner_incarnation_id: incarnation(4),
                    endpoint: "127.0.0.1:9010".to_owned(),
                    expected_previous_epoch: Some(OwnerEpoch::new(1).unwrap()),
                },
                AdmissionCode::PlannedOwnerAdmissionNotQualifiedV1,
            ),
            (
                OpenIntent::ReopenExisting,
                OwnerAdmission::Resume {
                    lease: lease.clone(),
                },
                AdmissionCode::ExactResumeNotQualifiedV1,
            ),
            (
                OpenIntent::ReconcilePreparedCreate,
                OwnerAdmission::ResumePreparedOrAcquireSuccessor {
                    lease,
                    endpoint: "127.0.0.1:9010".to_owned(),
                },
                AdmissionCode::PreparedResumeOrSuccessorNotQualifiedV1,
            ),
        ];

        for (index, (open_intent, admission, expected)) in cases.into_iter().enumerate() {
            let error = super::bootstrap_root_owner(
                control.clone(),
                Arc::clone(&registry),
                RootOwnerBootstrapRequest {
                    root_id: root(),
                    runtime: runtime.clone(),
                    open_intent,
                    admission,
                    install_request_id: request_id(0x70 + index as u8 * 2),
                    activate_request_id: request_id(0x71 + index as u8 * 2),
                    recovery: empty_recovery(),
                },
            )
            .err()
            .unwrap();
            assert!(matches!(
                error,
                ServerError::InvalidBootstrap(ref code) if code == &expected.to_string()
            ));
        }

        assert_eq!(control.unexpected_control_calls.load(Ordering::SeqCst), 0);
        assert_eq!(services.create_delegate_calls.load(Ordering::SeqCst), 0);
        assert_eq!(services.reopen_delegate_calls.load(Ordering::SeqCst), 0);
        assert_eq!(services.release_receipt_calls.load(Ordering::SeqCst), 0);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        assert!(!database.exists());
    }

    #[test]
    fn prepared_exact_resume_is_not_qualified_before_provider_open() {
        let temporary = TempDir::new().unwrap();
        let missing_database = temporary.path().join("missing-prepared-metadata");
        let control = active_control();
        let admission = serving_admission(&control);
        let lease = control
            .acquire_owner(
                &admission,
                NodeId::new("node-a").unwrap(),
                incarnation(1),
                "127.0.0.1:9010".to_owned(),
            )
            .unwrap();
        let services = Arc::new(TestRuntimeServices::default());

        let error = super::bootstrap_root_owner(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    missing_database.clone(),
                    test_descriptor(),
                    Arc::clone(&services),
                ),
                open_intent: OpenIntent::ReconcilePreparedCreate,
                admission: OwnerAdmission::ResumePreparedOrAcquireSuccessor {
                    lease: lease.clone(),
                    endpoint: "127.0.0.1:9010".to_owned(),
                },
                install_request_id: request_id(0x5b),
                activate_request_id: request_id(0x5c),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(matches!(
            error,
            ServerError::InvalidBootstrap(ref code)
                if code
                    == &AdmissionCode::PreparedResumeOrSuccessorNotQualifiedV1.to_string()
        ));
        assert_eq!(services.create_delegate_calls.load(Ordering::SeqCst), 0);
        assert!(!missing_database.exists());
        let retained = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(retained.owner.as_ref(), Some(&lease.owner));
        assert_eq!(retained.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(retained.lease_id, lease.lease_id);
        assert_eq!(retained.state, LogicalShardState::Recovering);
        control.renew_owner(&lease, &admission).unwrap();
        control.release_owner(&lease).unwrap();
    }

    #[test]
    fn post_admission_open_failures_release_new_leases_and_require_explicit_successors() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let missing_database = temporary.path().join("missing-successor-metadata");
        let control = active_control();

        let first_services = Arc::new(TestRuntimeServices::default());
        first_services
            .reject_commit_receipt_resolution
            .store(true, Ordering::Release);
        let mut first_request = acquire_request(database.clone());
        first_request.runtime = file_runtime_with_services(
            database.clone(),
            test_descriptor(),
            Arc::clone(&first_services),
        );
        let first_error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            first_request,
        )
        .err()
        .unwrap();
        assert!(
            matches!(first_error, ServerError::InvalidBootstrap(ref reason)
            if reason.contains("RuntimeBundlePoisoned"))
        );
        assert_eq!(
            first_services.create_delegate_calls.load(Ordering::SeqCst),
            1
        );
        assert!(database.exists());
        let after_first = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(after_first.owner.is_none());
        assert_eq!(after_first.owner_epoch, Some(OwnerEpoch::new(1).unwrap()));
        assert_eq!(after_first.state, LogicalShardState::Unassigned);

        let second_services = Arc::new(TestRuntimeServices::default());
        let second_error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    missing_database.clone(),
                    test_descriptor(),
                    Arc::clone(&second_services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    owner_incarnation_id: incarnation(2),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: after_first.owner_epoch,
                },
                install_request_id: request_id(0x57),
                activate_request_id: request_id(0x58),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();
        assert!(matches!(second_error, ServerError::Metadata(_)));
        assert_eq!(
            second_services.reopen_delegate_calls.load(Ordering::SeqCst),
            0
        );
        assert!(!missing_database.exists());
        let after_second = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(after_second.owner.is_none());
        assert_eq!(after_second.owner_epoch, Some(OwnerEpoch::new(2).unwrap()));
        assert_eq!(after_second.state, LogicalShardState::Unassigned);

        let recovery_services = Arc::new(TestRuntimeServices {
            commit_receipt: Arc::clone(&first_services.commit_receipt),
            ..TestRuntimeServices::default()
        });
        let recovery_error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    database.clone(),
                    test_descriptor(),
                    Arc::clone(&recovery_services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-c").unwrap(),
                    owner_incarnation_id: incarnation(3),
                    endpoint: "127.0.0.1:9030".to_owned(),
                    expected_previous_epoch: after_second.owner_epoch,
                },
                install_request_id: request_id(0x59),
                activate_request_id: request_id(0x5a),
                recovery: empty_recovery(),
            },
        )
        .err()
        .expect("the allocation that resolves Pending must remain recovery-only");
        assert!(matches!(
            recovery_error,
            ServerError::Metadata(
                nokv_meta::workspace::AgentMetadataError::CommitReceiptRecoveryRequired
            )
        ));
        assert_eq!(
            recovery_services
                .reopen_delegate_calls
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            recovery_services
                .recovery_reopen_delegate_calls
                .load(Ordering::SeqCst),
            0
        );
        let after_recovery = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(after_recovery.owner.is_none());
        assert_eq!(
            after_recovery.owner_epoch,
            Some(OwnerEpoch::new(3).unwrap())
        );
        assert_eq!(after_recovery.state, LogicalShardState::Unassigned);

        let serving_services = Arc::new(TestRuntimeServices {
            commit_receipt: Arc::clone(&first_services.commit_receipt),
            ..TestRuntimeServices::default()
        });
        let serving_error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    database,
                    test_descriptor(),
                    Arc::clone(&serving_services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-d").unwrap(),
                    owner_incarnation_id: incarnation(4),
                    endpoint: "127.0.0.1:9040".to_owned(),
                    expected_previous_epoch: after_recovery.owner_epoch,
                },
                install_request_id: request_id(0x5b),
                activate_request_id: request_id(0x5c),
                recovery: empty_recovery(),
            },
        )
        .err()
        .expect("an ordinary reopen cannot bypass dirty-receipt recovery");
        assert!(matches!(
            serving_error,
            ServerError::Metadata(
                nokv_meta::workspace::AgentMetadataError::CommitReceiptRecoveryRequired
            )
        ));
        assert_eq!(
            serving_services
                .reopen_delegate_calls
                .load(Ordering::SeqCst),
            0
        );
        assert_eq!(
            serving_services
                .recovery_reopen_delegate_calls
                .load(Ordering::SeqCst),
            0
        );
        let after_serving_attempt = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(after_serving_attempt.owner.is_none());
        assert_eq!(
            after_serving_attempt.owner_epoch,
            Some(OwnerEpoch::new(4).unwrap())
        );
        assert_eq!(after_serving_attempt.state, LogicalShardState::Unassigned);
    }

    #[test]
    fn mark_serving_failure_removes_pending_route_and_releases_acquired_lease() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let inner_control = active_control();
        let control: Arc<dyn ControlStore> = Arc::new(
            AcquireBarrierControl::rejecting_mark_serving(Arc::clone(&inner_control)),
        );
        let registry = Arc::new(RootOwnerRegistry::new());
        let services = Arc::new(TestRuntimeServices::default());
        let mut request = acquire_request(database.clone());
        request.runtime =
            file_runtime_with_services(database.clone(), test_descriptor(), Arc::clone(&services));

        let error = characterize_bootstrap_after_admission(control, Arc::clone(&registry), request)
            .err()
            .unwrap();

        assert!(matches!(
            error,
            ServerError::Control(ControlError::StaleLease(_))
        ));
        assert_eq!(services.create_delegate_calls.load(Ordering::SeqCst), 1);
        assert!(database.exists());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        let released = inner_control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(released.owner.is_none());
        assert_eq!(released.owner_epoch, Some(OwnerEpoch::new(1).unwrap()));
        assert_eq!(released.state, LogicalShardState::Unassigned);
    }

    #[test]
    fn fresh_bootstrap_activates_fence_installs_route_and_marks_serving() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();

        assert_eq!(owner.route.root_id, RootIdentity::from(root()));
        assert_eq!(
            owner.route.logical_shard_id,
            LogicalShardIdentity::from(shard())
        );
        assert_eq!(owner.route.placement_generation, 2);
        assert_eq!(owner.route.owner_epoch, 1);
        assert!(registry.contains_exact(owner.route).unwrap());
        assert_eq!(
            owner.store.root_fence(root()).unwrap().unwrap(),
            meta::RootFence {
                logical_shard_id: shard(),
                placement_generation: PlacementGeneration::new(2).unwrap(),
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                activation_state: RootActivationState::Active,
            }
        );
        assert_eq!(
            owner.store.current_owner_epoch().unwrap(),
            Some(owner.lease.owner_epoch)
        );
        assert_eq!(owner.serving_record.state, LogicalShardState::Serving);
        let authority = control.get_metadata_authority(&shard()).unwrap().unwrap();
        assert_eq!(
            owner.store.metadata_store_identity(),
            owner
                .runtime_descriptor()
                .validate_authority(&authority)
                .unwrap()
        );

        let server = WorkspaceServer::new(
            ServerOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                lease_renew_interval: Duration::from_millis(10),
            },
            registry,
            vec![owner.ownership],
        )
        .unwrap();
        server.renew_ownership().unwrap();
        assert_eq!(server.health().unwrap().installed_roots, 1);
    }

    #[test]
    fn external_profile_resolves_from_registry_and_reaches_generic_bootstrap() {
        let temporary = TempDir::new().unwrap();
        let descriptor = test_descriptor_with("external-registry-profile-v1", 0x6b);
        let profile_id = descriptor.profile_id().clone();
        let runtime = file_runtime_with_descriptor(
            temporary.path().join("external-metadata"),
            descriptor.clone(),
        );
        let factory: Arc<dyn crate::RuntimeFactory> = Arc::new(TestRegistryRuntimeFactory {
            descriptor: descriptor.clone(),
            resolved: runtime,
        });
        let runtime_registry = crate::RuntimeRegistry::new(vec![factory]).unwrap();
        let runtime = runtime_registry.resolve(&profile_id).unwrap();

        let control = active_control_with_descriptor(&descriptor);
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            registry,
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime,
                open_intent: OpenIntent::CreateFresh,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("external-node").unwrap(),
                    owner_incarnation_id: incarnation(5),
                    endpoint: "127.0.0.1:9030".to_owned(),
                    expected_previous_epoch: None,
                },
                install_request_id: request_id(0xa1),
                activate_request_id: request_id(0xa2),
                recovery: empty_recovery(),
            },
        )
        .unwrap();
        assert_eq!(
            owner.runtime_descriptor().profile_id().as_str(),
            "external-registry-profile-v1"
        );
        assert_eq!(owner.serving_record.state, LogicalShardState::Serving);
    }

    #[test]
    fn existing_fence_must_match_every_control_layout_field() {
        let placement = active_control()
            .get_root_placement(&root())
            .unwrap()
            .unwrap();
        let exact = meta::RootFence {
            logical_shard_id: placement.logical_shard_id,
            placement_generation: placement.placement_generation,
            layout_profile: placement.layout_profile,
            layout_generation: placement.layout_generation,
            partition_id: placement.partition_id,
            activation_state: RootActivationState::Active,
        };
        validate_existing_fence(&placement, exact).unwrap();

        let mismatches = [
            meta::RootFence {
                layout_profile: RootLayoutProfile::PartitionedRoot,
                ..exact
            },
            meta::RootFence {
                layout_generation: RootLayoutGeneration::new(2).unwrap(),
                ..exact
            },
            meta::RootFence {
                partition_id: RootPartitionId::from_bytes([0x77; 16]),
                ..exact
            },
        ];
        for mismatch in mismatches {
            assert!(matches!(
                validate_existing_fence(&placement, mismatch),
                Err(ServerError::InvalidBootstrap(_))
            ));
        }
    }

    #[test]
    fn missing_metadata_authority_is_rejected_without_store_or_owner_adoption() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id: root(),
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                logical_shard_id: shard(),
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        control
            .compare_and_set_root_placement(
                &provisioning,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..provisioning
                },
            )
            .unwrap();
        let registry = Arc::new(RootOwnerRegistry::new());

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .err()
        .unwrap();

        assert!(error
            .to_string()
            .contains("implicit store adoption is forbidden"));
        assert!(!database.exists());
        let shard_record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(shard_record.owner.is_none());
        assert_eq!(shard_record.lease_id, 0);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn mismatched_runtime_profile_fingerprint_is_rejected_before_owner_acquire() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        let mut authority = holt_runtime().initial_authority(shard());
        authority.active.profile_fingerprint = [0x7f; SHA256_BYTES];
        control.create_metadata_authority(authority).unwrap();
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id: root(),
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                logical_shard_id: shard(),
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        control
            .compare_and_set_root_placement(
                &provisioning,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..provisioning
                },
            )
            .unwrap();
        let registry = Arc::new(RootOwnerRegistry::new());

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("profile fingerprint"));
        assert!(!database.exists());
        assert!(control
            .get_logical_shard(&shard())
            .unwrap()
            .unwrap()
            .owner
            .is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn noncanonical_generation_one_consistency_domain_is_rejected() {
        let runtime = holt_runtime();
        let mut authority = runtime.initial_authority(shard());
        authority.active.consistency_domain_id =
            ConsistencyDomainId::from_bytes([0x7a; nokv_types::FIXED_ID_BYTES]);

        let error = runtime.validate_authority(&authority).unwrap_err();

        assert!(error.to_string().contains("consistency domain"));
    }

    #[test]
    fn exact_resume_is_not_qualified_before_foreign_store_reopen() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let authority = control.get_metadata_authority(&shard()).unwrap().unwrap();
        let runtime = holt_runtime();
        let mut wrong_identity = runtime.validate_authority(&authority).unwrap();
        wrong_identity.authority_id =
            MetadataAuthorityId::from_bytes([0x9a; nokv_types::FIXED_ID_BYTES]);
        let foreign_services = Arc::new(TestRuntimeServices::default());
        let foreign_bundle = Arc::new(TestExternalOwnerRuntimeBundle::new(
            database.clone(),
            foreign_services,
        ));
        meta::AgentMetadataStore::create_with_runtime_commit_bundle_v1(
            foreign_bundle,
            wrong_identity,
            nokv_meta::provider::v1::CreateRecoveryIntentV1::Fresh,
            meta::MetadataStoreCreateModeV1::Active,
        )
        .unwrap();
        let lease = control
            .acquire_owner(
                &serving_admission(&control),
                NodeId::new("node-a").unwrap(),
                incarnation(1),
                "127.0.0.1:9010".to_owned(),
            )
            .unwrap();
        let registry = Arc::new(RootOwnerRegistry::new());
        let resume_services = Arc::new(TestRuntimeServices::default());

        let error = super::bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    database,
                    test_descriptor(),
                    Arc::clone(&resume_services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Resume {
                    lease: lease.clone(),
                },
                install_request_id: request_id(91),
                activate_request_id: request_id(92),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(matches!(
            error,
            ServerError::InvalidBootstrap(ref code)
                if code == &AdmissionCode::ExactResumeNotQualifiedV1.to_string()
        ));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        assert_eq!(
            resume_services.reopen_delegate_calls.load(Ordering::SeqCst),
            0
        );
        let retained = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(retained.owner.as_ref(), Some(&lease.owner));
        assert_eq!(retained.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(retained.lease_id, lease.lease_id);
        assert_eq!(retained.state, LogicalShardState::Recovering);
        control
            .renew_owner(&lease, &serving_admission(&control))
            .unwrap();
        control.release_owner(&lease).unwrap();
    }

    #[test]
    fn provisioning_placement_is_not_owner_admission() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = Arc::new(InMemoryControlStore::new());
        control.create_logical_shard(shard()).unwrap();
        control
            .create_metadata_authority(holt_runtime().initial_authority(shard()))
            .unwrap();
        control
            .create_root_placement(RootPlacement {
                root_id: root(),
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                logical_shard_id: shard(),
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::new(RootOwnerRegistry::new()),
            acquire_request(database.clone()),
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("expected Active"));
        assert!(!database.exists());
        assert!(control
            .get_logical_shard(&shard())
            .unwrap()
            .unwrap()
            .owner
            .is_none());
    }

    #[test]
    fn second_root_on_one_local_shard_is_rejected_before_store_or_fence_side_effects() {
        let temporary = TempDir::new().unwrap();
        let first_database = temporary.path().join("first-metadata");
        let second_database = temporary.path().join("second-metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(first_database),
        )
        .unwrap();
        let provisioning = control
            .create_root_placement(RootPlacement {
                root_id: second_root(),
                layout_profile: RootLayoutProfile::SingleShardRoot,
                layout_generation: RootLayoutGeneration::new(1).unwrap(),
                partition_id: RootPartitionId::SINGLE_SHARD,
                logical_shard_id: shard(),
                placement_generation: PlacementGeneration::new(1).unwrap(),
                lifecycle: RootPlacementLifecycle::Provisioning,
            })
            .unwrap();
        control
            .compare_and_set_root_placement(
                &provisioning,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(2).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..provisioning
                },
            )
            .unwrap();

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: second_root(),
                runtime: file_runtime(second_database.clone()),
                open_intent: OpenIntent::CreateFresh,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    owner_incarnation_id: incarnation(2),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: None,
                },
                install_request_id: request_id(0x41),
                activate_request_id: request_id(0x42),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(matches!(error, ServerError::RouteRollback(_)));
        assert!(!second_database.exists());
        assert!(first.store.root_fence(second_root()).unwrap().is_none());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.owner_epoch, Some(first.lease.owner_epoch));
        assert_eq!(record.state, LogicalShardState::Serving);
        first.ownership.release().unwrap();
    }

    #[test]
    fn exact_current_owner_resume_is_not_qualified_before_reopen() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .unwrap();
        let route = first.route;
        let lease = first.lease.clone();
        assert!(registry
            .remove_candidate(&first.ownership.candidate)
            .unwrap());
        drop(first);

        let resume_services = Arc::new(TestRuntimeServices::default());
        let error = match super::bootstrap_root_owner(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_services(
                    database,
                    test_descriptor(),
                    Arc::clone(&resume_services),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Resume {
                    lease: lease.clone(),
                },
                install_request_id: request_id(5),
                activate_request_id: request_id(6),
                recovery: empty_recovery(),
            },
        ) {
            Ok(_) => panic!("exact resume must remain not qualified"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            ServerError::InvalidBootstrap(ref code)
                if code == &AdmissionCode::ExactResumeNotQualifiedV1.to_string()
        ));
        assert_eq!(
            resume_services.reopen_delegate_calls.load(Ordering::SeqCst),
            0
        );
        assert!(!registry.contains_exact(route).unwrap());
        let retained = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(retained.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(retained.lease_id, lease.lease_id);
        control.release_owner(&lease).unwrap();
    }

    #[test]
    fn exact_owner_resume_cannot_create_a_replacement_store() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database),
        )
        .unwrap();
        let lease = first.lease.clone();
        assert!(registry
            .remove_candidate(&first.ownership.candidate)
            .unwrap());

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime(temporary.path().join("replacement")),
                open_intent: OpenIntent::CreateFresh,
                admission: OwnerAdmission::Resume {
                    lease: lease.clone(),
                },
                install_request_id: request_id(35),
                activate_request_id: request_id(36),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("lifecycle transition"));
        assert!(!temporary.path().join("replacement").exists());
        assert_eq!(
            control
                .get_logical_shard(&shard())
                .unwrap()
                .unwrap()
                .owner_epoch,
            Some(lease.owner_epoch)
        );
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn stale_store_placement_is_rejected_and_never_installed() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .unwrap();
        let lease = first.lease.clone();
        assert!(registry
            .remove_candidate(&first.ownership.candidate)
            .unwrap());
        drop(first);

        let active = control.get_root_placement(&root()).unwrap().unwrap();
        let draining = control
            .compare_and_set_root_placement(
                &active,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(3).unwrap(),
                    lifecycle: RootPlacementLifecycle::Draining,
                    ..active
                },
            )
            .unwrap();
        control
            .compare_and_set_root_placement(
                &draining,
                RootPlacement {
                    placement_generation: PlacementGeneration::new(4).unwrap(),
                    lifecycle: RootPlacementLifecycle::Active,
                    ..draining
                },
            )
            .unwrap();

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime(database),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Resume { lease },
                install_request_id: request_id(7),
                activate_request_id: request_id(8),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();
        assert!(matches!(error, ServerError::InvalidBootstrap(_)));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn first_owner_reopen_is_rejected_before_acquiring_a_lease() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut request = acquire_request(temporary.path().join("missing"));
        request.runtime = file_runtime(temporary.path().join("missing"));
        request.open_intent = OpenIntent::ReopenExisting;

        assert!(characterize_bootstrap_after_admission(
            as_control(&control),
            registry.clone(),
            request,
        )
        .is_err());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert!(record.owner.is_none());
        assert_eq!(record.lease_id, 0);
        assert!(record.owner_epoch.is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn empty_frontier_successor_is_rejected_before_opening_any_store() {
        let temporary = TempDir::new().unwrap();
        let database = temporary.path().join("metadata");
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let first = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(database.clone()),
        )
        .unwrap();
        let first_epoch = first.lease.owner_epoch;
        first.ownership.release().unwrap();

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime_with_descriptor(
                    database,
                    crate::holt_runtime_descriptor().unwrap(),
                ),
                open_intent: OpenIntent::ReopenExisting,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    owner_incarnation_id: incarnation(2),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: Some(first_epoch),
                },
                install_request_id: request_id(33),
                activate_request_id: request_id(34),
                recovery: empty_recovery(),
            },
        )
        .err()
        .unwrap();

        assert!(error.to_string().contains("lifecycle admission"));
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, Some(first_epoch));
        assert_eq!(record.durable_lsn, 0);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn unverified_requested_recovery_frontier_is_rejected_before_acquire() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let mut request = acquire_request(temporary.path().join("metadata"));
        request.recovery.durable_lsn = 1;

        assert!(characterize_bootstrap_after_admission(
            as_control(&control),
            registry.clone(),
            request,
        )
        .is_err());
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert!(record.owner.is_none());
        assert_eq!(record.lease_id, 0);
        assert!(record.owner_epoch.is_none());
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn persisted_shared_frontier_is_rejected_before_successor_acquire() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let lease = control
            .acquire_owner(
                &serving_admission(&control),
                NodeId::new("node-a").unwrap(),
                incarnation(1),
                "127.0.0.1:9010".to_owned(),
            )
            .unwrap();
        let recovery = log_recovery(1);
        control
            .mark_serving(&lease, &serving_admission(&control), recovery.clone())
            .unwrap();
        control.release_owner(&lease).unwrap();

        let error = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            RootOwnerBootstrapRequest {
                root_id: root(),
                runtime: file_runtime(temporary.path().join("metadata")),
                open_intent: OpenIntent::ReconcilePreparedCreate,
                admission: OwnerAdmission::Acquire {
                    owner: NodeId::new("node-b").unwrap(),
                    owner_incarnation_id: incarnation(2),
                    endpoint: "127.0.0.1:9020".to_owned(),
                    expected_previous_epoch: Some(lease.owner_epoch),
                },
                install_request_id: request_id(31),
                activate_request_id: request_id(32),
                recovery,
            },
        )
        .err()
        .unwrap();

        assert!(error
            .to_string()
            .contains("unverified shared recovery frontier"));
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.state, LogicalShardState::Unassigned);
        assert_eq!(record.owner_epoch, Some(lease.owner_epoch));
        assert_eq!(record.durable_lsn, 1);
        assert_eq!(registry.installed_root_count().unwrap(), 0);
    }

    #[test]
    fn renewal_loss_uninstalls_the_exact_route() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();
        owner.ownership.renew_or_uninstall().unwrap();
        control.release_owner(&owner.lease).unwrap();

        assert!(owner.ownership.renew_or_uninstall().is_err());
        assert!(!registry.contains_exact(owner.route).unwrap());
    }

    #[test]
    fn release_pre_mutation_failure_retains_exact_tombstone_for_retry() {
        let temporary = TempDir::new().unwrap();
        let durable = active_control();
        let control: Arc<dyn ControlStore> = Arc::new(AcquireBarrierControl::release_faulting(
            Arc::clone(&durable),
            [ReleaseFault::FailBeforeMutation],
        ));
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            control,
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();

        assert!(matches!(
            owner.ownership.release(),
            Err(ServerError::OwnerReleaseRetryable { .. })
        ));
        assert!(!registry.contains_exact(owner.route).unwrap());
        let retained = durable.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(retained.owner.as_ref(), Some(&owner.lease.owner));
        assert_eq!(retained.lease_id, owner.lease.lease_id);

        let released = owner.ownership.release().unwrap();
        assert!(released.owner.is_none());
        assert_eq!(released.owner_epoch, Some(owner.lease.owner_epoch));
    }

    #[test]
    fn release_receipt_failure_permanently_converts_renew_into_release_only_retry() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let services = Arc::new(TestRuntimeServices::default());
        let mut request = acquire_request(temporary.path().join("metadata"));
        request.runtime = file_runtime_with_services(
            temporary.path().join("metadata"),
            test_descriptor(),
            Arc::clone(&services),
        );
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            request,
        )
        .unwrap();

        services
            .reject_release_receipt
            .store(true, Ordering::Release);
        assert!(matches!(
            owner.ownership.release(),
            Err(ServerError::OwnerReleaseReceipt(
                crate::OwnerReleaseReceiptError::PersistenceRejectedV1
            ))
        ));
        assert!(owner
            .ownership
            .candidate_token()
            .read_admission()
            .unwrap()
            .is_none());

        services
            .reject_release_receipt
            .store(false, Ordering::Release);
        let retry = owner.ownership.renew_or_uninstall().unwrap_err();
        assert!(retry
            .to_string()
            .contains("terminal and can only retry exact release"));
        let released = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert!(released.owner.is_none());
        assert_eq!(released.owner_epoch, Some(owner.lease.owner_epoch));
        assert_eq!(registry.installed_root_count().unwrap(), 0);
        assert_eq!(services.release_receipt_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn commit_before_release_response_reconciles_without_reopening_route() {
        let temporary = TempDir::new().unwrap();
        let durable = active_control();
        let control: Arc<dyn ControlStore> = Arc::new(AcquireBarrierControl::release_faulting(
            Arc::clone(&durable),
            [ReleaseFault::CommitBeforeUnknownResponse],
        ));
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            control,
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();

        assert!(matches!(
            owner.ownership.release(),
            Err(ServerError::OwnerReleasePending { .. })
        ));
        assert!(!registry.contains_exact(owner.route).unwrap());
        assert!(durable
            .get_logical_shard(&shard())
            .unwrap()
            .unwrap()
            .owner
            .is_none());

        let reconciled = owner.ownership.release().unwrap();
        assert!(reconciled.owner.is_none());
        assert_eq!(reconciled.owner_epoch, Some(owner.lease.owner_epoch));
    }

    #[test]
    fn runtime_failure_keeps_release_pending_until_exact_retry() {
        let temporary = TempDir::new().unwrap();
        let durable = active_control();
        let control: Arc<dyn ControlStore> = Arc::new(AcquireBarrierControl::release_faulting(
            Arc::clone(&durable),
            [ReleaseFault::OutcomeUnknownBeforeMutation],
        ));
        let registry = Arc::new(RootOwnerRegistry::new());
        let services = Arc::new(TestRuntimeServices::default());
        let mut request = acquire_request(temporary.path().join("metadata"));
        request.runtime = file_runtime_with_services(
            temporary.path().join("metadata"),
            test_descriptor(),
            Arc::clone(&services),
        );
        let owner = characterize_bootstrap_after_admission(control, Arc::clone(&registry), request)
            .unwrap();
        services.reject_provider.store(true, Ordering::Release);

        assert!(matches!(
            owner.ownership.renew_or_uninstall(),
            Err(ServerError::OwnerReleasePending { .. })
        ));
        assert!(services.poisoned.load(Ordering::Acquire));
        assert!(!registry.contains_exact(owner.route).unwrap());
        assert_eq!(
            durable
                .get_logical_shard(&shard())
                .unwrap()
                .unwrap()
                .lease_id,
            owner.lease.lease_id
        );

        owner.ownership.release().unwrap();
        assert!(durable
            .get_logical_shard(&shard())
            .unwrap()
            .unwrap()
            .owner
            .is_none());
    }

    #[test]
    fn provider_runtime_loss_poisoned_and_uninstalls_before_renewal() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let services = Arc::new(TestRuntimeServices::default());
        let mut request = acquire_request(temporary.path().join("metadata"));
        request.runtime = file_runtime_with_services(
            temporary.path().join("metadata"),
            test_descriptor(),
            Arc::clone(&services),
        );
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            request,
        )
        .unwrap();
        services.reject_provider.store(true, Ordering::Release);

        let error = owner.ownership.renew_or_uninstall().unwrap_err();

        assert!(matches!(error, ServerError::Metadata(_)));
        assert!(services.poisoned.load(Ordering::Acquire));
        assert!(!registry.contains_exact(owner.route).unwrap());
    }

    #[test]
    fn stale_ownership_cannot_release_a_reinstalled_exact_candidate() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();
        let stale = owner.ownership.clone();
        registry.terminate_candidate(&stale.candidate).unwrap();
        assert!(registry
            .remove_candidate(&owner.ownership.candidate)
            .unwrap());

        let installed_executor: Arc<dyn WorkspaceRequestExecutor> = owner.executor.clone();
        let current_poison_calls = Arc::new(AtomicUsize::new(0));
        let validator: Arc<dyn OwnerCandidateRuntimeValidator> =
            Arc::new(AlwaysValidCandidateRuntime {
                poison_calls: Arc::clone(&current_poison_calls),
            });
        let reservation = registry
            .reserve_logical_shard_bootstrap(owner.route.logical_shard_id)
            .unwrap();
        let current = registry
            .install_pending(&reservation, owner.route, installed_executor, validator)
            .unwrap();
        registry.activate(&current).unwrap();

        let _error = WorkspaceServer::new(
            ServerOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                lease_renew_interval: Duration::from_secs(1),
            },
            Arc::clone(&registry),
            vec![stale.clone()],
        )
        .err()
        .unwrap();
        assert!(matches!(stale.release(), Err(ServerError::InvalidRoute(_))));
        assert!(registry.contains_candidate(&current).unwrap());
        assert_eq!(current_poison_calls.load(Ordering::SeqCst), 0);
        let record = control.get_logical_shard(&shard()).unwrap().unwrap();
        assert_eq!(record.owner_epoch, Some(owner.lease.owner_epoch));
        assert_eq!(record.state, LogicalShardState::Serving);

        assert!(registry.remove_candidate(&current).unwrap());
        control.release_owner(&owner.lease).unwrap();
    }

    #[test]
    fn supervised_runtime_marks_owner_loss_before_further_accepts() {
        let temporary = TempDir::new().unwrap();
        let control = active_control();
        let registry = Arc::new(RootOwnerRegistry::new());
        let owner = characterize_bootstrap_after_admission(
            as_control(&control),
            Arc::clone(&registry),
            acquire_request(temporary.path().join("metadata")),
        )
        .unwrap();
        let route = owner.route;
        let lease = owner.lease.clone();
        let server = WorkspaceServer::new(
            ServerOptions {
                bind: "127.0.0.1:0".parse().unwrap(),
                read_timeout: Duration::from_secs(1),
                write_timeout: Duration::from_secs(1),
                lease_renew_interval: Duration::from_millis(10),
            },
            Arc::clone(&registry),
            vec![owner.ownership],
        )
        .unwrap();
        control.release_owner(&lease).unwrap();

        assert!(server.renew_ownership().is_err());
        assert!(server.owner_loss_signal().is_lost());
        assert!(!registry.contains_exact(route).unwrap());
    }
}
