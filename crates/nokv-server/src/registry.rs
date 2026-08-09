/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, RwLock};

use nokv_control::{LogicalShardRecord, OwnerReleaseOutcome};
use nokv_protocol::{
    ConflictKind, ErrorCode, LogicalShardIdentity, RootIdentity, RootRoute, RpcFailure,
    WorkspaceRpcOutcome, WorkspaceRpcRequest, WorkspaceRpcResponse,
};

use crate::{ExecutedRequest, ServerError, WorkspaceRequestExecutor};

struct RootOwner {
    route: RootRoute,
    executor: Arc<dyn WorkspaceRequestExecutor>,
    candidate: Option<Arc<OwnerCandidateMarker>>,
    serving: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OwnerCandidatePhase {
    Pending,
    Serving,
    Closed,
}

struct OwnerAdmissionState {
    phase: OwnerCandidatePhase,
    readers: usize,
    writer_active: bool,
    waiting_writers: usize,
    terminal: bool,
}

struct OwnerAdmissionGate {
    state: Mutex<OwnerAdmissionState>,
    changed: Condvar,
    #[cfg(test)]
    writer_waiting_hook: Mutex<Option<std::sync::mpsc::Sender<()>>>,
}

pub(crate) trait OwnerCandidateRuntimeValidator: Send + Sync {
    fn validate(&self) -> bool;
    fn poison(&self);
    fn persist_release_receipt(&self) -> Result<(), ServerError> {
        Ok(())
    }
}

struct OwnerCandidateMarker {
    admission: OwnerAdmissionGate,
    runtime: Arc<dyn OwnerCandidateRuntimeValidator>,
    release: OwnerReleaseStateMachine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerReleaseState {
    Active,
    ReleasePending,
    Released(LogicalShardRecord),
    Superseded(LogicalShardRecord),
}

struct OwnerReleaseStateMachine {
    state: Mutex<OwnerReleaseState>,
}

impl OwnerReleaseStateMachine {
    fn active() -> Self {
        Self {
            state: Mutex::new(OwnerReleaseState::Active),
        }
    }

    fn begin(&self) -> Result<OwnerReleaseState, ServerError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ServerError::InvalidRoute("owner release state is poisoned".to_owned()))?;
        if *state == OwnerReleaseState::Active {
            *state = OwnerReleaseState::ReleasePending;
        }
        Ok(state.clone())
    }

    fn snapshot(&self) -> Result<OwnerReleaseState, ServerError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| ServerError::InvalidRoute("owner release state is poisoned".to_owned()))
    }

    fn finish(&self, outcome: OwnerReleaseOutcome) -> Result<OwnerReleaseState, ServerError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ServerError::InvalidRoute("owner release state is poisoned".to_owned()))?;
        if matches!(*state, OwnerReleaseState::Active) {
            return Err(ServerError::InvalidRoute(
                "owner release completed before local admission closed".to_owned(),
            ));
        }
        if matches!(*state, OwnerReleaseState::ReleasePending) {
            match outcome {
                OwnerReleaseOutcome::Released(record)
                | OwnerReleaseOutcome::AlreadyReleased(record) => {
                    *state = OwnerReleaseState::Released(record);
                }
                OwnerReleaseOutcome::Superseded(record) => {
                    *state = OwnerReleaseState::Superseded(record);
                }
                OwnerReleaseOutcome::OutcomeUnknown => {}
            }
        }
        Ok(state.clone())
    }
}

pub(crate) struct OwnerAdmissionReadPermit {
    marker: Arc<OwnerCandidateMarker>,
}

pub(crate) struct OwnerAdmissionWritePermit<'a> {
    gate: &'a OwnerAdmissionGate,
}

/// One fully formed RPC response that still owns the exact candidate reader.
///
/// Socket encoding and publication must finish before this wrapper is dropped;
/// otherwise release or renewal could cross the response boundary after the
/// metadata effect but before the caller can observe it.
pub(crate) struct AdmittedResponse<T> {
    response: T,
    _admission: Option<OwnerAdmissionReadPermit>,
}

impl<T> AdmittedResponse<T> {
    fn unguarded(response: T) -> Self {
        Self {
            response,
            _admission: None,
        }
    }

    fn guarded(response: T, admission: Option<OwnerAdmissionReadPermit>) -> Self {
        Self {
            response,
            _admission: admission,
        }
    }

    pub(crate) const fn response(&self) -> &T {
        &self.response
    }

    #[cfg(test)]
    fn into_response(self) -> T {
        self.response
    }
}

impl OwnerAdmissionGate {
    fn pending() -> Self {
        Self {
            state: Mutex::new(OwnerAdmissionState {
                phase: OwnerCandidatePhase::Pending,
                readers: 0,
                writer_active: false,
                waiting_writers: 0,
                terminal: false,
            }),
            changed: Condvar::new(),
            #[cfg(test)]
            writer_waiting_hook: Mutex::new(None),
        }
    }

    fn begin_read(&self) -> Result<bool, ServerError> {
        let mut state = self.state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
        })?;
        if state.terminal || state.phase != OwnerCandidatePhase::Serving {
            return Ok(false);
        }
        while state.writer_active || state.waiting_writers != 0 {
            state = self.changed.wait(state).map_err(|_| {
                ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
            })?;
            if state.terminal || state.phase != OwnerCandidatePhase::Serving {
                return Ok(false);
            }
        }
        state.readers = state.readers.checked_add(1).ok_or_else(|| {
            ServerError::InvalidRoute("owner admission reader count overflowed".to_owned())
        })?;
        Ok(true)
    }

    fn write(&self) -> Result<OwnerAdmissionWritePermit<'_>, ServerError> {
        let mut state = self.state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
        })?;
        state.waiting_writers = state.waiting_writers.checked_add(1).ok_or_else(|| {
            ServerError::InvalidRoute("owner admission writer count overflowed".to_owned())
        })?;
        #[cfg(test)]
        if let Ok(mut hook) = self.writer_waiting_hook.lock() {
            if let Some(hook) = hook.take() {
                let _ = hook.send(());
            }
        }
        while state.writer_active || state.readers != 0 {
            state = self.changed.wait(state).map_err(|_| {
                ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
            })?;
        }
        state.waiting_writers -= 1;
        state.writer_active = true;
        Ok(OwnerAdmissionWritePermit { gate: self })
    }

    fn flag_terminal(&self) -> Result<(), ServerError> {
        let mut state = self.state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
        })?;
        state.terminal = true;
        self.changed.notify_all();
        Ok(())
    }
}

impl Drop for OwnerAdmissionReadPermit {
    fn drop(&mut self) {
        let Ok(mut state) = self.marker.admission.state.lock() else {
            return;
        };
        state.readers = state.readers.saturating_sub(1);
        self.marker.admission.changed.notify_all();
    }
}

impl OwnerAdmissionWritePermit<'_> {
    fn phase(&self) -> Result<OwnerCandidatePhase, ServerError> {
        self.gate
            .state
            .lock()
            .map(|state| state.phase)
            .map_err(|_| ServerError::InvalidRoute("owner admission gate is poisoned".to_owned()))
    }

    pub(crate) fn is_terminal(&self) -> Result<bool, ServerError> {
        self.gate
            .state
            .lock()
            .map(|state| state.terminal)
            .map_err(|_| ServerError::InvalidRoute("owner admission gate is poisoned".to_owned()))
    }

    fn activate(&mut self) -> Result<(), ServerError> {
        let mut state = self.gate.state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
        })?;
        if state.terminal || state.phase != OwnerCandidatePhase::Pending {
            return Err(ServerError::InvalidRoute(
                "owner candidate is not pending activation".to_owned(),
            ));
        }
        state.phase = OwnerCandidatePhase::Serving;
        Ok(())
    }

    fn close(&mut self) -> Result<(), ServerError> {
        let mut state = self.gate.state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner admission gate is poisoned".to_owned())
        })?;
        state.terminal = true;
        state.phase = OwnerCandidatePhase::Closed;
        Ok(())
    }
}

impl Drop for OwnerAdmissionWritePermit<'_> {
    fn drop(&mut self) {
        let Ok(mut state) = self.gate.state.lock() else {
            return;
        };
        state.writer_active = false;
        self.gate.changed.notify_all();
    }
}

/// Unforgeable identity for one not-yet-serving registry candidate.
///
/// The route alone is insufficient because two exact-resume attempts may carry
/// the same control-plane lease. Activation and rollback must therefore prove
/// that they still refer to the exact executor candidate they installed.
#[derive(Clone)]
pub(crate) struct OwnerCandidateToken {
    route: RootRoute,
    marker: Arc<OwnerCandidateMarker>,
}

struct ShardBootstrapMarker;

#[derive(Default)]
struct RegistryBootstrapState {
    reservations: BTreeMap<LogicalShardIdentity, Arc<ShardBootstrapMarker>>,
    sealed_for_server: bool,
}

pub(crate) struct ShardBootstrapReservation {
    logical_shard_id: LogicalShardIdentity,
    marker: Arc<ShardBootstrapMarker>,
    bootstrap_state: Arc<Mutex<RegistryBootstrapState>>,
}

impl Drop for ShardBootstrapReservation {
    fn drop(&mut self) {
        let Ok(mut state) = self.bootstrap_state.lock() else {
            return;
        };
        let matches = state
            .reservations
            .get(&self.logical_shard_id)
            .is_some_and(|marker| Arc::ptr_eq(marker, &self.marker));
        if matches {
            state.reservations.remove(&self.logical_shard_id);
        }
    }
}

impl ShardBootstrapReservation {
    pub(crate) fn is_current(&self) -> Result<bool, ServerError> {
        self.bootstrap_state
            .lock()
            .map(|state| {
                state
                    .reservations
                    .get(&self.logical_shard_id)
                    .is_some_and(|marker| Arc::ptr_eq(marker, &self.marker))
            })
            .map_err(|_| {
                ServerError::InvalidRoute("owner bootstrap reservation lock is poisoned".to_owned())
            })
    }
}

impl OwnerCandidateToken {
    pub(crate) fn read_admission(&self) -> Result<Option<OwnerAdmissionReadPermit>, ServerError> {
        if !self.marker.admission.begin_read()? {
            return Ok(None);
        }
        Ok(Some(OwnerAdmissionReadPermit {
            marker: Arc::clone(&self.marker),
        }))
    }

    pub(crate) fn write_admission(&self) -> Result<OwnerAdmissionWritePermit<'_>, ServerError> {
        self.marker.admission.write()
    }

    #[cfg(test)]
    fn notify_when_writer_waits(&self, hook: std::sync::mpsc::Sender<()>) {
        *self.marker.admission.writer_waiting_hook.lock().unwrap() = Some(hook);
    }

    pub(crate) fn flag_terminal(&self) -> Result<(), ServerError> {
        self.marker.admission.flag_terminal()
    }

    pub(crate) fn poison_runtime(&self) {
        self.marker.runtime.poison();
    }

    pub(crate) fn persist_release_receipt(&self) -> Result<(), ServerError> {
        self.marker.runtime.persist_release_receipt()
    }

    pub(crate) fn runtime_is_valid(&self) -> bool {
        self.marker.runtime.validate()
    }

    pub(crate) fn begin_release(&self) -> Result<OwnerReleaseState, ServerError> {
        self.marker.release.begin()
    }

    pub(crate) fn release_state(&self) -> Result<OwnerReleaseState, ServerError> {
        self.marker.release.snapshot()
    }

    pub(crate) fn finish_release(
        &self,
        outcome: OwnerReleaseOutcome,
    ) -> Result<OwnerReleaseState, ServerError> {
        self.marker.release.finish(outcome)
    }
}

#[derive(Default)]
pub struct RootOwnerRegistry {
    owners: RwLock<BTreeMap<RootIdentity, RootOwner>>,
    bootstrap_state: Arc<Mutex<RegistryBootstrapState>>,
}

impl RootOwnerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reserve_logical_shard_bootstrap(
        &self,
        logical_shard_id: LogicalShardIdentity,
    ) -> Result<ShardBootstrapReservation, ServerError> {
        let mut state = self.bootstrap_state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner bootstrap reservation lock is poisoned".to_owned())
        })?;
        if state.sealed_for_server {
            return Err(ServerError::RouteRollback(
                "the owner registry is already sealed to its serving server".to_owned(),
            ));
        }
        if state.reservations.contains_key(&logical_shard_id) {
            return Err(ServerError::RouteRollback(
                "this logical shard already has a local bootstrap in progress".to_owned(),
            ));
        }
        let owners = self
            .owners
            .read()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        if owners
            .values()
            .any(|owner| owner.route.logical_shard_id == logical_shard_id)
        {
            return Err(ServerError::RouteRollback(
                "this logical shard already has a local owner candidate".to_owned(),
            ));
        }
        let marker = Arc::new(ShardBootstrapMarker);
        state
            .reservations
            .insert(logical_shard_id, Arc::clone(&marker));
        Ok(ShardBootstrapReservation {
            logical_shard_id,
            marker,
            bootstrap_state: Arc::clone(&self.bootstrap_state),
        })
    }

    /// Installs or advances one current owner. Placement generation and owner
    /// epoch can never move backwards, and an exact route cannot replace the
    /// executor already bound to that owner identity.
    #[cfg(test)]
    pub(crate) fn install(
        &self,
        route: RootRoute,
        executor: Arc<dyn WorkspaceRequestExecutor>,
    ) -> Result<(), ServerError> {
        self.install_with_state(route, executor, None)
    }

    /// Install an exact owner route that cannot dispatch until bootstrap has
    /// atomically published the same owner as Serving in the control plane.
    pub(crate) fn install_pending(
        &self,
        reservation: &ShardBootstrapReservation,
        route: RootRoute,
        executor: Arc<dyn WorkspaceRequestExecutor>,
        runtime: Arc<dyn OwnerCandidateRuntimeValidator>,
    ) -> Result<OwnerCandidateToken, ServerError> {
        if reservation.logical_shard_id != route.logical_shard_id {
            return Err(ServerError::InvalidRoute(
                "owner candidate route differs from its shard bootstrap reservation".to_owned(),
            ));
        }
        let mut state = self.bootstrap_state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner bootstrap reservation lock is poisoned".to_owned())
        })?;
        if state.sealed_for_server {
            return Err(ServerError::RouteRollback(
                "the owner registry is already sealed to its serving server".to_owned(),
            ));
        }
        if !state
            .reservations
            .get(&route.logical_shard_id)
            .is_some_and(|marker| Arc::ptr_eq(marker, &reservation.marker))
        {
            return Err(ServerError::InvalidRoute(
                "owner shard bootstrap reservation is no longer current".to_owned(),
            ));
        }
        let marker = Arc::new(OwnerCandidateMarker {
            admission: OwnerAdmissionGate::pending(),
            runtime,
            release: OwnerReleaseStateMachine::active(),
        });
        self.install_with_state(route, executor, Some(Arc::clone(&marker)))?;
        state.reservations.remove(&route.logical_shard_id);
        Ok(OwnerCandidateToken { route, marker })
    }

    /// Bind this registry exactly once to the complete owner set supervised by
    /// one server. No bootstrap may begin after the set is sealed.
    pub(crate) fn seal_for_server(
        &self,
        candidates: &[OwnerCandidateToken],
    ) -> Result<(), ServerError> {
        if candidates.is_empty() {
            return Err(ServerError::InvalidOptions(
                "serving requires at least one owner candidate".to_owned(),
            ));
        }
        let mut state = self.bootstrap_state.lock().map_err(|_| {
            ServerError::InvalidRoute("owner bootstrap reservation lock is poisoned".to_owned())
        })?;
        if state.sealed_for_server {
            return Err(ServerError::InvalidOptions(
                "the owner registry is already bound to a serving server".to_owned(),
            ));
        }
        if !state.reservations.is_empty() {
            return Err(ServerError::InvalidOptions(
                "cannot bind a serving server while a shard bootstrap is in progress".to_owned(),
            ));
        }

        let mut admissions = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(admission) = candidate.read_admission()? else {
                return Err(ServerError::InvalidOptions(
                    "server ownership includes a non-Serving owner candidate".to_owned(),
                ));
            };
            if !candidate.runtime_is_valid() {
                return Err(ServerError::InvalidOptions(
                    "server ownership includes an invalid runtime binding".to_owned(),
                ));
            }
            admissions.push(admission);
        }

        let owners = self
            .owners
            .read()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let exact_owner = |candidate: &OwnerCandidateToken| {
            owners.get(&candidate.route.root_id).is_some_and(|owner| {
                owner.route == candidate.route
                    && owner.serving
                    && owner
                        .candidate
                        .as_ref()
                        .is_some_and(|marker| Arc::ptr_eq(marker, &candidate.marker))
            })
        };
        let exact_registry_entry = |owner: &RootOwner| {
            owner.serving
                && owner.candidate.as_ref().is_some_and(|marker| {
                    candidates.iter().any(|candidate| {
                        candidate.route == owner.route && Arc::ptr_eq(marker, &candidate.marker)
                    })
                })
        };
        if owners.len() != candidates.len()
            || !candidates.iter().all(exact_owner)
            || !owners.values().all(exact_registry_entry)
        {
            return Err(ServerError::InvalidOptions(
                "server ownership must exactly cover every installed owner candidate".to_owned(),
            ));
        }

        state.sealed_for_server = true;
        drop(owners);
        drop(admissions);
        Ok(())
    }

    fn install_with_state(
        &self,
        route: RootRoute,
        executor: Arc<dyn WorkspaceRequestExecutor>,
        candidate: Option<Arc<OwnerCandidateMarker>>,
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
            if route == current.route {
                return Err(ServerError::RouteRollback(
                    "the exact root-owner route already has a local candidate".to_owned(),
                ));
            }
            if current.candidate.is_some() || candidate.is_some() {
                return Err(ServerError::RouteRollback(
                    "control-backed owner candidates cannot be replaced without their exact token"
                        .to_owned(),
                ));
            }
        }
        if candidate.is_some()
            && owners.values().any(|owner| {
                owner.candidate.is_some() && owner.route.logical_shard_id == route.logical_shard_id
            })
        {
            return Err(ServerError::RouteRollback(
                "this owner session already has a local root candidate".to_owned(),
            ));
        }
        owners.insert(
            route.root_id,
            RootOwner {
                route,
                executor,
                serving: candidate.is_none(),
                candidate,
            },
        );
        Ok(())
    }

    /// Make one exact pending route dispatchable after control-plane Serving
    /// publication succeeds. A newer/replaced route is never activated.
    pub(crate) fn activate(&self, token: &OwnerCandidateToken) -> Result<(), ServerError> {
        token
            .route
            .validate()
            .map_err(|error| ServerError::InvalidRoute(error.to_string()))?;
        let mut admission = token.write_admission()?;
        if !token.marker.runtime.validate() {
            token.marker.runtime.poison();
            admission.close()?;
            return Err(ServerError::InvalidRoute(
                "owner runtime validation failed before candidate activation".to_owned(),
            ));
        }
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let owner = owners.get_mut(&token.route.root_id).ok_or_else(|| {
            ServerError::InvalidRoute("cannot activate a missing pending owner route".to_owned())
        })?;
        if owner.route != token.route
            || owner.serving
            || !owner
                .candidate
                .as_ref()
                .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker))
        {
            return Err(ServerError::InvalidRoute(
                "pending owner candidate changed before activation".to_owned(),
            ));
        }
        admission.activate()?;
        owner.serving = true;
        Ok(())
    }

    /// Remove only the exact pending candidate installed by one bootstrap.
    /// A concurrent replacement with the same route is left untouched.
    pub(crate) fn remove_pending(&self, token: &OwnerCandidateToken) -> Result<bool, ServerError> {
        let mut admission = token.write_admission()?;
        token.flag_terminal()?;
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let matches = owners.get(&token.route.root_id).is_some_and(|owner| {
            owner.route == token.route
                && !owner.serving
                && owner
                    .candidate
                    .as_ref()
                    .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker))
        });
        if matches {
            admission.close()?;
            owners.remove(&token.route.root_id);
        }
        Ok(matches)
    }

    /// Permanently close an exact non-serving bootstrap candidate while
    /// retaining its token-bound registry entry as a release-only tombstone.
    pub(crate) fn close_pending_for_release_with_admission(
        &self,
        token: &OwnerCandidateToken,
        admission: &mut OwnerAdmissionWritePermit<'_>,
    ) -> Result<bool, ServerError> {
        if admission.phase()? != OwnerCandidatePhase::Pending {
            return Ok(false);
        }
        let owners = self
            .owners
            .read()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let matches = owners.get(&token.route.root_id).is_some_and(|owner| {
            owner.route == token.route
                && !owner.serving
                && owner
                    .candidate
                    .as_ref()
                    .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker))
        });
        drop(owners);
        if matches {
            if let Err(error) = token.persist_release_receipt() {
                token.flag_terminal()?;
                return Err(error);
            }
            token.flag_terminal()?;
            token.begin_release()?;
            token.poison_runtime();
            admission.close()?;
        }
        Ok(matches)
    }

    /// Remove only the exact candidate installed by one bootstrap, whether it
    /// is still pending or has already been activated as Serving. An older
    /// ownership handle can never remove a later exact-route installation.
    #[cfg(test)]
    pub(crate) fn remove_candidate(
        &self,
        token: &OwnerCandidateToken,
    ) -> Result<bool, ServerError> {
        token.flag_terminal()?;
        let mut admission = token.write_admission()?;
        self.remove_candidate_with_admission(token, &mut admission)
    }

    pub(crate) fn remove_candidate_with_admission(
        &self,
        token: &OwnerCandidateToken,
        admission: &mut OwnerAdmissionWritePermit<'_>,
    ) -> Result<bool, ServerError> {
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let matches = owners.get(&token.route.root_id).is_some_and(|owner| {
            owner.route == token.route
                && owner
                    .candidate
                    .as_ref()
                    .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker))
        });
        if matches {
            admission.close()?;
            owners.remove(&token.route.root_id);
        }
        Ok(matches)
    }

    /// Return whether this exact candidate is still the active Serving entry.
    pub(crate) fn contains_candidate(
        &self,
        token: &OwnerCandidateToken,
    ) -> Result<bool, ServerError> {
        let Some(_admission) = token.read_admission()? else {
            return Ok(false);
        };
        self.contains_candidate_entry(token)
    }

    pub(crate) fn contains_candidate_with_admission(
        &self,
        token: &OwnerCandidateToken,
        admission: &OwnerAdmissionWritePermit<'_>,
    ) -> Result<bool, ServerError> {
        if admission.phase()? != OwnerCandidatePhase::Serving {
            return Ok(false);
        }
        self.contains_candidate_entry(token)
    }

    pub(crate) fn contains_release_tombstone_with_admission(
        &self,
        token: &OwnerCandidateToken,
        admission: &OwnerAdmissionWritePermit<'_>,
    ) -> Result<bool, ServerError> {
        if admission.phase()? != OwnerCandidatePhase::Closed {
            return Ok(false);
        }
        self.owners
            .read()
            .map(|owners| {
                owners.get(&token.route.root_id).is_some_and(|owner| {
                    owner.route == token.route
                        && !owner.serving
                        && owner
                            .candidate
                            .as_ref()
                            .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker))
                })
            })
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
    }

    fn contains_candidate_entry(&self, token: &OwnerCandidateToken) -> Result<bool, ServerError> {
        self.owners
            .read()
            .map(|owners| {
                owners.get(&token.route.root_id).is_some_and(|owner| {
                    owner.route == token.route
                        && owner.serving
                        && owner
                            .candidate
                            .as_ref()
                            .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker))
                })
            })
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
    }

    pub(crate) fn deactivate_candidate_with_admission(
        &self,
        token: &OwnerCandidateToken,
        admission: &mut OwnerAdmissionWritePermit<'_>,
    ) -> Result<bool, ServerError> {
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let Some(owner) = owners.get_mut(&token.route.root_id) else {
            return Ok(false);
        };
        let matches = owner.route == token.route
            && owner.serving
            && owner
                .candidate
                .as_ref()
                .is_some_and(|marker| Arc::ptr_eq(marker, &token.marker));
        if matches {
            owner.serving = false;
            admission.close()?;
        }
        Ok(matches)
    }

    pub(crate) fn terminate_candidate(
        &self,
        token: &OwnerCandidateToken,
    ) -> Result<(), ServerError> {
        let mut admission = token.write_admission()?;
        if !self.contains_candidate_with_admission(token, &admission)? {
            return Ok(());
        }
        if let Err(error) = token.persist_release_receipt() {
            token.flag_terminal()?;
            return Err(error);
        }
        token.flag_terminal()?;
        token.begin_release()?;
        token.marker.runtime.poison();
        let _ = self.deactivate_candidate_with_admission(token, &mut admission)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn remove(&self, route: RootRoute) -> Result<bool, ServerError> {
        route
            .validate()
            .map_err(|error| ServerError::InvalidRoute(error.to_string()))?;
        let mut owners = self
            .owners
            .write()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let matches = owners
            .get(&route.root_id)
            .is_some_and(|owner| owner.route == route && owner.candidate.is_none());
        if matches {
            owners.remove(&route.root_id);
        }
        Ok(matches)
    }

    pub(crate) fn installed_root_count(&self) -> Result<usize, ServerError> {
        self.owners
            .read()
            .map(|owners| owners.values().filter(|owner| owner.serving).count())
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
    }

    fn is_sealed_for_server(&self) -> Result<bool, ServerError> {
        self.bootstrap_state
            .lock()
            .map(|state| state.sealed_for_server)
            .map_err(|_| {
                ServerError::InvalidRoute("owner bootstrap reservation lock is poisoned".to_owned())
            })
    }

    #[cfg(test)]
    fn seal_direct_entries_for_test(&self) {
        let mut state = self.bootstrap_state.lock().unwrap();
        assert!(state.reservations.is_empty());
        assert!(!state.sealed_for_server);
        state.sealed_for_server = true;
    }

    pub(crate) fn contains_exact(&self, route: RootRoute) -> Result<bool, ServerError> {
        route
            .validate()
            .map_err(|error| ServerError::InvalidRoute(error.to_string()))?;
        self.owners
            .read()
            .map(|owners| {
                owners
                    .get(&route.root_id)
                    .is_some_and(|owner| owner.route == route && owner.serving)
            })
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))
    }

    #[cfg(test)]
    pub(crate) fn dispatch(
        &self,
        request: WorkspaceRpcRequest,
    ) -> Result<WorkspaceRpcResponse, ServerError> {
        Ok(self.dispatch_admitted(request)?.into_response())
    }

    pub(crate) fn dispatch_admitted(
        &self,
        request: WorkspaceRpcRequest,
    ) -> Result<AdmittedResponse<WorkspaceRpcResponse>, ServerError> {
        if !self.is_sealed_for_server()? {
            return Ok(AdmittedResponse::unguarded(not_owner_response(
                request,
                None,
                "the owner registry is not bound to a serving server",
            )));
        }
        let owners = self
            .owners
            .read()
            .map_err(|_| ServerError::InvalidRoute("owner registry lock is poisoned".to_owned()))?;
        let Some(owner) = owners.get(&request.route.root_id) else {
            return Ok(AdmittedResponse::unguarded(not_owner_response(
                request,
                None,
                "this server does not own the requested root",
            )));
        };
        if owner.route != request.route {
            return Ok(AdmittedResponse::unguarded(not_owner_response(
                request,
                Some(owner.route),
                "the requested root route is stale",
            )));
        }
        if !owner.serving {
            return Ok(AdmittedResponse::unguarded(not_owner_response(
                request,
                Some(owner.route),
                "the requested root owner is not Serving",
            )));
        }
        let route = owner.route;
        let executor = Arc::clone(&owner.executor);
        let candidate = owner.candidate.as_ref().map(|marker| OwnerCandidateToken {
            route,
            marker: Arc::clone(marker),
        });
        let request_id = request.request_id;
        drop(owners);
        let admission = match candidate.as_ref() {
            None => None,
            Some(candidate) => match candidate.read_admission()? {
                Some(admission) => Some(admission),
                None => {
                    return Ok(AdmittedResponse::unguarded(not_owner_response(
                        request,
                        Some(route),
                        "the exact owner candidate is no longer admitting work",
                    )));
                }
            },
        };
        if let Some(candidate) = candidate.as_ref() {
            if !candidate.marker.runtime.validate() {
                drop(admission);
                self.terminate_candidate(candidate)?;
                return Ok(AdmittedResponse::unguarded(not_owner_response(
                    request,
                    Some(route),
                    "metadata runtime validation failed before request execution",
                )));
            }
        }
        let outcome = executor.execute(&request);
        if let Some(candidate) = candidate.as_ref() {
            if !candidate.marker.runtime.validate() {
                drop(admission);
                self.terminate_candidate(candidate)?;
                return Ok(AdmittedResponse::unguarded(not_owner_response(
                    request,
                    Some(route),
                    "metadata runtime validation failed after request execution",
                )));
            }
        }
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
        Ok(AdmittedResponse::guarded(response, admission))
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use nokv_protocol::{
        CreateWorkspaceRequest, LogicalShardIdentity, RequestIdentity, WorkbenchName,
        WorkspaceIdentity, WorkspaceRequest, WorkspaceResult, WorkspaceSummary,
    };

    use super::*;

    struct EchoExecutor;

    struct AlwaysValidRuntime;

    impl OwnerCandidateRuntimeValidator for AlwaysValidRuntime {
        fn validate(&self) -> bool {
            true
        }

        fn poison(&self) {}
    }

    struct ReleaseRecordingRuntime {
        receipt_calls: Arc<AtomicUsize>,
        poison_calls: Arc<AtomicUsize>,
    }

    impl OwnerCandidateRuntimeValidator for ReleaseRecordingRuntime {
        fn validate(&self) -> bool {
            true
        }

        fn poison(&self) {
            self.poison_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn persist_release_receipt(&self) -> Result<(), ServerError> {
            self.receipt_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct BlockingExecutor {
        entered: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        calls: Arc<AtomicUsize>,
    }

    struct SwitchableRuntime {
        valid: Arc<AtomicBool>,
        poison_calls: Arc<AtomicUsize>,
    }

    impl OwnerCandidateRuntimeValidator for SwitchableRuntime {
        fn validate(&self) -> bool {
            self.valid.load(Ordering::Acquire)
        }

        fn poison(&self) {
            self.poison_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct InvalidatingExecutor {
        valid: Arc<AtomicBool>,
    }

    impl WorkspaceRequestExecutor for InvalidatingExecutor {
        fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            let result = EchoExecutor.execute(request);
            self.valid.store(false, Ordering::Release);
            result
        }
    }

    impl WorkspaceRequestExecutor for BlockingExecutor {
        fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
            EchoExecutor.execute(request)
        }
    }

    fn install_pending(
        registry: &RootOwnerRegistry,
        route: RootRoute,
    ) -> Result<OwnerCandidateToken, ServerError> {
        let reservation = registry.reserve_logical_shard_bootstrap(route.logical_shard_id)?;
        registry.install_pending(
            &reservation,
            route,
            Arc::new(EchoExecutor),
            Arc::new(AlwaysValidRuntime),
        )
    }

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
        route_for(1, 2, owner_epoch)
    }

    fn route_for(root: u8, shard: u8, owner_epoch: u64) -> RootRoute {
        RootRoute {
            root_id: RootIdentity([root; 16]),
            logical_shard_id: LogicalShardIdentity([shard; 16]),
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
        registry.seal_direct_entries_for_test();
        let response = registry.dispatch(request(route(7))).unwrap();
        assert_eq!(response.route, route(7));
        assert_eq!(response.commit_version, Some(9));
        assert!(matches!(response.outcome, WorkspaceRpcOutcome::Success(_)));
    }

    #[test]
    fn pending_route_never_dispatches_before_exact_serving_activation() {
        let registry = RootOwnerRegistry::new();
        let pending = route(7);
        let pending_owner = install_pending(&registry, pending).unwrap();

        assert_eq!(registry.installed_root_count().unwrap(), 0);
        assert!(!registry.contains_exact(pending).unwrap());
        let response = registry.dispatch(request(pending)).unwrap();
        assert!(matches!(
            response.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                retryable: true,
                ..
            })
        ));

        registry.activate(&pending_owner).unwrap();
        assert_eq!(registry.installed_root_count().unwrap(), 1);
        assert!(registry.contains_exact(pending).unwrap());
        let before_seal = registry.dispatch(request(pending)).unwrap();
        assert!(matches!(
            before_seal.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        registry
            .seal_for_server(std::slice::from_ref(&pending_owner))
            .unwrap();
        let response = registry.dispatch(request(pending)).unwrap();
        assert!(matches!(response.outcome, WorkspaceRpcOutcome::Success(_)));
    }

    #[test]
    fn concurrent_exact_pending_candidate_cannot_replace_the_first() {
        let registry = RootOwnerRegistry::new();
        let pending = route(7);
        let first = install_pending(&registry, pending).unwrap();
        assert!(matches!(
            install_pending(&registry, pending),
            Err(ServerError::RouteRollback(_))
        ));

        assert_eq!(registry.installed_root_count().unwrap(), 0);
        assert!(matches!(
            registry.dispatch(request(pending)).unwrap().outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                retryable: true,
                ..
            })
        ));

        registry.activate(&first).unwrap();
        assert!(registry.contains_exact(pending).unwrap());
    }

    #[test]
    fn shard_bootstrap_reservation_is_exclusive_and_released_on_drop() {
        let registry = RootOwnerRegistry::new();
        let logical_shard_id = route(7).logical_shard_id;
        let first = registry
            .reserve_logical_shard_bootstrap(logical_shard_id)
            .unwrap();
        assert!(matches!(
            registry.reserve_logical_shard_bootstrap(logical_shard_id),
            Err(ServerError::RouteRollback(_))
        ));
        drop(first);
        assert!(registry
            .reserve_logical_shard_bootstrap(logical_shard_id)
            .is_ok());
    }

    #[test]
    fn server_seal_requires_exact_owner_coverage_and_blocks_late_bootstrap() {
        let registry = RootOwnerRegistry::new();
        let first_route = route_for(1, 2, 7);
        let second_route = route_for(3, 4, 9);
        let first = install_pending(&registry, first_route).unwrap();
        registry.activate(&first).unwrap();
        let second = install_pending(&registry, second_route).unwrap();
        registry.activate(&second).unwrap();

        assert!(matches!(
            registry.seal_for_server(std::slice::from_ref(&first)),
            Err(ServerError::InvalidOptions(_))
        ));
        registry
            .seal_for_server(&[first.clone(), second.clone()])
            .unwrap();

        assert!(matches!(
            registry.reserve_logical_shard_bootstrap(LogicalShardIdentity([5; 16])),
            Err(ServerError::RouteRollback(_))
        ));
        assert!(matches!(
            registry.seal_for_server(&[first.clone(), second.clone()]),
            Err(ServerError::InvalidOptions(_))
        ));
        assert!(registry.contains_candidate(&first).unwrap());
        assert!(registry.contains_candidate(&second).unwrap());
        assert!(registry.remove_candidate(&first).unwrap());
        assert!(registry.remove_candidate(&second).unwrap());
    }

    #[test]
    fn stale_candidate_token_cannot_remove_a_reinstalled_exact_route() {
        let registry = RootOwnerRegistry::new();
        let exact = route(7);
        let first = install_pending(&registry, exact).unwrap();
        registry.activate(&first).unwrap();
        assert!(registry.remove_candidate(&first).unwrap());

        let second = install_pending(&registry, exact).unwrap();
        registry.activate(&second).unwrap();

        assert!(!registry.remove_candidate(&first).unwrap());
        assert!(registry.contains_exact(exact).unwrap());
        assert!(registry.remove_candidate(&second).unwrap());
    }

    #[test]
    fn stale_candidate_termination_cannot_poison_a_reinstalled_runtime() {
        let registry = RootOwnerRegistry::new();
        let exact = route(7);
        let valid = Arc::new(AtomicBool::new(true));
        let poison_calls = Arc::new(AtomicUsize::new(0));
        let runtime: Arc<dyn OwnerCandidateRuntimeValidator> = Arc::new(SwitchableRuntime {
            valid,
            poison_calls: Arc::clone(&poison_calls),
        });
        let first_reservation = registry
            .reserve_logical_shard_bootstrap(exact.logical_shard_id)
            .unwrap();
        let first = registry
            .install_pending(
                &first_reservation,
                exact,
                Arc::new(EchoExecutor),
                Arc::clone(&runtime),
            )
            .unwrap();
        registry.activate(&first).unwrap();
        assert!(registry.remove_candidate(&first).unwrap());

        let second_reservation = registry
            .reserve_logical_shard_bootstrap(exact.logical_shard_id)
            .unwrap();
        let second = registry
            .install_pending(&second_reservation, exact, Arc::new(EchoExecutor), runtime)
            .unwrap();
        registry.activate(&second).unwrap();

        registry.terminate_candidate(&first).unwrap();
        assert_eq!(poison_calls.load(Ordering::SeqCst), 0);
        assert!(registry.contains_candidate(&second).unwrap());
        assert!(registry.remove_candidate(&second).unwrap());
    }

    #[test]
    fn terminal_candidate_waits_for_inflight_execution_and_rejects_new_admission() {
        let registry = Arc::new(RootOwnerRegistry::new());
        let exact = route(7);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let receipt_calls = Arc::new(AtomicUsize::new(0));
        let poison_calls = Arc::new(AtomicUsize::new(0));
        let reservation = registry
            .reserve_logical_shard_bootstrap(exact.logical_shard_id)
            .unwrap();
        let token = registry
            .install_pending(
                &reservation,
                exact,
                Arc::new(BlockingExecutor {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                    calls: Arc::clone(&calls),
                }),
                Arc::new(ReleaseRecordingRuntime {
                    receipt_calls: Arc::clone(&receipt_calls),
                    poison_calls: Arc::clone(&poison_calls),
                }),
            )
            .unwrap();
        registry.activate(&token).unwrap();
        registry
            .seal_for_server(std::slice::from_ref(&token))
            .unwrap();

        let first_registry = Arc::clone(&registry);
        let first = thread::spawn(move || first_registry.dispatch(request(exact)).unwrap());
        entered_rx.recv().unwrap();

        let (closed_tx, closed_rx) = mpsc::channel();
        let (writer_waiting_tx, writer_waiting_rx) = mpsc::channel();
        token.notify_when_writer_waits(writer_waiting_tx);
        let closing_registry = Arc::clone(&registry);
        let closing_token = token.clone();
        let closing = thread::spawn(move || {
            let result = closing_registry.terminate_candidate(&closing_token);
            closed_tx.send(result).unwrap();
        });
        writer_waiting_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(!token.marker.admission.state.lock().unwrap().terminal);

        let rejected_registry = Arc::clone(&registry);
        let rejected = thread::spawn(move || rejected_registry.dispatch(request(exact)).unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(receipt_calls.load(Ordering::SeqCst), 0);
        assert_eq!(poison_calls.load(Ordering::SeqCst), 0);
        assert!(closed_rx.try_recv().is_err());

        release_tx.send(()).unwrap();
        assert!(matches!(
            first.join().unwrap().outcome,
            WorkspaceRpcOutcome::Success(_)
        ));
        closed_rx.recv().unwrap().unwrap();
        closing.join().unwrap();
        assert!(matches!(
            rejected.join().unwrap().outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        assert_eq!(receipt_calls.load(Ordering::SeqCst), 1);
        assert_eq!(poison_calls.load(Ordering::SeqCst), 1);
        assert!(!registry.contains_exact(exact).unwrap());
    }

    #[test]
    fn admitted_response_holds_candidate_reader_until_response_publication_finishes() {
        let registry = Arc::new(RootOwnerRegistry::new());
        let exact = route(7);
        let token = install_pending(&registry, exact).unwrap();
        registry.activate(&token).unwrap();
        registry
            .seal_for_server(std::slice::from_ref(&token))
            .unwrap();

        let response = registry.dispatch_admitted(request(exact)).unwrap();
        assert!(matches!(
            &response.response().outcome,
            WorkspaceRpcOutcome::Success(_)
        ));

        let (writer_waiting_tx, writer_waiting_rx) = mpsc::channel();
        token.notify_when_writer_waits(writer_waiting_tx);
        let (closed_tx, closed_rx) = mpsc::channel();
        let closing_registry = Arc::clone(&registry);
        let closing_token = token.clone();
        let closing = thread::spawn(move || {
            closed_tx
                .send(closing_registry.terminate_candidate(&closing_token))
                .unwrap();
        });
        writer_waiting_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
        assert!(closed_rx.try_recv().is_err());

        drop(response);
        closed_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        closing.join().unwrap();
        assert!(!registry.contains_exact(exact).unwrap());
    }

    #[test]
    fn post_execution_runtime_failure_terminally_removes_the_exact_candidate() {
        let registry = RootOwnerRegistry::new();
        let exact = route(7);
        let valid = Arc::new(AtomicBool::new(true));
        let poison_calls = Arc::new(AtomicUsize::new(0));
        let reservation = registry
            .reserve_logical_shard_bootstrap(exact.logical_shard_id)
            .unwrap();
        let token = registry
            .install_pending(
                &reservation,
                exact,
                Arc::new(InvalidatingExecutor {
                    valid: Arc::clone(&valid),
                }),
                Arc::new(SwitchableRuntime {
                    valid,
                    poison_calls: Arc::clone(&poison_calls),
                }),
            )
            .unwrap();
        registry.activate(&token).unwrap();
        registry
            .seal_for_server(std::slice::from_ref(&token))
            .unwrap();

        let response = registry.dispatch(request(exact)).unwrap();

        assert!(matches!(
            response.outcome,
            WorkspaceRpcOutcome::Failure(RpcFailure {
                code: ErrorCode::NotOwner,
                ..
            })
        ));
        assert_eq!(poison_calls.load(Ordering::SeqCst), 1);
        assert!(!registry.contains_exact(exact).unwrap());
    }

    #[test]
    fn preflight_dispatches_only_through_the_exact_installed_route() {
        let registry = RootOwnerRegistry::new();
        registry.install(route(8), Arc::new(EchoExecutor)).unwrap();
        registry.seal_direct_entries_for_test();

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
        registry.seal_direct_entries_for_test();
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
            registry.install(route(8), Arc::new(EchoExecutor)),
            Err(ServerError::RouteRollback(_))
        ));
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
