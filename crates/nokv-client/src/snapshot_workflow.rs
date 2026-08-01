/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Focused leased-snapshot workflows.
//!
//! Name selection remains an alias on every read and mutation. Point reads are
//! used only to shape no-op and terminal outcomes; they are never converted
//! into numeric selectors for a later mutation.

use std::collections::BTreeSet;

use nokv_protocol::{
    ConflictKind, ErrorCode, GetSnapshotRequest, ListSnapshotsRequest, MintSnapshotRequest,
    PageRequest, RenewSnapshotRequest, RetireSnapshotRequest, SnapshotAlias, SnapshotPage,
    SnapshotResult, SnapshotSelector, SnapshotStatus, WorkbenchName, WorkspaceIdentity,
};

use crate::{ClientCall, ClientError, RouteResolver, RpcTransport, WorkspaceClient};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMintOptions {
    pub workbench: WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub lease_deadline_ms: u64,
    pub alias: Option<SnapshotAlias>,
    pub annotation: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRenewOptions {
    pub workbench: WorkbenchName,
    pub selector: SnapshotSelector,
    pub lease_deadline_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRetireOptions {
    pub workbench: WorkbenchName,
    pub selector: SnapshotSelector,
    pub retire_annotation: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRetireOutcome {
    pub snapshot: SnapshotResult,
    pub retired: bool,
    pub commit_version: Option<u64>,
    pub replayed: bool,
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    /// Mints a snapshot and retries numeric-id collisions with a fresh id and
    /// request identity. The alias and all other semantic inputs remain fixed.
    pub fn mint_snapshot_workflow(
        &self,
        options: SnapshotMintOptions,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        let io = ClientSnapshotIo { client: self };
        drive_mint_snapshot(&io, self.max_attempts(), options)
    }

    /// Extends or revives one snapshot selected by the caller's exact selector.
    pub fn renew_snapshot_workflow(
        &self,
        options: SnapshotRenewOptions,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        let io = ClientSnapshotIo { client: self };
        drive_renew_snapshot(&io, self.max_attempts(), options)
    }

    /// Retires one snapshot selected by the caller's exact selector.
    pub fn retire_snapshot_workflow(
        &self,
        options: SnapshotRetireOptions,
    ) -> Result<SnapshotRetireOutcome, ClientError> {
        let io = ClientSnapshotIo { client: self };
        drive_retire_snapshot(&io, self.max_attempts(), options)
    }

    /// Reads every snapshot page. Lifecycle workflows deliberately do not use
    /// this scan; it exists only for the explicit list surface.
    pub fn list_all_snapshots(
        &self,
        workbench: WorkbenchName,
    ) -> Result<Vec<SnapshotResult>, ClientError> {
        let io = ClientSnapshotIo { client: self };
        drive_list_all_snapshots(&io, workbench)
    }
}

trait SnapshotWorkflowIo {
    fn next_snapshot_id(&self) -> u64;

    fn get_snapshot(
        &self,
        request: GetSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError>;

    fn mint_snapshot(
        &self,
        request: MintSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError>;

    fn renew_snapshot(
        &self,
        request: RenewSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError>;

    fn retire_snapshot(
        &self,
        request: RetireSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError>;

    fn list_snapshots(
        &self,
        request: ListSnapshotsRequest,
    ) -> Result<ClientCall<SnapshotPage>, ClientError>;
}

struct ClientSnapshotIo<'a, Transport, Resolver> {
    client: &'a WorkspaceClient<Transport, Resolver>,
}

impl<Transport, Resolver> SnapshotWorkflowIo for ClientSnapshotIo<'_, Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    fn next_snapshot_id(&self) -> u64 {
        let identity = self.client.new_request_id();
        let mut snapshot_id = u64::from_be_bytes(
            identity.0[..8]
                .try_into()
                .expect("request identity prefix has fixed width"),
        );
        if snapshot_id == 0 {
            snapshot_id = 1;
        }
        snapshot_id
    }

    fn get_snapshot(
        &self,
        request: GetSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.client.get_snapshot(request)
    }

    fn mint_snapshot(
        &self,
        request: MintSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.client
            .mint_snapshot(self.client.new_request_id(), request)
    }

    fn renew_snapshot(
        &self,
        request: RenewSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.client
            .renew_snapshot(self.client.new_request_id(), request)
    }

    fn retire_snapshot(
        &self,
        request: RetireSnapshotRequest,
    ) -> Result<ClientCall<SnapshotResult>, ClientError> {
        self.client
            .retire_snapshot(self.client.new_request_id(), request)
    }

    fn list_snapshots(
        &self,
        request: ListSnapshotsRequest,
    ) -> Result<ClientCall<SnapshotPage>, ClientError> {
        self.client.list_snapshots(request)
    }
}

fn drive_list_all_snapshots(
    io: &impl SnapshotWorkflowIo,
    workbench: WorkbenchName,
) -> Result<Vec<SnapshotResult>, ClientError> {
    let mut cursor = None;
    let mut seen_cursors = BTreeSet::new();
    let mut snapshots = Vec::new();
    loop {
        let page = io
            .list_snapshots(ListSnapshotsRequest {
                workbench: workbench.clone(),
                page: PageRequest {
                    cursor,
                    limit: PageRequest::MAX_LIMIT,
                },
            })?
            .value;
        if page
            .snapshots
            .iter()
            .any(|snapshot| snapshot.workbench != workbench)
        {
            return Err(ClientError::ResponseMismatch(
                "snapshot page contains a different workbench".to_owned(),
            ));
        }
        snapshots.extend(page.snapshots);
        let Some(next_cursor) = page.next_cursor else {
            return Ok(snapshots);
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(ClientError::ResponseMismatch(
                "snapshot pagination repeated a cursor".to_owned(),
            ));
        }
        cursor = Some(next_cursor);
    }
}

fn drive_mint_snapshot(
    io: &impl SnapshotWorkflowIo,
    max_attempts: u32,
    options: SnapshotMintOptions,
) -> Result<ClientCall<SnapshotResult>, ClientError> {
    for attempt in 1..=max_attempts {
        let request = MintSnapshotRequest {
            workbench: options.workbench.clone(),
            workspace_incarnation_id: options.workspace_incarnation_id,
            snapshot_id: io.next_snapshot_id(),
            lease_deadline_ms: options.lease_deadline_ms,
            alias: options.alias.clone(),
            annotation: options.annotation.clone(),
        };
        match io.mint_snapshot(request.clone()) {
            Ok(call) => {
                validate_minted_snapshot(&call.value, &request)?;
                return Ok(call);
            }
            Err(error)
                if error.rpc_code() == Some(ErrorCode::AlreadyExists) && attempt < max_attempts => {
            }
            Err(error) if error.rpc_code() == Some(ErrorCode::AlreadyExists) => {
                return Err(ClientError::RetryExhausted {
                    attempts: attempt,
                    last_error: Box::new(error),
                });
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("validated client options always allow at least one attempt")
}

fn drive_renew_snapshot(
    io: &impl SnapshotWorkflowIo,
    max_attempts: u32,
    options: SnapshotRenewOptions,
) -> Result<ClientCall<SnapshotResult>, ClientError> {
    for attempt in 1..=max_attempts {
        let current = io.get_snapshot(GetSnapshotRequest {
            workbench: options.workbench.clone(),
            selector: options.selector.clone(),
        })?;
        validate_snapshot_selection(&current.value, &options.workbench, &options.selector)?;
        if current.value.status == SnapshotStatus::Alive
            && options.lease_deadline_ms <= current.value.lease_deadline_ms
        {
            return Ok(current);
        }

        let request = RenewSnapshotRequest {
            workbench: options.workbench.clone(),
            selector: options.selector.clone(),
            lease_deadline_ms: options.lease_deadline_ms,
        };
        let retry_races = matches!(
            current.value.status,
            SnapshotStatus::Alive | SnapshotStatus::Expired
        );
        let current_snapshot_id = current.value.snapshot_id;
        match io.renew_snapshot(request) {
            Ok(call) => {
                validate_snapshot_selection(&call.value, &options.workbench, &options.selector)?;
                if call.value.lease_deadline_ms != options.lease_deadline_ms {
                    return Err(ClientError::ResponseMismatch(
                        "renewed snapshot returned a different lease deadline".to_owned(),
                    ));
                }
                return Ok(call);
            }
            Err(error)
                if retry_races && snapshot_lifecycle_conflict(&error) && attempt < max_attempts => {
            }
            Err(error) if retry_races && snapshot_lifecycle_conflict(&error) => {
                return Err(ClientError::RetryExhausted {
                    attempts: attempt,
                    last_error: Box::new(error),
                });
            }
            Err(error) if retry_races && snapshot_lifecycle_precondition(&error) => {
                let refreshed = io.get_snapshot(GetSnapshotRequest {
                    workbench: options.workbench.clone(),
                    selector: options.selector.clone(),
                })?;
                validate_snapshot_selection(
                    &refreshed.value,
                    &options.workbench,
                    &options.selector,
                )?;
                if refreshed.value.status == SnapshotStatus::Alive
                    && options.lease_deadline_ms <= refreshed.value.lease_deadline_ms
                {
                    return Ok(refreshed);
                }
                let changed_actionable_alias =
                    matches!(&options.selector, SnapshotSelector::Alias(_))
                        && refreshed.value.snapshot_id != current_snapshot_id
                        && matches!(
                            refreshed.value.status,
                            SnapshotStatus::Alive | SnapshotStatus::Expired
                        );
                if changed_actionable_alias && attempt < max_attempts {
                    continue;
                }
                if changed_actionable_alias {
                    return Err(ClientError::RetryExhausted {
                        attempts: attempt,
                        last_error: Box::new(error),
                    });
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("validated client options always allow at least one attempt")
}

fn drive_retire_snapshot(
    io: &impl SnapshotWorkflowIo,
    max_attempts: u32,
    options: SnapshotRetireOptions,
) -> Result<SnapshotRetireOutcome, ClientError> {
    for attempt in 1..=max_attempts {
        let current = io.get_snapshot(GetSnapshotRequest {
            workbench: options.workbench.clone(),
            selector: options.selector.clone(),
        })?;
        validate_snapshot_selection(&current.value, &options.workbench, &options.selector)?;
        if matches!(
            current.value.status,
            SnapshotStatus::Retired | SnapshotStatus::Reaped
        ) {
            return Ok(SnapshotRetireOutcome {
                snapshot: current.value,
                retired: false,
                commit_version: current.commit_version,
                replayed: current.replayed,
            });
        }

        let request = RetireSnapshotRequest {
            workbench: options.workbench.clone(),
            selector: options.selector.clone(),
            retire_annotation: options.retire_annotation.clone(),
        };
        let retry_races = matches!(
            current.value.status,
            SnapshotStatus::Alive | SnapshotStatus::Expired
        );
        let current_snapshot_id = current.value.snapshot_id;
        match io.retire_snapshot(request.clone()) {
            Ok(call) => {
                validate_retired_snapshot(&call.value, &request)?;
                return Ok(SnapshotRetireOutcome {
                    snapshot: call.value,
                    retired: true,
                    commit_version: call.commit_version,
                    replayed: call.replayed,
                });
            }
            Err(error)
                if retry_races && snapshot_lifecycle_conflict(&error) && attempt < max_attempts => {
            }
            Err(error) if retry_races && snapshot_lifecycle_conflict(&error) => {
                return Err(ClientError::RetryExhausted {
                    attempts: attempt,
                    last_error: Box::new(error),
                });
            }
            Err(error) if retry_races && snapshot_lifecycle_precondition(&error) => {
                let refreshed = io.get_snapshot(GetSnapshotRequest {
                    workbench: options.workbench.clone(),
                    selector: options.selector.clone(),
                })?;
                validate_snapshot_selection(
                    &refreshed.value,
                    &options.workbench,
                    &options.selector,
                )?;
                if matches!(
                    refreshed.value.status,
                    SnapshotStatus::Retired | SnapshotStatus::Reaped
                ) {
                    return Ok(SnapshotRetireOutcome {
                        snapshot: refreshed.value,
                        retired: false,
                        commit_version: refreshed.commit_version,
                        replayed: refreshed.replayed,
                    });
                }
                let changed_actionable_alias =
                    matches!(&options.selector, SnapshotSelector::Alias(_))
                        && refreshed.value.snapshot_id != current_snapshot_id
                        && matches!(
                            refreshed.value.status,
                            SnapshotStatus::Alive | SnapshotStatus::Expired
                        );
                if changed_actionable_alias && attempt < max_attempts {
                    continue;
                }
                if changed_actionable_alias {
                    return Err(ClientError::RetryExhausted {
                        attempts: attempt,
                        last_error: Box::new(error),
                    });
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("validated client options always allow at least one attempt")
}

fn validate_minted_snapshot(
    snapshot: &SnapshotResult,
    request: &MintSnapshotRequest,
) -> Result<(), ClientError> {
    if snapshot.workbench != request.workbench
        || snapshot.workspace_incarnation_id != request.workspace_incarnation_id
        || snapshot.snapshot_id != request.snapshot_id
        || snapshot.lease_deadline_ms != request.lease_deadline_ms
        || snapshot.alias != request.alias
        || snapshot.annotation != request.annotation
        || !matches!(
            snapshot.status,
            SnapshotStatus::Alive | SnapshotStatus::Expired
        )
    {
        return Err(ClientError::ResponseMismatch(
            "minted snapshot does not match the exact request".to_owned(),
        ));
    }
    Ok(())
}

fn validate_retired_snapshot(
    snapshot: &SnapshotResult,
    request: &RetireSnapshotRequest,
) -> Result<(), ClientError> {
    validate_snapshot_selection(snapshot, &request.workbench, &request.selector)?;
    if snapshot.status != SnapshotStatus::Retired
        || snapshot.retire_annotation != request.retire_annotation
    {
        return Err(ClientError::ResponseMismatch(
            "retired snapshot does not match the exact request".to_owned(),
        ));
    }
    Ok(())
}

fn validate_snapshot_selection(
    snapshot: &SnapshotResult,
    workbench: &WorkbenchName,
    selector: &SnapshotSelector,
) -> Result<(), ClientError> {
    let selected = match selector {
        SnapshotSelector::Id(snapshot_id) => snapshot.snapshot_id == *snapshot_id,
        SnapshotSelector::Alias(alias) => snapshot.alias.as_ref() == Some(alias),
    };
    if snapshot.workbench != *workbench || !selected {
        return Err(ClientError::ResponseMismatch(
            "snapshot result does not match its workbench and selector".to_owned(),
        ));
    }
    Ok(())
}

fn snapshot_lifecycle_conflict(error: &ClientError) -> bool {
    error.rpc_failure().is_some_and(|failure| {
        failure.conflict == Some(ConflictKind::SnapshotLifecycle)
            && failure.code == ErrorCode::Conflict
    })
}

fn snapshot_lifecycle_precondition(error: &ClientError) -> bool {
    error.rpc_failure().is_some_and(|failure| {
        failure.conflict == Some(ConflictKind::SnapshotLifecycle)
            && failure.code == ErrorCode::PreconditionFailed
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;

    use nokv_protocol::RpcFailure;

    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Operation {
        Get(GetSnapshotRequest),
        Mint(MintSnapshotRequest),
        Renew(RenewSnapshotRequest),
        Retire(RetireSnapshotRequest),
        List(ListSnapshotsRequest),
    }

    #[derive(Default)]
    struct ScriptedIo {
        ids: RefCell<VecDeque<u64>>,
        gets: RefCell<VecDeque<Result<ClientCall<SnapshotResult>, ClientError>>>,
        mints: RefCell<VecDeque<Result<ClientCall<SnapshotResult>, ClientError>>>,
        renews: RefCell<VecDeque<Result<ClientCall<SnapshotResult>, ClientError>>>,
        retires: RefCell<VecDeque<Result<ClientCall<SnapshotResult>, ClientError>>>,
        lists: RefCell<VecDeque<Result<ClientCall<SnapshotPage>, ClientError>>>,
        operations: RefCell<Vec<Operation>>,
    }

    impl SnapshotWorkflowIo for ScriptedIo {
        fn next_snapshot_id(&self) -> u64 {
            self.ids
                .borrow_mut()
                .pop_front()
                .expect("script omitted a snapshot id")
        }

        fn get_snapshot(
            &self,
            request: GetSnapshotRequest,
        ) -> Result<ClientCall<SnapshotResult>, ClientError> {
            self.operations.borrow_mut().push(Operation::Get(request));
            self.gets
                .borrow_mut()
                .pop_front()
                .expect("script omitted a point result")
        }

        fn mint_snapshot(
            &self,
            request: MintSnapshotRequest,
        ) -> Result<ClientCall<SnapshotResult>, ClientError> {
            self.operations.borrow_mut().push(Operation::Mint(request));
            self.mints
                .borrow_mut()
                .pop_front()
                .expect("script omitted a mint result")
        }

        fn renew_snapshot(
            &self,
            request: RenewSnapshotRequest,
        ) -> Result<ClientCall<SnapshotResult>, ClientError> {
            self.operations.borrow_mut().push(Operation::Renew(request));
            self.renews
                .borrow_mut()
                .pop_front()
                .expect("script omitted a renew result")
        }

        fn retire_snapshot(
            &self,
            request: RetireSnapshotRequest,
        ) -> Result<ClientCall<SnapshotResult>, ClientError> {
            self.operations
                .borrow_mut()
                .push(Operation::Retire(request));
            self.retires
                .borrow_mut()
                .pop_front()
                .expect("script omitted a retire result")
        }

        fn list_snapshots(
            &self,
            request: ListSnapshotsRequest,
        ) -> Result<ClientCall<SnapshotPage>, ClientError> {
            self.operations.borrow_mut().push(Operation::List(request));
            self.lists
                .borrow_mut()
                .pop_front()
                .expect("script omitted a list result")
        }
    }

    fn workbench() -> WorkbenchName {
        WorkbenchName::new("run-42").unwrap()
    }

    fn alias() -> SnapshotAlias {
        SnapshotAlias::new("checkpoint").unwrap()
    }

    fn snapshot(
        snapshot_id: u64,
        status: SnapshotStatus,
        lease_deadline_ms: u64,
        retire_annotation: Option<Vec<u8>>,
    ) -> SnapshotResult {
        SnapshotResult {
            snapshot_id,
            workbench: workbench(),
            workspace_incarnation_id: WorkspaceIdentity([7; 16]),
            read_version: 11,
            lease_deadline_ms,
            alias: Some(alias()),
            annotation: b"annotation".to_vec(),
            retire_annotation,
            status,
            consumer_count: 0,
        }
    }

    fn call(snapshot: SnapshotResult) -> ClientCall<SnapshotResult> {
        ClientCall {
            value: snapshot,
            commit_version: None,
            replayed: false,
        }
    }

    fn alias_selector() -> SnapshotSelector {
        SnapshotSelector::Alias(alias())
    }

    fn lifecycle_conflict() -> ClientError {
        ClientError::Rpc(RpcFailure {
            code: ErrorCode::Conflict,
            message: "snapshot changed concurrently".to_owned(),
            retryable: true,
            conflict: Some(ConflictKind::SnapshotLifecycle),
            current_generation: None,
            route_hint: None,
        })
    }

    fn lifecycle_precondition() -> ClientError {
        ClientError::Rpc(RpcFailure {
            code: ErrorCode::PreconditionFailed,
            message: "snapshot precondition failed".to_owned(),
            retryable: false,
            conflict: Some(ConflictKind::SnapshotLifecycle),
            current_generation: None,
            route_hint: None,
        })
    }

    #[test]
    fn mint_retries_id_collision_without_changing_the_alias() {
        let io = ScriptedIo::default();
        io.ids.borrow_mut().extend([900, 1]);
        io.mints.borrow_mut().extend([
            Err(ClientError::Rpc(RpcFailure {
                code: ErrorCode::AlreadyExists,
                message: "snapshot id exists".to_owned(),
                retryable: false,
                conflict: Some(ConflictKind::SnapshotLifecycle),
                current_generation: None,
                route_hint: None,
            })),
            Ok(call(snapshot(1, SnapshotStatus::Alive, 200, None))),
        ]);

        let result = drive_mint_snapshot(
            &io,
            3,
            SnapshotMintOptions {
                workbench: workbench(),
                workspace_incarnation_id: WorkspaceIdentity([7; 16]),
                lease_deadline_ms: 200,
                alias: Some(alias()),
                annotation: b"annotation".to_vec(),
            },
        )
        .unwrap();

        assert_eq!(result.value.snapshot_id, 1);
        let operations = io.operations.borrow();
        assert_eq!(operations.len(), 2);
        assert!(operations.iter().all(|operation| matches!(
            operation,
            Operation::Mint(MintSnapshotRequest {
                alias: Some(value),
                ..
            }) if value == &alias()
        )));
    }

    #[test]
    fn expired_snapshot_is_revived_with_the_original_alias_selector() {
        let io = ScriptedIo::default();
        io.gets
            .borrow_mut()
            .push_back(Ok(call(snapshot(9, SnapshotStatus::Expired, 100, None))));
        io.renews
            .borrow_mut()
            .push_back(Ok(call(snapshot(9, SnapshotStatus::Alive, 300, None))));

        let result = drive_renew_snapshot(
            &io,
            3,
            SnapshotRenewOptions {
                workbench: workbench(),
                selector: alias_selector(),
                lease_deadline_ms: 300,
            },
        )
        .unwrap();

        assert_eq!(result.value.status, SnapshotStatus::Alive);
        assert_eq!(
            io.operations.borrow().as_slice(),
            [
                Operation::Get(GetSnapshotRequest {
                    workbench: workbench(),
                    selector: alias_selector(),
                }),
                Operation::Renew(RenewSnapshotRequest {
                    workbench: workbench(),
                    selector: alias_selector(),
                    lease_deadline_ms: 300,
                }),
            ]
        );
    }

    #[test]
    fn alive_non_extension_returns_the_point_result_without_a_mutation_or_scan() {
        let io = ScriptedIo::default();
        io.gets
            .borrow_mut()
            .push_back(Ok(call(snapshot(9, SnapshotStatus::Alive, 400, None))));

        let result = drive_renew_snapshot(
            &io,
            3,
            SnapshotRenewOptions {
                workbench: workbench(),
                selector: alias_selector(),
                lease_deadline_ms: 300,
            },
        )
        .unwrap();

        assert_eq!(result.value.lease_deadline_ms, 400);
        assert_eq!(
            io.operations.borrow().as_slice(),
            [Operation::Get(GetSnapshotRequest {
                workbench: workbench(),
                selector: alias_selector(),
            })]
        );
    }

    #[test]
    fn terminal_renew_states_preserve_the_server_typed_failure() {
        for (status, code) in [
            (SnapshotStatus::ReapClaimed, ErrorCode::SnapshotExpired),
            (SnapshotStatus::Retired, ErrorCode::PreconditionFailed),
            (SnapshotStatus::Reaped, ErrorCode::SnapshotReaped),
        ] {
            let io = ScriptedIo::default();
            io.gets
                .borrow_mut()
                .push_back(Ok(call(snapshot(9, status, 100, None))));
            io.renews
                .borrow_mut()
                .push_back(Err(ClientError::Rpc(RpcFailure {
                    code,
                    message: "snapshot is not active".to_owned(),
                    retryable: false,
                    conflict: Some(ConflictKind::SnapshotLifecycle),
                    current_generation: None,
                    route_hint: None,
                })));

            let error = drive_renew_snapshot(
                &io,
                3,
                SnapshotRenewOptions {
                    workbench: workbench(),
                    selector: SnapshotSelector::Id(9),
                    lease_deadline_ms: 300,
                },
            )
            .unwrap_err();

            assert_eq!(error.rpc_code(), Some(code));
            assert!(io
                .operations
                .borrow()
                .iter()
                .all(|operation| !matches!(operation, Operation::List(_))));
        }
    }

    #[test]
    fn deterministic_lifecycle_preconditions_are_not_wrapped_as_retry_exhaustion() {
        let renew_io = ScriptedIo::default();
        renew_io.gets.borrow_mut().extend([
            Ok(call(snapshot(9, SnapshotStatus::Expired, 100, None))),
            Ok(call(snapshot(9, SnapshotStatus::Expired, 100, None))),
        ]);
        renew_io
            .renews
            .borrow_mut()
            .push_back(Err(lifecycle_precondition()));
        let renew_error = drive_renew_snapshot(
            &renew_io,
            3,
            SnapshotRenewOptions {
                workbench: workbench(),
                selector: SnapshotSelector::Id(9),
                lease_deadline_ms: 300,
            },
        )
        .unwrap_err();
        assert!(matches!(renew_error, ClientError::Rpc(_)));
        assert_eq!(
            renew_io
                .operations
                .borrow()
                .iter()
                .filter(|operation| matches!(operation, Operation::Renew(_)))
                .count(),
            1
        );

        let retire_io = ScriptedIo::default();
        retire_io.gets.borrow_mut().extend([
            Ok(call(snapshot(9, SnapshotStatus::Alive, 300, None))),
            Ok(call(snapshot(9, SnapshotStatus::Alive, 300, None))),
        ]);
        retire_io
            .retires
            .borrow_mut()
            .push_back(Err(lifecycle_precondition()));
        let retire_error = drive_retire_snapshot(
            &retire_io,
            3,
            SnapshotRetireOptions {
                workbench: workbench(),
                selector: SnapshotSelector::Id(9),
                retire_annotation: None,
            },
        )
        .unwrap_err();
        assert!(matches!(retire_error, ClientError::Rpc(_)));
        assert_eq!(
            retire_io
                .operations
                .borrow()
                .iter()
                .filter(|operation| matches!(operation, Operation::Retire(_)))
                .count(),
            1
        );
    }

    #[test]
    fn precondition_repoint_converges_concurrent_renew_and_retire() {
        let renew_io = ScriptedIo::default();
        renew_io.gets.borrow_mut().extend([
            Ok(call(snapshot(9, SnapshotStatus::Alive, 100, None))),
            Ok(call(snapshot(9, SnapshotStatus::Alive, 400, None))),
        ]);
        renew_io
            .renews
            .borrow_mut()
            .push_back(Err(lifecycle_precondition()));
        let renewed = drive_renew_snapshot(
            &renew_io,
            3,
            SnapshotRenewOptions {
                workbench: workbench(),
                selector: alias_selector(),
                lease_deadline_ms: 300,
            },
        )
        .unwrap();
        assert_eq!(renewed.value.lease_deadline_ms, 400);

        let retire_io = ScriptedIo::default();
        let first_reason = br#"{"metadata":null,"reason":"first"}"#.to_vec();
        retire_io.gets.borrow_mut().extend([
            Ok(call(snapshot(9, SnapshotStatus::Alive, 300, None))),
            Ok(call(snapshot(
                9,
                SnapshotStatus::Retired,
                300,
                Some(first_reason.clone()),
            ))),
        ]);
        retire_io
            .retires
            .borrow_mut()
            .push_back(Err(lifecycle_precondition()));
        let retired = drive_retire_snapshot(
            &retire_io,
            3,
            SnapshotRetireOptions {
                workbench: workbench(),
                selector: alias_selector(),
                retire_annotation: Some(br#"{"metadata":null,"reason":"second"}"#.to_vec()),
            },
        )
        .unwrap();
        assert!(!retired.retired);
        assert_eq!(retired.snapshot.retire_annotation, Some(first_reason));
        assert!(renew_io
            .operations
            .borrow()
            .iter()
            .all(|operation| !matches!(operation, Operation::List(_))));
        assert!(retire_io
            .operations
            .borrow()
            .iter()
            .all(|operation| !matches!(operation, Operation::List(_))));
    }

    #[test]
    fn alias_remint_race_repoints_and_never_retires_the_old_numeric_id() {
        let io = ScriptedIo::default();
        io.gets.borrow_mut().extend([
            Ok(call(snapshot(900, SnapshotStatus::Alive, 300, None))),
            Ok(call(snapshot(1, SnapshotStatus::Alive, 300, None))),
        ]);
        let reason = br#"{"metadata":null,"reason":"done"}"#.to_vec();
        io.retires.borrow_mut().extend([
            Err(lifecycle_conflict()),
            Ok(call(snapshot(
                1,
                SnapshotStatus::Retired,
                300,
                Some(reason.clone()),
            ))),
        ]);

        let result = drive_retire_snapshot(
            &io,
            3,
            SnapshotRetireOptions {
                workbench: workbench(),
                selector: alias_selector(),
                retire_annotation: Some(reason),
            },
        )
        .unwrap();

        assert!(result.retired);
        assert_eq!(result.snapshot.snapshot_id, 1);
        let operations = io.operations.borrow();
        assert_eq!(operations.len(), 4);
        assert!(operations.iter().all(|operation| match operation {
            Operation::Get(request) => request.selector == alias_selector(),
            Operation::Retire(request) => request.selector == alias_selector(),
            Operation::Mint(_) | Operation::Renew(_) | Operation::List(_) => false,
        }));
    }

    #[test]
    fn repeated_retire_returns_false_and_preserves_the_first_reason_without_a_scan() {
        let io = ScriptedIo::default();
        let first_reason = br#"{"metadata":null,"reason":"first"}"#.to_vec();
        let second_reason = br#"{"metadata":null,"reason":"second"}"#.to_vec();
        io.gets.borrow_mut().extend([
            Ok(call(snapshot(9, SnapshotStatus::Alive, 300, None))),
            Ok(call(snapshot(
                9,
                SnapshotStatus::Retired,
                300,
                Some(first_reason.clone()),
            ))),
        ]);
        io.retires.borrow_mut().push_back(Ok(call(snapshot(
            9,
            SnapshotStatus::Retired,
            300,
            Some(first_reason.clone()),
        ))));

        let first = drive_retire_snapshot(
            &io,
            3,
            SnapshotRetireOptions {
                workbench: workbench(),
                selector: alias_selector(),
                retire_annotation: Some(first_reason.clone()),
            },
        )
        .unwrap();
        let second = drive_retire_snapshot(
            &io,
            3,
            SnapshotRetireOptions {
                workbench: workbench(),
                selector: alias_selector(),
                retire_annotation: Some(second_reason),
            },
        )
        .unwrap();

        assert!(first.retired);
        assert!(!second.retired);
        assert_eq!(second.snapshot.retire_annotation, Some(first_reason));
        assert_eq!(
            io.operations
                .borrow()
                .iter()
                .filter(|operation| matches!(operation, Operation::Retire(_)))
                .count(),
            1
        );
        assert!(io
            .operations
            .borrow()
            .iter()
            .all(|operation| !matches!(operation, Operation::List(_))));
    }

    #[test]
    fn explicit_list_collects_every_page_with_the_server_limit() {
        let io = ScriptedIo::default();
        io.lists.borrow_mut().extend([
            Ok(ClientCall {
                value: SnapshotPage {
                    snapshots: vec![snapshot(9, SnapshotStatus::Alive, 300, None)],
                    next_cursor: Some(vec![1, 2, 3]),
                },
                commit_version: None,
                replayed: false,
            }),
            Ok(ClientCall {
                value: SnapshotPage {
                    snapshots: vec![snapshot(10, SnapshotStatus::Alive, 300, None)],
                    next_cursor: None,
                },
                commit_version: None,
                replayed: false,
            }),
        ]);

        let snapshots = drive_list_all_snapshots(&io, workbench()).unwrap();

        assert_eq!(
            snapshots
                .into_iter()
                .map(|snapshot| snapshot.snapshot_id)
                .collect::<Vec<_>>(),
            vec![9, 10]
        );
        assert_eq!(
            io.operations.borrow().as_slice(),
            [
                Operation::List(ListSnapshotsRequest {
                    workbench: workbench(),
                    page: PageRequest {
                        cursor: None,
                        limit: PageRequest::MAX_LIMIT,
                    },
                }),
                Operation::List(ListSnapshotsRequest {
                    workbench: workbench(),
                    page: PageRequest {
                        cursor: Some(vec![1, 2, 3]),
                        limit: PageRequest::MAX_LIMIT,
                    },
                }),
            ]
        );
    }
}
