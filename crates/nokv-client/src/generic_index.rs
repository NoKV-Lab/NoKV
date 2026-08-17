/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Recoverable Generic custom-index registration workflow.
//!
//! The SDK canonicalizes the complete plan before `Begin`, replays every
//! historical append page to prove its input commitment, and keeps each
//! metadata command inside the protocol's bounded row limit.

use nokv_protocol::{
    AbortGenericIndexRegistrationRequest, AppendGenericIndexRowsRequest,
    BeginGenericIndexRegistrationRequest, FinalizeGenericIndexRegistrationRequest,
    GenericIndexAbortResult, GenericIndexAppendReceipt, GenericIndexAppendResult,
    GenericIndexFieldCapability, GenericIndexGenerationIdentity, GenericIndexRegistrationPhase,
    GenericIndexRegistrationStatus, GenericIndexRow, GetGenericIndexRegistrationRequest,
    OperationIdentity, WorkspaceCapability, WorkspaceIdentity, WorkspaceRequest, WorkspaceResult,
    MAX_GENERIC_INDEX_ABORT_ROWS, MAX_GENERIC_INDEX_APPEND_ROWS,
};

use crate::{ClientCall, ClientError, RouteResolver, RpcTransport, WorkspaceClient};

/// Complete caller-owned identity and content for one logical registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexRegistrationPlan {
    pub operation_id: OperationIdentity,
    pub generation_id: GenericIndexGenerationIdentity,
    pub workbench: nokv_protocol::WorkbenchName,
    pub workspace_incarnation_id: WorkspaceIdentity,
    pub index_path: Option<nokv_protocol::RelativePath>,
    pub expected_current_generation: Option<u64>,
    pub capabilities: Vec<GenericIndexFieldCapability>,
    pub rows: Vec<GenericIndexRow>,
}

/// Terminal registration plus every immutable append receipt that the SDK
/// replayed or installed while proving the caller's complete row commitment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexRegistrationOutcome {
    pub finalization: ClientCall<GenericIndexRegistrationStatus>,
    pub append_receipts: Vec<GenericIndexAppendReceipt>,
}

/// Terminal result of a bounded, multi-command abort sweep.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GenericIndexAbortOutcome {
    pub final_batch: ClientCall<GenericIndexAbortResult>,
    pub batch_count: u64,
}

impl<Transport, Resolver> WorkspaceClient<Transport, Resolver>
where
    Transport: RpcTransport,
    Resolver: RouteResolver,
{
    pub fn begin_generic_index_registration(
        &self,
        request_id: nokv_protocol::RequestIdentity,
        request: BeginGenericIndexRegistrationRequest,
    ) -> Result<ClientCall<GenericIndexRegistrationStatus>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::BeginGenericIndexRegistration(request),
        )?
        .map(expect_generic_index_registration)
    }

    pub fn append_generic_index_rows(
        &self,
        request_id: nokv_protocol::RequestIdentity,
        request: AppendGenericIndexRowsRequest,
    ) -> Result<ClientCall<GenericIndexAppendResult>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::AppendGenericIndexRows(request),
        )?
        .map(expect_generic_index_append)
    }

    pub fn finalize_generic_index_registration(
        &self,
        request_id: nokv_protocol::RequestIdentity,
        request: FinalizeGenericIndexRegistrationRequest,
    ) -> Result<ClientCall<GenericIndexRegistrationStatus>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::FinalizeGenericIndexRegistration(request),
        )?
        .map(expect_generic_index_registration)
    }

    pub fn abort_generic_index_registration(
        &self,
        request_id: nokv_protocol::RequestIdentity,
        request: AbortGenericIndexRegistrationRequest,
    ) -> Result<ClientCall<GenericIndexAbortResult>, ClientError> {
        self.execute(
            request_id,
            WorkspaceRequest::AbortGenericIndexRegistration(request),
        )?
        .map(expect_generic_index_abort)
    }

    pub fn get_generic_index_registration(
        &self,
        request: GetGenericIndexRegistrationRequest,
    ) -> Result<ClientCall<GenericIndexRegistrationStatus>, ClientError> {
        self.execute_read(WorkspaceRequest::GetGenericIndexRegistration(request))?
            .map(expect_generic_index_registration)
    }

    /// Register one canonical plan. Re-running this method with the same
    /// identities and semantic inputs proves every prior page through its
    /// durable receipt before it can publish the pointer.
    pub fn register_generic_index(
        &self,
        plan: GenericIndexRegistrationPlan,
    ) -> Result<GenericIndexRegistrationOutcome, ClientError> {
        let plan = canonicalize_plan(plan)?;
        self.preflight([WorkspaceCapability::GenericCustomIndexV1])?;

        let begin_request = BeginGenericIndexRegistrationRequest {
            operation_id: plan.operation_id,
            generation_id: plan.generation_id,
            workbench: plan.workbench.clone(),
            workspace_incarnation_id: plan.workspace_incarnation_id,
            index_path: plan.index_path.clone(),
            expected_current_generation: plan.expected_current_generation,
            capabilities: plan.capabilities.clone(),
            declared_row_count: u64::try_from(plan.rows.len()).map_err(|_| {
                ClientError::InvalidOptions("Generic index row count exceeds u64".to_owned())
            })?,
        };
        begin_request.validate()?;
        let begin_request_id = self.new_request_id();
        let begin = retry_exact(self.max_attempts(), || {
            self.begin_generic_index_registration(begin_request_id, begin_request.clone())
        })?;
        validate_mutation_commit_version(
            begin.commit_version,
            begin.value.last_transition_version,
            "begin",
        )?;
        let commitment = RegistrationCommitment::new(&plan, &begin.value)?;
        let mut replayed = begin.replayed;
        let mut append_receipts =
            Vec::with_capacity(plan.rows.len().div_ceil(MAX_GENERIC_INDEX_APPEND_ROWS));
        let mut final_row_digest = begin.value.row_digest;

        for (batch, rows) in plan.rows.chunks(MAX_GENERIC_INDEX_APPEND_ROWS).enumerate() {
            let first_sequence = u64::try_from(batch)
                .ok()
                .and_then(|batch| batch.checked_mul(MAX_GENERIC_INDEX_APPEND_ROWS as u64))
                .ok_or_else(|| {
                    ClientError::InvalidOptions(
                        "Generic index append sequence overflows u64".to_owned(),
                    )
                })?;
            let append_request = AppendGenericIndexRowsRequest {
                operation_id: plan.operation_id,
                first_sequence,
                rows: rows.to_vec(),
            };
            append_request.validate()?;
            let request_id = self.new_request_id();
            let append = retry_exact(self.max_attempts(), || {
                self.append_generic_index_rows(request_id, append_request.clone())
            })?;
            commitment.validate_status(&append.value.registration)?;
            validate_append_result(&append, first_sequence, rows.len())?;
            replayed |= append.replayed;
            final_row_digest = append.value.receipt.resulting_row_digest;
            append_receipts.push(append.value.receipt);
        }

        let finalize_request = FinalizeGenericIndexRegistrationRequest {
            operation_id: plan.operation_id,
        };
        finalize_request.validate()?;
        let finalize_request_id = self.new_request_id();
        let mut finalization = retry_exact(self.max_attempts(), || {
            self.finalize_generic_index_registration(finalize_request_id, finalize_request.clone())
        })?;
        commitment.validate_status(&finalization.value)?;
        validate_mutation_commit_version(
            finalization.commit_version,
            finalization.value.last_transition_version,
            "finalize",
        )?;
        if finalization.value.phase != GenericIndexRegistrationPhase::Complete
            || finalization.value.appended_row_count != plan.rows.len() as u64
            || finalization.value.row_digest != final_row_digest
        {
            return Err(ClientError::ResponseMismatch(
                "Generic index finalization does not close the canonical row plan".to_owned(),
            ));
        }
        replayed |= finalization.replayed;
        finalization.replayed = replayed;
        Ok(GenericIndexRegistrationOutcome {
            finalization,
            append_receipts,
        })
    }

    /// Drive abort cleanup to its durable terminal phase using bounded rows
    /// per command and exact request replay for every lost response.
    pub fn abort_generic_index_registration_workflow(
        &self,
        operation_id: OperationIdentity,
    ) -> Result<GenericIndexAbortOutcome, ClientError> {
        let status = self
            .get_generic_index_registration(GetGenericIndexRegistrationRequest { operation_id })?
            .value;
        let maximum_batches = status
            .appended_row_count
            .div_ceil(u64::from(MAX_GENERIC_INDEX_ABORT_ROWS))
            .checked_add(2)
            .ok_or_else(|| {
                ClientError::ResponseMismatch(
                    "Generic index abort batch bound overflows u64".to_owned(),
                )
            })?;
        let mut previous_remaining = status.appended_row_count;
        for batch in 1..=maximum_batches {
            let request = AbortGenericIndexRegistrationRequest {
                operation_id,
                limit: MAX_GENERIC_INDEX_ABORT_ROWS,
            };
            request.validate()?;
            let request_id = self.new_request_id();
            let call = retry_exact(self.max_attempts(), || {
                self.abort_generic_index_registration(request_id, request.clone())
            })?;
            validate_mutation_commit_version(
                call.commit_version,
                call.value.registration.last_transition_version,
                "abort",
            )?;
            if call.value.cleanup_complete {
                return Ok(GenericIndexAbortOutcome {
                    final_batch: call,
                    batch_count: batch,
                });
            }
            let removed = u64::from(call.value.removed_rows);
            if removed == 0 || removed > previous_remaining {
                return Err(ClientError::ResponseMismatch(
                    "Generic index abort did not make bounded durable progress".to_owned(),
                ));
            }
            previous_remaining -= removed;
        }
        Err(ClientError::ResponseMismatch(
            "Generic index abort exceeded its declared cleanup bound".to_owned(),
        ))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RegistrationCommitment {
    operation_id: OperationIdentity,
    generation_id: GenericIndexGenerationIdentity,
    workspace_incarnation_id: WorkspaceIdentity,
    index_path: Option<nokv_protocol::RelativePath>,
    source_read_version: u64,
    expected_current_generation: Option<u64>,
    capability_digest: nokv_protocol::Digest,
    declared_row_count: u64,
}

impl RegistrationCommitment {
    fn new(
        plan: &GenericIndexRegistrationPlan,
        status: &GenericIndexRegistrationStatus,
    ) -> Result<Self, ClientError> {
        let commitment = Self {
            operation_id: plan.operation_id,
            generation_id: plan.generation_id,
            workspace_incarnation_id: plan.workspace_incarnation_id,
            index_path: plan.index_path.clone(),
            source_read_version: status.source_read_version,
            expected_current_generation: plan.expected_current_generation,
            capability_digest: status.capability_digest,
            declared_row_count: plan.rows.len() as u64,
        };
        commitment.validate_status(status)?;
        Ok(commitment)
    }

    fn validate_status(&self, status: &GenericIndexRegistrationStatus) -> Result<(), ClientError> {
        if status.operation_id != self.operation_id
            || status.generation_id != self.generation_id
            || status.workspace_incarnation_id != self.workspace_incarnation_id
            || status.index_path != self.index_path
            || status.source_read_version != self.source_read_version
            || status.expected_current_generation != self.expected_current_generation
            || status.capability_digest != self.capability_digest
            || status.declared_row_count != self.declared_row_count
        {
            return Err(ClientError::ResponseMismatch(
                "Generic index response changed its canonical registration commitment".to_owned(),
            ));
        }
        Ok(())
    }
}

fn canonicalize_plan(
    mut plan: GenericIndexRegistrationPlan,
) -> Result<GenericIndexRegistrationPlan, ClientError> {
    for capability in &mut plan.capabilities {
        capability.operators.sort_unstable();
        capability.operators.dedup();
    }
    plan.capabilities
        .sort_unstable_by(|left, right| left.field_id.cmp(&right.field_id));
    if plan
        .capabilities
        .windows(2)
        .any(|pair| pair[0].field_id == pair[1].field_id)
    {
        return Err(ClientError::InvalidOptions(
            "Generic index capabilities contain a duplicate field".to_owned(),
        ));
    }
    for capability in &plan.capabilities {
        capability.validate()?;
    }

    for row in &mut plan.rows {
        row.values
            .sort_unstable_by(|left, right| left.field_id.cmp(&right.field_id));
        if row
            .values
            .windows(2)
            .any(|pair| pair[0].field_id == pair[1].field_id)
        {
            return Err(ClientError::InvalidOptions(
                "Generic index row contains a duplicate field".to_owned(),
            ));
        }
        row.validate()?;
    }
    plan.rows
        .sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
    if plan
        .rows
        .windows(2)
        .any(|pair| pair[0].relative_path == pair[1].relative_path)
    {
        return Err(ClientError::InvalidOptions(
            "Generic index plan contains a duplicate relative path".to_owned(),
        ));
    }
    Ok(plan)
}

fn validate_append_result(
    call: &ClientCall<GenericIndexAppendResult>,
    first_sequence: u64,
    row_count: usize,
) -> Result<(), ClientError> {
    let receipt = &call.value.receipt;
    let row_count = u32::try_from(row_count).map_err(|_| {
        ClientError::ResponseMismatch("Generic index append row count exceeds u32".to_owned())
    })?;
    if receipt.first_sequence != first_sequence
        || receipt.row_count != row_count
        || receipt.resulting_row_count != first_sequence + u64::from(row_count)
        || call.value.registration.appended_row_count != receipt.resulting_row_count
        || call.value.registration.row_digest != receipt.resulting_row_digest
        || call.value.registration.phase != GenericIndexRegistrationPhase::Appending
    {
        return Err(ClientError::ResponseMismatch(
            "Generic index append response does not match its canonical page".to_owned(),
        ));
    }
    validate_mutation_commit_version(call.commit_version, receipt.commit_version, "append")
}

fn validate_mutation_commit_version(
    envelope: Option<u64>,
    durable: u64,
    stage: &str,
) -> Result<(), ClientError> {
    if envelope != Some(durable) {
        return Err(ClientError::ResponseMismatch(format!(
            "Generic index {stage} response commit version does not match durable state"
        )));
    }
    Ok(())
}

fn retry_exact<T>(
    max_attempts: u32,
    mut call: impl FnMut() -> Result<T, ClientError>,
) -> Result<T, ClientError> {
    for attempt in 1..=max_attempts {
        match call() {
            Ok(value) => return Ok(value),
            Err(error) if error.retryable() && attempt < max_attempts => {}
            Err(error) if error.retryable() => {
                return Err(ClientError::RetryExhausted {
                    attempts: attempt,
                    last_error: Box::new(error),
                });
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("validated client attempts are non-zero")
}

fn expect_generic_index_registration(
    result: WorkspaceResult,
) -> Result<GenericIndexRegistrationStatus, ClientError> {
    match result {
        WorkspaceResult::GenericIndexRegistration(status) => Ok(status),
        _ => Err(ClientError::ResponseMismatch(
            "expected Generic index registration result".to_owned(),
        )),
    }
}

fn expect_generic_index_append(
    result: WorkspaceResult,
) -> Result<GenericIndexAppendResult, ClientError> {
    match result {
        WorkspaceResult::GenericIndexAppend(append) => Ok(append),
        _ => Err(ClientError::ResponseMismatch(
            "expected Generic index append result".to_owned(),
        )),
    }
}

fn expect_generic_index_abort(
    result: WorkspaceResult,
) -> Result<GenericIndexAbortResult, ClientError> {
    match result {
        WorkspaceResult::GenericIndexAbort(abort) => Ok(abort),
        _ => Err(ClientError::ResponseMismatch(
            "expected Generic index abort result".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    use nokv_protocol::{
        decode_request, encode_response, Digest, GenericIndexFieldValues, LogicalShardIdentity,
        ObjectNamespaceIdentity, QueryOperator, RelativePath, RootIdentity, RootRoute, ScalarValue,
        WorkbenchName, WorkspacePreflightResult, WorkspaceRpcOutcome, WorkspaceRpcRequest,
        WorkspaceRpcResponse,
    };

    use super::*;
    use crate::{ClientOptions, StaticRouteResolver, TransportError};

    #[derive(Debug)]
    struct GenericWorkflowTransport {
        state: Mutex<GenericWorkflowState>,
        tamper_sequence: Option<u64>,
    }

    #[derive(Debug, Default)]
    struct GenericWorkflowState {
        requests: Vec<WorkspaceRpcRequest>,
        begin: Option<BeginGenericIndexRegistrationRequest>,
        lost_first_append_response: bool,
    }

    #[derive(Debug, Default)]
    struct GenericAbortTransport {
        state: Mutex<GenericAbortState>,
    }

    #[derive(Debug, Default)]
    struct GenericAbortState {
        requests: Vec<WorkspaceRpcRequest>,
        completed_batches: u32,
    }

    impl GenericWorkflowTransport {
        fn new(tamper_sequence: Option<u64>) -> Self {
            Self {
                state: Mutex::new(GenericWorkflowState::default()),
                tamper_sequence,
            }
        }
    }

    impl RpcTransport for GenericWorkflowTransport {
        fn round_trip(
            &self,
            _endpoint: SocketAddr,
            request: &[u8],
        ) -> Result<Vec<u8>, TransportError> {
            let request = decode_request(request)
                .map_err(|error| TransportError::new(error.to_string(), false))?;
            let mut state = self.state.lock().unwrap();
            state.requests.push(request.clone());

            let (result, commit_version, replayed) = match &request.operation {
                WorkspaceRequest::Preflight(_) => (
                    WorkspaceResult::Preflight(WorkspacePreflightResult::new(
                        request.route,
                        [WorkspaceCapability::GenericCustomIndexV1],
                    )),
                    None,
                    false,
                ),
                WorkspaceRequest::BeginGenericIndexRegistration(begin) => {
                    state.begin = Some(begin.clone());
                    (
                        WorkspaceResult::GenericIndexRegistration(scripted_status(
                            begin,
                            0,
                            8,
                            GenericIndexRegistrationPhase::Appending,
                        )),
                        Some(8),
                        false,
                    )
                }
                WorkspaceRequest::AppendGenericIndexRows(append) => {
                    if append.first_sequence == 0 && !state.lost_first_append_response {
                        state.lost_first_append_response = true;
                        return Err(TransportError::new(
                            "injected applied append response loss",
                            true,
                        ));
                    }
                    let begin = state.begin.as_ref().expect("begin precedes append");
                    let row_count = u32::try_from(append.rows.len()).unwrap();
                    let resulting_row_count = append.first_sequence + u64::from(row_count);
                    let commit_version = 9 + append.first_sequence / 240;
                    let mut registration = scripted_status(
                        begin,
                        resulting_row_count,
                        commit_version,
                        GenericIndexRegistrationPhase::Appending,
                    );
                    if self.tamper_sequence == Some(append.first_sequence) {
                        registration.capability_digest = Digest([0x99; 32]);
                    }
                    let resulting_row_digest = registration.row_digest;
                    (
                        WorkspaceResult::GenericIndexAppend(GenericIndexAppendResult {
                            registration,
                            receipt: GenericIndexAppendReceipt {
                                first_sequence: append.first_sequence,
                                row_count,
                                commit_version,
                                input_digest: Digest(
                                    [0x20 + u8::try_from(append.first_sequence / 240).unwrap(); 32],
                                ),
                                resulting_row_count,
                                resulting_row_digest,
                            },
                        }),
                        Some(commit_version),
                        append.first_sequence == 0,
                    )
                }
                WorkspaceRequest::FinalizeGenericIndexRegistration(_) => {
                    let begin = state.begin.as_ref().expect("begin precedes finalize");
                    (
                        WorkspaceResult::GenericIndexRegistration(scripted_status(
                            begin,
                            begin.declared_row_count,
                            20,
                            GenericIndexRegistrationPhase::Complete,
                        )),
                        Some(20),
                        false,
                    )
                }
                operation => {
                    return Err(TransportError::new(
                        format!("unexpected Generic workflow request: {operation:?}"),
                        false,
                    ));
                }
            };

            encode_response(&WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version,
                replayed,
                outcome: WorkspaceRpcOutcome::Success(Box::new(result)),
            })
            .map_err(|error| TransportError::new(error.to_string(), false))
        }
    }

    impl RpcTransport for GenericAbortTransport {
        fn round_trip(
            &self,
            _endpoint: SocketAddr,
            request: &[u8],
        ) -> Result<Vec<u8>, TransportError> {
            let request = decode_request(request)
                .map_err(|error| TransportError::new(error.to_string(), false))?;
            let mut state = self.state.lock().unwrap();
            state.requests.push(request.clone());
            let (result, commit_version) = match &request.operation {
                WorkspaceRequest::GetGenericIndexRegistration(get) => (
                    WorkspaceResult::GenericIndexRegistration(scripted_abort_status(
                        get.operation_id,
                        8,
                        GenericIndexRegistrationPhase::Appending,
                    )),
                    None,
                ),
                WorkspaceRequest::AbortGenericIndexRegistration(abort) => {
                    assert_eq!(abort.limit, MAX_GENERIC_INDEX_ABORT_ROWS);
                    state.completed_batches += 1;
                    let batch = state.completed_batches;
                    let cleanup_complete = batch == 3;
                    let commit_version = u64::from(8 + batch);
                    (
                        WorkspaceResult::GenericIndexAbort(GenericIndexAbortResult {
                            registration: scripted_abort_status(
                                abort.operation_id,
                                commit_version,
                                if cleanup_complete {
                                    GenericIndexRegistrationPhase::Cleaned
                                } else {
                                    GenericIndexRegistrationPhase::Cleaning
                                },
                            ),
                            removed_rows: if cleanup_complete { 33 } else { 120 },
                            removed_receipts: 0,
                            cleanup_complete,
                        }),
                        Some(commit_version),
                    )
                }
                operation => {
                    return Err(TransportError::new(
                        format!("unexpected Generic abort request: {operation:?}"),
                        false,
                    ));
                }
            };
            encode_response(&WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version,
                replayed: false,
                outcome: WorkspaceRpcOutcome::Success(Box::new(result)),
            })
            .map_err(|error| TransportError::new(error.to_string(), false))
        }
    }

    fn scripted_status(
        begin: &BeginGenericIndexRegistrationRequest,
        appended_row_count: u64,
        last_transition_version: u64,
        phase: GenericIndexRegistrationPhase,
    ) -> GenericIndexRegistrationStatus {
        let row_digest = if appended_row_count == 0 {
            Digest([0; 32])
        } else {
            Digest([u8::try_from(appended_row_count % 251).unwrap_or_default() + 1; 32])
        };
        GenericIndexRegistrationStatus {
            operation_id: begin.operation_id,
            generation_id: begin.generation_id,
            workspace_incarnation_id: begin.workspace_incarnation_id,
            index_path: begin.index_path.clone(),
            source_read_version: 7,
            last_transition_version,
            expected_current_generation: begin.expected_current_generation,
            capability_digest: Digest([0x44; 32]),
            declared_row_count: begin.declared_row_count,
            appended_row_count,
            row_digest,
            phase,
            published_pointer_generation: (phase == GenericIndexRegistrationPhase::Complete).then(
                || {
                    begin
                        .expected_current_generation
                        .map_or(1, |value| value + 1)
                },
            ),
            terminal_error: None,
        }
    }

    fn scripted_abort_status(
        operation_id: OperationIdentity,
        last_transition_version: u64,
        phase: GenericIndexRegistrationPhase,
    ) -> GenericIndexRegistrationStatus {
        GenericIndexRegistrationStatus {
            operation_id,
            generation_id: GenericIndexGenerationIdentity([0x22; 16]),
            workspace_incarnation_id: WorkspaceIdentity([0x23; 16]),
            index_path: None,
            source_read_version: 7,
            last_transition_version,
            expected_current_generation: None,
            capability_digest: Digest([0x24; 32]),
            declared_row_count: 273,
            appended_row_count: 273,
            row_digest: Digest([0x25; 32]),
            phase,
            published_pointer_generation: None,
            terminal_error: None,
        }
    }

    fn test_route() -> RootRoute {
        RootRoute {
            root_id: RootIdentity([0x11; 16]),
            logical_shard_id: LogicalShardIdentity([0x12; 16]),
            object_namespace_id: ObjectNamespaceIdentity([0x13; 16]),
            placement_generation: 1,
            owner_epoch: 1,
        }
    }

    fn workflow_client(
        transport: Arc<GenericWorkflowTransport>,
    ) -> WorkspaceClient<Arc<GenericWorkflowTransport>, StaticRouteResolver> {
        let route = test_route();
        WorkspaceClient::new(
            route.root_id,
            transport,
            StaticRouteResolver::new(route, "127.0.0.1:43119".parse().unwrap()).unwrap(),
            ClientOptions { max_attempts: 2 },
        )
        .unwrap()
    }

    fn abort_client(
        transport: Arc<GenericAbortTransport>,
    ) -> WorkspaceClient<Arc<GenericAbortTransport>, StaticRouteResolver> {
        let route = test_route();
        WorkspaceClient::new(
            route.root_id,
            transport,
            StaticRouteResolver::new(route, "127.0.0.1:43120".parse().unwrap()).unwrap(),
            ClientOptions { max_attempts: 2 },
        )
        .unwrap()
    }

    fn plan(row_count: usize) -> GenericIndexRegistrationPlan {
        GenericIndexRegistrationPlan {
            operation_id: OperationIdentity([1; 16]),
            generation_id: GenericIndexGenerationIdentity([2; 16]),
            workbench: WorkbenchName::new("generic-sdk").unwrap(),
            workspace_incarnation_id: WorkspaceIdentity([3; 16]),
            index_path: None,
            expected_current_generation: None,
            capabilities: vec![GenericIndexFieldCapability {
                field_id: "experiment.labels".to_owned(),
                operators: vec![
                    QueryOperator::Prefix,
                    QueryOperator::Equal,
                    QueryOperator::Equal,
                ],
                sortable: true,
                facetable: true,
            }],
            rows: (0..row_count)
                .rev()
                .map(|index| GenericIndexRow {
                    relative_path: Some(
                        RelativePath::new(format!("outputs/item-{index:04}.json")).unwrap(),
                    ),
                    values: vec![GenericIndexFieldValues {
                        field_id: "experiment.labels".to_owned(),
                        values: vec![
                            ScalarValue::String("alpha".to_owned()),
                            ScalarValue::Unsigned(index as u64),
                            ScalarValue::String("alpha".to_owned()),
                        ],
                    }],
                })
                .collect(),
        }
    }

    fn status(plan: &GenericIndexRegistrationPlan) -> GenericIndexRegistrationStatus {
        GenericIndexRegistrationStatus {
            operation_id: plan.operation_id,
            generation_id: plan.generation_id,
            workspace_incarnation_id: plan.workspace_incarnation_id,
            index_path: plan.index_path.clone(),
            source_read_version: 7,
            last_transition_version: 8,
            expected_current_generation: plan.expected_current_generation,
            capability_digest: Digest([4; 32]),
            declared_row_count: plan.rows.len() as u64,
            appended_row_count: 0,
            row_digest: Digest([0; 32]),
            phase: GenericIndexRegistrationPhase::Appending,
            published_pointer_generation: None,
            terminal_error: None,
        }
    }

    #[test]
    fn canonical_plan_pages_520_rows_without_collapsing_repeated_values() {
        let plan = canonicalize_plan(plan(520)).unwrap();
        assert_eq!(
            plan.capabilities[0].operators,
            vec![QueryOperator::Equal, QueryOperator::Prefix]
        );
        assert_eq!(
            plan.rows
                .chunks(MAX_GENERIC_INDEX_APPEND_ROWS)
                .map(<[GenericIndexRow]>::len)
                .collect::<Vec<_>>(),
            vec![240, 240, 40]
        );
        assert_eq!(
            plan.rows[0].values[0].values,
            vec![
                ScalarValue::String("alpha".to_owned()),
                ScalarValue::Unsigned(0),
                ScalarValue::String("alpha".to_owned()),
            ]
        );
    }

    #[test]
    fn zero_row_plan_is_valid_and_keeps_its_declared_catalog() {
        let canonical = canonicalize_plan(plan(0)).unwrap();
        assert!(canonical.rows.is_empty());
        assert_eq!(canonical.capabilities.len(), 1);
        BeginGenericIndexRegistrationRequest {
            operation_id: canonical.operation_id,
            generation_id: canonical.generation_id,
            workbench: canonical.workbench,
            workspace_incarnation_id: canonical.workspace_incarnation_id,
            index_path: canonical.index_path,
            expected_current_generation: canonical.expected_current_generation,
            capabilities: canonical.capabilities,
            declared_row_count: 0,
        }
        .validate()
        .unwrap();

        let transport = Arc::new(GenericWorkflowTransport::new(None));
        let outcome = workflow_client(transport)
            .register_generic_index(plan(0))
            .unwrap();
        assert!(outcome.append_receipts.is_empty());
        assert_eq!(
            outcome.finalization.value.phase,
            GenericIndexRegistrationPhase::Complete
        );
        assert_eq!(outcome.finalization.value.declared_row_count, 0);
    }

    #[test]
    fn durable_commitment_rejects_a_tampered_generation_or_catalog_digest() {
        let plan = canonicalize_plan(plan(1)).unwrap();
        let initial = status(&plan);
        let commitment = RegistrationCommitment::new(&plan, &initial).unwrap();

        let mut tampered = initial.clone();
        tampered.capability_digest = Digest([9; 32]);
        assert!(matches!(
            commitment.validate_status(&tampered),
            Err(ClientError::ResponseMismatch(_))
        ));

        tampered = initial;
        tampered.generation_id = GenericIndexGenerationIdentity([8; 16]);
        assert!(matches!(
            commitment.validate_status(&tampered),
            Err(ClientError::ResponseMismatch(_))
        ));
    }

    #[test]
    fn exact_retry_recovers_one_applied_but_lost_response() {
        let operation_id = OperationIdentity([5; 16]);
        let mut attempts = 0;
        let recovered = retry_exact(2, || {
            attempts += 1;
            assert_eq!(operation_id, OperationIdentity([5; 16]));
            if attempts == 1 {
                Err(ClientError::Transport(crate::TransportError::new(
                    "injected response loss",
                    true,
                )))
            } else {
                Ok(operation_id)
            }
        })
        .unwrap();
        assert_eq!(recovered, operation_id);
        assert_eq!(attempts, 2);
    }

    #[test]
    fn canonicalization_rejects_duplicate_paths_before_begin() {
        let mut plan = plan(2);
        plan.rows[1].relative_path = plan.rows[0].relative_path.clone();
        assert!(matches!(
            canonicalize_plan(plan),
            Err(ClientError::InvalidOptions(_))
        ));
    }

    #[test]
    fn workflow_pages_520_rows_and_exactly_replays_an_applied_lost_append() {
        let transport = Arc::new(GenericWorkflowTransport::new(None));
        let client = workflow_client(Arc::clone(&transport));

        let outcome = client.register_generic_index(plan(520)).unwrap();

        assert_eq!(outcome.append_receipts.len(), 3);
        assert!(outcome.finalization.replayed);
        assert_eq!(
            outcome.finalization.value.phase,
            GenericIndexRegistrationPhase::Complete
        );
        let state = transport.state.lock().unwrap();
        let append_requests = state
            .requests
            .iter()
            .filter_map(|request| match &request.operation {
                WorkspaceRequest::AppendGenericIndexRows(append) => {
                    Some((request.request_id, append))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            append_requests
                .iter()
                .map(|(_, append)| (append.first_sequence, append.rows.len()))
                .collect::<Vec<_>>(),
            vec![(0, 240), (0, 240), (240, 240), (480, 40)]
        );
        assert_eq!(append_requests[0].0, append_requests[1].0);
        assert_eq!(append_requests[0].1, append_requests[1].1);
        assert_eq!(
            append_requests[0].1.rows[0].values[0].values,
            vec![
                ScalarValue::String("alpha".to_owned()),
                ScalarValue::Unsigned(0),
                ScalarValue::String("alpha".to_owned()),
            ]
        );
    }

    #[test]
    fn workflow_rejects_a_midstream_capability_commitment_tamper() {
        let transport = Arc::new(GenericWorkflowTransport::new(Some(240)));
        let client = workflow_client(transport);

        assert!(matches!(
            client.register_generic_index(plan(520)),
            Err(ClientError::ResponseMismatch(message))
                if message.contains("canonical registration commitment")
        ));
    }

    #[test]
    fn abort_workflow_cleans_273_rows_in_bounded_batches() {
        let transport = Arc::new(GenericAbortTransport::default());
        let client = abort_client(Arc::clone(&transport));

        let outcome = client
            .abort_generic_index_registration_workflow(OperationIdentity([0x21; 16]))
            .unwrap();

        assert_eq!(outcome.batch_count, 3);
        assert!(outcome.final_batch.value.cleanup_complete);
        assert_eq!(outcome.final_batch.value.removed_rows, 33);
        assert_eq!(
            outcome.final_batch.value.registration.phase,
            GenericIndexRegistrationPhase::Cleaned
        );
        let state = transport.state.lock().unwrap();
        assert_eq!(state.completed_batches, 3);
        assert_eq!(state.requests.len(), 4);
        let request_ids = state
            .requests
            .iter()
            .filter_map(|request| match request.operation {
                WorkspaceRequest::AbortGenericIndexRegistration(_) => Some(request.request_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(request_ids.len(), 3);
        assert!(request_ids.windows(2).all(|pair| pair[0] != pair[1]));
    }
}
