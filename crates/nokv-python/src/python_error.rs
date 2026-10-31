/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Python exceptions for failed SDK calls.
//!
//! Every exception a `Client` call raises carries `error_class` and
//! `next_action`: the names the native CLI reports as `class` and
//! `next_action` (docs/cli-contract.md). The class is the one the error that
//! owns the failure reports; the next action also depends on how the call
//! identifies its effect. The exception types are unchanged: `ValueError` for
//! invalid arguments, `FileNotFoundError` and `FileExistsError` for path
//! absence and existence, `nokv.AppendError` and
//! `nokv.WorkspaceIncarnationMismatch` for their failures, and `RuntimeError`
//! otherwise.

use std::fmt;

use nokv_client::{ClientError, IdempotentPublishError, WorkbenchLifecycleError};
use nokv_object::ObjectError;
use nokv_protocol::{ConflictKind, ErrorCode, OperationIdentity, WorkspaceIdentity};
use nokv_types::{ErrorClass, IdempotencyScope, NextAction};
use pyo3::exceptions::{PyFileExistsError, PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::python_value::{cleanup_retry_receipt_to_py, hex};

/// Attach the class and next action of a failure that ended a call of
/// `scope`. A read changes nothing, so it never reports an unknown outcome.
fn classified(py: Python<'_>, error: PyErr, class: ErrorClass, scope: IdempotencyScope) -> PyErr {
    let class = match scope {
        IdempotencyScope::ReadOnly => class.for_read(),
        _ => class,
    };
    with_next_action(py, error, class, class.next_action(scope))
}

fn with_next_action(
    py: Python<'_>,
    error: PyErr,
    class: ErrorClass,
    next_action: NextAction,
) -> PyErr {
    let value = error.value(py);
    // Exception instances accept new attributes; failing to add them must
    // not replace the failure being raised.
    let _ = value.setattr("error_class", class.as_str());
    let _ = value.setattr("next_action", next_action.as_str());
    error
}

/// The call's arguments are invalid; nothing ran.
pub(crate) fn invalid_input(message: impl fmt::Display) -> PyErr {
    let message = message.to_string();
    unattached(ErrorClass::InvalidInput, || {
        PyValueError::new_err(message.clone())
    })
}

/// An unexpected failure inside the SDK.
pub(crate) fn internal(message: impl fmt::Display) -> PyErr {
    let message = message.to_string();
    unattached(ErrorClass::Internal, || {
        PyRuntimeError::new_err(message.clone())
    })
}

/// Classify an exception raised where no `Python` token is at hand. A
/// `Client` call always runs in an attached interpreter; without one, such as
/// in a Rust-only test, the exception is raised unclassified.
fn unattached(class: ErrorClass, exception: impl Fn() -> PyErr) -> PyErr {
    Python::try_attach(|py| classified(py, exception(), class, IdempotencyScope::ReadOnly))
        .unwrap_or_else(exception)
}

/// A client that could not be configured or routed raises `ValueError`.
/// Constructing a client changes nothing.
pub(crate) fn configuration_failure(py: Python<'_>, error: ClientError) -> PyErr {
    classified(
        py,
        PyValueError::new_err(error.to_string()),
        error.class(),
        IdempotencyScope::ReadOnly,
    )
}

/// A path call's failure: an absent path raises `FileNotFoundError` and an
/// existing one `FileExistsError`.
pub(crate) fn client_failure(py: Python<'_>, error: ClientError, scope: IdempotencyScope) -> PyErr {
    classified(py, path_exception(&error), error.class(), scope)
}

/// A client failure of a call that raises `RuntimeError` for it: queries,
/// workspace creation, and the client's own preflight.
pub(crate) fn runtime_client_failure(
    py: Python<'_>,
    error: ClientError,
    scope: IdempotencyScope,
) -> PyErr {
    classified(
        py,
        PyRuntimeError::new_err(error.to_string()),
        error.class(),
        scope,
    )
}

/// A publication failure. With an `expected` incarnation, a refused fence
/// raises `nokv.WorkspaceIncarnationMismatch` so callers never parse text.
pub(crate) fn publish_failure(
    py: Python<'_>,
    error: ClientError,
    expected: Option<&str>,
    scope: IdempotencyScope,
) -> PyErr {
    let exception = match expected {
        Some(expected) if incarnation_fenced(&error) => {
            incarnation_mismatch(py, error.to_string(), expected)
        }
        _ => path_exception(&error),
    };
    classified(py, exception, error.class(), scope)
}

/// A commit or restore failure. Both derive their identity from their
/// content, so an unknown outcome is resolved by repeating the call.
pub(crate) fn lifecycle_failure<ProjectionError: fmt::Display>(
    py: Python<'_>,
    error: WorkbenchLifecycleError<ProjectionError>,
) -> PyErr {
    classified(
        py,
        PyRuntimeError::new_err(error.to_string()),
        error.class(),
        IdempotencyScope::ContentDerived,
    )
}

/// A failure of `put_bytes` under the caller's operation identity. The
/// exception names the `operation_id`, the failed `attempt`, and the path's
/// `current_generation` when the owner refused a generation.
pub(crate) fn identified_publish_failure(
    py: Python<'_>,
    error: IdempotentPublishError,
    operation_id: OperationIdentity,
    expected: Option<&str>,
) -> PyErr {
    let (exception, current_generation) = match &error {
        IdempotentPublishError::Client { source, .. } => (
            match expected {
                Some(expected) if incarnation_fenced(source) => {
                    incarnation_mismatch(py, error.to_string(), expected)
                }
                _ => path_exception(source),
            },
            source
                .rpc_failure()
                .and_then(|failure| failure.current_generation),
        ),
        IdempotentPublishError::IdentityConflict { .. }
        | IdempotentPublishError::InProgress { .. }
        | IdempotentPublishError::Quarantined { .. } => {
            (PyRuntimeError::new_err(error.to_string()), None)
        }
    };
    let exception = classified(
        py,
        exception,
        error.class(),
        IdempotencyScope::CallerOperation,
    );
    let value = exception.value(py);
    let _ = value.setattr("operation_id", hex(&operation_id.0));
    let _ = value.setattr("attempt", error.attempt());
    let _ = value.setattr("current_generation", current_generation);
    exception
}

/// How an append-family call names its operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OperationCall {
    /// `append_bytes`: a mutation under the caller's operation identity.
    Append,
    /// `operation_status` and `operation_inspect`: reads of one operation.
    Observe,
    /// `operation_recover`: a cleanup request for one operation.
    Recover,
}

impl OperationCall {
    const fn scope(self) -> IdempotencyScope {
        match self {
            Self::Observe => IdempotencyScope::ReadOnly,
            Self::Append | Self::Recover => IdempotencyScope::CallerOperation,
        }
    }

    /// The action after a failure of `class`. An absent operation is a
    /// point-in-time observation: resubmitting the command that names it
    /// admits it at most once. A cleanup request whose answer was lost names
    /// its saved state digest, so its retry repeats that digest; a refused
    /// one is the refusal's to act on.
    fn next_action(self, error: &ClientError, class: ErrorClass) -> NextAction {
        if self != Self::Observe
            && class == ErrorClass::OutcomeUnknown
            && matches!(error, ClientError::AppendCleanupUnresolved { .. })
        {
            NextAction::RetrySameCleanup
        } else if self != Self::Append && class == ErrorClass::NotFound {
            NextAction::ResubmitSame
        } else {
            class.next_action(self.scope())
        }
    }
}

/// An append, operation query, or recovery failure under its operation
/// identity, raised as `nokv.AppendError`. With an `expected` incarnation, a
/// refused fence raises `nokv.WorkspaceIncarnationMismatch` carrying the
/// same recovery attributes.
pub(crate) fn append_failure(
    py: Python<'_>,
    error: ClientError,
    operation_id: OperationIdentity,
    expected: Option<WorkspaceIdentity>,
    call: OperationCall,
) -> PyErr {
    let class = match call.scope() {
        IdempotencyScope::ReadOnly => error.class().for_read(),
        _ => error.class(),
    };
    let next_action = call.next_action(&error, class);
    let message = error.to_string();
    let cause_code = error
        .rpc_failure()
        .map(|failure| format!("{:?}", failure.code));
    if let ClientError::AppendCleanupUnresolved {
        expected_token,
        receipt,
        ..
    } = &error
    {
        let exception = py
            .import("nokv")
            .and_then(|module| module.getattr("AppendError"))
            .and_then(|class| {
                let recovery_receipt = receipt
                    .as_deref()
                    .map(|value| cleanup_retry_receipt_to_py(py, value))
                    .transpose()?;
                class.call1((
                    message.clone(),
                    hex(&operation_id.0),
                    None::<String>,
                    "AppendCleanupUnresolved",
                    None::<String>,
                    cause_code,
                    next_action.as_str(),
                    receipt
                        .as_ref()
                        .map(|value| hex(&value.publication_operation_id.0)),
                    hex(&expected_token.state_digest.0),
                    recovery_receipt,
                ))
            })
            .map(PyErr::from_value)
            .unwrap_or_else(|_| PyRuntimeError::new_err(message));
        return with_next_action(py, exception, class, next_action);
    }
    let identity = hex(&operation_id.0);
    let expected = expected.map(|value| hex(&value.0));
    let state = match &error {
        ClientError::AppendUnresolved { state, .. } => state.map(|state| format!("{state:?}")),
        _ => None,
    };
    let code = match &error {
        ClientError::AppendUnresolved { .. } => "AppendUnresolved".to_owned(),
        ClientError::InvalidOptions(_) => "InvalidArgument".to_owned(),
        _ => cause_code
            .clone()
            .unwrap_or_else(|| "AppendFailed".to_owned()),
    };
    if let Some(expected) = expected.as_deref().filter(|_| incarnation_fenced(&error)) {
        let mapped = with_next_action(
            py,
            incarnation_mismatch(py, message, expected),
            class,
            next_action,
        );
        let value = mapped.value(py);
        let _ = value.setattr("operation_id", identity);
        let _ = value.setattr("state", state);
        let _ = value.setattr("code", code);
        let _ = value.setattr("cause_code", cause_code);
        let _ = value.setattr("publication_operation_id", py.None());
        let _ = value.setattr("retryable", false);
        return mapped;
    }
    let exception = new_append_error(py, message, identity, state, &code, expected, cause_code);
    with_next_action(py, exception, class, next_action)
}

/// An append-family failure the SDK detects before any request, such as an
/// invalid argument or an object store that could not be bound.
pub(crate) fn append_local_failure(
    py: Python<'_>,
    message: String,
    operation_id: OperationIdentity,
    expected: Option<WorkspaceIdentity>,
    code: &str,
    class: ErrorClass,
) -> PyErr {
    let exception = new_append_error(
        py,
        message,
        hex(&operation_id.0),
        None,
        code,
        expected.map(|value| hex(&value.0)),
        None,
    );
    classified(py, exception, class, IdempotencyScope::CallerOperation)
}

fn new_append_error(
    py: Python<'_>,
    message: String,
    operation_id: String,
    state: Option<String>,
    code: &str,
    expected: Option<String>,
    cause_code: Option<String>,
) -> PyErr {
    py.import("nokv")
        .and_then(|module| module.getattr("AppendError"))
        .and_then(|class| {
            class.call1((
                message.clone(),
                operation_id,
                state,
                code,
                expected,
                cause_code,
            ))
        })
        .map(PyErr::from_value)
        .unwrap_or_else(|_| PyRuntimeError::new_err(message))
}

/// Why a client could not bind its object store. Binding runs before the
/// call's own request and applies none of it.
#[derive(Debug)]
pub(crate) enum ObjectBindFailure {
    /// The client was constructed without an `object_store`.
    Unconfigured,
    /// The configured store could not be built.
    Build(ObjectError),
    /// The preflight that names the root's object namespace failed.
    Preflight(ClientError),
    /// The object namespace could not be opened or verified.
    Namespace(ObjectError),
    /// A panic poisoned the binding lock.
    Poisoned,
}

impl ObjectBindFailure {
    pub(crate) fn class(&self) -> ErrorClass {
        match self {
            // A store that cannot be constructed is a configuration the
            // caller must change, as the native CLI reports it.
            Self::Unconfigured | Self::Build(_) => ErrorClass::InvalidInput,
            Self::Preflight(error) => error.class(),
            Self::Namespace(error) => error.class(),
            Self::Poisoned => ErrorClass::Internal,
        }
        .for_read()
    }

    /// Raised as `ValueError` for the store's configuration and namespace,
    /// and `RuntimeError` otherwise, as before failures carried a class.
    pub(crate) fn into_py_err(self, py: Python<'_>) -> PyErr {
        let class = self.class();
        let exception = match &self {
            Self::Unconfigured | Self::Build(_) | Self::Namespace(_) => {
                PyValueError::new_err(self.to_string())
            }
            Self::Preflight(_) | Self::Poisoned => PyRuntimeError::new_err(self.to_string()),
        };
        classified(py, exception, class, IdempotencyScope::ReadOnly)
    }
}

impl fmt::Display for ObjectBindFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfigured => {
                formatter.write_str("this operation requires an object_store configuration")
            }
            Self::Build(error) => error.fmt(formatter),
            Self::Preflight(error) => error.fmt(formatter),
            Self::Namespace(error) => error.fmt(formatter),
            Self::Poisoned => formatter.write_str("object store initialization lock was poisoned"),
        }
    }
}

/// Why `materialize` failed: the local tree refused it, the workspace
/// changed under it, or a read failed.
#[derive(Debug)]
pub(crate) enum MaterializeFailure {
    /// The local destination refused the tree.
    Local(String),
    /// A path changed between the listing and its read; calling again reads
    /// the new state.
    Changed(String),
    /// Listing or reading the workspace failed.
    Read(ClientError),
}

impl MaterializeFailure {
    /// Raised as `RuntimeError`, as before failures carried a class.
    pub(crate) fn into_py_err(self, py: Python<'_>) -> PyErr {
        let class = match &self {
            Self::Local(_) => ErrorClass::InvalidInput,
            Self::Changed(_) => ErrorClass::Transient,
            Self::Read(error) => error.class(),
        };
        classified(
            py,
            PyRuntimeError::new_err(self.to_string()),
            class,
            IdempotencyScope::ReadOnly,
        )
    }
}

impl From<String> for MaterializeFailure {
    fn from(message: String) -> Self {
        Self::Local(message)
    }
}

impl From<ClientError> for MaterializeFailure {
    fn from(error: ClientError) -> Self {
        Self::Read(error)
    }
}

impl fmt::Display for MaterializeFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(message) | Self::Changed(message) => formatter.write_str(message),
            Self::Read(error) => error.fmt(formatter),
        }
    }
}

/// A local file the call names cannot be read, raised as `RuntimeError`.
pub(crate) fn local_file_failure(py: Python<'_>, message: String) -> PyErr {
    classified(
        py,
        PyRuntimeError::new_err(message),
        ErrorClass::InvalidInput,
        IdempotencyScope::ReadOnly,
    )
}

/// A failed `collect`. It publishes one create-only artifact per file under
/// fresh identities, so a failure can leave some files published: read the
/// workspace before deciding again.
pub(crate) fn collect_failure(py: Python<'_>, message: String) -> PyErr {
    classified(
        py,
        PyRuntimeError::new_err(message),
        ErrorClass::OutcomeUnknown,
        IdempotencyScope::Unidentified,
    )
}

/// `FileNotFoundError` and `FileExistsError` keep their meaning for path
/// callers.
fn path_exception(error: &ClientError) -> PyErr {
    let message = error.to_string();
    match error.rpc_failure().map(|failure| failure.code) {
        Some(ErrorCode::NotFound) => PyFileNotFoundError::new_err(message),
        Some(ErrorCode::AlreadyExists) => PyFileExistsError::new_err(message),
        _ => PyRuntimeError::new_err(message),
    }
}

fn incarnation_fenced(error: &ClientError) -> bool {
    error
        .rpc_failure()
        .is_some_and(|failure| failure.conflict == Some(ConflictKind::WorkspaceIncarnation))
}

fn incarnation_mismatch(py: Python<'_>, message: String, expected: &str) -> PyErr {
    py.import("nokv")
        .and_then(|module| module.getattr("WorkspaceIncarnationMismatch"))
        .and_then(|class| class.call1((message.clone(), expected)))
        .map(PyErr::from_value)
        .unwrap_or_else(|_| PyRuntimeError::new_err(message))
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::BTreeSet;
    use std::ffi::CString;
    use std::sync::Once;

    use nokv_client::TransportError;
    use nokv_protocol::{Digest, OperationState, OperationToken, RpcFailure};
    use pyo3::types::PyAnyMethods;

    use super::*;

    const ERRORS_MODULE: &str = include_str!("../python/nokv/_errors.py");

    /// Run `test` with the package's own exception classes importable as
    /// `nokv`, so the SDK raises the exceptions an installed package does.
    pub(crate) fn with_package_errors<R>(test: impl FnOnce(Python<'_>) -> R) -> R {
        static INSTALL: Once = Once::new();
        Python::initialize();
        // Executing a module publishes it in `sys.modules` before its body
        // has run, so concurrent tests wait here, outside the interpreter.
        INSTALL.call_once(|| {
            Python::attach(|py| {
                let code = CString::new(ERRORS_MODULE).unwrap();
                let module = PyModule::from_code(py, &code, c"nokv/_errors.py", c"nokv").unwrap();
                let modules = py.import("sys").unwrap().getattr("modules").unwrap();
                modules.set_item("nokv", module).unwrap();
            });
        });
        Python::attach(test)
    }

    /// The exception type name and the contract attributes of `error`.
    pub(crate) fn contract_of(py: Python<'_>, error: &PyErr) -> (String, String, String) {
        let value = error.value(py);
        (
            value.get_type().name().unwrap().to_string(),
            value.getattr("error_class").unwrap().extract().unwrap(),
            value.getattr("next_action").unwrap().extract().unwrap(),
        )
    }

    fn rpc(code: ErrorCode, retryable: bool) -> ClientError {
        ClientError::Rpc(RpcFailure {
            code,
            message: format!("{code:?}"),
            retryable,
            conflict: None,
            current_generation: None,
            route_hint: None,
        })
    }

    fn lost_reply() -> ClientError {
        ClientError::Transport(TransportError::new("connection reset", true))
    }

    const OPERATION: OperationIdentity = OperationIdentity([0xaa; 16]);

    #[test]
    fn every_failure_class_reaches_python_with_the_next_action_its_call_permits() {
        with_package_errors(|py| {
            let unresolved = |state, source| ClientError::AppendUnresolved {
                operation_id: OPERATION,
                state,
                source: Box::new(source),
            };
            let cases = [
                (
                    invalid_input("bad id"),
                    ("ValueError", "invalid_input", "fix_input"),
                ),
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::NotFound, false),
                        IdempotencyScope::ReadOnly,
                    ),
                    ("FileNotFoundError", "not_found", "do_not_retry"),
                ),
                // A missing workbench under a mutation is read before deciding.
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::NotFound, false),
                        IdempotencyScope::Unidentified,
                    ),
                    ("FileNotFoundError", "not_found", "reread_then_retry"),
                ),
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::AlreadyExists, false),
                        IdempotencyScope::Unidentified,
                    ),
                    (
                        "FileExistsError",
                        "precondition_failed",
                        "reread_then_retry",
                    ),
                ),
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::Conflict, true),
                        IdempotencyScope::Convergent,
                    ),
                    ("RuntimeError", "transient", "resubmit_same"),
                ),
                // A read changes nothing, so its lost reply is only transient.
                (
                    client_failure(py, lost_reply(), IdempotencyScope::ReadOnly),
                    ("RuntimeError", "transient", "resubmit_same"),
                ),
                // A mutation's lost reply is resolved through its identity.
                (
                    client_failure(py, lost_reply(), IdempotencyScope::Unidentified),
                    ("RuntimeError", "outcome_unknown", "reread_then_retry"),
                ),
                (
                    append_failure(py, lost_reply(), OPERATION, None, OperationCall::Append),
                    ("AppendError", "outcome_unknown", "query_same"),
                ),
                (
                    lifecycle_failure(py, WorkbenchLifecycleError::<String>::Client(lost_reply())),
                    ("RuntimeError", "outcome_unknown", "resubmit_same"),
                ),
                (
                    append_failure(
                        py,
                        unresolved(Some(OperationState::Running), lost_reply()),
                        OPERATION,
                        None,
                        OperationCall::Append,
                    ),
                    ("AppendError", "in_progress", "poll"),
                ),
                (
                    append_failure(
                        py,
                        unresolved(None, rpc(ErrorCode::RequestReplayMismatch, false)),
                        OPERATION,
                        None,
                        OperationCall::Append,
                    ),
                    ("AppendError", "identity_conflict", "do_not_retry"),
                ),
                (
                    runtime_client_failure(
                        py,
                        ClientError::Transport(TransportError::incompatible("schema")),
                        IdempotencyScope::ReadOnly,
                    ),
                    ("RuntimeError", "incompatible", "do_not_retry"),
                ),
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::Quarantined, false),
                        IdempotencyScope::Unidentified,
                    ),
                    ("RuntimeError", "operator_required", "operator"),
                ),
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::Unauthenticated, false),
                        IdempotencyScope::Unidentified,
                    ),
                    ("RuntimeError", "unauthenticated", "operator"),
                ),
                // The owner could not refresh its policy copy: retry.
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::Unauthenticated, true),
                        IdempotencyScope::Unidentified,
                    ),
                    ("RuntimeError", "transient", "resubmit_same"),
                ),
                (
                    client_failure(
                        py,
                        rpc(ErrorCode::PermissionDenied, false),
                        IdempotencyScope::Unidentified,
                    ),
                    ("RuntimeError", "permission_denied", "operator"),
                ),
                (
                    internal("bug"),
                    ("RuntimeError", "internal", "do_not_retry"),
                ),
                (
                    append_failure(
                        py,
                        ClientError::AppendCleanupUnresolved {
                            operation_id: OPERATION,
                            expected_token: OperationToken {
                                operation_id: OPERATION,
                                state_digest: Digest([0xcc; 32]),
                            },
                            receipt: None,
                            source: Box::new(lost_reply()),
                        },
                        OPERATION,
                        None,
                        OperationCall::Recover,
                    ),
                    ("AppendError", "outcome_unknown", "retry_same_cleanup"),
                ),
                // A cleanup retry of another principal's append is refused.
                (
                    append_failure(
                        py,
                        ClientError::AppendCleanupUnresolved {
                            operation_id: OPERATION,
                            expected_token: OperationToken {
                                operation_id: OPERATION,
                                state_digest: Digest([0xcc; 32]),
                            },
                            receipt: None,
                            source: Box::new(rpc(ErrorCode::PermissionDenied, false)),
                        },
                        OPERATION,
                        None,
                        OperationCall::Recover,
                    ),
                    ("AppendError", "permission_denied", "operator"),
                ),
                // An absent operation is resubmitted; it is admitted at most once.
                (
                    append_failure(
                        py,
                        rpc(ErrorCode::NotFound, false),
                        OPERATION,
                        None,
                        OperationCall::Observe,
                    ),
                    ("AppendError", "not_found", "resubmit_same"),
                ),
            ];
            let mut classes = BTreeSet::new();
            for (error, (kind, class, next_action)) in &cases {
                let observed = contract_of(py, error);
                assert_eq!(
                    observed,
                    (kind.to_string(), class.to_string(), next_action.to_string()),
                    "{error}"
                );
                classes.insert(*class);
            }
            let every_class: BTreeSet<_> =
                ErrorClass::ALL.iter().map(|class| class.as_str()).collect();
            assert_eq!(classes, every_class);
        });
    }

    #[test]
    fn an_append_input_error_is_fixed_not_queried() {
        with_package_errors(|py| {
            let error = append_local_failure(
                py,
                "append delta is too large".to_owned(),
                OPERATION,
                Some(WorkspaceIdentity([0xbb; 16])),
                "InvalidArgument",
                ErrorClass::InvalidInput,
            );
            assert_eq!(
                contract_of(py, &error),
                (
                    "AppendError".to_owned(),
                    "invalid_input".to_owned(),
                    "fix_input".to_owned()
                )
            );
            let value = error.value(py);
            let field = |name: &str| value.getattr(name).unwrap();
            assert_eq!(
                field("code").extract::<String>().unwrap(),
                "InvalidArgument"
            );
            assert_eq!(
                field("operation_id").extract::<String>().unwrap(),
                "aa".repeat(16)
            );
            assert_eq!(
                field("expected").extract::<String>().unwrap(),
                "bb".repeat(16)
            );
            assert!(!field("retryable").extract::<bool>().unwrap());
        });
    }

    #[test]
    fn a_refused_incarnation_fence_keeps_its_typed_exception_and_recovery_attributes() {
        with_package_errors(|py| {
            let fenced = ClientError::Rpc(RpcFailure {
                code: ErrorCode::Conflict,
                message: "workspace incarnation changed".to_owned(),
                retryable: false,
                conflict: Some(ConflictKind::WorkspaceIncarnation),
                current_generation: None,
                route_hint: None,
            });
            let publish = publish_failure(
                py,
                fenced.clone(),
                Some(&"bb".repeat(16)),
                IdempotencyScope::Unidentified,
            );
            assert_eq!(
                contract_of(py, &publish),
                (
                    "WorkspaceIncarnationMismatch".to_owned(),
                    "precondition_failed".to_owned(),
                    "reread_then_retry".to_owned()
                )
            );
            // An append resolves its fence before admission, so the refusal
            // applied nothing and is the fence conflict itself.
            let append = append_failure(
                py,
                fenced,
                OPERATION,
                Some(WorkspaceIdentity([0xbb; 16])),
                OperationCall::Append,
            );
            assert_eq!(
                contract_of(py, &append),
                (
                    "WorkspaceIncarnationMismatch".to_owned(),
                    "precondition_failed".to_owned(),
                    "reread_then_retry".to_owned()
                )
            );
            let value = append.value(py);
            let field = |name: &str| value.getattr(name).unwrap();
            assert_eq!(
                field("operation_id").extract::<String>().unwrap(),
                "aa".repeat(16)
            );
            assert!(field("state").is_none());
            assert_eq!(field("code").extract::<String>().unwrap(), "Conflict");
            assert_eq!(field("cause_code").extract::<String>().unwrap(), "Conflict");
            assert!(field("publication_operation_id").is_none());
            assert!(!field("retryable").extract::<bool>().unwrap());
        });
    }

    #[test]
    fn a_caller_identified_publication_names_its_identity_attempt_and_head() {
        with_package_errors(|py| {
            let stale = IdempotentPublishError::Client {
                attempt: 2,
                source: Box::new(ClientError::Rpc(RpcFailure {
                    code: ErrorCode::Conflict,
                    message: "path generation mismatch".to_owned(),
                    retryable: false,
                    conflict: None,
                    current_generation: Some(5),
                    route_hint: None,
                })),
            };
            let error = identified_publish_failure(py, stale, OPERATION, None);
            assert_eq!(
                contract_of(py, &error),
                (
                    "RuntimeError".to_owned(),
                    "precondition_failed".to_owned(),
                    "reread_then_retry".to_owned()
                )
            );
            let value = error.value(py);
            assert_eq!(
                value
                    .getattr("operation_id")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "aa".repeat(16)
            );
            assert_eq!(
                value.getattr("attempt").unwrap().extract::<u64>().unwrap(),
                2
            );
            assert_eq!(
                value
                    .getattr("current_generation")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                5
            );
            let reused = identified_publish_failure(
                py,
                IdempotentPublishError::IdentityConflict {
                    operation_id: OPERATION,
                    message: "operation id was already used for another publication".to_owned(),
                },
                OPERATION,
                None,
            );
            assert_eq!(contract_of(py, &reused).1, "identity_conflict");
            assert_eq!(contract_of(py, &reused).2, "do_not_retry");
            assert!(reused.value(py).getattr("attempt").unwrap().is_none());
            let running = identified_publish_failure(
                py,
                IdempotentPublishError::InProgress {
                    operation_id: OPERATION,
                    attempt: 0,
                    state: OperationState::Aborting,
                },
                OPERATION,
                None,
            );
            assert_eq!(contract_of(py, &running).2, "poll");
            let lost = identified_publish_failure(
                py,
                IdempotentPublishError::Client {
                    attempt: 0,
                    source: Box::new(lost_reply()),
                },
                OPERATION,
                None,
            );
            assert_eq!(contract_of(py, &lost).1, "outcome_unknown");
            assert_eq!(contract_of(py, &lost).2, "query_same");
        });
    }

    #[test]
    fn adapter_and_binding_failures_carry_the_class_of_what_failed() {
        with_package_errors(|py| {
            let cases = [
                (
                    ObjectBindFailure::Unconfigured.into_py_err(py),
                    ("ValueError", "invalid_input", "fix_input"),
                ),
                (
                    ObjectBindFailure::Build(ObjectError::MissingBucket).into_py_err(py),
                    ("ValueError", "invalid_input", "fix_input"),
                ),
                // Binding applies nothing, so a lost preflight reply is transient.
                (
                    ObjectBindFailure::Preflight(lost_reply()).into_py_err(py),
                    ("RuntimeError", "transient", "resubmit_same"),
                ),
                (
                    MaterializeFailure::Changed("changed".to_owned()).into_py_err(py),
                    ("RuntimeError", "transient", "resubmit_same"),
                ),
                (
                    MaterializeFailure::from("destination exists".to_owned()).into_py_err(py),
                    ("RuntimeError", "invalid_input", "fix_input"),
                ),
                (
                    MaterializeFailure::from(rpc(ErrorCode::NotFound, false)).into_py_err(py),
                    ("RuntimeError", "not_found", "do_not_retry"),
                ),
                // Some files may already be published under fresh identities.
                (
                    collect_failure(py, "upload failed".to_owned()),
                    ("RuntimeError", "outcome_unknown", "reread_then_retry"),
                ),
                (
                    lifecycle_failure(
                        py,
                        WorkbenchLifecycleError::<String>::Conflict(
                            "workbench already has a different commit head".to_owned(),
                        ),
                    ),
                    ("RuntimeError", "precondition_failed", "reread_then_retry"),
                ),
                (
                    local_file_failure(py, "not a regular file".to_owned()),
                    ("RuntimeError", "invalid_input", "fix_input"),
                ),
            ];
            for (error, (kind, class, next_action)) in &cases {
                assert_eq!(
                    contract_of(py, error),
                    (kind.to_string(), class.to_string(), next_action.to_string()),
                    "{error}"
                );
            }
        });
    }
}
