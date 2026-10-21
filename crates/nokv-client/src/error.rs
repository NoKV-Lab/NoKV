/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use nokv_object::{ArtifactUploadFailure, ObjectError};
use nokv_protocol::{
    AppendCleanupRetryResult, ConflictKind, ErrorCode, OperationIdentity, OperationState,
    OperationToken, ProtocolError, RpcFailure, WorkspaceCapability,
};
use nokv_types::ErrorClass;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError {
    message: String,
    retryable: bool,
    incompatible: bool,
}

impl TransportError {
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
            incompatible: false,
        }
    }

    /// The owner refused this client's protocol in the handshake, so no
    /// request was sent and repeating it cannot succeed.
    pub fn incompatible(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
            incompatible: true,
        }
    }

    pub const fn is_incompatible(&self) -> bool {
        self.incompatible
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn retryable(&self) -> bool {
        self.retryable
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for TransportError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArtifactPublishStage {
    Begin,
    StageObjects,
    /// `Begin` and staging of a whole one-batch object plan in one request.
    Prepare,
    UploadObjects,
    MarkObjectsUploaded,
    StageManifest,
    Complete,
    /// Marking uploads, staging the manifest, and `Complete` in one request.
    Finish,
    /// `Begin` and `Complete` of a body stored inline, in one request.
    PublishInline,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientError {
    InvalidOptions(String),
    InvalidRoute(String),
    Transport(TransportError),
    Protocol(ProtocolError),
    Rpc(RpcFailure),
    ResponseMismatch(String),
    MissingCapabilities(Vec<WorkspaceCapability>),
    ArtifactIntegrity(String),
    ArtifactReadFenceChanged,
    Object(ObjectError),
    ArtifactUpload(Box<ArtifactUploadFailure>),
    ArtifactPublishFailed {
        stage: ArtifactPublishStage,
        source: Box<ClientError>,
        abort_failure: Option<Box<ClientError>>,
    },
    /// The stable append identity must be retained until its recorded outcome
    /// is understood. A missing state is not proof that publication is absent.
    AppendUnresolved {
        operation_id: OperationIdentity,
        state: Option<OperationState>,
        source: Box<ClientError>,
    },
    /// Cleanup was requested against this exact observation. Retain its token
    /// across process retries; a fresh observation may authorize another round.
    AppendCleanupUnresolved {
        operation_id: OperationIdentity,
        expected_token: OperationToken,
        receipt: Option<Box<AppendCleanupRetryResult>>,
        source: Box<ClientError>,
    },
    RetryExhausted {
        attempts: u32,
        last_error: Box<ClientError>,
    },
}

impl ClientError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::Transport(error) => error.retryable(),
            Self::Rpc(error) => error.retryable,
            Self::Object(error) => error.retryable(),
            Self::ArtifactUpload(error) => error.retryable(),
            Self::ArtifactPublishFailed {
                source,
                abort_failure,
                ..
            } => abort_failure.is_none() && source.retryable(),
            Self::RetryExhausted { last_error, .. } => last_error.retryable(),
            // Generic retry loops must not turn an unresolved append into a
            // fresh operation. The caller can explicitly retry the same intent.
            Self::AppendUnresolved { .. } | Self::AppendCleanupUnresolved { .. } => false,
            Self::ArtifactReadFenceChanged => true,
            Self::InvalidOptions(_)
            | Self::InvalidRoute(_)
            | Self::Protocol(_)
            | Self::ResponseMismatch(_)
            | Self::MissingCapabilities(_)
            | Self::ArtifactIntegrity(_) => false,
        }
    }

    /// The owner's typed refusal this error carries, through any wrapping.
    pub fn rpc_failure(&self) -> Option<&RpcFailure> {
        match self {
            Self::Rpc(failure) => Some(failure),
            Self::ArtifactPublishFailed { source, .. } => source.rpc_failure(),
            Self::AppendUnresolved { source, .. } => source.rpc_failure(),
            Self::AppendCleanupUnresolved { source, .. } => source.rpc_failure(),
            Self::RetryExhausted { last_error, .. } => last_error.rpc_failure(),
            _ => None,
        }
    }

    pub(crate) fn rpc_code(&self) -> Option<ErrorCode> {
        self.rpc_failure().map(|failure| failure.code)
    }

    /// What this failure means for durable state when it ends a mutation.
    ///
    /// A failure that may follow delivery is an unknown outcome: an owner can
    /// apply a request and then lose its response, its connection, or its
    /// ownership proof. A read changes nothing and applies
    /// [`ErrorClass::for_read`].
    pub fn class(&self) -> ErrorClass {
        match self {
            Self::InvalidOptions(_) => ErrorClass::InvalidInput,
            // An unresolvable route sends nothing.
            Self::InvalidRoute(_) => ErrorClass::Transient,
            Self::Transport(error) if error.is_incompatible() => ErrorClass::Incompatible,
            Self::Transport(_) | Self::Protocol(_) | Self::ResponseMismatch(_) => {
                ErrorClass::OutcomeUnknown
            }
            Self::Rpc(failure) => rpc_failure_class(failure),
            Self::MissingCapabilities(_) => ErrorClass::Incompatible,
            Self::ArtifactIntegrity(_) => ErrorClass::OperatorRequired,
            Self::ArtifactReadFenceChanged => ErrorClass::Transient,
            Self::Object(error) => error.class(),
            Self::ArtifactUpload(error) => {
                if error.retryable() {
                    ErrorClass::Transient
                } else {
                    ErrorClass::OutcomeUnknown
                }
            }
            Self::ArtifactPublishFailed {
                source,
                abort_failure,
                ..
            } => {
                if abort_failure.is_some() {
                    // The publication's durable state was not confirmed.
                    ErrorClass::OutcomeUnknown
                } else {
                    source.class()
                }
            }
            Self::AppendUnresolved { state, source, .. } => {
                if source.rpc_code() == Some(ErrorCode::RequestReplayMismatch) {
                    return ErrorClass::IdentityConflict;
                }
                match state {
                    Some(OperationState::Running | OperationState::Aborting) => {
                        ErrorClass::InProgress
                    }
                    Some(OperationState::Quarantined) => ErrorClass::OperatorRequired,
                    Some(OperationState::Succeeded | OperationState::Failed) | None => {
                        ErrorClass::OutcomeUnknown
                    }
                }
            }
            Self::AppendCleanupUnresolved { .. } => ErrorClass::OutcomeUnknown,
            Self::RetryExhausted { last_error, .. } => last_error.class(),
        }
    }
}

/// What an owner's typed refusal means for a mutation. The owner marks a
/// refusal retryable only when it applied nothing, except for internal and
/// ownership failures, which a local-only owner can report after it applied
/// the request.
pub fn rpc_failure_class(failure: &RpcFailure) -> ErrorClass {
    match failure.code {
        ErrorCode::InvalidArgument => ErrorClass::InvalidInput,
        ErrorCode::NotFound => ErrorClass::NotFound,
        ErrorCode::AlreadyExists => ErrorClass::PreconditionFailed,
        ErrorCode::Conflict | ErrorCode::PreconditionFailed => {
            if failure.conflict == Some(ConflictKind::RootPlacement) {
                ErrorClass::OutcomeUnknown
            } else if failure.retryable {
                ErrorClass::Transient
            } else {
                ErrorClass::PreconditionFailed
            }
        }
        ErrorCode::RequestReplayMismatch => ErrorClass::IdentityConflict,
        ErrorCode::NotOwner | ErrorCode::ObjectUnavailable => ErrorClass::OutcomeUnknown,
        ErrorCode::SnapshotExpired | ErrorCode::SnapshotReaped => ErrorClass::NotFound,
        ErrorCode::CommitRetiring | ErrorCode::OperationFailed => ErrorClass::PreconditionFailed,
        ErrorCode::Quarantined | ErrorCode::ResourceExhausted => ErrorClass::OperatorRequired,
        ErrorCode::Internal => {
            if failure.retryable {
                ErrorClass::OutcomeUnknown
            } else {
                ErrorClass::Internal
            }
        }
    }
}

impl From<ProtocolError> for ClientError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<TransportError> for ClientError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<ObjectError> for ClientError {
    fn from(error: ObjectError) -> Self {
        Self::Object(error)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions(message) => write!(formatter, "invalid client options: {message}"),
            Self::InvalidRoute(message) => write!(formatter, "invalid root route: {message}"),
            Self::Transport(error) => write!(formatter, "RPC transport failed: {error}"),
            Self::Protocol(error) => write!(formatter, "RPC protocol failed: {error}"),
            Self::Rpc(error) => write!(formatter, "workspace request failed: {}", error.message),
            Self::ResponseMismatch(message) => {
                write!(formatter, "workspace response mismatch: {message}")
            }
            Self::MissingCapabilities(capabilities) => write!(
                formatter,
                "workspace owner is missing required capabilities: {capabilities:?}"
            ),
            Self::ArtifactIntegrity(message) => {
                write!(formatter, "artifact integrity check failed: {message}")
            }
            Self::ArtifactReadFenceChanged => {
                formatter.write_str("artifact metadata changed during a paged read")
            }
            Self::Object(error) => write!(formatter, "artifact object operation failed: {error}"),
            Self::ArtifactUpload(error) => write!(formatter, "{error}"),
            Self::ArtifactPublishFailed {
                stage,
                source,
                abort_failure,
            } => {
                write!(
                    formatter,
                    "artifact publication failed during {stage}: {source}"
                )?;
                if let Some(abort_failure) = abort_failure {
                    write!(
                        formatter,
                        "; durable abort could not be confirmed: {abort_failure}"
                    )?;
                }
                Ok(())
            }
            Self::RetryExhausted {
                attempts,
                last_error,
            } => write!(
                formatter,
                "workspace request exhausted {attempts} attempts: {last_error}"
            ),
            Self::AppendUnresolved {
                operation_id,
                state,
                source,
            } => write!(
                formatter,
                "append operation {operation_id:?} has state {state:?}: {source}; retain the same operation identity"
            ),
            Self::AppendCleanupUnresolved {
                operation_id,
                expected_token,
                source,
                ..
            } => write!(
                formatter,
                "append cleanup for {operation_id:?} is unresolved: {source}; retry with the same expected state digest {:?}",
                expected_token.state_digest
            ),
        }
    }
}

impl fmt::Display for ArtifactPublishStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Begin => "begin",
            Self::StageObjects => "stage objects",
            Self::Prepare => "prepare",
            Self::UploadObjects => "upload objects",
            Self::MarkObjectsUploaded => "mark objects uploaded",
            Self::StageManifest => "stage manifest",
            Self::Complete => "complete",
            Self::Finish => "finish",
            Self::PublishInline => "publish inline",
        })
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::Object(error) => Some(error),
            Self::ArtifactUpload(error) => Some(error.as_ref()),
            Self::ArtifactPublishFailed { source, .. } => Some(source.as_ref()),
            Self::AppendUnresolved { source, .. } => Some(source.as_ref()),
            Self::AppendCleanupUnresolved { source, .. } => Some(source.as_ref()),
            Self::RetryExhausted { last_error, .. } => Some(last_error.as_ref()),
            Self::InvalidOptions(_)
            | Self::InvalidRoute(_)
            | Self::Rpc(_)
            | Self::ResponseMismatch(_)
            | Self::MissingCapabilities(_)
            | Self::ArtifactIntegrity(_)
            | Self::ArtifactReadFenceChanged => None,
        }
    }
}
