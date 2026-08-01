/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use nokv_object::{ArtifactUploadFailure, ObjectError};
use nokv_protocol::{ErrorCode, ProtocolError, RpcFailure, WorkspaceCapability};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportError {
    message: String,
    retryable: bool,
}

impl TransportError {
    pub fn new(message: impl Into<String>, retryable: bool) -> Self {
        Self {
            message: message.into(),
            retryable,
        }
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
    UploadObjects,
    MarkObjectsUploaded,
    StageManifest,
    Complete,
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
            Self::RetryExhausted { .. }
            | Self::InvalidOptions(_)
            | Self::InvalidRoute(_)
            | Self::Protocol(_)
            | Self::ResponseMismatch(_)
            | Self::MissingCapabilities(_)
            | Self::ArtifactIntegrity(_)
            | Self::ArtifactReadFenceChanged
            | Self::Object(_)
            | Self::ArtifactUpload(_)
            | Self::ArtifactPublishFailed { .. } => false,
        }
    }

    pub(crate) fn rpc_failure(&self) -> Option<&RpcFailure> {
        match self {
            Self::Rpc(failure) => Some(failure),
            Self::ArtifactPublishFailed { source, .. } => source.rpc_failure(),
            Self::RetryExhausted { last_error, .. } => last_error.rpc_failure(),
            _ => None,
        }
    }

    pub(crate) fn rpc_code(&self) -> Option<ErrorCode> {
        self.rpc_failure().map(|failure| failure.code)
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
        }
    }
}

impl fmt::Display for ArtifactPublishStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Begin => "begin",
            Self::StageObjects => "stage objects",
            Self::UploadObjects => "upload objects",
            Self::MarkObjectsUploaded => "mark objects uploaded",
            Self::StageManifest => "stage manifest",
            Self::Complete => "complete",
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
