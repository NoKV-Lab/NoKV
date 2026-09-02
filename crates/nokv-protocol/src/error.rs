/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::types::DiscoveredRoute;

/// Failure to encode, decode, or validate one protocol frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    FrameTooLarge {
        bytes: usize,
        max: usize,
    },
    SchemaMismatch {
        actual: String,
        expected: &'static str,
    },
    InvalidField {
        field: &'static str,
        reason: String,
    },
    Encode(String),
    Decode(String),
}

impl ProtocolError {
    pub(crate) fn invalid(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidField {
            field,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrameTooLarge { bytes, max } => {
                write!(formatter, "RPC frame is {bytes} bytes, maximum is {max}")
            }
            Self::SchemaMismatch { actual, expected } => {
                write!(
                    formatter,
                    "RPC schema {actual:?} does not match {expected:?}"
                )
            }
            Self::InvalidField { field, reason } => {
                write!(formatter, "invalid RPC field {field}: {reason}")
            }
            Self::Encode(message) => write!(formatter, "RPC encode failed: {message}"),
            Self::Decode(message) => write!(formatter, "RPC decode failed: {message}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

/// Stable domain classification returned by a shard owner.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidArgument,
    NotFound,
    AlreadyExists,
    Conflict,
    PreconditionFailed,
    RequestReplayMismatch,
    NotOwner,
    RouteUnavailable,
    RouteExpired,
    SnapshotExpired,
    SnapshotReaped,
    CommitRetiring,
    OperationFailed,
    ObjectUnavailable,
    Quarantined,
    ResourceExhausted,
    Internal,
}

/// Optional stable conflict detail for conditional operations.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConflictKind {
    Workspace,
    PathGeneration,
    CommitHead,
    ReadVersion,
    SnapshotLifecycle,
    OperationState,
    RootPlacement,
}

/// Storage-neutral error body. Internal engine keys and provider credentials are
/// never serialized.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RpcFailure {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    pub conflict: Option<ConflictKind>,
    pub current_generation: Option<u64>,
    pub route_hint: Option<Box<DiscoveredRoute>>,
}

impl RpcFailure {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.message.is_empty() {
            return Err(ProtocolError::invalid(
                "failure.message",
                "must not be empty",
            ));
        }
        if self.message.len() > 4096 {
            return Err(ProtocolError::invalid(
                "failure.message",
                "exceeds 4096 bytes",
            ));
        }
        if self.message.contains('\0') {
            return Err(ProtocolError::invalid("failure.message", "contains NUL"));
        }
        if let Some(route) = &self.route_hint {
            route.validate()?;
        }
        Ok(())
    }
}
