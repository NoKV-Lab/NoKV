/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;

#[derive(Debug)]
pub enum ServerError {
    InvalidOptions(String),
    InvalidRoute(String),
    InvalidBootstrap(String),
    RouteRollback(String),
    Control(nokv_control::ControlError),
    Metadata(nokv_meta::workspace::AgentMetadataError),
    OwnerReleaseReceipt(crate::OwnerReleaseReceiptError),
    OwnerReleasePending {
        primary: Option<String>,
        lease: Box<nokv_control::LogicalShardLease>,
    },
    OwnerReleaseRetryable {
        primary: Option<String>,
        lease: Box<nokv_control::LogicalShardLease>,
        error: Box<nokv_control::ControlError>,
    },
    BootstrapOwnerReleasePending {
        primary: String,
        rollback: Option<String>,
        pending: Box<crate::PendingOwnerRelease>,
    },
    BootstrapOwnerReleaseRetryable {
        primary: String,
        rollback: Option<String>,
        pending: Box<crate::PendingOwnerRelease>,
        error: Box<nokv_control::ControlError>,
    },
    BootstrapOwnerReleaseReceiptRejected {
        primary: String,
        rollback: Option<String>,
        pending: Box<crate::PendingOwnerRelease>,
        error: crate::OwnerReleaseReceiptError,
    },
    BootstrapRollback {
        primary: String,
        rollback: String,
    },
    Protocol(nokv_protocol::ProtocolError),
    Bind(std::io::Error),
    Connection(std::io::Error),
    FrameTooLarge {
        bytes: usize,
        max: usize,
    },
    Executor(String),
}

impl From<nokv_protocol::ProtocolError> for ServerError {
    fn from(error: nokv_protocol::ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOptions(message) => write!(formatter, "invalid server options: {message}"),
            Self::InvalidRoute(message) => write!(formatter, "invalid root route: {message}"),
            Self::InvalidBootstrap(message) => {
                write!(formatter, "invalid root-owner bootstrap: {message}")
            }
            Self::RouteRollback(message) => write!(formatter, "root route rollback: {message}"),
            Self::Control(error) => write!(formatter, "control plane failed: {error}"),
            Self::Metadata(error) => write!(formatter, "metadata store failed: {error}"),
            Self::OwnerReleaseReceipt(message) => {
                write!(formatter, "owner release receipt failed: {message}")
            }
            Self::OwnerReleasePending { primary, lease } => {
                if let Some(primary) = primary {
                    write!(formatter, "owner failed: {primary}; ")?;
                }
                write!(
                    formatter,
                    "exact owner release outcome is unknown for shard {:?}, owner epoch {}, lease {}; retry release only",
                    lease.logical_shard_id, lease.owner_epoch, lease.lease_id
                )
            }
            Self::OwnerReleaseRetryable {
                primary,
                lease,
                error,
            } => {
                if let Some(primary) = primary {
                    write!(formatter, "owner failed: {primary}; ")?;
                }
                write!(
                    formatter,
                    "exact owner release for shard {:?}, owner epoch {}, lease {} failed before a terminal outcome: {error}; retry release only",
                    lease.logical_shard_id, lease.owner_epoch, lease.lease_id
                )
            }
            Self::BootstrapOwnerReleasePending {
                primary,
                rollback,
                pending,
            } => {
                write!(
                    formatter,
                    "root-owner bootstrap failed: {primary}; exact owner release outcome is unknown for {:?}",
                    pending.lease()
                )?;
                if let Some(rollback) = rollback {
                    write!(formatter, "; local rollback also failed: {rollback}")?;
                }
                write!(formatter, "; retry the retained release capability only")
            }
            Self::BootstrapOwnerReleaseRetryable {
                primary,
                rollback,
                pending,
                error,
            } => {
                write!(
                    formatter,
                    "root-owner bootstrap failed: {primary}; exact owner release for {:?} failed before mutation: {error}",
                    pending.lease()
                )?;
                if let Some(rollback) = rollback {
                    write!(formatter, "; local rollback also failed: {rollback}")?;
                }
                write!(formatter, "; retry the retained release capability only")
            }
            Self::BootstrapOwnerReleaseReceiptRejected {
                primary,
                rollback,
                pending,
                error,
            } => {
                write!(
                    formatter,
                    "root-owner bootstrap failed: {primary}; durable release-only receipt for {:?} was rejected: {error}",
                    pending.lease()
                )?;
                if let Some(rollback) = rollback {
                    write!(formatter, "; local rollback also failed: {rollback}")?;
                }
                write!(formatter, "; retry the retained release capability only")
            }
            Self::BootstrapRollback { primary, rollback } => write!(
                formatter,
                "root-owner bootstrap failed: {primary}; rollback also failed: {rollback}"
            ),
            Self::Protocol(error) => write!(formatter, "workspace protocol failed: {error}"),
            Self::Bind(error) => write!(formatter, "server bind failed: {error}"),
            Self::Connection(error) => write!(formatter, "server connection failed: {error}"),
            Self::FrameTooLarge { bytes, max } => {
                write!(formatter, "RPC frame is {bytes} bytes, maximum is {max}")
            }
            Self::Executor(message) => write!(formatter, "workspace executor failed: {message}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<nokv_control::ControlError> for ServerError {
    fn from(error: nokv_control::ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<nokv_meta::workspace::AgentMetadataError> for ServerError {
    fn from(error: nokv_meta::workspace::AgentMetadataError) -> Self {
        Self::Metadata(error)
    }
}
