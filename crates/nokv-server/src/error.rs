/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::path::PathBuf;

#[derive(Debug)]
pub enum ServerError {
    InvalidOptions(String),
    InvalidRoute(String),
    InvalidBootstrap(String),
    RouteRollback(String),
    Control(nokv_control::ControlError),
    PreparedOwnerAdmission {
        path: PathBuf,
        source: nokv_control::ControlError,
    },
    Store(nokv_meta_store::StoreError),
    Meta(nokv_meta::workspace::MetaError),
    RecoveryInstallation(crate::RecoveryInstallerError),
    RecoveryPublication(crate::RecoveryPublisherError),
    RecoveryPath {
        path: PathBuf,
        source: std::io::Error,
    },
    BootstrapRollback {
        primary: String,
        rollback: String,
    },
    Protocol(nokv_protocol::ProtocolError),
    Handshake(nokv_protocol::HandshakeError),
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
                write!(formatter, "invalid logical-shard bootstrap: {message}")
            }
            Self::RouteRollback(message) => write!(formatter, "root route rollback: {message}"),
            Self::Control(error) => write!(formatter, "control plane failed: {error}"),
            Self::PreparedOwnerAdmission { path, source } => {
                if matches!(source, nokv_control::ControlError::Backend(_)) {
                    write!(
                        formatter,
                        "control-plane owner acquisition outcome is unknown after preparing the \
                         epoch-zero metadata store at {}: {source}; preserve the store and \
                         retry with --metadata-reopen {} after the prior owner session settles; \
                         startup will acquire epoch one if the transaction did not apply, or \
                         rebind the durable Recovering epoch one if it did; do not delete the \
                         prepared store while acquisition may have succeeded",
                        path.display(),
                        path.display()
                    )
                } else {
                    write!(
                        formatter,
                        "control plane failed after preparing the epoch-zero metadata store at \
                         {}: {source}; retry the corrected first-owner command with \
                         --metadata-reopen {}",
                        path.display(),
                        path.display()
                    )
                }
            }
            Self::Store(error) => write!(formatter, "metadata store failed: {error}"),
            Self::Meta(error) => write!(formatter, "metadata failed: {error}"),
            Self::RecoveryInstallation(error) => {
                write!(formatter, "recovery installation failed: {error}")
            }
            Self::RecoveryPublication(error) => {
                write!(formatter, "recovery publication failed: {error}")
            }
            Self::RecoveryPath { path, source } => write!(
                formatter,
                "recovery metadata path {} cannot be inspected: {source}",
                path.display()
            ),
            Self::BootstrapRollback { primary, rollback } => write!(
                formatter,
                "logical-shard ownership failed: {primary}; cleanup also failed: {rollback}"
            ),
            Self::Protocol(error) => write!(formatter, "workspace protocol failed: {error}"),
            Self::Handshake(error) => write!(formatter, "workspace handshake failed: {error}"),
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

impl From<nokv_meta_store::StoreError> for ServerError {
    fn from(error: nokv_meta_store::StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<nokv_meta::workspace::MetaError> for ServerError {
    fn from(error: nokv_meta::workspace::MetaError) -> Self {
        Self::Meta(error)
    }
}

impl From<crate::RecoveryInstallerError> for ServerError {
    fn from(error: crate::RecoveryInstallerError) -> Self {
        Self::RecoveryInstallation(error)
    }
}

impl From<crate::RecoveryPublisherError> for ServerError {
    fn from(error: crate::RecoveryPublisherError) -> Self {
        Self::RecoveryPublication(error)
    }
}

impl From<nokv_protocol::HandshakeError> for ServerError {
    fn from(error: nokv_protocol::HandshakeError) -> Self {
        Self::Handshake(error)
    }
}
