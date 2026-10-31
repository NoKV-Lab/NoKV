/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! The refusals a TLS owner makes, one JSON line each, appended to a file the
//! operator names. A request the owner served is attributed in its operation
//! record instead, so the two together say who did, and who tried, what.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::net::SocketAddr;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::{SystemTime, UNIX_EPOCH};

use nokv_protocol::WorkspaceRpcRequest;
use nokv_types::{CredentialFingerprint, TenantId};
use serde::Serialize;

use crate::access::{classify, AccessDenial, ConnectionPrincipal};
use crate::server::lowercase_hex;
use crate::ServerError;

/// An append-only file of refusals.
pub struct AuditLog {
    path: PathBuf,
    file: Mutex<File>,
    /// Set by a failed write, so a full disk is reported once and the next
    /// line starts on a line of its own.
    failing: AtomicBool,
}

impl AuditLog {
    /// Open `path` for appending, creating it readable by its owner only.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, ServerError> {
        let path = path.into();
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o600)
            .open(&path)
            .map_err(|error| {
                ServerError::InvalidOptions(format!(
                    "cannot open audit log {}: {error}",
                    path.display()
                ))
            })?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            failing: AtomicBool::new(false),
        })
    }

    /// Append one line. A write that fails is reported on stderr and does
    /// not stop the owner: a full disk must not turn every Agent away.
    pub(crate) fn record(&self, refusal: &Refusal<'_>) {
        let mut line = Vec::with_capacity(320);
        if self.failing.load(Ordering::Acquire) {
            // A failed write may have left part of a line.
            line.push(b'\n');
        }
        serde_json::to_writer(&mut line, refusal).expect("a refusal serializes to JSON");
        line.push(b'\n');
        let written = self
            .file
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .write_all(&line);
        match written {
            Ok(()) => self.failing.store(false, Ordering::Release),
            Err(error) => {
                if !self.failing.swap(true, Ordering::AcqRel) {
                    eprintln!(
                        "nokv-server: cannot append to audit log {}: {error}",
                        self.path.display()
                    );
                }
            }
        }
    }
}

/// What the owner refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RefusalEvent {
    /// A connection, before any request.
    ConnectionRefused,
    /// One request on an admitted connection.
    RequestDenied,
}

/// Why the owner refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RefusalReason {
    /// A plaintext client of an owner that requires TLS.
    TlsRequired,
    /// The TLS handshake failed, or did not finish by its deadline.
    TlsHandshakeFailed,
    /// The client speaks another protocol schema.
    IncompatibleSchema,
    /// The certificate is not, or is no longer, a current credential of the
    /// tenant.
    NotACurrentCredential,
    /// The owner's copy of the tenant policy is too old to decide on.
    PolicyStale,
    /// The principal holds no role on the root that permits the request.
    PermissionDenied,
}

impl From<AccessDenial> for RefusalReason {
    fn from(denial: AccessDenial) -> Self {
        match denial {
            AccessDenial::NotACurrentCredential => Self::NotACurrentCredential,
            AccessDenial::PermissionDenied { .. } => Self::PermissionDenied,
            AccessDenial::PolicyStale => Self::PolicyStale,
        }
    }
}

/// One line of the audit log. Every key is present; one that does not apply
/// to the refusal is null.
#[derive(Debug, Serialize)]
pub(crate) struct Refusal<'a> {
    time_unix_ms: u64,
    event: RefusalEvent,
    reason: RefusalReason,
    peer: Option<String>,
    tenant_id: String,
    certificate_fingerprint: Option<String>,
    principal_id: Option<String>,
    root_id: Option<String>,
    request: Option<&'static str>,
    required_role: Option<&'static str>,
    detail: Option<&'a str>,
}

impl<'a> Refusal<'a> {
    /// A connection turned away before any request.
    pub(crate) fn connection(
        reason: RefusalReason,
        peer: Option<SocketAddr>,
        tenant_id: TenantId,
    ) -> Self {
        Self {
            time_unix_ms: unix_now_millis(),
            event: RefusalEvent::ConnectionRefused,
            reason,
            peer: peer.map(|peer| peer.to_string()),
            tenant_id: lowercase_hex(tenant_id.as_bytes()),
            certificate_fingerprint: None,
            principal_id: None,
            root_id: None,
            request: None,
            required_role: None,
            detail: None,
        }
    }

    /// A request the connection's principal may not make.
    pub(crate) fn request(
        denial: AccessDenial,
        peer: Option<SocketAddr>,
        tenant_id: TenantId,
        principal: &ConnectionPrincipal,
        request: &WorkspaceRpcRequest,
    ) -> Self {
        Self {
            event: RefusalEvent::RequestDenied,
            principal_id: Some(lowercase_hex(principal.principal_id.as_bytes())),
            root_id: Some(lowercase_hex(&request.route.root_id.0)),
            request: Some(classify(&request.operation).name),
            required_role: match denial {
                AccessDenial::PermissionDenied { required } => Some(required.name()),
                AccessDenial::NotACurrentCredential | AccessDenial::PolicyStale => None,
            },
            ..Self::connection(denial.into(), peer, tenant_id)
        }
        .with_certificate(&principal.fingerprint)
    }

    pub(crate) fn with_certificate(self, fingerprint: &CredentialFingerprint) -> Self {
        Self {
            certificate_fingerprint: Some(lowercase_hex(fingerprint.as_bytes())),
            ..self
        }
    }

    pub(crate) fn with_detail(self, detail: &'a str) -> Self {
        Self {
            detail: Some(detail),
            ..self
        }
    }
}

fn unix_now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_line_is_one_json_object_with_every_key() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("audit.jsonl");
        let log = AuditLog::open(&path).unwrap();
        let tenant = TenantId::from_bytes([5; 16]);
        let peer = Some("127.0.0.1:4000".parse().unwrap());
        log.record(
            &Refusal::connection(RefusalReason::TlsHandshakeFailed, peer, tenant)
                .with_detail("peer sent no certificates"),
        );
        log.record(
            &Refusal::connection(RefusalReason::NotACurrentCredential, peer, tenant)
                .with_certificate(&CredentialFingerprint::from_bytes([9; 32])),
        );
        drop(log);
        // Reopening appends.
        AuditLog::open(&path).unwrap().record(&Refusal::connection(
            RefusalReason::TlsRequired,
            None,
            tenant,
        ));

        let text = std::fs::read_to_string(&path).unwrap();
        let lines = text
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 3, "{text}");
        for line in &lines {
            let mut keys = line
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<Vec<_>>();
            keys.sort();
            assert_eq!(
                keys,
                [
                    "certificate_fingerprint",
                    "detail",
                    "event",
                    "peer",
                    "principal_id",
                    "reason",
                    "request",
                    "required_role",
                    "root_id",
                    "tenant_id",
                    "time_unix_ms",
                ]
            );
            assert_eq!(line["event"], "connection_refused");
            assert_eq!(line["tenant_id"], "05".repeat(16));
            assert!(line["time_unix_ms"].as_u64().unwrap() > 1_700_000_000_000);
        }
        assert_eq!(lines[0]["reason"], "tls_handshake_failed");
        assert_eq!(lines[0]["detail"], "peer sent no certificates");
        assert_eq!(lines[0]["peer"], "127.0.0.1:4000");
        assert_eq!(lines[1]["reason"], "not_a_current_credential");
        assert_eq!(lines[1]["certificate_fingerprint"], "09".repeat(32));
        assert_eq!(lines[2]["reason"], "tls_required");
        assert_eq!(lines[2]["peer"], serde_json::Value::Null);

        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0);
    }
}
