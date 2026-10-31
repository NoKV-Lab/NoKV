/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! TLS 1.3 for the workspace RPC listener.
//!
//! A client presents a certificate, and TLS proves that it holds the matching
//! private key. Whether that certificate authenticates a principal is decided
//! by the tenant's credential records (see `access`); no certificate
//! authority decides which client certificates to trust.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::audit::Refusal;
use crate::{AuditLog, ServerError, TenantAccess};
use rustls::client::danger::HandshakeSignatureValid;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::NoServerSessionStorage;
use rustls::{
    DigitallySignedStruct, DistinguishedName, ServerConfig, ServerConnection, SignatureScheme,
    StreamOwned,
};

/// The TLS identity of one RPC listener and the tenant access its clients are
/// checked against. A listener with this configuration never falls back to
/// plaintext.
pub struct RpcServerTls {
    pub(crate) config: Arc<ServerConfig>,
    pub(crate) access: TenantAccess,
    audit: Option<AuditLog>,
}

impl RpcServerTls {
    pub fn new(
        server_chain: Vec<CertificateDer<'static>>,
        server_key: PrivateKeyDer<'static>,
        access: TenantAccess,
    ) -> Result<Self, ServerError> {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = Arc::new(CredentialCertificateVerifier {
            algorithms: provider.signature_verification_algorithms,
        });
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(tls_configuration_error)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(server_chain, server_key)
            .map_err(tls_configuration_error)?;
        // Every connection proves possession of its certificate again: no
        // tickets, no session cache, and no early data.
        config.session_storage = Arc::new(NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        config.max_early_data_size = 0;
        Ok(Self {
            config: Arc::new(config),
            access,
            audit: None,
        })
    }

    /// Append every refused connection and denied request to `audit`.
    pub fn with_audit_log(self, audit: AuditLog) -> Self {
        Self {
            audit: Some(audit),
            ..self
        }
    }

    /// Record `refusal` when this listener keeps an audit log.
    pub(crate) fn audit(&self, refusal: &Refusal<'_>) {
        if let Some(audit) = &self.audit {
            audit.record(refusal);
        }
    }
}

fn tls_configuration_error(error: rustls::Error) -> ServerError {
    ServerError::TlsConfiguration(error.to_string())
}

/// Requires a client certificate and checks only that the client holds its
/// private key. Whether the certificate is trusted is decided after the TLS
/// handshake, from the tenant's credential records, so that an untrusted
/// certificate gets a typed refusal rather than a TLS alert.
#[derive(Debug)]
struct CredentialCertificateVerifier {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for CredentialCertificateVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, certificate, signature, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, certificate, signature, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// One admitted connection: plaintext, or TLS over the same socket.
pub(crate) enum RpcStream {
    Plain(DeadlineSocket),
    Tls(Box<StreamOwned<ServerConnection, DeadlineSocket>>),
}

impl RpcStream {
    /// End the admission deadline; later reads and writes are bounded by the
    /// per-call socket timeouts instead.
    pub(crate) fn finish_admission(
        &mut self,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> io::Result<()> {
        let socket = match self {
            Self::Plain(socket) => socket,
            Self::Tls(tls) => &mut tls.sock,
        };
        socket.deadline = None;
        socket.socket.set_read_timeout(Some(read_timeout))?;
        socket.socket.set_write_timeout(Some(write_timeout))
    }
}

impl Read for RpcStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(socket) => socket.read(buffer),
            Self::Tls(tls) => tls.read(buffer),
        }
    }
}

impl Write for RpcStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(socket) => socket.write(buffer),
            Self::Tls(tls) => tls.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(socket) => socket.flush(),
            Self::Tls(tls) => tls.flush(),
        }
    }
}

/// A socket whose reads and writes share one absolute deadline while it is
/// set. The deadline lives below rustls: one TLS read can make several socket
/// reads, and a timeout per read would let a peer that trickles bytes hold an
/// admission slot indefinitely.
pub(crate) struct DeadlineSocket {
    pub(crate) socket: TcpStream,
    pub(crate) deadline: Option<Instant>,
}

impl DeadlineSocket {
    /// The first byte the peer sends, without consuming it; `None` when the
    /// peer closed first.
    pub(crate) fn peek_first_byte(&mut self) -> io::Result<Option<u8>> {
        let mut first = [0_u8; 1];
        loop {
            if let Some(deadline) = self.deadline {
                self.socket.set_read_timeout(Some(remaining(deadline)?))?;
            }
            match self.socket.peek(&mut first) {
                Ok(0) => return Ok(None),
                Ok(_) => return Ok(Some(first[0])),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl Read for DeadlineSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if let Some(deadline) = self.deadline {
            self.socket.set_read_timeout(Some(remaining(deadline)?))?;
        }
        self.socket.read(buffer)
    }
}

impl Write for DeadlineSocket {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if let Some(deadline) = self.deadline {
            self.socket.set_write_timeout(Some(remaining(deadline)?))?;
        }
        self.socket.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.socket.flush()
    }
}

pub(crate) fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "workspace RPC admission deadline expired",
            )
        })
}

/// Whether a first byte opens a TLS record: a content type in 20..=23. A
/// plaintext frame starts with the high byte of its length, which is zero for
/// every frame this protocol allows.
pub(crate) fn opens_tls_record(first: u8) -> bool {
    (20..=23).contains(&first)
}

/// Run the TLS handshake under the socket's deadline and return the stream
/// with the client's certificate.
pub(crate) fn accept_tls(
    mut socket: DeadlineSocket,
    tls: &RpcServerTls,
) -> Result<
    (
        StreamOwned<ServerConnection, DeadlineSocket>,
        CertificateDer<'static>,
    ),
    ServerError,
> {
    let mut connection =
        ServerConnection::new(Arc::clone(&tls.config)).map_err(tls_configuration_error)?;
    while connection.is_handshaking() {
        connection
            .complete_io(&mut socket)
            .map_err(ServerError::Connection)?;
    }
    let certificate = connection
        .peer_certificates()
        .and_then(|chain| chain.first())
        .cloned()
        .ok_or_else(|| {
            ServerError::TlsConfiguration("a TLS client presented no certificate".to_owned())
        })?;
    Ok((StreamOwned::new(connection, socket), certificate))
}

/// Read and discard the client's first TLS record, so that closing the socket
/// after a plaintext answer does not reset the connection before the client
/// reads that answer.
pub(crate) fn drain_tls_record(socket: &mut DeadlineSocket) -> io::Result<()> {
    let mut header = [0_u8; 5];
    socket.read_exact(&mut header)?;
    let mut record = vec![0_u8; usize::from(u16::from_be_bytes([header[3], header[4]]))];
    socket.read_exact(&mut record)
}

/// Certificates for TLS tests: an owner certificate signed by a test CA, and
/// client certificates of tenant 5. `registered` is a tenant-wide writer,
/// `reader` a tenant-wide reader, and `unregistered` no credential at all.
#[cfg(test)]
pub(crate) mod test_pki {
    use crate::access::DEFAULT_MAX_POLICY_AGE;
    use std::sync::Arc;

    use nokv_control::{
        CredentialBinding, IdentityStore, InMemoryControlStore, PrincipalRecord, RoleBinding,
        TenantRecord,
    };
    use nokv_types::{CredentialFingerprint, PrincipalId, Role, RoleScope, TenantId};
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use rustls::{ClientConfig, RootCertStore};
    use sha2::{Digest as _, Sha256};

    use super::RpcServerTls;
    use crate::TenantAccess;

    pub(crate) const TENANT: TenantId = TenantId::from_bytes([5; 16]);

    pub(crate) struct TestCertificate {
        der: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
    }

    impl TestCertificate {
        fn self_signed() -> Self {
            let key = KeyPair::generate().unwrap();
            let certificate = CertificateParams::new(vec!["agent".to_owned()])
                .unwrap()
                .self_signed(&key)
                .unwrap();
            Self {
                der: certificate.der().clone(),
                key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            }
        }

        pub(crate) fn fingerprint(&self) -> CredentialFingerprint {
            CredentialFingerprint::from_bytes(Sha256::digest(&self.der).into())
        }
    }

    pub(crate) struct TestPki {
        ca: CertifiedIssuer<'static, KeyPair>,
        server: TestCertificate,
        pub(crate) registered: TestCertificate,
        pub(crate) reader: TestCertificate,
        pub(crate) unregistered: TestCertificate,
    }

    pub(crate) fn server_name() -> ServerName<'static> {
        ServerName::try_from("owner.test").unwrap()
    }

    impl TestPki {
        pub(crate) fn new() -> Self {
            let mut params = CertificateParams::default();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
            let server_key = KeyPair::generate().unwrap();
            let server = CertificateParams::new(vec!["owner.test".to_owned()])
                .unwrap()
                .signed_by(&server_key, &ca)
                .unwrap();
            Self {
                server: TestCertificate {
                    der: server.der().clone(),
                    key: PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
                },
                registered: TestCertificate::self_signed(),
                reader: TestCertificate::self_signed(),
                unregistered: TestCertificate::self_signed(),
                ca,
            }
        }

        /// The owner's TLS over tenant 5's records.
        pub(crate) fn server_tls(&self) -> RpcServerTls {
            self.server_tls_with_store().0
        }

        /// The owner's TLS and the store its tenant access reads.
        pub(crate) fn server_tls_with_store(&self) -> (RpcServerTls, Arc<InMemoryControlStore>) {
            let store = Arc::new(InMemoryControlStore::new());
            store
                .create_tenant(TenantRecord { tenant_id: TENANT })
                .unwrap();
            for (fill, certificate, role) in [
                (6, &self.registered, Role::Writer),
                (7, &self.reader, Role::Reader),
            ] {
                let principal_id = PrincipalId::from_bytes([fill; 16]);
                store
                    .create_principal(PrincipalRecord {
                        tenant_id: TENANT,
                        principal_id,
                    })
                    .unwrap();
                store
                    .add_credential(CredentialBinding {
                        tenant_id: TENANT,
                        principal_id,
                        fingerprint: certificate.fingerprint(),
                        expires_at_unix_seconds: u64::MAX,
                    })
                    .unwrap();
                store
                    .grant_role(RoleBinding {
                        tenant_id: TENANT,
                        principal_id,
                        role,
                        scope: RoleScope::Tenant,
                    })
                    .unwrap();
            }
            let tls = RpcServerTls::new(
                vec![self.server.der.clone()],
                self.server.key.clone_key(),
                TenantAccess::load(store.clone(), TENANT, DEFAULT_MAX_POLICY_AGE).unwrap(),
            )
            .unwrap();
            (tls, store)
        }

        /// A client that trusts the test CA and presents `certificate`.
        pub(crate) fn client_config(
            &self,
            certificate: Option<&TestCertificate>,
        ) -> Arc<ClientConfig> {
            let mut roots = RootCertStore::empty();
            roots.add(self.ca.der().clone()).unwrap();
            let builder = ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots);
            Arc::new(match certificate {
                Some(certificate) => builder
                    .with_client_auth_cert(
                        vec![certificate.der.clone()],
                        certificate.key.clone_key(),
                    )
                    .unwrap(),
                None => builder.with_no_client_auth(),
            })
        }
    }
}
