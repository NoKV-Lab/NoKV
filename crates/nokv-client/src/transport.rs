/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nokv_protocol::{
    decode_handshake_frame, encode_handshake_frame, HandshakeKind, WorkspaceHandshake,
    HANDSHAKE_FRAME_BYTES, MAX_FRAME_BYTES, WORKSPACE_PROTOCOL_SCHEMA,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

use crate::TransportError;

/// Byte transport for one complete request/response exchange.
pub trait RpcTransport: Send + Sync {
    fn round_trip(&self, endpoint: SocketAddr, request: &[u8]) -> Result<Vec<u8>, TransportError>;
}

impl<T> RpcTransport for Arc<T>
where
    T: RpcTransport + ?Sized,
{
    fn round_trip(&self, endpoint: SocketAddr, request: &[u8]) -> Result<Vec<u8>, TransportError> {
        (**self).round_trip(endpoint, request)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FramedTcpOptions {
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Default for FramedTcpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
        }
    }
}

/// TLS 1.3 settings for workspace RPC: the owner name and roots this client
/// trusts, and the certificate that authenticates its principal.
///
/// The server name is fixed here and never taken from a route. Routes name
/// socket addresses, and whoever can change routing must not be able to
/// change which certificate the client accepts.
#[derive(Clone)]
pub struct RpcClientTls {
    config: Arc<ClientConfig>,
    server_name: ServerName<'static>,
}

impl fmt::Debug for RpcClientTls {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcClientTls")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl RpcClientTls {
    pub fn new(
        server_name: ServerName<'static>,
        server_roots: Vec<CertificateDer<'static>>,
        client_chain: Vec<CertificateDer<'static>>,
        client_key: PrivateKeyDer<'static>,
    ) -> Result<Self, TransportError> {
        let mut roots = RootCertStore::empty();
        for root in server_roots {
            roots.add(root).map_err(tls_configuration_error)?;
        }
        if roots.is_empty() || client_chain.is_empty() {
            return Err(TransportError::security(
                "RPC TLS needs a trusted server root and a client certificate",
            ));
        }
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .map_err(tls_configuration_error)?
                .with_root_certificates(roots)
                .with_client_auth_cert(client_chain, client_key)
                .map_err(tls_configuration_error)?;
        // Every connection presents the certificate again, so the owner
        // authenticates each one: no resumption and no early data.
        config.resumption = rustls::client::Resumption::disabled();
        config.enable_early_data = false;
        Ok(Self {
            config: Arc::new(config),
            server_name,
        })
    }
}

fn tls_configuration_error(error: rustls::Error) -> TransportError {
    TransportError::security(format!("invalid RPC TLS configuration: {error}"))
}

/// Idle admitted connections kept for reuse per endpoint.
const MAX_IDLE_CONNECTIONS_PER_ENDPOINT: usize = 8;
/// Reuse a connection only while it is well inside the server's idle read
/// timeout; older ones are closed instead of risking a stale exchange.
const MAX_IDLE_CONNECTION_AGE: Duration = Duration::from_secs(10);

/// Framed request/response transport over admitted TCP connections.
///
/// Every exchange is one complete request/response on one connection. A
/// connection that completes an exchange returns to a bounded idle pool shared
/// by clones of this transport, so later requests skip the TCP connect and
/// protocol handshake. A pooled connection that fails before any response byte
/// arrives was closed by its peer; the request is resent once on a fresh
/// connection. Workspace requests carry their own request identity, so that
/// resend has the same replay semantics as any client retry.
///
/// A transport authenticates as one principal. Its TLS settings are fixed when
/// it is built and its clones share its pool, so a pooled connection is never
/// reused under another identity.
#[derive(Clone, Debug)]
pub struct FramedTcpTransport {
    options: FramedTcpOptions,
    tls: Option<RpcClientTls>,
    idle: Arc<Mutex<HashMap<SocketAddr, Vec<IdleConnection>>>>,
}

#[derive(Debug)]
struct IdleConnection {
    stream: RpcStream,
    idle_since: Instant,
}

/// Exchange failure, split by whether any response byte arrived.
#[derive(Debug)]
enum ExchangeError {
    BeforeResponse(TransportError),
    AfterResponse(TransportError),
}

impl FramedTcpTransport {
    pub fn new(options: FramedTcpOptions) -> Result<Self, TransportError> {
        if options.connect_timeout.is_zero()
            || options.handshake_timeout.is_zero()
            || options.read_timeout.is_zero()
            || options.write_timeout.is_zero()
        {
            return Err(TransportError::new(
                "TCP timeouts must be greater than zero",
                false,
            ));
        }
        Ok(Self {
            options,
            tls: None,
            idle: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Require TLS on every connection, with no plaintext fallback. The
    /// transport starts a new pool, so no connection admitted under other
    /// settings is reused.
    pub fn with_tls(self, tls: RpcClientTls) -> Self {
        Self {
            options: self.options,
            tls: Some(tls),
            idle: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Opens one connection and completes the TLS and exact-schema
    /// handshakes under one deadline. Nagle's algorithm is disabled before the
    /// first byte is written, so no frame waits for the server's delayed ACK
    /// of an earlier segment.
    fn connect(&self, endpoint: SocketAddr) -> Result<RpcStream, TransportError> {
        let socket = TcpStream::connect_timeout(&endpoint, self.options.connect_timeout)
            .map_err(io_error)?;
        socket.set_nodelay(true).map_err(io_error)?;
        let deadline = Instant::now()
            .checked_add(self.options.handshake_timeout)
            .ok_or_else(|| TransportError::new("TCP handshake deadline overflows", false))?;
        let socket = DeadlineSocket {
            socket,
            deadline: Some(deadline),
        };
        let mut stream = match &self.tls {
            Some(tls) => RpcStream::Tls(Box::new(start_tls(socket, tls)?)),
            None => RpcStream::Plain(socket),
        };
        perform_handshake(&mut stream)?;
        stream
            .finish_admission(self.options.read_timeout, self.options.write_timeout)
            .map_err(io_error)?;
        Ok(stream)
    }

    /// Take the most recently returned live connection for `endpoint`.
    fn take_idle(&self, endpoint: SocketAddr) -> Option<RpcStream> {
        let mut idle = self.idle.lock().ok()?;
        let connections = idle.get_mut(&endpoint)?;
        while let Some(mut connection) = connections.pop() {
            if connection.idle_since.elapsed() < MAX_IDLE_CONNECTION_AGE
                && connection.stream.is_quiet()
            {
                return Some(connection.stream);
            }
        }
        None
    }

    fn return_idle(&self, endpoint: SocketAddr, stream: RpcStream) {
        if let Ok(mut idle) = self.idle.lock() {
            let connections = idle.entry(endpoint).or_default();
            if connections.len() < MAX_IDLE_CONNECTIONS_PER_ENDPOINT {
                connections.push(IdleConnection {
                    stream,
                    idle_since: Instant::now(),
                });
            }
        }
    }

    /// Close every pooled connection for `endpoint` after one proved stale.
    fn discard_idle(&self, endpoint: SocketAddr) {
        if let Ok(mut idle) = self.idle.lock() {
            idle.remove(&endpoint);
        }
    }
}

impl RpcTransport for FramedTcpTransport {
    fn round_trip(&self, endpoint: SocketAddr, request: &[u8]) -> Result<Vec<u8>, TransportError> {
        let frame = length_prefixed_frame(request)?;
        if let Some(mut stream) = self.take_idle(endpoint) {
            match exchange_frame(&mut stream, &frame) {
                Ok(response) => {
                    self.return_idle(endpoint, stream);
                    return Ok(response);
                }
                Err(ExchangeError::AfterResponse(error)) => return Err(error),
                // The peer closed this pooled connection; so are its siblings.
                Err(ExchangeError::BeforeResponse(_)) => self.discard_idle(endpoint),
            }
        }
        let mut stream = self.connect(endpoint)?;
        match exchange_frame(&mut stream, &frame) {
            Ok(response) => {
                self.return_idle(endpoint, stream);
                Ok(response)
            }
            Err(ExchangeError::BeforeResponse(error) | ExchangeError::AfterResponse(error)) => {
                Err(error)
            }
        }
    }
}

/// One admitted connection: plaintext, or TLS over the same socket.
#[derive(Debug)]
enum RpcStream {
    Plain(DeadlineSocket),
    Tls(Box<StreamOwned<ClientConnection, DeadlineSocket>>),
}

impl RpcStream {
    fn socket(&mut self) -> &mut DeadlineSocket {
        match self {
            Self::Plain(socket) => socket,
            Self::Tls(tls) => &mut tls.sock,
        }
    }

    /// End the handshake deadline; later reads and writes are bounded by the
    /// per-call socket timeouts instead.
    fn finish_admission(
        &mut self,
        read_timeout: Duration,
        write_timeout: Duration,
    ) -> io::Result<()> {
        let socket = self.socket();
        socket.deadline = None;
        socket.socket.set_read_timeout(Some(read_timeout))?;
        socket.socket.set_write_timeout(Some(write_timeout))
    }

    /// A reusable connection has nothing to read: unread bytes, end of stream,
    /// or buffered TLS data mean the peer closed or desynchronized it. An
    /// owner sends nothing between exchanges, not even TLS session tickets.
    fn is_quiet(&mut self) -> bool {
        if let Self::Tls(tls) = self {
            match tls.conn.process_new_packets() {
                Ok(state) if state.plaintext_bytes_to_read() == 0 && !state.peer_has_closed() => {}
                _ => return false,
            }
        }
        socket_is_quiet(&self.socket().socket)
    }
}

fn socket_is_quiet(stream: &TcpStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return false;
    }
    let mut probe = [0_u8; 1];
    let quiet = matches!(
        stream.peek(&mut probe),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    );
    stream.set_nonblocking(false).is_ok() && quiet
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
/// reads, and a timeout per read would let a peer that trickles bytes extend
/// the handshake indefinitely.
#[derive(Debug)]
struct DeadlineSocket {
    socket: TcpStream,
    deadline: Option<Instant>,
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

/// Run the TLS handshake. An owner without TLS answers the TLS client hello
/// with a plaintext handshake frame, which is read here before rustls would
/// reject it as a malformed record.
fn start_tls(
    mut socket: DeadlineSocket,
    tls: &RpcClientTls,
) -> Result<StreamOwned<ClientConnection, DeadlineSocket>, TransportError> {
    let mut connection = ClientConnection::new(Arc::clone(&tls.config), tls.server_name.clone())
        .map_err(tls_configuration_error)?;
    while connection.wants_write() {
        connection.write_tls(&mut socket).map_err(io_error)?;
    }
    if !answer_is_tls(&mut socket)? {
        return Err(plaintext_answer(&mut socket));
    }
    while connection.is_handshaking() {
        connection.complete_io(&mut socket).map_err(stream_error)?;
    }
    Ok(StreamOwned::new(connection, socket))
}

/// Wait for the owner's first byte. A TLS record starts with a content type
/// in 20..=23; a plaintext frame starts with the high byte of its length,
/// which is zero for every frame this protocol allows.
fn answer_is_tls(socket: &mut DeadlineSocket) -> Result<bool, TransportError> {
    let mut first = [0_u8; 1];
    loop {
        if let Some(deadline) = socket.deadline {
            socket
                .socket
                .set_read_timeout(Some(remaining(deadline).map_err(io_error)?))
                .map_err(io_error)?;
        }
        match socket.socket.peek(&mut first) {
            Ok(0) => {
                return Err(io_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the owner closed the connection during the TLS handshake; an owner \
                     without TLS that predates this client does so",
                )));
            }
            Ok(_) => return Ok((20..=23).contains(&first[0])),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(io_error(error)),
        }
    }
}

fn plaintext_answer(socket: &mut DeadlineSocket) -> TransportError {
    let mut frame = [0_u8; HANDSHAKE_FRAME_BYTES];
    if socket.read_exact(&mut frame).is_ok() {
        if let Ok(answer) = decode_handshake_frame(&frame) {
            if answer.kind() == HandshakeKind::TlsUnavailable {
                return TransportError::security(
                    "the owner does not use TLS; configure this client without RPC TLS or \
                     enable TLS on the owner",
                );
            }
        }
    }
    TransportError::security("the owner answered the TLS client hello in plaintext")
}

/// Builds one complete request frame, the four-byte big-endian length followed
/// by the request, so it leaves in a single write. Writing the prefix on its own
/// lets Nagle's algorithm hold the request until the server acknowledges the
/// prefix, and delayed ACK postpones that acknowledgement by tens of
/// milliseconds.
fn length_prefixed_frame(request: &[u8]) -> Result<Vec<u8>, TransportError> {
    if request.len() > MAX_FRAME_BYTES {
        return Err(TransportError::new(
            format!(
                "request frame is {} bytes, maximum is {MAX_FRAME_BYTES}",
                request.len()
            ),
            false,
        ));
    }
    let length = u32::try_from(request.len())
        .map_err(|_| TransportError::new("request length exceeds u32", false))?;
    let mut frame = Vec::with_capacity(4 + request.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(request);
    Ok(frame)
}

/// Writes one complete request frame with a single `write_all` and reads one
/// length-prefixed response, reporting whether a failure came before or after
/// the first response byte.
fn exchange_frame(
    stream: &mut (impl Read + Write),
    frame: &[u8],
) -> Result<Vec<u8>, ExchangeError> {
    stream
        .write_all(frame)
        .and_then(|()| stream.flush())
        .map_err(|error| ExchangeError::BeforeResponse(io_error(error)))?;

    let mut response_length = [0_u8; 4];
    let first = loop {
        match stream.read(&mut response_length[..1]) {
            Ok(0) => {
                return Err(ExchangeError::BeforeResponse(io_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "workspace RPC connection closed before a response",
                ))));
            }
            Ok(read) => break read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(ExchangeError::BeforeResponse(io_error(error))),
        }
    };
    debug_assert_eq!(first, 1);
    stream
        .read_exact(&mut response_length[1..])
        .map_err(|error| ExchangeError::AfterResponse(io_error(error)))?;
    let response_length = usize::try_from(u32::from_be_bytes(response_length)).map_err(|_| {
        ExchangeError::AfterResponse(TransportError::new(
            "response length does not fit usize",
            false,
        ))
    })?;
    if response_length > MAX_FRAME_BYTES {
        return Err(ExchangeError::AfterResponse(TransportError::new(
            format!("response frame is {response_length} bytes, maximum is {MAX_FRAME_BYTES}"),
            false,
        )));
    }
    let mut response = vec![0_u8; response_length];
    stream
        .read_exact(&mut response)
        .map_err(|error| ExchangeError::AfterResponse(io_error(error)))?;
    Ok(response)
}

/// Offer the exact schema and read the owner's verdict. Both frames travel
/// inside TLS when it is configured, under the connection's handshake
/// deadline.
fn perform_handshake(stream: &mut RpcStream) -> Result<(), TransportError> {
    let hello = WorkspaceHandshake::new(HandshakeKind::ClientHello, WORKSPACE_PROTOCOL_SCHEMA)
        .expect("the compiled workspace protocol schema fits the handshake");
    let encoded = encode_handshake_frame(&hello)
        .expect("the compiled workspace protocol schema has one canonical handshake encoding");
    stream
        .write_all(&encoded)
        .and_then(|()| stream.flush())
        .map_err(stream_error)?;

    let mut response = [0_u8; HANDSHAKE_FRAME_BYTES];
    stream.read_exact(&mut response).map_err(stream_error)?;
    let response = decode_handshake_frame(&response).map_err(|error| {
        TransportError::incompatible(format!("invalid workspace RPC handshake: {error}"))
    })?;
    match response.kind() {
        HandshakeKind::Accepted if response.operation_schema() == WORKSPACE_PROTOCOL_SCHEMA => {
            Ok(())
        }
        HandshakeKind::Incompatible => Err(TransportError::incompatible(format!(
            "workspace RPC handshake is incompatible: this client speaks {WORKSPACE_PROTOCOL_SCHEMA} \
             and the owner answered {}; upgrade the NoKV client/server pair",
            response.operation_schema()
        ))),
        HandshakeKind::Accepted => Err(TransportError::incompatible(format!(
            "workspace RPC handshake accepted a different operation schema: {}",
            response.operation_schema()
        ))),
        HandshakeKind::TlsRequired => Err(TransportError::security(
            "the owner requires TLS; configure RPC TLS for this client",
        )),
        HandshakeKind::Unauthenticated => Err(TransportError::security(
            "the owner did not accept this client's certificate: it is not a current \
             credential of the owner's tenant",
        )),
        HandshakeKind::ClientHello | HandshakeKind::TlsUnavailable => Err(TransportError::new(
            format!(
                "workspace RPC server answered the handshake with {:?}",
                response.kind()
            ),
            false,
        )),
    }
}

fn remaining(deadline: Instant) -> io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "workspace RPC handshake deadline expired",
            )
        })
}

/// A TLS protocol failure surfaces as invalid data: a rejected certificate,
/// an alert from the owner, or a malformed record. None of them changes on
/// retry.
fn stream_error(error: io::Error) -> TransportError {
    if error.kind() == io::ErrorKind::InvalidData {
        TransportError::security(format!("RPC TLS failed: {error}"))
    } else {
        io_error(error)
    }
}

fn io_error(error: io::Error) -> TransportError {
    let retryable = matches!(
        error.kind(),
        io::ErrorKind::TimedOut
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::Interrupted
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    );
    TransportError::new(error.to_string(), retryable)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::net::TcpListener;
    use std::thread;

    use nokv_protocol::{
        decode_handshake_frame, encode_handshake_frame, HandshakeKind, WorkspaceHandshake,
        HANDSHAKE_FRAME_BYTES, WORKSPACE_PROTOCOL_SCHEMA,
    };
    use nokv_types::ErrorClass;
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
    use rustls::pki_types::PrivatePkcs8KeyDer;
    use rustls::server::WebPkiClientVerifier;
    use rustls::{ServerConfig, ServerConnection};

    use super::*;

    fn options() -> FramedTcpOptions {
        FramedTcpOptions {
            connect_timeout: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
        }
    }

    fn transport() -> FramedTcpTransport {
        FramedTcpTransport::new(options()).unwrap()
    }

    /// In-memory stream that keeps every `write` call as a separate entry.
    struct RecordingStream {
        response: Cursor<Vec<u8>>,
        writes: Vec<Vec<u8>>,
    }

    impl Read for RecordingStream {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.response.read(buffer)
        }
    }

    impl Write for RecordingStream {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.writes.push(buffer.to_vec());
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn accept_one_handshake(listener: TcpListener) -> TcpStream {
        let (mut stream, _) = listener.accept().unwrap();
        accept_hello(&mut stream);
        stream
    }

    #[test]
    fn request_frame_leaves_in_one_write() {
        let request = [7_u8; 48];
        let mut response = 3_u32.to_be_bytes().to_vec();
        response.extend_from_slice(b"ok!");
        let mut stream = RecordingStream {
            response: Cursor::new(response),
            writes: Vec::new(),
        };

        let frame = length_prefixed_frame(&request).unwrap();
        assert_eq!(exchange_frame(&mut stream, &frame).unwrap(), b"ok!");

        let mut expected = 48_u32.to_be_bytes().to_vec();
        expected.extend_from_slice(&request);
        assert_eq!(stream.writes, vec![expected]);
    }

    #[test]
    fn oversized_request_is_rejected_before_connecting() {
        // Nothing listens on the endpoint, so a connection attempt would fail
        // with a retryable refusal instead of the size error.
        let endpoint = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let error = transport()
            .round_trip(endpoint, &vec![0_u8; MAX_FRAME_BYTES + 1])
            .unwrap_err();
        assert!(!error.retryable());
        assert!(error.message().contains("maximum"));
    }

    #[test]
    fn connection_disables_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || accept_one_handshake(listener));

        let mut stream = transport().connect(endpoint).unwrap();
        assert!(stream.socket().socket.nodelay().unwrap());
        server.join().unwrap();
    }

    #[test]
    fn sends_an_exact_hello_before_the_operation_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let expected_request = [7_u8; 48];
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut hello = [0_u8; HANDSHAKE_FRAME_BYTES];
            stream.read_exact(&mut hello).unwrap();
            let hello = decode_handshake_frame(&hello).unwrap();
            assert_eq!(hello.kind(), HandshakeKind::ClientHello);
            assert_eq!(hello.operation_schema(), WORKSPACE_PROTOCOL_SCHEMA);

            let accepted =
                WorkspaceHandshake::new(HandshakeKind::Accepted, WORKSPACE_PROTOCOL_SCHEMA)
                    .unwrap();
            stream
                .write_all(&encode_handshake_frame(&accepted).unwrap())
                .unwrap();

            let mut length = [0_u8; 4];
            stream.read_exact(&mut length).unwrap();
            assert_eq!(u32::from_be_bytes(length), expected_request.len() as u32);
            let mut request = vec![0_u8; expected_request.len()];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(request, expected_request);

            stream.write_all(&3_u32.to_be_bytes()).unwrap();
            stream.write_all(b"ok!").unwrap();
        });

        assert_eq!(
            transport().round_trip(endpoint, &expected_request).unwrap(),
            b"ok!"
        );
        server.join().unwrap();
    }

    fn accept_hello(stream: &mut (impl Read + Write)) {
        answer_hello(stream, HandshakeKind::Accepted);
    }

    fn answer_hello(stream: &mut (impl Read + Write), kind: HandshakeKind) {
        let mut hello = [0_u8; HANDSHAKE_FRAME_BYTES];
        stream.read_exact(&mut hello).unwrap();
        decode_handshake_frame(&hello).unwrap();
        let answer = WorkspaceHandshake::new(kind, WORKSPACE_PROTOCOL_SCHEMA).unwrap();
        stream
            .write_all(&encode_handshake_frame(&answer).unwrap())
            .unwrap();
        stream.flush().unwrap();
    }

    fn echo_one(stream: &mut (impl Read + Write)) {
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).unwrap();
        let mut request = vec![0_u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut request).unwrap();
        stream.write_all(&length).unwrap();
        stream.write_all(&request).unwrap();
        stream.flush().unwrap();
    }

    #[test]
    fn sequential_requests_reuse_one_admitted_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            accept_hello(&mut stream);
            echo_one(&mut stream);
            echo_one(&mut stream);
            listener.set_nonblocking(true).unwrap();
            assert!(listener.accept().is_err(), "a second connection was opened");
        });

        let transport = transport();
        assert_eq!(transport.round_trip(endpoint, b"first").unwrap(), b"first");
        assert_eq!(
            transport.clone().round_trip(endpoint, b"second").unwrap(),
            b"second"
        );
        server.join().unwrap();
    }

    #[test]
    fn a_pooled_connection_closed_by_the_server_is_replaced() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            accept_hello(&mut first);
            echo_one(&mut first);
            drop(first);
            closed_tx.send(()).unwrap();
            let (mut second, _) = listener.accept().unwrap();
            accept_hello(&mut second);
            echo_one(&mut second);
        });

        let transport = transport();
        assert_eq!(transport.round_trip(endpoint, b"one").unwrap(), b"one");
        closed_rx.recv().unwrap();
        assert_eq!(transport.round_trip(endpoint, b"two").unwrap(), b"two");
        server.join().unwrap();
    }

    #[test]
    fn incompatible_server_is_nonretryable_and_receives_no_operation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            answer_hello(&mut stream, HandshakeKind::Incompatible);
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut next = [0_u8; 1];
            assert_eq!(stream.read(&mut next).unwrap(), 0);
        });

        let error = transport()
            .round_trip(endpoint, b"must-not-dispatch")
            .unwrap_err();
        assert!(!error.retryable());
        assert!(error.message().contains("incompatible"));
        server.join().unwrap();
    }

    #[test]
    fn an_owner_requiring_tls_refuses_a_plaintext_client() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            answer_hello(&mut stream, HandshakeKind::TlsRequired);
        });

        let error = transport().round_trip(endpoint, b"plain").unwrap_err();
        assert!(!error.retryable());
        assert_eq!(error.refusal_class(), Some(ErrorClass::OperatorRequired));
        assert!(error.message().contains("requires TLS"));
        server.join().unwrap();
    }

    /// A test CA and the owner (`owner.test`) and client certificates it
    /// signs.
    struct Pki {
        ca: CertifiedIssuer<'static, KeyPair>,
        server_chain: Vec<CertificateDer<'static>>,
        server_key: PrivateKeyDer<'static>,
        client_chain: Vec<CertificateDer<'static>>,
        client_key: PrivateKeyDer<'static>,
    }

    impl Pki {
        fn new() -> Self {
            let mut params = CertificateParams::default();
            params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
            let server_key = KeyPair::generate().unwrap();
            let server = CertificateParams::new(vec!["owner.test".to_owned()])
                .unwrap()
                .signed_by(&server_key, &ca)
                .unwrap();
            let client_key = KeyPair::generate().unwrap();
            let client = CertificateParams::new(vec!["agent".to_owned()])
                .unwrap()
                .signed_by(&client_key, &ca)
                .unwrap();
            Self {
                server_chain: vec![server.der().clone()],
                server_key: PrivatePkcs8KeyDer::from(server_key.serialize_der()).into(),
                client_chain: vec![client.der().clone()],
                client_key: PrivatePkcs8KeyDer::from(client_key.serialize_der()).into(),
                ca,
            }
        }

        fn client_tls(&self, server_name: &str) -> RpcClientTls {
            RpcClientTls::new(
                ServerName::try_from(server_name.to_owned()).unwrap(),
                vec![self.ca.der().clone()],
                self.client_chain.clone(),
                self.client_key.clone_key(),
            )
            .unwrap()
        }

        fn tls_transport(&self, server_name: &str) -> FramedTcpTransport {
            transport().with_tls(self.client_tls(server_name))
        }

        /// An owner that requires a client certificate signed by the test CA.
        fn server_config(&self) -> Arc<ServerConfig> {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let mut roots = RootCertStore::empty();
            roots.add(self.ca.der().clone()).unwrap();
            let verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
                    .build()
                    .unwrap();
            let mut config = ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_client_cert_verifier(verifier)
                .with_single_cert(self.server_chain.clone(), self.server_key.clone_key())
                .unwrap();
            config.send_tls13_tickets = 0;
            Arc::new(config)
        }
    }

    fn accept_tls(
        listener: &TcpListener,
        config: &Arc<ServerConfig>,
    ) -> StreamOwned<ServerConnection, TcpStream> {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut connection = ServerConnection::new(Arc::clone(config)).unwrap();
        while connection.is_handshaking() {
            connection.complete_io(&mut socket).unwrap();
        }
        StreamOwned::new(connection, socket)
    }

    #[test]
    fn tls_exchanges_reuse_one_authenticated_connection() {
        let pki = Pki::new();
        let config = pki.server_config();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut stream = accept_tls(&listener, &config);
            accept_hello(&mut stream);
            echo_one(&mut stream);
            echo_one(&mut stream);
            listener.set_nonblocking(true).unwrap();
            assert!(listener.accept().is_err(), "a second connection was opened");
        });

        let transport = pki.tls_transport("owner.test");
        assert_eq!(transport.round_trip(endpoint, b"first").unwrap(), b"first");
        assert_eq!(
            transport.round_trip(endpoint, b"second").unwrap(),
            b"second"
        );
        server.join().unwrap();
    }

    #[test]
    fn a_pooled_tls_connection_the_owner_closed_is_replaced() {
        let pki = Pki::new();
        let config = pki.server_config();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let (closed_tx, closed_rx) = std::sync::mpsc::channel();
        let server = thread::spawn(move || {
            let mut first = accept_tls(&listener, &config);
            accept_hello(&mut first);
            echo_one(&mut first);
            first.conn.send_close_notify();
            first.flush().unwrap();
            drop(first);
            closed_tx.send(()).unwrap();
            let mut second = accept_tls(&listener, &config);
            accept_hello(&mut second);
            echo_one(&mut second);
        });

        let transport = pki.tls_transport("owner.test");
        assert_eq!(transport.round_trip(endpoint, b"one").unwrap(), b"one");
        closed_rx.recv().unwrap();
        assert_eq!(transport.round_trip(endpoint, b"two").unwrap(), b"two");
        server.join().unwrap();
    }

    #[test]
    fn an_owner_certificate_for_another_name_is_a_security_refusal() {
        let pki = Pki::new();
        let config = pki.server_config();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut connection = ServerConnection::new(config).unwrap();
            // The client aborts the handshake; nothing reaches the protocol.
            while connection.is_handshaking() {
                if connection.complete_io(&mut socket).is_err() {
                    return;
                }
            }
            panic!("the handshake completed for a mismatched server name");
        });

        let error = pki
            .tls_transport("other.test")
            .round_trip(endpoint, b"never")
            .unwrap_err();
        assert!(!error.retryable());
        assert_eq!(error.refusal_class(), Some(ErrorClass::OperatorRequired));
        assert!(error.message().contains("RPC TLS failed"), "{error}");
        server.join().unwrap();
    }

    #[test]
    fn an_owner_without_tls_answers_a_tls_hello_in_plaintext() {
        let pki = Pki::new();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Read the whole client hello record first: closing over unread
            // bytes resets the connection before the client reads the answer.
            let mut header = [0_u8; 5];
            stream.read_exact(&mut header).unwrap();
            assert_eq!(
                header[0], 22,
                "the client opened with a TLS handshake record"
            );
            let mut record = vec![0_u8; usize::from(u16::from_be_bytes([header[3], header[4]]))];
            stream.read_exact(&mut record).unwrap();
            let answer =
                WorkspaceHandshake::new(HandshakeKind::TlsUnavailable, WORKSPACE_PROTOCOL_SCHEMA)
                    .unwrap();
            stream
                .write_all(&encode_handshake_frame(&answer).unwrap())
                .unwrap();
            let mut rest = Vec::new();
            let _ = stream.read_to_end(&mut rest);
        });

        let error = pki
            .tls_transport("owner.test")
            .round_trip(endpoint, b"never")
            .unwrap_err();
        assert!(!error.retryable());
        assert_eq!(error.refusal_class(), Some(ErrorClass::OperatorRequired));
        assert!(error.message().contains("does not use TLS"), "{error}");
        server.join().unwrap();
    }

    #[test]
    fn an_unauthenticated_answer_is_a_security_refusal() {
        let pki = Pki::new();
        let config = pki.server_config();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let mut stream = accept_tls(&listener, &config);
            answer_hello(&mut stream, HandshakeKind::Unauthenticated);
        });

        let error = pki
            .tls_transport("owner.test")
            .round_trip(endpoint, b"never")
            .unwrap_err();
        assert!(!error.retryable());
        assert_eq!(error.refusal_class(), Some(ErrorClass::OperatorRequired));
        assert!(
            error.message().contains("not a current credential"),
            "{error}"
        );
        server.join().unwrap();
    }

    #[test]
    fn a_trickling_owner_cannot_extend_the_tls_handshake() {
        let pki = Pki::new();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // A TLS record header that promises a long record, then one byte
            // every 50 ms: each read makes progress, the whole never ends.
            stream.write_all(&[22, 3, 3, 0x40, 0x00]).unwrap();
            for _ in 0..60 {
                thread::sleep(Duration::from_millis(50));
                if stream.write_all(&[0]).is_err() {
                    break;
                }
            }
        });
        let transport = FramedTcpTransport::new(FramedTcpOptions {
            handshake_timeout: Duration::from_millis(300),
            ..options()
        })
        .unwrap()
        .with_tls(pki.client_tls("owner.test"));

        let started = Instant::now();
        let error = transport.round_trip(endpoint, b"never").unwrap_err();
        assert!(error.retryable(), "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
        server.join().unwrap();
    }

    #[test]
    fn tls_settings_never_reuse_a_plaintext_pool() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            accept_hello(&mut stream);
            echo_one(&mut stream);
            stream
        });
        let plaintext = transport();
        assert_eq!(plaintext.round_trip(endpoint, b"plain").unwrap(), b"plain");
        let _held = server.join().unwrap();

        let secured = plaintext.with_tls(Pki::new().client_tls("owner.test"));
        assert!(secured.take_idle(endpoint).is_none());
    }
}
