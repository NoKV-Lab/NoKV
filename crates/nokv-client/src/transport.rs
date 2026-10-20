/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nokv_protocol::{
    decode_handshake_frame, encode_handshake_frame, HandshakeKind, WorkspaceHandshake,
    HANDSHAKE_FRAME_BYTES, MAX_FRAME_BYTES, WORKSPACE_PROTOCOL_SCHEMA,
};

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
#[derive(Clone, Debug)]
pub struct FramedTcpTransport {
    options: FramedTcpOptions,
    idle: Arc<Mutex<HashMap<SocketAddr, Vec<IdleConnection>>>>,
}

#[derive(Debug)]
struct IdleConnection {
    stream: TcpStream,
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
            idle: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Opens one connection and completes the exact-schema handshake. Nagle's
    /// algorithm is disabled before the first byte is written, so no frame
    /// waits for the server's delayed ACK of an earlier segment.
    fn connect(&self, endpoint: SocketAddr) -> Result<TcpStream, TransportError> {
        let mut stream = TcpStream::connect_timeout(&endpoint, self.options.connect_timeout)
            .map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        perform_handshake(&mut stream, self.options.handshake_timeout)?;
        stream
            .set_read_timeout(Some(self.options.read_timeout))
            .map_err(io_error)?;
        stream
            .set_write_timeout(Some(self.options.write_timeout))
            .map_err(io_error)?;
        Ok(stream)
    }

    /// Take the most recently returned live connection for `endpoint`.
    fn take_idle(&self, endpoint: SocketAddr) -> Option<TcpStream> {
        let mut idle = self.idle.lock().ok()?;
        let connections = idle.get_mut(&endpoint)?;
        while let Some(connection) = connections.pop() {
            if connection.idle_since.elapsed() < MAX_IDLE_CONNECTION_AGE
                && connection_is_quiet(&connection.stream)
            {
                return Some(connection.stream);
            }
        }
        None
    }

    fn return_idle(&self, endpoint: SocketAddr, stream: TcpStream) {
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

/// A reusable connection has nothing to read: unread bytes or end of stream
/// mean the peer closed or desynchronized it.
fn connection_is_quiet(stream: &TcpStream) -> bool {
    if stream.set_nonblocking(true).is_err() {
        return false;
    }
    let mut probe = [0_u8; 1];
    let quiet = matches!(
        stream.peek(&mut probe),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock
    );
    stream.set_nonblocking(false).is_ok() && quiet
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
                return Err(ExchangeError::BeforeResponse(io_error(
                    std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "workspace RPC connection closed before a response",
                    ),
                )));
            }
            Ok(read) => break read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
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

fn perform_handshake(stream: &mut TcpStream, timeout: Duration) -> Result<(), TransportError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| TransportError::new("TCP handshake deadline overflows", false))?;
    let hello = WorkspaceHandshake::new(HandshakeKind::ClientHello, WORKSPACE_PROTOCOL_SCHEMA)
        .expect("the compiled workspace protocol schema fits the handshake");
    let encoded = encode_handshake_frame(&hello)
        .expect("the compiled workspace protocol schema has one canonical handshake encoding");
    write_all_until(stream, &encoded, deadline).map_err(io_error)?;

    let mut response = [0_u8; HANDSHAKE_FRAME_BYTES];
    read_exact_until(stream, &mut response, deadline).map_err(io_error)?;
    let response = decode_handshake_frame(&response).map_err(|error| {
        TransportError::new(format!("invalid workspace RPC handshake: {error}"), false)
    })?;
    match response.kind() {
        HandshakeKind::Accepted if response.operation_schema() == WORKSPACE_PROTOCOL_SCHEMA => {
            Ok(())
        }
        HandshakeKind::Incompatible => Err(TransportError::new(
            "workspace RPC handshake is incompatible; upgrade the NoKV client/server pair",
            false,
        )),
        HandshakeKind::Accepted => Err(TransportError::new(
            "workspace RPC handshake accepted a different operation schema",
            false,
        )),
        HandshakeKind::ClientHello => Err(TransportError::new(
            "workspace RPC server returned a client hello",
            false,
        )),
    }
}

fn read_exact_until(
    stream: &mut TcpStream,
    mut buffer: &mut [u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !buffer.is_empty() {
        let remaining = remaining(deadline)?;
        stream.set_read_timeout(Some(remaining))?;
        match stream.read(buffer) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "workspace RPC handshake ended early",
                ));
            }
            Ok(read) => buffer = &mut buffer[read..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    remaining(deadline).map(|_| ())
}

fn write_all_until(
    stream: &mut TcpStream,
    mut buffer: &[u8],
    deadline: Instant,
) -> std::io::Result<()> {
    while !buffer.is_empty() {
        let remaining = remaining(deadline)?;
        stream.set_write_timeout(Some(remaining))?;
        match stream.write(buffer) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "workspace RPC handshake write made no progress",
                ));
            }
            Ok(written) => buffer = &buffer[written..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    remaining(deadline).map(|_| ())
}

fn remaining(deadline: Instant) -> std::io::Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "workspace RPC handshake deadline expired",
            )
        })
}

fn io_error(error: std::io::Error) -> TransportError {
    let retryable = matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::UnexpectedEof
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

    use super::*;

    fn transport() -> FramedTcpTransport {
        FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
        })
        .unwrap()
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

        let stream = transport().connect(endpoint).unwrap();
        assert!(stream.nodelay().unwrap());
        server.join().unwrap();
    }

    #[test]
    fn sends_an_exact_v9_hello_before_the_operation_frame() {
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

    fn accept_hello(stream: &mut TcpStream) {
        let mut hello = [0_u8; HANDSHAKE_FRAME_BYTES];
        stream.read_exact(&mut hello).unwrap();
        decode_handshake_frame(&hello).unwrap();
        let accepted =
            WorkspaceHandshake::new(HandshakeKind::Accepted, WORKSPACE_PROTOCOL_SCHEMA).unwrap();
        stream
            .write_all(&encode_handshake_frame(&accepted).unwrap())
            .unwrap();
    }

    fn echo_one(stream: &mut TcpStream) {
        let mut length = [0_u8; 4];
        stream.read_exact(&mut length).unwrap();
        let mut request = vec![0_u8; u32::from_be_bytes(length) as usize];
        stream.read_exact(&mut request).unwrap();
        stream.write_all(&length).unwrap();
        stream.write_all(&request).unwrap();
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
            let mut hello = [0_u8; HANDSHAKE_FRAME_BYTES];
            stream.read_exact(&mut hello).unwrap();
            decode_handshake_frame(&hello).unwrap();
            let incompatible =
                WorkspaceHandshake::new(HandshakeKind::Incompatible, WORKSPACE_PROTOCOL_SCHEMA)
                    .unwrap();
            stream
                .write_all(&encode_handshake_frame(&incompatible).unwrap())
                .unwrap();
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
}
