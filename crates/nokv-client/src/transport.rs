/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
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

#[derive(Clone, Debug)]
pub struct FramedTcpTransport {
    options: FramedTcpOptions,
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
        Ok(Self { options })
    }
}

impl RpcTransport for FramedTcpTransport {
    fn round_trip(&self, endpoint: SocketAddr, request: &[u8]) -> Result<Vec<u8>, TransportError> {
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
        let mut stream = TcpStream::connect_timeout(&endpoint, self.options.connect_timeout)
            .map_err(io_error)?;
        perform_handshake(&mut stream, self.options.handshake_timeout)?;
        stream
            .set_read_timeout(Some(self.options.read_timeout))
            .map_err(io_error)?;
        stream
            .set_write_timeout(Some(self.options.write_timeout))
            .map_err(io_error)?;
        stream.write_all(&length.to_be_bytes()).map_err(io_error)?;
        stream.write_all(request).map_err(io_error)?;
        stream.flush().map_err(io_error)?;

        let mut response_length = [0_u8; 4];
        stream.read_exact(&mut response_length).map_err(io_error)?;
        let response_length = usize::try_from(u32::from_be_bytes(response_length))
            .map_err(|_| TransportError::new("response length does not fit usize", false))?;
        if response_length > MAX_FRAME_BYTES {
            return Err(TransportError::new(
                format!("response frame is {response_length} bytes, maximum is {MAX_FRAME_BYTES}"),
                false,
            ));
        }
        let mut response = vec![0_u8; response_length];
        stream.read_exact(&mut response).map_err(io_error)?;
        Ok(response)
    }
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

    #[test]
    fn sends_an_exact_v6_hello_before_the_operation_frame() {
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
