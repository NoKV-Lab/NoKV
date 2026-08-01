/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use nokv_protocol::MAX_FRAME_BYTES;

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
    pub read_timeout: Duration,
    pub write_timeout: Duration,
}

impl Default for FramedTcpOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
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
