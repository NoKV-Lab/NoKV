/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nokv_protocol::{decode_request, encode_response, MAX_FRAME_BYTES, WORKSPACE_PROTOCOL_SCHEMA};

use crate::{ControlBackedRootOwner, RootOwnerRegistry, ServerError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub lease_renew_interval: Duration,
}

impl ServerOptions {
    fn validate(self) -> Result<(), ServerError> {
        if self.read_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.lease_renew_interval.is_zero()
        {
            return Err(ServerError::InvalidOptions(
                "connection timeouts and lease renewal interval must be greater than zero"
                    .to_owned(),
            ));
        }
        Ok(())
    }
}

/// Shared fail-closed owner-loss signal for the RPC runtime and future
/// owner-fenced background workers.
#[derive(Clone, Default)]
pub struct OwnerLossSignal(Arc<AtomicBool>);

impl OwnerLossSignal {
    pub fn is_lost(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Fail the complete owner scope closed.
    ///
    /// The process supervisor uses this when any owner-fenced companion worker
    /// encounters a terminal invariant failure. The RPC accept loop observes
    /// the same signal, stops admitting requests, and returns an error instead
    /// of continuing without lifecycle recovery.
    pub fn fail_closed(&self) {
        self.mark_lost();
    }

    fn mark_lost(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerHealth {
    pub protocol_schema: &'static str,
    pub installed_roots: usize,
}

#[derive(Clone)]
pub struct WorkspaceServer {
    options: ServerOptions,
    registry: Arc<RootOwnerRegistry>,
    ownership: Vec<ControlBackedRootOwner>,
    owner_loss: OwnerLossSignal,
}

impl WorkspaceServer {
    pub fn new(
        options: ServerOptions,
        registry: Arc<RootOwnerRegistry>,
        ownership: Vec<ControlBackedRootOwner>,
    ) -> Result<Self, ServerError> {
        options.validate()?;
        if ownership.is_empty() {
            return Err(ServerError::InvalidOptions(
                "serving requires at least one control-backed root owner".to_owned(),
            ));
        }
        for (index, owner) in ownership.iter().enumerate() {
            if !owner.is_for_registry(&registry) {
                return Err(ServerError::InvalidOptions(format!(
                    "ownership entry {index} belongs to another registry"
                )));
            }
            if !registry.contains_exact(owner.route())? {
                return Err(ServerError::InvalidOptions(format!(
                    "ownership entry {index} has no exact installed route"
                )));
            }
            if ownership[..index]
                .iter()
                .any(|current| current.route() == owner.route())
            {
                return Err(ServerError::InvalidOptions(format!(
                    "ownership entry {index} duplicates a root route"
                )));
            }
        }
        Ok(Self {
            options,
            registry,
            ownership,
            owner_loss: OwnerLossSignal::default(),
        })
    }

    pub fn health(&self) -> Result<ServerHealth, ServerError> {
        Ok(ServerHealth {
            protocol_schema: WORKSPACE_PROTOCOL_SCHEMA,
            installed_roots: self.registry.installed_root_count()?,
        })
    }

    /// Runtime lease-renewal hook. A failed renewal uninstalls that owner's
    /// exact route before the error is returned.
    pub fn renew_ownership(&self) -> Result<(), ServerError> {
        for owner in &self.ownership {
            if let Err(error) = owner.renew_or_uninstall() {
                self.owner_loss.mark_lost();
                return Err(error);
            }
        }
        Ok(())
    }

    pub fn owner_loss_signal(&self) -> OwnerLossSignal {
        self.owner_loss.clone()
    }

    pub fn run(&self) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.options.bind).map_err(ServerError::Bind)?;
        self.serve(listener)
    }

    pub fn serve(&self, listener: TcpListener) -> Result<(), ServerError> {
        listener
            .set_nonblocking(true)
            .map_err(ServerError::Connection)?;
        let mut next_renewal = Instant::now();
        loop {
            if self.owner_loss.is_lost() {
                return Err(ServerError::InvalidBootstrap(
                    "control-plane owner was lost".to_owned(),
                ));
            }
            let now = Instant::now();
            if now >= next_renewal {
                self.renew_ownership()?;
                next_renewal = Instant::now() + self.options.lease_renew_interval;
            }
            match listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_nonblocking(false)
                        .map_err(ServerError::Connection)?;
                    let registry = Arc::clone(&self.registry);
                    let options = self.options;
                    thread::Builder::new()
                        .name("nokv-workspace-rpc".to_owned())
                        .spawn(move || {
                            let _ = serve_connection(stream, registry, options);
                        })
                        .map_err(ServerError::Connection)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    let until_renewal = next_renewal.saturating_duration_since(Instant::now());
                    thread::sleep(until_renewal.min(Duration::from_millis(10)));
                }
                Err(error) => return Err(ServerError::Connection(error)),
            }
        }
    }

    pub fn dispatch_frame(&self, encoded: &[u8]) -> Result<Vec<u8>, ServerError> {
        let request = decode_request(encoded)?;
        let response = self.registry.dispatch(request)?;
        encode_response(&response).map_err(ServerError::Protocol)
    }
}

fn serve_connection(
    mut stream: TcpStream,
    registry: Arc<RootOwnerRegistry>,
    options: ServerOptions,
) -> Result<(), ServerError> {
    stream
        .set_read_timeout(Some(options.read_timeout))
        .map_err(ServerError::Connection)?;
    stream
        .set_write_timeout(Some(options.write_timeout))
        .map_err(ServerError::Connection)?;
    while let Some(request) = read_frame(&mut stream)? {
        let request = decode_request(&request)?;
        let response = registry.dispatch(request)?;
        let response = encode_response(&response)?;
        write_frame(&mut stream, &response)?;
    }
    Ok(())
}

fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, ServerError> {
    let mut length = [0_u8; 4];
    match reader.read(&mut length[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("one-byte read cannot return more than one byte"),
        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
            return read_frame(reader);
        }
        Err(error) => return Err(ServerError::Connection(error)),
    }
    reader
        .read_exact(&mut length[1..])
        .map_err(ServerError::Connection)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| ServerError::InvalidOptions("frame length does not fit usize".to_owned()))?;
    if length > MAX_FRAME_BYTES {
        return Err(ServerError::FrameTooLarge {
            bytes: length,
            max: MAX_FRAME_BYTES,
        });
    }
    let mut frame = vec![0_u8; length];
    reader
        .read_exact(&mut frame)
        .map_err(ServerError::Connection)?;
    Ok(Some(frame))
}

fn write_frame(writer: &mut impl Write, frame: &[u8]) -> Result<(), ServerError> {
    if frame.len() > MAX_FRAME_BYTES {
        return Err(ServerError::FrameTooLarge {
            bytes: frame.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    let length = u32::try_from(frame.len())
        .map_err(|_| ServerError::InvalidOptions("frame length exceeds u32".to_owned()))?;
    writer
        .write_all(&length.to_be_bytes())
        .map_err(ServerError::Connection)?;
    writer.write_all(frame).map_err(ServerError::Connection)?;
    writer.flush().map_err(ServerError::Connection)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn frame_round_trips_exact_bytes() {
        let expected = b"workspace request";
        let mut encoded = Vec::new();
        write_frame(&mut encoded, expected).unwrap();
        let mut cursor = Cursor::new(encoded);
        assert_eq!(read_frame(&mut cursor).unwrap(), Some(expected.to_vec()));
        assert_eq!(read_frame(&mut cursor).unwrap(), None);
    }

    #[test]
    fn oversized_inbound_frame_is_rejected_before_allocation() {
        let encoded = u32::try_from(MAX_FRAME_BYTES + 1)
            .unwrap()
            .to_be_bytes()
            .to_vec();
        assert!(matches!(
            read_frame(&mut Cursor::new(encoded)),
            Err(ServerError::FrameTooLarge { .. })
        ));
    }
}
