/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nokv_protocol::{
    decode_handshake_payload, decode_request, encode_handshake_frame, encode_response,
    has_handshake_magic, HandshakeKind, WorkspaceHandshake, HANDSHAKE_PAYLOAD_BYTES,
    MAX_FRAME_BYTES, WORKSPACE_PROTOCOL_SCHEMA,
};

use crate::legacy_rejection::{legacy_rejection_response, MAX_LEGACY_FIRST_FRAME_BYTES};
use crate::{RootOwnerRegistry, ServerError, ShardOwner};

static NEVER_SHUTDOWN: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServerOptions {
    pub bind: SocketAddr,
    pub handshake_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub lease_renew_interval: Duration,
    pub max_inflight_connections: usize,
}

impl ServerOptions {
    fn validate(self) -> Result<(), ServerError> {
        if self.handshake_timeout.is_zero()
            || self.read_timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.lease_renew_interval.is_zero()
        {
            return Err(ServerError::InvalidOptions(
                "connection timeouts and lease renewal interval must be greater than zero"
                    .to_owned(),
            ));
        }
        if self.max_inflight_connections == 0 {
            return Err(ServerError::InvalidOptions(
                "maximum in-flight connections must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }
}

struct ConnectionLimiter {
    maximum: usize,
    in_flight: AtomicUsize,
}

impl ConnectionLimiter {
    fn new(maximum: usize) -> Option<Self> {
        (maximum > 0).then(|| Self {
            maximum,
            in_flight: AtomicUsize::new(0),
        })
    }

    fn try_acquire(self: &Arc<Self>) -> Option<ConnectionPermit> {
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if current >= self.maximum {
                return None;
            }
            match self.in_flight.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ConnectionPermit {
                        limiter: Arc::clone(self),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
}

struct ConnectionPermit {
    limiter: Arc<ConnectionLimiter>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        let previous = self.limiter.in_flight.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
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

pub struct WorkspaceServer {
    options: ServerOptions,
    registry: Arc<RootOwnerRegistry>,
    ownership: Vec<ShardOwner>,
    owner_loss: OwnerLossSignal,
}

impl WorkspaceServer {
    pub fn new(
        options: ServerOptions,
        registry: Arc<RootOwnerRegistry>,
        ownership: Vec<ShardOwner>,
    ) -> Result<Self, ServerError> {
        if let Err(error) = validate_ownership(options, &registry, &ownership) {
            return Err(release_rejected_ownership(ownership, error));
        }
        Ok(Self {
            options,
            owner_loss: registry.owner_loss_signal(),
            registry,
            ownership,
        })
    }

    pub fn health(&self) -> Result<ServerHealth, ServerError> {
        Ok(ServerHealth {
            protocol_schema: WORKSPACE_PROTOCOL_SCHEMA,
            installed_roots: self.registry.installed_root_count()?,
        })
    }

    /// Runtime lease-renewal hook. A failed renewal uninstalls that owner's
    /// complete root-route set before the error is returned.
    pub fn renew_ownership(&self) -> Result<(), ServerError> {
        for owner in &self.ownership {
            if let Err(error) = owner.renew_or_uninstall() {
                self.owner_loss.mark_lost();
                return Err(error);
            }
        }
        Ok(())
    }

    /// Remove all routes and release every logical-shard lease once.
    pub fn release_ownership(self) -> Result<(), ServerError> {
        release_ownership(self.ownership)
    }

    pub fn owner_loss_signal(&self) -> OwnerLossSignal {
        self.owner_loss.clone()
    }

    pub fn run(&self) -> Result<(), ServerError> {
        self.run_until_shutdown(&NEVER_SHUTDOWN)
    }

    /// Serve until graceful shutdown is requested, then stop accepting new
    /// connections and drain every admitted connection while retaining and
    /// renewing ownership.
    pub fn run_until_shutdown(&self, shutdown: &AtomicBool) -> Result<(), ServerError> {
        let listener = TcpListener::bind(self.options.bind).map_err(ServerError::Bind)?;
        self.serve_until_shutdown(listener, shutdown)
    }

    pub fn serve(&self, listener: TcpListener) -> Result<(), ServerError> {
        self.serve_until_shutdown(listener, &NEVER_SHUTDOWN)
    }

    pub fn serve_until_shutdown(
        &self,
        listener: TcpListener,
        shutdown: &AtomicBool,
    ) -> Result<(), ServerError> {
        serve_socket_loop(
            listener,
            self.options,
            Arc::clone(&self.registry),
            self.owner_loss.clone(),
            shutdown,
            || self.renew_ownership(),
        )
    }

    pub fn dispatch_frame(&self, encoded: &[u8]) -> Result<Vec<u8>, ServerError> {
        let request = decode_request(encoded)?;
        let response = self.registry.dispatch_guarded(request)?;
        encode_response(response.response()).map_err(ServerError::Protocol)
    }
}

fn serve_socket_loop(
    listener: TcpListener,
    options: ServerOptions,
    registry: Arc<RootOwnerRegistry>,
    owner_loss: OwnerLossSignal,
    shutdown: &AtomicBool,
    mut renew_ownership: impl FnMut() -> Result<(), ServerError>,
) -> Result<(), ServerError> {
    options.validate()?;
    listener
        .set_nonblocking(true)
        .map_err(ServerError::Connection)?;
    let mut next_renewal = Instant::now();
    let connections = Arc::new(
        ConnectionLimiter::new(options.max_inflight_connections)
            .expect("validated connection maximum is nonzero"),
    );
    loop {
        require_owner_retained(&owner_loss)?;
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let now = Instant::now();
        if now >= next_renewal {
            renew_ownership()?;
            next_renewal = Instant::now() + options.lease_renew_interval;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                require_owner_retained(&owner_loss)?;
                if shutdown.load(Ordering::Acquire) {
                    drop(stream);
                    continue;
                }
                let Some(permit) = connections.try_acquire() else {
                    drop(stream);
                    continue;
                };
                stream
                    .set_nonblocking(false)
                    .map_err(ServerError::Connection)?;
                let registry = Arc::clone(&registry);
                thread::Builder::new()
                    .name("nokv-workspace-rpc".to_owned())
                    .spawn(move || {
                        let _permit = permit;
                        let _ = serve_connection(stream, registry, options);
                    })
                    .map_err(ServerError::Connection)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                sleep_until_runtime_event(next_renewal);
            }
            Err(error) => return Err(ServerError::Connection(error)),
        }
    }

    while connections.in_flight() != 0 {
        require_owner_retained(&owner_loss)?;
        let now = Instant::now();
        if now >= next_renewal {
            renew_ownership()?;
            next_renewal = Instant::now() + options.lease_renew_interval;
        }
        sleep_until_runtime_event(next_renewal);
    }
    require_owner_retained(&owner_loss)
}

fn require_owner_retained(owner_loss: &OwnerLossSignal) -> Result<(), ServerError> {
    if owner_loss.is_lost() {
        Err(ServerError::InvalidBootstrap(
            "control-plane owner was lost".to_owned(),
        ))
    } else {
        Ok(())
    }
}

fn sleep_until_runtime_event(next_renewal: Instant) {
    let until_renewal = next_renewal.saturating_duration_since(Instant::now());
    thread::sleep(until_renewal.min(Duration::from_millis(10)));
}

fn validate_ownership(
    options: ServerOptions,
    registry: &Arc<RootOwnerRegistry>,
    ownership: &[ShardOwner],
) -> Result<(), ServerError> {
    options.validate()?;
    if ownership.is_empty() {
        return Err(ServerError::InvalidOptions(
            "serving requires at least one logical-shard owner".to_owned(),
        ));
    }
    let mut shards = BTreeSet::new();
    let mut roots = BTreeSet::new();
    for (index, owner) in ownership.iter().enumerate() {
        if !owner.is_for_registry(registry) {
            return Err(ServerError::InvalidOptions(format!(
                "ownership entry {index} belongs to another registry"
            )));
        }
        if !shards.insert(owner.shard_id()) {
            return Err(ServerError::InvalidOptions(format!(
                "ownership entry {index} duplicates logical shard {:?}",
                owner.shard_id()
            )));
        }
        if owner.routes().is_empty() {
            return Err(ServerError::InvalidOptions(format!(
                "ownership entry {index} has no root routes"
            )));
        }
        for route in owner.routes() {
            if route.logical_shard_id != nokv_protocol::LogicalShardIdentity::from(owner.shard_id())
            {
                return Err(ServerError::InvalidOptions(format!(
                    "ownership entry {index} contains a route for another logical shard"
                )));
            }
            if !roots.insert(route.root_id) {
                return Err(ServerError::InvalidOptions(format!(
                    "ownership entry {index} duplicates root route {:?}",
                    route.root_id
                )));
            }
            if !registry.contains_exact(*route)? {
                return Err(ServerError::InvalidOptions(format!(
                    "ownership entry {index} has no exact installed route for root {:?}",
                    route.root_id
                )));
            }
        }
    }
    Ok(())
}

fn release_rejected_ownership(ownership: Vec<ShardOwner>, primary: ServerError) -> ServerError {
    match release_ownership(ownership) {
        Ok(()) => primary,
        Err(cleanup) => ServerError::BootstrapRollback {
            primary: primary.to_string(),
            rollback: cleanup.to_string(),
        },
    }
}

fn release_ownership(ownership: Vec<ShardOwner>) -> Result<(), ServerError> {
    let failures = ownership
        .into_iter()
        .filter_map(|owner| owner.release().err().map(|error| error.to_string()))
        .collect::<Vec<_>>();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(ServerError::BootstrapRollback {
            primary: "release logical-shard ownership".to_owned(),
            rollback: failures.join("; "),
        })
    }
}

fn serve_connection(
    mut stream: TcpStream,
    registry: Arc<RootOwnerRegistry>,
    options: ServerOptions,
) -> Result<(), ServerError> {
    if !admit_current_protocol(&mut stream, options.handshake_timeout)? {
        return Ok(());
    }
    stream
        .set_read_timeout(Some(options.read_timeout))
        .map_err(ServerError::Connection)?;
    stream
        .set_write_timeout(Some(options.write_timeout))
        .map_err(ServerError::Connection)?;
    while let Some(request) = read_frame(&mut stream)? {
        let request = decode_request(&request)?;
        let response = registry.dispatch_guarded(request)?;
        let encoded = encode_response(response.response())?;
        write_frame(&mut stream, &encoded)?;
        drop(response);
        if registry.owner_loss_signal().is_lost() {
            return Err(ServerError::InvalidBootstrap(
                "control-plane owner was lost".to_owned(),
            ));
        }
    }
    Ok(())
}

fn admit_current_protocol(stream: &mut TcpStream, timeout: Duration) -> Result<bool, ServerError> {
    let deadline = Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| ServerError::InvalidOptions("handshake deadline overflows".to_owned()))?;
    let mut length = [0_u8; 4];
    read_exact_until(stream, &mut length, deadline).map_err(ServerError::Connection)?;
    let length = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| ServerError::InvalidOptions("frame length does not fit usize".to_owned()))?;
    if length > MAX_FRAME_BYTES {
        return Err(ServerError::FrameTooLarge {
            bytes: length,
            max: MAX_FRAME_BYTES,
        });
    }
    if length != HANDSHAKE_PAYLOAD_BYTES && length > MAX_LEGACY_FIRST_FRAME_BYTES {
        return Ok(false);
    }

    if length == HANDSHAKE_PAYLOAD_BYTES {
        let mut payload = [0_u8; HANDSHAKE_PAYLOAD_BYTES];
        read_exact_until(stream, &mut payload, deadline).map_err(ServerError::Connection)?;
        if has_handshake_magic(&payload) {
            let hello = decode_handshake_payload(&payload)?;
            if hello.kind() != HandshakeKind::ClientHello {
                return Ok(false);
            }
            let accepted = hello.operation_schema() == WORKSPACE_PROTOCOL_SCHEMA;
            let response = WorkspaceHandshake::new(
                if accepted {
                    HandshakeKind::Accepted
                } else {
                    HandshakeKind::Incompatible
                },
                WORKSPACE_PROTOCOL_SCHEMA,
            )
            .expect("the compiled workspace protocol schema fits the handshake");
            write_all_until(stream, &encode_handshake_frame(&response)?, deadline)
                .map_err(ServerError::Connection)?;
            return Ok(accepted);
        }
        write_legacy_rejection_until(stream, &payload, deadline)?;
        return Ok(false);
    }

    let mut first_frame = vec![0_u8; length];
    read_exact_until(stream, &mut first_frame, deadline).map_err(ServerError::Connection)?;
    write_legacy_rejection_until(stream, &first_frame, deadline)?;
    Ok(false)
}

fn write_legacy_rejection_until(
    stream: &mut TcpStream,
    first_frame: &[u8],
    deadline: Instant,
) -> Result<(), ServerError> {
    let Some(response) = legacy_rejection_response(first_frame) else {
        return Ok(());
    };
    let length = u32::try_from(response.len())
        .map_err(|_| ServerError::InvalidOptions("frame length exceeds u32".to_owned()))?;
    write_all_until(stream, &length.to_be_bytes(), deadline).map_err(ServerError::Connection)?;
    write_all_until(stream, &response, deadline).map_err(ServerError::Connection)
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
                    "workspace RPC first frame ended early",
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

fn read_frame(reader: &mut impl Read) -> Result<Option<Vec<u8>>, ServerError> {
    let mut length = [0_u8; 4];
    loop {
        match reader.read(&mut length[..1]) {
            Ok(0) => return Ok(None),
            Ok(1) => break,
            Ok(_) => unreachable!("one-byte read cannot return more than one byte"),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(ServerError::Connection(error)),
        }
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
    use std::net::Shutdown;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::mpsc;

    use nokv_protocol::{
        decode_handshake_frame, decode_response, encode_handshake_frame, encode_request,
        CreateWorkspaceRequest, HandshakeKind, LogicalShardIdentity, ObjectNamespaceIdentity,
        RequestIdentity, RootIdentity, RootRoute, RpcFailure, WorkbenchName, WorkspaceHandshake,
        WorkspaceIdentity, WorkspaceRequest, WorkspaceResult, WorkspaceRpcOutcome,
        WorkspaceRpcRequest, WorkspaceSummary, HANDSHAKE_FRAME_BYTES, HANDSHAKE_PAYLOAD_BYTES,
    };
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{ExecutedRequest, WorkspaceRequestExecutor};

    struct CountingExecutor {
        calls: AtomicUsize,
    }

    impl WorkspaceRequestExecutor for CountingExecutor {
        fn execute(&self, request: &WorkspaceRpcRequest) -> Result<ExecutedRequest, RpcFailure> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            let WorkspaceRequest::CreateWorkspace(create) = &request.operation else {
                panic!("test executor received an unexpected operation");
            };
            Ok(ExecutedRequest {
                result: WorkspaceResult::Workspace(WorkspaceSummary {
                    workbench: create.workbench.clone(),
                    workspace_incarnation_id: create.workspace_incarnation_id,
                    workspace_revision: 0,
                    commit_head: None,
                    commit_head_generation: None,
                }),
                commit_version: Some(9),
                replayed: false,
            })
        }
    }

    fn options() -> ServerOptions {
        ServerOptions {
            bind: "127.0.0.1:0".parse().unwrap(),
            handshake_timeout: Duration::from_millis(100),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
            lease_renew_interval: Duration::from_secs(1),
            max_inflight_connections: 8,
        }
    }

    fn route() -> RootRoute {
        RootRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: ObjectNamespaceIdentity([3; 16]),
            placement_generation: 7,
            owner_epoch: 11,
        }
    }

    fn request() -> WorkspaceRpcRequest {
        WorkspaceRpcRequest {
            route: route(),
            request_id: RequestIdentity([4; 16]),
            operation: WorkspaceRequest::CreateWorkspace(CreateWorkspaceRequest {
                workbench: WorkbenchName::new("run-42").unwrap(),
                workspace_incarnation_id: WorkspaceIdentity([5; 16]),
            }),
        }
    }

    fn streams() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    fn registry() -> (Arc<RootOwnerRegistry>, Arc<CountingExecutor>) {
        let registry = Arc::new(RootOwnerRegistry::new());
        let executor = Arc::new(CountingExecutor {
            calls: AtomicUsize::new(0),
        });
        registry.install(route(), executor.clone()).unwrap();
        (registry, executor)
    }

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

    #[test]
    fn exact_handshake_preserves_multiple_operation_frames_on_one_connection() {
        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let serving = thread::spawn(move || serve_connection(server, registry, options()));

        let hello =
            WorkspaceHandshake::new(HandshakeKind::ClientHello, WORKSPACE_PROTOCOL_SCHEMA).unwrap();
        client
            .write_all(&encode_handshake_frame(&hello).unwrap())
            .unwrap();
        let mut accepted = [0_u8; HANDSHAKE_FRAME_BYTES];
        client.read_exact(&mut accepted).unwrap();
        let accepted = decode_handshake_frame(&accepted).unwrap();
        assert_eq!(accepted.kind(), HandshakeKind::Accepted);
        assert_eq!(accepted.operation_schema(), WORKSPACE_PROTOCOL_SCHEMA);

        for _ in 0..2 {
            write_frame(&mut client, &encode_request(&request()).unwrap()).unwrap();
            let response = read_frame(&mut client).unwrap().unwrap();
            let response = decode_response(&response).unwrap();
            assert!(matches!(response.outcome, WorkspaceRpcOutcome::Success(_)));
        }
        client.shutdown(Shutdown::Write).unwrap();
        serving.join().unwrap().unwrap();
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 2);
    }

    #[test]
    fn operation_first_v3_gets_a_readable_rejection_with_zero_dispatch() {
        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let serving = thread::spawn(move || serve_connection(server, registry, options()));

        let mut legacy = encode_request(&request()).unwrap();
        let position = legacy
            .windows(WORKSPACE_PROTOCOL_SCHEMA.len())
            .position(|window| window == WORKSPACE_PROTOCOL_SCHEMA.as_bytes())
            .unwrap();
        legacy[position..position + WORKSPACE_PROTOCOL_SCHEMA.len()]
            .copy_from_slice(crate::legacy_rejection::LEGACY_V3_SCHEMA.as_bytes());
        write_frame(&mut client, &legacy).unwrap();
        let response = read_frame(&mut client).unwrap().unwrap();
        let response = decode_response(&response).unwrap();
        let WorkspaceRpcOutcome::Failure(failure) = response.outcome else {
            panic!("legacy operation-first request must be rejected");
        };
        assert_eq!(failure.code, nokv_protocol::ErrorCode::PreconditionFailed);
        serving.join().unwrap().unwrap();
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn operation_first_public_v2_gets_a_readable_rejection_with_zero_dispatch() {
        #[derive(Serialize)]
        struct V2Frame<'a> {
            schema: &'static str,
            payload: V2Request<'a>,
        }
        #[derive(Serialize)]
        struct V2Request<'a> {
            route: V2Route,
            request_id: [u8; 16],
            operation: &'a WorkspaceRequest,
        }
        #[derive(Clone, Copy, Serialize)]
        struct V2Route {
            root_id: [u8; 16],
            logical_shard_id: [u8; 16],
            placement_generation: u64,
            owner_epoch: u64,
        }
        #[derive(Deserialize)]
        struct V2ResponseFrame {
            schema: String,
            #[serde(rename = "payload")]
            _payload: serde::de::IgnoredAny,
        }

        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let serving = thread::spawn(move || serve_connection(server, registry, options()));
        let operation = request().operation;
        let encoded = rmp_serde::to_vec_named(&V2Frame {
            schema: crate::legacy_rejection::LEGACY_V2_SCHEMA,
            payload: V2Request {
                route: V2Route {
                    root_id: [1; 16],
                    logical_shard_id: [2; 16],
                    placement_generation: 7,
                    owner_epoch: 11,
                },
                request_id: [4; 16],
                operation: &operation,
            },
        })
        .unwrap();
        write_frame(&mut client, &encoded).unwrap();
        let response = read_frame(&mut client).unwrap().unwrap();
        let decoded: V2ResponseFrame = rmp_serde::from_slice(&response).unwrap();
        assert_eq!(decoded.schema, crate::legacy_rejection::LEGACY_V2_SCHEMA);
        assert!(response
            .windows(crate::legacy_rejection::LEGACY_CLIENT_UPGRADE_MESSAGE.len())
            .any(|window| {
                window == crate::legacy_rejection::LEGACY_CLIENT_UPGRADE_MESSAGE.as_bytes()
            }));
        serving.join().unwrap().unwrap();
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn v7_hello_gets_v9_incompatible_and_zero_dispatch() {
        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let serving = thread::spawn(move || serve_connection(server, registry, options()));

        let hello =
            WorkspaceHandshake::new(HandshakeKind::ClientHello, "nokv.workspace.rpc.v7").unwrap();
        client
            .write_all(&encode_handshake_frame(&hello).unwrap())
            .unwrap();
        let mut response = [0_u8; HANDSHAKE_FRAME_BYTES];
        client.read_exact(&mut response).unwrap();
        let response = decode_handshake_frame(&response).unwrap();
        assert_eq!(response.kind(), HandshakeKind::Incompatible);
        assert_eq!(response.operation_schema(), WORKSPACE_PROTOCOL_SCHEMA);
        assert_eq!(client.read(&mut [0_u8; 1]).unwrap(), 0);
        serving.join().unwrap().unwrap();
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn matched_but_corrupt_handshake_magic_never_enters_legacy_classification() {
        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let serving = thread::spawn(move || serve_connection(server, registry, options()));

        let mut corrupt = [0_u8; HANDSHAKE_PAYLOAD_BYTES];
        corrupt[..8].copy_from_slice(b"NOKVHS1\0");
        corrupt[8] = HandshakeKind::ClientHello as u8;
        corrupt[9] = WORKSPACE_PROTOCOL_SCHEMA.len() as u8;
        corrupt[10] = 1;
        corrupt[16..16 + WORKSPACE_PROTOCOL_SCHEMA.len()]
            .copy_from_slice(WORKSPACE_PROTOCOL_SCHEMA.as_bytes());
        client
            .write_all(&(HANDSHAKE_PAYLOAD_BYTES as u32).to_be_bytes())
            .unwrap();
        client.write_all(&corrupt).unwrap();
        assert_eq!(client.read(&mut [0_u8; 1]).unwrap(), 0);
        assert!(serving.join().unwrap().is_err());
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn handshake_deadline_cannot_be_extended_by_partial_prefix_reads() {
        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let (completed_tx, completed_rx) = mpsc::channel();
        let serving = thread::spawn(move || {
            let result = serve_connection(server, registry, options());
            completed_tx.send(()).unwrap();
            result
        });

        client.write_all(&[0]).unwrap();
        completed_rx
            .recv_timeout(Duration::from_millis(400))
            .expect("absolute handshake deadline must close a partial first frame");
        drop(client);
        assert!(serving.join().unwrap().is_err());
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn global_connection_permit_is_released_by_raii() {
        let limiter = Arc::new(ConnectionLimiter::new(1).unwrap());
        let permit = limiter.try_acquire().unwrap();
        assert!(limiter.try_acquire().is_none());
        drop(permit);
        assert!(limiter.try_acquire().is_some());
    }

    #[test]
    fn graceful_shutdown_before_accept_returns_without_admitting_a_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let shutdown = AtomicBool::new(true);
        let owner_loss = OwnerLossSignal::default();
        let (registry, _) = registry();
        let renewals = AtomicUsize::new(0);

        serve_socket_loop(listener, options(), registry, owner_loss, &shutdown, || {
            renewals.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        })
        .unwrap();

        assert_eq!(renewals.load(AtomicOrdering::SeqCst), 0);
    }

    #[test]
    fn graceful_shutdown_drains_accepted_connections_while_renewing_ownership() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let owner_loss = OwnerLossSignal::default();
        let (registry, _) = registry();
        let renewals = Arc::new(AtomicUsize::new(0));
        let mut runtime_options = options();
        runtime_options.read_timeout = Duration::from_secs(5);
        runtime_options.lease_renew_interval = Duration::from_millis(5);
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_renewals = Arc::clone(&renewals);
        let (completed_tx, completed_rx) = mpsc::channel();
        let (renewed_tx, renewed_rx) = mpsc::channel();
        let serving = thread::spawn(move || {
            let result = serve_socket_loop(
                listener,
                runtime_options,
                registry,
                owner_loss,
                worker_shutdown.as_ref(),
                || {
                    let count = worker_renewals.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                    renewed_tx.send(count).unwrap();
                    Ok(())
                },
            );
            completed_tx.send(()).unwrap();
            result
        });

        let mut client = TcpStream::connect(address).unwrap();
        let hello =
            WorkspaceHandshake::new(HandshakeKind::ClientHello, WORKSPACE_PROTOCOL_SCHEMA).unwrap();
        client
            .write_all(&encode_handshake_frame(&hello).unwrap())
            .unwrap();
        let mut accepted = [0_u8; HANDSHAKE_FRAME_BYTES];
        client.read_exact(&mut accepted).unwrap();
        assert_eq!(renewed_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        shutdown.store(true, AtomicOrdering::Release);

        assert!(renewed_rx.recv_timeout(Duration::from_secs(1)).unwrap() > 1);
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        client.shutdown(Shutdown::Both).unwrap();
        completed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("drain must finish after the accepted connection exits");
        serving.join().unwrap().unwrap();
    }

    #[test]
    fn owner_loss_precedes_an_already_requested_graceful_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let shutdown = AtomicBool::new(true);
        let owner_loss = OwnerLossSignal::default();
        owner_loss.fail_closed();
        let (registry, _) = registry();

        let error = serve_socket_loop(listener, options(), registry, owner_loss, &shutdown, || {
            Ok(())
        })
        .unwrap_err();

        assert!(error.to_string().contains("owner was lost"));
    }

    #[test]
    fn oversized_legacy_first_frame_closes_before_reading_or_dispatching_its_body() {
        let (mut client, server) = streams();
        let (registry, executor) = registry();
        let serving = thread::spawn(move || serve_connection(server, registry, options()));

        client
            .write_all(
                &u32::try_from(MAX_LEGACY_FIRST_FRAME_BYTES + 1)
                    .unwrap()
                    .to_be_bytes(),
            )
            .unwrap();
        assert_eq!(client.read(&mut [0_u8; 1]).unwrap(), 0);
        serving.join().unwrap().unwrap();
        assert_eq!(executor.calls.load(AtomicOrdering::SeqCst), 0);
    }
}
