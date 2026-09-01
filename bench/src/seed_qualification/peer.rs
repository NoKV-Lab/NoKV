/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use nokv_client::{FramedTcpOptions, FramedTcpTransport, RpcTransport, TransportError};
use nokv_protocol::{
    decode_handshake_frame, decode_request, encode_handshake_frame, encode_response, ConflictKind,
    DiscoverRouteOutcome, DiscoverRouteResponse, DiscoveredRoute, ErrorCode, HandshakeKind,
    RpcFailure, RpcRequest, RpcResponse, WorkspaceHandshake, WorkspaceRpcOutcome,
    WorkspaceRpcResponse, HANDSHAKE_FRAME_BYTES, MAX_FRAME_BYTES, WORKSPACE_PROTOCOL_SCHEMA,
};
use serde::Serialize;

use super::scenario::RouteEvidence;
use crate::qualification_runtime::sha256_bytes;

const PEER_IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Serialize)]
pub(super) struct PeerTranscript {
    pub sequence: u64,
    pub peer: String,
    pub request_kind: String,
    pub action: Option<String>,
    pub route: Option<RouteEvidence>,
    pub outcome: String,
    pub request_wire_sha256: Option<String>,
    pub response_wire_sha256: Option<String>,
}

#[derive(Default)]
struct PeerJournal {
    next_sequence: AtomicU64,
    entries: Mutex<Vec<PeerTranscript>>,
}

impl PeerJournal {
    fn push(&self, mut entry: PeerTranscript) {
        entry.sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        self.entries
            .lock()
            .expect("qualification peer journal lock is not poisoned")
            .push(entry);
    }

    fn snapshot(&self) -> Vec<PeerTranscript> {
        self.entries
            .lock()
            .expect("qualification peer journal lock is not poisoned")
            .clone()
    }

    fn saw_action(&self, action: &str) -> bool {
        self.entries
            .lock()
            .expect("qualification peer journal lock is not poisoned")
            .iter()
            .any(|entry| entry.action.as_deref() == Some(action))
    }
}

#[derive(Clone)]
struct DiscoveryAction {
    label: String,
    route: DiscoveredRoute,
}

struct DiscoveryState {
    default: Mutex<DiscoveredRoute>,
    actions: Mutex<VecDeque<DiscoveryAction>>,
    journal: Arc<PeerJournal>,
}

#[derive(Clone)]
pub(super) struct DiscoveryControl {
    state: Arc<DiscoveryState>,
}

impl DiscoveryControl {
    pub(super) fn set_default(&self, route: DiscoveredRoute) {
        *self
            .state
            .default
            .lock()
            .expect("qualification discovery default lock is not poisoned") = route;
    }

    pub(super) fn enqueue(
        &self,
        label: impl Into<String>,
        route: DiscoveredRoute,
    ) -> Result<String, String> {
        let label = label.into();
        if label.trim().is_empty() {
            return Err("qualification discovery action label must not be empty".to_owned());
        }
        let mut actions = self
            .state
            .actions
            .lock()
            .map_err(|_| "qualification discovery action lock is poisoned".to_owned())?;
        if actions.iter().any(|action| action.label == label)
            || self.state.journal.saw_action(&label)
        {
            return Err(format!(
                "qualification discovery action {label:?} is not unique"
            ));
        }
        actions.push_back(DiscoveryAction {
            label: label.clone(),
            route,
        });
        Ok(label)
    }

    pub(super) fn saw_action(&self, label: &str) -> bool {
        self.state.journal.saw_action(label)
    }
}

#[derive(Clone)]
struct ProxyInjection {
    label: String,
    hint: DiscoveredRoute,
}

struct ProxyState {
    target: SocketAddr,
    transport: FramedTcpTransport,
    injection: Mutex<Option<ProxyInjection>>,
    journal: Arc<PeerJournal>,
}

#[derive(Clone)]
pub(super) struct ProxyControl {
    state: Arc<ProxyState>,
}

impl ProxyControl {
    pub(super) fn inject_not_owner_once(
        &self,
        label: impl Into<String>,
        hint: DiscoveredRoute,
    ) -> Result<String, String> {
        let label = label.into();
        if label.trim().is_empty() {
            return Err("qualification proxy action label must not be empty".to_owned());
        }
        let mut injection = self
            .state
            .injection
            .lock()
            .map_err(|_| "qualification proxy injection lock is poisoned".to_owned())?;
        if injection.is_some() || self.state.journal.saw_action(&label) {
            return Err("qualification proxy already has a pending or used injection".to_owned());
        }
        *injection = Some(ProxyInjection {
            label: label.clone(),
            hint,
        });
        Ok(label)
    }

    pub(super) fn saw_action(&self, label: &str) -> bool {
        self.state.journal.saw_action(label)
    }
}

enum PeerMode {
    Discovery(Arc<DiscoveryState>),
    Proxy(Arc<ProxyState>),
}

pub(super) struct QualificationPeer {
    endpoint: SocketAddr,
    journal: Arc<PeerJournal>,
    shutdown: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl QualificationPeer {
    pub(super) fn start_discovery(
        name: impl Into<String>,
        endpoint: SocketAddr,
        default: DiscoveredRoute,
    ) -> Result<(Self, DiscoveryControl), String> {
        let journal = Arc::new(PeerJournal::default());
        let state = Arc::new(DiscoveryState {
            default: Mutex::new(default),
            actions: Mutex::new(VecDeque::new()),
            journal: Arc::clone(&journal),
        });
        let peer = Self::start(
            name.into(),
            endpoint,
            PeerMode::Discovery(Arc::clone(&state)),
            journal,
        )?;
        Ok((peer, DiscoveryControl { state }))
    }

    pub(super) fn start_proxy(
        name: impl Into<String>,
        endpoint: SocketAddr,
        target: SocketAddr,
        operation_timeout: Duration,
    ) -> Result<(Self, ProxyControl), String> {
        let journal = Arc::new(PeerJournal::default());
        let transport = FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: operation_timeout.min(Duration::from_secs(2)),
            handshake_timeout: operation_timeout,
            read_timeout: operation_timeout,
            write_timeout: operation_timeout,
        })
        .map_err(|error| format!("cannot create qualification proxy transport: {error}"))?;
        let state = Arc::new(ProxyState {
            target,
            transport,
            injection: Mutex::new(None),
            journal: Arc::clone(&journal),
        });
        let peer = Self::start(
            name.into(),
            endpoint,
            PeerMode::Proxy(Arc::clone(&state)),
            journal,
        )?;
        Ok((peer, ProxyControl { state }))
    }

    fn start(
        name: String,
        endpoint: SocketAddr,
        mode: PeerMode,
        journal: Arc<PeerJournal>,
    ) -> Result<Self, String> {
        let listener = TcpListener::bind(endpoint)
            .map_err(|error| format!("cannot bind qualification peer {endpoint}: {error}"))?;
        let endpoint = listener
            .local_addr()
            .map_err(|error| format!("cannot inspect qualification peer address: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("cannot prepare qualification peer listener: {error}"))?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_shutdown = Arc::clone(&shutdown);
        let worker_journal = Arc::clone(&journal);
        let worker = thread::Builder::new()
            .name(format!("nokv-qualification-{name}"))
            .spawn(move || {
                while !worker_shutdown.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            if worker_shutdown.load(Ordering::Acquire) {
                                break;
                            }
                            serve_connection(&name, stream, &mode, &worker_journal);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => {
                            worker_journal.push(PeerTranscript {
                                sequence: 0,
                                peer: name.clone(),
                                request_kind: "accept".to_owned(),
                                action: None,
                                route: None,
                                outcome: format!("error: {error}"),
                                request_wire_sha256: None,
                                response_wire_sha256: None,
                            });
                            break;
                        }
                    }
                }
            })
            .map_err(|error| format!("cannot start qualification peer thread: {error}"))?;
        Ok(Self {
            endpoint,
            journal,
            shutdown,
            worker: Some(worker),
        })
    }

    pub(super) fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(super) fn transcripts(&self) -> Vec<PeerTranscript> {
        self.journal.snapshot()
    }

    pub(super) fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.endpoint, Duration::from_millis(100));
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for QualificationPeer {
    fn drop(&mut self) {
        self.stop();
    }
}

fn serve_connection(name: &str, mut stream: TcpStream, mode: &PeerMode, journal: &PeerJournal) {
    let mut transcript = PeerTranscript {
        sequence: 0,
        peer: name.to_owned(),
        request_kind: "unknown".to_owned(),
        action: None,
        route: None,
        outcome: "error: connection ended before a response".to_owned(),
        request_wire_sha256: None,
        response_wire_sha256: None,
    };
    let result = serve_connection_result(&mut stream, mode, &mut transcript);
    transcript.outcome = match result {
        Ok(()) => "success".to_owned(),
        Err(error) => format!("error: {error}"),
    };
    journal.push(transcript);
}

fn serve_connection_result(
    stream: &mut TcpStream,
    mode: &PeerMode,
    transcript: &mut PeerTranscript,
) -> Result<(), String> {
    stream
        .set_nonblocking(false)
        .and_then(|()| stream.set_read_timeout(Some(PEER_IO_TIMEOUT)))
        .and_then(|()| stream.set_write_timeout(Some(PEER_IO_TIMEOUT)))
        .map_err(|error| format!("cannot configure peer connection: {error}"))?;
    let mut hello = [0_u8; HANDSHAKE_FRAME_BYTES];
    stream
        .read_exact(&mut hello)
        .map_err(|error| format!("cannot read client handshake: {error}"))?;
    let decoded = decode_handshake_frame(&hello)
        .map_err(|error| format!("invalid client handshake: {error}"))?;
    if decoded.kind() != HandshakeKind::ClientHello
        || decoded.operation_schema() != WORKSPACE_PROTOCOL_SCHEMA
    {
        return Err("client handshake does not match the workspace protocol".to_owned());
    }
    let accepted = WorkspaceHandshake::new(HandshakeKind::Accepted, WORKSPACE_PROTOCOL_SCHEMA)
        .map_err(|error| format!("cannot build accepted handshake: {error}"))?;
    let accepted = encode_handshake_frame(&accepted)
        .map_err(|error| format!("cannot encode accepted handshake: {error}"))?;
    stream
        .write_all(&accepted)
        .map_err(|error| format!("cannot write accepted handshake: {error}"))?;

    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .map_err(|error| format!("cannot read request length: {error}"))?;
    let length_value = usize::try_from(u32::from_be_bytes(length))
        .map_err(|_| "request length does not fit usize".to_owned())?;
    if length_value > MAX_FRAME_BYTES {
        return Err(format!("request exceeds {MAX_FRAME_BYTES} bytes"));
    }
    let mut payload = vec![0_u8; length_value];
    stream
        .read_exact(&mut payload)
        .map_err(|error| format!("cannot read request payload: {error}"))?;
    let mut request_wire = Vec::with_capacity(hello.len() + length.len() + payload.len());
    request_wire.extend_from_slice(&hello);
    request_wire.extend_from_slice(&length);
    request_wire.extend_from_slice(&payload);
    transcript.request_wire_sha256 = Some(sha256_bytes(&request_wire));

    let request = decode_request(&payload).map_err(|error| format!("invalid request: {error}"))?;
    transcript.request_kind = match &request {
        RpcRequest::DiscoverRoute(_) => "discover_route".to_owned(),
        RpcRequest::Workspace(_) => "workspace".to_owned(),
    };
    let response = match mode {
        PeerMode::Discovery(state) => discovery_response(state, request, transcript)?,
        PeerMode::Proxy(state) => proxy_response(state, request, &payload, transcript)?,
    };
    if response.len() > MAX_FRAME_BYTES {
        return Err(format!("response exceeds {MAX_FRAME_BYTES} bytes"));
    }
    let response_length = u32::try_from(response.len())
        .map_err(|_| "response length does not fit u32".to_owned())?
        .to_be_bytes();
    stream
        .write_all(&response_length)
        .and_then(|()| stream.write_all(&response))
        .and_then(|()| stream.flush())
        .map_err(|error| format!("cannot write peer response: {error}"))?;
    let mut response_wire =
        Vec::with_capacity(accepted.len() + response_length.len() + response.len());
    response_wire.extend_from_slice(&accepted);
    response_wire.extend_from_slice(&response_length);
    response_wire.extend_from_slice(&response);
    transcript.response_wire_sha256 = Some(sha256_bytes(&response_wire));
    Ok(())
}

fn discovery_response(
    state: &DiscoveryState,
    request: RpcRequest,
    transcript: &mut PeerTranscript,
) -> Result<Vec<u8>, String> {
    let RpcRequest::DiscoverRoute(request) = request else {
        return Err("discovery peer received a workspace request".to_owned());
    };
    let action = state
        .actions
        .lock()
        .map_err(|_| "qualification discovery action lock is poisoned".to_owned())?
        .pop_front();
    let (route, label) = match action {
        Some(action) => (action.route, Some(action.label)),
        None => (
            state
                .default
                .lock()
                .map_err(|_| "qualification discovery default lock is poisoned".to_owned())?
                .clone(),
            None,
        ),
    };
    transcript.action = label;
    transcript.route = Some(RouteEvidence::from(&route));
    encode_response(&RpcResponse::DiscoverRoute(DiscoverRouteResponse {
        root_id: request.root_id,
        outcome: DiscoverRouteOutcome::Found(route),
    }))
    .map_err(|error| format!("cannot encode discovery response: {error}"))
}

fn proxy_response(
    state: &ProxyState,
    request: RpcRequest,
    payload: &[u8],
    transcript: &mut PeerTranscript,
) -> Result<Vec<u8>, String> {
    if let RpcRequest::Workspace(request) = &request {
        if let Some(injection) = state
            .injection
            .lock()
            .map_err(|_| "qualification proxy injection lock is poisoned".to_owned())?
            .take()
        {
            transcript.action = Some(injection.label);
            transcript.route = Some(RouteEvidence::from(&injection.hint));
            let response = WorkspaceRpcResponse {
                route: request.route,
                request_id: request.request_id,
                commit_version: None,
                replayed: false,
                outcome: WorkspaceRpcOutcome::Failure(RpcFailure {
                    code: ErrorCode::NotOwner,
                    message: "qualification peer injected a stale owner hint".to_owned(),
                    retryable: true,
                    conflict: Some(ConflictKind::RootPlacement),
                    current_generation: None,
                    route_hint: Some(Box::new(injection.hint)),
                }),
            };
            return encode_response(&RpcResponse::Workspace(Box::new(response)))
                .map_err(|error| format!("cannot encode injected NotOwner response: {error}"));
        }
    }
    state
        .transport
        .round_trip(state.target, payload)
        .map_err(|error| format!("owner proxy forward failed: {error}"))
}

#[derive(Clone, Debug, Serialize)]
pub(super) struct TransportEvent {
    pub sequence: u64,
    pub endpoint: SocketAddr,
    pub request_kind: String,
    pub outcome: String,
    pub request_sha256: String,
    pub response_sha256: Option<String>,
    pub elapsed_millis: u128,
}

#[derive(Default)]
struct TransportJournal {
    next_sequence: AtomicU64,
    events: Mutex<Vec<TransportEvent>>,
}

#[derive(Clone)]
pub(super) struct RecordingTransport {
    inner: FramedTcpTransport,
    journal: Arc<TransportJournal>,
}

impl RecordingTransport {
    pub(super) fn new(timeout: Duration) -> Result<Self, String> {
        let inner = FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: timeout.min(Duration::from_secs(2)),
            handshake_timeout: timeout,
            read_timeout: timeout,
            write_timeout: timeout,
        })
        .map_err(|error| format!("cannot create qualification client transport: {error}"))?;
        Ok(Self {
            inner,
            journal: Arc::new(TransportJournal::default()),
        })
    }

    pub(super) fn events(&self) -> Vec<TransportEvent> {
        self.journal
            .events
            .lock()
            .expect("qualification transport journal lock is not poisoned")
            .clone()
    }
}

impl RpcTransport for RecordingTransport {
    fn round_trip(&self, endpoint: SocketAddr, request: &[u8]) -> Result<Vec<u8>, TransportError> {
        let request_kind = match decode_request(request) {
            Ok(RpcRequest::DiscoverRoute(_)) => "discover_route".to_owned(),
            Ok(RpcRequest::Workspace(_)) => "workspace".to_owned(),
            Err(_) => "invalid".to_owned(),
        };
        let started = Instant::now();
        let result = self.inner.round_trip(endpoint, request);
        let (outcome, response_sha256) = match &result {
            Ok(response) => ("success".to_owned(), Some(sha256_bytes(response))),
            Err(error) => (format!("error: {error}"), None),
        };
        let event = TransportEvent {
            sequence: self.journal.next_sequence.fetch_add(1, Ordering::Relaxed),
            endpoint,
            request_kind,
            outcome,
            request_sha256: sha256_bytes(request),
            response_sha256,
            elapsed_millis: started.elapsed().as_millis(),
        };
        self.journal
            .events
            .lock()
            .expect("qualification transport journal lock is not poisoned")
            .push(event);
        result
    }
}

#[cfg(test)]
mod tests {
    use nokv_protocol::{
        decode_response, encode_request, DiscoverRouteRequest, LogicalShardIdentity,
        ObjectNamespaceIdentity, OwnerEndpoint, RootIdentity, RouteState,
    };

    use super::*;

    fn route() -> DiscoveredRoute {
        DiscoveredRoute {
            root_id: RootIdentity([1; 16]),
            logical_shard_id: LogicalShardIdentity([2; 16]),
            object_namespace_id: ObjectNamespaceIdentity([3; 16]),
            placement_generation: 4,
            owner_epoch: 5,
            session_generation: 6,
            owner_endpoint: OwnerEndpoint::new("127.0.0.1:17001").unwrap(),
            route_state: RouteState::Serving,
        }
    }

    #[test]
    fn discovery_peer_uses_the_real_handshake_and_codec() {
        let expected = route();
        let (mut peer, _) = QualificationPeer::start_discovery(
            "test-discovery",
            "127.0.0.1:0".parse().unwrap(),
            expected.clone(),
        )
        .unwrap();
        let transport = FramedTcpTransport::new(FramedTcpOptions {
            connect_timeout: Duration::from_secs(1),
            handshake_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_secs(1),
        })
        .unwrap();
        let request = encode_request(&RpcRequest::DiscoverRoute(DiscoverRouteRequest {
            root_id: expected.root_id,
        }))
        .unwrap();
        let response = transport
            .round_trip(peer.endpoint(), &request)
            .unwrap_or_else(|error| panic!("{error}; transcripts: {:?}", peer.transcripts()));
        let RpcResponse::DiscoverRoute(response) = decode_response(&response).unwrap() else {
            panic!("qualification peer returned the wrong response kind");
        };
        assert_eq!(response.outcome, DiscoverRouteOutcome::Found(expected));
        assert_eq!(peer.transcripts().len(), 1);
        peer.stop();
    }

    #[test]
    fn queued_discovery_action_is_consumed_once() {
        let initial = route();
        let (mut peer, control) = QualificationPeer::start_discovery(
            "test-discovery-action",
            "127.0.0.1:0".parse().unwrap(),
            initial.clone(),
        )
        .unwrap();
        let mut stale = initial;
        stale.owner_epoch = 4;
        let label = control.enqueue("stale", stale).unwrap();
        let transport = RecordingTransport::new(Duration::from_secs(1)).unwrap();
        let request = encode_request(&RpcRequest::DiscoverRoute(DiscoverRouteRequest {
            root_id: route().root_id,
        }))
        .unwrap();
        transport
            .round_trip(peer.endpoint(), &request)
            .unwrap_or_else(|error| panic!("{error}; transcripts: {:?}", peer.transcripts()));
        assert!(control.saw_action(&label));
        peer.stop();
    }
}
