/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Optional HTTP health surface for the logical-shard owner process.
//!
//! Three endpoints, one connection at a time, no external HTTP dependency:
//!
//! * `GET /healthz` — process liveness;
//! * `GET /readyz` — admission is complete, the owner is not draining, and
//!   the control-plane lease has not been lost;
//! * `GET /stats`  — one JSON snapshot of the counters below.
//!
//! Graceful shutdown marks the owner draining before the accept loop stops,
//! so readiness drops to 503 first, then admission stops, in-flight
//! connections drain while ownership is still renewed, and the lease is
//! released by the caller afterwards.
//!
//! The health listener is advisory once serving: per-connection failures never
//! fail the RPC server. Binding and thread startup do fail the server before
//! it begins serving, so a configured-but-unusable health endpoint cannot
//! pass silently.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::server::OwnerLossSignal;
use crate::ServerError;

const MAX_REQUEST_BYTES: usize = 4096;
const HEALTH_IO_TIMEOUT: Duration = Duration::from_secs(2);
const HEALTH_POLL_INTERVAL: Duration = Duration::from_millis(10);
const HEALTHZ_BODY: &str = "ok\n";
const READYZ_BODY: &str = "ready\n";
const NOT_READY_BODY: &str = "not ready\n";
const NOT_FOUND_BODY: &str = "not found\n";

#[derive(Clone)]
pub(crate) struct HealthState {
    inner: Arc<HealthInner>,
}

struct HealthInner {
    started: Instant,
    pid: u32,
    protocol_schema: &'static str,
    owner_loss: OwnerLossSignal,
    draining: AtomicBool,
    connections_total: AtomicU64,
    requests_total: AtomicU64,
    inflight: Arc<dyn Fn() -> usize + Send + Sync>,
    installed_roots: Arc<dyn Fn() -> Result<usize, ServerError> + Send + Sync>,
}

impl HealthState {
    pub(crate) fn new(
        protocol_schema: &'static str,
        owner_loss: OwnerLossSignal,
        inflight: Arc<dyn Fn() -> usize + Send + Sync>,
        installed_roots: Arc<dyn Fn() -> Result<usize, ServerError> + Send + Sync>,
    ) -> Self {
        Self {
            inner: Arc::new(HealthInner {
                started: Instant::now(),
                pid: std::process::id(),
                protocol_schema,
                owner_loss,
                draining: AtomicBool::new(false),
                connections_total: AtomicU64::new(0),
                requests_total: AtomicU64::new(0),
                inflight,
                installed_roots,
            }),
        }
    }

    pub(crate) fn record_connection(&self) {
        self.inner.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_request(&self) {
        self.inner.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Graceful shutdown has been requested: the owner keeps renewing its
    /// lease while draining, but it stops admitting work, so readiness drops
    /// immediately and stays down until the process exits.
    pub(crate) fn set_draining(&self) {
        self.inner.draining.store(true, Ordering::Release);
    }

    fn snapshot(&self) -> HealthSnapshot {
        let owner_loss = self.inner.owner_loss.is_lost();
        let draining = self.inner.draining.load(Ordering::Acquire);
        HealthSnapshot {
            pid: self.inner.pid,
            uptime_seconds: self.inner.started.elapsed().as_secs_f64(),
            protocol_schema: self.inner.protocol_schema,
            installed_roots: (self.inner.installed_roots)().ok(),
            owner_loss,
            draining,
            ready: !owner_loss && !draining,
            connections_total: self.inner.connections_total.load(Ordering::Relaxed),
            requests_total: self.inner.requests_total.load(Ordering::Relaxed),
            inflight_connections: (self.inner.inflight)(),
        }
    }
}

struct HealthSnapshot {
    pid: u32,
    uptime_seconds: f64,
    protocol_schema: &'static str,
    installed_roots: Option<usize>,
    owner_loss: bool,
    draining: bool,
    ready: bool,
    connections_total: u64,
    requests_total: u64,
    inflight_connections: usize,
}

impl HealthSnapshot {
    /// Hand-built JSON. Every field is numeric, boolean, or the compiled
    /// protocol-schema constant, so no field can carry an escape.
    fn json(&self) -> String {
        let installed_roots = self
            .installed_roots
            .map_or_else(|| "null".to_owned(), |count| count.to_string());
        format!(
            "{{\"pid\":{},\"uptime_seconds\":{:.3},\"protocol_schema\":\"{}\",\
             \"installed_roots\":{},\"owner_loss\":{},\"draining\":{},\"ready\":{},\
             \"connections_total\":{},\"requests_total\":{},\
             \"inflight_connections\":{}}}\n",
            self.pid,
            self.uptime_seconds,
            self.protocol_schema,
            installed_roots,
            self.owner_loss,
            self.draining,
            self.ready,
            self.connections_total,
            self.requests_total,
            self.inflight_connections,
        )
    }
}

pub(crate) fn serve_health(listener: TcpListener, state: HealthState, stop: Arc<AtomicBool>) {
    if listener.set_nonblocking(true).is_err() {
        return;
    }
    loop {
        if stop.load(Ordering::Acquire) {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => handle_connection(stream, &state),
            Err(_) => thread::sleep(HEALTH_POLL_INTERVAL),
        }
    }
}

fn handle_connection(mut stream: TcpStream, state: &HealthState) {
    // On BSD-derived platforms `accept` inherits the listener's non-blocking
    // mode; restore blocking reads so a client that has not finished writing
    // does not hit an immediate EAGAIN and get reset.
    let _ = stream.set_nonblocking(false);
    let _ = stream.set_read_timeout(Some(HEALTH_IO_TIMEOUT));
    let _ = stream.set_write_timeout(Some(HEALTH_IO_TIMEOUT));
    let mut buffer = [0_u8; 1024];
    let mut request = Vec::new();
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return,
            Ok(read) => {
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n")
                    || request.len() >= MAX_REQUEST_BYTES
                {
                    break;
                }
            }
            Err(_) => return,
        }
    }
    let snapshot = state.snapshot();
    let (status, content_type, body) = route(&request, &snapshot);
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

fn route(request: &[u8], snapshot: &HealthSnapshot) -> (&'static str, &'static str, String) {
    let Some(first_line_end) = request.iter().position(|byte| *byte == b'\n') else {
        return (
            "400 Bad Request",
            "text/plain; charset=utf-8",
            "bad request\n".to_owned(),
        );
    };
    let first_line = &request[..first_line_end];
    let mut fields = first_line.split(|byte| *byte == b' ');
    let method = fields.next().unwrap_or_default();
    let path = fields.next().unwrap_or_default();
    if method != b"GET" {
        return (
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            "method not allowed\n".to_owned(),
        );
    }
    match path {
        b"/healthz" => (
            "200 OK",
            "text/plain; charset=utf-8",
            HEALTHZ_BODY.to_owned(),
        ),
        b"/readyz" => {
            if snapshot.ready {
                (
                    "200 OK",
                    "text/plain; charset=utf-8",
                    READYZ_BODY.to_owned(),
                )
            } else {
                (
                    "503 Service Unavailable",
                    "text/plain; charset=utf-8",
                    NOT_READY_BODY.to_owned(),
                )
            }
        }
        b"/stats" => ("200 OK", "application/json", snapshot.json()),
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            NOT_FOUND_BODY.to_owned(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn state() -> (HealthState, OwnerLossSignal) {
        let owner_loss = OwnerLossSignal::default();
        let inflight = Arc::new(AtomicUsize::new(2));
        let inflight_provider = {
            let inflight = Arc::clone(&inflight);
            Arc::new(move || inflight.load(Ordering::Relaxed))
                as Arc<dyn Fn() -> usize + Send + Sync>
        };
        let state = HealthState::new(
            "nokv.workspace.rpc.test",
            owner_loss.clone(),
            inflight_provider,
            Arc::new(|| Ok(3)),
        );
        (state, owner_loss)
    }

    fn request(port: u16, state: HealthState, path: &str) -> (u16, String) {
        let listener = TcpListener::bind(("127.0.0.1", port)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || serve_health(listener, state, stop))
        };
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .unwrap();
        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
        stop.store(true, Ordering::Release);
        worker.join().unwrap();
        let status: u16 = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .expect("health response must carry an HTTP status line");
        let body = response
            .split("\r\n\r\n")
            .nth(1)
            .unwrap_or_default()
            .to_owned();
        (status, body)
    }

    #[test]
    fn healthz_is_always_alive() {
        let (state, _) = state();
        let (status, body) = request(0, state, "/healthz");
        assert_eq!(status, 200);
        assert_eq!(body, HEALTHZ_BODY);
    }

    #[test]
    fn readyz_is_ready_until_owner_loss() {
        let (state, owner_loss) = state();
        let (status, body) = request(0, state.clone(), "/readyz");
        assert_eq!(status, 200);
        assert_eq!(body, READYZ_BODY);

        owner_loss.fail_closed();
        let (status, body) = request(0, state, "/readyz");
        assert_eq!(status, 503);
        assert_eq!(body, NOT_READY_BODY);
    }

    #[test]
    fn readyz_drops_when_draining_begins() {
        let (state, _) = state();
        let (status, body) = request(0, state.clone(), "/readyz");
        assert_eq!(status, 200);
        assert_eq!(body, READYZ_BODY);

        state.set_draining();
        let (status, body) = request(0, state, "/readyz");
        assert_eq!(status, 503);
        assert_eq!(body, NOT_READY_BODY);
    }

    #[test]
    fn stats_snapshot_tracks_counters_and_identity() {
        let (state, owner_loss) = state();
        state.record_connection();
        state.record_request();
        let (status, body) = request(0, state.clone(), "/stats");
        assert_eq!(status, 200);
        assert!(body.contains("\"protocol_schema\":\"nokv.workspace.rpc.test\""));
        assert!(body.contains("\"installed_roots\":3"));
        assert!(body.contains("\"owner_loss\":false"));
        assert!(body.contains("\"draining\":false"));
        assert!(body.contains("\"ready\":true"));
        assert!(body.contains("\"connections_total\":1"));
        assert!(body.contains("\"requests_total\":1"));
        assert!(body.contains("\"inflight_connections\":2"));
        assert!(body.contains("\"pid\":") && !body.contains("\"pid\":0,"));

        owner_loss.fail_closed();
        let (status, body) = request(0, state, "/stats");
        assert_eq!(status, 200);
        assert!(body.contains("\"owner_loss\":true"));
        assert!(body.contains("\"ready\":false"));
    }

    #[test]
    fn unknown_path_and_method_are_rejected() {
        let (state, _) = state();
        let (status, _) = request(0, state.clone(), "/nope");
        assert_eq!(status, 404);

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || serve_health(listener, state, stop))
        };
        let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        stream
            .write_all(b"POST /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        let _ = stream.read_to_string(&mut response);
        stop.store(true, Ordering::Release);
        worker.join().unwrap();
        let status: u16 = response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .unwrap();
        assert_eq!(status, 405);
    }

    #[test]
    fn zero_length_request_fails_closed_without_a_response() {
        let (state, _) = state();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let stop = Arc::clone(&stop);
            thread::spawn(move || serve_health(listener, state, stop))
        };
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        stop.store(true, Ordering::Release);
        worker.join().unwrap();
    }
}
