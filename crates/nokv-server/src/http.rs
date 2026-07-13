use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::time::Duration;

use crate::rpc;
use crate::server::{Server, ServerError};

const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONTROL_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct HttpRequest {
    line: String,
    body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HttpResponse {
    pub(crate) status: &'static str,
    pub(crate) content_type: &'static str,
    pub(crate) body: String,
    keep_alive: bool,
}

pub(crate) fn handle_stream(server: Arc<Server>, mut stream: TcpStream) -> Result<(), ServerError> {
    stream
        .set_read_timeout(Some(CONTROL_IO_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .set_write_timeout(Some(CONTROL_IO_TIMEOUT))
        .map_err(ServerError::Io)?;
    let mut prefix = [0_u8; rpc::FRAMED_RPC_MAGIC.len()];
    match stream.read_exact(&mut prefix) {
        Ok(()) if &prefix == rpc::FRAMED_RPC_MAGIC => {
            return rpc::handle_framed_stream_after_magic(server, stream);
        }
        Ok(()) => {}
        Err(err) if is_idle_timeout(&err) || err.kind() == io::ErrorKind::UnexpectedEof => {
            return Ok(());
        }
        Err(err) => return Err(ServerError::Io(err)),
    }
    let mut initial = Some(prefix.to_vec());
    loop {
        let request =
            match read_request_with_initial(&mut stream, initial.take().unwrap_or_default()) {
                Ok(request) => request,
                Err(ServerError::Io(err)) if is_idle_timeout(&err) => return Ok(()),
                Err(err) => return Err(err),
            };
        if request.line.is_empty() && request.body.is_empty() {
            return Ok(());
        }
        let response = handle_request(server.as_ref(), &request);
        write_response(&mut stream, &response).map_err(ServerError::Io)?;
        if !response.keep_alive {
            return Ok(());
        }
    }
}

fn read_request_with_initial(
    stream: &mut TcpStream,
    initial: Vec<u8>,
) -> Result<HttpRequest, ServerError> {
    let mut bytes = initial;
    let mut buf = [0_u8; 4096];
    loop {
        if bytes.is_empty() || header_end_and_content_len(&bytes).is_none() {
            let read = stream.read(&mut buf).map_err(ServerError::Io)?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&buf[..read]);
        }
        if bytes.len() > MAX_CONTROL_REQUEST_BYTES {
            return Ok(HttpRequest {
                line: "GET /request-too-large HTTP/1.1".to_owned(),
                body: Vec::new(),
            });
        }
        if let Some((header_end, content_len)) = header_end_and_content_len(&bytes) {
            let body_start = header_end + 4;
            let expected = body_start.saturating_add(content_len);
            while bytes.len() < expected {
                let read = stream.read(&mut buf).map_err(ServerError::Io)?;
                if read == 0 {
                    break;
                }
                bytes.extend_from_slice(&buf[..read]);
                if bytes.len() > MAX_CONTROL_REQUEST_BYTES {
                    return Ok(HttpRequest {
                        line: "GET /request-too-large HTTP/1.1".to_owned(),
                        body: Vec::new(),
                    });
                }
            }
            let header = String::from_utf8_lossy(&bytes[..header_end]);
            let line = header.lines().next().unwrap_or_default().to_owned();
            let body_end = bytes.len().min(expected);
            return Ok(HttpRequest {
                line,
                body: bytes[body_start..body_end].to_vec(),
            });
        }
    }

    let line = String::from_utf8_lossy(&bytes)
        .lines()
        .next()
        .unwrap_or_default()
        .to_owned();
    Ok(HttpRequest {
        line,
        body: Vec::new(),
    })
}

fn header_end_and_content_len(bytes: &[u8]) -> Option<(usize, usize)> {
    let header_end = bytes.windows(4).position(|window| window == b"\r\n\r\n")?;
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let mut content_len = 0_usize;
    for line in header.lines().skip(1) {
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        if key.eq_ignore_ascii_case("content-length") {
            content_len = value.trim().parse().unwrap_or(0);
        }
    }
    Some((header_end, content_len))
}

fn handle_request(server: &Server, request: &HttpRequest) -> HttpResponse {
    handle_parts(server, &request.line, &request.body)
}

pub(crate) fn handle_parts(server: &Server, line: &str, _body: &[u8]) -> HttpResponse {
    let mut parts = line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let target = parts.next().unwrap_or_default();
    let (path, query) = split_target(target);
    match (method, path) {
        ("GET", "/healthz") => HttpResponse::text("200 OK", "ok\n"),
        ("GET", "/readyz") => match server.check_readiness() {
            Ok(()) => HttpResponse::text("200 OK", "ready\n"),
            Err(err) => {
                HttpResponse::text("503 Service Unavailable", format!("not ready: {err}\n"))
            }
        },
        ("GET", "/stats") => HttpResponse::json("200 OK", server.stats_json()),
        ("GET", "/gc") | ("POST", "/gc") => {
            let limit = match gc_limit(query) {
                Ok(limit) => limit,
                Err(err) => return HttpResponse::text("400 Bad Request", format!("{err}\n")),
            };
            match server.run_manual_gc(limit) {
                Ok(body) => HttpResponse::json("200 OK", body),
                Err(err) => HttpResponse::text("500 Internal Server Error", format!("{err}\n")),
            }
        }
        ("POST", "/checkpoint") => match server.run_manual_checkpoint() {
            Ok(body) => HttpResponse::json("200 OK", body),
            Err(err) => HttpResponse::text("500 Internal Server Error", format!("{err}\n")),
        },
        ("GET", "/backup") | ("POST", "/backup") => match server.run_manual_backup() {
            Ok(body) => HttpResponse::json("200 OK", body),
            Err(err) => HttpResponse::text("500 Internal Server Error", format!("{err}\n")),
        },
        ("GET", "/fsck") | ("POST", "/fsck") => match server.run_fsck() {
            Ok(body) => HttpResponse::json("200 OK", body),
            Err(err) => HttpResponse::text("500 Internal Server Error", format!("{err}\n")),
        },
        (_, "/request-too-large") => {
            HttpResponse::text("413 Payload Too Large", "request too large\n")
        }
        _ => HttpResponse::text("404 Not Found", "not found\n"),
    }
}

fn split_target(target: &str) -> (&str, Option<&str>) {
    target
        .split_once('?')
        .map_or((target, None), |(path, query)| (path, Some(query)))
}

fn gc_limit(query: Option<&str>) -> Result<usize, String> {
    let Some(query) = query else {
        return Ok(usize::MAX);
    };
    for pair in query.split('&') {
        let Some((key, value)) = pair.split_once('=') else {
            continue;
        };
        if key == "limit" {
            return value
                .parse::<usize>()
                .map_err(|_| format!("invalid gc limit {value}"));
        }
    }
    Ok(usize::MAX)
}

impl HttpResponse {
    fn text(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into(),
            keep_alive: false,
        }
    }

    fn json(status: &'static str, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: body.into(),
            keep_alive: false,
        }
    }
}

fn write_response(out: &mut impl Write, response: &HttpResponse) -> io::Result<()> {
    let connection = if response.keep_alive {
        "keep-alive"
    } else {
        "close"
    };
    write!(
        out,
        "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: {}\r\n\r\n{}",
        response.status,
        response.content_type,
        response.body.len(),
        connection,
        response.body
    )
}

fn is_idle_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::tests::test_server;

    #[test]
    fn health_endpoint_is_plain_text() {
        let server = test_server();
        let response = handle_parts(&server, "GET /healthz HTTP/1.1", &[]);
        assert_eq!(response.status, "200 OK");
        assert_eq!(response.content_type, "text/plain; charset=utf-8");
        assert_eq!(response.body, "ok\n");
    }

    #[test]
    fn stats_endpoint_reports_object_counters() {
        let server = test_server();
        let response = handle_parts(&server, "GET /stats HTTP/1.1", &[]);
        assert_eq!(response.status, "200 OK");
        assert!(response.body.contains("\"object_puts\":0"));
        assert!(response.body.contains("\"prefetch_enqueued\":0"));
        assert!(response.body.contains("\"metadata_store\""));
        assert!(response.body.contains("\"commit_total\""));
        assert!(response.body.contains("\"metadata_service\""));
        assert!(response.body.contains("\"path_index_hit_total\""));
        assert!(response.body.contains("\"block_cache_enabled\":true"));
    }

    #[test]
    fn gc_endpoint_accepts_limit_query() {
        let server = test_server();
        let response = handle_parts(&server, "GET /gc?limit=7 HTTP/1.1", &[]);
        assert_eq!(response.status, "200 OK");
        assert!(response.body.contains("\"object_gc\""));
        assert!(response.body.contains("\"history_gc\""));
    }

    #[test]
    fn gc_endpoint_rejects_invalid_limit_query() {
        let server = test_server();
        let response = handle_parts(&server, "GET /gc?limit=bad HTTP/1.1", &[]);
        assert_eq!(response.status, "400 Bad Request");
    }

    #[test]
    fn checkpoint_endpoint_triggers_metadata_snapshot() {
        let server = test_server();
        let response = handle_parts(&server, "POST /checkpoint HTTP/1.1", &[]);
        assert_eq!(response.status, "200 OK");
        assert!(response.body.contains("\"checkpointed\":true"));
    }

    #[test]
    fn unknown_endpoint_returns_404() {
        let server = test_server();
        let response = handle_parts(&server, "GET /missing HTTP/1.1", &[]);
        assert_eq!(response.status, "404 Not Found");
    }

    #[test]
    fn rpc_endpoint_is_not_a_control_http_api() {
        let server = test_server();
        let response = handle_parts(&server, "POST /rpc HTTP/1.1", &[]);
        assert_eq!(response.status, "404 Not Found");
    }
}
