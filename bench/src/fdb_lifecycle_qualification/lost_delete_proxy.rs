/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! External HTTP/1.1 proxy that loses one successful S3 DELETE acknowledgement.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::Serialize;
use sha2::{Digest as _, Sha256};

const MAX_HEADER_BYTES: usize = 64 * 1024;
// Provider admission uploads one full default artifact block (4 MiB). Keep
// the qualification proxy bounded, but large enough to be transparent to
// that mandatory preflight and to one in-flight normal block.
const MAX_BUFFERED_BODY_BYTES: usize = 8 * 1024 * 1024;
const IO_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize)]
pub(crate) struct LostDeleteEvent {
    pub(crate) method: String,
    pub(crate) upstream_status: u16,
    pub(crate) request_sha256: String,
    pub(crate) response_sha256: String,
    pub(crate) response_bytes: usize,
    pub(crate) forwarded_response_bytes: usize,
    pub(crate) successful_upstream_delete: bool,
}

pub(crate) struct LostDeleteProxy {
    endpoint: SocketAddr,
    armed: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    event: Arc<Mutex<Option<LostDeleteEvent>>>,
    failure: Arc<Mutex<Option<String>>>,
    listener: Option<JoinHandle<()>>,
}

impl LostDeleteProxy {
    pub(crate) fn start(bind: SocketAddr, upstream: SocketAddr) -> Result<Self, String> {
        let listener = TcpListener::bind(bind)
            .map_err(|error| format!("cannot bind lost-delete proxy at {bind}: {error}"))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| format!("cannot make lost-delete proxy nonblocking: {error}"))?;
        let endpoint = listener
            .local_addr()
            .map_err(|error| format!("cannot inspect lost-delete proxy endpoint: {error}"))?;
        let armed = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let event = Arc::new(Mutex::new(None));
        let failure = Arc::new(Mutex::new(None));
        let listener_thread = {
            let armed = Arc::clone(&armed);
            let stop = Arc::clone(&stop);
            let event = Arc::clone(&event);
            let failure = Arc::clone(&failure);
            thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((client, _peer)) => {
                            let armed = Arc::clone(&armed);
                            let event = Arc::clone(&event);
                            let failure = Arc::clone(&failure);
                            thread::spawn(move || {
                                if let Err(error) =
                                    proxy_connection(client, upstream, &armed, &event)
                                {
                                    let mut current = failure
                                        .lock()
                                        .expect("lost-delete proxy failure lock is available");
                                    if current.is_none() {
                                        *current = Some(error);
                                    }
                                }
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(error) => {
                            *failure
                                .lock()
                                .expect("lost-delete proxy failure lock is available") =
                                Some(format!("lost-delete proxy accept failed: {error}"));
                            break;
                        }
                    }
                }
            })
        };
        Ok(Self {
            endpoint,
            armed,
            stop,
            event,
            failure,
            listener: Some(listener_thread),
        })
    }

    pub(crate) const fn endpoint(&self) -> SocketAddr {
        self.endpoint
    }

    pub(crate) fn arm(&self) -> Result<(), String> {
        if self.event()?.is_some() {
            return Err("lost-delete proxy already selected a DELETE".to_owned());
        }
        if self.armed.swap(true, Ordering::AcqRel) {
            return Err("lost-delete proxy is already armed".to_owned());
        }
        Ok(())
    }

    pub(crate) fn event(&self) -> Result<Option<LostDeleteEvent>, String> {
        if let Some(error) = self
            .failure
            .lock()
            .map_err(|_| "lost-delete proxy failure lock is poisoned".to_owned())?
            .clone()
        {
            return Err(error);
        }
        self.event
            .lock()
            .map(|event| event.clone())
            .map_err(|_| "lost-delete proxy event lock is poisoned".to_owned())
    }

    pub(crate) fn finish(mut self) -> Result<LostDeleteEvent, String> {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.endpoint, Duration::from_millis(100));
        if let Some(listener) = self.listener.take() {
            listener
                .join()
                .map_err(|_| "lost-delete proxy listener panicked".to_owned())?;
        }
        if self.armed.load(Ordering::Acquire) {
            return Err("lost-delete proxy remained armed without a target".to_owned());
        }
        self.event()?
            .ok_or_else(|| "lost-delete proxy did not select a DELETE".to_owned())
    }
}

impl Drop for LostDeleteProxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect_timeout(&self.endpoint, Duration::from_millis(50));
        if let Some(listener) = self.listener.take() {
            let _ = listener.join();
        }
    }
}

fn proxy_connection(
    mut client: TcpStream,
    upstream: SocketAddr,
    armed: &AtomicBool,
    event: &Mutex<Option<LostDeleteEvent>>,
) -> Result<(), String> {
    client
        .set_read_timeout(Some(IO_TIMEOUT))
        .map_err(|error| format!("cannot set proxy client read timeout: {error}"))?;
    client
        .set_write_timeout(Some(IO_TIMEOUT))
        .map_err(|error| format!("cannot set proxy client write timeout: {error}"))?;

    loop {
        let Some(request) = read_request(&mut client)? else {
            return Ok(());
        };
        let selected = request.method == "DELETE"
            && armed
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
        let mut server = TcpStream::connect_timeout(&upstream, IO_TIMEOUT)
            .map_err(|error| format!("cannot connect lost-delete proxy upstream: {error}"))?;
        server
            .set_read_timeout(Some(IO_TIMEOUT))
            .map_err(|error| format!("cannot set proxy upstream read timeout: {error}"))?;
        server
            .set_write_timeout(Some(IO_TIMEOUT))
            .map_err(|error| format!("cannot set proxy upstream write timeout: {error}"))?;
        server
            .write_all(&request.bytes)
            .and_then(|()| server.flush())
            .map_err(|error| format!("cannot forward proxy request: {error}"))?;

        let response = read_response(&mut server, request.method == "HEAD")?;
        if selected {
            let status = response.status;
            let successful = (200..300).contains(&status);
            let selected_event = LostDeleteEvent {
                method: request.method,
                upstream_status: status,
                request_sha256: hex_digest(&request.bytes),
                response_sha256: hex_digest(&response.bytes),
                response_bytes: response.bytes.len(),
                forwarded_response_bytes: 0,
                successful_upstream_delete: successful,
            };
            let mut current = event
                .lock()
                .map_err(|_| "lost-delete proxy event lock is poisoned".to_owned())?;
            if current.replace(selected_event).is_some() {
                return Err("lost-delete proxy selected more than one DELETE".to_owned());
            }
            let _ = client.shutdown(Shutdown::Both);
            if !successful {
                return Err(format!(
                    "selected RustFS DELETE returned non-success status {status}"
                ));
            }
            return Ok(());
        }

        client
            .write_all(&response.bytes)
            .and_then(|()| client.flush())
            .map_err(|error| format!("cannot forward proxy response: {error}"))?;
        if request.connection_close || response.connection_close {
            return Ok(());
        }
    }
}

struct HttpRequest {
    method: String,
    bytes: Vec<u8>,
    connection_close: bool,
}

struct HttpResponse {
    status: u16,
    bytes: Vec<u8>,
    connection_close: bool,
}

fn read_request(stream: &mut TcpStream) -> Result<Option<HttpRequest>, String> {
    let Some((header, fields)) = read_header(stream, true)? else {
        return Ok(None);
    };
    let method = fields
        .start_line
        .split_whitespace()
        .next()
        .ok_or_else(|| "proxy request line has no method".to_owned())?
        .to_owned();
    let mut bytes = header;
    read_body(stream, &fields, false, &mut bytes)?;
    Ok(Some(HttpRequest {
        method,
        bytes,
        connection_close: fields.connection_close,
    }))
}

fn read_response(stream: &mut TcpStream, request_was_head: bool) -> Result<HttpResponse, String> {
    loop {
        let (header, fields) = read_header(stream, false)?
            .ok_or_else(|| "proxy upstream closed before its response".to_owned())?;
        let status = fields
            .start_line
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| "proxy response line has no status".to_owned())?
            .parse::<u16>()
            .map_err(|error| format!("proxy response status is invalid: {error}"))?;
        let mut bytes = header;
        let no_body =
            request_was_head || (100..200).contains(&status) || status == 204 || status == 304;
        if !no_body {
            read_body(stream, &fields, true, &mut bytes)?;
        }
        if status == 100 {
            continue;
        }
        return Ok(HttpResponse {
            status,
            bytes,
            connection_close: fields.connection_close,
        });
    }
}

struct HeaderFields {
    start_line: String,
    content_length: Option<usize>,
    chunked: bool,
    connection_close: bool,
}

fn read_header(
    stream: &mut TcpStream,
    allow_clean_eof: bool,
) -> Result<Option<(Vec<u8>, HeaderFields)>, String> {
    let mut bytes = Vec::new();
    let mut byte = [0_u8; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) if bytes.is_empty() && allow_clean_eof => return Ok(None),
            Ok(0) => return Err("HTTP peer closed inside a header".to_owned()),
            Ok(_) => bytes.push(byte[0]),
            Err(error) => return Err(format!("cannot read HTTP header: {error}")),
        }
        if bytes.ends_with(b"\r\n\r\n") {
            break;
        }
        if bytes.len() > MAX_HEADER_BYTES {
            return Err("HTTP header exceeds qualification proxy limit".to_owned());
        }
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| "HTTP header is not UTF-8 compatible".to_owned())?;
    let mut lines = text.split("\r\n");
    let start_line = lines
        .next()
        .ok_or_else(|| "HTTP message has no start line".to_owned())?
        .to_owned();
    let mut content_length = None;
    let mut chunked = false;
    let mut connection_close = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| "HTTP header field has no colon".to_owned())?;
        let value = value.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value
                .parse::<usize>()
                .map_err(|error| format!("HTTP content-length is invalid: {error}"))?;
            if content_length.replace(parsed).is_some() {
                return Err("HTTP message repeats content-length".to_owned());
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            chunked = value
                .split(',')
                .any(|coding| coding.trim().eq_ignore_ascii_case("chunked"));
        } else if name.eq_ignore_ascii_case("connection") {
            connection_close = value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("close"));
        }
    }
    if chunked && content_length.is_some() {
        return Err("HTTP message has both chunked encoding and content-length".to_owned());
    }
    Ok(Some((
        bytes,
        HeaderFields {
            start_line,
            content_length,
            chunked,
            connection_close,
        },
    )))
}

fn read_body(
    stream: &mut TcpStream,
    fields: &HeaderFields,
    allow_eof_body: bool,
    bytes: &mut Vec<u8>,
) -> Result<(), String> {
    if let Some(length) = fields.content_length {
        if length > MAX_BUFFERED_BODY_BYTES {
            return Err(format!(
                "HTTP body {length} exceeds qualification proxy limit {MAX_BUFFERED_BODY_BYTES}"
            ));
        }
        let start = bytes.len();
        bytes.resize(start + length, 0);
        stream
            .read_exact(&mut bytes[start..])
            .map_err(|error| format!("cannot read HTTP body: {error}"))?;
        return Ok(());
    }
    if fields.chunked {
        return read_chunked_body(stream, bytes);
    }
    if allow_eof_body && fields.connection_close {
        stream
            .read_to_end(bytes)
            .map_err(|error| format!("cannot read close-delimited HTTP body: {error}"))?;
    }
    Ok(())
}

fn read_chunked_body(stream: &mut TcpStream, bytes: &mut Vec<u8>) -> Result<(), String> {
    loop {
        let line = read_crlf_line(stream, bytes)?;
        let length = line
            .split(';')
            .next()
            .ok_or_else(|| "chunk header has no length".to_owned())?;
        let length = usize::from_str_radix(length.trim(), 16)
            .map_err(|error| format!("chunk length is invalid: {error}"))?;
        if length == 0 {
            loop {
                let trailer = read_crlf_line(stream, bytes)?;
                if trailer.is_empty() {
                    return Ok(());
                }
            }
        }
        if bytes.len().saturating_add(length) > MAX_BUFFERED_BODY_BYTES + MAX_HEADER_BYTES {
            return Err("chunked HTTP body exceeds qualification proxy limit".to_owned());
        }
        let start = bytes.len();
        bytes.resize(start + length + 2, 0);
        stream
            .read_exact(&mut bytes[start..])
            .map_err(|error| format!("cannot read HTTP chunk: {error}"))?;
        if &bytes[bytes.len() - 2..] != b"\r\n" {
            return Err("HTTP chunk lacks its trailing CRLF".to_owned());
        }
    }
}

fn read_crlf_line(stream: &mut TcpStream, bytes: &mut Vec<u8>) -> Result<String, String> {
    let start = bytes.len();
    let mut byte = [0_u8; 1];
    loop {
        stream
            .read_exact(&mut byte)
            .map_err(|error| format!("cannot read HTTP line: {error}"))?;
        bytes.push(byte[0]);
        if bytes[start..].ends_with(b"\r\n") {
            let line = &bytes[start..bytes.len() - 2];
            return std::str::from_utf8(line)
                .map(str::to_owned)
                .map_err(|_| "HTTP line is not UTF-8 compatible".to_owned());
        }
        if bytes.len().saturating_sub(start) > MAX_HEADER_BYTES {
            return Err("HTTP line exceeds qualification proxy limit".to_owned());
        }
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_buffer_covers_the_provider_admission_block() {
        assert!(MAX_BUFFERED_BODY_BYTES >= 4 * 1024 * 1024);
    }

    #[test]
    fn delete_event_contains_digests_not_authorization_bytes() {
        let request =
            b"DELETE /bucket/key HTTP/1.1\r\nAuthorization: secret\r\nContent-Length: 0\r\n\r\n";
        let response = b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n";
        let event = LostDeleteEvent {
            method: "DELETE".to_owned(),
            upstream_status: 204,
            request_sha256: hex_digest(request),
            response_sha256: hex_digest(response),
            response_bytes: response.len(),
            forwarded_response_bytes: 0,
            successful_upstream_delete: true,
        };
        let encoded = serde_json::to_string(&event).unwrap();
        assert!(!encoded.contains("secret"));
        assert_eq!(event.request_sha256.len(), 64);
    }
}
