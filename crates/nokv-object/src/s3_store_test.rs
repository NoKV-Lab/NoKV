//! Wire-level tests for `S3ArtifactStore` against an in-process S3 endpoint.
//!
//! The endpoint speaks just enough HTTP/1.1 to drive OpenDAL's real S3 service,
//! so these tests pin how provider status codes and error codes reach the
//! create, seal, and head paths. A hook can override any response before the
//! conditional object model answers it.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use opendal::raw::Service;

use super::*;

const BUCKET: &str = "nokv-test";

#[derive(Clone, Debug)]
struct WireRequest {
    method: String,
    key: String,
    if_match: Option<String>,
    if_none_match: Option<String>,
    body: Vec<u8>,
}

struct WireResponse {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

#[derive(Clone)]
struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
}

type Objects = BTreeMap<String, StoredObject>;
type Hook = Box<dyn FnMut(&WireRequest, &mut Objects) -> Option<WireResponse> + Send>;

#[derive(Default)]
struct EndpointState {
    objects: Objects,
    next_etag: u64,
    requests: Vec<WireRequest>,
    hook: Option<Hook>,
}

struct FakeS3Endpoint {
    address: String,
    state: Arc<Mutex<EndpointState>>,
}

impl FakeS3Endpoint {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(EndpointState::default()));
        let served = Arc::clone(&state);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                serve_connection(stream, &served);
            }
        });
        Self { address, state }
    }

    fn store(&self) -> S3ArtifactStore {
        S3ArtifactStore::new(self.options()).unwrap()
    }

    fn options(&self) -> S3ArtifactStoreOptions {
        S3ArtifactStoreOptions::for_endpoint(BUCKET, &self.address, "nokv-test", "nokv-test")
    }

    fn put_object(&self, key: &ObjectKey, bytes: &[u8]) {
        let mut state = self.state.lock().unwrap();
        let etag = next_etag(&mut state);
        state.objects.insert(
            key.as_str().to_owned(),
            StoredObject {
                bytes: bytes.to_vec(),
                etag,
            },
        );
    }

    fn object(&self, key: &ObjectKey) -> Option<Vec<u8>> {
        let state = self.state.lock().unwrap();
        state
            .objects
            .get(key.as_str())
            .map(|object| object.bytes.clone())
    }

    fn set_hook(
        &self,
        hook: impl FnMut(&WireRequest, &mut Objects) -> Option<WireResponse> + Send + 'static,
    ) {
        self.state.lock().unwrap().hook = Some(Box::new(hook));
    }

    fn requests(&self) -> Vec<WireRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

fn next_etag(state: &mut EndpointState) -> String {
    state.next_etag += 1;
    format!("\"etag-{}\"", state.next_etag)
}

fn serve_connection(stream: TcpStream, state: &Mutex<EndpointState>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let Some(request) = read_request(&mut reader) else {
        return;
    };
    let response = {
        let mut state = state.lock().unwrap();
        state.requests.push(request.clone());
        let mut hook = state.hook.take();
        let hooked = hook
            .as_mut()
            .and_then(|hook| hook(&request, &mut state.objects));
        state.hook = hook;
        hooked.unwrap_or_else(|| conditional_object_model(&request, &mut state))
    };
    write_response(stream, &request.method, response);
}

fn read_request(reader: &mut BufReader<TcpStream>) -> Option<WireRequest> {
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_owned();
    let target = parts.next()?;
    let path = target.split('?').next().unwrap_or(target);
    let key = path
        .strip_prefix(&format!("/{BUCKET}/"))
        .unwrap_or(path)
        .to_owned();

    let mut content_length = 0;
    let mut if_match = None;
    let mut if_none_match = None;
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).ok()?;
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':')?;
        let value = value.trim().to_owned();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => content_length = value.parse().ok()?,
            "if-match" => if_match = Some(value),
            "if-none-match" => if_none_match = Some(value),
            _ => {}
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).ok()?;
    Some(WireRequest {
        method,
        key,
        if_match,
        if_none_match,
        body,
    })
}

fn write_response(mut stream: TcpStream, method: &str, response: WireResponse) {
    let mut head = format!("HTTP/1.1 {} Fake\r\nConnection: close\r\n", response.status);
    let declares_length = response
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"));
    if !declares_length && method != "HEAD" {
        head.push_str(&format!("Content-Length: {}\r\n", response.body.len()));
    }
    for (name, value) in &response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    if method != "HEAD" {
        let _ = stream.write_all(&response.body);
    }
}

/// S3 conditional-write semantics for one bucket: `If-None-Match: *` fails
/// with 412 on a live key, and `If-Match` fails with 412 on a different ETag
/// and with 404 `NoSuchKey` on a missing key, as AWS S3 documents.
fn conditional_object_model(request: &WireRequest, state: &mut EndpointState) -> WireResponse {
    let existing = state.objects.get(&request.key).cloned();
    match request.method.as_str() {
        "HEAD" => match existing {
            Some(object) => object_headers(200, &object, Vec::new()),
            None => status(404),
        },
        "GET" => match existing {
            Some(object) => object_headers(200, &object, object.bytes.clone()),
            None => s3_error(404, "NoSuchKey"),
        },
        "PUT" => {
            if request.if_none_match.as_deref() == Some("*") && existing.is_some() {
                return s3_error(412, "PreconditionFailed");
            }
            if let Some(expected) = &request.if_match {
                match &existing {
                    None => return s3_error(404, "NoSuchKey"),
                    Some(object) if &object.etag != expected => {
                        return s3_error(412, "PreconditionFailed");
                    }
                    Some(_) => {}
                }
            }
            let etag = next_etag(state);
            state.objects.insert(
                request.key.clone(),
                StoredObject {
                    bytes: request.body.clone(),
                    etag: etag.clone(),
                },
            );
            WireResponse {
                status: 200,
                headers: vec![("ETag", etag)],
                body: Vec::new(),
            }
        }
        "DELETE" => {
            state.objects.remove(&request.key);
            status(204)
        }
        _ => status(405),
    }
}

fn object_headers(code: u16, object: &StoredObject, body: Vec<u8>) -> WireResponse {
    WireResponse {
        status: code,
        headers: vec![
            ("Content-Length", object.bytes.len().to_string()),
            ("ETag", object.etag.clone()),
        ],
        body,
    }
}

fn status(code: u16) -> WireResponse {
    WireResponse {
        status: code,
        headers: Vec::new(),
        body: Vec::new(),
    }
}

fn s3_error(code: u16, s3_code: &str) -> WireResponse {
    WireResponse {
        status: code,
        headers: vec![("Content-Type", "application/xml".to_owned())],
        body: format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <Error><Code>{s3_code}</Code><Message>fake</Message></Error>"
        )
        .into_bytes(),
    }
}

fn conditional_puts(requests: &[WireRequest]) -> usize {
    requests
        .iter()
        .filter(|request| {
            request.method == "PUT"
                && (request.if_match.is_some() || request.if_none_match.is_some())
        })
        .count()
}

fn key() -> ObjectKey {
    ObjectKey::new("nokv/artifacts/seal-target").unwrap()
}

#[test]
fn requests_reach_the_endpoint_through_the_installed_http_transport() {
    let endpoint = FakeS3Endpoint::start();
    let store = endpoint.store();

    assert_eq!(store.head(&key()).unwrap(), None);
    let requests = endpoint.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, "HEAD");
}

#[test]
fn seal_gate_requires_service_declared_conditional_writes() {
    let endpoint = FakeS3Endpoint::start();
    let store = endpoint.store();
    let declared = opendal::Builder::build(s3_builder(&endpoint.options()))
        .unwrap()
        .capability();
    let effective = store.operator.info().capability();

    // `capability()` is the composed operator's effective view. Sealing must
    // not be enabled by a default layer that emulates conditional writes, so
    // the effective flags must equal the S3 service's own declaration.
    assert!(declared.write_with_if_match && declared.write_with_if_not_exists);
    assert_eq!(effective.write_with_if_match, declared.write_with_if_match);
    assert_eq!(
        effective.write_with_if_not_exists,
        declared.write_with_if_not_exists
    );
}

#[test]
fn seal_reobserves_a_conditional_request_conflict_instead_of_reporting_ambiguity() {
    let endpoint = FakeS3Endpoint::start();
    let key = key();
    endpoint.put_object(&key, b"abandoned payload");
    let mut conflicted = false;
    endpoint.set_hook(move |request, _| {
        if request.method == "PUT" && request.if_match.is_some() && !conflicted {
            conflicted = true;
            return Some(s3_error(409, "ConditionalRequestConflict"));
        }
        None
    });

    let outcome = endpoint.store().seal_immutable(&key).unwrap();

    assert_eq!(outcome, ObjectSealOutcome::Sealed);
    assert_eq!(endpoint.object(&key), Some(Vec::new()));
    let requests = endpoint.requests();
    assert_eq!(conditional_puts(&requests), 2);
    assert!(requests.iter().all(|request| request.method != "DELETE"));
}

#[test]
fn seal_creates_the_fence_when_the_payload_vanishes_before_the_if_match_put() {
    let endpoint = FakeS3Endpoint::start();
    let key = key();
    endpoint.put_object(&key, b"abandoned payload");
    endpoint.set_hook(|request, objects| {
        if request.method == "PUT" && request.if_match.is_some() {
            objects.remove(&request.key);
            return Some(s3_error(404, "NoSuchKey"));
        }
        None
    });

    let outcome = endpoint.store().seal_immutable(&key).unwrap();

    // OpenDAL 0.59 reports an If-Match write against a missing key as
    // `ConditionNotMatch`; the reobserved absent key takes `If-None-Match: *`.
    assert_eq!(outcome, ObjectSealOutcome::Sealed);
    assert_eq!(endpoint.object(&key), Some(Vec::new()));
    let requests = endpoint.requests();
    let last_put = requests
        .iter()
        .rfind(|request| request.method == "PUT")
        .unwrap();
    assert_eq!(last_put.if_none_match.as_deref(), Some("*"));
    assert!(requests.iter().all(|request| request.method != "DELETE"));
}

#[test]
fn seal_never_accepts_a_head_without_content_length_as_an_empty_fence() {
    let endpoint = FakeS3Endpoint::start();
    let key = key();
    endpoint.put_object(&key, b"abandoned payload");
    endpoint.set_hook(|request, objects| {
        let object = objects.get(&request.key)?;
        (request.method == "HEAD").then(|| WireResponse {
            status: 200,
            headers: vec![("ETag", object.etag.clone())],
            body: Vec::new(),
        })
    });
    let store = endpoint.store();

    assert!(matches!(
        store.seal_immutable(&key),
        Err(ObjectError::Backend { .. })
    ));
    assert!(matches!(store.head(&key), Err(ObjectError::Backend { .. })));
    assert_eq!(endpoint.object(&key), Some(b"abandoned payload".to_vec()));
    let requests = endpoint.requests();
    assert!(requests.iter().all(|request| request.method == "HEAD"));
    assert_eq!(requests.len(), 2);
}

#[test]
fn create_reconciles_a_failed_precondition_from_the_stored_bytes() {
    let endpoint = FakeS3Endpoint::start();
    let store = endpoint.store();
    let same = ObjectKey::new("nokv/artifacts/same").unwrap();
    let different = ObjectKey::new("nokv/artifacts/different").unwrap();
    endpoint.put_object(&same, b"payload");
    endpoint.put_object(&different, b"foreign");

    assert_eq!(
        store.create_immutable(&same, b"payload").unwrap(),
        ImmutableCreateOutcome::Replayed
    );
    assert!(matches!(
        store.create_immutable(&different, b"payload"),
        Err(ObjectError::ImmutableCollision { .. })
    ));
    assert_eq!(endpoint.object(&different), Some(b"foreign".to_vec()));
}

#[test]
fn create_reports_a_conflict_on_an_absent_key_as_ambiguous() {
    let endpoint = FakeS3Endpoint::start();
    let key = ObjectKey::new("nokv/artifacts/racing").unwrap();
    endpoint.set_hook(|request, _| {
        (request.method == "PUT").then(|| s3_error(409, "ConditionalRequestConflict"))
    });

    assert!(matches!(
        endpoint.store().create_immutable(&key, b"payload"),
        Err(ObjectError::CreateAmbiguous { .. })
    ));
    assert_eq!(endpoint.object(&key), None);
    let methods = endpoint
        .requests()
        .into_iter()
        .map(|request| request.method)
        .collect::<Vec<_>>();
    assert_eq!(methods, ["PUT", "GET"]);
}
