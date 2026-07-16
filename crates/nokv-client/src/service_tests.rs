use super::*;
use crate::read_cache::ReadPipelineCache;
use crate::{ArtifactMetadata, NoKvFsClient, PathRangeReadRequest, PathReadRange};
use nokv_object::FileReadPipeline;
use nokv_object::{
    MemoryObjectStore, ObjectBytes, ObjectError, ObjectGetRequest, ObjectInfo, ObjectKey,
    ObjectRange, ObjectStore, DEFAULT_BLOCK_SIZE,
};
use nokv_protocol::{decode_request, encode_envelope, WireDentryRecord, WireInodeAttr};
use nokv_protocol::{MetadataRpcEnvelope, WireDentryWithAttr, WireXattrSetMode};
use nokv_types::{AdvisoryLockKind, FileType, InodeId};
use std::io::Read;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn durable_restore_uses_a_long_rpc_deadline_without_shortening_custom_timeouts() {
    assert_eq!(
        restore_rpc_timeout(Duration::from_secs(10)),
        Duration::from_secs(300)
    );
    assert_eq!(
        restore_rpc_timeout(Duration::from_secs(600)),
        Duration::from_secs(600)
    );
}

fn serve_one(body: &'static str) -> SocketAddr {
    serve_many(vec![response_body(body)])
}

fn serve_many(bodies: Vec<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        for body in bodies {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            decode_request(&request).expect("framed request is binary metadata rpc");
            write_frame(&mut stream, request_id, flags, &body).unwrap();
        }
    });
    addr
}

fn serve_one_request<F>(handler: F) -> SocketAddr
where
    F: FnOnce(MetadataRpcRequest) -> Vec<u8> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        let response = handler(request);
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    addr
}

fn serve_request_sequence(
    handlers: Vec<Box<dyn Fn(MetadataRpcRequest) -> Vec<u8> + Send>>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        for handler in handlers {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            let response = handler(request);
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        }
    });
    addr
}

fn serve_request_sequence_then_drop(
    handlers: Vec<Box<dyn Fn(MetadataRpcRequest) -> Vec<u8> + Send>>,
    dropped_request: Box<dyn Fn(MetadataRpcRequest) + Send>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        for handler in handlers {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            let response = handler(request);
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        }
        let (_, _, request) = read_frame(&mut stream).unwrap();
        dropped_request(decode_request(&request).unwrap());
        // Simulate a response lost after the server may have committed.
    });
    addr
}

fn response_body(json: &str) -> Vec<u8> {
    let envelope: MetadataRpcEnvelope = serde_json::from_str(json).unwrap();
    encode_envelope(&envelope).unwrap()
}

fn open_path_read_plan_batch_response(start_inode: u64, count: usize) -> Vec<u8> {
    let plans = (0..count)
        .map(|index| {
            let inode = start_inode + index as u64;
            format!(
                r#"{{"metadata":{{"attr":{{"inode":{inode},"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":1,"generation":7,"mtime_ms":7,"ctime_ms":7}},"body":{{"producer":"unit-test","digest_uri":"sha256:{inode}","size":1,"content_type":"application/octet-stream","manifest_id":"sample-{inode}.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}},"lease":{{"inode":{inode},"generation":7,"read_version":9,"lease_expires_unix_ms":12345}},"plan":{{"output_len":1,"blocks":[{{"object_key":"blocks/{inode}","digest_uri":"sha256:test","object_offset":0,"object_len":1,"len":1,"output_offset":0}}]}}}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    response_body(&format!(
        r#"{{"ok":true,"result":{{"type":"open_path_read_plan_batch","plans":[{plans}]}}}}"#
    ))
}

fn coalesced_gap_window_batch_response(request: MetadataRpcRequest) -> Vec<u8> {
    let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
        panic!("expected batch open request");
    };
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/shard-a.bin");
    assert_eq!(requests[0].offset, 1);
    assert_eq!(requests[0].len, 6);
    assert_eq!(requests[0].expected_generation, Some(7));
    response_body(
        r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard-a","size":8,"content_type":"application/octet-stream","manifest_id":"shard-a.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":6,"blocks":[{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":6,"output_offset":0}]}}]}}"#,
    )
}

fn two_shard_batch_response(request: MetadataRpcRequest) -> Vec<u8> {
    let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
        panic!("expected batch open request");
    };
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].path, "/shard-a.bin");
    assert_eq!(requests[0].offset, 1);
    assert_eq!(requests[0].len, 5);
    assert_eq!(requests[0].expected_generation, Some(7));
    assert_eq!(requests[1].path, "/shard-b.bin");
    assert_eq!(requests[1].offset, 2);
    assert_eq!(requests[1].len, 3);
    assert_eq!(requests[1].expected_generation, Some(8));
    response_body(
        r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard-a","size":8,"content_type":"application/octet-stream","manifest_id":"shard-a.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":5,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:shard-b","size":6,"content_type":"application/octet-stream","manifest_id":"shard-b.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/shard-b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":3,"output_offset":0}]}}]}}"#,
    )
}

#[derive(Clone)]
struct BatchTrackingObjectStore {
    inner: MemoryObjectStore,
    batch_sizes: Arc<Mutex<Vec<usize>>>,
}

impl BatchTrackingObjectStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
            batch_sizes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn batch_sizes(&self) -> Vec<usize> {
        self.batch_sizes.lock().unwrap().clone()
    }
}

impl ObjectStore for BatchTrackingObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        self.batch_sizes.lock().unwrap().push(requests.len());
        self.inner.get_many(requests)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

#[derive(Clone)]
struct ConcurrentBatchTrackingObjectStore {
    inner: MemoryObjectStore,
    state: Arc<ConcurrentBatchState>,
}

struct ConcurrentBatchState {
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    calls: Mutex<usize>,
    calls_changed: Condvar,
}

impl ConcurrentBatchTrackingObjectStore {
    fn new() -> Self {
        Self {
            inner: MemoryObjectStore::new(),
            state: Arc::new(ConcurrentBatchState {
                inflight: AtomicUsize::new(0),
                max_inflight: AtomicUsize::new(0),
                calls: Mutex::new(0),
                calls_changed: Condvar::new(),
            }),
        }
    }

    fn max_inflight(&self) -> usize {
        self.state.max_inflight.load(Ordering::SeqCst)
    }

    fn wait_for_peer_get_many(&self) {
        let mut calls = self.state.calls.lock().unwrap();
        *calls += 1;
        self.state.calls_changed.notify_all();
        if *calls < 2 {
            let _ = self
                .state
                .calls_changed
                .wait_timeout_while(calls, Duration::from_millis(500), |calls| *calls < 2)
                .unwrap();
        }
    }
}

impl ObjectStore for ConcurrentBatchTrackingObjectStore {
    fn put(
        &self,
        key: &ObjectKey,
        bytes: impl Into<ObjectBytes>,
    ) -> Result<ObjectInfo, ObjectError> {
        self.inner.put(key, bytes)
    }

    fn get(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        self.inner.get(key, range)
    }

    fn get_many(&self, requests: &[ObjectGetRequest]) -> Result<Vec<Vec<u8>>, ObjectError> {
        let inflight = self.state.inflight.fetch_add(1, Ordering::SeqCst) + 1;
        self.state
            .max_inflight
            .fetch_max(inflight, Ordering::SeqCst);
        self.wait_for_peer_get_many();
        let result = self.inner.get_many(requests);
        self.state.inflight.fetch_sub(1, Ordering::SeqCst);
        result
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        self.inner.head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<bool, ObjectError> {
        self.inner.delete(key)
    }
}

#[test]
fn read_pipeline_cache_evicts_oldest_unused_pipeline() {
    let mut cache = ReadPipelineCache::new(2);
    cache.insert("a#1".to_owned(), FileReadPipeline::default());
    cache.insert("b#1".to_owned(), FileReadPipeline::default());

    let pipeline = cache.take("a#1");
    cache.insert("a#1".to_owned(), pipeline);
    cache.insert("c#1".to_owned(), FileReadPipeline::default());

    assert_eq!(cache.len(), 2);
    assert!(cache.contains("a#1"));
    assert!(cache.contains("c#1"));
    assert!(!cache.contains("b#1"));
}

fn dentry_batch_response(names: &[String], first_inode: u64) -> Vec<u8> {
    let results = names
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let inode = first_inode + u64::try_from(idx).expect("test index fits u64");
            MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::Dentry {
                    entry: Some(Box::new(WireDentryWithAttr {
                        dentry: WireDentryRecord {
                            parent: 2,
                            name_hex: name
                                .as_bytes()
                                .iter()
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<String>(),
                            child: inode,
                            child_type: "file".to_owned(),
                            attr_generation: inode,
                        },
                        attr: WireInodeAttr {
                            inode,
                            file_type: "file".to_owned(),
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                            rdev: 0,
                            nlink: 1,
                            size: 0,
                            generation: inode,
                            mtime_ms: inode,
                            ctime_ms: inode,
                        },
                        body: None,
                    })),
                }),
                error: None,
                error_kind: None,
            }
        })
        .collect();
    encode_envelope(&MetadataRpcEnvelope {
        ok: true,
        result: Some(MetadataRpcResult::Batch { results }),
        error: None,
        error_kind: None,
    })
    .unwrap()
}

fn dentry_response(parent: u64, name: &str, inode: u64, generation: u64) -> Vec<u8> {
    let envelope = MetadataRpcEnvelope {
        ok: true,
        result: Some(MetadataRpcResult::Dentry {
            entry: Some(Box::new(WireDentryWithAttr {
                dentry: WireDentryRecord {
                    parent,
                    name_hex: name
                        .as_bytes()
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect::<String>(),
                    child: inode,
                    child_type: "file".to_owned(),
                    attr_generation: generation,
                },
                attr: WireInodeAttr {
                    inode,
                    file_type: "file".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                    rdev: 0,
                    nlink: 1,
                    size: 0,
                    generation,
                    mtime_ms: generation,
                    ctime_ms: generation,
                },
                body: None,
            })),
        }),
        error: None,
        error_kind: None,
    };
    encode_envelope(&envelope).unwrap()
}

fn dentry_response_for_request(request: &MetadataRpcRequest) -> Vec<u8> {
    let MetadataRpcRequest::CreateFilePath { path, .. } = request else {
        panic!("unexpected pipelined request: {request:?}");
    };
    let name = path.rsplit('/').next().expect("path has a final component");
    let inode = match name {
        "a.bin" => 40,
        "b.bin" => 41,
        other => panic!("unexpected file name: {other}"),
    };
    dentry_response(2, name, inode, inode)
}

fn artifact_metadata(manifest_id: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        producer: "unit-test".to_owned(),
        digest_uri: "sha256:demo".to_owned(),
        content_type: "application/octet-stream".to_owned(),
        manifest_id: manifest_id.to_owned(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

#[test]
fn service_mkdir_sends_metadata_rpc() {
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"72756e73","child":2,"child_type":"directory","attr_generation":1},"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":1,"mtime_ms":1,"ctime_ms":1},"body":null}}}"#,
    );
    let client = MetadataClient::connect(addr);
    let entry = client.mkdir("/runs", 0o755, 1000, 1000).unwrap();
    assert_eq!(entry.attr.inode.get(), 2);
    assert_eq!(entry.dentry.name.as_bytes(), b"runs");
}

#[test]
fn service_mkdirs_uses_single_batch_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::Batch { requests } => {
                assert_eq!(requests.len(), 2);
                assert_eq!(
                    requests[0],
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs/a".to_owned(),
                        mode: 0o755,
                        uid: 1000,
                        gid: 1000,
                    }
                );
                assert_eq!(
                    requests[1],
                    MetadataRpcRequest::CreateDirPath {
                        path: "/runs/b".to_owned(),
                        mode: 0o755,
                        uid: 1000,
                        gid: 1000,
                    }
                );
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"61","child":40,"child_type":"directory","attr_generation":7},"attr":{"inode":40,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"62","child":41,"child_type":"directory","attr_generation":8},"attr":{"inode":41,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a".to_owned(), "/runs/b".to_owned()];
    let entries = client.mkdirs(&paths, 0o755, 1000, 1000).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_create_file_uses_single_path_rpc_for_nested_parent() {
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"636865636b706f696e742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
    );
    let client = MetadataClient::connect(addr);
    let entry = client
        .create_file("/runs/checkpoint.bin", 0o644, 1000, 1000)
        .unwrap();
    assert_eq!(entry.attr.inode.get(), 42);
    assert_eq!(entry.dentry.name.as_bytes(), b"checkpoint.bin");
}

#[test]
fn service_create_special_node_sends_typed_rpc() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::CreateSpecialNode {
                parent: 1,
                name: "accelerator0".to_owned(),
                file_type: "char_device".to_owned(),
                mode: 0o660,
                rdev: 0x1234,
                uid: 0,
                gid: 44,
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"616363656c657261746f7230","child":42,"child_type":"char_device","attr_generation":7},"attr":{"inode":42,"file_type":"char_device","mode":432,"uid":0,"gid":44,"rdev":4660,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let entry = client
        .create_special_node(
            InodeId::root(),
            DentryName::new(b"accelerator0".to_vec()).unwrap(),
            SpecialNodeSpec {
                file_type: FileType::CharDevice,
                mode: 0o660,
                rdev: 0x1234,
                uid: 0,
                gid: 44,
            },
        )
        .unwrap();
    assert_eq!(entry.attr.file_type, FileType::CharDevice);
    assert_eq!(entry.attr.rdev, 0x1234);
}

#[test]
fn service_advisory_lock_sends_typed_rpc_and_maps_conflict() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        for response in [
            response_body(r#"{"ok":true,"result":{"type":"unit"}}"#),
            response_body(
                r#"{"ok":false,"error":"lock conflict","error_kind":{"type":"lock_conflict","lock":{"inode":42,"owner":7,"start":0,"end":99,"kind":"write","pid":700}}}"#,
            ),
        ] {
            let (request_id, flags, request) = read_frame(&mut stream).unwrap();
            let request = decode_request(&request).unwrap();
            assert!(matches!(
                request,
                MetadataRpcRequest::SetAdvisoryLock {
                    inode: 42,
                    owner: 9,
                    start: 10,
                    end: 20,
                    kind,
                    pid: 900,
                    wait: false,
                } if kind == "read"
            ));
            write_frame(&mut stream, request_id, flags, &response).unwrap();
        }
    });
    let client = MetadataClient::connect(addr);
    let err = client
        .set_advisory_lock(AdvisoryLockRequest {
            inode: InodeId::new(42).unwrap(),
            owner: 9,
            start: 10,
            end: 20,
            kind: AdvisoryLockKind::Read,
            pid: 900,
            wait: false,
        })
        .and_then(|_| {
            client.set_advisory_lock(AdvisoryLockRequest {
                inode: InodeId::new(42).unwrap(),
                owner: 9,
                start: 10,
                end: 20,
                kind: AdvisoryLockKind::Read,
                pid: 900,
                wait: false,
            })
        })
        .unwrap_err();
    match err {
        ClientError::LockConflict(lock) => {
            assert_eq!(lock.inode, InodeId::new(42).unwrap());
            assert_eq!(lock.owner, 7);
            assert_eq!(lock.kind, AdvisoryLockKind::Write);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn service_create_files_uses_single_coalesced_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::CreateFilesInDirPath {
                parent_path, names, ..
            } => {
                assert_eq!(parent_path, "/runs");
                assert_eq!(names, vec!["a.bin".to_owned(), "b.bin".to_owned()]);
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"612e62696e","child":40,"child_type":"file","attr_generation":7},"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"622e62696e","child":41,"child_type":"file","attr_generation":8},"attr":{"inode":41,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a.bin".to_owned(), "/runs/b.bin".to_owned()];
    let entries = client.create_files(&paths, 0o644, 1000, 1000).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_create_file_prepared_uses_single_frame() {
    let addr = serve_one_request(|request| {
        match request {
            MetadataRpcRequest::CreateFilePrepared {
                parent,
                name,
                mode,
                uid,
                gid,
            } => {
                assert_eq!(parent, 2);
                assert_eq!(name, "checkpoint.bin");
                assert_eq!(mode, 0o644);
                assert_eq!(uid, 1000);
                assert_eq!(gid, 1000);
            }
            other => panic!("unexpected request: {other:?}"),
        }
        response_body(
            r#"{"ok":true,"result":{"type":"created_prepared_artifact","entry":{"dentry":{"parent":2,"name_hex":"636865636b706f696e742e62696e","child":40,"child_type":"file","attr_generation":7},"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null},"prepared":{"mount":1,"parent":2,"name":"checkpoint.bin","inode":40,"generation":8,"mtime_ms":8,"ctime_ms":8,"replace":true,"dentry_version":7,"old_generation":null,"object_gc_claim_version":12}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let created = client
        .create_file_prepared_in_dir(
            InodeId::new(2).unwrap(),
            DentryName::new(b"checkpoint.bin".to_vec()).unwrap(),
            0o644,
            1000,
            1000,
        )
        .unwrap();
    assert_eq!(created.entry.attr.inode.get(), 40);
    assert_eq!(created.prepared.inode.get(), 40);
    assert_eq!(created.prepared.generation, 8);
    assert!(created.prepared.replace);
    assert_eq!(created.prepared.object_gc_claim_version, 12);
}

#[test]
fn service_refresh_prepared_upload_mints_generation_and_object_gc_epoch() {
    let addr = serve_one_request(|request| {
        match request {
            MetadataRpcRequest::RefreshPreparedArtifactObjectGcEpoch { prepared } => {
                assert_eq!(prepared.inode, 42);
                assert_eq!(prepared.generation, 7);
                assert_eq!(prepared.object_gc_claim_version, 12);
            }
            other => panic!("unexpected request: {other:?}"),
        }
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":2,"name":"artifact.bin","inode":42,"generation":13,"mtime_ms":1700000000001,"ctime_ms":1700000000001,"replace":true,"dentry_version":7,"old_generation":6,"object_gc_claim_version":14}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let refreshed = client
        .refresh_prepared_artifact_object_gc_epoch(ClientPreparedArtifact {
            mount: 1,
            parent: InodeId::new(2).unwrap(),
            name: DentryName::new(b"artifact.bin".to_vec()).unwrap(),
            path: None,
            inode: InodeId::new(42).unwrap(),
            generation: 7,
            mtime_ms: 1_700_000_000_000,
            ctime_ms: 1_700_000_000_000,
            replace: true,
            dentry_version: Some(7),
            old_generation: Some(6),
            object_gc_claim_version: 12,
        })
        .unwrap();
    assert_eq!(refreshed.generation, 13);
    assert_eq!(refreshed.object_gc_claim_version, 14);
}

#[test]
fn service_create_files_splits_large_coalesced_batches() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        let first_names = match request {
            MetadataRpcRequest::CreateFilesInDirPath {
                parent_path, names, ..
            } => {
                assert_eq!(parent_path, "/runs");
                assert_eq!(names.len(), MAX_BATCH_RPC_REQUESTS);
                names
            }
            other => panic!("unexpected first request: {other:?}"),
        };
        let response = dentry_batch_response(&first_names, 40);
        write_frame(&mut stream, request_id, flags, &response).unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        let second_names = match request {
            MetadataRpcRequest::CreateFilesInDirPath {
                parent_path, names, ..
            } => {
                assert_eq!(parent_path, "/runs");
                assert_eq!(names.len(), 1);
                names
            }
            other => panic!("unexpected second request: {other:?}"),
        };
        let response = dentry_batch_response(
            &second_names,
            40 + u64::try_from(first_names.len()).expect("test batch fits u64"),
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = (0..=MAX_BATCH_RPC_REQUESTS)
        .map(|idx| format!("/runs/file-{idx:03}.bin"))
        .collect::<Vec<_>>();
    let entries = client.create_files(&paths, 0o644, 1000, 1000).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries.len(), MAX_BATCH_RPC_REQUESTS + 1);
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(
        entries[MAX_BATCH_RPC_REQUESTS].attr.inode.get(),
        40 + u64::try_from(MAX_BATCH_RPC_REQUESTS).expect("test batch fits u64")
    );
}

#[test]
fn service_remove_many_uses_single_batch_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::Batch { requests } => {
                assert_eq!(requests.len(), 2);
                assert!(matches!(
                    &requests[0],
                    MetadataRpcRequest::RemoveFilePath { path } if path == "/runs/a.bin"
                ));
                assert!(matches!(
                    &requests[1],
                    MetadataRpcRequest::RemoveFilePath { path } if path == "/runs/b.bin"
                ));
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"612e62696e","child":40,"child_type":"file","attr_generation":7},"attr":{"inode":40,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"622e62696e","child":41,"child_type":"file","attr_generation":8},"attr":{"inode":41,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a.bin".to_owned(), "/runs/b.bin".to_owned()];
    let entries = client.remove_many(&paths).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_rmdir_many_uses_single_batch_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        match request {
            MetadataRpcRequest::Batch { requests } => {
                assert_eq!(requests.len(), 2);
                assert!(matches!(
                    &requests[0],
                    MetadataRpcRequest::RemoveEmptyDirPath { path } if path == "/runs/a"
                ));
                assert!(matches!(
                    &requests[1],
                    MetadataRpcRequest::RemoveEmptyDirPath { path } if path == "/runs/b"
                ));
            }
            other => panic!("unexpected request: {other:?}"),
        }
        let response = response_body(
            r#"{"ok":true,"result":{"type":"batch","results":[{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"61","child":40,"child_type":"directory","attr_generation":7},"attr":{"inode":40,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":null}}},{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":2,"name_hex":"62","child":41,"child_type":"directory","attr_generation":8},"attr":{"inode":41,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":null}}}]}}"#,
        );
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = MetadataClient::connect(addr);
    let paths = vec!["/runs/a".to_owned(), "/runs/b".to_owned()];
    let entries = client.rmdir_many(&paths).unwrap();
    let entries = entries.into_iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(entries[0].attr.inode.get(), 40);
    assert_eq!(entries[1].attr.inode.get(), 41);
}

#[test]
fn service_framed_rpc_accepts_out_of_order_responses() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let first = read_frame(&mut stream).unwrap();
        let second = read_frame(&mut stream).unwrap();
        let first_request = decode_request(&first.2).unwrap();
        let second_request = decode_request(&second.2).unwrap();
        let first_response = dentry_response_for_request(&first_request);
        let second_response = dentry_response_for_request(&second_request);

        write_frame(&mut stream, second.0, second.1, &second_response).unwrap();
        write_frame(&mut stream, first.0, first.1, &first_response).unwrap();
    });

    let client = Arc::new(MetadataClient::connect(addr));
    let first = {
        let client = Arc::clone(&client);
        thread::spawn(move || client.create_file("/runs/a.bin", 0o644, 1000, 1000))
    };
    let second = {
        let client = Arc::clone(&client);
        thread::spawn(move || client.create_file("/runs/b.bin", 0o644, 1000, 1000))
    };

    let first = first.join().unwrap().unwrap();
    let second = second.join().unwrap().unwrap();
    assert_eq!(first.dentry.name.as_bytes(), b"a.bin");
    assert_eq!(second.dentry.name.as_bytes(), b"b.bin");
}

#[test]
fn service_xattr_rpc_round_trips_bytes_names() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::SetXattr {
                inode: 42,
                name_hex: "757365722e636f6d6d656e74".to_owned(),
                value: b"training checkpoint".to_vec(),
                mode: WireXattrSetMode::Create,
            }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::Unit),
                error: None,
                error_kind: None,
            })
            .unwrap(),
        )
        .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::GetXattr {
                inode: 42,
                name_hex: "757365722e636f6d6d656e74".to_owned(),
            }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::XattrValue {
                    value: Some(b"training checkpoint".to_vec()),
                }),
                error: None,
                error_kind: None,
            })
            .unwrap(),
        )
        .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::ListXattr { inode: 42 }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::XattrNames {
                    names_hex: vec!["757365722e636f6d6d656e74".to_owned()],
                }),
                error: None,
                error_kind: None,
            })
            .unwrap(),
        )
        .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        assert_eq!(
            decode_request(&request).unwrap(),
            MetadataRpcRequest::RemoveXattr {
                inode: 42,
                name_hex: "757365722e636f6d6d656e74".to_owned(),
            }
        );
        write_frame(
            &mut stream,
            request_id,
            flags,
            &encode_envelope(&MetadataRpcEnvelope {
                ok: true,
                result: Some(MetadataRpcResult::Unit),
                error: None,
                error_kind: None,
            })
            .unwrap(),
        )
        .unwrap();
    });

    let client = MetadataClient::connect(addr);
    let inode = InodeId::new(42).unwrap();
    client
        .set_xattr(
            inode,
            b"user.comment",
            b"training checkpoint".to_vec(),
            XattrSetMode::Create,
        )
        .unwrap();
    assert_eq!(
        client.get_xattr(inode, b"user.comment").unwrap(),
        Some(b"training checkpoint".to_vec())
    );
    assert_eq!(
        client.list_xattr(inode).unwrap(),
        vec![b"user.comment".to_vec()]
    );
    client.remove_xattr(inode, b"user.comment").unwrap();
}

#[test]
fn service_error_without_error_kind_is_protocol_error() {
    let addr = serve_one(r#"{"ok":false,"error":"metadata command predicate failed"}"#);
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Protocol(ref err)
                if err.contains("missing typed error_kind")
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn service_typed_error_maps_predicate_failed_to_metadata_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"metadata command predicate failed","error_kind":{"type":"predicate_failed"}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::PredicateFailed
        ))
    ));
}

#[test]
fn service_typed_error_maps_stale_generation_to_metadata_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"body generation 7 is stale; current generation is 8","error_kind":{"type":"stale_body_generation","expected":7,"current":8}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client
        .read_body_plan(InodeId::new(42).unwrap(), 7, 0, 1)
        .unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::StaleBodyGeneration {
            expected: 7,
            current: 8
        })
    ));
}

#[test]
fn service_typed_error_maps_backend_metadata_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"metadata backend unavailable","error_kind":{"type":"metadata","message":"metadata backend unavailable"}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::Backend(ref message)
        )) if message == "metadata backend unavailable"
    ));
}

#[test]
fn service_typed_error_maps_backend_object_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"object backend unavailable","error_kind":{"type":"object","message":"object backend unavailable"}}"#,
    );
    let client = MetadataClient::connect(addr);
    let err = client.mkdir("/runs", 0o755, 1000, 1000).unwrap_err();
    assert!(matches!(
        err,
        ClientError::Object(nokv_object::ObjectError::Backend(ref message))
            if message == "object backend unavailable"
    ));
}

#[test]
fn service_snapshot_cat_uses_snapshot_file_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadArtifactPathAtSnapshot { root_path, snapshot_id, path }
                if root_path == "/runs" && snapshot_id == 9 && path == "/checkpoint"
        ));
        let response =
            response_body(r#"{"ok":true,"result":{"type":"file_bytes","bytes":[111,108,100]}}"#);
        write_frame(&mut stream, request_id, flags, &response).unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    assert_eq!(
        client.cat_snapshot("/runs", 9, "/checkpoint").unwrap(),
        b"old"
    );
}

#[test]
fn service_snapshot_namespace_methods_use_snapshot_rooted_rpcs() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::StatPathAtSnapshot { root_path, snapshot_id, path }
                if root_path == "/runs" && snapshot_id == 9 && path == "/"
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":2,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":2,"mtime_ms":2,"ctime_ms":2},"body":null}}}"#,
                ),
            )
            .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathAtSnapshot { root_path, snapshot_id, path }
                if root_path == "/runs" && snapshot_id == 9 && path == "/"
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries","entries":[{"dentry":{"parent":2,"name_hex":"6e6573746564","child":3,"child_type":"directory","attr_generation":3},"attr":{"inode":3,"file_type":"directory","mode":493,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}]}}"#,
                ),
            )
            .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadFilePathAtSnapshot {
                snapshot_id,
                root_path,
                path,
                offset,
                len
            } if root_path == "/runs" && snapshot_id == 9 && path == "/nested/model.bin" && offset == 7 && len == 3
        ));
        write_frame(
            &mut stream,
            request_id,
            flags,
            &response_body(r#"{"ok":true,"result":{"type":"file_bytes","bytes":[111,108,100]}}"#),
        )
        .unwrap();
    });

    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let root = client
        .metadata()
        .stat_path_at_snapshot("/runs", 9, "/")
        .unwrap()
        .unwrap();
    assert_eq!(root.attr.inode.get(), 2);
    let entries = client
        .metadata()
        .list_path_at_snapshot("/runs", 9, "/")
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].dentry.name.as_bytes(), b"nested");
    assert_eq!(
        client
            .read_snapshot("/runs", 9, "/nested/model.bin", 7, 3)
            .unwrap(),
        b"old"
    );
}

#[test]
fn service_snapshot_list_page_uses_cursor_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathAtSnapshotPage {
                root_path,
                snapshot_id,
                path,
                after_name_hex: Some(after),
                limit,
            } if root_path == "/workbench" && snapshot_id == 9 && path == "/outputs" && after == "612e747874" && limit == 2
        ));
        write_frame(
            &mut stream,
            request_id,
            flags,
            &response_body(
                r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e747874","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":1,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"622e747874"}}"#,
            ),
        )
        .unwrap();
    });

    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let after = DentryName::new(b"a.txt".to_vec()).unwrap();
    let page = client
        .metadata()
        .list_path_at_snapshot_page("/workbench", 9, "/outputs", Some(&after), 2)
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].dentry.name.as_bytes(), b"b.txt");
    assert_eq!(page.next_cursor.unwrap().as_bytes(), b"b.txt");
}

#[test]
fn service_list_page_uses_cursor_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs" && after_name_hex.as_deref() == Some("612e62696e") && limit == 2
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e62696e","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"622e62696e"}}"#,
                ),
            )
            .unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let after = DentryName::new(b"a.bin".to_vec()).unwrap();
    let page = client
        .metadata()
        .list_page("/runs", Some(&after), 2)
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].dentry.name.as_bytes(), b"b.bin");
    assert_eq!(
        page.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"b.bin".as_slice())
    );
}

#[test]
fn service_indexed_list_page_uses_indexed_cursor_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadIndexedPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs" && after_name_hex.as_deref() == Some("612e62696e") && limit == 2
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e62696e","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"622e62696e"}}"#,
                ),
            )
            .unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let after = DentryName::new(b"a.bin".to_vec()).unwrap();
    let page = client
        .metadata()
        .list_indexed_page("/runs", Some(&after), 2)
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].dentry.name.as_bytes(), b"b.bin");
    assert_eq!(
        page.next_cursor.as_ref().map(DentryName::as_bytes),
        Some(b"b.bin".as_slice())
    );
}

#[test]
fn service_list_uses_paged_rpc_until_cursor_is_exhausted() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs" && after_name_hex.is_none() && limit == DEFAULT_LIST_PAGE_SIZE
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"612e62696e","child":3,"child_type":"file","attr_generation":3},"attr":{"inode":3,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":3,"mtime_ms":3,"ctime_ms":3},"body":null}],"next_name_hex":"612e62696e"}}"#,
                ),
            )
            .unwrap();

        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::ReadDirPlusPathPage {
                path,
                after_name_hex,
                limit
            } if path == "/runs"
                && after_name_hex.as_deref() == Some("612e62696e")
                && limit == DEFAULT_LIST_PAGE_SIZE
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"dentries_page","entries":[{"dentry":{"parent":2,"name_hex":"622e62696e","child":4,"child_type":"file","attr_generation":4},"attr":{"inode":4,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":0,"generation":4,"mtime_ms":4,"ctime_ms":4},"body":null}],"next_name_hex":null}}"#,
                ),
            )
            .unwrap();
    });
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let entries = client.metadata().list("/runs").unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.dentry.name.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"a.bin".as_slice(), b"b.bin".as_slice()]
    );
}

#[test]
fn service_metadata_stat_path_uses_path_metadata_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut magic = [0_u8; FRAMED_RPC_MAGIC.len()];
        stream.read_exact(&mut magic).unwrap();
        assert_eq!(&magic, FRAMED_RPC_MAGIC);
        let (request_id, flags, request) = read_frame(&mut stream).unwrap();
        let request = decode_request(&request).unwrap();
        assert!(matches!(
            request,
            MetadataRpcRequest::StatPath { path } if path == "/artifact.bin"
        ));
        write_frame(
                &mut stream,
                request_id,
                flags,
                &response_body(
                    r#"{"ok":true,"result":{"type":"path_metadata","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}}"#,
                ),
            )
            .unwrap();
    });

    let client = MetadataClient::connect(addr);
    let metadata = client.stat_path("/artifact.bin").unwrap().unwrap();
    assert_eq!(metadata.attr.inode.get(), 42);
    assert_eq!(metadata.body.unwrap().digest_uri, "sha256:demo");
}

#[test]
fn service_file_client_read_path_returns_metadata_and_checks_expected_generation() {
    let store = MemoryObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/demo").unwrap(),
            b"hello server".to_vec(),
        )
        .unwrap();
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"open_path_read_plan","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":6,"object_len":12,"len":6,"output_offset":0}]}}}"#,
    );
    let mut client = NoKvFsClient::connect(addr, store);
    client.set_block_cache_enabled(false);
    let read = client.read_path("/artifact.bin", 6, 6, Some(7)).unwrap();
    assert_eq!(read.bytes, b"server");
    assert_eq!(read.metadata.attr.generation, 7);
    assert_eq!(read.metadata.body.unwrap().digest_uri, "sha256:demo");

    let addr = serve_one(
        r#"{"ok":false,"error":"stale body generation","error_kind":{"type":"stale_body_generation","expected":7,"current":8}}"#,
    );
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let err = client
        .read_path("/artifact.bin", 0, 6, Some(7))
        .unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::StaleBodyGeneration {
            expected: 7,
            current: 8
        })
    ));
}

#[test]
fn service_file_client_read_path_uses_batched_object_gets() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/a").unwrap(), b"abcdefgh".to_vec())
        .unwrap();
    store
        .put(&ObjectKey::new("blocks/b").unwrap(), b"uvwxyz".to_vec())
        .unwrap();
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"open_path_read_plan","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":8,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":3,"output_offset":0},{"object_key":"blocks/b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":2,"output_offset":3}]}}}"#,
    );
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let read = client.read_path("/artifact.bin", 0, 5, Some(7)).unwrap();

    assert_eq!(read.bytes, b"bcdwx");
    assert_eq!(store.batch_sizes(), vec![2]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 2);
    assert_eq!(stats.object_fallbacks, 2);
    assert_eq!(stats.local_nvme_hits, 0);
    assert_eq!(stats.object_gets, 2);
    assert_eq!(stats.object_get_bytes, 5);
}

#[test]
fn service_file_client_read_path_reports_coalesced_layout_ranges() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/a").unwrap(), b"abcdefgh".to_vec())
        .unwrap();
    let addr = serve_one(
        r#"{"ok":true,"result":{"type":"open_path_read_plan","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":6,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":6,"blocks":[{"object_key":"blocks/a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":3,"output_offset":0},{"object_key":"blocks/a","digest_uri":"sha256:test","object_offset":4,"object_len":8,"len":3,"output_offset":3}]}}}"#,
    );
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let read = client.read_path("/artifact.bin", 0, 6, Some(7)).unwrap();

    assert_eq!(read.bytes, b"bcdefg");
    assert_eq!(store.batch_sizes(), vec![1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 2);
    assert_eq!(stats.object_fallbacks, 2);
    assert_eq!(stats.object_gets, 1);
    assert_eq!(stats.object_get_bytes, 6);
    assert_eq!(stats.coalesced_ranges, 1);
    assert_eq!(stats.coalesced_range_bytes, 6);
}

#[test]
fn metadata_client_open_read_plan_returns_lease_and_plan() {
    let addr = serve_one_request(|request| {
        assert!(matches!(
            &request,
            MetadataRpcRequest::OpenPathReadPlan {
                path,
                offset: 6,
                len: 5,
                expected_generation: Some(7)
            } if path == "/artifact.bin"
        ));
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":6,"object_len":12,"len":5,"output_offset":0}]}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let open = client
        .open_path_read_plan("/artifact.bin", 6, 5, Some(7))
        .unwrap();
    assert_eq!(open.metadata.attr.inode.get(), 42);
    assert_eq!(open.lease.inode.get(), 42);
    assert_eq!(open.lease.generation, 7);
    assert_eq!(open.plan.output_len, 5);
    assert_eq!(open.plan.blocks[0].digest_uri, "sha256:test");
}

#[test]
fn metadata_client_open_read_plan_batch_returns_plans() {
    let addr = serve_one_request(|request| {
        let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
            panic!("expected batch open request");
        };
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/sample-0.bin");
        assert_eq!(requests[0].offset, 1);
        assert_eq!(requests[0].len, 3);
        assert_eq!(requests[0].expected_generation, Some(7));
        assert_eq!(requests[1].path, "/sample-1.bin");
        assert_eq!(requests[1].offset, 2);
        assert_eq!(requests[1].len, 2);
        assert_eq!(requests[1].expected_generation, None);
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:a","size":8,"content_type":"application/octet-stream","manifest_id":"sample-0.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":3,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:b","size":6,"content_type":"application/octet-stream","manifest_id":"sample-1.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":2,"blocks":[{"object_key":"blocks/b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":2,"output_offset":0}]}}]}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let opens = client
        .open_path_read_plan_batch(&[
            PathLayoutOpenRequest::new("/sample-0.bin", 1, 3).with_expected_generation(7),
            PathLayoutOpenRequest::new("/sample-1.bin", 2, 2),
        ])
        .unwrap();

    assert_eq!(opens.len(), 2);
    assert_eq!(opens[0].metadata.attr.inode.get(), 42);
    assert_eq!(opens[0].plan.output_len, 3);
    assert_eq!(opens[1].metadata.attr.inode.get(), 43);
    assert_eq!(opens[1].plan.output_len, 2);
    assert_eq!(opens[0].lease.read_version, opens[1].lease.read_version);
}

#[test]
fn metadata_client_open_read_plan_batch_chunks_large_requests() {
    let addr = serve_many(vec![
        open_path_read_plan_batch_response(1000, 128),
        open_path_read_plan_batch_response(1128, 1),
    ]);
    let client = MetadataClient::connect(addr);
    let requests = (0..129)
        .map(|index| PathLayoutOpenRequest::new(format!("/sample-{index}.bin"), 0, 1))
        .collect::<Vec<_>>();

    let opens = client.open_path_read_plan_batch(&requests).unwrap();

    assert_eq!(opens.len(), 129);
    assert_eq!(opens[0].metadata.attr.inode.get(), 1000);
    assert_eq!(opens[127].metadata.attr.inode.get(), 1127);
    assert_eq!(opens[128].metadata.attr.inode.get(), 1128);
}

#[test]
fn metadata_client_open_read_plan_batch_pins_first_generation_across_chunks() {
    // 129 windows for ONE file split into two metadata-open chunks (128 + 1). The
    // caller pins no generation, so the first chunk opens at the file's current
    // generation (7); the second chunk must inherit that generation as its
    // expected_generation so the two chunks can never splice two generations.
    let addr = serve_request_sequence(vec![
        Box::new(|request| {
            let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
                panic!("expected batch open request");
            };
            assert_eq!(requests.len(), 128);
            // The caller did not pin a generation, so the first chunk opens unpinned.
            assert!(requests.iter().all(|r| r.path == "/big.bin"));
            assert!(requests.iter().all(|r| r.expected_generation.is_none()));
            open_path_read_plan_batch_response(1000, 128)
        }),
        Box::new(|request| {
            let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
                panic!("expected batch open request");
            };
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].path, "/big.bin");
            // The second chunk inherits the generation observed in the first chunk.
            assert_eq!(
                requests[0].expected_generation,
                Some(7),
                "the second chunk of a file must inherit the first chunk's generation"
            );
            open_path_read_plan_batch_response(1128, 1)
        }),
    ]);
    let client = MetadataClient::connect(addr);
    let requests = (0..129)
        .map(|_| PathLayoutOpenRequest::new("/big.bin", 0, 1))
        .collect::<Vec<_>>();

    let opens = client.open_path_read_plan_batch(&requests).unwrap();

    assert_eq!(opens.len(), 129);
    assert!(opens.iter().all(|open| open.lease.generation == 7));
}

#[test]
fn metadata_client_open_read_plan_batch_respects_caller_pinned_generation() {
    // When the caller already pins a generation, every chunk of that file carries
    // it unchanged (the pin map never overrides a caller-supplied generation).
    let addr = serve_request_sequence(vec![
        Box::new(|request| {
            let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
                panic!("expected batch open request");
            };
            assert_eq!(requests.len(), 128);
            assert!(requests.iter().all(|r| r.expected_generation == Some(7)));
            open_path_read_plan_batch_response(1000, 128)
        }),
        Box::new(|request| {
            let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
                panic!("expected batch open request");
            };
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].expected_generation, Some(7));
            open_path_read_plan_batch_response(1128, 1)
        }),
    ]);
    let client = MetadataClient::connect(addr);
    let requests = (0..129)
        .map(|_| PathLayoutOpenRequest::new("/big.bin", 0, 1).with_expected_generation(7))
        .collect::<Vec<_>>();

    let opens = client.open_path_read_plan_batch(&requests).unwrap();
    assert_eq!(opens.len(), 129);
}

#[test]
fn metadata_client_preserves_stale_owner_epoch_error() {
    let addr = serve_one(
        r#"{"ok":false,"error":"owner epoch 1 is stale; required owner epoch is 2","error_kind":{"type":"stale_owner_epoch","owner_epoch":1,"required_epoch":2}}"#,
    );
    let client = MetadataClient::connect(addr);

    let err = client.mkdir("/stale-owner", 0o755, 1000, 1000).unwrap_err();

    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::StaleOwnerEpoch {
            owner_epoch: 1,
            required_epoch: 2
        })
    ));
}

#[test]
fn service_file_client_read_paths_uses_single_batch_open() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/a").unwrap(), b"abcdefgh".to_vec())
        .unwrap();
    store
        .put(&ObjectKey::new("blocks/b").unwrap(), b"uvwxyz".to_vec())
        .unwrap();
    let addr = serve_one_request(|request| {
        let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
            panic!("expected batch open request");
        };
        assert_eq!(requests.len(), 2);
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:a","size":8,"content_type":"application/octet-stream","manifest_id":"sample-0.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":3,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:b","size":6,"content_type":"application/octet-stream","manifest_id":"sample-1.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":2,"blocks":[{"object_key":"blocks/b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":2,"output_offset":0}]}}]}}"#,
        )
    });
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let reads = client
        .read_paths(&[
            PathLayoutOpenRequest::new("/sample-0.bin", 1, 3).with_expected_generation(7),
            PathLayoutOpenRequest::new("/sample-1.bin", 2, 2),
        ])
        .unwrap();

    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0].bytes, b"bcd");
    assert_eq!(reads[1].bytes, b"wx");
    assert_eq!(store.batch_sizes(), vec![1, 1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 2);
    assert_eq!(stats.object_gets, 2);
    assert_eq!(stats.object_get_bytes, 5);
}

#[test]
fn service_file_client_read_paths_reads_distinct_plans_in_parallel() {
    let store = ConcurrentBatchTrackingObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/a").unwrap(), b"abcdefgh".to_vec())
        .unwrap();
    store
        .put(&ObjectKey::new("blocks/b").unwrap(), b"uvwxyz".to_vec())
        .unwrap();
    let addr = serve_one_request(|request| {
        let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
            panic!("expected batch open request");
        };
        assert_eq!(requests.len(), 2);
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:a","size":8,"content_type":"application/octet-stream","manifest_id":"sample-0.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":3,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:b","size":6,"content_type":"application/octet-stream","manifest_id":"sample-1.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":2,"blocks":[{"object_key":"blocks/b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":2,"output_offset":0}]}}]}}"#,
        )
    });
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let reads = client
        .read_paths(&[
            PathLayoutOpenRequest::new("/sample-0.bin", 1, 3).with_expected_generation(7),
            PathLayoutOpenRequest::new("/sample-1.bin", 2, 2).with_expected_generation(8),
        ])
        .unwrap();

    assert_eq!(reads.len(), 2);
    assert_eq!(reads[0].bytes, b"bcd");
    assert_eq!(reads[1].bytes, b"wx");
    assert!(
        store.max_inflight() >= 2,
        "distinct files in one training batch should issue object reads concurrently"
    );
}

#[test]
fn service_file_client_read_path_ranges_coalesces_contiguous_ranges() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    let addr = serve_one_request(|request| {
        assert!(matches!(
            &request,
            MetadataRpcRequest::OpenPathReadPlan {
                path,
                offset: 1,
                len: 5,
                expected_generation: Some(7)
            } if path == "/shard.bin"
        ));
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan","metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard","size":8,"content_type":"application/octet-stream","manifest_id":"shard.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/shard","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":5,"output_offset":0}]}}}"#,
        )
    });
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let reads = client
        .read_path_ranges("/shard.bin", &[(1, 3), (4, 2)], Some(7), 0)
        .unwrap();

    assert_eq!(reads, vec![b"bcd".to_vec(), b"ef".to_vec()]);
    assert_eq!(store.batch_sizes(), vec![1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 1);
    assert_eq!(stats.object_gets, 1);
    assert_eq!(stats.object_get_bytes, 5);
}

#[test]
fn service_file_client_read_path_ranges_batch_uses_single_batch_open() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    store
        .put(
            &ObjectKey::new("blocks/shard-b").unwrap(),
            b"uvwxyz".to_vec(),
        )
        .unwrap();
    let addr = serve_one_request(|request| {
        let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
            panic!("expected batch open request");
        };
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/shard-a.bin");
        assert_eq!(requests[0].offset, 1);
        assert_eq!(requests[0].len, 5);
        assert_eq!(requests[0].expected_generation, Some(7));
        assert_eq!(requests[1].path, "/shard-b.bin");
        assert_eq!(requests[1].offset, 2);
        assert_eq!(requests[1].len, 3);
        assert_eq!(requests[1].expected_generation, Some(8));
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard-a","size":8,"content_type":"application/octet-stream","manifest_id":"shard-a.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":5,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:shard-b","size":6,"content_type":"application/octet-stream","manifest_id":"shard-b.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/shard-b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":3,"output_offset":0}]}}]}}"#,
        )
    });
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let reads = client
        .read_path_ranges_batch(&[
            PathRangeReadRequest::new(
                "/shard-a.bin",
                vec![PathReadRange::new(1, 3), PathReadRange::new(4, 2)],
            )
            .with_expected_generation(7),
            PathRangeReadRequest::new(
                "/shard-b.bin",
                vec![PathReadRange::new(2, 2), PathReadRange::new(4, 1)],
            )
            .with_expected_generation(8),
        ])
        .unwrap();

    assert_eq!(
        reads,
        vec![
            vec![b"bcd".to_vec(), b"ef".to_vec()],
            vec![b"wx".to_vec(), b"y".to_vec()]
        ]
    );
    assert_eq!(store.batch_sizes(), vec![1, 1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 2);
    assert_eq!(stats.object_gets, 2);
    assert_eq!(stats.object_get_bytes, 8);
}

#[test]
fn service_file_client_read_path_ranges_batch_packed_uses_single_batch_open() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    store
        .put(
            &ObjectKey::new("blocks/shard-b").unwrap(),
            b"uvwxyz".to_vec(),
        )
        .unwrap();
    let addr = serve_one_request(|request| {
        let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
            panic!("expected batch open request");
        };
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/shard-a.bin");
        assert_eq!(requests[0].offset, 1);
        assert_eq!(requests[0].len, 5);
        assert_eq!(requests[0].expected_generation, Some(7));
        assert_eq!(requests[1].path, "/shard-b.bin");
        assert_eq!(requests[1].offset, 2);
        assert_eq!(requests[1].len, 3);
        assert_eq!(requests[1].expected_generation, Some(8));
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard-a","size":8,"content_type":"application/octet-stream","manifest_id":"shard-a.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":5,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:shard-b","size":6,"content_type":"application/octet-stream","manifest_id":"shard-b.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/shard-b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":3,"output_offset":0}]}}]}}"#,
        )
    });
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);

    let reads = client
        .read_path_ranges_batch_packed(&[
            PathRangeReadRequest::new(
                "/shard-a.bin",
                vec![PathReadRange::new(1, 3), PathReadRange::new(4, 2)],
            )
            .with_expected_generation(7),
            PathRangeReadRequest::new(
                "/shard-b.bin",
                vec![PathReadRange::new(2, 2), PathReadRange::new(4, 1)],
            )
            .with_expected_generation(8),
        ])
        .unwrap();

    assert_eq!(reads, vec![b"bcdef".to_vec(), b"wxy".to_vec()]);
    assert_eq!(store.batch_sizes(), vec![1, 1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 2);
    assert_eq!(stats.object_gets, 2);
    assert_eq!(stats.object_get_bytes, 8);
}

#[test]
fn service_file_client_read_path_ranges_batch_into_uses_caller_buffer() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    store
        .put(
            &ObjectKey::new("blocks/shard-b").unwrap(),
            b"uvwxyz".to_vec(),
        )
        .unwrap();
    let addr = serve_one_request(|request| {
        let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
            panic!("expected batch open request");
        };
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/shard-a.bin");
        assert_eq!(requests[0].offset, 1);
        assert_eq!(requests[0].len, 5);
        assert_eq!(requests[0].expected_generation, Some(7));
        assert_eq!(requests[1].path, "/shard-b.bin");
        assert_eq!(requests[1].offset, 2);
        assert_eq!(requests[1].len, 3);
        assert_eq!(requests[1].expected_generation, Some(8));
        response_body(
            r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":8,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard-a","size":8,"content_type":"application/octet-stream","manifest_id":"shard-a.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":5,"blocks":[{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":1,"object_len":8,"len":5,"output_offset":0}]}},{"metadata":{"attr":{"inode":43,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":6,"generation":8,"mtime_ms":8,"ctime_ms":8},"body":{"producer":"unit-test","digest_uri":"sha256:shard-b","size":6,"content_type":"application/octet-stream","manifest_id":"shard-b.bin","generation":8,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":43,"generation":8,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":3,"blocks":[{"object_key":"blocks/shard-b","digest_uri":"sha256:test","object_offset":2,"object_len":6,"len":3,"output_offset":0}]}}]}}"#,
        )
    });
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);
    let mut output = vec![0_u8; 8];

    let lengths = client
        .read_path_ranges_batch_into(
            &[
                PathRangeReadRequest::new(
                    "/shard-a.bin",
                    vec![PathReadRange::new(1, 3), PathReadRange::new(4, 2)],
                )
                .with_expected_generation(7),
                PathRangeReadRequest::new(
                    "/shard-b.bin",
                    vec![PathReadRange::new(2, 2), PathReadRange::new(4, 1)],
                )
                .with_expected_generation(8),
            ],
            &mut output,
            &[0, 5],
        )
        .unwrap();

    assert_eq!(lengths, vec![5, 3]);
    assert_eq!(output, b"bcdefwxy");
    assert_eq!(store.batch_sizes(), vec![1, 1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 2);
    assert_eq!(stats.object_gets, 2);
    assert_eq!(stats.object_get_bytes, 8);
}

#[test]
fn service_file_client_prepared_path_ranges_batch_reuses_native_layout() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    store
        .put(
            &ObjectKey::new("blocks/shard-b").unwrap(),
            b"uvwxyz".to_vec(),
        )
        .unwrap();
    let addr = serve_request_sequence(vec![
        Box::new(two_shard_batch_response),
        Box::new(two_shard_batch_response),
    ]);
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);
    let requests = [
        PathRangeReadRequest::new(
            "/shard-a.bin",
            vec![PathReadRange::new(1, 3), PathReadRange::new(4, 2)],
        )
        .with_expected_generation(7),
        PathRangeReadRequest::new(
            "/shard-b.bin",
            vec![PathReadRange::new(2, 2), PathReadRange::new(4, 1)],
        )
        .with_expected_generation(8),
    ];

    let plan = client.prepare_path_ranges_batch(&requests).unwrap();

    assert_eq!(plan.request_count(), 2);
    assert_eq!(plan.range_count(), 4);
    assert_eq!(plan.output_len(), 8);
    assert_eq!(plan.request_layout(), vec![(0, 5), (5, 3)]);

    let mut first = vec![0_u8; plan.output_len()];
    let first_lengths = client
        .read_prepared_path_ranges_batch_into(&plan, &mut first)
        .unwrap();
    let mut second = vec![0_u8; plan.output_len()];
    let second_lengths = client
        .read_prepared_path_ranges_batch_into(&plan, &mut second)
        .unwrap();

    assert_eq!(first_lengths, vec![5, 3]);
    assert_eq!(second_lengths, vec![5, 3]);
    assert_eq!(first, b"bcdefwxy");
    assert_eq!(second, b"bcdefwxy");
    assert_eq!(store.batch_sizes(), vec![1, 1, 1, 1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 4);
    assert_eq!(stats.object_gets, 4);
    assert_eq!(stats.object_get_bytes, 16);
}

#[test]
fn service_file_client_read_path_ranges_batch_into_keeps_coalesced_gap_window() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    let addr = serve_one_request(coalesced_gap_window_batch_response);
    let mut client = NoKvFsClient::connect(addr, store.clone());
    client.set_block_cache_enabled(false);
    let mut output = vec![0_u8; 4];

    let lengths = client
        .read_path_ranges_batch_into(
            &[PathRangeReadRequest::new(
                "/shard-a.bin",
                vec![PathReadRange::new(1, 2), PathReadRange::new(5, 2)],
            )
            .with_expected_generation(7)
            .with_max_gap_bytes(2)],
            &mut output,
            &[0],
        )
        .unwrap();

    assert_eq!(lengths, vec![4]);
    assert_eq!(output, b"bcfg");
    assert_eq!(store.batch_sizes(), vec![1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 1);
    assert_eq!(stats.object_gets, 1);
    assert_eq!(stats.object_get_bytes, 6);
}

#[test]
fn service_file_client_read_path_ranges_batch_into_uses_cache_aware_scatter_for_hot_gap_window() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"abcdefgh".to_vec(),
        )
        .unwrap();
    let addr = serve_request_sequence(vec![
        Box::new(coalesced_gap_window_batch_response),
        Box::new(coalesced_gap_window_batch_response),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    let requests = [PathRangeReadRequest::new(
        "/shard-a.bin",
        vec![PathReadRange::new(1, 2), PathReadRange::new(5, 2)],
    )
    .with_expected_generation(7)
    .with_max_gap_bytes(2)];

    let mut cold_output = vec![0_u8; 4];
    let cold_lengths = client
        .read_path_ranges_batch_into(&requests, &mut cold_output, &[0])
        .unwrap();
    assert_eq!(cold_lengths, vec![4]);
    assert_eq!(cold_output, b"bcfg");
    assert_eq!(store.batch_sizes(), vec![1]);

    let mut warm_output = vec![0_u8; 4];
    let warm_lengths = client
        .read_path_ranges_batch_into(&requests, &mut warm_output, &[0])
        .unwrap();

    assert_eq!(warm_lengths, vec![4]);
    assert_eq!(warm_output, b"bcfg");
    assert_eq!(store.batch_sizes(), vec![1]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.planned_blocks, 3);
    assert_eq!(stats.object_gets, 1);
    assert_eq!(stats.object_get_bytes, 6);
    assert_eq!(stats.cache_hits, 2);
    assert_eq!(stats.cache_hit_bytes, 4);
}

/// A coalesced window whose middle range falls in a sparse-file HOLE: block A
/// backs file offsets [1,9) (object [0,8)) and block B backs file offsets
/// [13,15) (object [12,14)), but file offsets [9,13) have no backing block. The
/// requested ranges R0=[1,2), R1=[5,7), R3=[13,15) are split across A and B (so
/// the scatter expands physical reads and takes the cache fast-path), while
/// R2=[10,12) lands squarely in the hole.
fn sparse_hole_window_batch_response(request: MetadataRpcRequest) -> Vec<u8> {
    let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
        panic!("expected batch open request");
    };
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].path, "/shard-a.bin");
    assert_eq!(requests[0].offset, 1);
    assert_eq!(requests[0].len, 14);
    assert_eq!(requests[0].expected_generation, Some(7));
    response_body(
        r#"{"ok":true,"result":{"type":"open_path_read_plan_batch","plans":[{"metadata":{"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":15,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard-a","size":15,"content_type":"application/octet-stream","manifest_id":"shard-a.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"lease":{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345},"plan":{"output_len":14,"blocks":[{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":0,"object_len":16,"len":8,"output_offset":0},{"object_key":"blocks/shard-a","digest_uri":"sha256:test","object_offset":12,"object_len":16,"len":2,"output_offset":12}]}}]}}"#,
    )
}

#[test]
fn service_file_client_read_path_ranges_batch_into_zero_fills_sparse_hole_in_hot_scatter() {
    let store = BatchTrackingObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard-a").unwrap(),
            b"ABCDEFGHIJKLMNOP".to_vec(),
        )
        .unwrap();
    let addr = serve_request_sequence(vec![
        Box::new(sparse_hole_window_batch_response),
        Box::new(sparse_hole_window_batch_response),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    // R0=[1,2)->"AB", R1=[5,7)->"EF", R2=[10,12)->hole, R3=[13,15)->"MN". A gap of
    // 3 (R1 end 7 .. R2 start 10) is the largest, so coalesce them all.
    let requests = [PathRangeReadRequest::new(
        "/shard-a.bin",
        vec![
            PathReadRange::new(1, 2),
            PathReadRange::new(5, 2),
            PathReadRange::new(10, 2),
            PathReadRange::new(13, 2),
        ],
    )
    .with_expected_generation(7)
    .with_max_gap_bytes(3)];

    // Cold read warms the block cache for blocks A and B.
    let mut cold_output = vec![0_u8; 8];
    let cold_lengths = client
        .read_path_ranges_batch_into(&requests, &mut cold_output, &[0])
        .unwrap();
    assert_eq!(cold_lengths, vec![8]);
    assert_eq!(cold_output, b"ABEF\0\0MN");

    // Warm read into a DIRTY (reused) buffer pre-filled with a stale marker. The
    // hot scatter cache path must zero the sparse hole, not leave stale bytes.
    let mut warm_output = vec![0xFF_u8; 8];
    let warm_lengths = client
        .read_path_ranges_batch_into(&requests, &mut warm_output, &[0])
        .unwrap();

    assert_eq!(warm_lengths, vec![8]);
    assert_eq!(
        warm_output, b"ABEF\0\0MN",
        "the sparse hole must read as zeros, not stale bytes from the reused buffer"
    );
    // The warm read served the data ranges from cache: the only object I/O was
    // the single cold `get_many` that coalesced the two backing blocks.
    assert_eq!(store.batch_sizes(), vec![2]);
    let stats = client.data_fabric_stats().unwrap();
    assert_eq!(stats.cache_hits, 3);
    assert_eq!(stats.cache_hit_bytes, 6);
}

#[test]
fn service_file_client_read_path_ranges_batch_into_rejects_overlapping_output_regions() {
    let addr = "127.0.0.1:1".parse().unwrap();
    let client = NoKvFsClient::connect(addr, MemoryObjectStore::new());
    let mut output = vec![0_u8; 8];

    let err = client
        .read_path_ranges_batch_into(
            &[
                PathRangeReadRequest::new(
                    "/shard-a.bin",
                    vec![PathReadRange::new(1, 3), PathReadRange::new(4, 2)],
                ),
                PathRangeReadRequest::new(
                    "/shard-b.bin",
                    vec![PathReadRange::new(2, 2), PathReadRange::new(4, 1)],
                ),
            ],
            &mut output,
            &[0, 3],
        )
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("packed range read into output regions must not overlap"));
}

#[test]
fn service_file_client_read_path_ranges_skips_small_next_sparse_window_prefetch() {
    let store = MemoryObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/shard").unwrap(),
            b"abcdefghij".to_vec(),
        )
        .unwrap();
    let attr = r#""attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":10,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:shard","size":10,"content_type":"application/octet-stream","manifest_id":"shard.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}"#;
    let addr = serve_request_sequence(vec![
        Box::new(move |request| {
            assert!(matches!(
                &request,
                MetadataRpcRequest::OpenPathReadPlan {
                    path,
                    offset: 2,
                    len: 2,
                    expected_generation: Some(7)
                } if path == "/shard.bin"
            ));
            response_body(&format!(
                r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{attr}}},"lease":{{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345}},"plan":{{"output_len":2,"blocks":[{{"object_key":"blocks/shard","digest_uri":"sha256:test","object_offset":2,"object_len":10,"len":2,"output_offset":0}}]}}}}}}"#,
            ))
        }),
        Box::new(move |request| {
            assert!(matches!(
                &request,
                MetadataRpcRequest::OpenPathReadPlan {
                    path,
                    offset: 6,
                    len: 2,
                    expected_generation: Some(7)
                } if path == "/shard.bin"
            ));
            response_body(&format!(
                r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{attr}}},"lease":{{"inode":42,"generation":7,"read_version":10,"lease_expires_unix_ms":12346}},"plan":{{"output_len":2,"blocks":[{{"object_key":"blocks/shard","digest_uri":"sha256:test","object_offset":6,"object_len":10,"len":2,"output_offset":0}}]}}}}}}"#,
            ))
        }),
    ]);
    let client = NoKvFsClient::connect(addr, store);

    let reads = client
        .read_path_ranges("/shard.bin", &[(2, 2), (6, 2)], Some(7), 0)
        .unwrap();

    assert_eq!(reads, vec![b"cd".to_vec(), b"gh".to_vec()]);
    assert_eq!(client.object_stats().prefetch_enqueued, 0);
}

#[test]
fn service_file_client_read_path_ranges_prefetches_large_next_sparse_window() {
    let window_len = DEFAULT_BLOCK_SIZE / 4;
    let first_offset = 2_u64;
    let second_offset = first_offset + window_len as u64 + 4;
    let file_size = usize::try_from(second_offset).unwrap() + window_len;
    let payload = (0..file_size)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let expected_first =
        payload[first_offset as usize..first_offset as usize + window_len].to_vec();
    let expected_second =
        payload[second_offset as usize..second_offset as usize + window_len].to_vec();
    let store = MemoryObjectStore::new();
    store
        .put(&ObjectKey::new("blocks/shard").unwrap(), payload)
        .unwrap();
    let attr = format!(
        r#""attr":{{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":{file_size},"generation":7,"mtime_ms":7,"ctime_ms":7}},"body":{{"producer":"unit-test","digest_uri":"sha256:shard","size":{file_size},"content_type":"application/octet-stream","manifest_id":"shard.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}"#
    );
    let open_attr = attr.clone();
    let addr = serve_request_sequence(vec![
        Box::new(move |request| {
            assert!(matches!(
                &request,
                MetadataRpcRequest::OpenPathReadPlan {
                    path,
                    offset,
                    len,
                    expected_generation: Some(7)
                } if path == "/shard.bin" && *offset == first_offset && *len == window_len as u64
            ));
            response_body(&format!(
                r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{open_attr}}},"lease":{{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345}},"plan":{{"output_len":{window_len},"blocks":[{{"object_key":"blocks/shard","digest_uri":"sha256:test","object_offset":{first_offset},"object_len":{file_size},"len":{window_len},"output_offset":0}}]}}}}}}"#,
            ))
        }),
        Box::new(move |request| {
            assert!(matches!(
                request,
                MetadataRpcRequest::ReadBodyPlan {
                    inode: 42,
                    generation: 7,
                    offset,
                    len
                } if offset == second_offset && len == window_len as u64
            ));
            response_body(&format!(
                r#"{{"ok":true,"result":{{"type":"body_read_plan","plan":{{"output_len":{window_len},"blocks":[{{"object_key":"blocks/shard","digest_uri":"sha256:test","object_offset":{second_offset},"object_len":{file_size},"len":{window_len},"output_offset":0}}]}}}}}}"#,
            ))
        }),
        Box::new(move |request| {
            assert!(matches!(
                &request,
                MetadataRpcRequest::OpenPathReadPlan {
                    path,
                    offset,
                    len,
                    expected_generation: Some(7)
                } if path == "/shard.bin" && *offset == second_offset && *len == window_len as u64
            ));
            response_body(&format!(
                r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{attr}}},"lease":{{"inode":42,"generation":7,"read_version":10,"lease_expires_unix_ms":12346}},"plan":{{"output_len":{window_len},"blocks":[{{"object_key":"blocks/shard","digest_uri":"sha256:test","object_offset":{second_offset},"object_len":{file_size},"len":{window_len},"output_offset":0}}]}}}}}}"#,
            ))
        }),
    ]);
    let client = NoKvFsClient::connect(addr, store);

    let reads = client
        .read_path_ranges(
            "/shard.bin",
            &[(first_offset, window_len), (second_offset, window_len)],
            Some(7),
            0,
        )
        .unwrap();

    assert_eq!(reads, vec![expected_first, expected_second]);
    assert!(client.object_stats().prefetch_enqueued >= 1);
}

#[test]
fn service_file_client_stats_deduplicate_background_prefetch_gets() {
    let store = MemoryObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/demo").unwrap(),
            b"abcdefghijklmnopqr".to_vec(),
        )
        .unwrap();
    let attr = r#""attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":18,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":18,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}"#;
    // Prefetch arms on the second sequential read. That read's forward-aligned
    // cache fill warms the remainder of the object [6,18), so the readahead it
    // schedules for [12,18) finds those bytes already cached: the background
    // prefetch dedups into a cache hit rather than re-issuing an object GET. The
    // third read is then served entirely from the warmed cache.
    let addr = serve_many(vec![
        response_body(&format!(
            r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{attr}}},"lease":{{"inode":42,"generation":7,"read_version":9,"lease_expires_unix_ms":12345}},"plan":{{"output_len":6,"blocks":[{{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":0,"object_len":18,"len":6,"output_offset":0}}]}}}}}}"#,
        )),
        response_body(&format!(
            r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{attr}}},"lease":{{"inode":42,"generation":7,"read_version":10,"lease_expires_unix_ms":12346}},"plan":{{"output_len":6,"blocks":[{{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":6,"object_len":18,"len":6,"output_offset":0}}]}}}}}}"#,
        )),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":12,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(&format!(
            r#"{{"ok":true,"result":{{"type":"open_path_read_plan","metadata":{{{attr}}},"lease":{{"inode":42,"generation":7,"read_version":11,"lease_expires_unix_ms":12347}},"plan":{{"output_len":6,"blocks":[{{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":12,"object_len":18,"len":6,"output_offset":0}}]}}}}}}"#,
        )),
    ]);
    let client = NoKvFsClient::connect(addr, store);

    assert_eq!(
        client
            .read_path("/artifact.bin", 0, 6, Some(7))
            .unwrap()
            .bytes,
        b"abcdef"
    );
    assert_eq!(
        client
            .read_path("/artifact.bin", 6, 6, Some(7))
            .unwrap()
            .bytes,
        b"ghijkl"
    );
    let deadline = Instant::now() + Duration::from_secs(2);
    while client.object_stats().prefetch_completed < 1 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        client
            .read_path("/artifact.bin", 12, 6, Some(7))
            .unwrap()
            .bytes,
        b"mnopqr"
    );

    let stats = client.object_stats();
    // A sequential read stream arms forward prefetch, and every enqueued prefetch
    // completes cleanly (none dropped or failed). The foreground stream then reads
    // bytes a prior read/prefetch already warmed straight from the cache instead of
    // re-fetching them (>=1 cache hit). The exact object-GET counts depend on the
    // readahead-window policy (forward-fill window size, prefetch overlap) and are
    // intentionally not pinned here — only the dedup invariant is.
    assert!(
        stats.prefetch_enqueued >= 1,
        "a sequential read stream must arm at least one forward prefetch"
    );
    assert_eq!(
        stats.prefetch_completed, stats.prefetch_enqueued,
        "every enqueued prefetch must complete"
    );
    assert_eq!(stats.prefetch_dropped, 0);
    assert_eq!(stats.prefetch_failed, 0);
    assert!(
        stats.cache_hits >= 1,
        "the foreground stream must reuse warmed bytes instead of re-fetching them"
    );
}

#[test]
fn service_file_client_reuses_prefetched_body_read_plan() {
    let store = MemoryObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/demo").unwrap(),
            b"abcdefghijklmnopqr".to_vec(),
        )
        .unwrap();
    // Forward readahead only arms on the *second* sequential read: the data
    // plane suppresses prefetch on a stream's first (possibly random) read, so
    // the two leading contiguous reads both miss the read-plan cache. The second
    // read's forward prefetch fills the plan for the next window [12,18), which
    // the third read then reuses (an exact read-plan-cache hit).
    let dentry = r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":18,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":18,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}}"#;
    let addr = serve_many(vec![
        response_body(dentry),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":0,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(dentry),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":6,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":12,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(dentry),
    ]);
    let client = NoKvFsClient::connect(addr, store);

    assert_eq!(client.read("/artifact.bin", 0, 6).unwrap(), b"abcdef");
    assert_eq!(client.read("/artifact.bin", 6, 6).unwrap(), b"ghijkl");
    assert_eq!(client.read("/artifact.bin", 12, 6).unwrap(), b"mnopqr");

    let stats = client.object_stats();
    assert_eq!(stats.read_plan_cache_misses, 2);
    assert_eq!(stats.read_plan_cache_hits, 1);
}

#[test]
fn service_file_client_reuses_covering_prefetched_body_read_plan() {
    let store = MemoryObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/demo").unwrap(),
            b"abcdefghijklmnopqr".to_vec(),
        )
        .unwrap();
    // Same readahead-arming policy as the exact-reuse test: the prefetch fires
    // on the second sequential read and fills the plan for window [12,18). The
    // third read asks for a strict sub-range [14,16) of that window, which the
    // read-plan cache satisfies by slicing the covering prefetched plan (a
    // covering, not exact, hit).
    let dentry = r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":18,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":18,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}}"#;
    let addr = serve_many(vec![
        response_body(dentry),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":0,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(dentry),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":6,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":12,"object_len":18,"len":6,"output_offset":0}]}}}"#,
        ),
        response_body(dentry),
    ]);
    let client = NoKvFsClient::connect(addr, store);

    assert_eq!(client.read("/artifact.bin", 0, 6).unwrap(), b"abcdef");
    assert_eq!(client.read("/artifact.bin", 6, 6).unwrap(), b"ghijkl");
    assert_eq!(client.read("/artifact.bin", 14, 2).unwrap(), b"op");

    let stats = client.object_stats();
    assert_eq!(stats.read_plan_cache_misses, 2);
    assert_eq!(stats.read_plan_cache_hits, 1);
}

#[test]
fn service_file_client_reads_body_from_object_store() {
    let store = MemoryObjectStore::new();
    store
        .put(
            &ObjectKey::new("blocks/demo").unwrap(),
            b"hello server".to_vec(),
        )
        .unwrap();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":12,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":12,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"body_read_plan","plan":{"output_len":6,"blocks":[{"object_key":"blocks/demo","digest_uri":"sha256:test","object_offset":6,"object_len":12,"len":6,"output_offset":0}]}}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store);
    let bytes = client.read("/artifact.bin", 6, 6).unwrap();
    assert_eq!(bytes, b"server");
}

#[test]
fn service_file_client_uploads_blocks_then_publishes_metadata() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"rename_replace","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":11,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:demo","size":11,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"replaced":null}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    let entry = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap();
    assert_eq!(entry.attr.inode.get(), 42);
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some(),
        "metadata publish should upload object block before metadata commit"
    );
}

#[test]
fn service_file_client_refreshes_and_restages_after_object_gc_epoch_change() {
    let store = MemoryObjectStore::new();
    let addr = serve_request_sequence(vec![
        Box::new(|request| {
            assert_eq!(
                request,
                MetadataRpcRequest::PrepareArtifactPath {
                    path: "/artifact.bin".to_owned(),
                    replace: false,
                }
            );
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","path":"/artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
            )
        }),
        Box::new(|request| {
            let MetadataRpcRequest::PublishPreparedArtifact { prepared, body, .. } = request else {
                panic!("expected initial prepared publish")
            };
            assert_eq!(prepared.generation, 7);
            assert_eq!(prepared.object_gc_claim_version, 12);
            assert_eq!(body.generation, 7);
            response_body(
                r#"{"ok":false,"error":"prepared artifact object-GC epoch 12 is stale; current epoch is 14","error_kind":{"type":"stale_prepared_artifact_object_gc_epoch","expected":12,"current":14}}"#,
            )
        }),
        Box::new(|request| {
            let MetadataRpcRequest::RefreshPreparedArtifactObjectGcEpoch { prepared } = request
            else {
                panic!("expected prepared object-GC refresh")
            };
            assert_eq!(prepared.path.as_deref(), Some("/artifact.bin"));
            assert_eq!(prepared.generation, 7);
            assert_eq!(prepared.object_gc_claim_version, 12);
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","path":"/artifact.bin","inode":42,"generation":13,"mtime_ms":1700000000001,"ctime_ms":1700000000001,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":14}}}"#,
            )
        }),
        Box::new(|request| {
            let MetadataRpcRequest::PublishPreparedArtifact { prepared, body, .. } = request else {
                panic!("expected refreshed prepared publish")
            };
            assert_eq!(prepared.generation, 13);
            assert_eq!(prepared.object_gc_claim_version, 14);
            assert_eq!(body.generation, 13);
            response_body(
                r#"{"ok":true,"result":{"type":"rename_replace","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":13},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":11,"generation":13,"mtime_ms":13,"ctime_ms":13},"body":{"producer":"unit-test","digest_uri":"sha256:artifact.bin","size":11,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":13,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"replaced":null}}"#,
            )
        }),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());

    let entry = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap();

    assert_eq!(entry.attr.generation, 13);
    assert!(store
        .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
        .unwrap()
        .is_none());
    assert!(store
        .head(&ObjectKey::new("blocks/1/42/13/0/0").unwrap())
        .unwrap()
        .is_some());
}

#[test]
fn service_file_client_replays_seekable_reader_from_initial_position_after_refresh() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"prepared artifact object-GC epoch 12 is stale; current epoch is 14","error_kind":{"type":"stale_prepared_artifact_object_gc_epoch","expected":12,"current":14}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":13,"mtime_ms":1700000000001,"ctime_ms":1700000000001,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":14}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"rename_replace","entry":{"dentry":{"parent":1,"name_hex":"61727469666163742e62696e","child":42,"child_type":"file","attr_generation":13},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":11,"generation":13,"mtime_ms":13,"ctime_ms":13},"body":{"producer":"unit-test","digest_uri":"sha256:artifact.bin","size":11,"content_type":"application/octet-stream","manifest_id":"artifact.bin","generation":13,"base_generation":0,"chunk_size":67108864,"block_size":4194304}},"replaced":null}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    let mut reader = std::io::Cursor::new(b"skip--hello world".to_vec());
    reader.set_position(6);

    let entry = client
        .put_artifact_from_reader("/artifact.bin", reader, artifact_metadata("artifact.bin"))
        .unwrap();

    assert_eq!(entry.attr.size, 11);
    assert!(store
        .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
        .unwrap()
        .is_none());
    assert!(store
        .head(&ObjectKey::new("blocks/1/42/13/0/0").unwrap())
        .unwrap()
        .is_some());
}

#[test]
fn service_file_client_cleans_staged_blocks_after_publish_failure() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"metadata command predicate failed","error_kind":{"type":"predicate_failed"}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());
    let err = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap_err();
    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::PredicateFailed
        ))
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_none(),
        "failed metadata publish should clean staged object block"
    );
}

#[test]
fn service_file_client_keeps_bytes_staged_when_publish_reports_committed() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"metadata sync log archive failed after commit","error_kind":{"type":"sync_log_archive_failed","committed":true,"message":"injected"}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());

    let err = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::SyncLogArchiveFailed {
            committed: true,
            ..
        })
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some(),
        "a committed publish may already reference the staged object"
    );
}

#[test]
fn service_file_client_keeps_bytes_staged_when_metadata_backend_publish_is_uncertain() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"metadata backend unavailable after publish attempt","error_kind":{"type":"metadata","message":"metadata backend unavailable after publish attempt"}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());

    let err = client
        .put_artifact(
            "/artifact.bin",
            b"hello world".to_vec(),
            artifact_metadata("artifact.bin"),
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::Backend(_)
        ))
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some(),
        "a metadata backend failure does not prove the publish was rejected"
    );
}

#[test]
fn service_file_client_keeps_bytes_staged_for_ownership_errors_with_unknown_commit_phase() {
    let ownership_errors = [
        (
            r#"{"ok":false,"error":"owner epoch 7 is stale; required owner epoch is 8","error_kind":{"type":"stale_owner_epoch","owner_epoch":7,"required_epoch":8}}"#,
            "stale owner epoch",
        ),
        (
            r#"{"ok":false,"error":"owner lease expired","error_kind":{"type":"lease_expired","now_ms":101,"deadline_ms":100}}"#,
            "expired owner lease",
        ),
        (
            r#"{"ok":false,"error":"not owner","error_kind":{"type":"not_owner","shard_id":"mount-1:/","endpoint":null}}"#,
            "owner handoff",
        ),
    ];

    for (wire_error, context) in ownership_errors {
        let store = MemoryObjectStore::new();
        let addr = serve_many(vec![
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
            ),
            response_body(wire_error),
        ]);
        let client = NoKvFsClient::connect(addr, store.clone());

        let err = client
            .put_artifact(
                "/artifact.bin",
                b"hello world".to_vec(),
                artifact_metadata("artifact.bin"),
            )
            .unwrap_err();

        assert!(matches!(
            err,
            ClientError::Metadata(
                nokv_meta::MetadError::StaleOwnerEpoch { .. }
                    | nokv_meta::MetadError::LeaseExpired { .. }
                    | nokv_meta::MetadError::NotOwner { .. }
            )
        ));
        assert!(
            store
                .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
                .unwrap()
                .is_some(),
            "{context} does not prove the publish was rejected before commit"
        );
    }
}

#[test]
fn service_file_client_keeps_reader_staged_when_publish_response_is_lost() {
    let store = MemoryObjectStore::new();
    let addr = serve_request_sequence_then_drop(
        vec![Box::new(|request| {
            assert_eq!(
                request,
                MetadataRpcRequest::PrepareArtifactPath {
                    path: "/artifact.bin".to_owned(),
                    replace: false,
                }
            );
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
            )
        })],
        Box::new(|request| {
            let MetadataRpcRequest::PublishPreparedArtifact { prepared, body, .. } = request else {
                panic!("expected prepared artifact publish")
            };
            assert_eq!(prepared.generation, 7);
            assert_eq!(body.generation, 7);
        }),
    );
    let client = NoKvFsClient::connect(addr, store.clone());

    let err = client
        .put_artifact_from_reader(
            "/artifact.bin",
            std::io::Cursor::new(b"hello world".to_vec()),
            artifact_metadata("artifact.bin"),
        )
        .unwrap_err();

    assert!(matches!(err, ClientError::Io(_)));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some(),
        "a lost response leaves the publish outcome unknown"
    );
}

#[test]
fn service_file_client_keeps_reader_staged_when_metadata_backend_publish_is_uncertain() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"artifact.bin","inode":42,"generation":7,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":false,"dentry_version":null,"old_generation":null,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"metadata backend unavailable after publish attempt","error_kind":{"type":"metadata","message":"metadata backend unavailable after publish attempt"}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());

    let err = client
        .put_artifact_from_reader(
            "/artifact.bin",
            std::io::Cursor::new(b"hello world".to_vec()),
            artifact_metadata("artifact.bin"),
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::Backend(_)
        ))
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/7/0/0").unwrap())
            .unwrap()
            .is_some(),
        "a metadata backend failure does not prove the reader publish was rejected"
    );
}

#[test]
fn service_file_client_keeps_append_staged_when_publish_reports_committed() {
    let store = MemoryObjectStore::new();
    let addr = serve_request_sequence(vec![
        Box::new(|request| {
            assert_eq!(
                request,
                MetadataRpcRequest::LookupPath {
                    path: "/log.txt".to_owned(),
                }
            );
            response_body(
                r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"6c6f672e747874","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":5,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:base","size":5,"content_type":"application/octet-stream","manifest_id":"base.log","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}}"#,
            )
        }),
        Box::new(|request| {
            assert_eq!(
                request,
                MetadataRpcRequest::PrepareArtifactPath {
                    path: "/log.txt".to_owned(),
                    replace: true,
                }
            );
            response_body(
                r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"log.txt","inode":42,"generation":9,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":true,"dentry_version":7,"old_generation":7,"object_gc_claim_version":12}}}"#,
            )
        }),
        Box::new(|request| {
            let MetadataRpcRequest::PublishPreparedArtifactStagedSession {
                prepared,
                size,
                chunks,
                staged,
                ..
            } = request
            else {
                panic!("expected staged-session artifact publish")
            };
            assert_eq!(prepared.generation, 9);
            assert_eq!(prepared.old_generation, Some(7));
            assert_eq!(size, 11);
            assert!(!chunks.is_empty());
            assert!(!staged.objects.is_empty());
            response_body(
                r#"{"ok":false,"error":"metadata sync log archive failed after commit","error_kind":{"type":"sync_log_archive_failed","committed":true,"message":"injected"}}"#,
            )
        }),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());

    let err = client
        .append_artifact(
            "/log.txt",
            b" world".to_vec(),
            artifact_metadata("delta.log"),
            None,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::SyncLogArchiveFailed {
            committed: true,
            ..
        })
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/9/0/0").unwrap())
            .unwrap()
            .is_some(),
        "a committed append may already reference the staged delta"
    );
}

#[test]
fn service_file_client_keeps_append_staged_when_metadata_backend_publish_is_uncertain() {
    let store = MemoryObjectStore::new();
    let addr = serve_many(vec![
        response_body(
            r#"{"ok":true,"result":{"type":"dentry","entry":{"dentry":{"parent":1,"name_hex":"6c6f672e747874","child":42,"child_type":"file","attr_generation":7},"attr":{"inode":42,"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":5,"generation":7,"mtime_ms":7,"ctime_ms":7},"body":{"producer":"unit-test","digest_uri":"sha256:base","size":5,"content_type":"application/octet-stream","manifest_id":"base.log","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}}"#,
        ),
        response_body(
            r#"{"ok":true,"result":{"type":"prepared_artifact","prepared":{"mount":1,"parent":1,"name":"log.txt","inode":42,"generation":9,"mtime_ms":1700000000000,"ctime_ms":1700000000000,"replace":true,"dentry_version":7,"old_generation":7,"object_gc_claim_version":12}}}"#,
        ),
        response_body(
            r#"{"ok":false,"error":"metadata backend unavailable after publish attempt","error_kind":{"type":"metadata","message":"metadata backend unavailable after publish attempt"}}"#,
        ),
    ]);
    let client = NoKvFsClient::connect(addr, store.clone());

    let err = client
        .append_artifact(
            "/log.txt",
            b" world".to_vec(),
            artifact_metadata("delta.log"),
            None,
        )
        .unwrap_err();

    assert!(matches!(
        err,
        ClientError::Metadata(nokv_meta::MetadError::Metadata(
            nokv_meta::MetadataError::Backend(_)
        ))
    ));
    assert!(
        store
            .head(&ObjectKey::new("blocks/1/42/9/0/0").unwrap())
            .unwrap()
            .is_some(),
        "a metadata backend failure does not prove the append was rejected"
    );
}

#[test]
fn service_clone_subtree_path_sends_typed_rpc_and_maps_outcome() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::CloneSubtreePath {
                src_path: "/base".to_owned(),
                dst_path: "/forks/agent-1".to_owned(),
            }
        );
        response_body(r#"{"ok":true,"result":{"type":"clone_subtree","root":99,"snapshot_id":7}}"#)
    });
    let client = MetadataClient::connect(addr);
    let outcome = client
        .clone_subtree_path("/base", "/forks/agent-1")
        .unwrap();
    assert_eq!(outcome.root.get(), 99);
    assert_eq!(outcome.snapshot_id, 7);
}

#[test]
fn service_restore_probes_owner_capability_and_maps_typed_outcome() {
    let addr = serve_request_sequence(vec![
        Box::new(|request| {
            assert_eq!(
                request,
                MetadataRpcRequest::MetadataCapabilities {
                    path: "/workbenches/restored".to_owned(),
                }
            );
            response_body(
                r#"{"ok":true,"result":{"type":"metadata_capabilities","capabilities":{"mount_id":1,"restore_to_fork_v1":true}}}"#,
            )
        }),
        Box::new(|request| {
            assert_eq!(
                request,
                MetadataRpcRequest::RestoreSubtreePathToFork {
                    source_path: "/workbenches/source".to_owned(),
                    snapshot_id: 17,
                    destination_path: "/workbenches/restored".to_owned(),
                    initialization: nokv_protocol::WireRestoreInitialization {
                        remove_relative_paths: vec!["metadata/checkpoints.jsonl".to_owned()],
                        files: vec![nokv_protocol::WireRestoreInitializationFile {
                            relative_path: "metadata/restore_manifest.json".to_owned(),
                            bytes: b"manifest".to_vec(),
                            content_type: "application/json".to_owned(),
                            mode: 0o644,
                            uid: 1000,
                            gid: 1000,
                        }],
                    },
                }
            );
            let operation_id = nokv_meta::restore_operation_id(
                nokv_types::MountId::new(1).unwrap(),
                "/workbenches/source",
                17,
                "/workbenches/restored",
            )
            .unwrap();
            response_body(&format!(
                r#"{{"ok":true,"result":{{"type":"restore","outcome":{{"operation_id":"{operation_id}","state":"complete","source_root":7,"destination_root":8,"snapshot_id":17,"read_version":42,"cleanup_pending":false}}}}}}"#,
            ))
        }),
    ]);
    let client = MetadataClient::connect(addr);
    let outcome = client
        .restore_subtree_path_to_fork_initialized(
            "/workbenches/source",
            17,
            "/workbenches/restored",
            nokv_meta::RestoreInitialization {
                remove_relative_paths: vec!["metadata/checkpoints.jsonl".to_owned()],
                files: vec![nokv_meta::RestoreInitializationFile {
                    relative_path: "metadata/restore_manifest.json".to_owned(),
                    bytes: b"manifest".to_vec(),
                    content_type: "application/json".to_owned(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }],
            },
        )
        .unwrap();
    assert_eq!(
        outcome.operation_id,
        nokv_meta::restore_operation_id(
            nokv_types::MountId::new(1).unwrap(),
            "/workbenches/source",
            17,
            "/workbenches/restored",
        )
        .unwrap()
    );
    assert_eq!(outcome.state, nokv_meta::RestoreState::Complete);
    assert_eq!(outcome.source_root.get(), 7);
    assert_eq!(outcome.destination_root.get(), 8);
    assert_eq!(outcome.snapshot_id, 17);
    assert_eq!(outcome.read_version, 42);
    assert!(!outcome.cleanup_pending);
}

#[test]
fn service_restore_fails_closed_when_owner_lacks_capability() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::MetadataCapabilities {
                path: "/workbenches/restored".to_owned(),
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"metadata_capabilities","capabilities":{"mount_id":1,"restore_to_fork_v1":false}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let error = client
        .restore_subtree_path_to_fork("/workbenches/source", 17, "/workbenches/restored")
        .unwrap_err();
    assert!(matches!(error, ClientError::Protocol(ref message)
        if message.contains(nokv_protocol::RESTORE_TO_FORK_V1_CAPABILITY)
            && message.contains("/workbenches/restored")));
}

#[test]
fn service_diff_subtrees_sends_typed_rpc_and_maps_deltas() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::DiffSubtrees {
                a_path: "/base".to_owned(),
                b_path: "/forks/agent-1".to_owned(),
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"subtree_deltas","deltas":[{"path":"/a","kind":"modified","digest":"sha256:x","size_delta":12},{"path":"/c","kind":"added","digest":null,"size_delta":-5}]}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let deltas = client.diff_subtrees("/base", "/forks/agent-1").unwrap();
    assert_eq!(
        deltas,
        vec![
            nokv_meta::SubtreeDelta {
                path: "/a".to_owned(),
                kind: nokv_meta::SubtreeDeltaKind::Modified,
                digest: Some("sha256:x".to_owned()),
                size_delta: 12,
            },
            nokv_meta::SubtreeDelta {
                path: "/c".to_owned(),
                kind: nokv_meta::SubtreeDeltaKind::Added,
                digest: None,
                size_delta: -5,
            },
        ]
    );
}

#[test]
fn service_snapshot_subtree_path_sends_typed_rpc_and_maps_outcome() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::SnapshotSubtreePath {
                root_path: "/base".to_owned(),
                lease_ms: nokv_meta::DEFAULT_SNAPSHOT_LEASE_MS,
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"snapshot","snapshot":{"snapshot_id":7,"root":2,"read_version":6,"created_version":7,"lease_expires_unix_ms":0}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let outcome = client.snapshot_subtree_path("/base").unwrap();
    assert_eq!(outcome.snapshot_id, 7);
    assert_eq!(outcome.read_version, 6);
}

#[test]
fn service_snapshot_renew_returns_the_authoritative_pin() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::RenewSnapshot {
                root_path: "/base".to_owned(),
                snapshot_id: 7,
                lease_ms: 60_000,
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"renewed_snapshot","outcome":{"state":"renewed","pin":{"snapshot_id":7,"root":2,"read_version":6,"created_version":7,"lease_expires_unix_ms":61000},"extended":true}}}"#,
        )
    });
    let client = MetadataClient::connect(addr);
    let outcome = client.renew_snapshot("/base", 7, 60_000).unwrap();
    match outcome {
        nokv_meta::SnapshotRenewOutcome::Renewed { pin, extended } => {
            assert!(extended);
            assert_eq!(pin.snapshot_id, 7);
            assert_eq!(pin.lease_expires_unix_ms, 61_000);
        }
        other => panic!("unexpected renew outcome: {other:?}"),
    }
}

#[test]
fn service_rollback_subtree_path_sends_typed_rpc_and_maps_unit() {
    let addr = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::RollbackSubtreePath {
                target_path: "/base".to_owned(),
                snapshot_id: 7,
            }
        );
        response_body(r#"{"ok":true,"result":{"type":"unit"}}"#)
    });
    let client = MetadataClient::connect(addr);
    client.rollback_subtree_path("/base", 7).unwrap();
}

// --- Fleet (multi-shard) routing ---------------------------------------------

use nokv_control::{ControlStore, InMemoryControlStore, NodeId, ShardId};
use nokv_types::MountId;

/// Register a shard's identity (prefix + index) and assign it an owner whose
/// `NodeId` is its reachable `host:port`, so the control record carries the
/// `endpoint` a fleet client routes to. Returns the owner epoch (for a later
/// handoff).
fn register_owned_shard(
    store: &dyn ControlStore,
    prefix: &str,
    shard_index: u16,
    endpoint: &str,
) -> u64 {
    let shard_id = ShardId::new(format!("mount-1:{prefix}"));
    store
        .register_shard(shard_id.clone(), prefix.to_owned(), shard_index)
        .unwrap();
    let lease = store
        .acquire_unassigned(shard_id, NodeId::new(endpoint))
        .unwrap();
    store.mark_serving(&lease, None, None, 0).unwrap();
    lease.epoch
}

fn mount_one() -> MountId {
    MountId::new(1).unwrap()
}

#[test]
fn fleet_resolves_each_request_to_its_shard_owner_endpoint() {
    let store: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
    // Default shard owns "/" (index 0) at endpoint A; "/dataset" (index 1) at B.
    register_owned_shard(store.as_ref(), "/", 0, "127.0.0.1:7001");
    register_owned_shard(store.as_ref(), "/dataset", 1, "127.0.0.1:7002");
    let endpoint_a: SocketAddr = "127.0.0.1:7001".parse().unwrap();
    let endpoint_b: SocketAddr = "127.0.0.1:7002".parse().unwrap();

    let client = MetadataClient::fleet(store, mount_one()).unwrap();

    // A path under "/dataset" routes to shard 1 -> B.
    let dataset_request = MetadataRpcRequest::StatPath {
        path: "/dataset/imagenet/train".to_owned(),
    };
    assert_eq!(
        client.resolve_target_for_test(&dataset_request).unwrap(),
        Some(endpoint_b),
    );

    // A path NOT under any registered prefix falls to the default shard -> A.
    let other_request = MetadataRpcRequest::StatPath {
        path: "/other/file".to_owned(),
    };
    assert_eq!(
        client.resolve_target_for_test(&other_request).unwrap(),
        Some(endpoint_a),
    );

    // A bare-inode request routes on the shard index encoded in the inode id;
    // an inode minted by shard 1 -> B, with no path lookup.
    let inode_on_shard_one = InodeId::compose(1, 42).unwrap();
    let inode_request = MetadataRpcRequest::GetAttr {
        inode: inode_on_shard_one.get(),
    };
    assert_eq!(
        client.resolve_target_for_test(&inode_request).unwrap(),
        Some(endpoint_b),
    );
}

#[test]
fn fleet_subtree_capabilities_probe_fallback_and_every_descendant_owner() {
    let fallback = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::MetadataCapabilities {
                path: "/workbenches".to_owned(),
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"metadata_capabilities","capabilities":{"mount_id":1,"restore_to_fork_v1":true}}}"#,
        )
    });
    let nested = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::MetadataCapabilities {
                path: "/workbenches/special".to_owned(),
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"metadata_capabilities","capabilities":{"mount_id":1,"restore_to_fork_v1":false}}}"#,
        )
    });
    let deep = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::MetadataCapabilities {
                path: "/workbenches/special/deep".to_owned(),
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"metadata_capabilities","capabilities":{"mount_id":1,"restore_to_fork_v1":true}}}"#,
        )
    });
    let outside = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::MetadataCapabilities {
                path: "/workbenches-other".to_owned(),
            }
        );
        response_body(
            r#"{"ok":true,"result":{"type":"metadata_capabilities","capabilities":{"mount_id":1,"restore_to_fork_v1":true}}}"#,
        )
    });

    let store: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
    register_owned_shard(store.as_ref(), "/", 0, &fallback.to_string());
    register_owned_shard(
        store.as_ref(),
        "/workbenches/special",
        1,
        &nested.to_string(),
    );
    register_owned_shard(
        store.as_ref(),
        "/workbenches/special/deep",
        2,
        &deep.to_string(),
    );
    // These component-boundary neighbours are outside the requested subtree.
    // An accidental broad starts_with match would add a fourth capability.
    register_owned_shard(
        store.as_ref(),
        "/workbenches-other",
        3,
        &outside.to_string(),
    );

    let client = MetadataClient::fleet(store, mount_one()).unwrap();
    let capabilities = client
        .metadata_capabilities_for_subtree_owners("/workbenches")
        .unwrap();

    assert_eq!(capabilities.len(), 3);
    assert_eq!(
        capabilities
            .iter()
            .filter(|capabilities| capabilities.restore_to_fork_v1)
            .count(),
        2
    );
    assert!(capabilities
        .iter()
        .any(|capabilities| !capabilities.restore_to_fork_v1));
}

#[test]
fn fleet_keeps_recovering_owner_unroutable_until_mark_serving() {
    let store = Arc::new(InMemoryControlStore::new());
    let shard_id = ShardId::new("mount-1:/dataset");
    store
        .register_shard(shard_id.clone(), "/dataset".to_owned(), 1)
        .unwrap();
    let lease = store
        .acquire_unassigned(shard_id, NodeId::new("127.0.0.1:7002"))
        .unwrap();
    let control: Arc<dyn ControlStore> = store.clone();
    let client = MetadataClient::fleet(control, mount_one()).unwrap();
    let request = MetadataRpcRequest::StatPath {
        path: "/dataset/recovering.bin".to_owned(),
    };

    assert_eq!(client.resolve_target_for_test(&request).unwrap(), None);

    store.mark_serving(&lease, None, None, 0).unwrap();
    let RoutingMode::Fleet(router) = &client.mode else {
        panic!("expected fleet router");
    };
    router.refresh(&client).unwrap();
    assert_eq!(
        client.resolve_target_for_test(&request).unwrap(),
        Some("127.0.0.1:7002".parse().unwrap())
    );
}

#[test]
fn fleet_refreshes_and_retries_against_new_owner_on_not_owner() {
    // Two tiny one-shot servers: the stale owner replies NotOwner, the new owner
    // replies success. The client must refresh the shard map from control after
    // the NotOwner and retry against the new owner.
    let stale_owner = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::RollbackSubtreePath {
                target_path: "/dataset/run".to_owned(),
                snapshot_id: 7,
            }
        );
        response_body(
            r#"{"ok":false,"error":"not owner","error_kind":{"type":"not_owner","shard_id":"mount-1:/dataset","endpoint":null}}"#,
        )
    });
    let new_owner = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::RollbackSubtreePath {
                target_path: "/dataset/run".to_owned(),
                snapshot_id: 7,
            }
        );
        response_body(r#"{"ok":true,"result":{"type":"unit"}}"#)
    });

    let store: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
    // The dataset shard initially points at the (soon-to-be) stale owner. The
    // default shard is registered too but never contacted by this request.
    register_owned_shard(store.as_ref(), "/", 0, "127.0.0.1:7001");
    let dataset_id = ShardId::new("mount-1:/dataset");
    store
        .register_shard(dataset_id.clone(), "/dataset".to_owned(), 1)
        .unwrap();
    let lease = store
        .acquire_unassigned(dataset_id.clone(), NodeId::new(stale_owner.to_string()))
        .unwrap();
    store.mark_serving(&lease, None, None, 0).unwrap();

    // Build the client BEFORE the handoff so it caches the stale endpoint.
    let client = MetadataClient::fleet(Arc::clone(&store), mount_one()).unwrap();
    assert_eq!(
        client
            .resolve_target_for_test(&MetadataRpcRequest::StatPath {
                path: "/dataset/run".to_owned(),
            })
            .unwrap(),
        Some(stale_owner),
    );

    // Hand the shard off to the new owner in the control plane (bumps the epoch
    // and rewrites the endpoint). The client's cache is now stale.
    let successor = store
        .acquire_after_failure(dataset_id, NodeId::new(new_owner.to_string()), lease.epoch)
        .unwrap();
    store.mark_serving(&successor, None, None, 0).unwrap();

    // The call hits the stale owner (NotOwner), refreshes from control, and
    // retries against the new owner, which succeeds.
    client.rollback_subtree_path("/dataset/run", 7).unwrap();

    // After the refresh the cache reflects the new owner.
    assert_eq!(
        client
            .resolve_target_for_test(&MetadataRpcRequest::StatPath {
                path: "/dataset/run".to_owned(),
            })
            .unwrap(),
        Some(new_owner),
    );
}

/// Build a batch layout-open response from the request, minting one plan per
/// entry whose inode is `base_inode + position`. This both asserts the server saw
/// the expected single-shard set of paths (via `expected_paths`) and lets the
/// caller distinguish which shard a returned plan came from by its inode range.
fn fleet_batch_open_response(
    request: MetadataRpcRequest,
    expected_paths: &[&str],
    base_inode: u64,
) -> Vec<u8> {
    let MetadataRpcRequest::OpenPathReadPlanBatch { requests } = request else {
        panic!("expected batch open request");
    };
    let got: Vec<&str> = requests.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(got, expected_paths, "shard received the wrong path set");
    let plans = requests
        .iter()
        .enumerate()
        .map(|(position, req)| {
            let inode = base_inode + position as u64;
            let manifest = req.path.trim_start_matches('/');
            format!(
                r#"{{"metadata":{{"attr":{{"inode":{inode},"file_type":"file","mode":420,"uid":1000,"gid":1000,"rdev":0,"nlink":1,"size":1,"generation":7,"mtime_ms":7,"ctime_ms":7}},"body":{{"producer":"unit-test","digest_uri":"sha256:{inode}","size":1,"content_type":"application/octet-stream","manifest_id":"{manifest}","generation":7,"base_generation":0,"chunk_size":67108864,"block_size":4194304}}}},"lease":{{"inode":{inode},"generation":7,"read_version":9,"lease_expires_unix_ms":12345}},"plan":{{"output_len":1,"blocks":[{{"object_key":"blocks/{inode}","digest_uri":"sha256:test","object_offset":0,"object_len":1,"len":1,"output_offset":0}}]}}}}"#
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    response_body(&format!(
        r#"{{"ok":true,"result":{{"type":"open_path_read_plan_batch","plans":[{plans}]}}}}"#
    ))
}

#[test]
fn fleet_open_path_read_plan_batch_fans_out_by_shard_and_preserves_order() {
    // Two shards: default "/" (index 0) at endpoint A and "/dataset" (index 1) at
    // endpoint B. Each endpoint is a one-shot server that handles exactly one
    // (single-shard) batch RPC. Inodes 100+ come from the default shard, 200+ from
    // the dataset shard, so we can prove each plan came from the right shard.
    let default_endpoint = serve_one_request(|request| {
        fleet_batch_open_response(request, &["/runs/x.bin", "/runs/y.bin"], 100)
    });
    let dataset_endpoint = serve_one_request(|request| {
        fleet_batch_open_response(request, &["/dataset/a.bin", "/dataset/b.bin"], 200)
    });

    let store: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
    register_owned_shard(store.as_ref(), "/", 0, &default_endpoint.to_string());
    register_owned_shard(store.as_ref(), "/dataset", 1, &dataset_endpoint.to_string());
    let client = MetadataClient::fleet(store, mount_one()).unwrap();

    // The input interleaves the two shards; group-by-shard sends /dataset entries
    // to B and /runs entries to A, then re-scatters into the original order.
    let opens = client
        .open_path_read_plan_batch(&[
            PathLayoutOpenRequest::new("/dataset/a.bin", 0, 1),
            PathLayoutOpenRequest::new("/runs/x.bin", 0, 1),
            PathLayoutOpenRequest::new("/dataset/b.bin", 0, 1),
            PathLayoutOpenRequest::new("/runs/y.bin", 0, 1),
        ])
        .unwrap();

    assert_eq!(opens.len(), 4);
    // Order matches the input: dataset(200..) and runs(100..) plans are scattered
    // back into their original positions.
    assert_eq!(opens[0].metadata.attr.inode.get(), 200); // /dataset/a.bin -> shard 1
    assert_eq!(opens[1].metadata.attr.inode.get(), 100); // /runs/x.bin    -> shard 0
    assert_eq!(opens[2].metadata.attr.inode.get(), 201); // /dataset/b.bin -> shard 1
    assert_eq!(opens[3].metadata.attr.inode.get(), 101); // /runs/y.bin    -> shard 0
                                                         // The manifest id is carried through positionally, confirming each plan maps
                                                         // back to its original request after the re-scatter.
    assert_eq!(
        opens[0].metadata.body.as_ref().unwrap().manifest_id,
        "dataset/a.bin"
    );
    assert_eq!(
        opens[1].metadata.body.as_ref().unwrap().manifest_id,
        "runs/x.bin"
    );
    assert_eq!(
        opens[3].metadata.body.as_ref().unwrap().manifest_id,
        "runs/y.bin"
    );
}

#[test]
fn fleet_rename_across_shard_boundary_is_exdev_without_rpc() {
    // Two shards: "/" (index 0) and "/dataset" (index 1). Both endpoints point at
    // addresses with no listener, so an attempted RPC would surface as a transport
    // error — not `CrossShard`. The client's primary path-pair fence must trip
    // first, returning EXDEV before any connection is made.
    let store: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
    register_owned_shard(store.as_ref(), "/", 0, "127.0.0.1:1");
    register_owned_shard(store.as_ref(), "/dataset", 1, "127.0.0.1:2");
    let client = MetadataClient::fleet(store, mount_one()).unwrap();

    // "/dataset/a" routes to shard 1; "/other/b" falls to the default shard 0.
    let err = client.rename("/dataset/a", "/other/b").unwrap_err();
    assert!(
        matches!(
            err,
            ClientError::Metadata(nokv_meta::MetadError::CrossShard {
                source_shard: 1,
                dest_shard: 0
            })
        ),
        "expected CrossShard EXDEV, got {err:?}"
    );

    // The same fence covers the other path-pair ops (clone, rename_replace, diff).
    let clone_err = client
        .clone_subtree_path("/dataset/a", "/other/b")
        .unwrap_err();
    assert!(matches!(
        clone_err,
        ClientError::Metadata(nokv_meta::MetadError::CrossShard {
            source_shard: 1,
            dest_shard: 0
        })
    ));
}

#[test]
fn fleet_rename_within_one_shard_routes_normally() {
    // A rename whose source and destination both fall under "/dataset" stays in
    // shard 1: the fence is a no-op and the RPC is issued to that shard's owner.
    let owner = serve_one_request(|request| {
        assert_eq!(
            request,
            MetadataRpcRequest::RenamePath {
                source: "/dataset/a".to_owned(),
                destination: "/dataset/b".to_owned(),
            }
        );
        dentry_response(2, "b", 42, 7)
    });

    let store: Arc<dyn ControlStore> = Arc::new(InMemoryControlStore::new());
    register_owned_shard(store.as_ref(), "/", 0, "127.0.0.1:1");
    register_owned_shard(store.as_ref(), "/dataset", 1, &owner.to_string());
    let client = MetadataClient::fleet(store, mount_one()).unwrap();

    let entry = client.rename("/dataset/a", "/dataset/b").unwrap();
    assert_eq!(entry.attr.inode.get(), 42);
}
