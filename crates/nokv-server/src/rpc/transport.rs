use std::io::{self, Read, Write};
#[cfg(test)]
use std::net::SocketAddr;
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
use nokv_protocol::{decode_envelope, encode_request, MetadataRpcRequest};
use nokv_protocol::{decode_request, encode_envelope, MetadataRpcEnvelope};

use super::wire::wire_server_error;
use crate::server::{Server, ServerError};

pub(crate) const FRAMED_RPC_MAGIC: &[u8; 8] = b"NKVRPC3\n";
const FRAME_HEADER_BYTES: usize = 16;
const MAX_FRAMED_RPC_BYTES: usize = 16 * 1024 * 1024;
const MAX_FRAMED_RPC_BATCH: usize = 64;
pub(crate) const MIN_FRAMED_RPC_WORKERS: usize = 4;
pub(crate) const MAX_FRAMED_RPC_WORKERS: usize = 64;
const FRAMED_RPC_QUEUE_PER_WORKER: usize = 256;
#[cfg(test)]
const OUTBOUND_FRAMED_RPC_TIMEOUT: Duration = Duration::from_secs(5);

type RpcJob = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct RpcWorkerPool {
    sender: mpsc::SyncSender<RpcJob>,
}

pub(crate) struct RpcFrame {
    pub(crate) request_id: u64,
    pub(crate) flags: u32,
    pub(crate) payload: Vec<u8>,
}

impl RpcWorkerPool {
    pub(crate) fn new(workers: usize, queue_capacity: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<RpcJob>(queue_capacity.max(workers));
        let receiver = Arc::new(Mutex::new(receiver));
        for worker in 0..workers {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("nokv-rpc-{worker}"))
                .spawn(move || loop {
                    let job = {
                        let receiver = receiver.lock().expect("metadata rpc worker receiver");
                        receiver.recv()
                    };
                    match job {
                        Ok(job) => job(),
                        Err(_) => return,
                    }
                })
                .expect("spawn metadata rpc worker");
        }
        Self { sender }
    }

    pub(crate) fn submit(&self, job: RpcJob) -> Result<(), ServerError> {
        self.sender.send(job).map_err(|_| {
            ServerError::Io(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "metadata framed rpc worker pool stopped",
            ))
        })
    }
}

pub(crate) fn handle_framed_stream_after_magic(
    server: Arc<Server>,
    mut stream: TcpStream,
) -> Result<(), ServerError> {
    stream.set_nodelay(true).map_err(ServerError::Io)?;
    stream.set_read_timeout(None).map_err(ServerError::Io)?;
    stream.set_write_timeout(None).map_err(ServerError::Io)?;
    let writer = Arc::new(Mutex::new(stream.try_clone().map_err(ServerError::Io)?));

    loop {
        let Some(frame) = read_frame(&mut stream)? else {
            return Ok(());
        };
        let mut frames = vec![frame];
        drain_ready_frames(&mut stream, &mut frames)?;
        let task_server = Arc::clone(&server);
        let writer = Arc::clone(&writer);
        server.framed_rpc_workers().submit(Box::new(move || {
            let responses = handle_binary_rpc_frames(task_server.as_ref(), frames);
            for (request_id, flags, response) in responses {
                let response = match response {
                    Ok(response) => response,
                    Err(err) => match encode_server_error(&err) {
                        Ok(response) => response,
                        Err(err) => {
                            eprintln!("metadata framed rpc error encode failed: {err}");
                            return;
                        }
                    },
                };
                let mut writer = writer.lock().expect("framed rpc writer");
                if let Err(err) = write_frame(&mut writer, request_id, flags, &response) {
                    eprintln!("metadata framed rpc response write failed: {err}");
                    return;
                }
            }
        }))?;
    }
}

#[cfg(test)]
pub(crate) fn open_framed_rpc_stream(address: SocketAddr) -> Result<TcpStream, ServerError> {
    let mut stream = TcpStream::connect(address).map_err(ServerError::Io)?;
    stream.set_nodelay(true).map_err(ServerError::Io)?;
    stream
        .set_read_timeout(Some(OUTBOUND_FRAMED_RPC_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .set_write_timeout(Some(OUTBOUND_FRAMED_RPC_TIMEOUT))
        .map_err(ServerError::Io)?;
    stream
        .write_all(FRAMED_RPC_MAGIC)
        .map_err(ServerError::Io)?;
    Ok(stream)
}

#[cfg(test)]
pub(crate) fn call_framed_rpc_on_stream(
    stream: &mut TcpStream,
    request_id: u64,
    request: &MetadataRpcRequest,
) -> Result<MetadataRpcEnvelope, ServerError> {
    let payload = encode_request(request).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata framed rpc request encode failed: {err}"),
        ))
    })?;
    write_frame(stream, request_id, 0, &payload)?;
    let Some(frame) = read_frame(stream)? else {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "metadata framed rpc peer closed before response",
        )));
    };
    if frame.request_id != request_id {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "metadata framed rpc response id {} did not match request id {}",
                frame.request_id, request_id
            ),
        )));
    }
    decode_envelope(&frame.payload).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata framed rpc response decode failed: {err}"),
        ))
    })
}

fn drain_ready_frames(
    stream: &mut TcpStream,
    frames: &mut Vec<RpcFrame>,
) -> Result<(), ServerError> {
    stream.set_nonblocking(true).map_err(ServerError::Io)?;
    let drain = drain_ready_frames_nonblocking(stream, frames);
    let restore = stream.set_nonblocking(false).map_err(ServerError::Io);
    match (drain, restore) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(err), _) => Err(err),
        (Ok(()), Err(err)) => Err(err),
    }
}

fn drain_ready_frames_nonblocking(
    stream: &mut TcpStream,
    frames: &mut Vec<RpcFrame>,
) -> Result<(), ServerError> {
    while frames.len() < MAX_FRAMED_RPC_BATCH {
        let Some(_len) = peek_ready_frame_len(stream)? else {
            return Ok(());
        };
        let Some(frame) = read_frame(stream)? else {
            return Ok(());
        };
        frames.push(frame);
    }
    Ok(())
}

fn peek_ready_frame_len(stream: &TcpStream) -> Result<Option<usize>, ServerError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    let read = match stream.peek(&mut header) {
        Ok(read) => read,
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => return Ok(None),
        Err(err) if err.kind() == io::ErrorKind::Interrupted => return Ok(None),
        Err(err) => return Err(ServerError::Io(err)),
    };
    if read < FRAME_HEADER_BYTES {
        return Ok(None);
    }
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_FRAMED_RPC_BYTES {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata framed rpc request exceeds size limit",
        )));
    }
    let total = FRAME_HEADER_BYTES + len;
    let mut frame = vec![0_u8; total];
    match stream.peek(&mut frame) {
        Ok(read) if read >= total => Ok(Some(total)),
        Ok(_) => Ok(None),
        Err(err) if err.kind() == io::ErrorKind::WouldBlock => Ok(None),
        Err(err) if err.kind() == io::ErrorKind::Interrupted => Ok(None),
        Err(err) => Err(ServerError::Io(err)),
    }
}

fn handle_binary_rpc_frames(
    server: &Server,
    frames: Vec<RpcFrame>,
) -> Vec<(u64, u32, Result<Vec<u8>, ServerError>)> {
    if frames.len() <= 1 {
        return frames
            .into_iter()
            .map(|frame| handle_binary_rpc_frame(server, frame))
            .collect();
    }

    let mut requests = Vec::with_capacity(frames.len());
    for frame in &frames {
        match decode_request(&frame.payload) {
            Ok(request) => requests.push(request),
            Err(_) => {
                return frames
                    .into_iter()
                    .map(|frame| handle_binary_rpc_frame(server, frame))
                    .collect();
            }
        }
    }

    let results = match super::batch::try_execute_coalesced_batch(server, requests) {
        Ok(Some(results)) => results,
        Ok(None) => {
            return frames
                .into_iter()
                .map(|frame| handle_binary_rpc_frame(server, frame))
                .collect();
        }
        Err(err) => return encode_batch_error_frames(frames, &err),
    };
    if results.len() != frames.len() {
        let err = ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "coalesced metadata batch returned an unexpected result count",
        ));
        return encode_batch_error_frames(frames, &err);
    }

    frames
        .into_iter()
        .zip(results)
        .map(|(frame, envelope)| {
            (
                frame.request_id,
                frame.flags,
                encode_envelope(&envelope).map_err(|err| {
                    ServerError::Io(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("metadata binary rpc response encode failed: {err}"),
                    ))
                }),
            )
        })
        .collect()
}

fn encode_batch_error_frames(
    frames: Vec<RpcFrame>,
    err: &ServerError,
) -> Vec<(u64, u32, Result<Vec<u8>, ServerError>)> {
    match encode_server_error(err) {
        Ok(encoded) => frames
            .into_iter()
            .map(|frame| (frame.request_id, frame.flags, Ok(encoded.clone())))
            .collect(),
        Err(encode_err) => {
            let message = encode_err.to_string();
            frames
                .into_iter()
                .map(|frame| {
                    (
                        frame.request_id,
                        frame.flags,
                        Err(ServerError::Io(io::Error::new(
                            io::ErrorKind::InvalidData,
                            message.clone(),
                        ))),
                    )
                })
                .collect()
        }
    }
}

fn handle_binary_rpc_frame(
    server: &Server,
    frame: RpcFrame,
) -> (u64, u32, Result<Vec<u8>, ServerError>) {
    (
        frame.request_id,
        frame.flags,
        super::handle_binary_rpc(server, &frame.payload),
    )
}

pub(crate) fn encode_server_error(err: &ServerError) -> Result<Vec<u8>, ServerError> {
    let envelope = MetadataRpcEnvelope {
        ok: false,
        result: None,
        error: Some(err.to_string()),
        error_kind: Some(wire_server_error(err)),
    };
    encode_envelope(&envelope).map_err(|err| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("metadata binary rpc response encode failed: {err}"),
        ))
    })
}

pub(crate) fn default_framed_rpc_worker_count() -> usize {
    thread::available_parallelism()
        .map(|parallelism| parallelism.get().saturating_mul(4))
        .unwrap_or(MIN_FRAMED_RPC_WORKERS)
        .clamp(MIN_FRAMED_RPC_WORKERS, MAX_FRAMED_RPC_WORKERS)
}

pub(crate) fn default_framed_rpc_queue_capacity() -> usize {
    default_framed_rpc_worker_count().saturating_mul(FRAMED_RPC_QUEUE_PER_WORKER)
}

pub(crate) fn read_frame(stream: &mut TcpStream) -> Result<Option<RpcFrame>, ServerError> {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(err) if is_clean_framed_end(&err) => return Ok(None),
        Err(err) => return Err(ServerError::Io(err)),
    }
    let request_id = u64::from_be_bytes(header[0..8].try_into().expect("request id header"));
    let flags = u32::from_be_bytes(header[8..12].try_into().expect("flags header"));
    let len = u32::from_be_bytes(header[12..16].try_into().expect("length header")) as usize;
    if len > MAX_FRAMED_RPC_BYTES {
        return Err(ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata framed rpc request exceeds size limit",
        )));
    }
    let mut body = vec![0_u8; len];
    stream.read_exact(&mut body).map_err(ServerError::Io)?;
    Ok(Some(RpcFrame {
        request_id,
        flags,
        payload: body,
    }))
}

pub(crate) fn write_frame(
    stream: &mut TcpStream,
    request_id: u64,
    flags: u32,
    body: &[u8],
) -> Result<(), ServerError> {
    let len = u32::try_from(body.len()).map_err(|_| {
        ServerError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "metadata framed rpc response exceeds u32 length",
        ))
    })?;
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[0..8].copy_from_slice(&request_id.to_be_bytes());
    header[8..12].copy_from_slice(&flags.to_be_bytes());
    header[12..16].copy_from_slice(&len.to_be_bytes());
    stream
        .write_all(&header)
        .and_then(|_| stream.write_all(body))
        .map_err(ServerError::Io)
}

fn is_clean_framed_end(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof | io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}
