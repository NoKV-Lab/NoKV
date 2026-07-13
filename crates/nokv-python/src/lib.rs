//! Python bindings for the NoKV training data path.
//!
//! This crate is a thin binding over `nokv-client`: metadata remains behind the
//! service RPC boundary and object bytes remain behind `nokv-object`.

mod staging;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;

use nokv_client::{
    ArtifactMetadata, NoKvFsClient, PathRangeReadRequest, PathReadRange, PreparedPathRangeBatch,
};
use nokv_meta::DentryWithAttr;
use nokv_object::{
    ConfiguredObjectStore, LocalObjectStoreOptions, ObjectStoreConfig, S3ObjectStoreOptions,
    TieredObjectStoreOptions,
};
use nokv_types::{FileType, InodeAttr, PathMetadata, SnapshotPin};
use pyo3::exceptions::{PyBufferError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyByteArrayMethods, PyBytes, PyDict, PyList};
use staging::{StagingBuffer, StagingMemoryError, StagingMemoryKind};

type PythonRangeBatchInput = (String, Vec<(u64, usize)>, Option<u64>, Option<u64>);
type PythonRangeBatchEpochInput = Vec<PythonRangeBatchInput>;
const MAX_EPOCH_READ_ALL_PARALLELISM: usize = 2;

#[pyclass(name = "ReadBuffer")]
struct PythonReadBuffer {
    inner: Arc<Mutex<PythonReadBufferState>>,
}

struct PythonReadBufferState {
    buffer: StagingBuffer,
    active_exports: usize,
}

#[pyclass(name = "ReadBufferView")]
struct PythonReadBufferView {
    inner: Arc<Mutex<PythonReadBufferState>>,
    offset: usize,
    len: usize,
}

#[pyclass(name = "RangeBatchPlan", skip_from_py_object)]
#[derive(Clone)]
struct PythonRangeBatchPlan {
    inner: Arc<PreparedPathRangeBatch>,
}

#[pyclass(name = "RangeBatchReader", skip_from_py_object)]
struct PythonRangeBatchReader {
    client: Arc<NoKvFsClient<ConfiguredObjectStore>>,
    plan: Arc<PreparedPathRangeBatch>,
    buffer: Arc<Mutex<PythonReadBufferState>>,
}

#[pyclass(name = "RangeBatchEpochReader", skip_from_py_object)]
struct PythonRangeBatchEpochReader {
    readers: Vec<PythonRangeBatchReader>,
    next_index: Mutex<usize>,
    workers: EpochReadWorkerPool,
}

struct DetachedRangeBatchReader {
    client: Arc<NoKvFsClient<ConfiguredObjectStore>>,
    plan: Arc<PreparedPathRangeBatch>,
    buffer: Arc<Mutex<PythonReadBufferState>>,
}

struct EpochReadWorkerPool {
    senders: Vec<mpsc::Sender<EpochReadWorkerMessage>>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

enum EpochReadWorkerMessage {
    Read(EpochReadWorkerJob),
    Shutdown,
}

struct EpochReadWorkerJob {
    position: usize,
    epoch_index: usize,
    reader: DetachedRangeBatchReader,
    result: mpsc::Sender<EpochReadWorkerResult>,
}

struct EpochReadWorkerResult {
    position: usize,
    epoch_index: usize,
    result: Result<(), String>,
}

#[pyclass(name = "Client")]
struct PythonNoKvClient {
    inner: Arc<NoKvFsClient<ConfiguredObjectStore>>,
}

#[pymethods]
impl PythonReadBuffer {
    #[new]
    #[pyo3(signature = (capacity = 0, memory_kind = "system"))]
    fn new(capacity: usize, memory_kind: &str) -> PyResult<Self> {
        let memory_kind = parse_staging_memory_kind(memory_kind)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(PythonReadBufferState {
                buffer: StagingBuffer::with_capacity(memory_kind, capacity)
                    .map_err(staging_memory_error)?,
                active_exports: 0,
            })),
        })
    }

    fn __len__(&self) -> PyResult<usize> {
        Ok(self.state()?.buffer.len())
    }

    fn __getitem__(&self, index: isize) -> PyResult<u8> {
        let state = self.state()?;
        let len = state.buffer.len();
        let index = if index < 0 {
            len.checked_sub(index.unsigned_abs())
        } else {
            usize::try_from(index).ok()
        }
        .ok_or_else(|| PyValueError::new_err("read buffer index is out of range"))?;
        state
            .buffer
            .get_byte(index)
            .ok_or_else(|| PyValueError::new_err("read buffer index is out of range"))
    }

    fn capacity(&self) -> PyResult<usize> {
        Ok(self.state()?.buffer.capacity())
    }

    fn memory_kind(&self) -> PyResult<&'static str> {
        Ok(self.state()?.buffer.kind().as_str())
    }

    fn reserve(&self, capacity: usize) -> PyResult<()> {
        let mut state = self.state_mut()?;
        ensure_read_buffer_not_exported(&state)?;
        state
            .buffer
            .reserve(capacity)
            .map_err(staging_memory_error)?;
        Ok(())
    }

    fn clear(&self) -> PyResult<()> {
        let mut state = self.state_mut()?;
        ensure_read_buffer_not_exported(&state)?;
        state.buffer.clear();
        Ok(())
    }

    fn export_count(&self) -> PyResult<usize> {
        Ok(self.state()?.active_exports)
    }

    #[pyo3(signature = (offset = 0, length = None))]
    fn export(&self, offset: usize, length: Option<usize>) -> PyResult<PythonReadBufferView> {
        let mut state = self.state_mut()?;
        let available =
            state.buffer.len().checked_sub(offset).ok_or_else(|| {
                PyValueError::new_err("read buffer export offset is out of bounds")
            })?;
        let len = length.unwrap_or(available);
        let end = offset
            .checked_add(len)
            .ok_or_else(|| PyValueError::new_err("read buffer export end exceeds usize"))?;
        if end > state.buffer.len() {
            return Err(PyValueError::new_err(
                "read buffer export slice is out of bounds",
            ));
        }
        state.active_exports = state.active_exports.saturating_add(1);
        Ok(PythonReadBufferView {
            inner: Arc::clone(&self.inner),
            offset,
            len,
        })
    }

    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, self.state()?.buffer.as_slice()))
    }

    fn slice<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        len: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let state = self.state()?;
        let end = offset
            .checked_add(len)
            .ok_or_else(|| PyValueError::new_err("read buffer slice end exceeds usize"))?;
        let bytes = state
            .buffer
            .get(offset..end)
            .ok_or_else(|| PyValueError::new_err("read buffer slice is out of bounds"))?;
        Ok(PyBytes::new(py, bytes))
    }
}

impl PythonReadBuffer {
    fn state(&self) -> PyResult<std::sync::MutexGuard<'_, PythonReadBufferState>> {
        self.inner
            .lock()
            .map_err(|err| PyRuntimeError::new_err(format!("read buffer lock poisoned: {err}")))
    }

    fn state_mut(&self) -> PyResult<std::sync::MutexGuard<'_, PythonReadBufferState>> {
        self.state()
    }
}

fn ensure_read_buffer_not_exported(state: &PythonReadBufferState) -> PyResult<()> {
    if state.active_exports == 0 {
        return Ok(());
    }
    Err(PyBufferError::new_err(
        "ReadBuffer has active exported views; release them before mutating",
    ))
}

#[pymethods]
impl PythonReadBufferView {
    fn __len__(&self) -> usize {
        self.len
    }

    fn __getitem__(&self, index: isize) -> PyResult<u8> {
        let index = if index < 0 {
            self.len.checked_sub(index.unsigned_abs())
        } else {
            usize::try_from(index).ok()
        }
        .ok_or_else(|| PyValueError::new_err("read buffer view index is out of range"))?;
        if index >= self.len {
            return Err(PyValueError::new_err(
                "read buffer view index is out of range",
            ));
        }
        let state = self.state()?;
        let absolute_index = self
            .offset
            .checked_add(index)
            .ok_or_else(|| PyValueError::new_err("read buffer view index exceeds usize"))?;
        state
            .buffer
            .get_byte(absolute_index)
            .ok_or_else(|| PyValueError::new_err("read buffer view index is out of range"))
    }

    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let state = self.state()?;
        let end = self
            .offset
            .checked_add(self.len)
            .ok_or_else(|| PyValueError::new_err("read buffer view end exceeds usize"))?;
        let bytes = state
            .buffer
            .get(self.offset..end)
            .ok_or_else(|| PyValueError::new_err("read buffer view is out of bounds"))?;
        Ok(PyBytes::new(py, bytes))
    }

    fn slice<'py>(
        &self,
        py: Python<'py>,
        offset: usize,
        len: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let view_end = offset
            .checked_add(len)
            .ok_or_else(|| PyValueError::new_err("read buffer view slice end exceeds usize"))?;
        if view_end > self.len {
            return Err(PyValueError::new_err(
                "read buffer view slice is out of bounds",
            ));
        }
        let state = self.state()?;
        let start = self
            .offset
            .checked_add(offset)
            .ok_or_else(|| PyValueError::new_err("read buffer view slice start exceeds usize"))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| PyValueError::new_err("read buffer view slice end exceeds usize"))?;
        let bytes = state
            .buffer
            .get(start..end)
            .ok_or_else(|| PyValueError::new_err("read buffer view slice is out of bounds"))?;
        Ok(PyBytes::new(py, bytes))
    }
}

impl PythonReadBufferView {
    fn state(&self) -> PyResult<std::sync::MutexGuard<'_, PythonReadBufferState>> {
        self.inner
            .lock()
            .map_err(|err| PyRuntimeError::new_err(format!("read buffer lock poisoned: {err}")))
    }
}

impl Drop for PythonReadBufferView {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.lock() {
            state.active_exports = state.active_exports.saturating_sub(1);
        }
    }
}

#[pymethods]
impl PythonRangeBatchPlan {
    fn __len__(&self) -> usize {
        self.inner.request_count()
    }

    fn request_count(&self) -> usize {
        self.inner.request_count()
    }

    fn range_count(&self) -> usize {
        self.inner.range_count()
    }

    fn output_len(&self) -> usize {
        self.inner.output_len()
    }

    fn layout(&self) -> Vec<(usize, usize)> {
        self.inner.request_layout()
    }
}

#[pymethods]
impl PythonRangeBatchReader {
    fn __len__(&self) -> usize {
        self.plan.request_count()
    }

    fn request_count(&self) -> usize {
        self.plan.request_count()
    }

    fn range_count(&self) -> usize {
        self.plan.range_count()
    }

    fn output_len(&self) -> usize {
        self.plan.output_len()
    }

    fn layout(&self) -> Vec<(usize, usize)> {
        self.plan.request_layout()
    }

    fn memory_kind(&self) -> PyResult<&'static str> {
        Ok(self.state()?.buffer.kind().as_str())
    }

    fn buffer(&self) -> PythonReadBuffer {
        PythonReadBuffer {
            inner: Arc::clone(&self.buffer),
        }
    }

    fn read(&self, py: Python<'_>) -> PyResult<()> {
        let reader = self.detached();
        py.detach(move || reader.read())
            .map_err(PyRuntimeError::new_err)
    }
}

impl PythonRangeBatchReader {
    fn detached(&self) -> DetachedRangeBatchReader {
        DetachedRangeBatchReader {
            client: Arc::clone(&self.client),
            plan: Arc::clone(&self.plan),
            buffer: Arc::clone(&self.buffer),
        }
    }

    fn state(&self) -> PyResult<std::sync::MutexGuard<'_, PythonReadBufferState>> {
        self.buffer.lock().map_err(|err| {
            PyRuntimeError::new_err(format!("range batch reader lock poisoned: {err}"))
        })
    }
}

#[pymethods]
impl PythonRangeBatchEpochReader {
    fn __len__(&self) -> usize {
        self.readers.len()
    }

    fn batch_count(&self) -> usize {
        self.readers.len()
    }

    fn worker_count(&self) -> usize {
        self.workers.worker_count()
    }

    fn reset(&self) -> PyResult<()> {
        *self.next_index()? = 0;
        Ok(())
    }

    fn read_next(&self, py: Python<'_>) -> PyResult<usize> {
        if self.readers.is_empty() {
            return Err(PyValueError::new_err(
                "RangeBatchEpochReader requires at least one batch",
            ));
        }
        let index = *self.next_index()?;
        let reader = self.readers.get(index).ok_or_else(|| {
            PyRuntimeError::new_err("range batch epoch reader index is out of bounds")
        })?;
        reader.read(py)?;
        let mut next = self.next_index()?;
        *next = (index + 1) % self.readers.len();
        Ok(index)
    }

    fn read_all(&self, py: Python<'_>) -> PyResult<Vec<usize>> {
        if self.readers.is_empty() {
            return Err(PyValueError::new_err(
                "RangeBatchEpochReader requires at least one batch",
            ));
        }
        let start = *self.next_index()?;
        let mut order = Vec::with_capacity(self.readers.len());
        let mut readers = Vec::with_capacity(self.readers.len());
        for step in 0..self.readers.len() {
            let index = (start + step) % self.readers.len();
            let reader = self.readers.get(index).ok_or_else(|| {
                PyRuntimeError::new_err("range batch epoch reader index is out of bounds")
            })?;
            let reader = reader.detached();
            reader
                .ensure_not_exported()
                .map_err(PyRuntimeError::new_err)?;
            order.push(index);
            readers.push((step, index, reader));
        }
        let senders = self.workers.senders();
        py.detach(move || read_epoch_readers(readers, senders))
            .map_err(PyRuntimeError::new_err)?;
        Ok(order)
    }

    fn buffer(&self, index: usize) -> PyResult<PythonReadBuffer> {
        let reader = self.reader(index)?;
        Ok(PythonReadBuffer {
            inner: Arc::clone(&reader.buffer),
        })
    }

    fn layout(&self, index: usize) -> PyResult<Vec<(usize, usize)>> {
        Ok(self.reader(index)?.plan.request_layout())
    }

    fn output_len(&self, index: usize) -> PyResult<usize> {
        Ok(self.reader(index)?.plan.output_len())
    }

    fn memory_kind(&self, index: usize) -> PyResult<&'static str> {
        Ok(self.reader(index)?.state()?.buffer.kind().as_str())
    }
}

impl PythonRangeBatchEpochReader {
    fn next_index(&self) -> PyResult<std::sync::MutexGuard<'_, usize>> {
        self.next_index.lock().map_err(|err| {
            PyRuntimeError::new_err(format!("range batch epoch reader lock poisoned: {err}"))
        })
    }

    fn reader(&self, index: usize) -> PyResult<&PythonRangeBatchReader> {
        self.readers
            .get(index)
            .ok_or_else(|| PyValueError::new_err("range batch epoch reader index is out of bounds"))
    }
}

impl DetachedRangeBatchReader {
    fn ensure_not_exported(&self) -> Result<(), String> {
        let state = self
            .buffer
            .lock()
            .map_err(|err| format!("read buffer lock poisoned: {err}"))?;
        if state.active_exports == 0 {
            return Ok(());
        }
        Err("ReadBuffer has active exported views; release them before mutating".to_owned())
    }

    fn read(self) -> Result<(), String> {
        let mut state = self
            .buffer
            .lock()
            .map_err(|err| format!("read buffer lock poisoned: {err}"))?;
        if state.active_exports != 0 {
            return Err(
                "ReadBuffer has active exported views; release them before mutating".to_owned(),
            );
        }
        state
            .buffer
            .resize(self.plan.output_len(), 0)
            .map_err(|err| err.to_string())?;
        self.client
            .read_prepared_path_ranges_batch_into(self.plan.as_ref(), state.buffer.as_mut_slice())
            .map_err(|err| err.to_string())?;
        Ok(())
    }
}

fn read_epoch_readers(
    readers: Vec<(usize, usize, DetachedRangeBatchReader)>,
    senders: Vec<mpsc::Sender<EpochReadWorkerMessage>>,
) -> Result<(), String> {
    if readers.len() <= 1 || senders.is_empty() {
        for (_, epoch_index, reader) in readers {
            reader
                .read()
                .map_err(|err| format!("range batch epoch reader {epoch_index} failed: {err}"))?;
        }
        return Ok(());
    }

    let submitted = readers.len();
    let (result_sender, result_receiver) = mpsc::channel();
    for (position, epoch_index, reader) in readers {
        let sender_index = position % senders.len();
        let job = EpochReadWorkerJob {
            position,
            epoch_index,
            reader,
            result: result_sender.clone(),
        };
        senders[sender_index]
            .send(EpochReadWorkerMessage::Read(job))
            .map_err(|err| {
                format!("range batch epoch worker {sender_index} is unavailable: {err}")
            })?;
    }
    drop(result_sender);

    let mut errors = (0..submitted).map(|_| None).collect::<Vec<_>>();
    for _ in 0..submitted {
        let result = result_receiver
            .recv()
            .map_err(|err| format!("range batch epoch worker result channel failed: {err}"))?;
        if let Err(err) = result.result {
            errors[result.position] = Some(format!(
                "range batch epoch reader {} failed: {err}",
                result.epoch_index
            ));
        }
    }

    match errors.into_iter().flatten().next() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

impl EpochReadWorkerPool {
    fn new(worker_count: usize) -> Result<Self, String> {
        let worker_count = worker_count.max(1);
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let (sender, receiver) = mpsc::channel();
            let handle = std::thread::Builder::new()
                .name(format!("nokv-python-epoch-reader-{worker_index}"))
                .spawn(move || epoch_read_worker_loop(receiver))
                .map_err(|err| {
                    format!("failed to spawn range batch epoch worker {worker_index}: {err}")
                })?;
            senders.push(sender);
            handles.push(handle);
        }
        Ok(Self {
            senders,
            handles: Mutex::new(handles),
        })
    }

    fn worker_count(&self) -> usize {
        self.senders.len()
    }

    fn senders(&self) -> Vec<mpsc::Sender<EpochReadWorkerMessage>> {
        self.senders.clone()
    }
}

impl Drop for EpochReadWorkerPool {
    fn drop(&mut self) {
        for sender in &self.senders {
            let _ = sender.send(EpochReadWorkerMessage::Shutdown);
        }
        if let Ok(mut handles) = self.handles.lock() {
            while let Some(handle) = handles.pop() {
                let _ = handle.join();
            }
        }
    }
}

fn epoch_read_worker_loop(receiver: mpsc::Receiver<EpochReadWorkerMessage>) {
    while let Ok(message) = receiver.recv() {
        match message {
            EpochReadWorkerMessage::Read(job) => {
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.reader.read()))
                        .unwrap_or_else(|_| Err("worker panicked".to_owned()));
                let _ = job.result.send(EpochReadWorkerResult {
                    position: job.position,
                    epoch_index: job.epoch_index,
                    result,
                });
            }
            EpochReadWorkerMessage::Shutdown => break,
        }
    }
}

#[pymethods]
impl PythonNoKvClient {
    #[new]
    #[pyo3(signature = (
        metadata_addr,
        bucket,
        endpoint = None,
        access_key_id = None,
        secret_access_key = None,
        region = None,
        root = None,
        session_token = None,
        virtual_host_style = false,
        skip_signature = false,
        hot_object_root = None,
        block_cache = true
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        metadata_addr: &str,
        bucket: &str,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        region: Option<String>,
        root: Option<String>,
        session_token: Option<String>,
        virtual_host_style: bool,
        skip_signature: bool,
        hot_object_root: Option<String>,
        block_cache: bool,
    ) -> PyResult<Self> {
        let address = parse_metadata_addr(metadata_addr)?;
        let object_config = object_config(
            bucket,
            endpoint,
            access_key_id,
            secret_access_key,
            region,
            root,
            session_token,
            virtual_host_style,
            skip_signature,
            hot_object_root,
        );
        let objects = object_config.open().map_err(runtime_error)?;
        let mut inner = NoKvFsClient::connect(address, objects);
        inner.set_block_cache_enabled(block_cache);
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    fn read_ranges_batch<'py>(
        &self,
        py: Python<'py>,
        requests: Vec<PythonRangeBatchInput>,
    ) -> PyResult<Vec<Vec<Bound<'py, PyBytes>>>> {
        let requests = python_range_batch_requests(requests);
        let reads = py
            .detach(|| self.inner.read_path_ranges_batch(&requests))
            .map_err(runtime_error)?;
        Ok(reads
            .into_iter()
            .map(|request_reads| {
                request_reads
                    .into_iter()
                    .map(|bytes| PyBytes::new(py, &bytes))
                    .collect::<Vec<_>>()
            })
            .collect())
    }

    fn read_ranges_batch_packed<'py>(
        &self,
        py: Python<'py>,
        requests: Vec<PythonRangeBatchInput>,
    ) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        let requests = python_range_batch_requests(requests);
        let reads = py
            .detach(|| self.inner.read_path_ranges_batch_packed(&requests))
            .map_err(runtime_error)?;
        Ok(reads
            .into_iter()
            .map(|packed| PyBytes::new(py, &packed))
            .collect())
    }

    fn read_ranges_batch_into(
        &self,
        requests: Vec<PythonRangeBatchInput>,
        output: &Bound<'_, PyByteArray>,
    ) -> PyResult<Vec<(usize, usize)>> {
        let requests = python_range_batch_requests(requests);
        let (offsets, _) = packed_request_layout(&requests)?;
        // The bytearray pointer is stable while the GIL is held; this method
        // keeps the GIL until the SDK write is complete.
        let output = unsafe { output.as_bytes_mut() };
        let lengths = self
            .inner
            .read_path_ranges_batch_into(&requests, output, &offsets)
            .map_err(runtime_error)?;
        Ok(offsets.into_iter().zip(lengths).collect())
    }

    fn prepare_range_batch(
        &self,
        requests: Vec<PythonRangeBatchInput>,
    ) -> PyResult<PythonRangeBatchPlan> {
        let requests = python_range_batch_requests(requests);
        range_batch_plan(requests)
    }

    #[pyo3(signature = (requests, memory_kind = "system"))]
    fn prepare_range_batch_reader(
        &self,
        requests: Vec<PythonRangeBatchInput>,
        memory_kind: &str,
    ) -> PyResult<PythonRangeBatchReader> {
        let requests = python_range_batch_requests(requests);
        range_batch_reader(Arc::clone(&self.inner), requests, memory_kind)
    }

    #[pyo3(signature = (request_batches, memory_kind = "system"))]
    fn prepare_range_batch_epoch(
        &self,
        request_batches: Vec<PythonRangeBatchEpochInput>,
        memory_kind: &str,
    ) -> PyResult<PythonRangeBatchEpochReader> {
        if request_batches.is_empty() {
            return Err(PyValueError::new_err(
                "RangeBatchEpochReader requires at least one batch",
            ));
        }
        let mut readers = Vec::with_capacity(request_batches.len());
        for requests in request_batches {
            let requests = python_range_batch_requests(requests);
            readers.push(range_batch_reader(
                Arc::clone(&self.inner),
                requests,
                memory_kind,
            )?);
        }
        let worker_count = readers.len().min(MAX_EPOCH_READ_ALL_PARALLELISM);
        let workers = EpochReadWorkerPool::new(worker_count).map_err(PyRuntimeError::new_err)?;
        Ok(PythonRangeBatchEpochReader {
            readers,
            next_index: Mutex::new(0),
            workers,
        })
    }

    fn read_ranges_batch_buffer(
        &self,
        py: Python<'_>,
        requests: Vec<PythonRangeBatchInput>,
        buffer: &Bound<'_, PythonReadBuffer>,
    ) -> PyResult<Vec<(usize, usize)>> {
        let requests = python_range_batch_requests(requests);
        let (offsets, output_len) = packed_request_layout(&requests)?;
        let buffer = Arc::clone(&buffer.borrow().inner);
        let lengths = py
            .detach(|| {
                let mut state = buffer
                    .lock()
                    .map_err(|err| format!("read buffer lock poisoned: {err}"))?;
                if state.active_exports != 0 {
                    return Err(
                        "ReadBuffer has active exported views; release them before mutating"
                            .to_owned(),
                    );
                }
                state
                    .buffer
                    .resize(output_len, 0)
                    .map_err(|err| err.to_string())?;
                self.inner
                    .read_path_ranges_batch_into(&requests, state.buffer.as_mut_slice(), &offsets)
                    .map_err(|err| err.to_string())
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(offsets.into_iter().zip(lengths).collect())
    }

    fn read_range_batch_plan_buffer(
        &self,
        py: Python<'_>,
        plan: &Bound<'_, PythonRangeBatchPlan>,
        buffer: &Bound<'_, PythonReadBuffer>,
    ) -> PyResult<Vec<(usize, usize)>> {
        let plan = Arc::clone(&plan.borrow().inner);
        let buffer = Arc::clone(&buffer.borrow().inner);
        let lengths = py
            .detach(|| {
                let mut state = buffer
                    .lock()
                    .map_err(|err| format!("read buffer lock poisoned: {err}"))?;
                if state.active_exports != 0 {
                    return Err(
                        "ReadBuffer has active exported views; release them before mutating"
                            .to_owned(),
                    );
                }
                state
                    .buffer
                    .resize(plan.output_len(), 0)
                    .map_err(|err| err.to_string())?;
                self.inner
                    .read_prepared_path_ranges_batch_into(
                        plan.as_ref(),
                        state.buffer.as_mut_slice(),
                    )
                    .map_err(|err| err.to_string())
            })
            .map_err(PyRuntimeError::new_err)?;
        Ok(plan
            .request_layout()
            .into_iter()
            .map(|(offset, _)| offset)
            .zip(lengths)
            .collect())
    }

    fn stats<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let object = self.inner.object_stats();
        let data_fabric = self.inner.data_fabric_stats().map_err(runtime_error)?;
        let stats = PyDict::new(py);
        stats.set_item("object_puts", object.object_puts)?;
        stats.set_item("object_put_bytes", object.object_put_bytes)?;
        stats.set_item("object_gets", object.object_gets)?;
        stats.set_item("object_get_bytes", object.object_get_bytes)?;
        stats.set_item("coalesced_gets", object.coalesced_gets)?;
        stats.set_item("coalesced_get_bytes", object.coalesced_get_bytes)?;
        stats.set_item("cache_hits", object.cache_hits)?;
        stats.set_item("cache_hit_bytes", object.cache_hit_bytes)?;
        stats.set_item("prefetch_enqueued", object.prefetch_enqueued)?;
        stats.set_item("prefetch_dropped", object.prefetch_dropped)?;
        stats.set_item("prefetch_completed", object.prefetch_completed)?;
        stats.set_item("prefetch_failed", object.prefetch_failed)?;
        stats.set_item("prefetch_object_gets", object.prefetch_object_gets)?;
        stats.set_item(
            "prefetch_object_get_bytes",
            object.prefetch_object_get_bytes,
        )?;
        stats.set_item("prefetch_cache_hits", object.prefetch_cache_hits)?;
        stats.set_item("prefetch_cache_hit_bytes", object.prefetch_cache_hit_bytes)?;
        stats.set_item("read_plan_cache_hits", object.read_plan_cache_hits)?;
        stats.set_item("read_plan_cache_misses", object.read_plan_cache_misses)?;
        stats.set_item("manifest_chunks", object.manifest_chunks)?;
        stats.set_item("manifest_blocks", object.manifest_blocks)?;
        stats.set_item("data_fabric_planned_blocks", data_fabric.planned_blocks)?;
        stats.set_item("data_fabric_local_nvme_hits", data_fabric.local_nvme_hits)?;
        stats.set_item("data_fabric_object_fallbacks", data_fabric.object_fallbacks)?;
        stats.set_item("data_fabric_object_gets", data_fabric.object_gets)?;
        stats.set_item("data_fabric_object_get_bytes", data_fabric.object_get_bytes)?;
        stats.set_item("data_fabric_coalesced_ranges", data_fabric.coalesced_ranges)?;
        stats.set_item(
            "data_fabric_coalesced_range_bytes",
            data_fabric.coalesced_range_bytes,
        )?;
        stats.set_item("data_fabric_cache_hits", data_fabric.cache_hits)?;
        stats.set_item("data_fabric_cache_hit_bytes", data_fabric.cache_hit_bytes)?;
        Ok(stats)
    }

    // ---- write path -------------------------------------------------------

    /// Publish an immutable artifact (the AI checkpoint/shard write path). The
    /// bytes are staged into object storage and the new generation is published
    /// atomically through the metadata prepare/publish protocol; on metadata
    /// failure the staged objects are rolled back. With `replace=True` an
    /// existing file at `path` is atomically superseded by the new generation.
    #[pyo3(signature = (
        path,
        data,
        producer = "python-sdk",
        digest_uri = "",
        content_type = "application/octet-stream",
        manifest_id = "",
        mode = 0o644,
        uid = 0,
        gid = 0,
        replace = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn put_artifact<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        data: Vec<u8>,
        producer: &str,
        digest_uri: &str,
        content_type: &str,
        manifest_id: &str,
        mode: u32,
        uid: u32,
        gid: u32,
        replace: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        // The manifest id is the artifact's chunk-object key prefix and must be a
        // non-empty, relative object key. Default it from the path (matching the
        // `nokv` CLI: `artifacts/<path>`) so `put_artifact(path, data)` just works.
        let manifest_id = if manifest_id.is_empty() {
            let trimmed = path.trim_start_matches('/');
            if trimmed.is_empty() {
                return Err(PyValueError::new_err(
                    "artifact path must name a file, not the root",
                ));
            }
            format!("artifacts/{trimmed}")
        } else {
            manifest_id.to_owned()
        };
        let metadata = ArtifactMetadata {
            producer: producer.to_owned(),
            digest_uri: digest_uri.to_owned(),
            content_type: content_type.to_owned(),
            manifest_id,
            mode,
            uid,
            gid,
        };
        let entry = py
            .detach(move || {
                if replace {
                    // create-or-replace: supersede an existing generation, or
                    // create when absent (the natural overwrite semantic for
                    // fsspec `wb` and idempotent checkpoint-shard re-publish).
                    if self.inner.metadata().lookup(path)?.is_some() {
                        self.inner
                            .put_artifact_replace(path, data, metadata)
                            .map(|result| result.entry)
                    } else {
                        self.inner.put_artifact(path, data, metadata)
                    }
                } else {
                    self.inner.put_artifact(path, data, metadata)
                }
            })
            .map_err(runtime_error)?;
        dentry_to_py(py, &entry)
    }

    #[pyo3(signature = (path, mode = 0o755, uid = 0, gid = 0))]
    fn mkdir<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> PyResult<Bound<'py, PyDict>> {
        let entry = py
            .detach(|| self.inner.metadata().mkdir(path, mode, uid, gid))
            .map_err(runtime_error)?;
        dentry_to_py(py, &entry)
    }

    #[pyo3(signature = (path, mode = 0o644, uid = 0, gid = 0))]
    fn create_file<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> PyResult<Bound<'py, PyDict>> {
        let entry = py
            .detach(|| self.inner.metadata().create_file(path, mode, uid, gid))
            .map_err(runtime_error)?;
        dentry_to_py(py, &entry)
    }

    // ---- namespace --------------------------------------------------------

    fn lookup<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let entry = py
            .detach(|| self.inner.metadata().lookup(path))
            .map_err(runtime_error)?;
        entry.map(|entry| dentry_to_py(py, &entry)).transpose()
    }

    fn stat<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Option<Bound<'py, PyDict>>> {
        let metadata = py
            .detach(|| self.inner.metadata().stat_path(path))
            .map_err(runtime_error)?;
        metadata
            .map(|metadata| path_metadata_to_py(py, &metadata))
            .transpose()
    }

    fn exists(&self, py: Python<'_>, path: &str) -> PyResult<bool> {
        let metadata = py
            .detach(|| self.inner.metadata().stat_path(path))
            .map_err(runtime_error)?;
        Ok(metadata.is_some())
    }

    /// Page through a directory and return every entry as a dict (name + attrs).
    #[pyo3(signature = (path, page_limit = 1024))]
    fn list_dir<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        page_limit: usize,
    ) -> PyResult<Bound<'py, PyList>> {
        let limit = page_limit.max(1);
        let entries = py
            .detach(
                || -> Result<Vec<DentryWithAttr>, nokv_client::ClientError> {
                    let mut all = Vec::new();
                    let mut cursor: Option<nokv_types::DentryName> = None;
                    loop {
                        let page = self
                            .inner
                            .metadata()
                            .list_page(path, cursor.as_ref(), limit)?;
                        all.extend(page.entries);
                        match page.next_cursor {
                            Some(next) => cursor = Some(next),
                            None => break,
                        }
                    }
                    Ok(all)
                },
            )
            .map_err(runtime_error)?;
        let list = PyList::empty(py);
        for entry in &entries {
            list.append(dentry_to_py(py, entry)?)?;
        }
        Ok(list)
    }

    fn remove_file<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let entry = py
            .detach(|| self.inner.metadata().remove(path))
            .map_err(runtime_error)?;
        dentry_to_py(py, &entry)
    }

    fn rmdir<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let entry = py
            .detach(|| self.inner.metadata().rmdir(path))
            .map_err(runtime_error)?;
        dentry_to_py(py, &entry)
    }

    /// Rename within a shard. Cross-shard renames return `EXDEV` (no 2PC). With
    /// `replace=True` an existing destination is atomically superseded.
    #[pyo3(signature = (source, destination, replace = false))]
    fn rename<'py>(
        &self,
        py: Python<'py>,
        source: &str,
        destination: &str,
        replace: bool,
    ) -> PyResult<Bound<'py, PyDict>> {
        let entry = py
            .detach(|| {
                if replace {
                    self.inner
                        .metadata()
                        .rename_replace(source, destination)
                        .map(|result| result.entry)
                } else {
                    self.inner.metadata().rename(source, destination)
                }
            })
            .map_err(runtime_error)?;
        dentry_to_py(py, &entry)
    }

    // ---- snapshots (reproducible datasets / checkpoint pins) ---------------

    fn snapshot<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyDict>> {
        let pin = py
            .detach(|| self.inner.metadata().snapshot(path))
            .map_err(runtime_error)?;
        snapshot_pin_to_py(py, &pin)
    }

    fn snapshot_pin<'py>(
        &self,
        py: Python<'py>,
        root_path: &str,
        snapshot_id: u64,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let pin = py
            .detach(|| self.inner.metadata().snapshot_pin(root_path, snapshot_id))
            .map_err(runtime_error)?;
        pin.map(|pin| snapshot_pin_to_py(py, &pin)).transpose()
    }

    fn retire_snapshot(&self, py: Python<'_>, root_path: &str, snapshot_id: u64) -> PyResult<bool> {
        py.detach(|| {
            self.inner
                .metadata()
                .retire_snapshot(root_path, snapshot_id)
        })
        .map_err(runtime_error)
    }

    fn renew_snapshot<'py>(
        &self,
        py: Python<'py>,
        root_path: &str,
        snapshot_id: u64,
        lease_ms: u64,
    ) -> PyResult<Bound<'py, PyDict>> {
        let outcome = py
            .detach(|| {
                self.inner
                    .metadata()
                    .renew_snapshot(root_path, snapshot_id, lease_ms)
            })
            .map_err(runtime_error)?;
        let result = PyDict::new(py);
        match outcome {
            nokv_meta::SnapshotRenewOutcome::Renewed { pin, extended } => {
                result.set_item("state", "renewed")?;
                result.set_item("extended", extended)?;
                result.set_item("pin", snapshot_pin_to_py(py, &pin)?)?;
            }
            nokv_meta::SnapshotRenewOutcome::Missing { snapshot_id } => {
                result.set_item("state", "missing")?;
                result.set_item("snapshot_id", snapshot_id)?;
            }
        }
        Ok(result)
    }

    /// Read a whole file. With `snapshot_id` set, read it as of that subtree
    /// snapshot (reproducible reads against a pinned dataset version) — resolved
    /// through the snapshot's namespace via the path-based snapshot read.
    #[pyo3(signature = (path, snapshot_id = None, snapshot_root_path = None))]
    fn cat<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        snapshot_id: Option<u64>,
        snapshot_root_path: Option<&str>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py
            .detach(|| -> Result<Vec<u8>, nokv_client::ClientError> {
                match snapshot_id {
                    Some(id) => {
                        let root_path = snapshot_root_path.ok_or_else(|| {
                            nokv_client::ClientError::Protocol(
                                "snapshot_root_path is required with snapshot_id".to_owned(),
                            )
                        })?;
                        let metadata = self
                            .inner
                            .metadata()
                            .stat_path_at_snapshot(root_path, id, path)?
                            .ok_or_else(|| nokv_client::ClientError::NotFound(path.to_owned()))?;
                        let size = usize::try_from(metadata.attr.size).map_err(|_| {
                            nokv_client::ClientError::Protocol(
                                "artifact size exceeds usize".to_owned(),
                            )
                        })?;
                        self.inner.read_snapshot(root_path, id, path, 0, size)
                    }
                    None => self.inner.cat(path),
                }
            })
            .map_err(runtime_error)?;
        Ok(PyBytes::new(py, &bytes))
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PythonNoKvClient>()?;
    m.add_class::<PythonReadBuffer>()?;
    m.add_class::<PythonReadBufferView>()?;
    m.add_class::<PythonRangeBatchPlan>()?;
    m.add_class::<PythonRangeBatchReader>()?;
    m.add_class::<PythonRangeBatchEpochReader>()?;
    Ok(())
}

fn python_range_batch_requests(requests: Vec<PythonRangeBatchInput>) -> Vec<PathRangeReadRequest> {
    requests
        .into_iter()
        .map(|(path, ranges, expected_generation, max_gap_bytes)| {
            let ranges = ranges
                .into_iter()
                .map(|(offset, len)| PathReadRange::new(offset, len))
                .collect::<Vec<_>>();
            let mut request = PathRangeReadRequest::new(path, ranges)
                .with_max_gap_bytes(max_gap_bytes.unwrap_or(0));
            if let Some(generation) = expected_generation {
                request = request.with_expected_generation(generation);
            }
            request
        })
        .collect()
}

fn packed_request_layout(requests: &[PathRangeReadRequest]) -> PyResult<(Vec<usize>, usize)> {
    let mut offsets = Vec::with_capacity(requests.len());
    let mut cursor = 0_usize;
    for request in requests {
        offsets.push(cursor);
        for range in &request.ranges {
            cursor = cursor.checked_add(range.len).ok_or_else(|| {
                PyValueError::new_err("packed range batch output length exceeds usize")
            })?;
        }
    }
    Ok((offsets, cursor))
}

fn range_batch_plan(requests: Vec<PathRangeReadRequest>) -> PyResult<PythonRangeBatchPlan> {
    Ok(PythonRangeBatchPlan {
        inner: Arc::new(PreparedPathRangeBatch::new(&requests).map_err(runtime_error)?),
    })
}

fn range_batch_reader(
    client: Arc<NoKvFsClient<ConfiguredObjectStore>>,
    requests: Vec<PathRangeReadRequest>,
    memory_kind: &str,
) -> PyResult<PythonRangeBatchReader> {
    let plan = Arc::new(PreparedPathRangeBatch::new(&requests).map_err(runtime_error)?);
    let memory_kind = parse_staging_memory_kind(memory_kind)?;
    let buffer = StagingBuffer::with_capacity(memory_kind, plan.output_len())
        .map_err(staging_memory_error)?;
    Ok(PythonRangeBatchReader {
        client,
        plan,
        buffer: Arc::new(Mutex::new(PythonReadBufferState {
            buffer,
            active_exports: 0,
        })),
    })
}

#[allow(clippy::too_many_arguments)]
fn object_config(
    bucket: &str,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    region: Option<String>,
    root: Option<String>,
    session_token: Option<String>,
    virtual_host_style: bool,
    skip_signature: bool,
    hot_object_root: Option<String>,
) -> ObjectStoreConfig {
    let mut s3 = S3ObjectStoreOptions::new(bucket);
    if let Some(endpoint) = endpoint {
        s3.endpoint = Some(endpoint);
    }
    if let Some(access_key_id) = access_key_id {
        s3.access_key_id = Some(access_key_id);
    }
    if let Some(secret_access_key) = secret_access_key {
        s3.secret_access_key = Some(secret_access_key);
    }
    if let Some(region) = region {
        s3.region = region;
    }
    if let Some(root) = root {
        s3.root = root;
    }
    if let Some(session_token) = session_token {
        s3.session_token = Some(session_token);
    }
    s3.virtual_host_style = virtual_host_style;
    s3.skip_signature = skip_signature;

    match hot_object_root {
        Some(root) => ObjectStoreConfig::tiered_local_with_options(
            LocalObjectStoreOptions::new(PathBuf::from(root)),
            s3,
            TieredObjectStoreOptions::default(),
        ),
        None => ObjectStoreConfig::s3(s3),
    }
}

fn parse_metadata_addr(raw: &str) -> PyResult<SocketAddr> {
    raw.parse::<SocketAddr>()
        .map_err(|err| PyValueError::new_err(format!("invalid metadata_addr {raw:?}: {err}")))
}

fn runtime_error(err: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn parse_staging_memory_kind(raw: &str) -> PyResult<StagingMemoryKind> {
    StagingMemoryKind::parse(raw).ok_or_else(|| {
        PyValueError::new_err(format!(
            "invalid ReadBuffer memory_kind {raw:?}; expected 'system' or 'page_locked'"
        ))
    })
}

fn staging_memory_error(err: StagingMemoryError) -> PyErr {
    PyRuntimeError::new_err(err.to_string())
}

fn file_type_str(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "directory",
        FileType::Symlink => "symlink",
        FileType::NamedPipe => "named_pipe",
        FileType::CharDevice => "char_device",
        FileType::BlockDevice => "block_device",
        FileType::Socket => "socket",
    }
}

fn attr_to_py<'py>(py: Python<'py>, attr: &InodeAttr) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("inode", attr.inode.get())?;
    dict.set_item("type", file_type_str(attr.file_type))?;
    dict.set_item("mode", attr.mode)?;
    dict.set_item("uid", attr.uid)?;
    dict.set_item("gid", attr.gid)?;
    dict.set_item("rdev", attr.rdev)?;
    dict.set_item("nlink", attr.nlink)?;
    dict.set_item("size", attr.size)?;
    dict.set_item("generation", attr.generation)?;
    dict.set_item("mtime_ms", attr.mtime_ms)?;
    dict.set_item("ctime_ms", attr.ctime_ms)?;
    Ok(dict)
}

fn set_body_items(dict: &Bound<'_, PyDict>, body: &nokv_types::BodyDescriptor) -> PyResult<()> {
    dict.set_item("body_size", body.size)?;
    dict.set_item("producer", body.producer.as_str())?;
    dict.set_item("digest_uri", body.digest_uri.as_str())?;
    dict.set_item("content_type", body.content_type.as_str())?;
    dict.set_item("manifest_id", body.manifest_id.as_str())?;
    dict.set_item("body_generation", body.generation)?;
    dict.set_item("chunk_size", body.chunk_size)?;
    dict.set_item("block_size", body.block_size)?;
    Ok(())
}

fn path_metadata_to_py<'py>(
    py: Python<'py>,
    metadata: &PathMetadata,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = attr_to_py(py, &metadata.attr)?;
    if let Some(body) = &metadata.body {
        set_body_items(&dict, body)?;
    }
    Ok(dict)
}

fn dentry_to_py<'py>(py: Python<'py>, entry: &DentryWithAttr) -> PyResult<Bound<'py, PyDict>> {
    let dict = attr_to_py(py, &entry.attr)?;
    dict.set_item(
        "name",
        String::from_utf8_lossy(entry.dentry.name.as_bytes()).into_owned(),
    )?;
    if let Some(body) = &entry.body {
        set_body_items(&dict, body)?;
    }
    Ok(dict)
}

fn snapshot_pin_to_py<'py>(py: Python<'py>, pin: &SnapshotPin) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("snapshot_id", pin.snapshot_id)?;
    dict.set_item("root", pin.root.get())?;
    dict.set_item("read_version", pin.read_version)?;
    dict.set_item("created_version", pin.created_version)?;
    dict.set_item("lease_expires_unix_ms", pin.lease_expires_unix_ms)?;
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_config_preserves_s3_fields() {
        let config = object_config(
            "nokv",
            Some("http://127.0.0.1:9000".to_owned()),
            Some("key".to_owned()),
            Some("secret".to_owned()),
            Some("auto".to_owned()),
            Some("/prefix".to_owned()),
            Some("session".to_owned()),
            true,
            true,
            None,
        );

        let options = config.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(options.access_key_id.as_deref(), Some("key"));
        assert_eq!(options.secret_access_key.as_deref(), Some("secret"));
        assert_eq!(options.region, "auto");
        assert_eq!(options.root, "/prefix");
        assert_eq!(options.session_token.as_deref(), Some("session"));
        assert!(options.virtual_host_style);
        assert!(options.skip_signature);
    }

    #[test]
    fn object_config_keeps_hot_root_out_of_metadata() {
        let config = object_config(
            "nokv",
            Some("http://127.0.0.1:9000".to_owned()),
            Some("key".to_owned()),
            Some("secret".to_owned()),
            Some("auto".to_owned()),
            None,
            None,
            false,
            false,
            Some("/tmp/nokv-hot".to_owned()),
        );

        assert_eq!(
            config.local_hot_root(),
            Some(PathBuf::from("/tmp/nokv-hot").as_path())
        );
        assert_eq!(config.options().bucket, "nokv");
    }
}
