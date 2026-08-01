use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex};

use opendal::blocking::Operator as BlockingOperator;
use opendal::options::WriteOptions;
use opendal::services::S3;
use opendal::{ErrorKind, Operator};

use crate::digest::{hex, sha256};

pub const DEFAULT_S3_MULTIPART_PART_SIZE: usize = 8 * 1024 * 1024;
pub const DEFAULT_S3_MULTIPART_CONCURRENCY: usize = 8;

static OPENDAL_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("nokv-artifact-object")
        .build()
        .expect("create NoKV artifact object runtime")
});

/// Provider-neutral relative key for one immutable artifact object.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectRange {
    pub offset: u64,
    pub len: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: ObjectKey,
    pub size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImmutableCreateOutcome {
    Created,
    Replayed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectDeleteOutcome {
    Deleted,
    Absent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArtifactStoreCapabilities {
    pub range_read: bool,
    pub multipart_create: bool,
    pub atomic_create_if_absent: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectError {
    EmptyKey,
    AbsoluteKey,
    EmptyKeyComponent,
    ParentTraversal,
    CurrentDirectory,
    BackslashInKey,
    ContainsNul,
    InvalidRange {
        offset: u64,
        len: usize,
        object_size: Option<u64>,
    },
    ObjectNotFound {
        key: ObjectKey,
    },
    ImmutableCollision {
        key: ObjectKey,
        expected_sha256: String,
        actual_sha256: String,
    },
    DigestMismatch {
        key: ObjectKey,
        expected_sha256: String,
        actual_sha256: String,
    },
    InvalidManifest(String),
    MissingBucket,
    MissingRegion,
    CreateAmbiguous {
        key: ObjectKey,
        detail: String,
    },
    DeleteAmbiguous {
        key: ObjectKey,
        detail: String,
    },
    Backend(String),
}

/// Immutable durable-object boundary.
///
/// `create_immutable` may create a missing key or accept an exact-byte replay.
/// It must never replace different bytes at an existing key.
pub trait ArtifactObjectStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities;

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError>;

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError>;

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError>;

    /// Delete one immutable object.
    ///
    /// A backend error after dispatch must be returned as
    /// [`ObjectError::DeleteAmbiguous`], never collapsed into absence.
    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError>;
}

impl<T> ArtifactObjectStore for Arc<T>
where
    T: ArtifactObjectStore + ?Sized,
{
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        (**self).capabilities()
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        (**self).create_immutable(key, bytes)
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        (**self).read(key, range)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        (**self).head(key)
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        (**self).delete(key)
    }
}

impl ObjectKey {
    pub fn new(raw: impl Into<String>) -> Result<Self, ObjectError> {
        let raw = raw.into();
        validate_key(&raw)?;
        Ok(Self(raw))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl ObjectRange {
    pub fn new(offset: u64, len: usize) -> Result<Self, ObjectError> {
        let range = Self { offset, len };
        range.end()?;
        if len == 0 {
            return Err(ObjectError::InvalidRange {
                offset,
                len,
                object_size: None,
            });
        }
        Ok(range)
    }

    pub fn end(self) -> Result<u64, ObjectError> {
        let len = u64::try_from(self.len).map_err(|_| ObjectError::InvalidRange {
            offset: self.offset,
            len: self.len,
            object_size: None,
        })?;
        self.offset
            .checked_add(len)
            .ok_or(ObjectError::InvalidRange {
                offset: self.offset,
                len: self.len,
                object_size: None,
            })
    }

    pub fn validate_within(self, object_size: u64) -> Result<(), ObjectError> {
        if self.len == 0 || self.end()? > object_size {
            return Err(ObjectError::InvalidRange {
                offset: self.offset,
                len: self.len,
                object_size: Some(object_size),
            });
        }
        Ok(())
    }
}

impl Default for ArtifactStoreCapabilities {
    fn default() -> Self {
        Self {
            range_read: true,
            multipart_create: false,
            atomic_create_if_absent: true,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryArtifactStore {
    state: Arc<Mutex<MemoryArtifactStoreState>>,
}

#[derive(Clone, Debug, Default)]
struct MemoryArtifactStoreState {
    objects: BTreeMap<ObjectKey, Vec<u8>>,
    stats: MemoryArtifactStoreStats,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryArtifactStoreStats {
    pub resident_objects: u64,
    pub resident_bytes: u64,
    pub creates: u64,
    pub replays: u64,
    pub collisions: u64,
    pub reads: u64,
    pub read_bytes: u64,
    pub deletes: u64,
}

impl MemoryArtifactStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn stats(&self) -> Result<MemoryArtifactStoreStats, ObjectError> {
        self.state
            .lock()
            .map(|state| state.stats)
            .map_err(ObjectError::poisoned)
    }
}

impl ArtifactObjectStore for MemoryArtifactStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        ArtifactStoreCapabilities::default()
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        if let Some(existing) = state.objects.get(key).cloned() {
            let matches = existing.as_slice() == bytes;
            if matches {
                state.stats.replays = state.stats.replays.saturating_add(1);
                return Ok(ImmutableCreateOutcome::Replayed);
            }
            state.stats.collisions = state.stats.collisions.saturating_add(1);
            return Err(immutable_collision(key, bytes, &existing));
        }
        state.objects.insert(key.clone(), bytes.to_vec());
        state.stats.creates = state.stats.creates.saturating_add(1);
        state.stats.resident_objects = state.stats.resident_objects.saturating_add(1);
        state.stats.resident_bytes = state
            .stats
            .resident_bytes
            .saturating_add(bytes.len() as u64);
        Ok(ImmutableCreateOutcome::Created)
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        let bytes = state
            .objects
            .get(key)
            .ok_or_else(|| ObjectError::ObjectNotFound { key: key.clone() })?;
        let result = strict_range(bytes, range)?;
        let result_len = result.len() as u64;
        state.stats.reads = state.stats.reads.saturating_add(1);
        state.stats.read_bytes = state.stats.read_bytes.saturating_add(result_len);
        Ok(result)
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        Ok(self
            .state
            .lock()
            .map_err(ObjectError::poisoned)?
            .objects
            .get(key)
            .map(|bytes| ObjectInfo {
                key: key.clone(),
                size: bytes.len() as u64,
            }))
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        let mut state = self.state.lock().map_err(ObjectError::poisoned)?;
        let Some(bytes) = state.objects.remove(key) else {
            return Ok(ObjectDeleteOutcome::Absent);
        };
        state.stats.deletes = state.stats.deletes.saturating_add(1);
        state.stats.resident_objects = state.stats.resident_objects.saturating_sub(1);
        state.stats.resident_bytes = state
            .stats
            .resident_bytes
            .saturating_sub(bytes.len() as u64);
        Ok(ObjectDeleteOutcome::Deleted)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3ArtifactStoreOptions {
    pub bucket: String,
    pub root: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub virtual_host_style: bool,
    pub skip_signature: bool,
}

impl S3ArtifactStoreOptions {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            root: "/".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_host_style: false,
            skip_signature: false,
        }
    }

    pub fn for_endpoint(
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            root: "/".to_owned(),
            region: "auto".to_owned(),
            endpoint: Some(endpoint.into()),
            access_key_id: Some(access_key_id.into()),
            secret_access_key: Some(secret_access_key.into()),
            session_token: None,
            virtual_host_style: false,
            skip_signature: false,
        }
    }

    pub fn validate(&self) -> Result<(), ObjectError> {
        if self.bucket.is_empty() {
            return Err(ObjectError::MissingBucket);
        }
        if self.region.is_empty() {
            return Err(ObjectError::MissingRegion);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct S3ArtifactStore {
    operator: BlockingOperator,
}

impl S3ArtifactStore {
    pub fn new(options: S3ArtifactStoreOptions) -> Result<Self, ObjectError> {
        options.validate()?;
        let mut builder = S3::default()
            .bucket(&options.bucket)
            .root(&options.root)
            .region(&options.region);
        if let Some(endpoint) = &options.endpoint {
            builder = builder.endpoint(endpoint);
        }
        if let Some(access_key_id) = &options.access_key_id {
            builder = builder.access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &options.secret_access_key {
            builder = builder.secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &options.session_token {
            builder = builder.session_token(session_token);
        }
        if options.virtual_host_style {
            builder = builder.enable_virtual_host_style();
        }
        if options.skip_signature {
            builder = builder.skip_signature();
        }

        let operator = Operator::new(builder)
            .map_err(ObjectError::backend)?
            .finish();
        let _runtime_guard = OPENDAL_RUNTIME.enter();
        let operator = BlockingOperator::new(operator).map_err(ObjectError::backend)?;
        Ok(Self { operator })
    }

    fn read_existing_for_replay(
        &self,
        key: &ObjectKey,
        expected: &[u8],
        create_error: impl fmt::Display,
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        match self.operator.read(key.as_str()) {
            Ok(existing) if existing.to_vec() == expected => Ok(ImmutableCreateOutcome::Replayed),
            Ok(existing) => Err(immutable_collision(key, expected, &existing.to_vec())),
            Err(read_error) => Err(ObjectError::CreateAmbiguous {
                key: key.clone(),
                detail: format!(
                    "conditional create failed ({create_error}); replay reconciliation failed ({read_error})"
                ),
            }),
        }
    }
}

impl ArtifactObjectStore for S3ArtifactStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        ArtifactStoreCapabilities {
            range_read: true,
            multipart_create: true,
            atomic_create_if_absent: true,
        }
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        let mut writer = match self.operator.writer_options(
            key.as_str(),
            WriteOptions {
                if_not_exists: true,
                chunk: Some(DEFAULT_S3_MULTIPART_PART_SIZE),
                concurrent: DEFAULT_S3_MULTIPART_CONCURRENCY,
                ..WriteOptions::default()
            },
        ) {
            Ok(writer) => writer,
            Err(error) => return self.read_existing_for_replay(key, bytes, error),
        };
        if let Err(error) = writer.write(bytes.to_vec()) {
            return self.read_existing_for_replay(key, bytes, error);
        }
        match writer.close() {
            Ok(_) => Ok(ImmutableCreateOutcome::Created),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) =>
            {
                self.read_existing_for_replay(key, bytes, error)
            }
            Err(error) => self.read_existing_for_replay(key, bytes, error),
        }
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        let buffer = match range {
            Some(range) => {
                let end = range.end()?;
                self.operator
                    .reader(key.as_str())
                    .and_then(|reader| reader.read(range.offset..end))
            }
            None => self.operator.read(key.as_str()),
        };
        match buffer {
            Ok(bytes) => {
                let bytes = bytes.to_vec();
                if let Some(range) = range {
                    if bytes.len() != range.len {
                        return Err(ObjectError::InvalidRange {
                            offset: range.offset,
                            len: range.len,
                            object_size: None,
                        });
                    }
                }
                Ok(bytes)
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                Err(ObjectError::ObjectNotFound { key: key.clone() })
            }
            Err(error) => Err(ObjectError::backend(error)),
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match self.operator.stat(key.as_str()) {
            Ok(metadata) => Ok(Some(ObjectInfo {
                key: key.clone(),
                size: metadata.content_length(),
            })),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(ObjectError::backend(error)),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        let existed = match self.head(key) {
            Ok(Some(_)) => true,
            Ok(None) => return Ok(ObjectDeleteOutcome::Absent),
            Err(error) => {
                return Err(ObjectError::DeleteAmbiguous {
                    key: key.clone(),
                    detail: format!("pre-delete state could not be established: {error}"),
                });
            }
        };
        match self.operator.delete(key.as_str()) {
            Ok(()) if existed => Ok(ObjectDeleteOutcome::Deleted),
            Ok(()) => Ok(ObjectDeleteOutcome::Absent),
            Err(error) => Err(ObjectError::DeleteAmbiguous {
                key: key.clone(),
                detail: error.to_string(),
            }),
        }
    }
}

fn validate_key(raw: &str) -> Result<(), ObjectError> {
    if raw.is_empty() {
        return Err(ObjectError::EmptyKey);
    }
    if raw.starts_with('/') {
        return Err(ObjectError::AbsoluteKey);
    }
    if raw.contains('\\') {
        return Err(ObjectError::BackslashInKey);
    }
    if raw.as_bytes().contains(&0) {
        return Err(ObjectError::ContainsNul);
    }
    for component in raw.split('/') {
        match component {
            "" => return Err(ObjectError::EmptyKeyComponent),
            "." => return Err(ObjectError::CurrentDirectory),
            ".." => return Err(ObjectError::ParentTraversal),
            _ => {}
        }
    }
    Ok(())
}

pub(crate) fn strict_range(
    bytes: &[u8],
    range: Option<ObjectRange>,
) -> Result<Vec<u8>, ObjectError> {
    let Some(range) = range else {
        return Ok(bytes.to_vec());
    };
    range.validate_within(bytes.len() as u64)?;
    let start = usize::try_from(range.offset).map_err(|_| ObjectError::InvalidRange {
        offset: range.offset,
        len: range.len,
        object_size: Some(bytes.len() as u64),
    })?;
    let end = start
        .checked_add(range.len)
        .ok_or(ObjectError::InvalidRange {
            offset: range.offset,
            len: range.len,
            object_size: Some(bytes.len() as u64),
        })?;
    Ok(bytes[start..end].to_vec())
}

fn immutable_collision(key: &ObjectKey, expected: &[u8], actual: &[u8]) -> ObjectError {
    ObjectError::ImmutableCollision {
        key: key.clone(),
        expected_sha256: hex(&sha256(expected)),
        actual_sha256: hex(&sha256(actual)),
    }
}

impl ObjectError {
    pub(crate) fn backend(error: impl fmt::Display) -> Self {
        Self::Backend(error.to_string())
    }

    pub(crate) fn poisoned(error: impl fmt::Display) -> Self {
        Self::Backend(format!("artifact object lock poisoned: {error}"))
    }
}

impl fmt::Display for ObjectError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyKey => formatter.write_str("object key is empty"),
            Self::AbsoluteKey => formatter.write_str("object key must be relative"),
            Self::EmptyKeyComponent => formatter.write_str("object key contains an empty component"),
            Self::ParentTraversal => formatter.write_str("object key contains '..'"),
            Self::CurrentDirectory => formatter.write_str("object key contains '.'"),
            Self::BackslashInKey => {
                formatter.write_str("object key must use provider-neutral '/' separators")
            }
            Self::ContainsNul => formatter.write_str("object key contains NUL"),
            Self::InvalidRange {
                offset,
                len,
                object_size,
            } => write!(
                formatter,
                "invalid object range offset={offset} len={len} object_size={object_size:?}"
            ),
            Self::ObjectNotFound { key } => write!(formatter, "object not found: {key}"),
            Self::ImmutableCollision {
                key,
                expected_sha256,
                actual_sha256,
            } => write!(
                formatter,
                "immutable object collision at {key}: expected sha256={expected_sha256}, actual sha256={actual_sha256}"
            ),
            Self::DigestMismatch {
                key,
                expected_sha256,
                actual_sha256,
            } => write!(
                formatter,
                "artifact block digest mismatch at {key}: expected sha256={expected_sha256}, actual sha256={actual_sha256}"
            ),
            Self::InvalidManifest(detail) => write!(formatter, "invalid artifact manifest: {detail}"),
            Self::MissingBucket => formatter.write_str("S3 bucket is required"),
            Self::MissingRegion => formatter.write_str("S3 region is required"),
            Self::CreateAmbiguous { key, detail } => {
                write!(formatter, "immutable create outcome is ambiguous for {key}: {detail}")
            }
            Self::DeleteAmbiguous { key, detail } => {
                write!(formatter, "delete outcome is ambiguous for {key}: {detail}")
            }
            Self::Backend(detail) => write!(formatter, "artifact object backend error: {detail}"),
        }
    }
}

impl std::error::Error for ObjectError {}
