//! Path-oriented Rust client for NoKV.
//!
//! This crate owns SDK ergonomics over the metadata service. It does not
//! own metadata layout, Holt trees, object-store internals, FUSE, or metadata
//! wire-format definitions.

use std::fmt;

mod agent;
mod artifact;
#[cfg(test)]
mod artifact_tests;
mod file_client;
mod framed;
mod read_cache;
mod service;
mod wire;

use nokv_meta::{MetadError, MetadataError};
use nokv_object::ObjectError;
use nokv_types::{AdvisoryLock, PathError, PathMetadata};

pub use agent::{agent_tool_definitions, execute_agent_tool, AgentNamespace, AgentToolDefinition};
pub use artifact::{
    normalize_artifact_path, ArtifactBackend, ArtifactInfo, ArtifactRepository,
    ArtifactRepositoryOptions,
};
pub use file_client::{NoKvFsClient, PathRangeReadRequest, PathReadRange, PreparedPathRangeBatch};
pub use nokv_object::{DataFabricReadStats, ObjectReadPlan};
pub use service::{
    ClientPreparedArtifact, CloneOutcome, MetadataClient, MetadataClientOptions, PathLayoutOpen,
    PathLayoutOpenRequest, SnapshotOutcome,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactMetadata {
    pub producer: String,
    pub digest_uri: String,
    pub content_type: String,
    pub manifest_id: String,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceRead {
    pub metadata: PathMetadata,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum ClientError {
    EmptyPath,
    RelativePath,
    ParentTraversal,
    InvalidArtifactPath(String),
    ArtifactIsDirectory(String),
    ArtifactIsFile(String),
    InvalidName(String),
    RootHasNoParent,
    NotFound(String),
    LockConflict(AdvisoryLock),
    Metadata(MetadError),
    Object(ObjectError),
    Io(String),
    Protocol(String),
}

impl From<MetadError> for ClientError {
    fn from(err: MetadError) -> Self {
        Self::Metadata(err)
    }
}

impl From<PathError> for ClientError {
    fn from(err: PathError) -> Self {
        match err {
            PathError::Empty => Self::EmptyPath,
            PathError::Relative => Self::RelativePath,
            PathError::ParentTraversal => Self::ParentTraversal,
            PathError::InvalidName(err) => Self::InvalidName(err.to_string()),
        }
    }
}

impl From<ObjectError> for ClientError {
    fn from(err: ObjectError) -> Self {
        Self::Object(err)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPath => write!(f, "path is empty"),
            Self::RelativePath => write!(f, "path must be absolute"),
            Self::ParentTraversal => write!(f, "path must not contain '..'"),
            Self::InvalidArtifactPath(err) => write!(f, "invalid artifact path: {err}"),
            Self::ArtifactIsDirectory(path) => write!(f, "artifact is a directory: {path}"),
            Self::ArtifactIsFile(path) => write!(f, "artifact is a file: {path}"),
            Self::InvalidName(err) => write!(f, "invalid path component: {err}"),
            Self::RootHasNoParent => write!(f, "root path has no parent"),
            Self::NotFound(path) => write!(f, "path component not found: {path}"),
            Self::LockConflict(lock) => write!(
                f,
                "advisory lock conflicts with {:?} lock on inode {} range {}..={} owned by {}",
                lock.kind,
                lock.inode.get(),
                lock.start,
                lock.end,
                lock.owner
            ),
            Self::Metadata(err) => write!(f, "metadata service error: {err}"),
            Self::Object(err) => write!(f, "object store error: {err}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Protocol(err) => write!(f, "metadata protocol error: {err}"),
        }
    }
}

impl std::error::Error for ClientError {}

fn is_metadata_predicate_failed(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Metadata(MetadError::Metadata(MetadataError::PredicateFailed))
    )
}

fn is_not_found(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::NotFound(_) | ClientError::Metadata(MetadError::NotFound)
    )
}
