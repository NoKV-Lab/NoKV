use nokv_meta::{DentryWithAttr, MetadError, RenameReplaceResult};
use nokv_object::ObjectStore;
use nokv_types::FileType;
use sha2::{Digest, Sha256};

use crate::{
    is_metadata_not_found, is_metadata_predicate_failed, ArtifactMetadata, ClientError,
    NoKvFsClient,
};

const DEFAULT_ARTIFACT_FILE_MODE: u32 = 0o644;
const DEFAULT_ARTIFACT_DIRECTORY_MODE: u32 = 0o755;
const DEFAULT_ARTIFACT_UID: u32 = 1000;
const DEFAULT_ARTIFACT_GID: u32 = 1000;
const DEFAULT_ARTIFACT_PRODUCER: &str = "nokv-client";
const DEFAULT_ARTIFACT_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactRepositoryOptions {
    pub file_mode: u32,
    pub directory_mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub producer: String,
    pub content_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactInfo {
    pub path: String,
    pub is_dir: bool,
    pub size: Option<u64>,
    pub metadata: Option<ArtifactMetadata>,
}

pub trait ArtifactBackend {
    fn lookup_path(&self, absolute_path: &str) -> Result<Option<DentryWithAttr>, ClientError>;

    fn list_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError>;

    fn list_indexed_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError>;

    fn create_directory_path(
        &self,
        absolute_path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError>;

    fn publish_new_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError>;

    fn replace_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError>;

    fn read_file_path(&self, absolute_path: &str) -> Result<Vec<u8>, ClientError>;

    fn remove_file_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError>;

    fn remove_file_paths(
        &self,
        absolute_paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut results = Vec::with_capacity(absolute_paths.len());
        for path in absolute_paths {
            results.push(self.remove_file_path(path));
        }
        Ok(results)
    }

    fn remove_empty_dir_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError>;

    fn remove_empty_dir_paths(
        &self,
        absolute_paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        let mut results = Vec::with_capacity(absolute_paths.len());
        for path in absolute_paths {
            results.push(self.remove_empty_dir_path(path));
        }
        Ok(results)
    }
}

pub struct ArtifactRepository<B> {
    backend: B,
    options: ArtifactRepositoryOptions,
}

impl Default for ArtifactRepositoryOptions {
    fn default() -> Self {
        Self {
            file_mode: DEFAULT_ARTIFACT_FILE_MODE,
            directory_mode: DEFAULT_ARTIFACT_DIRECTORY_MODE,
            uid: DEFAULT_ARTIFACT_UID,
            gid: DEFAULT_ARTIFACT_GID,
            producer: DEFAULT_ARTIFACT_PRODUCER.to_owned(),
            content_type: DEFAULT_ARTIFACT_CONTENT_TYPE.to_owned(),
        }
    }
}

impl<B> ArtifactRepository<B> {
    pub fn new(backend: B) -> Self {
        Self::with_options(backend, ArtifactRepositoryOptions::default())
    }

    pub fn with_options(backend: B, options: ArtifactRepositoryOptions) -> Self {
        Self { backend, options }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }

    pub fn into_backend(self) -> B {
        self.backend
    }
}

impl<B> ArtifactRepository<B>
where
    B: ArtifactBackend,
{
    pub fn put_bytes(
        &self,
        artifact_path: &str,
        bytes: Vec<u8>,
    ) -> Result<ArtifactInfo, ClientError> {
        let normalized = normalize_artifact_path(artifact_path, false)?;
        self.ensure_parent_directories(&normalized)?;
        let absolute_path = absolute_artifact_path(&normalized);
        let metadata = self.metadata_for_put(&normalized, &bytes);
        let entry = match self.backend.lookup_path(&absolute_path)? {
            Some(existing) if existing.attr.file_type == FileType::Directory => {
                return Err(ClientError::ArtifactIsDirectory(normalized));
            }
            Some(_) => {
                self.backend
                    .replace_artifact_path(&absolute_path, bytes, metadata)?
                    .entry
            }
            None => match self
                .backend
                .publish_new_artifact_path(&absolute_path, bytes, metadata)
            {
                Ok(entry) => entry,
                Err(err) if is_metadata_predicate_failed(&err) => {
                    return Err(ClientError::ArtifactIsFile(normalized));
                }
                Err(err) => return Err(err),
            },
        };
        artifact_info(&normalized, &entry)
    }

    pub fn get_bytes(&self, artifact_path: &str) -> Result<Vec<u8>, ClientError> {
        let normalized = normalize_artifact_path(artifact_path, false)?;
        let absolute_path = absolute_artifact_path(&normalized);
        match self.backend.lookup_path(&absolute_path)? {
            Some(entry) if entry.attr.file_type == FileType::Directory => {
                Err(ClientError::ArtifactIsDirectory(normalized))
            }
            Some(_) => self.backend.read_file_path(&absolute_path),
            None => Err(ClientError::NotFound(normalized)),
        }
    }

    pub fn stat(&self, artifact_path: &str) -> Result<ArtifactInfo, ClientError> {
        let normalized = normalize_artifact_path(artifact_path, true)?;
        if normalized.is_empty() {
            return Ok(ArtifactInfo {
                path: String::new(),
                is_dir: true,
                size: None,
                metadata: None,
            });
        }
        let absolute_path = absolute_artifact_path(&normalized);
        let entry = self
            .backend
            .lookup_path(&absolute_path)?
            .ok_or_else(|| ClientError::NotFound(normalized.clone()))?;
        artifact_info(&normalized, &entry)
    }

    pub fn list(&self, artifact_path: &str) -> Result<Vec<ArtifactInfo>, ClientError> {
        let normalized = normalize_artifact_path(artifact_path, true)?;
        let absolute_path = absolute_artifact_path(&normalized);
        if !normalized.is_empty() {
            let Some(entry) = self.backend.lookup_path(&absolute_path)? else {
                return Ok(Vec::new());
            };
            if entry.attr.file_type == FileType::File {
                return Ok(Vec::new());
            }
        }
        let mut entries = self
            .backend
            .list_indexed_path(&absolute_path)?
            .into_iter()
            .map(|entry| {
                let child_path = child_artifact_path(&normalized, &entry)?;
                artifact_info(&child_path, &entry)
            })
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(entries)
    }

    pub fn delete(&self, artifact_path: &str) -> Result<(), ClientError> {
        let normalized = normalize_artifact_path(artifact_path, true)?;
        if normalized.is_empty() {
            return self.delete_directory_children("");
        }
        let absolute_path = absolute_artifact_path(&normalized);
        let entry = match self.backend.lookup_path(&absolute_path) {
            Ok(Some(entry)) => entry,
            Ok(None) => return Ok(()),
            Err(err) if is_metadata_not_found(&err) => return Ok(()),
            Err(err) => return Err(err),
        };
        if entry.attr.file_type == FileType::Directory {
            self.delete_directory_children(&normalized)?;
            match self.backend.remove_empty_dir_path(&absolute_path) {
                Ok(_) => Ok(()),
                Err(err) if is_metadata_not_found(&err) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            match self.backend.remove_file_path(&absolute_path) {
                Ok(_) => Ok(()),
                Err(err) if is_metadata_not_found(&err) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    fn ensure_parent_directories(&self, normalized_path: &str) -> Result<(), ClientError> {
        let mut parts = normalized_path.split('/').collect::<Vec<_>>();
        parts.pop();
        let mut current = String::new();
        for part in parts {
            current = if current.is_empty() {
                part.to_owned()
            } else {
                format!("{current}/{part}")
            };
            let absolute_path = absolute_artifact_path(&current);
            match self.backend.lookup_path(&absolute_path)? {
                Some(entry) if entry.attr.file_type == FileType::Directory => {}
                Some(_) => return Err(ClientError::ArtifactIsFile(current)),
                None => {
                    self.backend.create_directory_path(
                        &absolute_path,
                        self.options.directory_mode,
                        self.options.uid,
                        self.options.gid,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn delete_directory_children(&self, normalized_path: &str) -> Result<(), ClientError> {
        let children = self.list(normalized_path)?;
        let mut file_paths = Vec::new();
        let mut directories = Vec::new();
        for child in children {
            if child.is_dir {
                directories.push(child.path);
            } else {
                file_paths.push(absolute_artifact_path(&child.path));
            }
        }
        if !file_paths.is_empty() {
            for result in self.backend.remove_file_paths(&file_paths)? {
                match result {
                    Ok(_) => {}
                    Err(err) if is_metadata_not_found(&err) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        let mut directory_paths = Vec::with_capacity(directories.len());
        for directory in directories {
            self.delete_directory_children(&directory)?;
            directory_paths.push(absolute_artifact_path(&directory));
        }
        if !directory_paths.is_empty() {
            for result in self.backend.remove_empty_dir_paths(&directory_paths)? {
                match result {
                    Ok(_) => {}
                    Err(err) if is_metadata_not_found(&err) => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    fn metadata_for_put(&self, normalized_path: &str, bytes: &[u8]) -> ArtifactMetadata {
        ArtifactMetadata {
            producer: self.options.producer.clone(),
            digest_uri: artifact_digest_uri(bytes),
            content_type: self.options.content_type.clone(),
            manifest_id: normalized_path.to_owned(),
            mode: self.options.file_mode,
            uid: self.options.uid,
            gid: self.options.gid,
        }
    }
}

impl<T> ArtifactBackend for &T
where
    T: ArtifactBackend + ?Sized,
{
    fn lookup_path(&self, absolute_path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        (*self).lookup_path(absolute_path)
    }

    fn list_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        (*self).list_path(absolute_path)
    }

    fn list_indexed_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        (*self).list_indexed_path(absolute_path)
    }

    fn create_directory_path(
        &self,
        absolute_path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        (*self).create_directory_path(absolute_path, mode, uid, gid)
    }

    fn publish_new_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        (*self).publish_new_artifact_path(absolute_path, bytes, metadata)
    }

    fn replace_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        (*self).replace_artifact_path(absolute_path, bytes, metadata)
    }

    fn read_file_path(&self, absolute_path: &str) -> Result<Vec<u8>, ClientError> {
        (*self).read_file_path(absolute_path)
    }

    fn remove_file_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError> {
        (*self).remove_file_path(absolute_path)
    }

    fn remove_file_paths(
        &self,
        absolute_paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        (*self).remove_file_paths(absolute_paths)
    }

    fn remove_empty_dir_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError> {
        (*self).remove_empty_dir_path(absolute_path)
    }

    fn remove_empty_dir_paths(
        &self,
        absolute_paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        (*self).remove_empty_dir_paths(absolute_paths)
    }
}

impl<O> ArtifactBackend for NoKvFsClient<O>
where
    O: ObjectStore + Send + Sync + 'static,
{
    fn lookup_path(&self, absolute_path: &str) -> Result<Option<DentryWithAttr>, ClientError> {
        self.metadata().lookup(absolute_path)
    }

    fn list_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        self.metadata().list(absolute_path)
    }

    fn list_indexed_path(&self, absolute_path: &str) -> Result<Vec<DentryWithAttr>, ClientError> {
        self.metadata().list_indexed(absolute_path)
    }

    fn create_directory_path(
        &self,
        absolute_path: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<DentryWithAttr, ClientError> {
        self.metadata().mkdir(absolute_path, mode, uid, gid)
    }

    fn publish_new_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<DentryWithAttr, ClientError> {
        self.put_artifact(absolute_path, bytes, metadata)
    }

    fn replace_artifact_path(
        &self,
        absolute_path: &str,
        bytes: Vec<u8>,
        metadata: ArtifactMetadata,
    ) -> Result<RenameReplaceResult, ClientError> {
        self.put_artifact_replace(absolute_path, bytes, metadata)
    }

    fn read_file_path(&self, absolute_path: &str) -> Result<Vec<u8>, ClientError> {
        self.cat(absolute_path)
    }

    fn remove_file_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError> {
        self.metadata().remove(absolute_path)
    }

    fn remove_file_paths(
        &self,
        absolute_paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        self.metadata().remove_many(absolute_paths)
    }

    fn remove_empty_dir_path(&self, absolute_path: &str) -> Result<DentryWithAttr, ClientError> {
        self.metadata().rmdir(absolute_path)
    }

    fn remove_empty_dir_paths(
        &self,
        absolute_paths: &[String],
    ) -> Result<Vec<Result<DentryWithAttr, ClientError>>, ClientError> {
        self.metadata().rmdir_many(absolute_paths)
    }
}

pub fn normalize_artifact_path(path: &str, allow_empty: bool) -> Result<String, ClientError> {
    if path.is_empty() {
        if allow_empty {
            return Ok(String::new());
        }
        return Err(ClientError::InvalidArtifactPath(
            "artifact path is required".to_owned(),
        ));
    }
    if path.starts_with('/') {
        return Err(ClientError::InvalidArtifactPath(
            "artifact path must be relative".to_owned(),
        ));
    }
    if path.ends_with('/') {
        return Err(ClientError::InvalidArtifactPath(
            "artifact path must not end with '/'".to_owned(),
        ));
    }
    if path.contains("//") {
        return Err(ClientError::InvalidArtifactPath(
            "artifact path must not contain empty components".to_owned(),
        ));
    }
    if path.contains('\\') {
        return Err(ClientError::InvalidArtifactPath(
            "artifact path must use POSIX separators".to_owned(),
        ));
    }
    if path.contains('\0') {
        return Err(ClientError::InvalidArtifactPath(
            "artifact path contains a NUL byte".to_owned(),
        ));
    }
    for component in path.split('/') {
        if component == "." || component == ".." {
            return Err(ClientError::InvalidArtifactPath(
                "artifact path must not contain '.' or '..' components".to_owned(),
            ));
        }
    }
    Ok(path.to_owned())
}

fn absolute_artifact_path(normalized_path: &str) -> String {
    if normalized_path.is_empty() {
        "/".to_owned()
    } else {
        format!("/{normalized_path}")
    }
}

fn child_artifact_path(parent: &str, entry: &DentryWithAttr) -> Result<String, ClientError> {
    let name = String::from_utf8(entry.dentry.name.as_bytes().to_vec())
        .map_err(|_| ClientError::InvalidName("artifact paths require utf-8 names".to_owned()))?;
    if parent.is_empty() {
        Ok(name)
    } else {
        Ok(format!("{parent}/{name}"))
    }
}

fn artifact_info(path: &str, entry: &DentryWithAttr) -> Result<ArtifactInfo, ClientError> {
    if entry.attr.file_type == FileType::Directory {
        return Ok(ArtifactInfo {
            path: path.to_owned(),
            is_dir: true,
            size: None,
            metadata: None,
        });
    }
    if entry.attr.file_type != FileType::File {
        return Err(ClientError::Metadata(MetadError::NotFile));
    }
    let metadata = entry.body.as_ref().map(|body| ArtifactMetadata {
        producer: body.producer.clone(),
        digest_uri: body.digest_uri.clone(),
        content_type: body.content_type.clone(),
        manifest_id: body.manifest_id.clone(),
        mode: entry.attr.mode,
        uid: entry.attr.uid,
        gid: entry.attr.gid,
    });
    Ok(ArtifactInfo {
        path: path.to_owned(),
        is_dir: false,
        size: Some(entry.attr.size),
        metadata,
    })
}

fn artifact_digest_uri(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}
