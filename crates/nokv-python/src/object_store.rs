/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use nokv_object::{
    ArtifactObjectStore, ArtifactStoreCapabilities, ImmutableCreateOutcome, MemoryArtifactStore,
    ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange, S3ArtifactStore,
    S3ArtifactStoreOptions,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[derive(Clone, Debug)]
pub(crate) enum ConfiguredObjectStore {
    Memory(MemoryArtifactStore),
    S3(S3ArtifactStore),
}

#[derive(Clone, Debug)]
enum ConfiguredObjectStoreOptions {
    Memory,
    S3(S3ArtifactStoreOptions),
}

#[pyclass(name = "ObjectStoreConfig", frozen, skip_from_py_object)]
#[derive(Clone, Debug)]
pub(crate) struct PythonObjectStoreConfig {
    options: ConfiguredObjectStoreOptions,
}

#[pymethods]
impl PythonObjectStoreConfig {
    /// In-process object storage for tests and local SDK evaluation.
    #[staticmethod]
    fn memory() -> Self {
        Self {
            options: ConfiguredObjectStoreOptions::Memory,
        }
    }

    /// Configure one S3-compatible durable artifact store.
    #[staticmethod]
    #[pyo3(signature = (
        bucket,
        region = "us-east-1",
        root = "/",
        endpoint = None,
        access_key_id = None,
        secret_access_key = None,
        session_token = None,
        virtual_host_style = false,
        skip_signature = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn s3(
        bucket: String,
        region: &str,
        root: &str,
        endpoint: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        virtual_host_style: bool,
        skip_signature: bool,
    ) -> PyResult<Self> {
        if access_key_id.is_some() != secret_access_key.is_some() {
            return Err(PyValueError::new_err(
                "access_key_id and secret_access_key must be provided together",
            ));
        }
        if session_token.is_some() && access_key_id.is_none() {
            return Err(PyValueError::new_err(
                "session_token requires access_key_id and secret_access_key",
            ));
        }
        let mut options = S3ArtifactStoreOptions::new(bucket);
        options.region = region.to_owned();
        options.root = root.to_owned();
        options.endpoint = endpoint;
        options.access_key_id = access_key_id;
        options.secret_access_key = secret_access_key;
        options.session_token = session_token;
        options.virtual_host_style = virtual_host_style;
        options.skip_signature = skip_signature;
        options
            .validate()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self {
            options: ConfiguredObjectStoreOptions::S3(options),
        })
    }

    fn kind(&self) -> &'static str {
        match &self.options {
            ConfiguredObjectStoreOptions::Memory => "memory",
            ConfiguredObjectStoreOptions::S3(_) => "s3",
        }
    }
}

impl PythonObjectStoreConfig {
    pub(crate) fn build(&self) -> Result<ConfiguredObjectStore, ObjectError> {
        match &self.options {
            ConfiguredObjectStoreOptions::Memory => {
                Ok(ConfiguredObjectStore::Memory(MemoryArtifactStore::new()))
            }
            ConfiguredObjectStoreOptions::S3(options) => {
                S3ArtifactStore::new(options.clone()).map(ConfiguredObjectStore::S3)
            }
        }
    }
}

impl ArtifactObjectStore for ConfiguredObjectStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        match self {
            Self::Memory(store) => store.capabilities(),
            Self::S3(store) => store.capabilities(),
        }
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        match self {
            Self::Memory(store) => store.create_immutable(key, bytes),
            Self::S3(store) => store.create_immutable(key, bytes),
        }
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        match self {
            Self::Memory(store) => store.read(key, range),
            Self::S3(store) => store.read(key, range),
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match self {
            Self::Memory(store) => store.head(key),
            Self::S3(store) => store.head(key),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        match self {
            Self::Memory(store) => store.delete(key),
            Self::S3(store) => store.delete(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_configuration_builds_the_explicit_test_store() {
        let config = PythonObjectStoreConfig::memory();
        assert_eq!(config.kind(), "memory");
        assert!(matches!(
            config.build().unwrap(),
            ConfiguredObjectStore::Memory(_)
        ));
    }

    #[test]
    fn partial_s3_credentials_fail_closed() {
        Python::initialize();
        let error = PythonObjectStoreConfig::s3(
            "artifacts".to_owned(),
            "us-east-1",
            "/",
            Some("http://127.0.0.1:9000".to_owned()),
            Some("access".to_owned()),
            None,
            None,
            false,
            false,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be provided together"));
    }
}
