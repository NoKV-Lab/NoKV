/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::fmt;
use std::sync::Arc;

use nokv_object::{
    admit_artifact_provider, ArtifactObjectStore, ArtifactStoreCapabilities,
    ImmutableCreateOutcome, MemoryArtifactStore, ObjectDeleteOutcome, ObjectError, ObjectInfo,
    ObjectKey, ObjectRange, ProviderAdmissionError, ProviderAdmissionProfile,
    ProviderAdmissionReceipt, ProviderHandleIdentity, S3ArtifactStore, S3ArtifactStoreOptions,
    DEFAULT_ARTIFACT_BLOCK_SIZE,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

#[derive(Clone, Debug)]
pub(crate) enum ConfiguredObjectStore {
    Memory(MemoryArtifactStore),
    S3 {
        store: S3ArtifactStore,
        admission: Arc<ProviderAdmissionReceipt>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ConfiguredObjectStoreBuildError {
    Object(ObjectError),
    Admission(ProviderAdmissionError),
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

impl ConfiguredObjectStore {
    pub(crate) fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }
}

impl PythonObjectStoreConfig {
    pub(crate) fn build(&self) -> Result<ConfiguredObjectStore, ConfiguredObjectStoreBuildError> {
        match &self.options {
            ConfiguredObjectStoreOptions::Memory => {
                Ok(ConfiguredObjectStore::Memory(MemoryArtifactStore::new()))
            }
            ConfiguredObjectStoreOptions::S3(options) => {
                let store = S3ArtifactStore::new(options.clone())?;
                let profile = ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE)?;
                let admission = admit_artifact_provider(&store, profile)?;
                Ok(ConfiguredObjectStore::S3 {
                    store,
                    admission: Arc::new(admission),
                })
            }
        }
    }
}

impl From<ObjectError> for ConfiguredObjectStoreBuildError {
    fn from(error: ObjectError) -> Self {
        Self::Object(error)
    }
}

impl From<ProviderAdmissionError> for ConfiguredObjectStoreBuildError {
    fn from(error: ProviderAdmissionError) -> Self {
        Self::Admission(error)
    }
}

impl fmt::Display for ConfiguredObjectStoreBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Object(error) => error.fmt(formatter),
            Self::Admission(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ConfiguredObjectStoreBuildError {}

impl ArtifactObjectStore for ConfiguredObjectStore {
    fn capabilities(&self) -> ArtifactStoreCapabilities {
        match self {
            Self::Memory(store) => store.capabilities(),
            Self::S3 { store, .. } => store.capabilities(),
        }
    }

    fn provider_handle_identity(&self) -> ProviderHandleIdentity {
        match self {
            Self::Memory(store) => store.provider_handle_identity(),
            Self::S3 { store, .. } => store.provider_handle_identity(),
        }
    }

    fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
        match self {
            Self::Memory(store) => store.provider_admission_receipt(),
            Self::S3 { admission, .. } => Some(admission.as_ref()),
        }
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        match self {
            Self::Memory(store) => store.create_immutable(key, bytes),
            Self::S3 { store, .. } => store.create_immutable(key, bytes),
        }
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        match self {
            Self::Memory(store) => store.read(key, range),
            Self::S3 { store, .. } => store.read(key, range),
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match self {
            Self::Memory(store) => store.head(key),
            Self::S3 { store, .. } => store.head(key),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        match self {
            Self::Memory(store) => store.delete(key),
            Self::S3 { store, .. } => store.delete(key),
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
        let store = config.build().unwrap();
        assert!(matches!(store, ConfiguredObjectStore::Memory(_)));
        let receipt = store.provider_admission_receipt().unwrap();
        assert!(receipt.admits_store(&store, DEFAULT_ARTIFACT_BLOCK_SIZE));
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

    #[test]
    fn python_s3_cannot_reuse_a_receipt_from_another_handle() {
        let source = MemoryArtifactStore::new();
        let foreign = admit_artifact_provider(
            &source,
            ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE).unwrap(),
        )
        .unwrap();
        let store = ConfiguredObjectStore::S3 {
            store: S3ArtifactStore::new(S3ArtifactStoreOptions::new("unused-test-bucket")).unwrap(),
            admission: Arc::new(foreign),
        };

        assert!(!store
            .provider_admission_receipt()
            .unwrap()
            .admits_store(&store, DEFAULT_ARTIFACT_BLOCK_SIZE));
    }

    fn live_s3_config() -> PythonObjectStoreConfig {
        let required = |name: &str| {
            std::env::var(name)
                .unwrap_or_else(|_| panic!("{name} is required for the live Python provider test"))
        };
        PythonObjectStoreConfig {
            options: ConfiguredObjectStoreOptions::S3(S3ArtifactStoreOptions {
                bucket: required("NOKV_TEST_S3_BUCKET"),
                root: required("NOKV_TEST_S3_ROOT"),
                region: std::env::var("NOKV_TEST_S3_REGION")
                    .unwrap_or_else(|_| "us-east-1".to_owned()),
                endpoint: Some(required("NOKV_TEST_S3_ENDPOINT")),
                access_key_id: Some(required("NOKV_TEST_S3_ACCESS_KEY_ID")),
                secret_access_key: Some(required("NOKV_TEST_S3_SECRET_ACCESS_KEY")),
                session_token: None,
                virtual_host_style: false,
                skip_signature: false,
            }),
        }
    }

    #[test]
    #[ignore = "requires an explicitly configured live S3-compatible endpoint"]
    fn live_python_s3_build_returns_an_exact_handle_bound_receipt() {
        let store = live_s3_config().build().expect("live Python S3 admission");
        let receipt = store.provider_admission_receipt().unwrap();
        assert!(receipt.admits_store(&store, DEFAULT_ARTIFACT_BLOCK_SIZE));
    }

    #[test]
    #[ignore = "requires an explicitly configured incompatible S3-compatible endpoint"]
    fn live_python_s3_build_fails_closed_with_a_redacted_admission_error() {
        let error = live_s3_config()
            .build()
            .expect_err("incompatible provider must fail before client construction");
        assert_eq!(
            error,
            ConfiguredObjectStoreBuildError::Admission(ProviderAdmissionError::Inconclusive)
        );
        let rendered = error.to_string();
        for secret in [
            std::env::var("NOKV_TEST_S3_ENDPOINT").unwrap(),
            std::env::var("NOKV_TEST_S3_BUCKET").unwrap(),
            std::env::var("NOKV_TEST_S3_ROOT").unwrap(),
        ] {
            assert!(!rendered.contains(&secret));
        }
    }
}
