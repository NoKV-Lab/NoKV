/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Object-provider composition for the custom Agent CLI.

use std::sync::{Arc, Mutex, OnceLock};

use nokv_object::{
    admit_artifact_provider, ensure_object_namespace, load_object_namespace,
    verify_object_namespace, ArtifactObjectStore, ArtifactStoreCapabilities,
    ImmutableCreateOutcome, LocalHotTier, LocalHotTierOptions, ObjectDeleteOutcome, ObjectError,
    ObjectInfo, ObjectKey, ObjectRange, ObjectSealOutcome, ProviderAdmissionError,
    ProviderAdmissionProfile, ProviderAdmissionReceipt, ProviderHandleIdentity, S3ArtifactStore,
    S3ArtifactStoreOptions, TieredArtifactStore, TieredArtifactStoreOptions,
    DEFAULT_ARTIFACT_BLOCK_SIZE,
};
use nokv_types::ObjectNamespaceId;

use super::cli::ObjectConfig;

type CachedS3Store = TieredArtifactStore<LocalHotTier, S3ArtifactStore>;

#[derive(Clone, Debug)]
enum CliObjectStoreInner {
    S3(S3ArtifactStore),
    CachedS3(CachedS3Store),
}

#[derive(Clone, Debug)]
pub struct CliObjectStore {
    inner: CliObjectStoreInner,
    namespace_id: Option<ObjectNamespaceId>,
    admission: Arc<OnceLock<ProviderAdmissionReceipt>>,
    append_admission: Arc<OnceLock<ProviderAdmissionReceipt>>,
    admission_probe: Arc<Mutex<()>>,
}

impl CliObjectStore {
    pub fn build(config: &ObjectConfig) -> Result<Self, String> {
        let bucket = config
            .bucket
            .clone()
            .filter(|bucket| !bucket.is_empty())
            .ok_or_else(|| "--object-bucket is required for artifact operations".to_owned())?;
        if config.access_key_id.is_some() != config.secret_access_key.is_some() {
            return Err(
                "--object-access-key-id and --object-secret-access-key must be set together"
                    .to_owned(),
            );
        }
        if config.session_token.is_some() && config.access_key_id.is_none() {
            return Err("--object-session-token requires object access and secret keys".to_owned());
        }
        match (&config.hot_cache_dir, config.hot_cache_bytes) {
            (None, 0) | (Some(_), 1..) => {}
            (Some(_), 0) => {
                return Err(
                    "--hot-cache-bytes must be positive when a cache directory is set".to_owned(),
                );
            }
            (None, _) => {
                return Err(
                    "--hot-cache-dir is required when --hot-cache-bytes is positive".to_owned(),
                );
            }
        }

        let durable = S3ArtifactStore::new(S3ArtifactStoreOptions {
            bucket,
            root: config.root.clone(),
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
            access_key_id: config.access_key_id.clone(),
            secret_access_key: config.secret_access_key.clone(),
            session_token: config.session_token.clone(),
            virtual_host_style: config.virtual_host_style,
            skip_signature: config.skip_signature,
        })
        .map_err(|error| error.to_string())?;

        let Some(cache_root) = &config.hot_cache_dir else {
            return Ok(Self {
                inner: CliObjectStoreInner::S3(durable),
                namespace_id: None,
                admission: Arc::new(OnceLock::new()),
                append_admission: Arc::new(OnceLock::new()),
                admission_probe: Arc::new(Mutex::new(())),
            });
        };
        let hot = LocalHotTier::new(LocalHotTierOptions::new(cache_root, config.hot_cache_bytes))
            .map_err(|error| error.to_string())?;
        Ok(Self {
            inner: CliObjectStoreInner::CachedS3(TieredArtifactStore::new(
                hot,
                durable,
                TieredArtifactStoreOptions::default(),
            )),
            namespace_id: None,
            admission: Arc::new(OnceLock::new()),
            append_admission: Arc::new(OnceLock::new()),
            admission_probe: Arc::new(Mutex::new(())),
        })
    }

    /// Verify the immutable object semantics required by every Agent tool
    /// before the CLI advertises its MCP surface.
    pub fn validate_agent_capabilities(&self) -> Result<(), String> {
        self.cache_admission(&self.admission, |store| {
            let profile = ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE)?;
            admit_artifact_provider(store, profile)
        })
        .map_err(|error| error.to_string())
    }

    /// Prove monotonic sealing before admitting stable append uploads.
    pub fn validate_append_capabilities(&self) -> Result<(), ProviderAdmissionError> {
        self.cache_admission(&self.append_admission, |store| {
            let profile = ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE)?
                .with_append_sealing();
            admit_artifact_provider(store, profile)
        })
    }

    fn cache_admission(
        &self,
        admission: &OnceLock<ProviderAdmissionReceipt>,
        probe: impl FnOnce(&S3ArtifactStore) -> Result<ProviderAdmissionReceipt, ProviderAdmissionError>,
    ) -> Result<(), ProviderAdmissionError> {
        if admission.get().is_some() {
            return Ok(());
        }
        let _guard = self
            .admission_probe
            .lock()
            .map_err(|_| ProviderAdmissionError::Inconclusive)?;
        if admission.get().is_none() {
            // Cache evidence of success, never a transient outage. A caller
            // using this same handle may safely try the probe again later.
            let receipt = probe(self.durable())?;
            admission.get_or_init(|| receipt);
        }
        Ok(())
    }

    pub fn bind(mut self, expected: ObjectNamespaceId) -> Result<Self, String> {
        verify_object_namespace(self.durable(), expected).map_err(|error| error.to_string())?;
        self.namespace_id = Some(expected);
        Ok(self)
    }

    pub fn load_namespace(&self) -> Result<Option<ObjectNamespaceId>, String> {
        load_object_namespace(self.durable()).map_err(|error| error.to_string())
    }

    pub fn ensure_namespace(&self, namespace_id: ObjectNamespaceId) -> Result<(), String> {
        ensure_object_namespace(self.durable(), namespace_id)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn durable(&self) -> &S3ArtifactStore {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store,
            CliObjectStoreInner::CachedS3(store) => store.durable(),
        }
    }
}

impl ArtifactObjectStore for CliObjectStore {
    fn object_namespace(&self) -> Option<ObjectNamespaceId> {
        self.namespace_id
    }

    fn capabilities(&self) -> ArtifactStoreCapabilities {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store.capabilities(),
            CliObjectStoreInner::CachedS3(store) => store.capabilities(),
        }
    }

    fn provider_handle_identity(&self) -> ProviderHandleIdentity {
        self.durable().provider_handle_identity()
    }

    fn provider_admission_receipt(&self) -> Option<&ProviderAdmissionReceipt> {
        self.append_admission.get().or_else(|| self.admission.get())
    }

    fn create_immutable(
        &self,
        key: &ObjectKey,
        bytes: &[u8],
    ) -> Result<ImmutableCreateOutcome, ObjectError> {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store.create_immutable(key, bytes),
            CliObjectStoreInner::CachedS3(store) => store.create_immutable(key, bytes),
        }
    }

    fn seal_immutable(&self, key: &ObjectKey) -> Result<ObjectSealOutcome, ObjectError> {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store.seal_immutable(key),
            CliObjectStoreInner::CachedS3(store) => store.seal_immutable(key),
        }
    }

    fn read(&self, key: &ObjectKey, range: Option<ObjectRange>) -> Result<Vec<u8>, ObjectError> {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store.read(key, range),
            CliObjectStoreInner::CachedS3(store) => store.read(key, range),
        }
    }

    fn head(&self, key: &ObjectKey) -> Result<Option<ObjectInfo>, ObjectError> {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store.head(key),
            CliObjectStoreInner::CachedS3(store) => store.head(key),
        }
    }

    fn delete(&self, key: &ObjectKey) -> Result<ObjectDeleteOutcome, ObjectError> {
        match &self.inner {
            CliObjectStoreInner::S3(store) => store.delete(key),
            CliObjectStoreInner::CachedS3(store) => store.delete(key),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn durable_bucket_is_required() {
        let error = CliObjectStore::build(&ObjectConfig::default()).unwrap_err();
        assert!(error.contains("--object-bucket"));
    }

    #[test]
    fn partial_credentials_fail_closed_before_provider_construction() {
        let config = ObjectConfig {
            bucket: Some("artifacts".to_owned()),
            access_key_id: Some("access".to_owned()),
            ..ObjectConfig::default()
        };
        let error = CliObjectStore::build(&config).unwrap_err();
        assert!(error.contains("must be set together"));
    }

    #[test]
    fn hot_cache_configuration_requires_both_path_and_capacity() {
        let config = ObjectConfig {
            bucket: Some("artifacts".to_owned()),
            hot_cache_bytes: 1024,
            ..ObjectConfig::default()
        };
        let error = CliObjectStore::build(&config).unwrap_err();
        assert!(error.contains("--hot-cache-dir"));
    }

    #[test]
    fn configured_s3_is_not_admitted_by_static_capability_flags() {
        let config = ObjectConfig {
            bucket: Some("artifacts".to_owned()),
            ..ObjectConfig::default()
        };
        let store = CliObjectStore::build(&config).unwrap();
        assert!(store.capabilities().atomic_create_if_absent);
        assert!(store.capabilities().range_read);
        assert!(store.provider_admission_receipt().is_none());
    }

    #[test]
    fn one_provider_handle_retries_admission_after_a_failed_probe() {
        let config = ObjectConfig {
            bucket: Some("artifacts".to_owned()),
            ..ObjectConfig::default()
        };
        let store = CliObjectStore::build(&config).unwrap();
        let calls = AtomicUsize::new(0);

        for _ in 0..2 {
            let error = store
                .cache_admission(&store.admission, |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(ProviderAdmissionError::Inconclusive)
                })
                .unwrap_err();
            assert_eq!(error, ProviderAdmissionError::Inconclusive);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(store.provider_admission_receipt().is_none());
    }
}
