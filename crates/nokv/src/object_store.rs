/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Object-provider composition for the custom Agent CLI.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use nokv_client::ClientOptions;
use nokv_object::{
    admit_artifact_provider, ensure_object_namespace, load_object_namespace,
    verify_object_namespace, ArtifactObjectStore, ArtifactStoreCapabilities,
    ImmutableCreateOutcome, LocalHotTier, LocalHotTierOptions, ObjectDeleteOutcome, ObjectError,
    ObjectInfo, ObjectKey, ObjectRange, ObjectSealOutcome, ProviderAdmissionError,
    ProviderAdmissionProfile, ProviderAdmissionReceipt, ProviderHandleIdentity, S3ArtifactStore,
    S3ArtifactStoreOptions, TieredArtifactStore, TieredArtifactStoreOptions,
    DEFAULT_ARTIFACT_BLOCK_SIZE,
};
use nokv_types::{ErrorClass, ObjectNamespaceId};

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
    /// The owner's write admission; an Agent process never records one.
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
            append_admission: Arc::new(OnceLock::new()),
            admission_probe: Arc::new(Mutex::new(())),
        })
    }

    /// Prove the single-put and monotonic-sealing semantics the owner's
    /// object writes need: the one admission `serve` and `provision` run.
    pub fn validate_append_capabilities(&self) -> Result<(), ProviderAdmissionError> {
        self.cache_admission(&self.append_admission, |store| {
            let profile = ProviderAdmissionProfile::single_put(DEFAULT_ARTIFACT_BLOCK_SIZE)?
                .with_append_sealing();
            admit_artifact_provider(store, profile)
        })
    }

    /// [`Self::validate_append_capabilities`] within a retry budget: a probe
    /// that failed before it could take effect (an unreachable or
    /// inconclusive provider) is repeated after the budget's pause; a
    /// provider that lacks a capability is not. On failure, returns the last
    /// attempt's cause and how many attempts ran.
    pub fn admit_within(
        &self,
        options: ClientOptions,
        sleep: impl FnMut(Duration),
    ) -> Result<(), (ProviderAdmissionError, u32)> {
        repeat_transient_admission(options, || self.validate_append_capabilities(), sleep)
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

    pub fn bind(mut self, expected: ObjectNamespaceId) -> Result<Self, ObjectError> {
        verify_object_namespace(self.durable(), expected)?;
        self.namespace_id = Some(expected);
        Ok(self)
    }

    /// Bind to `namespace_id` without reading its marker, for tests that
    /// must not reach the provider.
    #[cfg(test)]
    pub(crate) fn assume_namespace(mut self, namespace_id: ObjectNamespaceId) -> Self {
        self.namespace_id = Some(namespace_id);
        self
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
        self.append_admission.get()
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

fn repeat_transient_admission(
    options: ClientOptions,
    mut probe: impl FnMut() -> Result<(), ProviderAdmissionError>,
    mut sleep: impl FnMut(Duration),
) -> Result<(), (ProviderAdmissionError, u32)> {
    let mut attempt = 1;
    loop {
        match probe() {
            Ok(()) => return Ok(()),
            Err(error)
                if error.class() == ErrorClass::Transient && attempt < options.max_attempts =>
            {
                sleep(options.retry_delay(attempt));
                attempt += 1;
            }
            Err(error) => return Err((error, attempt)),
        }
    }
}

/// A configuration whose provider refuses connections, so an admission probe
/// fails at once without leaving the host: nothing listens on privileged
/// port 1, and no test can bind it.
#[cfg(test)]
pub(crate) fn refused_provider_config() -> ObjectConfig {
    ObjectConfig {
        bucket: Some("unused-test-bucket".to_owned()),
        endpoint: Some("http://127.0.0.1:1".to_owned()),
        skip_signature: true,
        ..ObjectConfig::default()
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
                .cache_admission(&store.append_admission, |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(ProviderAdmissionError::Inconclusive)
                })
                .unwrap_err();
            assert_eq!(error, ProviderAdmissionError::Inconclusive);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(store.provider_admission_receipt().is_none());
    }

    fn budget(max_attempts: u32, backoff_ms: u64) -> ClientOptions {
        ClientOptions {
            max_attempts,
            retry_backoff: Duration::from_millis(backoff_ms),
            max_retry_backoff: Duration::from_millis(2_000),
        }
    }

    #[test]
    fn a_transient_admission_failure_is_repeated_within_the_budget() {
        let mut outcomes = vec![
            Err(ProviderAdmissionError::Unavailable),
            Err(ProviderAdmissionError::Inconclusive),
            Ok(()),
        ]
        .into_iter();
        let mut pauses = Vec::new();
        let admitted = repeat_transient_admission(
            budget(3, 250),
            || outcomes.next().unwrap(),
            |pause| pauses.push(pause),
        );
        assert_eq!(admitted, Ok(()));
        assert_eq!(
            pauses,
            [Duration::from_millis(250), Duration::from_millis(500)]
        );
    }

    #[test]
    fn the_last_transient_failure_is_reported_with_its_attempt_count() {
        let mut probes = 0;
        let mut pauses = 0;
        let error = repeat_transient_admission(
            budget(4, 0),
            || {
                probes += 1;
                Err(ProviderAdmissionError::Unavailable)
            },
            |_| pauses += 1,
        )
        .unwrap_err();
        assert_eq!(error, (ProviderAdmissionError::Unavailable, 4));
        assert_eq!((probes, pauses), (4, 3));
    }

    #[test]
    fn a_provider_without_a_required_capability_is_probed_once() {
        let rejected = ProviderAdmissionError::Rejected {
            capability: nokv_object::ProviderAdmissionCapability::AtomicCreateIfAbsent,
        };
        let mut probes = 0;
        let error = repeat_transient_admission(
            budget(16, 1),
            || {
                probes += 1;
                Err(rejected)
            },
            |_| panic!("a rejected provider must not be probed again"),
        )
        .unwrap_err();
        assert_eq!(error, (rejected, 1));
        assert_eq!(error.0.class(), ErrorClass::OperatorRequired);
        assert_eq!(probes, 1);
    }

    #[test]
    fn a_refusing_provider_spends_the_budget_and_stays_unadmitted() {
        let store = CliObjectStore::build(&refused_provider_config()).unwrap();
        let mut pauses = 0;
        let (cause, attempts) = store
            .admit_within(budget(3, 0), |_| pauses += 1)
            .unwrap_err();
        assert_eq!(cause.class(), ErrorClass::Transient);
        assert_eq!((attempts, pauses), (3, 2));
        assert!(store.provider_admission_receipt().is_none());
    }

    #[test]
    fn a_refusing_provider_fails_the_owner_admission_typed_and_keeps_no_receipt() {
        let store = CliObjectStore::build(&refused_provider_config()).unwrap();
        for _ in 0..2 {
            let cause = store.validate_append_capabilities().unwrap_err();
            assert_eq!(cause.class(), ErrorClass::Transient);
            assert!(store.provider_admission_receipt().is_none());
        }
    }
}
