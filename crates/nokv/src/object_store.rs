/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Object-provider composition for the custom Agent CLI.

use nokv_object::{
    ensure_object_namespace, load_object_namespace, verify_object_namespace, ArtifactObjectStore,
    ArtifactStoreCapabilities, ImmutableCreateOutcome, LocalHotTier, LocalHotTierOptions,
    ObjectDeleteOutcome, ObjectError, ObjectInfo, ObjectKey, ObjectRange, S3ArtifactStore,
    S3ArtifactStoreOptions, TieredArtifactStore, TieredArtifactStoreOptions,
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
        })
    }

    /// Verify the immutable object semantics required by every Agent tool
    /// before the CLI advertises its MCP surface.
    pub fn validate_agent_capabilities(&self) -> Result<(), String> {
        let capabilities = self.capabilities();
        if !capabilities.atomic_create_if_absent {
            return Err(
                "object provider does not support atomic immutable create-if-absent".to_owned(),
            );
        }
        if !capabilities.range_read {
            return Err("object provider does not support verified range reads".to_owned());
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
    fn configured_s3_exposes_required_agent_capabilities() {
        let config = ObjectConfig {
            bucket: Some("artifacts".to_owned()),
            ..ObjectConfig::default()
        };
        CliObjectStore::build(&config)
            .unwrap()
            .validate_agent_capabilities()
            .unwrap();
    }
}
