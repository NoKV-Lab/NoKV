/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::path::{Path, PathBuf};
use std::time::Duration;

use nokv_control::{
    ControlError, StoreManifest, StoreProvider, PROVIDER_NAMESPACE_DIGEST_BYTES,
    SUPPORTED_WORKSPACE_FORMAT_VERSION,
};
use nokv_fdb::{FdbConnectionOptions, FdbStorePrefix, FDB_PHYSICAL_ENCODING_VERSION};
use sha2::{Digest, Sha256};

const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(10);
const MIN_LEASE_TTL: Duration = Duration::from_millis(1);
const MAX_LEASE_TTL: Duration = Duration::from_secs(300);
const PREFIX_DIGEST_DOMAIN: &[u8] = b"nokv/fdb/provider-namespace/v1\0";

/// Physical connection, isolation prefix, and local observation policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FdbControlOptions {
    connection: FdbConnectionOptions,
    store_prefix: FdbStorePrefix,
    lease_ttl: Duration,
}

impl FdbControlOptions {
    pub fn new(
        cluster_file: impl Into<PathBuf>,
        store_prefix: impl AsRef<str>,
    ) -> Result<Self, ControlError> {
        let store_prefix = store_prefix.as_ref();
        let options = Self {
            connection: FdbConnectionOptions::new(cluster_file),
            store_prefix: FdbStorePrefix::new(store_prefix.as_bytes())
                .map_err(|error| ControlError::InvalidOptions(error.to_string()))?,
            lease_ttl: DEFAULT_LEASE_TTL,
        };
        options.validate()?;
        Ok(options)
    }

    pub fn with_transaction_timeout(mut self, timeout: Duration) -> Result<Self, ControlError> {
        self.connection = self.connection.with_transaction_timeout(timeout);
        self.validate()?;
        Ok(self)
    }

    pub fn with_lease_ttl(mut self, lease_ttl: Duration) -> Result<Self, ControlError> {
        self.lease_ttl = lease_ttl;
        self.validate()?;
        Ok(self)
    }

    pub fn cluster_file(&self) -> &Path {
        self.connection.cluster_file()
    }

    pub fn store_prefix(&self) -> &[u8] {
        self.store_prefix.token()
    }

    pub const fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    pub fn provider_namespace_digest(&self) -> [u8; PROVIDER_NAMESPACE_DIGEST_BYTES] {
        let mut hasher = Sha256::new();
        hasher.update(PREFIX_DIGEST_DOMAIN);
        hasher.update(self.store_prefix.token());
        hasher.finalize().into()
    }

    pub fn validate_manifest_binding(&self, manifest: &StoreManifest) -> Result<(), ControlError> {
        if manifest.provider() != StoreProvider::FoundationDb {
            return Err(ControlError::InvalidOptions(
                "FoundationDB control requires a FoundationDb store manifest".to_owned(),
            ));
        }
        if manifest.workspace_format_version() != SUPPORTED_WORKSPACE_FORMAT_VERSION {
            return Err(ControlError::InvalidOptions(format!(
                "FoundationDB control supports workspace format {}, manifest declares {}",
                SUPPORTED_WORKSPACE_FORMAT_VERSION,
                manifest.workspace_format_version()
            )));
        }
        if manifest.physical_encoding_version() != FDB_PHYSICAL_ENCODING_VERSION {
            return Err(ControlError::InvalidOptions(format!(
                "FoundationDB physical encoding version is {}, manifest declares {}",
                FDB_PHYSICAL_ENCODING_VERSION,
                manifest.physical_encoding_version()
            )));
        }
        if manifest.provider_namespace_digest() != &self.provider_namespace_digest() {
            return Err(ControlError::InvalidOptions(
                "FoundationDB store manifest does not bind the selected prefix".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate(&self) -> Result<(), ControlError> {
        self.connection
            .validate()
            .map_err(|error| ControlError::InvalidOptions(error.to_string()))?;
        if self.lease_ttl < MIN_LEASE_TTL || self.lease_ttl > MAX_LEASE_TTL {
            return Err(ControlError::InvalidOptions(format!(
                "FoundationDB owner lease TTL {}ms is outside {}..={}ms",
                self.lease_ttl.as_millis(),
                MIN_LEASE_TTL.as_millis(),
                MAX_LEASE_TTL.as_millis()
            )));
        }
        Ok(())
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn connection(&self) -> &FdbConnectionOptions {
        &self.connection
    }

    #[cfg(feature = "fdb")]
    pub(crate) fn physical_prefix(&self) -> &FdbStorePrefix {
        &self.store_prefix
    }
}
