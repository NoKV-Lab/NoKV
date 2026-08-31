/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Environment-gated FoundationDB seed-discovery qualification.
//!
//! This module owns only live orchestration and retained evidence. Product
//! routing remains in `nokv-client`, and metadata authority remains behind the
//! exact candidate `nokv-fdb` process.

mod evidence;
mod orchestrator;
mod peer;
mod scenario;

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

pub use orchestrator::run;

pub const LIVE_GATE_ENV: &str = "NOKV_FDB_SEED_QUALIFICATION";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QualificationOptions {
    pub candidate_binary: PathBuf,
    pub fdb_cluster_file: PathBuf,
    pub fdb_prefix_base: String,
    pub fdbcli: PathBuf,
    pub curl: PathBuf,
    pub object_endpoint: String,
    pub object_bucket: String,
    pub object_region: String,
    pub object_root_base: String,
    pub object_access_key_id: String,
    pub object_secret_access_key: String,
    pub rustfs_service_identity: String,
    pub rustfs_health_url: String,
    pub owner_a_endpoint: SocketAddr,
    pub owner_b_backend_endpoint: SocketAddr,
    pub owner_b_endpoint: SocketAddr,
    pub seed_peer_endpoint: SocketAddr,
    pub failed_seed_endpoint: SocketAddr,
    pub evidence_dir: PathBuf,
    pub source_revision: String,
    pub source_dirty: bool,
    pub takeover_timeout: Duration,
    pub operation_timeout: Duration,
}

impl QualificationOptions {
    pub fn parse(arguments: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut arguments = arguments.peekable();
        let mut candidate_binary = None;
        let mut fdb_cluster_file = None;
        let mut fdb_prefix_base = None;
        let mut fdbcli = None;
        let mut curl = None;
        let mut object_endpoint = None;
        let mut object_bucket = None;
        let mut object_region = Some("us-east-1".to_owned());
        let mut object_root_base = None;
        let mut object_access_key_id = None;
        let mut object_secret_access_key = None;
        let mut rustfs_service_identity = None;
        let mut rustfs_health_url = None;
        let mut owner_a_endpoint = None;
        let mut owner_b_backend_endpoint = None;
        let mut owner_b_endpoint = None;
        let mut seed_peer_endpoint = None;
        let mut failed_seed_endpoint = None;
        let mut evidence_dir = None;
        let mut source_revision = None;
        let mut source_dirty = None;
        let mut takeover_timeout = Some(Duration::from_secs(45));
        let mut operation_timeout = Some(Duration::from_secs(20));

        while let Some(flag) = arguments.next() {
            let value = next_value(&mut arguments, &flag)?;
            match flag.as_str() {
                "--candidate-binary" => candidate_binary = Some(PathBuf::from(value)),
                "--fdb-cluster-file" => fdb_cluster_file = Some(PathBuf::from(value)),
                "--fdb-prefix-base" => fdb_prefix_base = Some(value),
                "--fdbcli" => fdbcli = Some(PathBuf::from(value)),
                "--curl" => curl = Some(PathBuf::from(value)),
                "--object-endpoint" => object_endpoint = Some(value),
                "--object-bucket" => object_bucket = Some(value),
                "--object-region" => object_region = Some(value),
                "--object-root-base" => object_root_base = Some(value),
                "--object-access-key-id" => object_access_key_id = Some(value),
                "--object-secret-access-key" => object_secret_access_key = Some(value),
                "--rustfs-service-identity" => rustfs_service_identity = Some(value),
                "--rustfs-health-url" => rustfs_health_url = Some(value),
                "--owner-a-endpoint" => owner_a_endpoint = Some(parse_endpoint(&flag, value)?),
                "--owner-b-backend-endpoint" => {
                    owner_b_backend_endpoint = Some(parse_endpoint(&flag, value)?)
                }
                "--owner-b-endpoint" => owner_b_endpoint = Some(parse_endpoint(&flag, value)?),
                "--seed-peer-endpoint" => seed_peer_endpoint = Some(parse_endpoint(&flag, value)?),
                "--failed-seed-endpoint" => {
                    failed_seed_endpoint = Some(parse_endpoint(&flag, value)?)
                }
                "--evidence-dir" => evidence_dir = Some(PathBuf::from(value)),
                "--source-revision" => source_revision = Some(value),
                "--source-dirty" => source_dirty = Some(parse_bool(&flag, &value)?),
                "--takeover-timeout-seconds" => {
                    takeover_timeout = Some(parse_duration(&flag, &value)?)
                }
                "--operation-timeout-seconds" => {
                    operation_timeout = Some(parse_duration(&flag, &value)?)
                }
                _ => {
                    return Err(format!(
                        "unknown qualification option {flag:?}\n{}",
                        usage()
                    ))
                }
            }
        }

        let options = Self {
            candidate_binary: required(candidate_binary, "--candidate-binary")?,
            fdb_cluster_file: required(fdb_cluster_file, "--fdb-cluster-file")?,
            fdb_prefix_base: required(fdb_prefix_base, "--fdb-prefix-base")?,
            fdbcli: required(fdbcli, "--fdbcli")?,
            curl: required(curl, "--curl")?,
            object_endpoint: required(object_endpoint, "--object-endpoint")?,
            object_bucket: required(object_bucket, "--object-bucket")?,
            object_region: required(object_region, "--object-region")?,
            object_root_base: required(object_root_base, "--object-root-base")?,
            object_access_key_id: required(object_access_key_id, "--object-access-key-id")?,
            object_secret_access_key: required(
                object_secret_access_key,
                "--object-secret-access-key",
            )?,
            rustfs_service_identity: required(
                rustfs_service_identity,
                "--rustfs-service-identity",
            )?,
            rustfs_health_url: required(rustfs_health_url, "--rustfs-health-url")?,
            owner_a_endpoint: required(owner_a_endpoint, "--owner-a-endpoint")?,
            owner_b_backend_endpoint: required(
                owner_b_backend_endpoint,
                "--owner-b-backend-endpoint",
            )?,
            owner_b_endpoint: required(owner_b_endpoint, "--owner-b-endpoint")?,
            seed_peer_endpoint: required(seed_peer_endpoint, "--seed-peer-endpoint")?,
            failed_seed_endpoint: required(failed_seed_endpoint, "--failed-seed-endpoint")?,
            evidence_dir: required(evidence_dir, "--evidence-dir")?,
            source_revision: required(source_revision, "--source-revision")?,
            source_dirty: required(source_dirty, "--source-dirty")?,
            takeover_timeout: required(takeover_timeout, "--takeover-timeout-seconds")?,
            operation_timeout: required(operation_timeout, "--operation-timeout-seconds")?,
        };
        options.validate()?;
        Ok(options)
    }

    fn validate(&self) -> Result<(), String> {
        for (flag, path) in [
            ("--candidate-binary", &self.candidate_binary),
            ("--fdb-cluster-file", &self.fdb_cluster_file),
            ("--fdbcli", &self.fdbcli),
            ("--curl", &self.curl),
            ("--evidence-dir", &self.evidence_dir),
        ] {
            if !path.is_absolute() {
                return Err(format!("{flag} must be an absolute path"));
            }
        }
        if !self.candidate_binary.is_file() {
            return Err("--candidate-binary must name an existing file".to_owned());
        }
        if !self.fdb_cluster_file.is_file() {
            return Err("--fdb-cluster-file must name an existing file".to_owned());
        }
        if !self.fdbcli.is_file() {
            return Err("--fdbcli must name an existing file".to_owned());
        }
        if !self.curl.is_file() {
            return Err("--curl must name an existing file".to_owned());
        }
        if self.evidence_dir.exists() {
            return Err("--evidence-dir must not already exist".to_owned());
        }
        for (flag, value) in [
            ("--fdb-prefix-base", self.fdb_prefix_base.as_str()),
            ("--object-endpoint", self.object_endpoint.as_str()),
            ("--object-bucket", self.object_bucket.as_str()),
            ("--object-region", self.object_region.as_str()),
            ("--object-root-base", self.object_root_base.as_str()),
            ("--source-revision", self.source_revision.as_str()),
            (
                "--rustfs-service-identity",
                self.rustfs_service_identity.as_str(),
            ),
            ("--rustfs-health-url", self.rustfs_health_url.as_str()),
        ] {
            if value.trim().is_empty() || value.trim() != value {
                return Err(format!(
                    "{flag} must be non-empty without surrounding space"
                ));
            }
        }
        if !self
            .fdb_prefix_base
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(
                "--fdb-prefix-base must contain only ASCII letters, digits, '-' or '_'".to_owned(),
            );
        }
        if self.fdb_prefix_base.len() > 40 {
            return Err("--fdb-prefix-base must not exceed 40 bytes".to_owned());
        }
        let health = url::Url::parse(&self.rustfs_health_url)
            .map_err(|error| format!("--rustfs-health-url is invalid: {error}"))?;
        if !matches!(health.scheme(), "http" | "https")
            || health.host_str().is_none()
            || !health.username().is_empty()
            || health.password().is_some()
            || health.query().is_some()
            || health.fragment().is_some()
        {
            return Err(
                "--rustfs-health-url must be an HTTP(S) URL without credentials, query, or fragment"
                    .to_owned(),
            );
        }
        let object_endpoint = url::Url::parse(&self.object_endpoint)
            .map_err(|error| format!("--object-endpoint is invalid: {error}"))?;
        if !matches!(object_endpoint.scheme(), "http" | "https")
            || object_endpoint.host_str().is_none()
            || !object_endpoint.username().is_empty()
            || object_endpoint.password().is_some()
        {
            return Err(
                "--object-endpoint must be an HTTP(S) URL without embedded credentials".to_owned(),
            );
        }
        if self.source_revision.len() != 40
            || !self
                .source_revision
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("--source-revision must be 40 lowercase hexadecimal characters".to_owned());
        }
        let endpoints = [
            self.owner_a_endpoint,
            self.owner_b_backend_endpoint,
            self.owner_b_endpoint,
            self.seed_peer_endpoint,
            self.failed_seed_endpoint,
        ];
        if endpoints
            .iter()
            .any(|endpoint| endpoint.ip().is_unspecified() || endpoint.port() == 0)
        {
            return Err(
                "qualification endpoints must be connectable with nonzero ports".to_owned(),
            );
        }
        if endpoints.iter().copied().collect::<BTreeSet<_>>().len() != endpoints.len() {
            return Err("qualification endpoints must be pairwise distinct".to_owned());
        }
        Ok(())
    }
}

fn required<T>(value: Option<T>, flag: &'static str) -> Result<T, String> {
    value.ok_or_else(|| format!("missing required option {flag}\n{}", usage()))
}

fn next_value(arguments: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("{flag} requires a value\n{}", usage()))
}

fn parse_endpoint(flag: &str, value: String) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} must be a socket address"))
}

fn parse_duration(flag: &str, value: &str) -> Result<Duration, String> {
    let seconds = value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be an unsigned integer"))?;
    if !(1..=300).contains(&seconds) {
        return Err(format!("{flag} must be within 1..=300"));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_bool(flag: &str, value: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{flag} must be true or false")),
    }
}

pub fn usage() -> &'static str {
    "usage: nokv-fdb-seed-qualification \
     --candidate-binary ABSOLUTE_PATH --fdb-cluster-file ABSOLUTE_PATH \
     --fdb-prefix-base NAME --fdbcli ABSOLUTE_PATH --curl ABSOLUTE_PATH \
     --object-endpoint URL --object-bucket NAME [--object-region NAME] \
     --object-root-base NAME --object-access-key-id VALUE \
     --object-secret-access-key VALUE --rustfs-service-identity VALUE \
     --rustfs-health-url URL \
     --owner-a-endpoint IP:PORT --owner-b-backend-endpoint IP:PORT \
     --owner-b-endpoint IP:PORT --seed-peer-endpoint IP:PORT \
     --failed-seed-endpoint IP:PORT --evidence-dir ABSOLUTE_PATH \
     --source-revision COMMIT --source-dirty true|false \
     [--takeover-timeout-seconds N] [--operation-timeout-seconds N]"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unknown_options_before_live_work() {
        let error = QualificationOptions::parse(["--wat".to_owned(), "x".to_owned()].into_iter())
            .unwrap_err();
        assert!(error.contains("unknown qualification option"));
    }

    #[test]
    fn duration_is_bounded() {
        assert!(parse_duration("--timeout", "0").is_err());
        assert_eq!(
            parse_duration("--timeout", "45").unwrap(),
            Duration::from_secs(45)
        );
        assert!(parse_duration("--timeout", "301").is_err());
    }
}
