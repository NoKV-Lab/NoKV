/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Process, provider, and current-session support shared by live FDB Gates 8-10.

use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use nokv_control::{DistributedControlStore, OwnershipSnapshot, ShardRouteState};
use nokv_control_fdb::{FdbControlOptions, FdbControlStore, FdbSessionFence};
use nokv_fdb::FdbRuntime;
use nokv_meta::workspace::MetaShard;
use nokv_meta_fdb::{FdbMetadataSessionFence, FdbOptions, FdbStore};
use nokv_protocol::{RootIdentity, RootRoute};
use nokv_types::RootId;
use serde::Serialize;
use serde_json::Value;
use url::Url;

use crate::qualification_runtime::{lowercase_hex, EvidenceBundle, ProcessSet};

#[derive(Clone, Debug)]
pub(crate) struct ObjectProviderOptions {
    pub(crate) endpoint: String,
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) root: String,
    pub(crate) access_key_id: String,
    pub(crate) secret_access_key: String,
}

#[derive(Clone, Debug)]
pub(crate) struct HealthOptions {
    pub(crate) fdbcli: PathBuf,
    pub(crate) fdb_cluster_file: PathBuf,
    pub(crate) curl: PathBuf,
    pub(crate) rustfs_health_url: String,
}

#[derive(Serialize)]
struct CommandStatus {
    success: bool,
    code: Option<i32>,
}

pub(crate) fn metadata_url(cluster_file: &Path, prefix: &str) -> Result<String, String> {
    let cluster_file = cluster_file
        .to_str()
        .ok_or_else(|| "FDB cluster-file path must be valid UTF-8".to_owned())?;
    let mut url = Url::parse("fdb:///").expect("static FDB URL is valid");
    url.set_path(cluster_file);
    url.query_pairs_mut().append_pair("prefix", prefix);
    Ok(url.to_string())
}

pub(crate) fn append_object_arguments(command: &mut Command, options: &ObjectProviderOptions) {
    command
        .args(["--object-endpoint", &options.endpoint])
        .args(["--object-bucket", &options.bucket])
        .args(["--object-region", &options.region])
        .args(["--object-root", &options.root])
        .args(["--object-access-key-id", &options.access_key_id])
        .args(["--object-secret-access-key", &options.secret_access_key]);
}

pub(crate) fn append_client_arguments(
    command: &mut Command,
    root: RootIdentity,
    seed: SocketAddr,
    workbench_root: &str,
    objects: &ObjectProviderOptions,
) {
    command
        .args(["--root-id", &lowercase_hex(&root.0)])
        .args(["--seed", &seed.to_string()])
        .args(["--workbench-root", workbench_root]);
    append_object_arguments(command, objects);
}

pub(crate) fn run_checked_command(
    evidence: &EvidenceBundle,
    name: &str,
    command: &mut Command,
) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("cannot run qualification command {name:?}: {error}"))?;
    evidence.write_bytes(format!("commands/{name}.stdout"), &output.stdout)?;
    evidence.write_bytes(format!("commands/{name}.stderr"), &output.stderr)?;
    evidence.write_json(
        format!("commands/{name}.status.json"),
        &CommandStatus {
            success: output.status.success(),
            code: output.status.code(),
        },
    )?;
    if !output.status.success() {
        return Err(format!(
            "qualification command {name:?} exited with {:?}",
            output.status.code()
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| format!("qualification command {name:?} stdout is not UTF-8"))
}

pub(crate) fn command_stdout(command: &mut Command) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("cannot run environment command: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "environment command exited with {:?}",
            output.status.code()
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|_| "environment command stdout is not UTF-8".to_owned())
}

pub(crate) fn capture_health(
    evidence: &EvidenceBundle,
    options: &HealthOptions,
    label: &str,
) -> Result<(), String> {
    let mut fdb = Command::new(&options.fdbcli);
    fdb.args(["-C"])
        .arg(&options.fdb_cluster_file)
        .args(["--exec", "status json"]);
    let output = run_checked_command(evidence, &format!("{label}-fdb"), &mut fdb)?;
    let status: Value = serde_json::from_str(&output)
        .map_err(|error| format!("FoundationDB status is not JSON: {error}"))?;
    if status
        .pointer("/client/database_status/available")
        .and_then(Value::as_bool)
        != Some(true)
        || status
            .pointer("/client/database_status/healthy")
            .and_then(Value::as_bool)
            != Some(true)
    {
        return Err("FoundationDB does not report healthy and available".to_owned());
    }

    let mut rustfs = Command::new(&options.curl);
    rustfs.args([
        "--silent",
        "--show-error",
        "--fail",
        "--output",
        "/dev/null",
        "--write-out",
        "%{http_code}\n",
        &options.rustfs_health_url,
    ]);
    let output = run_checked_command(evidence, &format!("{label}-rustfs"), &mut rustfs)?;
    if output.trim() != "200" {
        return Err(format!(
            "RustFS health endpoint returned unexpected status {:?}",
            output.trim()
        ));
    }
    Ok(())
}

pub(crate) fn require_unused_endpoint(endpoint: SocketAddr) -> Result<(), String> {
    match TcpStream::connect_timeout(&endpoint, Duration::from_millis(100)) {
        Ok(_) => Err(format!(
            "qualification endpoint {endpoint} unexpectedly accepts connections"
        )),
        Err(_) => Ok(()),
    }
}

pub(crate) struct LiveControl {
    runtime: FdbRuntime,
    store: FdbControlStore,
    root: nokv_control::RootCatalogEntry,
    cluster_file: PathBuf,
    prefix: String,
}

impl LiveControl {
    pub(crate) fn open(
        cluster_file: &Path,
        prefix: &str,
        root_id: RootIdentity,
    ) -> Result<Self, String> {
        let runtime = FdbRuntime::start().map_err(|error| error.to_string())?;
        let options =
            FdbControlOptions::new(cluster_file, prefix).map_err(|error| error.to_string())?;
        let manifest = FdbControlStore::inspect_manifest(&runtime, &options)
            .map_err(|error| error.to_string())?;
        let store = FdbControlStore::open(&runtime, options, manifest)
            .map_err(|error| error.to_string())?;
        let root = store
            .get_root_catalog(&RootId::from_bytes(root_id.0))
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "provisioned root is absent from the FDB catalog".to_owned())?;
        Ok(Self {
            runtime,
            store,
            root,
            cluster_file: cluster_file.to_path_buf(),
            prefix: prefix.to_owned(),
        })
    }

    pub(crate) fn observe(&self) -> Result<OwnershipSnapshot, String> {
        self.store
            .observe_ownership(&self.root.logical_shard_id())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn wait_for_serving(
        &self,
        endpoint: SocketAddr,
        timeout: Duration,
        process: &str,
        processes: &mut ProcessSet,
    ) -> Result<OwnershipSnapshot, String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| "route polling deadline overflowed".to_owned())?;
        let mut last = "no route observation was made".to_owned();
        while Instant::now() < deadline {
            processes.require_running(process)?;
            match self.observe() {
                Ok(snapshot)
                    if snapshot.route().state() == ShardRouteState::Serving
                        && snapshot.route().endpoint().map(|value| value.as_str())
                            == Some(endpoint.to_string().as_str()) =>
                {
                    return Ok(snapshot)
                }
                Ok(snapshot) => last = format!("last ownership was {snapshot:?}"),
                Err(error) => last = error,
            }
            thread::sleep(Duration::from_millis(25));
        }
        Err(format!("timed out waiting for Serving route: {last}"))
    }

    pub(crate) fn root_route(
        &self,
        root_id: RootIdentity,
        snapshot: &OwnershipSnapshot,
    ) -> Result<RootRoute, String> {
        let route = snapshot.route();
        Ok(RootRoute {
            root_id,
            logical_shard_id: route.logical_shard_id().into(),
            object_namespace_id: self.root.object_namespace_id().into(),
            placement_generation: self.root.placement_generation().get(),
            owner_epoch: route
                .owner_epoch()
                .ok_or_else(|| "serving route has no owner epoch".to_owned())?
                .get(),
        })
    }

    pub(crate) fn open_meta(&self) -> Result<Arc<MetaShard>, String> {
        let snapshot = self.observe()?;
        let session = snapshot
            .session()
            .cloned()
            .ok_or_else(|| "current ownership has no stable session".to_owned())?;
        let fence = FdbSessionFence::new(self.store.keys(), session.clone())
            .map_err(|error| error.to_string())?;
        let fence = FdbMetadataSessionFence::new(
            fence.key(),
            fence.expected_value(),
            session.owner_epoch().get(),
            session.session_generation().get(),
        )
        .map_err(|error| error.to_string())?;
        let store = FdbStore::open(
            &self.runtime,
            FdbOptions::new(&self.cluster_file, self.prefix.as_bytes().to_vec(), fence),
        )
        .map_err(|error| error.to_string())?;
        MetaShard::open(Arc::new(store), self.root.logical_shard_id())
            .map(Arc::new)
            .map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_url_percent_encodes_path_and_prefix() {
        let url = metadata_url(Path::new("/tmp/fdb cluster"), "gate 8").unwrap();
        assert_eq!(url, "fdb:///tmp/fdb%20cluster?prefix=gate+8");
    }
}
