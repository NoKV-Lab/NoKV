/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Real-etcd fault driver for the local-WAL recovery qualification workload.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nokv_control::{
    ControlStore, EtcdControlStore, EtcdControlStoreOptions, LogicalShardId, NodeId, OwnerEpoch,
};
use nokv_meta::workspace as meta;
use nokv_meta_holt::{HoltOptions, HoltStore, TreeBinding};
use nokv_meta_store::TxnStore;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum CrashStage {
    BeforeLocalFence,
    AfterLocalFence,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    etcd_endpoints: Vec<String>,
    etcd_key_prefix: String,
    lease_ttl_seconds: i64,
    logical_shard_id: String,
    metadata_path: PathBuf,
    previous_owner_epoch: u64,
    node_id: String,
    endpoint: String,
    stage: CrashStage,
    ready_path: PathBuf,
}

#[derive(Debug, Serialize)]
struct ReadyRecord {
    stage: CrashStage,
    previous_owner_epoch: u64,
    recovery_owner_epoch: u64,
    local_epoch_at_crash: u64,
    lease_id: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("nokv-local-wal-recovery-fault: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config_path = std::env::args_os()
        .nth(1)
        .ok_or_else(|| "usage: nokv-local-wal-recovery-fault CONFIG.json".to_owned())?;
    if std::env::args_os().nth(2).is_some() {
        return Err("usage: nokv-local-wal-recovery-fault CONFIG.json".to_owned());
    }
    let config_bytes = std::fs::read(&config_path).map_err(|error| {
        format!(
            "read config {}: {error}",
            PathBuf::from(config_path).display()
        )
    })?;
    let config: Config = serde_json::from_slice(&config_bytes)
        .map_err(|error| format!("decode fault-driver config: {error}"))?;
    validate_config(&config)?;

    let logical_shard_id = parse_logical_shard_id(&config.logical_shard_id)?;
    let previous_owner_epoch = OwnerEpoch::new(config.previous_owner_epoch)
        .map_err(|error| format!("invalid previous owner epoch: {error}"))?;

    // The production bootstrap opens and validates the exclusive local
    // authority before changing the control epoch. Keep that authority open
    // until the parent kills this process so the crash boundary is faithful.
    let catalog = meta::keyspaces()
        .iter()
        .map(|definition| TreeBinding::new(definition.id, definition.name));
    let holt = HoltStore::open(HoltOptions::file(
        &config.metadata_path,
        catalog,
        meta::store_limits(),
    ))
    .map_err(|error| format!("open local Holt authority: {error}"))?;
    let store: Arc<dyn TxnStore> = Arc::new(holt);
    let meta = meta::MetaShard::open(store, logical_shard_id)
        .map_err(|error| format!("open metadata shard: {error}"))?;
    let local_before = meta
        .current_owner_epoch()
        .map_err(|error| format!("read local owner epoch: {error}"))?;
    if local_before != Some(previous_owner_epoch) {
        return Err(format!(
            "local authority is fenced at {}, expected previous epoch {}",
            display_epoch(local_before),
            previous_owner_epoch
        ));
    }

    let control = EtcdControlStore::connect(
        EtcdControlStoreOptions::new(config.etcd_endpoints.clone())
            .with_key_prefix(config.etcd_key_prefix.clone())
            .with_lease_ttl_seconds(config.lease_ttl_seconds),
    )
    .map_err(|error| format!("connect etcd control store: {error}"))?;
    let lease = control
        .acquire_successor(
            &logical_shard_id,
            previous_owner_epoch,
            NodeId::new(config.node_id.clone())
                .map_err(|error| format!("invalid node id: {error}"))?,
            config.endpoint.clone(),
        )
        .map_err(|error| format!("acquire successor: {error}"))?;

    if matches!(config.stage, CrashStage::AfterLocalFence) {
        meta.advance_owner_epoch(Some(previous_owner_epoch), lease.owner_epoch)
            .map_err(|error| format!("advance local owner epoch: {error}"))?;
    }
    let local_epoch_at_crash = meta
        .current_owner_epoch()
        .map_err(|error| format!("read crash-boundary owner epoch: {error}"))?
        .ok_or_else(|| "crash-boundary local owner epoch is absent".to_owned())?;

    write_ready(
        &config.ready_path,
        &ReadyRecord {
            stage: config.stage,
            previous_owner_epoch: previous_owner_epoch.get(),
            recovery_owner_epoch: lease.owner_epoch.get(),
            local_epoch_at_crash: local_epoch_at_crash.get(),
            lease_id: lease.lease_id,
        },
    )?;

    // Stay live until the qualification parent sends SIGKILL. Renewing avoids
    // accidentally testing natural expiry before the requested crash boundary.
    let renew_interval_ms = u64::try_from(config.lease_ttl_seconds)
        .map_err(|_| "lease TTL must be positive".to_owned())?
        .saturating_mul(1_000)
        .saturating_div(3)
        .max(100);
    loop {
        thread::sleep(Duration::from_millis(renew_interval_ms));
        control
            .renew_owner(&lease)
            .map_err(|error| format!("renew crash-boundary owner: {error}"))?;
    }
}

fn validate_config(config: &Config) -> Result<(), String> {
    if config.etcd_endpoints.is_empty() || config.etcd_endpoints.iter().any(String::is_empty) {
        return Err("etcd_endpoints must contain at least one non-empty endpoint".to_owned());
    }
    if config.etcd_key_prefix.is_empty() || !config.etcd_key_prefix.starts_with('/') {
        return Err("etcd_key_prefix must be an absolute non-empty key prefix".to_owned());
    }
    if config.lease_ttl_seconds <= 0 {
        return Err("lease_ttl_seconds must be positive".to_owned());
    }
    if config.endpoint.is_empty() {
        return Err("endpoint must be non-empty".to_owned());
    }
    if config.ready_path.exists() {
        return Err(format!(
            "ready path already exists: {}",
            config.ready_path.display()
        ));
    }
    Ok(())
}

fn parse_logical_shard_id(raw: &str) -> Result<LogicalShardId, String> {
    if raw.len() != LogicalShardId::BYTE_WIDTH * 2
        || !raw.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("logical_shard_id must be exactly 32 hexadecimal characters".to_owned());
    }
    let mut bytes = [0_u8; LogicalShardId::BYTE_WIDTH];
    for (index, slot) in bytes.iter_mut().enumerate() {
        let offset = index * 2;
        *slot = u8::from_str_radix(&raw[offset..offset + 2], 16)
            .map_err(|error| format!("decode logical_shard_id: {error}"))?;
    }
    Ok(LogicalShardId::from_bytes(bytes))
}

fn write_ready(path: &Path, record: &ReadyRecord) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("create ready record {}: {error}", path.display()))?;
    serde_json::to_writer_pretty(&mut file, record)
        .map_err(|error| format!("encode ready record: {error}"))?;
    file.write_all(b"\n")
        .map_err(|error| format!("finish ready record: {error}"))?;
    file.sync_all()
        .map_err(|error| format!("sync ready record: {error}"))
}

fn display_epoch(epoch: Option<OwnerEpoch>) -> String {
    epoch.map_or_else(|| "epoch-zero".to_owned(), |epoch| epoch.get().to_string())
}
