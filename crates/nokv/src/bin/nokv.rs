//! Minimal NoKV command line interface.

#[path = "nokv/workbench_mcp.rs"]
mod workbench_mcp;

use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::Duration;

use nokv_client::{ArtifactMetadata, MetadataClient, MetadataClientOptions, NoKvFsClient};
use nokv_control::EtcdControlStoreOptions;
use nokv_meta::{HistoryGcOptions, ObjectGcOptions, SubtreeDeltaKind};
use nokv_object::{
    BlockCachePolicy, ConfiguredObjectStore, DiskBlockCacheOptions, FileReadPipelineOptions,
    HotFillMode, LocalObjectStoreOptions, MemoryBlockCacheOptions, ObjectKey, ObjectStoreConfig,
    S3ObjectStoreOptions, TieredObjectStoreOptions,
};
use nokv_server::{
    ServerControlOptions, ServerControlStoreOptions, ServerOptions, ServerShardOwnerOptions,
    ServerShardOwnerRenewalOptions, ServerSharedLogOptions,
    DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX, DEFAULT_SERVER_BIND,
};
use nokv_types::{FileType, MountId};
use sha2::{Digest, Sha256};

const DEFAULT_MODE_DIR: u32 = 0o755;
const DEFAULT_MODE_FILE: u32 = 0o644;
const DEFAULT_UID: u32 = 1000;
const DEFAULT_GID: u32 = 1000;
const DEFAULT_GC_LIMIT: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    meta: PathBuf,
    metadata_checkpoint_archive_prefix: Option<String>,
    object: ObjectStoreConfig,
    mount: MountId,
    uid: u32,
    gid: u32,
    object_gc_interval: Duration,
    object_gc_limit: usize,
    history_gc_interval: Duration,
    history_gc_limit: usize,
    server_bind: SocketAddr,
    control: Option<ServerControlOptions>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectBackendKind {
    S3,
    RustFs,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ControlBackendKind {
    #[default]
    None,
    Etcd,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ControlCliConfig {
    backend: ControlBackendKind,
    etcd_endpoints: Option<Vec<String>>,
    etcd_key_prefix: Option<String>,
    etcd_lease_ttl_seconds: Option<i64>,
    shard_id: Option<String>,
    shard_index: Option<u16>,
    node_id: Option<String>,
    failover_from_epoch: Option<u64>,
    renewal: Option<ServerShardOwnerRenewalOptions>,
    shared_log_prefix: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Init,
    Mkdir {
        path: String,
    },
    RegisterGraft {
        prefix: String,
    },
    UnregisterGraft {
        prefix: String,
    },
    ReconcileGrafts,
    PutArtifact {
        path: String,
        source: PathBuf,
    },
    Ls {
        path: String,
    },
    Cat {
        path: String,
    },
    Rm {
        path: String,
    },
    Rmdir {
        path: String,
    },
    Rename {
        source: String,
        destination: String,
    },
    RenameReplace {
        source: String,
        destination: String,
    },
    Mount {
        mountpoint: PathBuf,
        options: MountCliOptions,
    },
    MountSnapshot {
        snapshot_id: u64,
        mountpoint: PathBuf,
        options: MountCliOptions,
    },
    Serve,
    Mcp(McpCliOptions),
    Gc {
        limit: usize,
    },
    Backup,
    Restore,
    Fsck,
    Snapshot {
        path: String,
    },
    Clone {
        src: String,
        dst: String,
    },
    Diff {
        a: String,
        b: String,
    },
    Rollback {
        path: String,
        snapshot_id: u64,
    },
    CatSnapshot {
        snapshot_id: u64,
        path: String,
    },
    RetireSnapshot {
        snapshot_id: u64,
    },
    RenewSnapshot {
        snapshot_id: u64,
        lease_ms: u64,
    },
    Help,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MountCliOptions {
    read_only: bool,
    threads: usize,
    kernel_cache: bool,
    direct_io: bool,
    stats_bind: Option<SocketAddr>,
    entry_ttl: Duration,
    attr_ttl: Duration,
    block_cache: BlockCachePolicy,
    prefetch: nokv_fuse::FusePrefetchOptions,
    read_pipeline: FileReadPipelineOptions,
    writeback: nokv_fuse::FuseWritebackOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct McpCliOptions {
    profile: McpProfile,
    workbench_root: Option<String>,
    workbench_max_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpProfile {
    Agent,
    Workbench,
}

#[derive(Debug)]
enum CliError {
    MissingValue(&'static str),
    UnknownOption(String),
    UnknownCommand(String),
    MissingArgument(&'static str),
    TooManyArguments,
    InvalidMount(String),
    InvalidOption { field: &'static str, value: String },
    InvalidAddress { field: &'static str, value: String },
    InvalidNumber { field: &'static str, value: String },
    Io(String),
    Client(String),
}

impl Default for MountCliOptions {
    fn default() -> Self {
        let defaults = nokv_fuse::FuseOptions::default();
        Self {
            read_only: false,
            threads: defaults.threads,
            kernel_cache: defaults.kernel_cache,
            direct_io: defaults.direct_io,
            stats_bind: defaults.stats_bind,
            entry_ttl: defaults.entry_ttl,
            attr_ttl: defaults.attr_ttl,
            block_cache: defaults.block_cache,
            prefetch: defaults.prefetch,
            read_pipeline: defaults.read_pipeline,
            writeback: defaults.writeback,
        }
    }
}

impl Default for McpCliOptions {
    fn default() -> Self {
        Self {
            profile: McpProfile::Agent,
            workbench_root: None,
            workbench_max_bytes: workbench_mcp::DEFAULT_WORKBENCH_MAX_BYTES,
        }
    }
}

impl Default for ControlCliConfig {
    fn default() -> Self {
        Self {
            backend: ControlBackendKind::None,
            etcd_endpoints: None,
            etcd_key_prefix: None,
            etcd_lease_ttl_seconds: None,
            shard_id: None,
            shard_index: None,
            node_id: None,
            failover_from_epoch: None,
            renewal: Some(ServerShardOwnerRenewalOptions::default()),
            shared_log_prefix: None,
        }
    }
}

impl ControlCliConfig {
    fn requires_backend(&self) -> bool {
        self.etcd_endpoints.is_some()
            || self.etcd_key_prefix.is_some()
            || self.etcd_lease_ttl_seconds.is_some()
            || self.shard_id.is_some()
            || self.shard_index.is_some()
            || self.node_id.is_some()
            || self.failover_from_epoch.is_some()
            || self.renewal != Some(ServerShardOwnerRenewalOptions::default())
            || self.shared_log_prefix.is_some()
    }
}

impl MountCliOptions {
    fn fuse_options(&self, access: nokv_fuse::FuseAccessMode) -> nokv_fuse::FuseOptions {
        nokv_fuse::FuseOptions {
            access,
            entry_ttl: self.entry_ttl,
            attr_ttl: self.attr_ttl,
            threads: self.threads,
            kernel_cache: self.kernel_cache,
            direct_io: self.direct_io,
            stats_bind: self.stats_bind,
            block_cache: self.block_cache.clone(),
            prefetch: self.prefetch.clone(),
            read_pipeline: self.read_pipeline,
            writeback: self.writeback.clone(),
            ..nokv_fuse::FuseOptions::default()
        }
    }
}

type Client = NoKvFsClient<ConfiguredObjectStore>;

fn main() {
    if let Err(err) = run(env::args().skip(1).collect()) {
        eprintln!("error: {err}");
        eprintln!();
        print_help(&mut io::stderr()).ok();
        std::process::exit(2);
    }
}

fn run(args: Vec<String>) -> Result<(), CliError> {
    let (config, command) = parse(args)?;
    match command {
        Command::Init => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!("initialized mount {}", config.mount.get());
        }
        Command::Mkdir { path } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let entry = client
                .metadata()
                .mkdir(&path, DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!("dir {} inode={}", path, entry.attr.inode.get());
        }
        Command::RegisterGraft { prefix } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let entry = client
                .metadata()
                .register_graft(&prefix, DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            println!(
                "graft {} -> inode={} (shard {})",
                prefix,
                entry.attr.inode.get(),
                entry.attr.inode.shard_index()
            );
        }
        Command::UnregisterGraft { prefix } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .unregister_graft(&prefix)
                .map_err(from_client)?;
            println!("unregistered graft {prefix}");
        }
        Command::ReconcileGrafts => {
            let client = open_client(&config)?;
            let reconciled = client.metadata().reconcile_grafts().map_err(from_client)?;
            if reconciled.is_empty() {
                println!("grafts reconciled: none missing");
            } else {
                for prefix in reconciled {
                    println!("reconciled graft {prefix}");
                }
            }
        }
        Command::PutArtifact { path, source } => {
            let mut digest_source = fs::File::open(&source).map_err(from_io)?;
            let digest_uri = artifact_digest_reader(&mut digest_source)?;
            let source_reader = fs::File::open(&source).map_err(from_io)?;
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let manifest_id = default_manifest_id(&path)?;
            let entry = client
                .put_artifact_from_reader(
                    &path,
                    source_reader,
                    ArtifactMetadata {
                        producer: "nokv".to_owned(),
                        digest_uri,
                        content_type: "application/octet-stream".to_owned(),
                        manifest_id,
                        mode: DEFAULT_MODE_FILE,
                        uid: config.uid,
                        gid: config.gid,
                    },
                )
                .map_err(from_client)?;
            println!(
                "artifact {} inode={} size={}",
                path,
                entry.attr.inode.get(),
                entry.attr.size
            );
        }
        Command::Ls { path } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            for entry in client.metadata().list(&path).map_err(from_client)? {
                println!(
                    "{}\t{}\t{}\t{}",
                    file_type_label(entry.attr.file_type),
                    entry.attr.inode.get(),
                    entry.attr.size,
                    String::from_utf8_lossy(entry.dentry.name.as_bytes())
                );
            }
        }
        Command::Cat { path } => {
            let client = open_client(&config)?;
            let bytes = client.cat(&path).map_err(from_client)?;
            io::stdout().write_all(&bytes).map_err(from_io)?;
        }
        Command::Rm { path } => {
            let client = open_client(&config)?;
            let removed = client.metadata().remove(&path).map_err(from_client)?;
            println!(
                "removed file {} inode={} body={}",
                path,
                removed.attr.inode.get(),
                removed
                    .body
                    .as_ref()
                    .map(|body| body.manifest_id.as_str())
                    .unwrap_or("-")
            );
        }
        Command::Rmdir { path } => {
            let client = open_client(&config)?;
            let removed = client.metadata().rmdir(&path).map_err(from_client)?;
            println!("removed dir {} inode={}", path, removed.attr.inode.get());
        }
        Command::Rename {
            source,
            destination,
        } => {
            let client = open_client(&config)?;
            let renamed = client
                .metadata()
                .rename(&source, &destination)
                .map_err(from_client)?;
            println!(
                "renamed {} -> {} inode={}",
                source,
                destination,
                renamed.attr.inode.get()
            );
        }
        Command::RenameReplace {
            source,
            destination,
        } => {
            let client = open_client(&config)?;
            let result = client
                .metadata()
                .rename_replace(&source, &destination)
                .map_err(from_client)?;
            println!(
                "renamed {} -> {} inode={} replaced_body={}",
                source,
                destination,
                result.entry.attr.inode.get(),
                result
                    .replaced
                    .as_ref()
                    .and_then(|entry| entry.body.as_ref())
                    .map(|body| body.manifest_id.as_str())
                    .unwrap_or("-")
            );
        }
        Command::Mount {
            mountpoint,
            options,
        } => {
            let metadata = open_mount_metadata(&config)?;
            let objects = config.object.open().map_err(from_object)?;
            metadata
                .bootstrap_root(DEFAULT_MODE_DIR, config.uid, config.gid)
                .map_err(from_client)?;
            let mount = nokv_fuse::spawn_mount_client(
                metadata,
                objects,
                mountpoint,
                options.fuse_options(if options.read_only {
                    nokv_fuse::FuseAccessMode::ReadOnly
                } else {
                    nokv_fuse::FuseAccessMode::ReadWrite
                }),
            )
            .map_err(from_io)?;
            mount.join().map_err(from_io)?;
        }
        Command::MountSnapshot {
            snapshot_id,
            mountpoint,
            options,
        } => {
            let metadata = open_mount_metadata(&config)?;
            let objects = config.object.open().map_err(from_object)?;
            let snapshot = metadata
                .snapshot_pin(snapshot_id)
                .map_err(from_client)?
                .ok_or_else(|| CliError::Client(format!("snapshot {snapshot_id} not found")))?;
            let mount = nokv_fuse::spawn_mount_client(
                metadata,
                objects,
                mountpoint,
                nokv_fuse::FuseOptions {
                    fs_name: format!("nokv-snapshot-{snapshot_id}"),
                    view: nokv_fuse::FuseView::Snapshot {
                        snapshot_id,
                        root: snapshot.root,
                    },
                    ..options.fuse_options(nokv_fuse::FuseAccessMode::ReadOnly)
                },
            )
            .map_err(from_io)?;
            mount.join().map_err(from_io)?;
        }
        Command::Serve => {
            nokv_server::run(server_options_from_config(config)).map_err(from_io)?;
        }
        Command::Mcp(options) => {
            let client = open_client(&config)?;
            run_mcp(client, options, config.uid, config.gid).map_err(from_io)?;
        }
        Command::Gc { limit } => {
            let body = control_get(&config, &format!("/gc?limit={limit}"))?;
            print!("{body}");
        }
        Command::Backup => {
            let body = control_get(&config, "/backup")?;
            print!("{body}");
        }
        Command::Restore => {
            let report =
                nokv_server::restore(server_options_from_config(config)).map_err(from_io)?;
            print!("{report}");
        }
        Command::Fsck => {
            let body = control_get(&config, "/fsck")?;
            print!("{body}");
        }
        Command::Snapshot { path } => {
            let client = open_client(&config)?;
            let snapshot = client
                .metadata()
                .snapshot_subtree_path(&path)
                .map_err(from_client)?;
            println!(
                "snapshot {} id={} version={}",
                path, snapshot.snapshot_id, snapshot.read_version
            );
        }
        Command::Clone { src, dst } => {
            let client = open_client(&config)?;
            let outcome = client
                .metadata()
                .clone_subtree_path(&src, &dst)
                .map_err(from_client)?;
            println!(
                "cloned {} -> {} root={} snapshot={}",
                src,
                dst,
                outcome.root.get(),
                outcome.snapshot_id
            );
        }
        Command::Diff { a, b } => {
            let client = open_client(&config)?;
            let deltas = client
                .metadata()
                .diff_subtrees(&a, &b)
                .map_err(from_client)?;
            for delta in deltas {
                println!(
                    "{}\t{}\t{:+}\t{}",
                    subtree_delta_label(delta.kind),
                    delta.path,
                    delta.size_delta,
                    delta.digest.as_deref().unwrap_or("-")
                );
            }
        }
        Command::Rollback { path, snapshot_id } => {
            let client = open_client(&config)?;
            client
                .metadata()
                .rollback_subtree_path(&path, snapshot_id)
                .map_err(from_client)?;
            println!("rolled back {} to snapshot {}", path, snapshot_id);
        }
        Command::CatSnapshot { snapshot_id, path } => {
            let client = open_client(&config)?;
            let bytes = client
                .cat_snapshot(snapshot_id, &path)
                .map_err(from_client)?;
            io::stdout().write_all(&bytes).map_err(from_io)?;
        }
        Command::RetireSnapshot { snapshot_id } => {
            let client = open_client(&config)?;
            let retired = client
                .metadata()
                .retire_snapshot(snapshot_id)
                .map_err(from_client)?;
            println!("retired_snapshot id={} retired={}", snapshot_id, retired);
        }
        Command::RenewSnapshot {
            snapshot_id,
            lease_ms,
        } => {
            let client = open_client(&config)?;
            let renewed = client
                .metadata()
                .renew_snapshot(snapshot_id, lease_ms)
                .map_err(from_client)?;
            println!(
                "renewed_snapshot id={} renewed={} lease_ms={}",
                snapshot_id, renewed, lease_ms
            );
        }
        Command::Help => {
            print_help(&mut io::stdout()).map_err(from_io)?;
        }
    }
    Ok(())
}

fn open_client(config: &Config) -> Result<Client, CliError> {
    let objects = config.object.open().map_err(from_object)?;
    // With an etcd control backend configured, route client requests across the
    // fleet: each request resolves the owning shard's endpoint from the control
    // plane and transparently re-resolves on an owner handoff. Without a control
    // backend this is the single-shard client pinned to `--server-bind`.
    if let Some(control) = config.control.as_ref() {
        let ServerControlStoreOptions::Etcd(etcd) = &control.store;
        return open_fleet_client(etcd.clone(), config.mount, objects);
    }
    let metadata = MetadataClient::new(MetadataClientOptions::new(config.server_bind));
    Ok(NoKvFsClient::new(metadata, objects))
}

#[cfg(feature = "etcd")]
fn open_fleet_client(
    etcd: EtcdControlStoreOptions,
    mount: MountId,
    objects: ConfiguredObjectStore,
) -> Result<Client, CliError> {
    use std::sync::Arc;
    let store: Arc<dyn nokv_control::ControlStore> = Arc::new(
        nokv_control::EtcdControlStore::connect(etcd).map_err(|err| CliError::InvalidOption {
            field: "control",
            value: err.to_string(),
        })?,
    );
    NoKvFsClient::connect_fleet(store, mount, objects).map_err(|err| CliError::InvalidOption {
        field: "control",
        value: err.to_string(),
    })
}

#[cfg(not(feature = "etcd"))]
fn open_fleet_client(
    _etcd: EtcdControlStoreOptions,
    _mount: MountId,
    _objects: ConfiguredObjectStore,
) -> Result<Client, CliError> {
    Err(CliError::InvalidOption {
        field: "control",
        value: "fleet client routing requires the etcd control feature".to_owned(),
    })
}

/// Build the metadata client for the FUSE mount. With an etcd control backend it
/// routes across the fleet (each path resolves its owning shard's endpoint from
/// the control plane); otherwise it is the single-shard client pinned to
/// `--server-bind`. Mirrors [`open_client`] for the mount's `MetadataClient`.
fn open_mount_metadata(config: &Config) -> Result<MetadataClient, CliError> {
    if let Some(control) = config.control.as_ref() {
        let ServerControlStoreOptions::Etcd(etcd) = &control.store;
        return open_fleet_metadata(etcd.clone(), config.mount);
    }
    Ok(MetadataClient::new(MetadataClientOptions::new(
        config.server_bind,
    )))
}

#[cfg(feature = "etcd")]
fn open_fleet_metadata(
    etcd: EtcdControlStoreOptions,
    mount: MountId,
) -> Result<MetadataClient, CliError> {
    use std::sync::Arc;
    let store: Arc<dyn nokv_control::ControlStore> = Arc::new(
        nokv_control::EtcdControlStore::connect(etcd).map_err(|err| CliError::InvalidOption {
            field: "control",
            value: err.to_string(),
        })?,
    );
    MetadataClient::fleet(store, mount).map_err(|err| CliError::InvalidOption {
        field: "control",
        value: err.to_string(),
    })
}

#[cfg(not(feature = "etcd"))]
fn open_fleet_metadata(
    _etcd: EtcdControlStoreOptions,
    _mount: MountId,
) -> Result<MetadataClient, CliError> {
    Err(CliError::InvalidOption {
        field: "control",
        value: "fleet mount routing requires the etcd control feature".to_owned(),
    })
}

fn control_get(config: &Config, path: &str) -> Result<String, CliError> {
    let mut stream = TcpStream::connect(config.server_bind).map_err(from_io)?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\n\r\n",
        config.server_bind
    )
    .map_err(from_io)?;
    let mut response = String::new();
    stream.read_to_string(&mut response).map_err(from_io)?;
    let Some((headers, body)) = response.split_once("\r\n\r\n") else {
        return Err(CliError::Client("malformed control response".to_owned()));
    };
    if !headers.starts_with("HTTP/1.1 200 ") {
        return Err(CliError::Client(response));
    }
    Ok(body.to_owned())
}

fn server_options_from_config(config: Config) -> ServerOptions {
    ServerOptions {
        bind: config.server_bind,
        mount: config.mount,
        meta_path: config.meta,
        metadata_checkpoint_archive_prefix: config.metadata_checkpoint_archive_prefix,
        object: config.object,
        uid: config.uid,
        gid: config.gid,
        object_gc: ObjectGcOptions {
            interval: config.object_gc_interval,
            limit: config.object_gc_limit,
            run_immediately: false,
            read_lease_grace: ObjectGcOptions::default().read_lease_grace,
        },
        history_gc: HistoryGcOptions {
            interval: config.history_gc_interval,
            limit: config.history_gc_limit,
            run_immediately: false,
        },
        control: config.control,
    }
}

fn parse(args: Vec<String>) -> Result<(Config, Command), CliError> {
    let mut meta = PathBuf::from(".nokv/meta");
    let mut metadata_checkpoint_archive_prefix =
        Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX.to_owned());
    let mut object_backend = ObjectBackendKind::RustFs;
    let mut s3 = S3ObjectStoreOptions::new("");
    let mut hot_object_root: Option<PathBuf> = None;
    let mut hot_object_max_bytes: Option<u64> = None;
    let mut hot_fill_mode = HotFillMode::Inline;
    let mut mount = MountId::new(1).expect("default mount id is non-zero");
    let mut uid = DEFAULT_UID;
    let mut gid = DEFAULT_GID;
    let mut object_gc_interval = ObjectGcOptions::default().interval;
    let mut object_gc_limit = ObjectGcOptions::default().limit;
    let mut history_gc_interval = HistoryGcOptions::default().interval;
    let mut history_gc_limit = HistoryGcOptions::default().limit;
    let mut server_bind = DEFAULT_SERVER_BIND;
    let mut control = ControlCliConfig::default();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--meta" => {
                index += 1;
                meta = PathBuf::from(value(&args, index, "--meta")?);
            }
            "--metadata-checkpoint-archive-prefix" => {
                index += 1;
                metadata_checkpoint_archive_prefix = Some(parse_archive_prefix(
                    value(&args, index, "--metadata-checkpoint-archive-prefix")?,
                    "--metadata-checkpoint-archive-prefix",
                )?);
            }
            "--no-metadata-checkpoint-archive" => {
                metadata_checkpoint_archive_prefix = None;
            }
            "--object-backend" => {
                index += 1;
                object_backend = parse_object_backend(value(&args, index, "--object-backend")?)?;
            }
            "--s3-bucket" => {
                index += 1;
                s3.bucket = value(&args, index, "--s3-bucket")?.to_owned();
            }
            "--s3-endpoint" => {
                index += 1;
                s3.endpoint = Some(value(&args, index, "--s3-endpoint")?.to_owned());
            }
            "--s3-region" => {
                index += 1;
                s3.region = value(&args, index, "--s3-region")?.to_owned();
            }
            "--s3-access-key-id" => {
                index += 1;
                s3.access_key_id = Some(value(&args, index, "--s3-access-key-id")?.to_owned());
            }
            "--s3-secret-access-key" => {
                index += 1;
                s3.secret_access_key =
                    Some(value(&args, index, "--s3-secret-access-key")?.to_owned());
            }
            "--s3-session-token" => {
                index += 1;
                s3.session_token = Some(value(&args, index, "--s3-session-token")?.to_owned());
            }
            "--s3-root" => {
                index += 1;
                s3.root = value(&args, index, "--s3-root")?.to_owned();
            }
            "--s3-virtual-host-style" => {
                s3.virtual_host_style = true;
            }
            "--s3-skip-signature" => {
                s3.skip_signature = true;
            }
            "--hot-object-root" => {
                index += 1;
                hot_object_root = Some(PathBuf::from(value(&args, index, "--hot-object-root")?));
            }
            "--hot-object-max-bytes" => {
                index += 1;
                let max_bytes = parse_u64(
                    value(&args, index, "--hot-object-max-bytes")?,
                    "hot_object_max_bytes",
                )?;
                if max_bytes == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "hot_object_max_bytes",
                        value: max_bytes.to_string(),
                    });
                }
                hot_object_max_bytes = Some(max_bytes);
            }
            "--hot-fill-mode" => {
                index += 1;
                hot_fill_mode = parse_hot_fill_mode(value(&args, index, "--hot-fill-mode")?)?;
            }
            "--mount" => {
                index += 1;
                let raw = value(&args, index, "--mount")?;
                let parsed = raw
                    .parse::<u64>()
                    .map_err(|_| CliError::InvalidMount(raw.to_owned()))?;
                mount = MountId::new(parsed).map_err(|_| CliError::InvalidMount(raw.to_owned()))?;
            }
            "--uid" => {
                index += 1;
                uid = parse_u32(value(&args, index, "--uid")?, "uid")?;
            }
            "--gid" => {
                index += 1;
                gid = parse_u32(value(&args, index, "--gid")?, "gid")?;
            }
            "--object-gc-interval-ms" => {
                index += 1;
                let interval_ms = parse_u64(
                    value(&args, index, "--object-gc-interval-ms")?,
                    "object_gc_interval_ms",
                )?;
                if interval_ms == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "object_gc_interval_ms",
                        value: interval_ms.to_string(),
                    });
                }
                object_gc_interval = Duration::from_millis(interval_ms);
            }
            "--object-gc-limit" => {
                index += 1;
                object_gc_limit =
                    parse_usize(value(&args, index, "--object-gc-limit")?, "object_gc_limit")?;
            }
            "--history-gc-interval-ms" => {
                index += 1;
                let interval_ms = parse_u64(
                    value(&args, index, "--history-gc-interval-ms")?,
                    "history_gc_interval_ms",
                )?;
                if interval_ms == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "history_gc_interval_ms",
                        value: interval_ms.to_string(),
                    });
                }
                history_gc_interval = Duration::from_millis(interval_ms);
            }
            "--history-gc-limit" => {
                index += 1;
                history_gc_limit = parse_usize(
                    value(&args, index, "--history-gc-limit")?,
                    "history_gc_limit",
                )?;
            }
            "--server-bind" => {
                index += 1;
                server_bind =
                    parse_socket_addr(value(&args, index, "--server-bind")?, "server_bind")?;
            }
            "--control-backend" => {
                index += 1;
                control.backend = parse_control_backend(value(&args, index, "--control-backend")?)?;
            }
            "--control-etcd-endpoints" => {
                index += 1;
                control.backend = ControlBackendKind::Etcd;
                control.etcd_endpoints = Some(parse_etcd_endpoints(value(
                    &args,
                    index,
                    "--control-etcd-endpoints",
                )?)?);
            }
            "--control-etcd-prefix" => {
                index += 1;
                control.backend = ControlBackendKind::Etcd;
                control.etcd_key_prefix =
                    Some(value(&args, index, "--control-etcd-prefix")?.to_owned());
            }
            "--control-etcd-lease-ttl-seconds" => {
                index += 1;
                control.backend = ControlBackendKind::Etcd;
                control.etcd_lease_ttl_seconds = Some(parse_positive_i64(
                    value(&args, index, "--control-etcd-lease-ttl-seconds")?,
                    "control_etcd_lease_ttl_seconds",
                )?);
            }
            "--shard-id" => {
                index += 1;
                control.shard_id = Some(value(&args, index, "--shard-id")?.to_owned());
            }
            "--shard-index" => {
                index += 1;
                control.shard_index = Some(parse_u16(
                    value(&args, index, "--shard-index")?,
                    "shard_index",
                )?);
            }
            "--node-id" => {
                index += 1;
                control.node_id = Some(value(&args, index, "--node-id")?.to_owned());
            }
            "--failover-from-epoch" => {
                index += 1;
                control.failover_from_epoch = Some(parse_u64(
                    value(&args, index, "--failover-from-epoch")?,
                    "failover_from_epoch",
                )?);
            }
            "--no-shard-owner-renewal" => {
                control.renewal = None;
            }
            "--shard-owner-renewal-interval-ms" => {
                index += 1;
                let interval_ms = parse_u64(
                    value(&args, index, "--shard-owner-renewal-interval-ms")?,
                    "shard_owner_renewal_interval_ms",
                )?;
                if interval_ms == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "shard_owner_renewal_interval_ms",
                        value: interval_ms.to_string(),
                    });
                }
                if let Some(renewal) = control.renewal.as_mut() {
                    renewal.interval = Duration::from_millis(interval_ms);
                }
            }
            "--metadata-shared-log-prefix" => {
                index += 1;
                control.shared_log_prefix = Some(parse_archive_prefix(
                    value(&args, index, "--metadata-shared-log-prefix")?,
                    "--metadata-shared-log-prefix",
                )?);
            }
            "--help" | "-h" => {
                return Ok((
                    Config {
                        meta,
                        metadata_checkpoint_archive_prefix,
                        object: object_config(
                            object_backend,
                            s3,
                            hot_object_root,
                            hot_object_max_bytes,
                            hot_fill_mode,
                        ),
                        mount,
                        uid,
                        gid,
                        object_gc_interval,
                        object_gc_limit,
                        history_gc_interval,
                        history_gc_limit,
                        server_bind,
                        control: None,
                    },
                    Command::Help,
                ));
            }
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.to_owned()));
            }
            _ => break,
        }
        index += 1;
    }

    let command = parse_command(&args[index..])?;
    let control = build_control_options(control, mount, server_bind)?;
    Ok((
        Config {
            meta,
            metadata_checkpoint_archive_prefix,
            object: object_config(
                object_backend,
                s3,
                hot_object_root,
                hot_object_max_bytes,
                hot_fill_mode,
            ),
            mount,
            uid,
            gid,
            object_gc_interval,
            object_gc_limit,
            history_gc_interval,
            history_gc_limit,
            server_bind,
            control,
        },
        command,
    ))
}

fn parse_object_backend(raw: &str) -> Result<ObjectBackendKind, CliError> {
    match raw {
        "s3" => Ok(ObjectBackendKind::S3),
        "rustfs" => Ok(ObjectBackendKind::RustFs),
        _ => Err(CliError::UnknownOption(format!("--object-backend {raw}"))),
    }
}

fn parse_control_backend(raw: &str) -> Result<ControlBackendKind, CliError> {
    match raw {
        "none" => Ok(ControlBackendKind::None),
        "etcd" => Ok(ControlBackendKind::Etcd),
        _ => Err(CliError::UnknownOption(format!("--control-backend {raw}"))),
    }
}

fn parse_etcd_endpoints(raw: &str) -> Result<Vec<String>, CliError> {
    let endpoints = raw
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() {
        return Err(CliError::InvalidOption {
            field: "control_etcd_endpoints",
            value: raw.to_owned(),
        });
    }
    Ok(endpoints)
}

fn parse_positive_i64(raw: &str, field: &'static str) -> Result<i64, CliError> {
    let value = parse_u64(raw, field)?;
    if value == 0 || value > i64::MAX as u64 {
        return Err(CliError::InvalidNumber {
            field,
            value: raw.to_owned(),
        });
    }
    Ok(value as i64)
}

fn build_control_options(
    control: ControlCliConfig,
    mount: MountId,
    server_bind: SocketAddr,
) -> Result<Option<ServerControlOptions>, CliError> {
    match control.backend {
        ControlBackendKind::None => {
            if control.requires_backend() {
                return Err(CliError::InvalidOption {
                    field: "control",
                    value: "control owner options require --control-backend etcd".to_owned(),
                });
            }
            Ok(None)
        }
        ControlBackendKind::Etcd => {
            let mut store =
                EtcdControlStoreOptions::new(control.etcd_endpoints.unwrap_or_default());
            if let Some(prefix) = control.etcd_key_prefix {
                store = store.with_key_prefix(prefix);
            }
            if let Some(ttl_seconds) = control.etcd_lease_ttl_seconds {
                store = store.with_lease_ttl_seconds(ttl_seconds);
            }
            store.validate().map_err(|err| CliError::InvalidOption {
                field: "control",
                value: err.to_string(),
            })?;

            // Couple renewal + self-fence to the etcd lease TTL: renew at most
            // every TTL/3 (>=1s) so a short TTL never lapses before renewal, and
            // arm the wall-clock self-fence exactly at the lease TTL so a
            // partitioned owner stops committing no later than etcd expires it.
            // A user-set faster interval is respected (we only shrink, never grow).
            let renewal = control.renewal.map(|mut renewal| {
                let ttl = Duration::from_secs(store.lease_ttl_seconds().max(1) as u64);
                renewal.lease_ttl = ttl;
                renewal.interval = renewal.interval.min((ttl / 3).max(Duration::from_secs(1)));
                renewal
            });

            let shard_id = control
                .shard_id
                .unwrap_or_else(|| format!("mount-{}:/", mount.get()));
            let node_id = control.node_id.unwrap_or_else(|| server_bind.to_string());
            let shard_owner = match control.failover_from_epoch {
                Some(previous_epoch) => {
                    ServerShardOwnerOptions::failover(shard_id, node_id, previous_epoch)
                }
                None => ServerShardOwnerOptions::fresh(shard_id, node_id),
            }
            .with_renewal(renewal)
            .with_shared_log(control.shared_log_prefix.map(ServerSharedLogOptions::new))
            .with_shard_index(control.shard_index);

            Ok(Some(ServerControlOptions {
                store: ServerControlStoreOptions::Etcd(store),
                shard_owner,
            }))
        }
    }
}

fn parse_hot_fill_mode(raw: &str) -> Result<HotFillMode, CliError> {
    match raw {
        "inline" => Ok(HotFillMode::Inline),
        "background" => Ok(HotFillMode::Background),
        _ => Err(CliError::UnknownOption(format!("--hot-fill-mode {raw}"))),
    }
}

fn parse_archive_prefix(raw: &str, option: &str) -> Result<String, CliError> {
    let prefix = raw.trim_matches('/');
    ObjectKey::new(prefix).map_err(|_| CliError::UnknownOption(format!("{option} {raw}")))?;
    Ok(prefix.to_owned())
}

fn parse_mcp_args(args: &[String]) -> Result<McpCliOptions, CliError> {
    let mut options = McpCliOptions::default();
    let mut index = 1;
    while index < args.len() {
        match args[index].as_str() {
            "--profile" => {
                index += 1;
                options.profile = parse_mcp_profile(value(args, index, "--profile")?)?;
            }
            "--workbench-root" => {
                index += 1;
                let root = workbench_mcp::normalize_workbench_root(value(
                    args,
                    index,
                    "--workbench-root",
                )?)
                .map_err(|err| CliError::InvalidOption {
                    field: "workbench-root",
                    value: err,
                })?;
                options.workbench_root = Some(root);
            }
            "--workbench-max-bytes" => {
                index += 1;
                options.workbench_max_bytes = parse_usize(
                    value(args, index, "--workbench-max-bytes")?,
                    "workbench-max-bytes",
                )?;
            }
            other => return Err(CliError::UnknownOption(other.to_owned())),
        }
        index += 1;
    }
    if options.profile == McpProfile::Workbench && options.workbench_root.is_none() {
        return Err(CliError::MissingArgument("workbench root"));
    }
    if options.profile == McpProfile::Agent
        && (options.workbench_root.is_some()
            || options.workbench_max_bytes != workbench_mcp::DEFAULT_WORKBENCH_MAX_BYTES)
    {
        return Err(CliError::InvalidOption {
            field: "mcp profile",
            value: "workbench options require --profile workbench".to_owned(),
        });
    }
    Ok(options)
}

fn parse_mcp_profile(raw: &str) -> Result<McpProfile, CliError> {
    match raw {
        "agent" => Ok(McpProfile::Agent),
        "workbench" => Ok(McpProfile::Workbench),
        _ => Err(CliError::InvalidOption {
            field: "profile",
            value: raw.to_owned(),
        }),
    }
}

fn object_config(
    backend: ObjectBackendKind,
    mut s3: S3ObjectStoreOptions,
    hot_object_root: Option<PathBuf>,
    hot_object_max_bytes: Option<u64>,
    hot_fill_mode: HotFillMode,
) -> ObjectStoreConfig {
    let cold = match backend {
        ObjectBackendKind::S3 => s3,
        ObjectBackendKind::RustFs => {
            apply_rustfs_defaults(&mut s3);
            s3
        }
    };
    match hot_object_root {
        Some(root) => {
            let hot = match hot_object_max_bytes {
                Some(max_bytes) => LocalObjectStoreOptions::new(root).with_max_bytes(max_bytes),
                None => LocalObjectStoreOptions::new(root),
            };
            ObjectStoreConfig::tiered_local_with_options(
                hot,
                cold,
                TieredObjectStoreOptions {
                    hot_fill_mode,
                    ..TieredObjectStoreOptions::default()
                },
            )
        }
        None => ObjectStoreConfig::s3(cold),
    }
}

fn apply_rustfs_defaults(options: &mut S3ObjectStoreOptions) {
    if options.bucket.is_empty() {
        options.bucket = "nokv".to_owned();
    }
    if options.region.is_empty() || options.region == "us-east-1" {
        options.region = "auto".to_owned();
    }
    if options.endpoint.is_none() {
        options.endpoint = Some("http://127.0.0.1:9000".to_owned());
    }
    if options.access_key_id.is_none() {
        options.access_key_id = Some("rustfsadmin".to_owned());
    }
    if options.secret_access_key.is_none() {
        options.secret_access_key = Some("rustfsadmin".to_owned());
    }
}

fn parse_command(args: &[String]) -> Result<Command, CliError> {
    let Some(command) = args.first().map(String::as_str) else {
        return Ok(Command::Help);
    };
    match command {
        "init" => exact_args(args, 1).map(|()| Command::Init),
        "mkdir" => exact_args(args, 2).map(|()| Command::Mkdir {
            path: args[1].clone(),
        }),
        "register-graft" => exact_args(args, 2).map(|()| Command::RegisterGraft {
            prefix: args[1].clone(),
        }),
        "unregister-graft" => exact_args(args, 2).map(|()| Command::UnregisterGraft {
            prefix: args[1].clone(),
        }),
        "reconcile-grafts" => exact_args(args, 1).map(|()| Command::ReconcileGrafts),
        "put-artifact" => exact_args(args, 3).map(|()| Command::PutArtifact {
            path: args[1].clone(),
            source: PathBuf::from(&args[2]),
        }),
        "ls" => exact_args(args, 2).map(|()| Command::Ls {
            path: args[1].clone(),
        }),
        "cat" => exact_args(args, 2).map(|()| Command::Cat {
            path: args[1].clone(),
        }),
        "rm" => exact_args(args, 2).map(|()| Command::Rm {
            path: args[1].clone(),
        }),
        "rmdir" => exact_args(args, 2).map(|()| Command::Rmdir {
            path: args[1].clone(),
        }),
        "rename" => exact_args(args, 3).map(|()| Command::Rename {
            source: args[1].clone(),
            destination: args[2].clone(),
        }),
        "rename-replace" => exact_args(args, 3).map(|()| Command::RenameReplace {
            source: args[1].clone(),
            destination: args[2].clone(),
        }),
        "mount" => {
            let (options, mountpoint) = parse_mount_args(args, 1, true)?;
            Ok(Command::Mount {
                mountpoint,
                options,
            })
        }
        "mount-snapshot" => {
            if args.len() < 3 {
                return Err(CliError::MissingArgument("snapshot id and mountpoint"));
            }
            let (options, mountpoint) = parse_mount_args(args, 2, false)?;
            Ok(Command::MountSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                mountpoint,
                options,
            })
        }
        "serve" => exact_args(args, 1).map(|()| Command::Serve),
        "mcp" => parse_mcp_args(args).map(Command::Mcp),
        "backup" => exact_args(args, 1).map(|()| Command::Backup),
        "restore" => exact_args(args, 1).map(|()| Command::Restore),
        "fsck" => exact_args(args, 1).map(|()| Command::Fsck),
        "gc" => match args.len() {
            1 => Ok(Command::Gc {
                limit: DEFAULT_GC_LIMIT,
            }),
            2 => Ok(Command::Gc {
                limit: parse_usize(&args[1], "limit")?,
            }),
            _ => Err(CliError::TooManyArguments),
        },
        "snapshot" => exact_args(args, 2).map(|()| Command::Snapshot {
            path: args[1].clone(),
        }),
        "clone" => exact_args(args, 3).map(|()| Command::Clone {
            src: args[1].clone(),
            dst: args[2].clone(),
        }),
        "diff" => exact_args(args, 3).map(|()| Command::Diff {
            a: args[1].clone(),
            b: args[2].clone(),
        }),
        "rollback" => {
            exact_args(args, 3)?;
            Ok(Command::Rollback {
                path: args[1].clone(),
                snapshot_id: parse_u64(&args[2], "snapshot_id")?,
            })
        }
        "cat-snapshot" => {
            exact_args(args, 3)?;
            Ok(Command::CatSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                path: args[2].clone(),
            })
        }
        "retire-snapshot" => {
            exact_args(args, 2)?;
            Ok(Command::RetireSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
            })
        }
        "renew-snapshot" => match args.len() {
            2 => Ok(Command::RenewSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                lease_ms: nokv_meta::DEFAULT_SNAPSHOT_LEASE_MS,
            }),
            3 => Ok(Command::RenewSnapshot {
                snapshot_id: parse_u64(&args[1], "snapshot_id")?,
                lease_ms: parse_u64(&args[2], "lease_ms")?,
            }),
            _ => Err(CliError::TooManyArguments),
        },
        "help" => Ok(Command::Help),
        other => Err(CliError::UnknownCommand(other.to_owned())),
    }
}

fn default_disk_block_cache_options() -> DiskBlockCacheOptions {
    DiskBlockCacheOptions {
        root: std::env::temp_dir().join(format!("nokv-block-cache-{}", std::process::id())),
        max_bytes: 8 * 1024 * 1024 * 1024,
        max_items: 16 * 1024,
        ttl: None,
    }
}

fn set_block_cache_kind(options: &mut MountCliOptions, raw: &str) -> Result<(), CliError> {
    options.block_cache = match raw {
        "off" => BlockCachePolicy::Off,
        "memory" => BlockCachePolicy::Memory(MemoryBlockCacheOptions::default()),
        "disk" => BlockCachePolicy::Disk(default_disk_block_cache_options()),
        other => {
            return Err(CliError::InvalidNumber {
                field: "block_cache",
                value: other.to_owned(),
            })
        }
    };
    Ok(())
}

fn set_disk_block_cache_root(options: &mut MountCliOptions, root: PathBuf) {
    if !matches!(options.block_cache, BlockCachePolicy::Disk(_)) {
        options.block_cache = BlockCachePolicy::Disk(default_disk_block_cache_options());
    }
    if let BlockCachePolicy::Disk(cache) = &mut options.block_cache {
        cache.root = root;
    }
}

fn set_block_cache_bytes(options: &mut MountCliOptions, max_bytes: u64) {
    match &mut options.block_cache {
        BlockCachePolicy::Memory(cache) => cache.max_bytes = max_bytes,
        BlockCachePolicy::Disk(cache) => cache.max_bytes = max_bytes,
        BlockCachePolicy::Off => {
            options.block_cache = BlockCachePolicy::Memory(MemoryBlockCacheOptions {
                max_bytes,
                ..MemoryBlockCacheOptions::default()
            });
        }
    }
}

fn set_block_cache_items(options: &mut MountCliOptions, max_items: usize) {
    match &mut options.block_cache {
        BlockCachePolicy::Memory(cache) => cache.max_items = max_items,
        BlockCachePolicy::Disk(cache) => cache.max_items = max_items,
        BlockCachePolicy::Off => {
            options.block_cache = BlockCachePolicy::Memory(MemoryBlockCacheOptions {
                max_items,
                ..MemoryBlockCacheOptions::default()
            });
        }
    }
}

fn set_block_cache_ttl(options: &mut MountCliOptions, ttl: Option<Duration>) {
    match &mut options.block_cache {
        BlockCachePolicy::Memory(cache) => cache.ttl = ttl,
        BlockCachePolicy::Disk(cache) => cache.ttl = ttl,
        BlockCachePolicy::Off => {
            options.block_cache = BlockCachePolicy::Memory(MemoryBlockCacheOptions {
                ttl,
                ..MemoryBlockCacheOptions::default()
            });
        }
    }
}

fn parse_mount_args(
    args: &[String],
    start_index: usize,
    allow_read_only: bool,
) -> Result<(MountCliOptions, PathBuf), CliError> {
    let mut options = MountCliOptions::default();
    let mut mountpoint = None;
    let mut index = start_index;
    while index < args.len() {
        match args[index].as_str() {
            "--read-only" if allow_read_only => {
                options.read_only = true;
                index += 1;
            }
            "--read-only" => return Err(CliError::UnknownOption(args[index].clone())),
            "--no-kernel-cache" => {
                options.kernel_cache = false;
                index += 1;
            }
            "--direct-io" => {
                options.direct_io = true;
                index += 1;
            }
            "--stats-bind" => {
                index += 1;
                options.stats_bind = Some(parse_socket_addr(
                    value(args, index, "--stats-bind")?,
                    "stats_bind",
                )?);
                index += 1;
            }
            "--fuse-threads" => {
                index += 1;
                let threads = parse_usize(value(args, index, "--fuse-threads")?, "fuse_threads")?;
                if threads == 0 {
                    return Err(CliError::InvalidNumber {
                        field: "fuse_threads",
                        value: "0".to_owned(),
                    });
                }
                options.threads = threads;
                index += 1;
            }
            "--entry-ttl-ms" => {
                index += 1;
                options.entry_ttl = Duration::from_millis(parse_u64(
                    value(args, index, "--entry-ttl-ms")?,
                    "entry_ttl_ms",
                )?);
                index += 1;
            }
            "--attr-ttl-ms" => {
                index += 1;
                options.attr_ttl = Duration::from_millis(parse_u64(
                    value(args, index, "--attr-ttl-ms")?,
                    "attr_ttl_ms",
                )?);
                index += 1;
            }
            "--no-block-cache" => {
                options.block_cache = BlockCachePolicy::Off;
                index += 1;
            }
            "--block-cache" => {
                index += 1;
                set_block_cache_kind(&mut options, value(args, index, "--block-cache")?)?;
                index += 1;
            }
            "--block-cache-dir" => {
                index += 1;
                set_disk_block_cache_root(
                    &mut options,
                    PathBuf::from(value(args, index, "--block-cache-dir")?),
                );
                index += 1;
            }
            "--block-cache-bytes" => {
                index += 1;
                let max_bytes = parse_u64(
                    value(args, index, "--block-cache-bytes")?,
                    "block_cache_bytes",
                )?;
                set_block_cache_bytes(&mut options, max_bytes);
                index += 1;
            }
            "--block-cache-items" => {
                index += 1;
                let max_items = parse_usize(
                    value(args, index, "--block-cache-items")?,
                    "block_cache_items",
                )?;
                set_block_cache_items(&mut options, max_items);
                index += 1;
            }
            "--block-cache-ttl-ms" => {
                index += 1;
                let ttl_ms = parse_u64(
                    value(args, index, "--block-cache-ttl-ms")?,
                    "block_cache_ttl_ms",
                )?;
                let ttl = (ttl_ms > 0).then(|| Duration::from_millis(ttl_ms));
                set_block_cache_ttl(&mut options, ttl);
                index += 1;
            }
            "--no-prefetch" => {
                options.prefetch.enabled = false;
                index += 1;
            }
            "--prefetch-workers" => {
                index += 1;
                options.prefetch.workers = parse_usize(
                    value(args, index, "--prefetch-workers")?,
                    "prefetch_workers",
                )?;
                index += 1;
            }
            "--prefetch-queue-capacity" => {
                index += 1;
                options.prefetch.queue_capacity = parse_usize(
                    value(args, index, "--prefetch-queue-capacity")?,
                    "prefetch_queue_capacity",
                )?;
                index += 1;
            }
            "--max-readahead-bytes" => {
                index += 1;
                options.read_pipeline.max_readahead_bytes = parse_usize(
                    value(args, index, "--max-readahead-bytes")?,
                    "max_readahead_bytes",
                )?;
                index += 1;
            }
            "--no-writeback-cache" => {
                options.writeback.cache_enabled = false;
                index += 1;
            }
            "--writeback-cache" => {
                options.writeback.cache_enabled = true;
                index += 1;
            }
            "--writeback-async-publish" => {
                options.writeback.async_publish = true;
                index += 1;
            }
            "--writeback-cache-dir" => {
                index += 1;
                options.writeback.root =
                    PathBuf::from(value(args, index, "--writeback-cache-dir")?);
                index += 1;
            }
            "--writeback-cache-bytes" => {
                index += 1;
                options.writeback.max_bytes = parse_u64(
                    value(args, index, "--writeback-cache-bytes")?,
                    "writeback_cache_bytes",
                )?;
                index += 1;
            }
            "--writeback-cache-items" => {
                index += 1;
                options.writeback.max_items = parse_usize(
                    value(args, index, "--writeback-cache-items")?,
                    "writeback_cache_items",
                )?;
                index += 1;
            }
            "--writeback-workers" => {
                index += 1;
                options.writeback.workers = parse_usize(
                    value(args, index, "--writeback-workers")?,
                    "writeback_workers",
                )?;
                index += 1;
            }
            "--writeback-queue-capacity" => {
                index += 1;
                options.writeback.queue_capacity = parse_usize(
                    value(args, index, "--writeback-queue-capacity")?,
                    "writeback_queue_capacity",
                )?;
                index += 1;
            }
            "--writeback-upload-workers-per-request" => {
                index += 1;
                options.writeback.upload_workers_per_request = parse_usize(
                    value(args, index, "--writeback-upload-workers-per-request")?,
                    "writeback_upload_workers_per_request",
                )?;
                index += 1;
            }
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.to_owned()))
            }
            raw => {
                if mountpoint.replace(PathBuf::from(raw)).is_some() {
                    return Err(CliError::TooManyArguments);
                }
                index += 1;
            }
        }
    }
    if options.writeback.async_publish && !options.writeback.cache_enabled {
        // Async-publish acks on a durable cache+journal write; it cannot run on
        // the in-memory write buffer, so it requires --writeback-cache.
        return Err(CliError::InvalidOption {
            field: "writeback_async_publish",
            value: "requires --writeback-cache".to_owned(),
        });
    }
    let mountpoint = mountpoint.ok_or(CliError::MissingArgument("mountpoint"))?;
    Ok((options, mountpoint))
}

fn exact_args(args: &[String], expected: usize) -> Result<(), CliError> {
    if args.len() < expected {
        return Err(CliError::MissingArgument(
            match args.first().map(String::as_str) {
                Some("mkdir") | Some("ls") | Some("cat") | Some("rm") | Some("rmdir") => "path",
                Some("register-graft") | Some("unregister-graft") => "prefix",
                Some("put-artifact") => "path and source",
                Some("snapshot") => "path",
                Some("cat-snapshot") => "snapshot id and path",
                Some("retire-snapshot") => "snapshot id",
                Some("mount-snapshot") => "snapshot id and mountpoint",
                Some("rename") | Some("rename-replace") => "source and destination",
                Some("clone") => "source and destination paths",
                Some("diff") => "two paths",
                Some("rollback") => "path and snapshot id",
                Some("mount") => "mountpoint",
                _ => "argument",
            },
        ));
    }
    if args.len() > expected {
        return Err(CliError::TooManyArguments);
    }
    Ok(())
}

fn value<'a>(args: &'a [String], index: usize, option: &'static str) -> Result<&'a str, CliError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(CliError::MissingValue(option))
}

fn parse_u16(raw: &str, field: &'static str) -> Result<u16, CliError> {
    raw.parse::<u16>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_u32(raw: &str, field: &'static str) -> Result<u32, CliError> {
    raw.parse::<u32>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_u64(raw: &str, field: &'static str) -> Result<u64, CliError> {
    raw.parse::<u64>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_usize(raw: &str, field: &'static str) -> Result<usize, CliError> {
    raw.parse::<usize>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn parse_socket_addr(raw: &str, field: &'static str) -> Result<SocketAddr, CliError> {
    raw.parse::<SocketAddr>()
        .map_err(|_| CliError::InvalidAddress {
            field,
            value: raw.to_owned(),
        })
}

fn default_manifest_id(path: &str) -> Result<String, CliError> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(CliError::MissingArgument("artifact path"));
    }
    Ok(format!("artifacts/{trimmed}"))
}

fn artifact_digest_reader(reader: &mut impl Read) -> Result<String, CliError> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer).map_err(from_io)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn subtree_delta_label(kind: SubtreeDeltaKind) -> &'static str {
    match kind {
        SubtreeDeltaKind::Added => "A",
        SubtreeDeltaKind::Modified => "M",
        SubtreeDeltaKind::Removed => "D",
    }
}

fn file_type_label(file_type: FileType) -> &'static str {
    match file_type {
        FileType::File => "file",
        FileType::Directory => "dir",
        FileType::Symlink => "symlink",
        FileType::NamedPipe => "fifo",
        FileType::CharDevice => "char",
        FileType::BlockDevice => "block",
        FileType::Socket => "socket",
    }
}

fn from_io(err: impl Error) -> CliError {
    CliError::Io(err.to_string())
}

fn from_client(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}

fn from_object(err: impl Error) -> CliError {
    CliError::Client(err.to_string())
}
fn run_mcp(client: Client, options: McpCliOptions, uid: u32, gid: u32) -> std::io::Result<()> {
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    run_mcp_stream_with_options(client, options, uid, gid, stdin.lock(), stdout.lock())
}

#[cfg(test)]
fn run_mcp_stream<O>(
    client: nokv_client::NoKvFsClient<O>,
    mut reader: impl std::io::BufRead,
    mut writer: impl std::io::Write,
) -> std::io::Result<()>
where
    O: nokv_object::ObjectStore + Send + Sync + 'static,
{
    run_mcp_stream_with_options(
        client,
        McpCliOptions::default(),
        DEFAULT_UID,
        DEFAULT_GID,
        &mut reader,
        &mut writer,
    )
}

fn run_mcp_stream_with_options<O>(
    client: nokv_client::NoKvFsClient<O>,
    options: McpCliOptions,
    uid: u32,
    gid: u32,
    mut reader: impl std::io::BufRead,
    mut writer: impl std::io::Write,
) -> std::io::Result<()>
where
    O: nokv_object::ObjectStore + Send + Sync + 'static,
{
    use serde_json::json;

    #[derive(serde::Deserialize)]
    struct JsonRpcRequest {
        jsonrpc: Option<String>,
        #[serde(default, deserialize_with = "deserialize_some")]
        id: Option<Option<serde_json::Value>>,
        method: Option<String>,
        params: Option<serde_json::Value>,
    }

    fn deserialize_some<'de, D>(
        deserializer: D,
    ) -> Result<Option<Option<serde_json::Value>>, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer).map(Some)
    }

    #[derive(serde::Deserialize)]
    struct CallToolParams {
        name: String,
        arguments: Option<serde_json::Value>,
    }

    fn ok_response(id: Option<serde_json::Value>, result: serde_json::Value) -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": id, "result": result })
    }

    fn err_response(id: Option<serde_json::Value>, code: i64, message: &str) -> serde_json::Value {
        json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
    }

    let workbench_options = match options.profile {
        McpProfile::Agent => None,
        McpProfile::Workbench => Some(workbench_mcp::WorkbenchMcpOptions {
            root: options
                .workbench_root
                .clone()
                .expect("workbench profile requires a root"),
            max_bytes: options.workbench_max_bytes,
            uid,
            gid,
        }),
    };

    let mut line = String::new();
    while reader.read_line(&mut line)? > 0 {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            let maybe_response = match serde_json::from_str::<serde_json::Value>(trimmed) {
                Err(_) => Some(err_response(None, -32700, "Parse error")),
                Ok(raw) => {
                    if raw.is_array() {
                        Some(err_response(
                            None,
                            -32600,
                            "Batch requests are not supported",
                        ))
                    } else if !raw.is_object() {
                        Some(err_response(None, -32600, "Invalid Request"))
                    } else {
                        match serde_json::from_value::<JsonRpcRequest>(raw) {
                            Err(_) => Some(err_response(None, -32600, "Invalid Request")),
                            Ok(req) => {
                                let id = req.id.clone().flatten();
                                if req.jsonrpc.as_deref() != Some("2.0") {
                                    Some(err_response(
                                        id,
                                        -32600,
                                        "Invalid Request: jsonrpc must be \"2.0\"",
                                    ))
                                } else if req.method.is_none() {
                                    Some(err_response(
                                        id,
                                        -32600,
                                        "Invalid Request: missing method",
                                    ))
                                } else {
                                    let is_notification = req.id.is_none();
                                    let method = req.method.clone().unwrap();
                                    let result = match method.as_str() {
                                        "initialize" => {
                                            // Ordered latest-first: index 0 is the newest revision
                                            // we speak. Per the MCP spec, `initialize` negotiates a
                                            // protocol version rather than rejecting the handshake:
                                            // when the client requests a version we recognize we
                                            // echo it back, otherwise we respond with our latest
                                            // supported version and let the client decide whether it
                                            // can proceed. Failing on an unknown version broke newer
                                            // clients (e.g. the mcp SDK requesting "2025-11-25").
                                            const SUPPORTED_PROTOCOL_VERSIONS: &[&str] = &[
                                                "2025-11-25",
                                                "2025-06-18",
                                                "2025-03-26",
                                                "2024-11-05",
                                            ];
                                            let requested = req
                                                .params
                                                .as_ref()
                                                .and_then(|p| p.get("protocolVersion"))
                                                .and_then(|v| v.as_str());
                                            let negotiated = match requested {
                                                Some(v)
                                                    if SUPPORTED_PROTOCOL_VERSIONS.contains(&v) =>
                                                {
                                                    v
                                                }
                                                _ => SUPPORTED_PROTOCOL_VERSIONS[0],
                                            };
                                            Ok(json!({
                                                "protocolVersion": negotiated,
                                                "capabilities": { "tools": {} },
                                                "serverInfo": { "name": "nokv-mcp", "version": "0.1.0" }
                                            }))
                                        }
                                        "initialized" | "ping" => Ok(json!({})),
                                        "tools/list" => {
                                            let definitions = match options.profile {
                                                McpProfile::Agent => {
                                                    nokv_agent::agent_tool_definitions()
                                                }
                                                McpProfile::Workbench => {
                                                    workbench_mcp::tool_definitions()
                                                }
                                            };
                                            let tools: Vec<serde_json::Value> = definitions
                                                .into_iter()
                                                .map(|t| {
                                                    json!({
                                                        "name": t.name,
                                                        "description": t.description,
                                                        "inputSchema": t.parameters,
                                                    })
                                                })
                                                .collect();
                                            Ok(json!({ "tools": tools }))
                                        }
                                        "tools/call" => {
                                            let params = req.params.clone().unwrap_or(json!({}));
                                            match serde_json::from_value::<CallToolParams>(params) {
                                                Err(e) => Err((
                                                    -32602_i64,
                                                    format!("Invalid params: {e}"),
                                                )),
                                                Ok(call) => {
                                                    let args = call.arguments.unwrap_or(json!({}));
                                                    let tool_result = match options.profile {
                                                        McpProfile::Agent => {
                                                            nokv_agent::execute_agent_tool(
                                                                &client, &call.name, &args,
                                                            )
                                                            .map_err(|err| err.to_string())
                                                        }
                                                        McpProfile::Workbench => {
                                                            workbench_mcp::execute_tool(
                                                                &client,
                                                                workbench_options
                                                                    .as_ref()
                                                                    .expect("workbench options"),
                                                                &call.name,
                                                                &args,
                                                            )
                                                            .map_err(|err| err.to_string())
                                                        }
                                                    };
                                                    match tool_result {
                                                        Ok(val) => Ok(json!({
                                                            "content": [{ "type": "text", "text": serde_json::to_string_pretty(&val).unwrap_or_default() }],
                                                            "structuredContent": val,
                                                        })),
                                                        Err(err) => Ok(json!({
                                                            "content": [{ "type": "text", "text": err.to_string() }],
                                                            "isError": true
                                                        })),
                                                    }
                                                }
                                            }
                                        }
                                        other => {
                                            Err((-32601_i64, format!("Method not found: {other}")))
                                        }
                                    };
                                    if is_notification {
                                        None
                                    } else {
                                        Some(match result {
                                            Ok(val) => ok_response(id, val),
                                            Err((code, msg)) => err_response(id, code, &msg),
                                        })
                                    }
                                }
                            }
                        }
                    }
                }
            };
            if let Some(response) = maybe_response {
                serde_json::to_writer(&mut writer, &response)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
        }
        line.clear();
    }
    Ok(())
}

fn print_help(out: &mut impl Write) -> io::Result<()> {
    writeln!(
        out,
        "NoKV metadata client/server CLI\n\
\n\
Usage:\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] init\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mkdir PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] register-graft PREFIX\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] unregister-graft PREFIX\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] reconcile-grafts\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] put-artifact PATH SOURCE\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] ls PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] cat PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rm PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rmdir PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rename SOURCE DESTINATION\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rename-replace SOURCE DESTINATION\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mount [--read-only] [--no-kernel-cache] [--direct-io] [--entry-ttl-ms MS] [--attr-ttl-ms MS] [--writeback-cache] MOUNTPOINT\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mount-snapshot SNAPSHOT_ID [--no-kernel-cache] [--direct-io] [--entry-ttl-ms MS] [--attr-ttl-ms MS] [--writeback-cache] MOUNTPOINT\n\
  nokv [--meta PATH] [--object-backend s3|rustfs] [--mount ID] [--control-backend etcd] serve\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] mcp [--profile agent|workbench] [--workbench-root PATH] [--workbench-max-bytes BYTES]\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] backup\n\
  nokv [--meta PATH] [--object-backend s3|rustfs] [--mount ID] restore\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] fsck\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] gc [LIMIT]\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] snapshot PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] clone SRC_PATH DST_PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] diff PATH_A PATH_B\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] rollback PATH SNAPSHOT_ID\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] cat-snapshot SNAPSHOT_ID PATH\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] retire-snapshot SNAPSHOT_ID\n\
  nokv [--server-bind ADDR] [--object-backend s3|rustfs] [--mount ID] renew-snapshot SNAPSHOT_ID [LEASE_MS]\n\
\n\
Object backends:\n\
  --object-backend s3|rustfs\n\
  --s3-bucket BUCKET                S3/RustFS bucket\n\
  --s3-endpoint URL                 S3/RustFS endpoint\n\
  --s3-region REGION                S3 region; RustFS commonly uses auto\n\
  --s3-access-key-id KEY\n\
  --s3-secret-access-key SECRET\n\
  --s3-root PREFIX\n\
  --hot-object-root PATH           Local NVMe-shaped hot object tier; cold S3/RustFS remains durable truth\n\
  --hot-object-max-bytes BYTES     Maximum local hot-tier bytes before LRU eviction\n\
  --hot-fill-mode inline|background  Cold-read hot fill mode; default inline\n\
  --object-gc-interval-ms MS       Background object GC interval for live mount\n\
  --object-gc-limit LIMIT          Max queued object records per GC iteration\n\
  --history-gc-interval-ms MS      Background metadata history GC interval for live mount\n\
  --history-gc-limit LIMIT         Max history records removed per GC iteration\n\
  --server-bind ADDR              Metadata service address for client commands and serve bind\n\
\n\
Control plane options:\n\
  --control-backend none|etcd       Shard ownership backend for serve; default none\n\
  --control-etcd-endpoints URLS     Comma-separated etcd endpoints; implies --control-backend etcd\n\
  --control-etcd-prefix PREFIX      etcd key prefix; default /nokv/control\n\
  --control-etcd-lease-ttl-seconds N\n\
                                    etcd owner lease TTL in seconds; default 10\n\
  --shard-id ID                     Metadata shard id; default mount-<mount>:/\n\
  --shard-index N                   Stable shard index this process registers and owns;\n\
                                    default adopts the registered index (0 for the default shard).\n\
                                    Set per process to run a multi-shard fleet over one mount.\n\
  --node-id ID                      Owner node id; default server bind address\n\
  --failover-from-epoch N           Acquire the shard after failed owner epoch N\n\
  --no-shard-owner-renewal          Disable background owner lease renewal\n\
  --shard-owner-renewal-interval-ms MS\n\
                                    Background owner lease renewal interval\n\
  --metadata-shared-log-prefix PREFIX\n\
                                    Object prefix for synchronous metadata log archive; requires control backend\n\
\n\
Mount cache options:\n\
  --no-kernel-cache              Do not ask FUSE to keep file/directory cache on open\n\
  --direct-io                    Ask FUSE to bypass kernel page cache for file handles\n\
  --entry-ttl-ms MS              Kernel dentry cache TTL\n\
  --attr-ttl-ms MS               Kernel attribute cache TTL\n\
  --stats-bind ADDR              Optional mount-local HTTP stats bind address\n\
  --fuse-threads N              FUSE worker thread count for the mount\n\
  --no-block-cache               Disable NoKV object block cache\n\
  --block-cache off|memory|disk  Select NoKV object block cache policy\n\
  --block-cache-dir PATH         Directory for disk object block cache\n\
  --block-cache-bytes N          Max object block cache bytes\n\
  --block-cache-items N          Max object block cache entries\n\
  --block-cache-ttl-ms MS        Object block cache TTL; 0 means no TTL\n\
  --no-prefetch                  Disable object prefetch\n\
  --prefetch-workers N           Object prefetch worker count\n\
  --prefetch-queue-capacity N    Object prefetch queue capacity\n\
  --max-readahead-bytes N        Max sequential read-ahead bytes per hint\n\
  --writeback-cache              Stage completed write blocks in the disk cache before object upload\n\
  --no-writeback-cache           Use the default in-memory write buffer and direct object upload\n\
  --writeback-async-publish      Ack flush/close on the durable cache+journal; upload+publish in the background (requires --writeback-cache)\n\
  --writeback-cache-dir PATH     Directory for staged object upload cache\n\
  --writeback-cache-bytes N      Max staged object upload cache bytes\n\
  --writeback-cache-items N      Max staged object upload cache entries\n\
  --writeback-workers N          Background staged upload worker count\n\
  --writeback-queue-capacity N   Background staged upload queue capacity\n\
  --writeback-upload-workers-per-request N\n\
                                  Parallel object PUT workers per staged upload request\n\
\n\
Defaults:\n\
  --meta .nokv/meta\n\
  --object-backend rustfs\n\
  --s3-bucket nokv\n\
  --s3-endpoint http://127.0.0.1:9000\n\
  --object-gc-interval-ms 30000\n\
  --object-gc-limit 1024\n\
  --history-gc-interval-ms 30000\n\
  --history-gc-limit 1024\n\
  --server-bind 127.0.0.1:7777\n\
  --control-backend none\n\
  --metadata-checkpoint-archive-prefix metadata/checkpoints\n\
  --no-metadata-checkpoint-archive\n\
  --mount 1"
    )
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(option) => write!(f, "{option} requires a value"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::UnknownCommand(command) => write!(f, "unknown command {command}"),
            Self::MissingArgument(arg) => write!(f, "missing {arg}"),
            Self::TooManyArguments => write!(f, "too many arguments"),
            Self::InvalidMount(value) => write!(f, "invalid mount id {value}"),
            Self::InvalidOption { field, value } => write!(f, "invalid {field} option {value}"),
            Self::InvalidAddress { field, value } => write!(f, "invalid {field} address {value}"),
            Self::InvalidNumber { field, value } => write!(f, "invalid {field} value {value}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Client(err) => write!(f, "{err}"),
        }
    }
}

impl Error for CliError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    use nokv_client::NoKvFsClient;
    use nokv_object::{LocalObjectStore, MemoryObjectStore, ObjectKey, ObjectStore};
    use tempfile::tempdir;

    fn s(value: &str) -> String {
        value.to_owned()
    }

    fn fake_s3_options() -> S3ObjectStoreOptions {
        S3ObjectStoreOptions {
            bucket: "test".to_owned(),
            root: "/".to_owned(),
            region: "auto".to_owned(),
            endpoint: Some("http://127.0.0.1:1".to_owned()),
            access_key_id: Some("test".to_owned()),
            secret_access_key: Some("test".to_owned()),
            session_token: None,
            virtual_host_style: false,
            skip_signature: true,
        }
    }

    fn fake_server_object_config() -> ObjectStoreConfig {
        ObjectStoreConfig::s3(fake_s3_options())
    }

    fn spawn_test_server() -> SocketAddr {
        spawn_test_server_with_object_config(fake_server_object_config())
    }

    fn spawn_test_server_with_object_config(object: ObjectStoreConfig) -> SocketAddr {
        let dir = tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = listener.local_addr().unwrap();
        let server = nokv_server::Server::open(ServerOptions {
            bind,
            mount: MountId::new(1).unwrap(),
            meta_path: dir.path().join("meta"),
            metadata_checkpoint_archive_prefix: None,
            object,
            uid: 1000,
            gid: 1000,
            object_gc: ObjectGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
                read_lease_grace: ObjectGcOptions::default().read_lease_grace,
            },
            history_gc: HistoryGcOptions {
                interval: Duration::from_secs(3600),
                limit: 128,
                run_immediately: false,
            },
            control: None,
        })
        .unwrap();
        thread::spawn(move || {
            let _dir = dir;
            let _ = server.serve(listener);
        });
        bind
    }

    #[test]
    fn parse_defaults_to_rustfs() {
        let (config, command) = parse(vec![s("ls"), s("/")]).unwrap();
        assert_eq!(config.meta, PathBuf::from(".nokv/meta"));
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.mount.get(), 1);
        assert_eq!(
            config.metadata_checkpoint_archive_prefix.as_deref(),
            Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX)
        );
        assert_eq!(config.server_bind, DEFAULT_SERVER_BIND);
        assert_eq!(config.control, None);
        assert_eq!(command, Command::Ls { path: s("/") });
    }

    #[test]
    fn service_sdk_round_trip_uses_server_metadata_and_client_object_store() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store.clone());
        let body = b"hello metadata server";
        let entry = client
            .put_artifact(
                "/checkpoint.bin",
                body.to_vec(),
                ArtifactMetadata {
                    producer: "cli-test".to_owned(),
                    digest_uri: "sha256:demo".to_owned(),
                    content_type: "application/octet-stream".to_owned(),
                    manifest_id: "checkpoint.bin".to_owned(),
                    mode: DEFAULT_MODE_FILE,
                    uid: DEFAULT_UID,
                    gid: DEFAULT_GID,
                },
            )
            .unwrap();
        assert_eq!(entry.attr.size, body.len() as u64);
        assert!(store
            .head(
                &ObjectKey::new(format!(
                    "blocks/1/{}/{}/0/0",
                    entry.attr.inode.get(),
                    entry.attr.generation
                ))
                .unwrap()
            )
            .unwrap()
            .is_some());
        assert_eq!(client.cat("/checkpoint.bin").unwrap(), body);
        let listed = client.metadata().list("/").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].attr.inode, entry.attr.inode);
    }

    #[test]
    fn parse_global_options_before_command() {
        let (config, command) = parse(vec![
            s("--meta"),
            s("/tmp/meta"),
            s("--object-backend"),
            s("rustfs"),
            s("--mount"),
            s("7"),
            s("--object-gc-interval-ms"),
            s("50"),
            s("--object-gc-limit"),
            s("9"),
            s("--history-gc-interval-ms"),
            s("60"),
            s("--history-gc-limit"),
            s("11"),
            s("--server-bind"),
            s("127.0.0.1:17777"),
            s("mkdir"),
            s("/runs"),
        ])
        .unwrap();
        assert_eq!(config.meta, PathBuf::from("/tmp/meta"));
        assert_eq!(config.mount.get(), 7);
        assert_eq!(config.object_gc_interval, Duration::from_millis(50));
        assert_eq!(config.object_gc_limit, 9);
        assert_eq!(config.history_gc_interval, Duration::from_millis(60));
        assert_eq!(config.history_gc_limit, 11);
        assert_eq!(
            config.server_bind,
            "127.0.0.1:17777".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(command, Command::Mkdir { path: s("/runs") });
    }

    #[test]
    fn parse_etcd_control_options() {
        let (config, command) = parse(vec![
            s("--mount"),
            s("7"),
            s("--server-bind"),
            s("127.0.0.1:17777"),
            s("--control-etcd-endpoints"),
            s("http://127.0.0.1:2379,http://127.0.0.2:2379"),
            s("--control-etcd-prefix"),
            s("/nokv/test"),
            s("--control-etcd-lease-ttl-seconds"),
            s("3"),
            s("--shard-id"),
            s("mount-7:/datasets/train"),
            s("--shard-index"),
            s("3"),
            s("--node-id"),
            s("node-a"),
            s("--failover-from-epoch"),
            s("4"),
            s("--no-shard-owner-renewal"),
            s("--metadata-shared-log-prefix"),
            s("metadata/shared-log"),
            s("serve"),
        ])
        .unwrap();
        assert_eq!(command, Command::Serve);
        let control = config.control.unwrap();
        let ServerControlStoreOptions::Etcd(store) = control.store;
        assert_eq!(
            store.endpoints(),
            &[
                "http://127.0.0.1:2379".to_owned(),
                "http://127.0.0.2:2379".to_owned(),
            ]
        );
        assert_eq!(store.key_prefix(), "/nokv/test");
        assert_eq!(store.lease_ttl_seconds(), 3);
        assert_eq!(
            control.shard_owner.shard_id.as_str(),
            "mount-7:/datasets/train"
        );
        assert_eq!(control.shard_owner.node_id.as_str(), "node-a");
        assert_eq!(
            control.shard_owner.acquisition,
            nokv_server::ServerShardAcquisition::Failover { previous_epoch: 4 }
        );
        assert_eq!(control.shard_owner.shard_index, Some(3));
        assert_eq!(control.shard_owner.renewal, None);
        assert_eq!(
            control.shard_owner.shared_log.unwrap().archive_prefix,
            "metadata/shared-log"
        );
    }

    #[test]
    fn parse_etcd_control_defaults_owner_identity() {
        let (config, command) = parse(vec![
            s("--mount"),
            s("9"),
            s("--server-bind"),
            s("127.0.0.1:19999"),
            s("--control-backend"),
            s("etcd"),
            s("--control-etcd-endpoints"),
            s("http://127.0.0.1:2379"),
            s("serve"),
        ])
        .unwrap();
        assert_eq!(command, Command::Serve);
        let control = config.control.unwrap();
        assert_eq!(control.shard_owner.shard_id.as_str(), "mount-9:/");
        assert_eq!(control.shard_owner.node_id.as_str(), "127.0.0.1:19999");
        assert_eq!(control.shard_owner.shard_index, None);
        assert_eq!(
            control.shard_owner.acquisition,
            nokv_server::ServerShardAcquisition::Fresh
        );
        // The etcd backend couples renewal to the lease TTL (default 10s): renew at
        // ttl/3 so a short TTL never lapses, and arm the self-fence at the TTL.
        let renewal = control.shard_owner.renewal.unwrap();
        assert_eq!(renewal.lease_ttl, Duration::from_secs(10));
        // ttl/3 (capped down from the 5s default) so the lease never lapses.
        assert_eq!(renewal.interval, Duration::from_secs(10) / 3);
        assert!(renewal.interval <= renewal.lease_ttl / 3);
        assert!(!renewal.run_immediately);
    }

    #[test]
    fn parse_shared_log_requires_control_backend() {
        assert!(matches!(
            parse(vec![
                s("--metadata-shared-log-prefix"),
                s("metadata/shared-log"),
                s("serve"),
            ]),
            Err(CliError::InvalidOption {
                field: "control",
                ..
            })
        ));
    }

    #[test]
    fn parse_rejects_zero_object_gc_interval() {
        assert!(matches!(
            parse(vec![
                s("--object-gc-interval-ms"),
                s("0"),
                s("mount"),
                s("/tmp/nokv")
            ]),
            Err(CliError::InvalidNumber {
                field: "object_gc_interval_ms",
                ..
            })
        ));
    }

    #[test]
    fn parse_rejects_zero_history_gc_interval() {
        assert!(matches!(
            parse(vec![
                s("--history-gc-interval-ms"),
                s("0"),
                s("mount"),
                s("/tmp/nokv")
            ]),
            Err(CliError::InvalidNumber {
                field: "history_gc_interval_ms",
                ..
            })
        ));
    }

    #[test]
    fn parse_rejects_invalid_server_bind() {
        assert!(matches!(
            parse(vec![s("--server-bind"), s("localhost"), s("serve")]),
            Err(CliError::InvalidAddress {
                field: "server_bind",
                ..
            })
        ));
    }

    #[test]
    fn parse_rustfs_object_options() {
        let (config, command) = parse(vec![
            s("--object-backend"),
            s("rustfs"),
            s("--s3-bucket"),
            s("nokv"),
            s("--s3-endpoint"),
            s("http://127.0.0.1:9000"),
            s("--s3-access-key-id"),
            s("access"),
            s("--s3-secret-access-key"),
            s("secret"),
            s("init"),
        ])
        .unwrap();
        assert_eq!(command, Command::Init);
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.region, "auto");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(options.access_key_id.as_deref(), Some("access"));
        assert_eq!(options.secret_access_key.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_s3_does_not_inherit_rustfs_endpoint() {
        let (config, command) = parse(vec![
            s("--s3-bucket"),
            s("training-artifacts"),
            s("--object-backend"),
            s("s3"),
            s("init"),
        ])
        .unwrap();
        assert_eq!(command, Command::Init);
        let options = config.object.options();
        assert_eq!(options.bucket, "training-artifacts");
        assert_eq!(options.region, "us-east-1");
        assert_eq!(options.endpoint, None);
        assert_eq!(options.access_key_id, None);
        assert_eq!(options.secret_access_key, None);
    }

    #[test]
    fn parse_hot_object_root_keeps_cold_backend_options() {
        let (config, command) = parse(vec![
            s("--object-backend"),
            s("rustfs"),
            s("--hot-object-root"),
            s("/mnt/nvme/nokv-hot"),
            s("--hot-object-max-bytes"),
            s("1048576"),
            s("--hot-fill-mode"),
            s("background"),
            s("init"),
        ])
        .unwrap();
        assert_eq!(command, Command::Init);
        assert_eq!(
            config.object.local_hot_root().map(PathBuf::from),
            Some(PathBuf::from("/mnt/nvme/nokv-hot"))
        );
        assert_eq!(
            config
                .object
                .local_hot_options()
                .and_then(|options| options.max_bytes),
            Some(1_048_576)
        );
        assert_eq!(
            config.object.tiered_options(),
            Some(TieredObjectStoreOptions {
                hot_fill_mode: HotFillMode::Background,
                ..TieredObjectStoreOptions::default()
            })
        );
        let options = config.object.options();
        assert_eq!(options.bucket, "nokv");
        assert_eq!(options.region, "auto");
        assert_eq!(options.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
    }

    #[test]
    fn default_manifest_id_is_relative_and_stable() {
        assert_eq!(
            default_manifest_id("/runs/1/checkpoint").unwrap(),
            "artifacts/runs/1/checkpoint"
        );
        assert_eq!(
            default_manifest_id("runs/1/checkpoint").unwrap(),
            "artifacts/runs/1/checkpoint"
        );
    }

    #[test]
    fn artifact_digest_reader_is_sha256_uri() {
        let mut reader = io::Cursor::new(b"body".to_vec());
        assert_eq!(
            artifact_digest_reader(&mut reader).unwrap(),
            "sha256:230d8358dc8e8890b4c58deeb62912ee2f20357ae92a5cc861b98e68fe31acb5"
        );
    }

    #[test]
    fn parse_mount_command() {
        let (_config, command) = parse(vec![s("mount"), s("/tmp/nokv")]).unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv"),
                options: MountCliOptions::default(),
            }
        );
        let (_config, command) =
            parse(vec![s("mount"), s("--read-only"), s("/tmp/nokv-ro")]).unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-ro"),
                options: MountCliOptions {
                    read_only: true,
                    ..MountCliOptions::default()
                },
            }
        );
        let (_config, command) = parse(vec![
            s("mount"),
            s("--no-kernel-cache"),
            s("--direct-io"),
            s("--stats-bind"),
            s("127.0.0.1:0"),
            s("--fuse-threads"),
            s("8"),
            s("--entry-ttl-ms"),
            s("0"),
            s("--attr-ttl-ms"),
            s("250"),
            s("/tmp/nokv-cache"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-cache"),
                options: MountCliOptions {
                    kernel_cache: false,
                    direct_io: true,
                    stats_bind: Some("127.0.0.1:0".parse().unwrap()),
                    threads: 8,
                    entry_ttl: Duration::from_millis(0),
                    attr_ttl: Duration::from_millis(250),
                    ..MountCliOptions::default()
                },
            }
        );
        let read_cache_options = MountCliOptions {
            block_cache: BlockCachePolicy::Disk(DiskBlockCacheOptions {
                root: PathBuf::from("/tmp/nokv-block-cache"),
                max_bytes: 8192,
                max_items: 64,
                ttl: Some(Duration::from_millis(5000)),
            }),
            prefetch: nokv_fuse::FusePrefetchOptions {
                enabled: false,
                workers: 3,
                queue_capacity: 16,
            },
            read_pipeline: FileReadPipelineOptions {
                max_readahead_bytes: 2 * 1024 * 1024,
            },
            ..MountCliOptions::default()
        };
        let (_config, command) = parse(vec![
            s("mount"),
            s("--block-cache"),
            s("disk"),
            s("--block-cache-dir"),
            s("/tmp/nokv-block-cache"),
            s("--block-cache-bytes"),
            s("8192"),
            s("--block-cache-items"),
            s("64"),
            s("--block-cache-ttl-ms"),
            s("5000"),
            s("--no-prefetch"),
            s("--prefetch-workers"),
            s("3"),
            s("--prefetch-queue-capacity"),
            s("16"),
            s("--max-readahead-bytes"),
            s("2097152"),
            s("/tmp/nokv-read-cache-mount"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-read-cache-mount"),
                options: read_cache_options,
            }
        );
        let writeback_options = MountCliOptions {
            writeback: nokv_fuse::FuseWritebackOptions {
                cache_enabled: true,
                root: PathBuf::from("/tmp/nokv-writeback"),
                max_bytes: 4096,
                max_items: 32,
                queue_capacity: 128,
                workers: 4,
                upload_workers_per_request: 2,
                async_publish: false,
            },
            ..MountCliOptions::default()
        };
        let (_config, command) = parse(vec![
            s("mount"),
            s("--writeback-cache"),
            s("--writeback-cache-dir"),
            s("/tmp/nokv-writeback"),
            s("--writeback-cache-bytes"),
            s("4096"),
            s("--writeback-cache-items"),
            s("32"),
            s("--writeback-workers"),
            s("4"),
            s("--writeback-queue-capacity"),
            s("128"),
            s("--writeback-upload-workers-per-request"),
            s("2"),
            s("/tmp/nokv-writeback-mount"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::Mount {
                mountpoint: PathBuf::from("/tmp/nokv-writeback-mount"),
                options: writeback_options,
            }
        );
        let (_config, command) = parse(vec![
            s("mount-snapshot"),
            s("42"),
            s("--direct-io"),
            s("/tmp/nokv-ro"),
        ])
        .unwrap();
        assert_eq!(
            command,
            Command::MountSnapshot {
                snapshot_id: 42,
                mountpoint: PathBuf::from("/tmp/nokv-ro"),
                options: MountCliOptions {
                    direct_io: true,
                    ..MountCliOptions::default()
                },
            }
        );
        assert!(matches!(
            parse(vec![s("mount-snapshot"), s("bad"), s("/tmp/nokv-ro")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
    }

    #[test]
    fn parse_mount_enables_async_publish_with_cache() {
        let (_config, command) = parse(vec![
            s("mount"),
            s("--writeback-cache"),
            s("--writeback-async-publish"),
            s("/tmp/nokv-async"),
        ])
        .unwrap();
        let Command::Mount { options, .. } = command else {
            panic!("expected mount command");
        };
        assert!(options.writeback.cache_enabled);
        assert!(options.writeback.async_publish);
    }

    #[test]
    fn parse_mount_rejects_async_publish_without_cache() {
        assert!(matches!(
            parse(vec![
                s("mount"),
                s("--writeback-async-publish"),
                s("/tmp/nokv-async"),
            ]),
            Err(CliError::InvalidOption {
                field: "writeback_async_publish",
                ..
            })
        ));
    }

    #[test]
    fn parse_mount_rejects_zero_fuse_threads() {
        assert!(matches!(
            parse(vec![
                s("mount"),
                s("--fuse-threads"),
                s("0"),
                s("/tmp/nokv")
            ]),
            Err(CliError::InvalidNumber {
                field: "fuse_threads",
                ..
            })
        ));
    }

    #[test]
    fn parse_serve_command() {
        let command = parse(vec![s("serve")]).unwrap().1;
        assert_eq!(command, Command::Serve);
        assert!(matches!(
            parse(vec![s("serve"), s("extra")]),
            Err(CliError::TooManyArguments)
        ));
    }

    #[test]
    fn parse_gc_command() {
        assert_eq!(
            parse(vec![s("gc")]).unwrap().1,
            Command::Gc {
                limit: DEFAULT_GC_LIMIT
            }
        );
        assert_eq!(
            parse(vec![s("gc"), s("7")]).unwrap().1,
            Command::Gc { limit: 7 }
        );
        assert!(matches!(
            parse(vec![s("gc"), s("bad")]),
            Err(CliError::InvalidNumber { field: "limit", .. })
        ));
    }

    #[test]
    fn parse_clone_and_diff_commands() {
        assert_eq!(
            parse(vec![s("clone"), s("/base"), s("/forks/agent-1")])
                .unwrap()
                .1,
            Command::Clone {
                src: s("/base"),
                dst: s("/forks/agent-1"),
            }
        );
        assert_eq!(
            parse(vec![s("diff"), s("/base"), s("/forks/agent-1")])
                .unwrap()
                .1,
            Command::Diff {
                a: s("/base"),
                b: s("/forks/agent-1"),
            }
        );
        assert!(matches!(
            parse(vec![s("clone"), s("/base")]),
            Err(CliError::MissingArgument("source and destination paths"))
        ));
        assert!(matches!(
            parse(vec![s("diff"), s("/base"), s("/b"), s("/c")]),
            Err(CliError::TooManyArguments)
        ));
    }

    #[test]
    fn parse_rollback_command() {
        assert_eq!(
            parse(vec![s("rollback"), s("/base"), s("42")]).unwrap().1,
            Command::Rollback {
                path: s("/base"),
                snapshot_id: 42,
            }
        );
        assert!(matches!(
            parse(vec![s("rollback"), s("/base")]),
            Err(CliError::MissingArgument("path and snapshot id"))
        ));
        assert!(matches!(
            parse(vec![s("rollback"), s("/base"), s("bad")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("rollback"), s("/base"), s("1"), s("2")]),
            Err(CliError::TooManyArguments)
        ));
    }

    #[test]
    fn parse_snapshot_commands() {
        assert_eq!(
            parse(vec![s("snapshot"), s("/runs")]).unwrap().1,
            Command::Snapshot { path: s("/runs") }
        );
        assert_eq!(
            parse(vec![s("cat-snapshot"), s("42"), s("/runs/checkpoint")])
                .unwrap()
                .1,
            Command::CatSnapshot {
                snapshot_id: 42,
                path: s("/runs/checkpoint")
            }
        );
        assert_eq!(
            parse(vec![s("retire-snapshot"), s("42")]).unwrap().1,
            Command::RetireSnapshot { snapshot_id: 42 }
        );
        assert!(matches!(
            parse(vec![s("cat-snapshot"), s("bad"), s("/runs/checkpoint")]),
            Err(CliError::InvalidNumber {
                field: "snapshot_id",
                ..
            })
        ));
    }

    #[test]
    fn rejects_removed_local_object_options() {
        assert!(matches!(
            parse(vec![s("--object-backend"), s("local"), s("init")]),
            Err(CliError::UnknownOption(_))
        ));
        assert!(matches!(
            parse(vec![s("--objects"), s("/tmp/objects"), s("init")]),
            Err(CliError::UnknownOption(_))
        ));
    }

    #[test]
    fn parse_mutation_commands() {
        assert_eq!(
            parse(vec![s("rm"), s("/runs/a")]).unwrap().1,
            Command::Rm { path: s("/runs/a") }
        );
        assert_eq!(
            parse(vec![s("rmdir"), s("/runs")]).unwrap().1,
            Command::Rmdir { path: s("/runs") }
        );
        assert_eq!(
            parse(vec![s("rename"), s("/a"), s("/b")]).unwrap().1,
            Command::Rename {
                source: s("/a"),
                destination: s("/b")
            }
        );
        assert_eq!(
            parse(vec![s("rename-replace"), s("/stage"), s("/final")])
                .unwrap()
                .1,
            Command::RenameReplace {
                source: s("/stage"),
                destination: s("/final")
            }
        );
    }

    #[test]
    fn parse_metadata_checkpoint_archive_options_for_serve() {
        let (config, command) = parse(vec![s("serve")]).unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(
            config.metadata_checkpoint_archive_prefix.as_deref(),
            Some(DEFAULT_METADATA_CHECKPOINT_ARCHIVE_PREFIX)
        );
        let (archive, command) = parse(vec![
            s("--metadata-checkpoint-archive-prefix"),
            s("/custom/checkpoints/"),
            s("serve"),
        ])
        .unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(
            archive.metadata_checkpoint_archive_prefix.as_deref(),
            Some("custom/checkpoints")
        );
        let (disabled, command) =
            parse(vec![s("--no-metadata-checkpoint-archive"), s("serve")]).unwrap();
        assert_eq!(command, Command::Serve);
        assert_eq!(disabled.metadata_checkpoint_archive_prefix, None);
    }
    #[test]
    fn parse_mcp_command() {
        let command = parse(vec![s("mcp")]).unwrap().1;
        assert_eq!(command, Command::Mcp(McpCliOptions::default()));
        assert_eq!(
            parse(vec![
                s("mcp"),
                s("--profile"),
                s("workbench"),
                s("--workbench-root"),
                s("/workbenches")
            ])
            .unwrap()
            .1,
            Command::Mcp(McpCliOptions {
                profile: McpProfile::Workbench,
                workbench_root: Some(s("/workbenches")),
                workbench_max_bytes: workbench_mcp::DEFAULT_WORKBENCH_MAX_BYTES,
            })
        );
        assert_eq!(
            parse(vec![
                s("mcp"),
                s("--profile"),
                s("workbench"),
                s("--workbench-root"),
                s("/workbenches/"),
                s("--workbench-max-bytes"),
                s("1024")
            ])
            .unwrap()
            .1,
            Command::Mcp(McpCliOptions {
                profile: McpProfile::Workbench,
                workbench_root: Some(s("/workbenches")),
                workbench_max_bytes: 1024,
            })
        );
        assert!(matches!(
            parse(vec![s("mcp"), s("extra")]),
            Err(CliError::UnknownOption(_))
        ));
        assert!(matches!(
            parse(vec![s("mcp"), s("--profile"), s("workbench")]),
            Err(CliError::MissingArgument("workbench root"))
        ));
        assert!(matches!(
            parse(vec![s("mcp"), s("--workbench-root"), s("/workbenches")]),
            Err(CliError::InvalidOption {
                field: "mcp profile",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("mcp"), s("--profile"), s("unknown")]),
            Err(CliError::InvalidOption {
                field: "profile",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![
                s("mcp"),
                s("--profile"),
                s("workbench"),
                s("--workbench-root"),
                s("/")
            ]),
            Err(CliError::InvalidOption {
                field: "workbench-root",
                ..
            })
        ));
    }

    fn workbench_mcp_options() -> McpCliOptions {
        McpCliOptions {
            profile: McpProfile::Workbench,
            workbench_root: Some(s("/workbenches")),
            workbench_max_bytes: workbench_mcp::DEFAULT_WORKBENCH_MAX_BYTES,
        }
    }

    fn run_workbench_mcp_requests(requests: Vec<serde_json::Value>) -> Vec<serde_json::Value> {
        run_workbench_mcp_requests_with_options(workbench_mcp_options(), requests)
    }

    fn run_workbench_mcp_requests_with_options(
        options: McpCliOptions,
        requests: Vec<serde_json::Value>,
    ) -> Vec<serde_json::Value> {
        let object_dir = tempdir().unwrap();
        let object_root = object_dir.path().join("objects");
        let client_store =
            LocalObjectStore::new(LocalObjectStoreOptions::new(object_root.clone())).unwrap();
        let client = NoKvFsClient::connect(
            spawn_test_server_with_object_config(ObjectStoreConfig::tiered_local(
                object_root,
                fake_s3_options(),
            )),
            client_store,
        );
        let reqs = requests
            .into_iter()
            .map(|request| serde_json::to_string(&request).unwrap())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream_with_options(
            client,
            options,
            DEFAULT_UID,
            DEFAULT_GID,
            reader,
            &mut writer,
        )
        .unwrap();
        String::from_utf8(writer)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn test_mcp_server_stdio() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store.clone());
        let reqs = [
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","clientInfo":{"name":"test-client","version":"1.0"}}}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"ls","arguments":{"path":"/"}}}"#,
        ].join("\n") + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let mut lines = output.lines();
        let resp1: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(resp1["id"], 1);
        assert_eq!(resp1["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(resp1["result"]["serverInfo"]["name"], "nokv-mcp");
        let resp2: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(resp2["id"], 2);
        let tools = resp2["result"]["tools"].as_array().unwrap();
        assert!(tools.iter().any(|t| t["name"] == "ls"));
        let resp3: serde_json::Value = serde_json::from_str(lines.next().unwrap()).unwrap();
        assert_eq!(resp3["id"], 3);
        assert!(resp3["result"]["content"][0]["text"].as_str().is_some());
    }

    #[test]
    fn mcp_tools_list_exposes_exactly_seven_tools() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            vec!["ls", "stat", "catalog", "read", "aggregate", "find", "grep"]
        );
    }

    #[test]
    fn workbench_mcp_tools_use_workbench_prefix_and_are_isolated() {
        let responses = run_workbench_mcp_requests(vec![serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list"
        })]);
        let tools = responses[0]["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            vec![
                "workbench_create",
                "workbench_put_file",
                "workbench_list",
                "workbench_stat",
                "workbench_read",
                "workbench_grep",
                "workbench_find",
                "workbench_commit",
                "workbench_snapshot",
            ]
        );
        assert!(names.iter().all(|name| name.starts_with("workbench_")));
        assert!(names.iter().all(|name| !name.starts_with("nokv_")));
        assert!(!names.contains(&"read"));
        assert!(!names.contains(&"grep"));
    }

    #[test]
    fn workbench_mcp_tool_schemas_are_closed() {
        let responses = run_workbench_mcp_requests(vec![serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list"
        })]);
        let tools = responses[0]["result"]["tools"].as_array().unwrap();
        for tool in tools {
            assert_eq!(tool["inputSchema"]["type"], "object");
            assert_eq!(tool["inputSchema"]["additionalProperties"], false);
            assert!(tool["inputSchema"]["properties"].is_object());
        }
    }

    #[test]
    fn workbench_mcp_create_put_read_list_stat_grep_commit_snapshot_flow() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-001"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "input",
                        "path": "event.json",
                        "text": "{\"event\":\"storm\"}",
                        "content_type": "application/json"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "outputs",
                        "path": "spectrum.csv",
                        "text": "freq,power\n1,2\n",
                        "content_type": "text/csv"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "scripts",
                        "path": "analysis.py",
                        "text": "print('spedas')\n",
                        "content_type": "text/x-python"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "workbench_list",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "outputs"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "workbench_stat",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "outputs",
                        "path": "spectrum.csv"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 7,
                "method": "tools/call",
                "params": {
                    "name": "workbench_read",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "outputs",
                        "path": "spectrum.csv",
                        "format": "structured"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 8,
                "method": "tools/call",
                "params": {
                    "name": "workbench_grep",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "outputs",
                        "pattern": "freq",
                        "recursive": true
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "tools/call",
                "params": {
                    "name": "workbench_commit",
                    "arguments": {
                        "id": "spedas-task-001",
                        "manifest": {
                            "task": "spedas-task-001",
                            "outputs": ["outputs/spectrum.csv"]
                        }
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 10,
                "method": "tools/call",
                "params": {
                    "name": "workbench_read",
                    "arguments": {
                        "id": "spedas-task-001",
                        "section": "metadata",
                        "path": "run_manifest.json",
                        "format": "structured"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 11,
                "method": "tools/call",
                "params": {
                    "name": "workbench_snapshot",
                    "arguments": {"id": "spedas-task-001"}
                }
            }),
        ]);
        for (index, response) in responses.iter().enumerate() {
            assert_ne!(
                response["result"]["isError"], true,
                "response {index}: {response}"
            );
        }
        assert_eq!(
            responses[1]["result"]["structuredContent"]["path"],
            "/workbenches/spedas-task-001/input/event.json"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["entries"][0]["name"],
            "spectrum.csv"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["entries"][0]["relative_path"],
            "spectrum.csv"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["entries"][0]["section"],
            "outputs"
        );
        assert_eq!(
            responses[5]["result"]["structuredContent"]["card"]["path"],
            "/workbenches/spedas-task-001/outputs/spectrum.csv"
        );
        assert_eq!(
            responses[5]["result"]["structuredContent"]["card"]["relative_path"],
            "spectrum.csv"
        );
        assert_eq!(
            responses[5]["result"]["structuredContent"]["card"]["content_type"],
            "text/csv"
        );
        assert!(
            responses[5]["result"]["structuredContent"]["card"]["digest_uri"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
        assert_eq!(
            responses[6]["result"]["structuredContent"]["record_type"], "text_lines",
            "read response: {}",
            responses[6]
        );
        assert_eq!(
            responses[6]["result"]["structuredContent"]["items"][0]["value"]["text"],
            "freq,power"
        );
        assert_eq!(
            responses[7]["result"]["structuredContent"]["matches"][0]["relative_path"],
            "spectrum.csv"
        );
        assert_eq!(
            responses[8]["result"]["structuredContent"]["path"],
            "/workbenches/spedas-task-001/metadata/run_manifest.json"
        );
        let manifest_items = responses[9]["result"]["structuredContent"]["items"]
            .as_array()
            .unwrap();
        assert!(
            manifest_items.iter().any(|item| {
                item["value"]["key"] == "schema"
                    && item["value"]["value"] == "nokv.workbench.run_manifest.v0"
            }),
            "manifest read response: {}",
            responses[9]
        );
        assert!(
            responses[10]["result"]["structuredContent"]["snapshot_id"]
                .as_u64()
                .unwrap()
                > 0
        );
    }

    #[test]
    fn workbench_mcp_find_lists_committed_workbenches_with_manifest_summary() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-010"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-011"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_commit",
                    "arguments": {
                        "id": "spedas-task-010",
                        "manifest": {
                            "task": "spedas-task-010",
                            "dataset": "spedas",
                            "outputs": ["outputs/spectrum.csv"]
                        }
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "workbench_find",
                    "arguments": {
                        "committed": true,
                        "manifest_pattern": "spedas",
                        "limit": 10
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "workbench_find",
                    "arguments": {
                        "committed": false,
                        "limit": 10
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "workbench_find",
                    "arguments": {
                        "committed": true,
                        "include_manifest": true,
                        "limit": 10
                    }
                }
            }),
        ]);
        for (index, response) in responses.iter().enumerate() {
            assert_ne!(
                response["result"]["isError"], true,
                "response {index}: {response}"
            );
        }
        assert_eq!(
            responses[3]["result"]["structuredContent"]["matches"][0]["workbench_id"],
            "spedas-task-010"
        );
        assert_eq!(
            responses[3]["result"]["structuredContent"]["matches"][0]["committed"],
            true
        );
        assert!(responses[3]["result"]["structuredContent"]["matches"][0]["manifest"].is_null());
        assert_eq!(
            responses[3]["result"]["structuredContent"]["matches"][0]["manifest_summary"]
                ["manifest_task"],
            "spedas-task-010"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["matches"][0]["workbench_id"],
            "spedas-task-011"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["matches"][0]["committed"],
            false
        );
        assert_eq!(
            responses[5]["result"]["structuredContent"]["matches"][0]["manifest"]["manifest"]
                ["dataset"],
            "spedas"
        );
    }

    #[test]
    fn workbench_mcp_rejects_path_escape_and_default_overwrite() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-002"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-002",
                        "section": "outputs",
                        "path": "../escape.csv",
                        "text": "bad"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-002",
                        "section": "outputs",
                        "path": "spectrum.csv",
                        "text": "freq,power\n1,2\n"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-002",
                        "section": "outputs",
                        "path": "spectrum.csv",
                        "text": "freq,power\n1,3\n"
                    }
                }
            }),
        ]);
        assert_eq!(responses[1]["result"]["isError"], true);
        assert_eq!(responses[3]["result"]["isError"], true);
    }

    #[test]
    fn workbench_mcp_list_and_grep_tolerate_out_of_band_entries() {
        let object_dir = tempdir().unwrap();
        let object_root = object_dir.path().join("objects");
        let addr = spawn_test_server_with_object_config(ObjectStoreConfig::tiered_local(
            object_root.clone(),
            fake_s3_options(),
        ));
        let connect = || {
            let store =
                LocalObjectStore::new(LocalObjectStoreOptions::new(object_root.clone())).unwrap();
            NoKvFsClient::connect(addr, store)
        };
        let run_requests = |requests: Vec<serde_json::Value>| -> Vec<serde_json::Value> {
            let reqs = requests
                .into_iter()
                .map(|request| serde_json::to_string(&request).unwrap())
                .collect::<Vec<_>>()
                .join("\n")
                + "\n";
            let reader = std::io::Cursor::new(reqs);
            let mut writer = Vec::new();
            run_mcp_stream_with_options(
                connect(),
                workbench_mcp_options(),
                DEFAULT_UID,
                DEFAULT_GID,
                reader,
                &mut writer,
            )
            .unwrap();
            String::from_utf8(writer)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        };

        let setup = run_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-003"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-003",
                        "section": "outputs",
                        "path": "spectrum.csv",
                        "text": "freq,power\n1,2\n",
                        "content_type": "text/csv"
                    }
                }
            }),
        ]);
        for (index, response) in setup.iter().enumerate() {
            assert_ne!(
                response["result"]["isError"], true,
                "setup response {index}: {response}"
            );
        }

        let out_of_band_metadata = |name: &str| ArtifactMetadata {
            producer: "outside-writer".to_owned(),
            digest_uri: "sha256:demo".to_owned(),
            content_type: "text/plain".to_owned(),
            manifest_id: name.to_owned(),
            mode: DEFAULT_MODE_FILE,
            uid: DEFAULT_UID,
            gid: DEFAULT_GID,
        };
        let out_of_band = connect();
        out_of_band
            .put_artifact(
                "/workbenches/spedas-task-003/note.txt",
                b"scratch note".to_vec(),
                out_of_band_metadata("note.txt"),
            )
            .unwrap();
        out_of_band
            .metadata()
            .mkdir(
                "/workbenches/spedas-task-003/junk",
                DEFAULT_MODE_DIR,
                DEFAULT_UID,
                DEFAULT_GID,
            )
            .unwrap();
        out_of_band
            .put_artifact(
                "/workbenches/spedas-task-003/junk/scratch.txt",
                b"freq rogue\n".to_vec(),
                out_of_band_metadata("junk/scratch.txt"),
            )
            .unwrap();

        let responses = run_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_list",
                    "arguments": {"id": "spedas-task-003"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_grep",
                    "arguments": {
                        "id": "spedas-task-003",
                        "pattern": "freq",
                        "recursive": true
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_stat",
                    "arguments": {
                        "id": "spedas-task-003",
                        "path": "junk/scratch.txt"
                    }
                }
            }),
        ]);
        for (index, response) in responses.iter().take(2).enumerate() {
            assert_ne!(
                response["result"]["isError"], true,
                "response {index}: {response}"
            );
        }
        // Out-of-band entries are enumerable but stay unaddressable: request
        // targets keep the strict section validation.
        assert_eq!(
            responses[2]["result"]["isError"], true,
            "stat of out-of-band entry must keep failing: {}",
            responses[2]
        );

        let entries = responses[0]["result"]["structuredContent"]["entries"]
            .as_array()
            .unwrap();
        let entry = |name: &str| {
            entries
                .iter()
                .find(|entry| entry["name"] == name)
                .unwrap_or_else(|| panic!("missing list entry {name}: {entries:?}"))
        };
        for section in workbench_mcp::SECTIONS {
            assert_eq!(entry(section)["section"], *section);
        }
        assert_eq!(entry("note.txt")["section"], serde_json::Value::Null);
        assert_eq!(entry("note.txt")["relative_path"], "note.txt");
        assert_eq!(entry("junk")["section"], serde_json::Value::Null);
        assert_eq!(entry("junk")["relative_path"], "junk");

        let matches = responses[1]["result"]["structuredContent"]["matches"]
            .as_array()
            .unwrap();
        let match_for = |path: &str| {
            matches
                .iter()
                .find(|match_| match_["path"] == path)
                .unwrap_or_else(|| panic!("missing grep match {path}: {matches:?}"))
        };
        let section_match = match_for("/workbenches/spedas-task-003/outputs/spectrum.csv");
        assert_eq!(section_match["section"], "outputs");
        assert_eq!(section_match["relative_path"], "spectrum.csv");
        let alien_match = match_for("/workbenches/spedas-task-003/junk/scratch.txt");
        assert_eq!(alien_match["section"], serde_json::Value::Null);
        assert_eq!(alien_match["relative_path"], "junk/scratch.txt");
    }

    #[test]
    fn workbench_mcp_put_file_ignores_empty_unused_payload_variant() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-012"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-012",
                        "section": "input",
                        "path": "from-text.txt",
                        "text": "hello from text",
                        "base64": "",
                        "content_type": "text/plain; charset=utf-8"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-012",
                        "section": "input",
                        "path": "from-base64.txt",
                        "text": "",
                        "base64": "aGVsbG8gZnJvbSBiYXNlNjQ=",
                        "content_type": "text/plain; charset=utf-8"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "workbench_read",
                    "arguments": {
                        "id": "spedas-task-012",
                        "section": "input",
                        "path": "from-text.txt",
                        "format": "structured"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "workbench_read",
                    "arguments": {
                        "id": "spedas-task-012",
                        "section": "input",
                        "path": "from-base64.txt",
                        "format": "structured"
                    }
                }
            }),
        ]);
        for (index, response) in responses.iter().enumerate() {
            assert_ne!(
                response["result"]["isError"], true,
                "response {index}: {response}"
            );
        }
        assert_eq!(
            responses[3]["result"]["structuredContent"]["items"][0]["value"]["text"],
            "hello from text"
        );
        assert_eq!(
            responses[4]["result"]["structuredContent"]["items"][0]["value"]["text"],
            "hello from base64"
        );
    }

    #[test]
    fn workbench_mcp_rejects_section_prefixed_paths() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-006",
                        "section": "outputs",
                        "path": "outputs/spectrum.csv",
                        "text": "freq,power\n1,2\n"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_read",
                    "arguments": {
                        "id": "spedas-task-006",
                        "section": "outputs",
                        "path": "outputs/spectrum.csv"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_list",
                    "arguments": {
                        "id": "spedas-task-006",
                        "section": "outputs",
                        "path": "outputs"
                    }
                }
            }),
        ]);

        for response in responses {
            assert_eq!(response["result"]["isError"], true);
            assert!(response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("relative to section outputs"));
        }
    }

    #[test]
    fn workbench_mcp_rejects_invalid_ids_sections_payloads_and_manifest() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "_bad"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-004",
                        "section": "tmp",
                        "path": "x.txt",
                        "text": "x"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-004",
                        "section": "outputs",
                        "path": "x.txt",
                        "text": "x",
                        "base64": "eA=="
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-004",
                        "section": "outputs",
                        "path": "x.txt"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "workbench_put_file",
                    "arguments": {
                        "id": "spedas-task-004",
                        "section": "outputs",
                        "path": "x.txt",
                        "base64": "not base64"
                    }
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 6,
                "method": "tools/call",
                "params": {
                    "name": "workbench_commit",
                    "arguments": {
                        "id": "spedas-task-004",
                        "manifest": "done"
                    }
                }
            }),
        ]);
        for response in responses {
            assert_eq!(response["result"]["isError"], true);
        }
    }

    #[test]
    fn workbench_mcp_respects_max_bytes_and_replace_true() {
        let mut options = workbench_mcp_options();
        options.workbench_max_bytes = 4;
        let responses = run_workbench_mcp_requests_with_options(
            options,
            vec![
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "workbench_put_file",
                        "arguments": {
                            "id": "spedas-task-005",
                            "section": "outputs",
                            "path": "small.txt",
                            "text": "12345"
                        }
                    }
                }),
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "workbench_put_file",
                        "arguments": {
                            "id": "spedas-task-005",
                            "section": "outputs",
                            "path": "small.txt",
                            "text": "1234"
                        }
                    }
                }),
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": "workbench_put_file",
                        "arguments": {
                            "id": "spedas-task-005",
                            "section": "outputs",
                            "path": "small.txt",
                            "text": "abcd",
                            "replace": true
                        }
                    }
                }),
            ],
        );
        assert_eq!(responses[0]["result"]["isError"], true);
        assert_ne!(responses[1]["result"]["isError"], true);
        assert_ne!(responses[2]["result"]["isError"], true);
        assert_eq!(responses[2]["result"]["structuredContent"]["replace"], true);
    }

    #[test]
    fn workbench_mcp_snapshot_requires_commit_manifest() {
        let responses = run_workbench_mcp_requests(vec![
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {
                    "name": "workbench_create",
                    "arguments": {"id": "spedas-task-003"}
                }
            }),
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "workbench_snapshot",
                    "arguments": {"id": "spedas-task-003"}
                }
            }),
        ]);
        assert_ne!(responses[0]["result"]["isError"], true);
        assert_eq!(responses[1]["result"]["isError"], true);
        assert!(responses[1]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("not committed"));
    }

    #[test]
    fn mcp_tools_list_passes_input_schema_unchanged() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let ls_tool = tools.iter().find(|t| t["name"] == "ls").unwrap();
        let expected = nokv_agent::agent_tool_definitions()
            .into_iter()
            .find(|t| t.name == "ls")
            .unwrap();
        assert_eq!(ls_tool["inputSchema"], expected.parameters);
    }

    #[test]
    fn mcp_tools_call_returns_structured_content() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"ls","arguments":{"path":"/"}}}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert!(resp["result"]["structuredContent"].is_object());
        assert!(resp["result"]["content"][0]["text"].as_str().is_some());
    }

    #[test]
    fn mcp_tools_call_unknown_tool_returns_error_without_crashing() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"nonexistent","arguments":{}}}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }

    #[test]
    fn mcp_tools_call_invalid_params_returns_jsonrpc_error() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"arguments":{}}}"#
            .to_owned()
            + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32602);
    }

    #[test]
    fn mcp_unknown_method_returns_method_not_found() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"nonexistent/method"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[test]
    fn mcp_rejects_missing_jsonrpc_field() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"id":1,"method":"tools/list"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32600);
    }

    #[test]
    fn mcp_rejects_wrong_jsonrpc_version() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"1.0","id":1,"method":"tools/list"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32600);
    }

    #[test]
    fn mcp_rejects_invalid_json_with_parse_error() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = "not valid json\n".to_owned();
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32700);
    }

    #[test]
    fn mcp_rejects_batch_requests() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"[{"jsonrpc":"2.0","id":1,"method":"tools/list"}]"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["error"]["code"], -32600);
    }

    #[test]
    fn mcp_id_null_gets_a_response_not_treated_as_notification() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":null,"method":"tools/list"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        assert!(!output.is_empty());
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert!(resp["result"].is_object());
    }

    #[test]
    fn mcp_missing_id_is_treated_as_notification_no_response() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","method":"initialized"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn mcp_initialize_negotiates_latest_on_unknown_version() {
        // An unknown/newer protocol version must not fail the handshake: the
        // server negotiates down to its latest supported version and lets the
        // client decide, per the MCP spec.
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"1999-01-01"}}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert!(
            resp.get("error").is_none(),
            "unknown version must not error"
        );
        assert_eq!(resp["result"]["protocolVersion"], "2025-11-25");
    }

    #[test]
    fn mcp_initialize_accepts_current_sdk_protocol_version() {
        // Regression: the mcp SDK (>=1.28) requests "2025-11-25"; the server
        // must accept and echo it so clients like LingTai connect out of the box.
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25"}}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert!(resp.get("error").is_none());
        assert_eq!(resp["result"]["protocolVersion"], "2025-11-25");
    }

    #[test]
    fn mcp_initialize_accepts_second_supported_version() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["result"]["protocolVersion"], "2025-03-26");
    }

    #[test]
    fn mcp_initialize_defaults_when_version_omitted() {
        let store = MemoryObjectStore::new();
        let client = NoKvFsClient::connect(spawn_test_server(), store);
        let reqs = r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#.to_owned() + "\n";
        let reader = std::io::Cursor::new(reqs);
        let mut writer = Vec::new();
        run_mcp_stream(client, reader, &mut writer).unwrap();
        let output = String::from_utf8(writer).unwrap();
        let resp: serde_json::Value = serde_json::from_str(output.lines().next().unwrap()).unwrap();
        assert_eq!(resp["result"]["protocolVersion"], "2025-11-25");
    }
}
