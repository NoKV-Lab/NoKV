/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Argument parsing for the Agent-workspace CLI.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

pub const DEFAULT_METADATA_ADDRESS: &str = "127.0.0.1:7750";
pub const DEFAULT_SERVER_BIND: &str = "127.0.0.1:7750";
pub const DEFAULT_MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_ETCD_KEY_PREFIX: &str = "/nokv/control";
pub const DEFAULT_ETCD_LEASE_TTL_SECONDS: i64 = 10;
pub const DEFAULT_LIFECYCLE_INTERVAL_MILLIS: u64 = 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    pub root_id: Option<String>,
    pub routing: RoutingConfig,
    pub max_attempts: u32,
    pub max_artifact_bytes: usize,
    pub object: ObjectConfig,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutingConfig {
    Static(StaticRoutingConfig),
    Etcd(EtcdRoutingConfig),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaticRoutingConfig {
    pub metadata_address: SocketAddr,
    pub logical_shard_id: Option<String>,
    pub object_namespace_id: Option<String>,
    pub placement_generation: u64,
    pub owner_epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EtcdRoutingConfig {
    pub endpoints: Vec<String>,
    pub key_prefix: String,
    pub lease_ttl_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectConfig {
    pub bucket: Option<String>,
    pub root: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    pub virtual_host_style: bool,
    pub skip_signature: bool,
    pub hot_cache_dir: Option<PathBuf>,
    pub hot_cache_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub bind: SocketAddr,
    pub advertise_endpoint: Option<String>,
    pub node_id: Option<String>,
    pub metadata_store: Option<MetadataStoreConfig>,
    pub lifecycle_interval_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataStoreConfig {
    Create(PathBuf),
    Reopen(PathBuf),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Invocation {
    pub client: ClientConfig,
    pub server: ServerConfig,
    pub agent_id: Option<String>,
    pub workbench_root: Option<String>,
    pub command: Command,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Workbench {
        tool: String,
        arguments: String,
    },
    Mcp,
    Materialize {
        workbench: String,
        section: String,
        path: String,
        destination: PathBuf,
    },
    Collect {
        workbench: String,
        section: String,
        source: PathBuf,
        path: String,
        replace: bool,
        content_type: Option<String>,
    },
    Provision {
        logical_shard_id: String,
        adopt_legacy_object_namespace: bool,
        adopt_legacy_agent_binding: bool,
    },
    Serve,
    Schema,
    Version {
        json: bool,
    },
    Help,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CliError {
    MissingValue(String),
    MissingArgument(&'static str),
    MissingOption(&'static str),
    UnknownOption(String),
    UnknownCommand(String),
    UnexpectedArgument(String),
    InvalidNumber { option: &'static str, value: String },
    InvalidAddress { option: &'static str, value: String },
    MixedRoutingOptions,
    MixedMetadataStoreOptions,
}

impl fmt::Display for CliError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingValue(option) => write!(formatter, "missing value for {option}"),
            Self::MissingArgument(argument) => write!(formatter, "missing {argument}"),
            Self::MissingOption(option) => write!(formatter, "missing required option {option}"),
            Self::UnknownOption(option) => write!(formatter, "unknown option {option}"),
            Self::UnknownCommand(command) => write!(formatter, "unknown command {command}"),
            Self::UnexpectedArgument(argument) => {
                write!(formatter, "unexpected argument {argument}")
            }
            Self::InvalidNumber { option, value } => {
                write!(formatter, "{option} has invalid number {value:?}")
            }
            Self::InvalidAddress { option, value } => {
                write!(formatter, "{option} has invalid socket address {value:?}")
            }
            Self::MixedRoutingOptions => formatter.write_str(
                "static metadata routing options and etcd routing options cannot be combined",
            ),
            Self::MixedMetadataStoreOptions => formatter
                .write_str("--metadata-create and --metadata-reopen are mutually exclusive"),
        }
    }
}

impl std::error::Error for CliError {}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            root_id: None,
            routing: RoutingConfig::Static(StaticRoutingConfig::default()),
            max_attempts: 3,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            object: ObjectConfig::default(),
        }
    }
}

impl Default for StaticRoutingConfig {
    fn default() -> Self {
        Self {
            metadata_address: DEFAULT_METADATA_ADDRESS
                .parse()
                .expect("default metadata address is valid"),
            logical_shard_id: None,
            object_namespace_id: None,
            placement_generation: 1,
            owner_epoch: 1,
        }
    }
}

impl Default for EtcdRoutingConfig {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            key_prefix: DEFAULT_ETCD_KEY_PREFIX.to_owned(),
            lease_ttl_seconds: DEFAULT_ETCD_LEASE_TTL_SECONDS,
        }
    }
}

impl Default for ObjectConfig {
    fn default() -> Self {
        Self {
            bucket: None,
            root: "/".to_owned(),
            region: "us-east-1".to_owned(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            virtual_host_style: false,
            skip_signature: false,
            hot_cache_dir: None,
            hot_cache_bytes: 0,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: DEFAULT_SERVER_BIND
                .parse()
                .expect("default server bind is valid"),
            advertise_endpoint: None,
            node_id: None,
            metadata_store: None,
            lifecycle_interval_millis: DEFAULT_LIFECYCLE_INTERVAL_MILLIS,
        }
    }
}

pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Invocation, CliError> {
    let mut arguments = arguments.into_iter();
    let mut client = ClientConfig::default();
    let mut server = ServerConfig::default();
    let mut static_routing = StaticRoutingConfig::default();
    let mut etcd_routing = EtcdRoutingConfig::default();
    let mut routing_kind = None;
    let mut agent_id = None;
    let mut workbench_root = None;
    let command = loop {
        let Some(argument) = arguments.next() else {
            break Command::Help;
        };
        if !argument.starts_with("--") {
            break parse_command(argument, &mut arguments)?;
        }
        match argument.as_str() {
            "--metadata-address" => {
                select_routing(&mut routing_kind, RoutingKind::Static)?;
                static_routing.metadata_address =
                    parse_address("--metadata-address", next_value(&mut arguments, &argument)?)?;
            }
            "--root-id" => client.root_id = Some(next_value(&mut arguments, &argument)?),
            "--agent-id" => agent_id = Some(next_value(&mut arguments, &argument)?),
            "--workbench-root" => {
                workbench_root = Some(next_value(&mut arguments, &argument)?);
            }
            "--logical-shard-id" => {
                select_routing(&mut routing_kind, RoutingKind::Static)?;
                static_routing.logical_shard_id = Some(next_value(&mut arguments, &argument)?);
            }
            "--object-namespace-id" => {
                select_routing(&mut routing_kind, RoutingKind::Static)?;
                static_routing.object_namespace_id = Some(next_value(&mut arguments, &argument)?);
            }
            "--placement-generation" => {
                select_routing(&mut routing_kind, RoutingKind::Static)?;
                static_routing.placement_generation = parse_number(
                    "--placement-generation",
                    next_value(&mut arguments, &argument)?,
                )?;
            }
            "--owner-epoch" => {
                select_routing(&mut routing_kind, RoutingKind::Static)?;
                static_routing.owner_epoch =
                    parse_number("--owner-epoch", next_value(&mut arguments, &argument)?)?;
            }
            "--etcd-endpoint" => {
                select_routing(&mut routing_kind, RoutingKind::Etcd)?;
                etcd_routing
                    .endpoints
                    .push(next_value(&mut arguments, &argument)?);
            }
            "--etcd-key-prefix" => {
                select_routing(&mut routing_kind, RoutingKind::Etcd)?;
                etcd_routing.key_prefix = next_value(&mut arguments, &argument)?;
            }
            "--etcd-lease-ttl-seconds" => {
                select_routing(&mut routing_kind, RoutingKind::Etcd)?;
                etcd_routing.lease_ttl_seconds = parse_number(
                    "--etcd-lease-ttl-seconds",
                    next_value(&mut arguments, &argument)?,
                )?;
            }
            "--max-attempts" => {
                client.max_attempts =
                    parse_number("--max-attempts", next_value(&mut arguments, &argument)?)?;
            }
            "--max-artifact-bytes" => {
                client.max_artifact_bytes = parse_number(
                    "--max-artifact-bytes",
                    next_value(&mut arguments, &argument)?,
                )?;
            }
            "--object-bucket" => {
                client.object.bucket = Some(next_value(&mut arguments, &argument)?);
            }
            "--object-root" => client.object.root = next_value(&mut arguments, &argument)?,
            "--object-region" => client.object.region = next_value(&mut arguments, &argument)?,
            "--object-endpoint" => {
                client.object.endpoint = Some(next_value(&mut arguments, &argument)?);
            }
            "--object-access-key-id" => {
                client.object.access_key_id = Some(next_value(&mut arguments, &argument)?);
            }
            "--object-secret-access-key" => {
                client.object.secret_access_key = Some(next_value(&mut arguments, &argument)?);
            }
            "--object-session-token" => {
                client.object.session_token = Some(next_value(&mut arguments, &argument)?);
            }
            "--object-virtual-host-style" => client.object.virtual_host_style = true,
            "--object-skip-signature" => client.object.skip_signature = true,
            "--hot-cache-dir" => {
                client.object.hot_cache_dir =
                    Some(PathBuf::from(next_value(&mut arguments, &argument)?));
            }
            "--hot-cache-bytes" => {
                client.object.hot_cache_bytes =
                    parse_number("--hot-cache-bytes", next_value(&mut arguments, &argument)?)?;
            }
            "--bind" => {
                server.bind = parse_address("--bind", next_value(&mut arguments, &argument)?)?;
            }
            "--advertise-endpoint" => {
                server.advertise_endpoint = Some(next_value(&mut arguments, &argument)?);
            }
            "--node-id" => server.node_id = Some(next_value(&mut arguments, &argument)?),
            "--metadata-create" => {
                select_metadata_store(
                    &mut server.metadata_store,
                    MetadataStoreConfig::Create(PathBuf::from(next_value(
                        &mut arguments,
                        &argument,
                    )?)),
                )?;
            }
            "--metadata-reopen" => {
                select_metadata_store(
                    &mut server.metadata_store,
                    MetadataStoreConfig::Reopen(PathBuf::from(next_value(
                        &mut arguments,
                        &argument,
                    )?)),
                )?;
            }
            "--lifecycle-interval-millis" => {
                server.lifecycle_interval_millis = parse_number(
                    "--lifecycle-interval-millis",
                    next_value(&mut arguments, &argument)?,
                )?;
            }
            "--help" => break Command::Help,
            "--version" => break Command::Version { json: false },
            _ => return Err(CliError::UnknownOption(argument)),
        }
    };
    client.routing = match routing_kind.unwrap_or(RoutingKind::Static) {
        RoutingKind::Static => RoutingConfig::Static(static_routing),
        RoutingKind::Etcd => RoutingConfig::Etcd(etcd_routing),
    };
    if let Some(argument) = arguments.next() {
        return Err(CliError::UnexpectedArgument(argument));
    }
    if matches!(
        &command,
        Command::Workbench { .. } | Command::Mcp | Command::Collect { .. }
    ) && workbench_root.is_none()
    {
        return Err(CliError::MissingOption("--workbench-root"));
    }
    if matches!(
        &command,
        Command::Workbench { .. }
            | Command::Mcp
            | Command::Materialize { .. }
            | Command::Collect { .. }
            | Command::Provision { .. }
    ) && agent_id.is_none()
    {
        return Err(CliError::MissingOption("--agent-id"));
    }
    Ok(Invocation {
        client,
        server,
        agent_id,
        workbench_root,
        command,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RoutingKind {
    Static,
    Etcd,
}

fn select_routing(
    selected: &mut Option<RoutingKind>,
    requested: RoutingKind,
) -> Result<(), CliError> {
    match *selected {
        Some(current) if current != requested => Err(CliError::MixedRoutingOptions),
        Some(_) => Ok(()),
        None => {
            *selected = Some(requested);
            Ok(())
        }
    }
}

fn select_metadata_store(
    selected: &mut Option<MetadataStoreConfig>,
    requested: MetadataStoreConfig,
) -> Result<(), CliError> {
    if selected.is_some() {
        return Err(CliError::MixedMetadataStoreOptions);
    }
    *selected = Some(requested);
    Ok(())
}

fn parse_command(
    command: String,
    arguments: &mut impl Iterator<Item = String>,
) -> Result<Command, CliError> {
    match command.as_str() {
        "workbench" => Ok(Command::Workbench {
            tool: arguments
                .next()
                .ok_or(CliError::MissingArgument("Workbench tool name"))?,
            arguments: arguments.next().unwrap_or_else(|| "{}".to_owned()),
        }),
        "mcp" => Ok(Command::Mcp),
        "materialize" => Ok(Command::Materialize {
            workbench: arguments
                .next()
                .ok_or(CliError::MissingArgument("workbench id"))?,
            section: arguments
                .next()
                .ok_or(CliError::MissingArgument("section"))?,
            path: arguments
                .next()
                .ok_or(CliError::MissingArgument("workspace path"))?,
            destination: PathBuf::from(
                arguments
                    .next()
                    .ok_or(CliError::MissingArgument("local destination"))?,
            ),
        }),
        "collect" => parse_collect(arguments),
        "provision" => parse_provision(arguments),
        "serve" => Ok(Command::Serve),
        "schema" => Ok(Command::Schema),
        "version" => parse_version(arguments),
        "help" => Ok(Command::Help),
        _ => Err(CliError::UnknownCommand(command)),
    }
}

fn parse_provision(arguments: &mut impl Iterator<Item = String>) -> Result<Command, CliError> {
    let logical_shard_id = arguments
        .next()
        .ok_or(CliError::MissingArgument("logical shard id"))?;
    let mut adopt_legacy_object_namespace = false;
    let mut adopt_legacy_agent_binding = false;
    for argument in arguments.by_ref() {
        match argument.as_str() {
            "--adopt-legacy-object-namespace" => adopt_legacy_object_namespace = true,
            "--adopt-legacy-agent-binding" => adopt_legacy_agent_binding = true,
            _ if argument.starts_with("--") => return Err(CliError::UnknownOption(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    Ok(Command::Provision {
        logical_shard_id,
        adopt_legacy_object_namespace,
        adopt_legacy_agent_binding,
    })
}

fn parse_version(arguments: &mut impl Iterator<Item = String>) -> Result<Command, CliError> {
    match arguments.next() {
        None => Ok(Command::Version { json: false }),
        Some(argument) if argument == "--json" => Ok(Command::Version { json: true }),
        Some(argument) if argument.starts_with("--") => Err(CliError::UnknownOption(argument)),
        Some(argument) => Err(CliError::UnexpectedArgument(argument)),
    }
}

fn parse_collect(arguments: &mut impl Iterator<Item = String>) -> Result<Command, CliError> {
    let workbench = arguments
        .next()
        .ok_or(CliError::MissingArgument("workbench id"))?;
    let section = arguments
        .next()
        .ok_or(CliError::MissingArgument("section"))?;
    let source = PathBuf::from(
        arguments
            .next()
            .ok_or(CliError::MissingArgument("local source"))?,
    );
    let path = arguments
        .next()
        .ok_or(CliError::MissingArgument("workspace path"))?;
    let mut replace = false;
    let mut content_type = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--replace" => replace = true,
            "--content-type" => {
                content_type = Some(next_value(arguments, &argument)?);
            }
            _ if argument.starts_with("--") => return Err(CliError::UnknownOption(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    Ok(Command::Collect {
        workbench,
        section,
        source,
        path,
        replace,
        content_type,
    })
}

fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, CliError> {
    arguments
        .next()
        .ok_or_else(|| CliError::MissingValue(option.to_owned()))
}

fn parse_number<T>(option: &'static str, value: String) -> Result<T, CliError>
where
    T: FromStr,
{
    value
        .parse()
        .map_err(|_| CliError::InvalidNumber { option, value })
}

fn parse_address(option: &'static str, value: String) -> Result<SocketAddr, CliError> {
    value
        .parse()
        .map_err(|_| CliError::InvalidAddress { option, value })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn parses_root_routed_workbench_call() {
        let parsed = parse(args(&[
            "--root-id",
            "01",
            "--agent-id",
            "44444444444444444444444444444444",
            "--workbench-root",
            "/agents/test/wb",
            "--logical-shard-id",
            "02",
            "--placement-generation",
            "7",
            "--owner-epoch",
            "9",
            "--object-bucket",
            "artifacts",
            "workbench",
            "workbench_create",
            r#"{"id":"run-1"}"#,
        ]))
        .unwrap();
        assert_eq!(parsed.client.root_id.as_deref(), Some("01"));
        assert_eq!(
            parsed.agent_id.as_deref(),
            Some("44444444444444444444444444444444")
        );
        assert_eq!(parsed.workbench_root.as_deref(), Some("/agents/test/wb"));
        let RoutingConfig::Static(route) = parsed.client.routing else {
            panic!("static route expected");
        };
        assert_eq!(route.logical_shard_id.as_deref(), Some("02"));
        assert_eq!(route.placement_generation, 7);
        assert_eq!(route.owner_epoch, 9);
        assert_eq!(
            parsed.command,
            Command::Workbench {
                tool: "workbench_create".to_owned(),
                arguments: r#"{"id":"run-1"}"#.to_owned(),
            }
        );
    }

    #[test]
    fn collect_is_explicit_local_transfer() {
        let parsed = parse(args(&[
            "--agent-id",
            "44444444444444444444444444444444",
            "--workbench-root",
            "/agents/test/wb",
            "collect",
            "run-1",
            "outputs",
            "/tmp/result.bin",
            "result.bin",
            "--replace",
            "--content-type",
            "application/octet-stream",
        ]))
        .unwrap();
        assert_eq!(
            parsed.command,
            Command::Collect {
                workbench: "run-1".to_owned(),
                section: "outputs".to_owned(),
                source: PathBuf::from("/tmp/result.bin"),
                path: "result.bin".to_owned(),
                replace: true,
                content_type: Some("application/octet-stream".to_owned()),
            }
        );
    }

    #[test]
    fn absent_command_is_help() {
        assert_eq!(parse(Vec::new()).unwrap().command, Command::Help);
    }

    #[test]
    fn parses_human_and_machine_readable_version_commands() {
        assert_eq!(
            parse(args(&["--version"])).unwrap().command,
            Command::Version { json: false }
        );
        assert_eq!(
            parse(args(&["version"])).unwrap().command,
            Command::Version { json: false }
        );
        assert_eq!(
            parse(args(&["version", "--json"])).unwrap().command,
            Command::Version { json: true }
        );
    }

    #[test]
    fn parses_control_plane_routing_without_static_fences() {
        let parsed = parse(args(&[
            "--root-id",
            "01",
            "--agent-id",
            "44444444444444444444444444444444",
            "--workbench-root",
            "/agents/test/wb",
            "--etcd-endpoint",
            "http://127.0.0.1:2379",
            "--etcd-endpoint",
            "http://127.0.0.1:22379",
            "--etcd-key-prefix",
            "/nokv/production",
            "mcp",
        ]))
        .unwrap();
        let RoutingConfig::Etcd(route) = parsed.client.routing else {
            panic!("etcd route expected");
        };
        assert_eq!(
            route.endpoints,
            ["http://127.0.0.1:2379", "http://127.0.0.1:22379"]
        );
        assert_eq!(route.key_prefix, "/nokv/production");
        assert_eq!(route.lease_ttl_seconds, 10);
    }

    #[test]
    fn parses_explicit_root_provisioning() {
        let parsed = parse(args(&[
            "--root-id",
            "11",
            "--agent-id",
            "44444444444444444444444444444444",
            "--etcd-endpoint",
            "http://127.0.0.1:2379",
            "provision",
            "22222222222222222222222222222222",
        ]))
        .unwrap();
        assert_eq!(
            parsed.command,
            Command::Provision {
                logical_shard_id: "22222222222222222222222222222222".to_owned(),
                adopt_legacy_object_namespace: false,
                adopt_legacy_agent_binding: false,
            }
        );
        assert!(matches!(parsed.client.routing, RoutingConfig::Etcd(_)));
    }

    #[test]
    fn legacy_namespace_adoption_requires_an_explicit_provision_flag() {
        let parsed = parse(args(&[
            "--root-id",
            "11",
            "--agent-id",
            "44444444444444444444444444444444",
            "--etcd-endpoint",
            "http://127.0.0.1:2379",
            "provision",
            "22222222222222222222222222222222",
            "--adopt-legacy-object-namespace",
        ]))
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::Provision {
                adopt_legacy_object_namespace: true,
                ..
            }
        ));
    }

    #[test]
    fn legacy_namespace_adoption_rejects_unparsed_trailing_arguments() {
        let error = parse(args(&[
            "--root-id",
            "11",
            "--agent-id",
            "44444444444444444444444444444444",
            "--etcd-endpoint",
            "http://127.0.0.1:2379",
            "provision",
            "22222222222222222222222222222222",
            "--adopt-legacy-object-namespace",
            "unexpected",
        ]))
        .unwrap_err();

        assert_eq!(error, CliError::UnexpectedArgument("unexpected".to_owned()));
    }

    #[test]
    fn mixed_static_and_control_routing_fails_closed() {
        assert_eq!(
            parse(args(&[
                "--metadata-address",
                "127.0.0.1:7750",
                "--workbench-root",
                "/agents/test/wb",
                "--etcd-endpoint",
                "http://127.0.0.1:2379",
                "mcp",
            ])),
            Err(CliError::MixedRoutingOptions)
        );
    }

    #[test]
    fn server_metadata_open_mode_is_explicit_and_unique() {
        let parsed = parse(args(&[
            "--metadata-reopen",
            "/var/lib/nokv/shard.holt",
            "--node-id",
            "owner-a",
            "--advertise-endpoint",
            "metadata-a.internal:7750",
            "serve",
        ]))
        .unwrap();
        assert_eq!(
            parsed.server.metadata_store,
            Some(MetadataStoreConfig::Reopen(PathBuf::from(
                "/var/lib/nokv/shard.holt"
            )))
        );
        assert_eq!(parsed.server.node_id.as_deref(), Some("owner-a"));
        assert_eq!(
            parsed.server.advertise_endpoint.as_deref(),
            Some("metadata-a.internal:7750")
        );

        assert_eq!(
            parse(args(&[
                "--metadata-create",
                "/tmp/new.holt",
                "--metadata-reopen",
                "/tmp/existing.holt",
                "serve",
            ])),
            Err(CliError::MixedMetadataStoreOptions)
        );
    }

    #[test]
    fn unknown_options_fail_closed() {
        assert!(matches!(
            parse(args(&["--mount", "3", "mcp"])),
            Err(CliError::UnknownOption(option)) if option == "--mount"
        ));
    }

    #[test]
    fn agent_commands_require_an_explicit_presentation_root() {
        assert_eq!(
            parse(args(&[
                "--agent-id",
                "44444444444444444444444444444444",
                "mcp",
            ])),
            Err(CliError::MissingOption("--workbench-root"))
        );
        assert_eq!(
            parse(args(&[
                "--agent-id",
                "44444444444444444444444444444444",
                "workbench",
                "workbench_create",
                r#"{"id":"run-1"}"#,
            ])),
            Err(CliError::MissingOption("--workbench-root"))
        );
    }

    #[test]
    fn agent_commands_and_provision_require_an_explicit_agent_id() {
        for arguments in [
            args(&["--workbench-root", "/agents/test/wb", "mcp"]),
            args(&[
                "--workbench-root",
                "/agents/test/wb",
                "workbench",
                "workbench_create",
                r#"{"id":"run-1"}"#,
            ]),
            args(&[
                "materialize",
                "run-1",
                "outputs",
                "result.bin",
                "/tmp/result.bin",
            ]),
            args(&[
                "--workbench-root",
                "/agents/test/wb",
                "collect",
                "run-1",
                "outputs",
                "/tmp/result.bin",
                "result.bin",
            ]),
            args(&[
                "--root-id",
                "11111111111111111111111111111111",
                "--etcd-endpoint",
                "http://127.0.0.1:2379",
                "provision",
                "22222222222222222222222222222222",
            ]),
        ] {
            assert_eq!(parse(arguments), Err(CliError::MissingOption("--agent-id")));
        }
    }

    #[test]
    fn schema_version_help_and_serve_do_not_require_an_agent_id() {
        assert_eq!(parse(args(&["schema"])).unwrap().command, Command::Schema);
        assert!(matches!(
            parse(args(&["version"])).unwrap().command,
            Command::Version { .. }
        ));
        assert_eq!(parse(args(&["help"])).unwrap().command, Command::Help);
        assert_eq!(parse(args(&["serve"])).unwrap().command, Command::Serve);
    }

    #[test]
    fn legacy_agent_binding_adoption_is_explicit_and_provision_only() {
        let parsed = parse(args(&[
            "--root-id",
            "11111111111111111111111111111111",
            "--agent-id",
            "44444444444444444444444444444444",
            "--etcd-endpoint",
            "http://127.0.0.1:2379",
            "provision",
            "22222222222222222222222222222222",
            "--adopt-legacy-agent-binding",
            "--adopt-legacy-object-namespace",
        ]))
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::Provision {
                adopt_legacy_agent_binding: true,
                adopt_legacy_object_namespace: true,
                ..
            }
        ));

        assert!(matches!(
            parse(args(&[
                "--agent-id",
                "44444444444444444444444444444444",
                "--workbench-root",
                "/agents/test/wb",
                "mcp",
                "--adopt-legacy-agent-binding",
            ])),
            Err(CliError::UnexpectedArgument(argument))
                if argument == "--adopt-legacy-agent-binding"
        ));
    }
}
