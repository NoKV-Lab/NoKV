/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Argument parsing for the Agent-workspace CLI.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

pub const DEFAULT_SERVER_BIND: &str = "127.0.0.1:7750";
pub const DEFAULT_MAX_ARTIFACT_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_LIFECYCLE_INTERVAL_MILLIS: u64 = 1_000;
pub const DEFAULT_HANDSHAKE_TIMEOUT_MILLIS: u64 = 5_000;
pub const DEFAULT_MAX_INFLIGHT_CONNECTIONS: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    pub root_id: Option<String>,
    pub seeds: Vec<SocketAddr>,
    pub max_attempts: u32,
    pub max_artifact_bytes: usize,
    pub object: ObjectConfig,
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
    pub handshake_timeout_millis: u64,
    pub max_inflight_connections: usize,
    pub advertise_endpoint: Option<String>,
    pub node_id: Option<String>,
    pub metadata_url: Option<String>,
    pub lifecycle_interval_millis: u64,
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
    Mcp {
        profile: McpProfile,
    },
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
        expected_generation: Option<u64>,
        content_type: Option<String>,
    },
    WorkspacePath(WorkspacePathCommand),
    Format,
    Provision,
    Serve,
    Schema,
    Version {
        json: bool,
    },
    Help,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum McpProfile {
    Agent,
    #[default]
    Workbench,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkspacePathCommand {
    Rename {
        workbench: String,
        section: String,
        source: String,
        destination: String,
        expected_generation: u64,
        request_id: [u8; 16],
    },
    Remove {
        workbench: String,
        section: String,
        path: String,
        expected_generation: u64,
        request_id: [u8; 16],
    },
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
    InvalidOption { option: &'static str, value: String },
    InvalidRequestId(String),
    UnpinnedExpectedGeneration,
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
            Self::InvalidOption { option, value } => {
                write!(formatter, "{option} has invalid value {value:?}")
            }
            Self::InvalidRequestId(value) => write!(
                formatter,
                "--request-id must be exactly 32 lowercase hexadecimal characters, got {value:?}"
            ),
            Self::UnpinnedExpectedGeneration => formatter.write_str(
                "--expected-generation pins a replace-only publication and requires --replace",
            ),
        }
    }
}

impl std::error::Error for CliError {}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            root_id: None,
            seeds: Vec::new(),
            max_attempts: 3,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            object: ObjectConfig::default(),
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
            handshake_timeout_millis: DEFAULT_HANDSHAKE_TIMEOUT_MILLIS,
            max_inflight_connections: DEFAULT_MAX_INFLIGHT_CONNECTIONS,
            advertise_endpoint: None,
            node_id: None,
            metadata_url: None,
            lifecycle_interval_millis: DEFAULT_LIFECYCLE_INTERVAL_MILLIS,
        }
    }
}

pub fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Invocation, CliError> {
    let mut arguments = arguments.into_iter();
    let mut client = ClientConfig::default();
    let mut server = ServerConfig::default();
    let mut agent_id = None;
    let mut workbench_root = None;
    let command = loop {
        let Some(argument) = arguments.next() else {
            break Command::Help;
        };
        if !argument.starts_with("--") {
            break parse_command(argument, &mut arguments, &mut server)?;
        }
        match argument.as_str() {
            "--seed" => client.seeds.push(parse_address(
                "--seed",
                next_value(&mut arguments, &argument)?,
            )?),
            "--root-id" => client.root_id = Some(next_value(&mut arguments, &argument)?),
            "--agent-id" => agent_id = Some(next_value(&mut arguments, &argument)?),
            "--workbench-root" => {
                workbench_root = Some(next_value(&mut arguments, &argument)?);
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
            "--handshake-timeout-millis" => {
                server.handshake_timeout_millis = parse_number(
                    "--handshake-timeout-millis",
                    next_value(&mut arguments, &argument)?,
                )?;
            }
            "--max-inflight-connections" => {
                server.max_inflight_connections = parse_number(
                    "--max-inflight-connections",
                    next_value(&mut arguments, &argument)?,
                )?;
            }
            "--advertise-endpoint" => {
                server.advertise_endpoint = Some(next_value(&mut arguments, &argument)?);
            }
            "--node-id" => server.node_id = Some(next_value(&mut arguments, &argument)?),
            "--meta-url" => select_metadata_url(
                &mut server.metadata_url,
                next_value(&mut arguments, &argument)?,
            )?,
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
    if let Some(argument) = arguments.next() {
        return Err(CliError::UnexpectedArgument(argument));
    }
    if matches!(
        &command,
        Command::Workbench { .. } | Command::Mcp { .. } | Command::Collect { .. }
    ) && workbench_root.is_none()
    {
        return Err(CliError::MissingOption("--workbench-root"));
    }
    if matches!(
        &command,
        Command::Workbench { .. }
            | Command::Mcp { .. }
            | Command::Materialize { .. }
            | Command::Collect { .. }
            | Command::WorkspacePath(_)
    ) && client.seeds.is_empty()
    {
        return Err(CliError::MissingOption("--seed"));
    }
    if matches!(&command, Command::Provision) && agent_id.is_none() {
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

fn select_metadata_url(selected: &mut Option<String>, requested: String) -> Result<(), CliError> {
    if selected.is_some() {
        return Err(CliError::UnexpectedArgument("--meta-url".to_owned()));
    }
    *selected = Some(requested);
    Ok(())
}

fn parse_command(
    command: String,
    arguments: &mut impl Iterator<Item = String>,
    server: &mut ServerConfig,
) -> Result<Command, CliError> {
    match command.as_str() {
        "workbench" => Ok(Command::Workbench {
            tool: arguments
                .next()
                .ok_or(CliError::MissingArgument("Workbench tool name"))?,
            arguments: arguments.next().unwrap_or_else(|| "{}".to_owned()),
        }),
        "mcp" => parse_mcp(arguments),
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
        "workspace-path" => parse_workspace_path(arguments),
        "format" => parse_runtime_options(arguments, server).map(|()| Command::Format),
        "provision" => parse_provision(arguments, server),
        "serve" => parse_runtime_options(arguments, server).map(|()| Command::Serve),
        "schema" => Ok(Command::Schema),
        "version" => parse_version(arguments),
        "help" => Ok(Command::Help),
        _ => Err(CliError::UnknownCommand(command)),
    }
}

fn parse_mcp(arguments: &mut impl Iterator<Item = String>) -> Result<Command, CliError> {
    let mut profile = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--profile" if profile.is_none() => {
                let value = next_value(arguments, &argument)?;
                profile = Some(match value.as_str() {
                    "agent" => McpProfile::Agent,
                    "workbench" => McpProfile::Workbench,
                    _ => {
                        return Err(CliError::InvalidOption {
                            option: "--profile",
                            value,
                        });
                    }
                });
            }
            "--profile" => return Err(CliError::UnexpectedArgument(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    Ok(Command::Mcp {
        profile: profile.unwrap_or_default(),
    })
}

fn parse_workspace_path(arguments: &mut impl Iterator<Item = String>) -> Result<Command, CliError> {
    let operation = arguments
        .next()
        .ok_or(CliError::MissingArgument("workspace-path operation"))?;
    let workbench = arguments
        .next()
        .ok_or(CliError::MissingArgument("workbench id"))?;
    let section = arguments
        .next()
        .ok_or(CliError::MissingArgument("section"))?;
    let first_path = arguments
        .next()
        .ok_or(CliError::MissingArgument("workspace path"))?;
    let destination = match operation.as_str() {
        "rename" => Some(
            arguments
                .next()
                .ok_or(CliError::MissingArgument("destination workspace path"))?,
        ),
        "remove" => None,
        _ => {
            return Err(CliError::UnknownCommand(format!(
                "workspace-path {operation}"
            )))
        }
    };

    let mut expected_generation = None;
    let mut request_id = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--expected-generation" if expected_generation.is_none() => {
                expected_generation = Some(parse_number(
                    "--expected-generation",
                    next_value(arguments, &argument)?,
                )?);
            }
            "--request-id" if request_id.is_none() => {
                request_id = Some(parse_request_id(next_value(arguments, &argument)?)?);
            }
            "--expected-generation" | "--request-id" => {
                return Err(CliError::UnexpectedArgument(argument));
            }
            _ if argument.starts_with("--") => return Err(CliError::UnknownOption(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    let expected_generation =
        expected_generation.ok_or(CliError::MissingOption("--expected-generation"))?;
    let request_id = request_id.ok_or(CliError::MissingOption("--request-id"))?;

    Ok(Command::WorkspacePath(match destination {
        Some(destination) => WorkspacePathCommand::Rename {
            workbench,
            section,
            source: first_path,
            destination,
            expected_generation,
            request_id,
        },
        None => WorkspacePathCommand::Remove {
            workbench,
            section,
            path: first_path,
            expected_generation,
            request_id,
        },
    }))
}

fn parse_runtime_options(
    arguments: &mut impl Iterator<Item = String>,
    server: &mut ServerConfig,
) -> Result<(), CliError> {
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--meta-url" => {
                select_metadata_url(&mut server.metadata_url, next_value(arguments, &argument)?)?
            }
            _ if argument.starts_with("--") => return Err(CliError::UnknownOption(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    Ok(())
}

fn parse_provision(
    arguments: &mut impl Iterator<Item = String>,
    server: &mut ServerConfig,
) -> Result<Command, CliError> {
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--meta-url" => {
                select_metadata_url(&mut server.metadata_url, next_value(arguments, &argument)?)?
            }
            _ if argument.starts_with("--") => return Err(CliError::UnknownOption(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    Ok(Command::Provision)
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
    let mut expected_generation = None;
    let mut content_type = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--replace" => replace = true,
            "--expected-generation" => {
                expected_generation = Some(parse_number(
                    "--expected-generation",
                    next_value(arguments, &argument)?,
                )?);
            }
            "--content-type" => {
                content_type = Some(next_value(arguments, &argument)?);
            }
            _ if argument.starts_with("--") => return Err(CliError::UnknownOption(argument)),
            _ => return Err(CliError::UnexpectedArgument(argument)),
        }
    }
    if expected_generation.is_some() && !replace {
        return Err(CliError::UnpinnedExpectedGeneration);
    }
    Ok(Command::Collect {
        workbench,
        section,
        source,
        path,
        replace,
        expected_generation,
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

fn parse_request_id(value: String) -> Result<[u8; 16], CliError> {
    if value.len() != 32
        || !value
            .as_bytes()
            .iter()
            .copied()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CliError::InvalidRequestId(value));
    }
    let mut decoded = [0_u8; 16];
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        decoded[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(decoded)
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("parse_request_id validates lowercase hexadecimal bytes"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn client_commands_accept_repeatable_seeds() {
        let parsed = parse(args(&[
            "--root-id",
            "11111111111111111111111111111111",
            "--seed",
            "127.0.0.1:7750",
            "--seed",
            "127.0.0.1:7751",
            "--workbench-root",
            "/agents/test/wb",
            "workbench",
            "workbench_create",
            r#"{"id":"run-1"}"#,
        ]))
        .unwrap();
        assert_eq!(
            parsed.client.seeds,
            [
                "127.0.0.1:7750".parse::<SocketAddr>().unwrap(),
                "127.0.0.1:7751".parse::<SocketAddr>().unwrap(),
            ]
        );
        assert_eq!(parsed.agent_id, None);
    }

    #[test]
    fn client_commands_require_a_seed() {
        let error = parse(args(&[
            "--workbench-root",
            "/agents/test/wb",
            "workbench",
            "workbench_create",
        ]))
        .unwrap_err();
        assert_eq!(error, CliError::MissingOption("--seed"));
    }

    #[test]
    fn metadata_runtime_commands_accept_one_explicit_url() {
        for (command, expected) in [("format", Command::Format), ("serve", Command::Serve)] {
            let parsed = parse(args(&[
                command,
                "--meta-url",
                "holt:///var/lib/nokv/metadata",
            ]))
            .unwrap();
            assert_eq!(parsed.command, expected);
            assert_eq!(
                parsed.server.metadata_url.as_deref(),
                Some("holt:///var/lib/nokv/metadata")
            );
        }
        assert!(parse(args(&[
            "format",
            "--meta-url",
            "holt:///one",
            "--meta-url",
            "holt:///two",
        ]))
        .is_err());
    }

    #[test]
    fn provision_requires_agent_and_rejects_legacy_arguments() {
        let parsed = parse(args(&[
            "--root-id",
            "11111111111111111111111111111111",
            "--agent-id",
            "44444444444444444444444444444444",
            "provision",
            "--meta-url",
            "holt:///var/lib/nokv/metadata",
        ]))
        .unwrap();
        assert_eq!(parsed.command, Command::Provision);

        assert_eq!(
            parse(args(&[
                "--root-id",
                "11111111111111111111111111111111",
                "provision",
                "--meta-url",
                "holt:///var/lib/nokv/metadata",
            ])),
            Err(CliError::MissingOption("--agent-id"))
        );
        assert!(matches!(
            parse(args(&[
                "--agent-id",
                "44444444444444444444444444444444",
                "provision",
                "22222222222222222222222222222222",
            ])),
            Err(CliError::UnexpectedArgument(_))
        ));
    }

    #[test]
    fn removed_control_and_local_wal_options_are_unknown() {
        for option in [
            "--etcd-endpoint",
            "--metadata-address",
            "--logical-shard-id",
            "--object-namespace-id",
            "--placement-generation",
            "--owner-epoch",
            "--metadata-create",
            "--metadata-reopen",
            "--metadata-recover-log",
            "--recovery-publication",
        ] {
            assert!(matches!(
                parse(args(&[option, "value", "help"])),
                Err(CliError::UnknownOption(actual)) if actual == option
            ));
        }
    }

    #[test]
    fn workspace_path_requires_exact_replay_inputs() {
        let parsed = parse(args(&[
            "--seed",
            "127.0.0.1:7750",
            "workspace-path",
            "remove",
            "run-1",
            "outputs",
            "result.bin",
            "--expected-generation",
            "7",
            "--request-id",
            "0123456789abcdef0123456789abcdef",
        ]))
        .unwrap();
        assert!(matches!(
            parsed.command,
            Command::WorkspacePath(WorkspacePathCommand::Remove {
                expected_generation: 7,
                ..
            })
        ));
    }

    #[test]
    fn collect_expected_generation_requires_replace() {
        let base = [
            "--seed",
            "127.0.0.1:7750",
            "--workbench-root",
            "/agents/test/wb",
            "collect",
            "run-1",
            "outputs",
            "/tmp/result.bin",
            "result.bin",
            "--expected-generation",
            "7",
        ];
        assert_eq!(
            parse(args(&base)),
            Err(CliError::UnpinnedExpectedGeneration)
        );
    }

    #[test]
    fn absent_command_is_help() {
        assert_eq!(parse(Vec::new()).unwrap().command, Command::Help);
    }
}
