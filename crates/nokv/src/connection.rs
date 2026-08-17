/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Construction of the root-routed workspace SDK used by the CLI.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

#[cfg(feature = "etcd")]
use nokv_client::ControlRouteResolver;
#[cfg(feature = "etcd")]
use nokv_client::EtcdRouteOptions;
use nokv_client::{
    ClientError, ClientOptions, FramedTcpOptions, FramedTcpTransport, RouteResolver,
    StaticRouteResolver, TransportError, WorkspaceClient,
};
use nokv_protocol::{LogicalShardIdentity, ObjectNamespaceIdentity, RootIdentity, RootRoute};
use nokv_types::AgentId;

use super::cli::{ClientConfig, EtcdRoutingConfig, RoutingConfig, StaticRoutingConfig};

const FIXED_ID_BYTES: usize = 16;
const FIXED_ID_HEX_BYTES: usize = FIXED_ID_BYTES * 2;

/// Concrete workspace SDK used by every CLI and MCP command.
pub type CliWorkspaceClient = WorkspaceClient<FramedTcpTransport, Arc<dyn RouteResolver>>;

/// Configuration failure while constructing the root-routed SDK.
#[derive(Debug)]
pub enum ConnectionError {
    MissingOption(&'static str),
    InvalidIdentity {
        option: &'static str,
        value: String,
    },
    #[cfg(not(feature = "etcd"))]
    FeatureDisabled {
        routing: &'static str,
        feature: &'static str,
    },
    Transport(TransportError),
    Client(ClientError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingOption(option) => write!(formatter, "{option} is required"),
            Self::InvalidIdentity { option, value } => write!(
                formatter,
                "{option} must be exactly {FIXED_ID_HEX_BYTES} lowercase hexadecimal characters, got {value:?}"
            ),
            #[cfg(not(feature = "etcd"))]
            Self::FeatureDisabled { routing, feature } => write!(
                formatter,
                "{routing} routing requires the nokv `{feature}` feature; rebuild with --features {feature}"
            ),
            Self::Transport(error) => write!(formatter, "cannot configure RPC transport: {error}"),
            Self::Client(error) => write!(formatter, "cannot configure workspace client: {error}"),
        }
    }
}

impl Error for ConnectionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Client(error) => Some(error),
            Self::MissingOption(_) | Self::InvalidIdentity { .. } => None,
            #[cfg(not(feature = "etcd"))]
            Self::FeatureDisabled { .. } => None,
        }
    }
}

impl From<TransportError> for ConnectionError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

impl From<ClientError> for ConnectionError {
    fn from(error: ClientError) -> Self {
        Self::Client(error)
    }
}

/// Build the real transport and route resolver from the parsed CLI options.
pub fn connect(config: &ClientConfig) -> Result<CliWorkspaceClient, ConnectionError> {
    let root_id = configured_root_id(config)?;
    let routes = route_resolver(root_id, &config.routing)?;
    let transport = FramedTcpTransport::new(FramedTcpOptions::default())?;
    WorkspaceClient::new(
        root_id,
        transport,
        routes,
        ClientOptions {
            max_attempts: config.max_attempts,
        },
    )
    .map_err(ConnectionError::from)
}

/// Decode the one canonical root identity shared by client and owner commands.
pub fn configured_root_id(config: &ClientConfig) -> Result<RootIdentity, ConnectionError> {
    Ok(RootIdentity(required_fixed_identity(
        "--root-id",
        config.root_id.as_deref(),
    )?))
}

/// Decode the explicit logical-shard identity accepted by control-plane
/// provisioning commands.
pub fn parse_logical_shard_id(value: &str) -> Result<LogicalShardIdentity, ConnectionError> {
    Ok(LogicalShardIdentity(decode_fixed_identity(
        "logical shard id",
        value,
    )?))
}

/// Decode the deployment-owned stable Agent identity used for root admission.
///
/// This identity catches misconfigured root routing; it is not an
/// authentication credential.
pub fn parse_agent_id(value: &str) -> Result<AgentId, ConnectionError> {
    Ok(AgentId::from_bytes(decode_fixed_identity(
        "--agent-id",
        value,
    )?))
}

fn route_resolver(
    root_id: RootIdentity,
    routing: &RoutingConfig,
) -> Result<Arc<dyn RouteResolver>, ConnectionError> {
    match routing {
        RoutingConfig::Static(config) => static_route_resolver(root_id, config),
        RoutingConfig::Etcd(config) => etcd_route_resolver(config),
    }
}

fn static_route_resolver(
    root_id: RootIdentity,
    config: &StaticRoutingConfig,
) -> Result<Arc<dyn RouteResolver>, ConnectionError> {
    let logical_shard_id = LogicalShardIdentity(required_fixed_identity(
        "--logical-shard-id",
        config.logical_shard_id.as_deref(),
    )?);
    let object_namespace_id = ObjectNamespaceIdentity(required_fixed_identity(
        "--object-namespace-id",
        config.object_namespace_id.as_deref(),
    )?);
    let placement_generation = config
        .placement_generation
        .ok_or(ConnectionError::MissingOption("--placement-generation"))?;
    let owner_epoch = config
        .owner_epoch
        .ok_or(ConnectionError::MissingOption("--owner-epoch"))?;
    let resolver = StaticRouteResolver::new(
        RootRoute {
            root_id,
            logical_shard_id,
            object_namespace_id,
            placement_generation,
            owner_epoch,
        },
        config.metadata_address,
    )?;
    Ok(Arc::new(resolver))
}

#[cfg(feature = "etcd")]
fn etcd_route_resolver(
    config: &EtcdRoutingConfig,
) -> Result<Arc<dyn RouteResolver>, ConnectionError> {
    let resolver = ControlRouteResolver::connect_etcd(etcd_options(config))?;
    Ok(Arc::new(resolver))
}

#[cfg(not(feature = "etcd"))]
fn etcd_route_resolver(
    _config: &EtcdRoutingConfig,
) -> Result<Arc<dyn RouteResolver>, ConnectionError> {
    Err(ConnectionError::FeatureDisabled {
        routing: "etcd",
        feature: "etcd",
    })
}

#[cfg(feature = "etcd")]
fn etcd_options(config: &EtcdRoutingConfig) -> EtcdRouteOptions {
    EtcdRouteOptions::new(config.endpoints.clone())
        .with_key_prefix(config.key_prefix.clone())
        .with_lease_ttl_seconds(config.lease_ttl_seconds)
}

fn required_fixed_identity(
    option: &'static str,
    value: Option<&str>,
) -> Result<[u8; FIXED_ID_BYTES], ConnectionError> {
    let value = value.ok_or(ConnectionError::MissingOption(option))?;
    decode_fixed_identity(option, value)
}

fn decode_fixed_identity(
    option: &'static str,
    value: &str,
) -> Result<[u8; FIXED_ID_BYTES], ConnectionError> {
    if value.len() != FIXED_ID_HEX_BYTES
        || !value
            .as_bytes()
            .iter()
            .copied()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ConnectionError::InvalidIdentity {
            option,
            value: value.to_owned(),
        });
    }
    let mut decoded = [0_u8; FIXED_ID_BYTES];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        decoded[index] = (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]);
    }
    Ok(decoded)
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => unreachable!("decode_fixed_identity validates lowercase hexadecimal bytes"),
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    fn identity(byte: u8) -> String {
        format!("{byte:02x}").repeat(FIXED_ID_BYTES)
    }

    fn static_config() -> ClientConfig {
        ClientConfig {
            root_id: Some(identity(0x11)),
            routing: RoutingConfig::Static(StaticRoutingConfig {
                metadata_address: "127.0.0.1:17750".parse().expect("test address is valid"),
                logical_shard_id: Some(identity(0x22)),
                object_namespace_id: Some(identity(0x33)),
                placement_generation: Some(7),
                owner_epoch: Some(9),
            }),
            ..ClientConfig::default()
        }
    }

    #[test]
    fn builds_exact_static_root_route_and_workspace_client() {
        let config = static_config();
        let root_id = RootIdentity([0x11; FIXED_ID_BYTES]);
        let resolver = route_resolver(root_id, &config.routing).unwrap();
        let resolved = resolver.resolve(root_id, false).unwrap();
        assert_eq!(resolved.route.root_id, root_id);
        assert_eq!(
            resolved.route.logical_shard_id,
            LogicalShardIdentity([0x22; FIXED_ID_BYTES])
        );
        assert_eq!(
            resolved.route.object_namespace_id,
            ObjectNamespaceIdentity([0x33; FIXED_ID_BYTES])
        );
        assert_eq!(resolved.route.placement_generation, 7);
        assert_eq!(resolved.route.owner_epoch, 9);
        assert_eq!(
            resolved.endpoint,
            "127.0.0.1:17750"
                .parse::<SocketAddr>()
                .expect("test address is valid")
        );

        let client = connect(&config).unwrap();
        assert_eq!(client.root_id(), root_id);
    }

    #[test]
    fn requires_root_and_static_logical_shard_id() {
        let mut config = static_config();
        config.root_id = None;
        assert!(matches!(
            connect(&config),
            Err(ConnectionError::MissingOption("--root-id"))
        ));

        config.root_id = Some(identity(0x11));
        let RoutingConfig::Static(route) = &mut config.routing else {
            panic!("static route expected");
        };
        route.logical_shard_id = None;
        assert!(matches!(
            connect(&config),
            Err(ConnectionError::MissingOption("--logical-shard-id"))
        ));
    }

    #[test]
    fn parsed_static_route_requires_explicit_fences_before_connecting() {
        let root_id = identity(0x11);
        let agent_id = identity(0x44);
        let logical_shard_id = identity(0x22);
        let object_namespace_id = identity(0x33);
        let parse = |extra: &[&str]| {
            crate::cli::parse(
                [
                    "--root-id",
                    root_id.as_str(),
                    // Agent-facing commands parse only with a durable AgentId;
                    // the static-route fence checks below happen at connect().
                    "--agent-id",
                    agent_id.as_str(),
                    "--metadata-address",
                    "127.0.0.1:17750",
                    "--logical-shard-id",
                    logical_shard_id.as_str(),
                    "--object-namespace-id",
                    object_namespace_id.as_str(),
                ]
                .into_iter()
                .chain(extra.iter().copied())
                .chain([
                    "--workbench-root",
                    "/agents/test/wb",
                    "workbench",
                    "workbench_create",
                    r#"{"id":"run-1"}"#,
                ])
                .map(str::to_owned),
            )
            .unwrap()
            .client
        };

        assert!(matches!(
            connect(&parse(&["--owner-epoch", "1"])),
            Err(ConnectionError::MissingOption("--placement-generation"))
        ));
        assert!(matches!(
            connect(&parse(&["--placement-generation", "2"])),
            Err(ConnectionError::MissingOption("--owner-epoch"))
        ));
    }

    #[test]
    fn rejects_invalid_or_noncanonical_fixed_identities() {
        for invalid in [
            "11".repeat(FIXED_ID_BYTES - 1),
            "GG".repeat(FIXED_ID_BYTES),
            "AA".repeat(FIXED_ID_BYTES),
        ] {
            let mut config = static_config();
            config.root_id = Some(invalid);
            assert!(matches!(
                connect(&config),
                Err(ConnectionError::InvalidIdentity {
                    option: "--root-id",
                    ..
                })
            ));
        }

        let mut config = static_config();
        let RoutingConfig::Static(route) = &mut config.routing else {
            panic!("static route expected");
        };
        route.logical_shard_id = Some("AA".repeat(FIXED_ID_BYTES));
        assert!(matches!(
            connect(&config),
            Err(ConnectionError::InvalidIdentity {
                option: "--logical-shard-id",
                ..
            })
        ));
    }

    #[test]
    fn parses_only_canonical_explicit_agent_identities() {
        let parsed = parse_agent_id(&identity(0x44)).unwrap();
        assert_eq!(parsed.as_bytes(), &[0x44; FIXED_ID_BYTES]);

        for invalid in [
            "44".repeat(FIXED_ID_BYTES - 1),
            "GG".repeat(FIXED_ID_BYTES),
            "AA".repeat(FIXED_ID_BYTES),
        ] {
            assert!(matches!(
                parse_agent_id(&invalid),
                Err(ConnectionError::InvalidIdentity {
                    option: "--agent-id",
                    ..
                })
            ));
        }
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn etcd_options_preserve_cli_configuration() {
        let options = etcd_options(&EtcdRoutingConfig {
            endpoints: vec![
                "http://127.0.0.1:2379".to_owned(),
                "http://127.0.0.1:22379".to_owned(),
            ],
            key_prefix: "/nokv/test-control".to_owned(),
            lease_ttl_seconds: 27,
        });
        assert_eq!(
            options.endpoints(),
            ["http://127.0.0.1:2379", "http://127.0.0.1:22379"]
        );
        assert_eq!(options.key_prefix(), "/nokv/test-control");
        assert_eq!(options.lease_ttl_seconds(), 27);
    }

    #[cfg(not(feature = "etcd"))]
    #[test]
    fn etcd_routing_requires_the_compile_time_feature_without_fallback() {
        let mut config = static_config();
        config.routing = RoutingConfig::Etcd(EtcdRoutingConfig {
            endpoints: vec!["http://127.0.0.1:2379".to_owned()],
            key_prefix: "/nokv/control".to_owned(),
            lease_ttl_seconds: 10,
        });
        let error = match connect(&config) {
            Ok(_) => panic!("etcd routing must not fall back without its feature"),
            Err(error) => error,
        };
        assert!(matches!(
            &error,
            ConnectionError::FeatureDisabled {
                routing: "etcd",
                feature: "etcd"
            }
        ));
        assert!(error.to_string().contains("--features etcd"));
    }

    #[cfg(feature = "etcd")]
    #[test]
    fn etcd_feature_validates_options_before_connecting() {
        let mut config = static_config();
        config.routing = RoutingConfig::Etcd(EtcdRoutingConfig {
            endpoints: Vec::new(),
            key_prefix: "/nokv/control".to_owned(),
            lease_ttl_seconds: 10,
        });
        assert!(matches!(connect(&config), Err(ConnectionError::Client(_))));
    }
}
