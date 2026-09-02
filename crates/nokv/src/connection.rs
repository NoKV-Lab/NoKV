/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Construction of the seed-discovered workspace SDK used by the CLI.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use nokv_client::{
    ClientError, ClientOptions, FramedTcpOptions, FramedTcpTransport, RouteResolver,
    SeedRouteOptions, SeedRouteResolver, TransportError, WorkspaceClient,
};
use nokv_protocol::RootIdentity;
use nokv_types::AgentId;

use super::cli::ClientConfig;

const FIXED_ID_BYTES: usize = 16;
const FIXED_ID_HEX_BYTES: usize = FIXED_ID_BYTES * 2;

/// Concrete workspace SDK used by every CLI and MCP command.
pub type CliWorkspaceClient = WorkspaceClient<FramedTcpTransport, Arc<dyn RouteResolver>>;

/// Configuration failure while constructing the root-routed SDK.
#[derive(Debug)]
pub enum ConnectionError {
    MissingOption(&'static str),
    InvalidIdentity { option: &'static str, value: String },
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

/// Build the real transport and NoKV seed resolver from parsed CLI options.
pub fn connect(config: &ClientConfig) -> Result<CliWorkspaceClient, ConnectionError> {
    let root_id = configured_root_id(config)?;
    let discovery_transport = FramedTcpTransport::new(FramedTcpOptions::default())?;
    let routes = SeedRouteResolver::new(
        discovery_transport,
        config.seeds.iter().copied(),
        SeedRouteOptions {
            max_attempts: config.max_attempts,
            ..SeedRouteOptions::default()
        },
    )?;
    let routes: Arc<dyn RouteResolver> = Arc::new(routes);
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

/// Decode the one canonical root identity shared by client and provisioning commands.
pub fn configured_root_id(config: &ClientConfig) -> Result<RootIdentity, ConnectionError> {
    Ok(RootIdentity(required_fixed_identity(
        "--root-id",
        config.root_id.as_deref(),
    )?))
}

/// Decode the deployment-owned stable Agent identity used for provisioning.
///
/// This identity catches misconfigured root selection; it is not an
/// authentication credential.
pub fn parse_agent_id(value: &str) -> Result<AgentId, ConnectionError> {
    Ok(AgentId::from_bytes(decode_fixed_identity(
        "--agent-id",
        value,
    )?))
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
    for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
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
    use super::*;

    fn identity(byte: u8) -> String {
        format!("{byte:02x}").repeat(FIXED_ID_BYTES)
    }

    fn seed_config() -> ClientConfig {
        ClientConfig {
            root_id: Some(identity(0x11)),
            seeds: vec![
                "127.0.0.1:17750".parse().unwrap(),
                "127.0.0.1:17751".parse().unwrap(),
            ],
            ..ClientConfig::default()
        }
    }

    #[test]
    fn builds_seed_discovered_workspace_client_without_connecting_eagerly() {
        let client = connect(&seed_config()).unwrap();
        assert_eq!(client.root_id(), RootIdentity([0x11; FIXED_ID_BYTES]));
    }

    #[test]
    fn requires_root_and_at_least_one_seed() {
        let mut config = seed_config();
        config.root_id = None;
        assert!(matches!(
            connect(&config),
            Err(ConnectionError::MissingOption("--root-id"))
        ));

        config.root_id = Some(identity(0x11));
        config.seeds.clear();
        assert!(matches!(connect(&config), Err(ConnectionError::Client(_))));
    }

    #[test]
    fn rejects_invalid_or_noncanonical_root_identities() {
        for invalid in [
            "11".repeat(FIXED_ID_BYTES - 1),
            "GG".repeat(FIXED_ID_BYTES),
            "AA".repeat(FIXED_ID_BYTES),
        ] {
            let mut config = seed_config();
            config.root_id = Some(invalid);
            assert!(matches!(
                connect(&config),
                Err(ConnectionError::InvalidIdentity {
                    option: "--root-id",
                    ..
                })
            ));
        }
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
}
