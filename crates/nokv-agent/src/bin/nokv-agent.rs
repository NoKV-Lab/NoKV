use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use nokv_agent::{
    normalize_workbench_root, AgentFs, AgentId, HoltAgentStore, WorkbenchMcpOptions,
    WorkbenchMcpSurface, DEFAULT_WORKBENCH_MAX_BYTES, DEFAULT_WORKBENCH_ROOT,
};

const DEFAULT_STORE: &str = ".nokv/agent";
const DEFAULT_AGENT: &str = "default";

#[derive(Clone, Debug, PartialEq, Eq)]
struct Config {
    store: PathBuf,
    agent: AgentId,
    command: Command,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Command {
    Mcp(McpOptions),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct McpOptions {
    profile: McpProfile,
    workbench_root: String,
    workbench_max_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum McpProfile {
    Agent,
    Workbench,
}

#[derive(Debug, PartialEq, Eq)]
enum CliError {
    Help,
    MissingValue(&'static str),
    MissingCommand,
    UnknownOption(String),
    UnknownCommand(String),
    TooManyCommands,
    InvalidNumber { field: &'static str, value: String },
    InvalidOption { field: &'static str, value: String },
    Io(String),
    Agent(String),
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Help => write!(f, "help requested"),
            Self::MissingValue(option) => write!(f, "missing value for {option}"),
            Self::MissingCommand => write!(f, "missing command"),
            Self::UnknownOption(option) => write!(f, "unknown option {option}"),
            Self::UnknownCommand(command) => write!(f, "unknown command {command}"),
            Self::TooManyCommands => write!(f, "too many commands"),
            Self::InvalidNumber { field, value } => write!(f, "invalid {field}: {value}"),
            Self::InvalidOption { field, value } => write!(f, "invalid {field}: {value}"),
            Self::Io(msg) => write!(f, "io error: {msg}"),
            Self::Agent(msg) => write!(f, "agent error: {msg}"),
        }
    }
}

impl Error for CliError {}

fn main() {
    match run(env::args().skip(1).collect()) {
        Ok(()) | Err(CliError::Help) => {}
        Err(err) => {
            eprintln!("error: {err}");
            eprintln!();
            print_help(&mut io::stderr()).ok();
            std::process::exit(2);
        }
    }
}

fn run(args: Vec<String>) -> Result<(), CliError> {
    let config = parse(args)?;
    match config.command {
        Command::Mcp(ref options) => run_mcp(&config, options),
    }
}

fn parse(args: Vec<String>) -> Result<Config, CliError> {
    let mut store = PathBuf::from(DEFAULT_STORE);
    let mut agent = AgentId::new(DEFAULT_AGENT);
    let mut command = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "-h" | "--help" => {
                print_help(&mut io::stdout()).map_err(from_io)?;
                return Err(CliError::Help);
            }
            "--store" => {
                index += 1;
                store = PathBuf::from(value(&args, index, "--store")?);
                index += 1;
            }
            "--agent" => {
                index += 1;
                agent = AgentId::new(value(&args, index, "--agent")?);
                index += 1;
            }
            "--profile" => {
                index += 1;
                mcp_options_mut(&mut command)?.profile =
                    parse_mcp_profile(value(&args, index, "--profile")?)?;
                index += 1;
            }
            "--workbench-root" => {
                index += 1;
                mcp_options_mut(&mut command)?.workbench_root =
                    normalize_workbench_root(value(&args, index, "--workbench-root")?).map_err(
                        |err| CliError::InvalidOption {
                            field: "workbench-root",
                            value: err,
                        },
                    )?;
                index += 1;
            }
            "--workbench-max-bytes" => {
                index += 1;
                mcp_options_mut(&mut command)?.workbench_max_bytes = parse_usize(
                    value(&args, index, "--workbench-max-bytes")?,
                    "workbench-max-bytes",
                )?;
                index += 1;
            }
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.into()))
            }
            "mcp" => {
                if command
                    .replace(Command::Mcp(McpOptions::default()))
                    .is_some()
                {
                    return Err(CliError::TooManyCommands);
                }
                index += 1;
            }
            other => return Err(CliError::UnknownCommand(other.into())),
        }
    }

    let command = command.ok_or(CliError::MissingCommand)?;
    validate_command(&command)?;
    Ok(Config {
        store,
        agent,
        command,
    })
}

fn mcp_options_mut(command: &mut Option<Command>) -> Result<&mut McpOptions, CliError> {
    match command {
        Some(Command::Mcp(options)) => Ok(options),
        None => Err(CliError::InvalidOption {
            field: "mcp",
            value: "mcp options must follow the mcp command".to_owned(),
        }),
    }
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

fn validate_command(command: &Command) -> Result<(), CliError> {
    match command {
        Command::Mcp(options)
            if options.profile == McpProfile::Agent
                && (options.workbench_root != DEFAULT_WORKBENCH_ROOT
                    || options.workbench_max_bytes != DEFAULT_WORKBENCH_MAX_BYTES) =>
        {
            Err(CliError::InvalidOption {
                field: "profile",
                value: "workbench options require --profile workbench".to_owned(),
            })
        }
        _ => Ok(()),
    }
}

fn value<'a>(args: &'a [String], index: usize, option: &'static str) -> Result<&'a str, CliError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(CliError::MissingValue(option))
}

fn parse_usize(raw: &str, field: &'static str) -> Result<usize, CliError> {
    raw.parse::<usize>().map_err(|_| CliError::InvalidNumber {
        field,
        value: raw.to_owned(),
    })
}

fn run_mcp(config: &Config, options: &McpOptions) -> Result<(), CliError> {
    fs::create_dir_all(&config.store).map_err(from_io)?;
    let store = HoltAgentStore::open(&config.store).map_err(from_agent)?;
    let agent_fs = AgentFs::new(config.agent.clone(), store);
    agent_fs.bootstrap().map_err(from_agent)?;
    match options.profile {
        McpProfile::Agent => nokv_agent::run_mcp(&agent_fs).map_err(from_io),
        McpProfile::Workbench => {
            let surface = WorkbenchMcpSurface::new(
                &agent_fs,
                WorkbenchMcpOptions {
                    root: options.workbench_root.clone(),
                    max_bytes: options.workbench_max_bytes,
                },
            );
            nokv_agent::run_mcp_surface(&surface).map_err(from_io)
        }
    }
}

fn from_io(err: impl Error) -> CliError {
    CliError::Io(err.to_string())
}

fn from_agent(err: impl Error) -> CliError {
    CliError::Agent(err.to_string())
}

fn print_help(out: &mut impl Write) -> io::Result<()> {
    writeln!(
        out,
        "NoKV agent runtime CLI\n\
\n\
Usage:\n\
  nokv-agent [--store PATH] [--agent ID] mcp [--profile agent|workbench] [--workbench-root PATH] [--workbench-max-bytes BYTES]\n\
\n\
Options:\n\
  --store PATH  Holt-backed agent store directory; default .nokv/agent\n\
  --agent ID    Agent identity used for tool state; default default\n\
\n\
Commands:\n\
  mcp           Serve an agent-native MCP tool surface over stdio\n"
    )
}

impl Default for McpOptions {
    fn default() -> Self {
        Self {
            profile: McpProfile::Agent,
            workbench_root: DEFAULT_WORKBENCH_ROOT.to_owned(),
            workbench_max_bytes: DEFAULT_WORKBENCH_MAX_BYTES,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(value: &str) -> String {
        value.to_owned()
    }

    #[test]
    fn parse_mcp_defaults() {
        let parsed = parse(vec![s("mcp")]).unwrap();
        assert_eq!(parsed.command, Command::Mcp(McpOptions::default()));
        assert_eq!(parsed.store, PathBuf::from(DEFAULT_STORE));
        assert_eq!(parsed.agent, AgentId::new(DEFAULT_AGENT));
    }

    #[test]
    fn parse_options_before_or_after_command() {
        let parsed = parse(vec![
            s("--store"),
            s("/tmp/agent"),
            s("mcp"),
            s("--agent"),
            s("lingtai"),
        ])
        .unwrap();
        assert_eq!(parsed.store, PathBuf::from("/tmp/agent"));
        assert_eq!(parsed.agent, AgentId::new("lingtai"));
    }

    #[test]
    fn parse_workbench_profile() {
        let parsed = parse(vec![
            s("mcp"),
            s("--profile"),
            s("workbench"),
            s("--workbench-root"),
            s("/agents/work"),
            s("--workbench-max-bytes"),
            s("1024"),
        ])
        .unwrap();
        assert_eq!(
            parsed.command,
            Command::Mcp(McpOptions {
                profile: McpProfile::Workbench,
                workbench_root: "/agents/work".to_owned(),
                workbench_max_bytes: 1024,
            })
        );
    }

    #[test]
    fn parse_rejects_bad_invocations() {
        assert_eq!(parse(vec![]), Err(CliError::MissingCommand));
        assert_eq!(
            parse(vec![s("--store")]),
            Err(CliError::MissingValue("--store"))
        );
        assert_eq!(
            parse(vec![s("--bad")]),
            Err(CliError::UnknownOption("--bad".to_owned()))
        );
        assert_eq!(
            parse(vec![s("mcp"), s("mcp")]),
            Err(CliError::TooManyCommands)
        );
        assert_eq!(
            parse(vec![s("--profile"), s("workbench"), s("mcp")]),
            Err(CliError::InvalidOption {
                field: "mcp",
                value: "mcp options must follow the mcp command".to_owned(),
            })
        );
        assert!(matches!(
            parse(vec![s("mcp"), s("--profile"), s("unknown")]),
            Err(CliError::InvalidOption {
                field: "profile",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("mcp"), s("--workbench-root"), s("/")]),
            Err(CliError::InvalidOption {
                field: "workbench-root",
                ..
            })
        ));
        assert!(matches!(
            parse(vec![s("mcp"), s("--workbench-root"), s("/agents/work")]),
            Err(CliError::InvalidOption {
                field: "profile",
                ..
            })
        ));
    }
}
