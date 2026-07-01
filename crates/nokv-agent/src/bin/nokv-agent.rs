use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use nokv_agent::{AgentFs, AgentId, HoltAgentStore};

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
    Mcp,
}

#[derive(Debug, PartialEq, Eq)]
enum CliError {
    Help,
    MissingValue(&'static str),
    MissingCommand,
    UnknownOption(String),
    UnknownCommand(String),
    TooManyCommands,
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
        Command::Mcp => run_mcp(&config),
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
            option if option.starts_with('-') => {
                return Err(CliError::UnknownOption(option.into()))
            }
            "mcp" => {
                if command.replace(Command::Mcp).is_some() {
                    return Err(CliError::TooManyCommands);
                }
                index += 1;
            }
            other => return Err(CliError::UnknownCommand(other.into())),
        }
    }

    let command = command.ok_or(CliError::MissingCommand)?;
    Ok(Config {
        store,
        agent,
        command,
    })
}

fn value<'a>(args: &'a [String], index: usize, option: &'static str) -> Result<&'a str, CliError> {
    args.get(index)
        .map(String::as_str)
        .ok_or(CliError::MissingValue(option))
}

fn run_mcp(config: &Config) -> Result<(), CliError> {
    fs::create_dir_all(&config.store).map_err(from_io)?;
    let store = HoltAgentStore::open(&config.store).map_err(from_agent)?;
    let agent_fs = AgentFs::new(config.agent.clone(), store);
    agent_fs.bootstrap().map_err(from_agent)?;
    nokv_agent::run_mcp(&agent_fs).map_err(from_io)
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
  nokv-agent [--store PATH] [--agent ID] mcp\n\
\n\
Options:\n\
  --store PATH  Holt-backed agent store directory; default .nokv/agent\n\
  --agent ID    Agent identity used for tool state; default default\n\
\n\
Commands:\n\
  mcp           Serve the agent-native tool surface over MCP stdio\n"
    )
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
        assert_eq!(parsed.command, Command::Mcp);
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
    }
}
