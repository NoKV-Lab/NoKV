use std::fmt;

pub type AgentIndexResult<T> = Result<T, AgentIndexError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentIndexError {
    InvalidArgument(String),
    Store(String),
    Codec(String),
    Conflict(String),
    NotFound(String),
}

impl fmt::Display for AgentIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(msg) => write!(f, "invalid agent index argument: {msg}"),
            Self::Store(msg) => write!(f, "agent store error: {msg}"),
            Self::Codec(msg) => write!(f, "agent index codec error: {msg}"),
            Self::Conflict(msg) => write!(f, "agent index conflict: {msg}"),
            Self::NotFound(msg) => write!(f, "agent index entry not found: {msg}"),
        }
    }
}

impl std::error::Error for AgentIndexError {}
