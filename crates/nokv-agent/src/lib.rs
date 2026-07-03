//! NoKV agent surfaces.
//!
//! The shipped namespace tool surface is transport-free: JSON tool
//! definitions, dispatch, validation, and result shaping over NoKV namespace
//! verbs. LingTai event-index code lives under [`event`] as a separate derived
//! index surface over `logs/events.jsonl`.

pub mod event;
mod namespace;

pub use namespace::{
    agent_tool_definitions, execute_agent_tool, AgentError, AgentNamespace, AgentToolDefinition,
};
