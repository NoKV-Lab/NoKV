//! Agent runtime and indexing primitives for NoKV.
//!
//! The crate owns the agent-local event model, ingestion path, query surface,
//! and Holt-backed index store. It deliberately does not expose NoKV filesystem
//! namespace tools or depend on NoKV DFS crates.

mod codec;
mod error;
mod event;
mod fs;
mod holt;
mod index;
mod key;
mod mcp;
mod model;
mod query;
mod store;
mod tool;
mod workbench;

pub use error::{AgentIndexError, AgentIndexResult};
pub use event::{
    AgentId, EventBatch, EventId, EventRecord, IndexCoverage, IngestReport, SourceRef,
};
pub use fs::AgentFs;
pub use holt::HoltAgentStore;
pub use index::AgentEventIndex;
pub use mcp::{run_mcp, run_mcp_stream, run_mcp_surface, run_mcp_surface_stream, McpToolSurface};
pub use model::{
    AgentFindField, AgentIndexField, AgentIndexRegistration, AgentIndexRow, AgentIndexValue,
    AgentNode, AgentNodeKind, AgentPredicateOp, AgentPredicateValue,
};
pub use query::{EventOrder, EventPage, EventQuery};
pub use store::{
    AgentBatch, AgentCommit, AgentMutation, AgentScanItem, AgentScanPage, AgentStore, ScanDirection,
};
pub use tool::{agent_tool_definitions, execute_agent_tool, AgentToolDefinition};
pub use workbench::{
    normalize_workbench_root, WorkbenchMcpOptions, WorkbenchMcpSurface,
    DEFAULT_WORKBENCH_MAX_BYTES, DEFAULT_WORKBENCH_ROOT,
};
