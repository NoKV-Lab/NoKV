use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type AgentEventResult<T> = Result<T, AgentEventError>;

pub const LINGTAI_SESSION_EVENT_TYPES: &[&str] = &[
    "thinking",
    "diary",
    "text_input",
    "text_output",
    "tool_call",
    "tool_result",
    "llm_call",
    "llm_response",
    "insight",
    "consultation_fire",
    "notification_pair_injected",
    "apriori_summary_generated",
    "apriori_summary_cap_refused",
    "apriori_summary_failed",
    "apriori_summary_empty",
    "apriori_summary_no_summarizer",
    "aed_attempt",
    "aed_exhausted",
    "aed_timeout",
];

#[derive(Debug)]
pub enum AgentEventError {
    InvalidArgument(String),
    Io(String),
    Json(String),
    Store(String),
}

impl std::fmt::Display for AgentEventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidArgument(msg) => write!(f, "invalid agent event argument: {msg}"),
            Self::Io(msg) => write!(f, "agent event io error: {msg}"),
            Self::Json(msg) => write!(f, "agent event json error: {msg}"),
            Self::Store(msg) => write!(f, "agent event store error: {msg}"),
        }
    }
}

impl std::error::Error for AgentEventError {}

impl From<std::io::Error> for AgentEventError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<serde_json::Error> for AgentEventError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err.to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventProjection {
    pub tool_name: Option<String>,
    pub tool_action: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_trace_id: Option<String>,
    pub api_call_id: Option<String>,
    pub command_head: Option<String>,
    pub file_extension: Option<String>,
    pub notification_channel: Option<String>,
    pub notification_ref_id: Option<String>,
    pub notification_event_id: Option<String>,
    pub notification_call_id: Option<String>,
    pub status: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NewEventRecord {
    pub agent_id: String,
    pub source_file: String,
    pub source_offset: u64,
    pub source_line: u64,
    pub ts: f64,
    pub event_type: String,
    pub fields_json: Value,
    pub projection: EventProjection,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub id: u64,
    pub agent_id: String,
    pub source_file: String,
    pub source_offset: u64,
    pub source_line: u64,
    pub ts: f64,
    pub event_type: String,
    pub fields_json: Value,
    pub projection: EventProjection,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCoverage {
    pub agent_id: String,
    pub source_file: String,
    pub file_size: u64,
    pub min_offset: Option<u64>,
    pub max_offset: Option<u64>,
    pub row_count: u64,
}

impl IndexCoverage {
    pub fn has_rows(&self) -> bool {
        self.row_count > 0 && self.min_offset.is_some() && self.max_offset.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestReport {
    pub accepted: u64,
    pub duplicates: u64,
    pub parse_errors: u64,
    pub partial_lines: u64,
    pub coverage: IndexCoverage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LatestEventsRequest {
    pub agent_id: String,
    pub event_type: String,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEventsRequest {
    pub agent_id: String,
    pub event_types: Vec<String>,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRowsRequest {
    pub agent_id: String,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEventRow {
    pub id: u64,
    pub ts: f64,
    pub event_type: String,
    pub fields_json: Value,
    pub source_file: String,
    pub source_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolFacetRequest {
    pub agent_id: String,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolFacet {
    pub tool_name: String,
    pub action: Option<String>,
    pub count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ToolTraceRequest {
    pub agent_id: String,
    pub tool_call_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecentTimesRequest {
    pub agent_id: String,
    pub event_type: String,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventTime {
    pub id: u64,
    pub ts: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MoltSessionWindows {
    pub ok: bool,
    pub current_since: Option<f64>,
    pub last_since: Option<f64>,
    pub last_before: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorEventsRequest {
    pub agent_id: String,
    pub event_types: Vec<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletionAfterRequest {
    pub agent_id: String,
    pub event_type: String,
    pub source_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompletionAfter {
    pub found: bool,
    pub event: Option<EventRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TuiClearCompletionRequest {
    pub agent_id: String,
    pub source_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TuiClearCompletion {
    pub found: bool,
    pub event: Option<EventRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationEventsRequest {
    pub agent_id: String,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationEventByIdRequest {
    pub agent_id: String,
    pub event_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationNeighborRequest {
    pub agent_id: String,
    pub pivot_event_id: u64,
    pub direction: NotificationNeighborDirection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotificationNeighborDirection {
    Before,
    After,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NotificationLifecycleRequest {
    pub agent_id: String,
    pub ref_id: Option<String>,
    pub event_id: Option<String>,
    pub call_id: Option<String>,
    pub channel: Option<String>,
    pub limit: usize,
}
