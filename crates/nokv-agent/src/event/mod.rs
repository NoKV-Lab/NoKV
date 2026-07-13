//! LingTai event-index surface.
//!
//! This module indexes LingTai `logs/events.jsonl` as a derived view. JSONL is
//! still authoritative; the index is rebuildable and must report coverage
//! clearly enough for LingTai to fall back to JSONL when needed.

pub mod codec;
pub mod holt;
pub mod ingest;
pub mod key;
pub mod notification;
pub mod store;
pub mod types;

pub use holt::HoltAgentEventStore;
pub use ingest::{ingest_jsonl_reader, JsonlIngestOptions};
pub use notification::{
    NotificationBlockMeta, NotificationBlockSnapshot, NotificationSummaryEntry,
};
pub use store::AgentEventStore;
pub use types::{
    AgentEventError, AgentEventResult, CompletionAfter, CompletionAfterRequest, ErrorEventsRequest,
    EventProjection, EventRecord, EventTime, ExternalFieldsJsonRef, IndexCoverage, IngestReport,
    LatestEventsRequest, MoltSessionWindows, NewEventRecord, NotificationEventByIdRequest,
    NotificationEventsRequest, NotificationLifecycleRequest, NotificationNeighborDirection,
    NotificationNeighborRequest, RecentTimesRequest, SessionEventRow, SessionEventsRequest,
    SessionRowsRequest, ToolFacet, ToolFacetRequest, ToolTraceRequest, TuiClearCompletion,
    TuiClearCompletionRequest, LINGTAI_SESSION_EVENT_TYPES,
};
