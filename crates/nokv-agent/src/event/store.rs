use super::types::{
    AgentEventResult, CompletionAfter, CompletionAfterRequest, ErrorEventsRequest, EventRecord,
    EventTime, IndexCoverage, IngestReport, LatestEventsRequest, MoltSessionWindows,
    NewEventRecord, NotificationEventByIdRequest, NotificationEventsRequest,
    NotificationLifecycleRequest, NotificationNeighborRequest, RecentTimesRequest, SessionEventRow,
    SessionEventsRequest, SessionRowsRequest, ToolFacet, ToolFacetRequest, ToolTraceRequest,
    TuiClearCompletion, TuiClearCompletionRequest,
};

pub trait AgentEventStore {
    fn ingest_batch(
        &self,
        records: Vec<NewEventRecord>,
        file_size: u64,
    ) -> AgentEventResult<IngestReport>;

    fn coverage(
        &self,
        agent_id: &str,
        source_file: &str,
    ) -> AgentEventResult<Option<IndexCoverage>>;

    fn latest_events(&self, request: LatestEventsRequest) -> AgentEventResult<Vec<EventRecord>>;

    fn stream_session_events(
        &self,
        request: SessionEventsRequest,
    ) -> AgentEventResult<Vec<EventRecord>>;

    fn stream_session_rows(
        &self,
        request: SessionRowsRequest,
    ) -> AgentEventResult<Vec<SessionEventRow>>;

    fn tool_facets(&self, request: ToolFacetRequest) -> AgentEventResult<Vec<ToolFacet>>;

    fn tool_trace(&self, request: ToolTraceRequest) -> AgentEventResult<Vec<EventRecord>>;

    fn recent_times(&self, request: RecentTimesRequest) -> AgentEventResult<Vec<EventTime>>;

    fn molt_session_windows(&self, agent_id: &str) -> AgentEventResult<MoltSessionWindows>;

    fn error_events(&self, request: ErrorEventsRequest) -> AgentEventResult<Vec<EventRecord>>;

    fn completion_after(
        &self,
        request: CompletionAfterRequest,
    ) -> AgentEventResult<CompletionAfter>;

    fn tui_clear_completion(
        &self,
        request: TuiClearCompletionRequest,
    ) -> AgentEventResult<TuiClearCompletion>;

    fn notification_events(
        &self,
        request: NotificationEventsRequest,
    ) -> AgentEventResult<Vec<EventRecord>>;

    fn notification_event_by_id(
        &self,
        request: NotificationEventByIdRequest,
    ) -> AgentEventResult<Option<EventRecord>>;

    fn notification_neighbor(
        &self,
        request: NotificationNeighborRequest,
    ) -> AgentEventResult<Option<EventRecord>>;

    fn notification_lifecycle(
        &self,
        request: NotificationLifecycleRequest,
    ) -> AgentEventResult<Vec<EventRecord>>;
}
