use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use holt::{DBAtomicBatch, RangeEntry, Tree, TreeConfig, DB};

use super::codec::{
    decode_coverage, decode_event, decode_session_event_row, decode_tool_facet, encode_coverage,
    encode_event, encode_tool_facet,
};
use super::ingest::empty_coverage;
use super::key::{
    coverage_key, event_key, id_from_index_value, id_value, notification_id_key,
    notification_id_prefix, notification_key, notification_next_key, notification_prefix,
    notification_prev_key, notification_rev_key, notification_rev_prefix, notification_tail_key,
    session_key, session_prefix, source_file_hash, source_key, tool_action_facet_key,
    tool_action_facet_prefix, tool_name_facet_key, trace_key, trace_prefix, tui_clear_rev_key,
    tui_clear_rev_prefix, type_id_key, type_id_prefix, type_ts_key, type_ts_prefix, TREE_COVERAGE,
    TREE_EVENTS, TREE_INDEX,
};
use super::store::AgentEventStore;
use super::types::{
    AgentEventError, AgentEventResult, CompletionAfter, CompletionAfterRequest, ErrorEventsRequest,
    EventRecord, EventTime, IndexCoverage, IngestReport, LatestEventsRequest, MoltSessionWindows,
    NewEventRecord, NotificationEventByIdRequest, NotificationEventsRequest,
    NotificationLifecycleRequest, NotificationNeighborDirection, NotificationNeighborRequest,
    RecentTimesRequest, SessionEventRow, SessionEventsRequest, SessionRowsRequest, ToolFacet,
    ToolFacetRequest, ToolTraceRequest, TuiClearCompletion, TuiClearCompletionRequest,
    LINGTAI_SESSION_EVENT_TYPES,
};

pub struct HoltAgentEventStore {
    db: DB,
    events: Tree,
    index: Tree,
    coverage: Tree,
}

impl HoltAgentEventStore {
    pub fn open_memory() -> AgentEventResult<Self> {
        Self::open(TreeConfig::memory())
    }

    pub fn open_file(path: impl AsRef<Path>) -> AgentEventResult<Self> {
        Self::open(TreeConfig::new(path.as_ref()))
    }

    pub fn open(config: TreeConfig) -> AgentEventResult<Self> {
        let db = DB::open(config).map_err(to_store_error)?;
        let events = db
            .open_or_create_tree(TREE_EVENTS)
            .map_err(to_store_error)?;
        let index = db.open_or_create_tree(TREE_INDEX).map_err(to_store_error)?;
        let coverage = db
            .open_or_create_tree(TREE_COVERAGE)
            .map_err(to_store_error)?;
        Ok(Self {
            db,
            events,
            index,
            coverage,
        })
    }

    fn event_by_id(&self, agent_id: &str, event_id: u64) -> AgentEventResult<Option<EventRecord>> {
        let key = event_key(agent_id, event_id);
        self.events
            .get(&key)
            .map_err(to_store_error)?
            .map(|bytes| decode_event(&bytes))
            .transpose()?
            .map(Some)
            // Large file-backed stores can expose a key through range scan
            // before the point path observes it; keep secondary-index lookups exact.
            .map_or_else(|| self.event_by_id_scan_fallback(&key), Ok)
    }

    fn event_by_id_scan_fallback(&self, key: &[u8]) -> AgentEventResult<Option<EventRecord>> {
        for entry in self.events.scan(key) {
            let entry = entry.map_err(to_store_error)?;
            let RangeEntry::Key {
                key: found, value, ..
            } = entry
            else {
                continue;
            };
            if found == key {
                return decode_event(&value).map(Some);
            }
            break;
        }
        Ok(None)
    }

    fn scan_event_ids(&self, prefix: &[u8], limit: usize) -> AgentEventResult<Vec<u64>> {
        let mut out = Vec::new();
        for entry in self.index.scan(prefix) {
            let entry = entry.map_err(to_store_error)?;
            if let RangeEntry::Key { value, .. } = entry {
                if let Some(id) = id_from_index_value(&value) {
                    out.push(id);
                    if out.len() == limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn events_by_index_prefix(
        &self,
        agent_id: &str,
        prefix: &[u8],
        limit: usize,
    ) -> AgentEventResult<Vec<EventRecord>> {
        self.scan_event_ids(prefix, limit)?
            .into_iter()
            .filter_map(|id| self.event_by_id(agent_id, id).transpose())
            .collect()
    }

    fn first_tui_clear_completion(
        &self,
        agent_id: &str,
        source_offset: u64,
    ) -> AgentEventResult<Option<EventRecord>> {
        let prefix = tui_clear_rev_prefix(agent_id);
        for entry in self.index.scan(&prefix) {
            let entry = entry.map_err(to_store_error)?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let Some(id) = id_from_index_value(&value) else {
                continue;
            };
            let Some(record) = self.event_by_id(agent_id, id)? else {
                continue;
            };
            if record.source_offset >= source_offset {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn first_event_after_index_key(
        &self,
        agent_id: &str,
        prefix: &[u8],
        start_after: &[u8],
    ) -> AgentEventResult<Option<EventRecord>> {
        for entry in self.index.scan(prefix).start_after(start_after) {
            let entry = entry.map_err(to_store_error)?;
            let RangeEntry::Key { value, .. } = entry else {
                continue;
            };
            let Some(id) = id_from_index_value(&value) else {
                continue;
            };
            if let Some(record) = self.event_by_id(agent_id, id)? {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    fn facet_by_key(&self, key: &[u8], fallback: ToolFacet) -> AgentEventResult<ToolFacet> {
        self.index
            .get(key)
            .map_err(to_store_error)?
            .map(|bytes| decode_tool_facet(&bytes))
            .transpose()
            .map(|facet| facet.unwrap_or(fallback))
    }

    fn id_by_index_key(&self, key: &[u8]) -> AgentEventResult<Option<u64>> {
        if let Some(id) = self
            .index
            .get(key)
            .map_err(to_store_error)?
            .and_then(|bytes| id_from_index_value(&bytes))
        {
            return Ok(Some(id));
        }
        for entry in self.index.scan(key) {
            let entry = entry.map_err(to_store_error)?;
            let RangeEntry::Key {
                key: found, value, ..
            } = entry
            else {
                continue;
            };
            if found == key {
                return Ok(id_from_index_value(&value));
            }
            break;
        }
        Ok(None)
    }

    fn scan_tool_facets(&self, prefix: &[u8], limit: usize) -> AgentEventResult<Vec<ToolFacet>> {
        let mut out = Vec::new();
        for entry in self.index.scan(prefix) {
            let entry = entry.map_err(to_store_error)?;
            if let RangeEntry::Key { value, .. } = entry {
                out.push(decode_tool_facet(&value)?);
            }
        }
        sort_tool_facets(&mut out);
        out.truncate(limit);
        Ok(out)
    }

    fn scan_session_rows(
        &self,
        agent_id: &str,
        prefix: &[u8],
        limit: Option<usize>,
    ) -> AgentEventResult<Vec<SessionEventRow>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in self.index.scan(prefix) {
            let entry = entry.map_err(to_store_error)?;
            if let RangeEntry::Key { value, .. } = entry {
                if let Some(id) = id_from_index_value(&value) {
                    if let Some(record) = self.event_by_id(agent_id, id)? {
                        out.push(session_row_from_event(record));
                    }
                } else {
                    out.push(decode_session_event_row(&value)?);
                }
                if limit.is_some_and(|limit| out.len() == limit) {
                    break;
                }
            }
        }
        Ok(out)
    }

    fn materialize_tool_facet_updates(
        &self,
        materialized: &[(Vec<u8>, EventRecord)],
    ) -> AgentEventResult<Vec<(Vec<u8>, ToolFacet)>> {
        let mut increments = BTreeMap::<Vec<u8>, ToolFacet>::new();
        for (_, record) in materialized {
            if record.event_type != "tool_call" {
                continue;
            }
            let Some(tool_name) = record.projection.tool_name.as_deref() else {
                continue;
            };
            increment_facet(
                &mut increments,
                tool_name_facet_key(&record.agent_id, tool_name),
                tool_name,
                None,
            );
            increment_facet(
                &mut increments,
                tool_action_facet_key(
                    &record.agent_id,
                    tool_name,
                    record.projection.tool_action.as_deref(),
                ),
                tool_name,
                record.projection.tool_action.as_deref(),
            );
        }

        let mut out = Vec::with_capacity(increments.len());
        for (key, increment) in increments {
            let mut facet = self.facet_by_key(
                &key,
                ToolFacet {
                    tool_name: increment.tool_name,
                    action: increment.action,
                    count: 0,
                },
            )?;
            facet.count = facet.count.saturating_add(increment.count);
            out.push((key, facet));
        }
        Ok(out)
    }

    fn materialize_notification_neighbor_updates(
        &self,
        materialized: &[(Vec<u8>, EventRecord)],
    ) -> AgentEventResult<NotificationNeighborUpdates> {
        let mut by_agent = BTreeMap::<String, Vec<u64>>::new();
        for (_, record) in materialized {
            if is_notification_event_type(&record.event_type) {
                by_agent
                    .entry(record.agent_id.clone())
                    .or_default()
                    .push(record.id);
            }
        }

        let mut updates = NotificationNeighborUpdates::default();
        for (agent_id, mut ids) in by_agent {
            ids.sort_unstable();
            ids.dedup();
            let mut previous = self.id_by_index_key(&notification_tail_key(&agent_id))?;
            for id in ids {
                if let Some(previous_id) = previous {
                    updates
                        .next
                        .push((notification_next_key(&agent_id, previous_id), id_value(id)));
                    updates
                        .prev
                        .push((notification_prev_key(&agent_id, id), id_value(previous_id)));
                }
                previous = Some(id);
            }
            if let Some(last_id) = previous {
                updates
                    .tail
                    .push((notification_tail_key(&agent_id), id_value(last_id)));
            }
        }
        Ok(updates)
    }
}

#[derive(Default)]
struct NotificationNeighborUpdates {
    prev: Vec<(Vec<u8>, [u8; 8])>,
    next: Vec<(Vec<u8>, [u8; 8])>,
    tail: Vec<(Vec<u8>, [u8; 8])>,
}

impl AgentEventStore for HoltAgentEventStore {
    fn ingest_batch(
        &self,
        records: Vec<NewEventRecord>,
        file_size: u64,
    ) -> AgentEventResult<IngestReport> {
        let mut batch_seen = BTreeSet::new();
        let mut accepted_records = Vec::new();
        let mut duplicates = 0_u64;
        let mut last_source = None;

        for record in records {
            last_source = Some((record.agent_id.clone(), record.source_file.clone()));
            let source = source_key(&record.agent_id, &record.source_file, record.source_offset);
            if !batch_seen.insert(source.clone())
                || self.index.get(&source).map_err(to_store_error)?.is_some()
            {
                duplicates = duplicates.saturating_add(1);
                continue;
            }
            accepted_records.push((source, record));
        }

        let mut coverage_updates = BTreeMap::<(String, String), IndexCoverage>::new();
        let mut materialized = Vec::with_capacity(accepted_records.len());
        for (source_key, new) in accepted_records {
            let id = event_id_for_source(&new.source_file, new.source_offset);
            let record = EventRecord {
                id,
                agent_id: new.agent_id,
                source_file: new.source_file,
                source_offset: new.source_offset,
                source_line: new.source_line,
                ts: new.ts,
                event_type: new.event_type,
                fields_json: new.fields_json,
                projection: new.projection,
            };
            let key = (record.agent_id.clone(), record.source_file.clone());
            let coverage = coverage_updates.entry(key.clone()).or_insert_with(|| {
                self.coverage(&key.0, &key.1)
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| empty_coverage(&key.0, &key.1, file_size))
            });
            coverage.file_size = coverage.file_size.max(file_size);
            coverage.min_offset =
                Some(coverage.min_offset.map_or(record.source_offset, |current| {
                    current.min(record.source_offset)
                }));
            coverage.max_offset =
                Some(coverage.max_offset.map_or(record.source_offset, |current| {
                    current.max(record.source_offset)
                }));
            coverage.row_count = coverage.row_count.saturating_add(1);
            materialized.push((source_key, record));
        }
        let facet_values = self.materialize_tool_facet_updates(&materialized)?;
        let notification_neighbor_updates =
            self.materialize_notification_neighbor_updates(&materialized)?;

        if !materialized.is_empty() {
            self.db
                .atomic(|batch| {
                    for (source, record) in &materialized {
                        write_record_batch(batch, source, record);
                    }
                    for coverage in coverage_updates.values() {
                        batch.put(
                            TREE_COVERAGE,
                            &coverage_key(&coverage.agent_id, &coverage.source_file),
                            &encode_coverage(coverage).expect("coverage encodes"),
                        );
                    }
                    for (key, facet) in &facet_values {
                        batch.put(
                            TREE_INDEX,
                            key,
                            &encode_tool_facet(facet).expect("tool facet encodes"),
                        );
                    }
                    for (key, id) in &notification_neighbor_updates.prev {
                        batch.put(TREE_INDEX, key, id);
                    }
                    for (key, id) in &notification_neighbor_updates.next {
                        batch.put(TREE_INDEX, key, id);
                    }
                    for (key, id) in &notification_neighbor_updates.tail {
                        batch.put(TREE_INDEX, key, id);
                    }
                })
                .map_err(to_store_error)?;
        }

        let coverage = coverage_updates
            .values()
            .next()
            .cloned()
            .or_else(|| {
                last_source.as_ref().and_then(|(agent_id, source_file)| {
                    self.coverage(agent_id, source_file).ok().flatten()
                })
            })
            .unwrap_or_default();
        Ok(IngestReport {
            accepted: materialized.len() as u64,
            duplicates,
            parse_errors: 0,
            partial_lines: 0,
            coverage,
        })
    }

    fn coverage(
        &self,
        agent_id: &str,
        source_file: &str,
    ) -> AgentEventResult<Option<IndexCoverage>> {
        self.coverage
            .get(&coverage_key(agent_id, source_file))
            .map_err(to_store_error)?
            .map(|bytes| decode_coverage(&bytes))
            .transpose()
    }

    fn latest_events(&self, request: LatestEventsRequest) -> AgentEventResult<Vec<EventRecord>> {
        let prefix = type_id_prefix(&request.agent_id, &request.event_type);
        self.events_by_index_prefix(&request.agent_id, &prefix, request.limit)
    }

    fn stream_session_events(
        &self,
        request: SessionEventsRequest,
    ) -> AgentEventResult<Vec<EventRecord>> {
        let mut ids = Vec::new();
        for event_type in request.event_types {
            ids.extend(
                self.scan_event_ids(&type_id_prefix(&request.agent_id, &event_type), usize::MAX)?,
            );
        }
        ids.sort_unstable();
        ids.dedup();
        if let Some(limit) = request.limit {
            ids.truncate(limit);
        }
        ids.into_iter()
            .filter_map(|id| self.event_by_id(&request.agent_id, id).transpose())
            .collect()
    }

    fn stream_session_rows(
        &self,
        request: SessionRowsRequest,
    ) -> AgentEventResult<Vec<SessionEventRow>> {
        let prefix = session_prefix(&request.agent_id);
        self.scan_session_rows(&request.agent_id, &prefix, request.limit)
    }

    fn tool_facets(&self, request: ToolFacetRequest) -> AgentEventResult<Vec<ToolFacet>> {
        self.scan_tool_facets(&tool_action_facet_prefix(&request.agent_id), request.limit)
    }

    fn tool_trace(&self, request: ToolTraceRequest) -> AgentEventResult<Vec<EventRecord>> {
        let prefix = trace_prefix(&request.agent_id, &request.tool_call_id);
        let mut out = self.events_by_index_prefix(&request.agent_id, &prefix, usize::MAX)?;
        out.sort_by_key(|record| record.id);
        Ok(out)
    }

    fn recent_times(&self, request: RecentTimesRequest) -> AgentEventResult<Vec<EventTime>> {
        let prefix = type_ts_prefix(&request.agent_id, &request.event_type);
        let records = self.events_by_index_prefix(&request.agent_id, &prefix, request.limit)?;
        Ok(records
            .into_iter()
            .map(|record| EventTime {
                id: record.id,
                ts: record.ts,
            })
            .collect())
    }

    fn molt_session_windows(&self, agent_id: &str) -> AgentEventResult<MoltSessionWindows> {
        let times = self.recent_times(RecentTimesRequest {
            agent_id: agent_id.to_owned(),
            event_type: "psyche_molt".to_owned(),
            limit: 2,
        })?;
        let current_since = times.first().map(|event| event.ts);
        let last_since = times.get(1).map(|event| event.ts);
        let last_before = last_since.and(current_since);
        Ok(MoltSessionWindows {
            ok: true,
            current_since,
            last_since,
            last_before,
        })
    }

    fn error_events(&self, request: ErrorEventsRequest) -> AgentEventResult<Vec<EventRecord>> {
        if request.event_types.is_empty() || request.limit == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for event_type in request.event_types {
            let prefix = type_id_prefix(&request.agent_id, &event_type);
            out.extend(self.events_by_index_prefix(&request.agent_id, &prefix, request.limit)?);
        }
        out.sort_by_key(|record| Reverse(record.id));
        out.truncate(request.limit);
        Ok(out)
    }

    fn completion_after(
        &self,
        request: CompletionAfterRequest,
    ) -> AgentEventResult<CompletionAfter> {
        let prefix = type_id_prefix(&request.agent_id, &request.event_type);
        let mut matches = self
            .events_by_index_prefix(&request.agent_id, &prefix, usize::MAX)?
            .into_iter()
            .filter(|record| record.source_offset > request.source_offset)
            .collect::<Vec<_>>();
        matches.sort_by_key(|record| (record.source_offset, record.id));
        Ok(CompletionAfter {
            found: !matches.is_empty(),
            event: matches.into_iter().next(),
        })
    }

    fn tui_clear_completion(
        &self,
        request: TuiClearCompletionRequest,
    ) -> AgentEventResult<TuiClearCompletion> {
        if let Some(event) =
            self.first_tui_clear_completion(&request.agent_id, request.source_offset)?
        {
            return Ok(TuiClearCompletion {
                found: true,
                event: Some(event),
            });
        }

        let mut matches = Vec::new();
        for event_type in ["psyche_molt", "clear_received"] {
            let prefix = type_id_prefix(&request.agent_id, event_type);
            matches.extend(
                self.events_by_index_prefix(&request.agent_id, &prefix, usize::MAX)?
                    .into_iter()
                    .filter(|record| {
                        record.source_offset >= request.source_offset
                            && record
                                .fields_json
                                .get("source")
                                .and_then(|value| value.as_str())
                                == Some("tui")
                    }),
            );
        }
        matches.sort_by_key(|record| Reverse(record.id));
        Ok(TuiClearCompletion {
            found: !matches.is_empty(),
            event: matches.into_iter().next(),
        })
    }

    fn notification_events(
        &self,
        request: NotificationEventsRequest,
    ) -> AgentEventResult<Vec<EventRecord>> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }
        let prefix = notification_rev_prefix(&request.agent_id);
        self.events_by_index_prefix(&request.agent_id, &prefix, request.limit)
    }

    fn notification_event_by_id(
        &self,
        request: NotificationEventByIdRequest,
    ) -> AgentEventResult<Option<EventRecord>> {
        Ok(self
            .event_by_id(&request.agent_id, request.event_id)?
            .filter(|record| is_notification_event_type(&record.event_type)))
    }

    fn notification_neighbor(
        &self,
        request: NotificationNeighborRequest,
    ) -> AgentEventResult<Option<EventRecord>> {
        let neighbor_key = match request.direction {
            NotificationNeighborDirection::Before => {
                if request.pivot_event_id == 0 {
                    return Ok(None);
                }
                notification_prev_key(&request.agent_id, request.pivot_event_id)
            }
            NotificationNeighborDirection::After => {
                notification_next_key(&request.agent_id, request.pivot_event_id)
            }
        };
        if let Some(id) = self.id_by_index_key(&neighbor_key)? {
            return self.event_by_id(&request.agent_id, id);
        }

        match request.direction {
            NotificationNeighborDirection::Before => {
                let prefix = notification_rev_prefix(&request.agent_id);
                let start_after = notification_rev_key(&request.agent_id, request.pivot_event_id);
                self.first_event_after_index_key(&request.agent_id, &prefix, &start_after)
            }
            NotificationNeighborDirection::After => {
                let prefix = notification_id_prefix(&request.agent_id);
                let start_after = notification_id_key(&request.agent_id, request.pivot_event_id);
                self.first_event_after_index_key(&request.agent_id, &prefix, &start_after)
            }
        }
    }

    fn notification_lifecycle(
        &self,
        request: NotificationLifecycleRequest,
    ) -> AgentEventResult<Vec<EventRecord>> {
        if request.limit == 0 {
            return Ok(Vec::new());
        }
        let filters = notification_filters(&request);
        let Some((field, value)) = filters.first() else {
            return Err(AgentEventError::InvalidArgument(
                "notification_lifecycle requires ref-id, event-id, call-id, or channel".to_owned(),
            ));
        };
        let prefix = notification_prefix(&request.agent_id, field, value);
        let mut out = self
            .events_by_index_prefix(&request.agent_id, &prefix, usize::MAX)?
            .into_iter()
            .filter(|record| notification_record_matches(record, &request))
            .collect::<Vec<_>>();
        out.sort_by_key(|record| record.id);
        out.truncate(request.limit);
        Ok(out)
    }
}

fn write_record_batch(batch: &mut DBAtomicBatch, source: &[u8], record: &EventRecord) {
    let id = id_value(record.id);
    batch.put_if_absent(TREE_INDEX, source, &id);
    batch.put(
        TREE_EVENTS,
        &event_key(&record.agent_id, record.id),
        &encode_event(record).expect("event encodes"),
    );
    batch.put(
        TREE_INDEX,
        &type_id_key(&record.agent_id, &record.event_type, record.id),
        &id,
    );
    batch.put(
        TREE_INDEX,
        &type_ts_key(&record.agent_id, &record.event_type, record.ts, record.id),
        &id,
    );
    if is_lingtai_session_event_type(&record.event_type) {
        batch.put(TREE_INDEX, &session_key(&record.agent_id, record.id), &id);
    }
    if is_notification_event_type(&record.event_type) {
        batch.put(
            TREE_INDEX,
            &notification_id_key(&record.agent_id, record.id),
            &id,
        );
        batch.put(
            TREE_INDEX,
            &notification_rev_key(&record.agent_id, record.id),
            &id,
        );
    }
    if is_tui_clear_completion_event(record) {
        batch.put(
            TREE_INDEX,
            &tui_clear_rev_key(&record.agent_id, record.id),
            &id,
        );
    }
    if let Some(tool_name) = &record.projection.tool_name {
        batch.put(
            TREE_INDEX,
            &super::key::tool_key(
                &record.agent_id,
                tool_name,
                record.projection.tool_action.as_deref(),
                record.id,
            ),
            &id,
        );
    }
    for (field, value) in [
        ("ref_id", record.projection.notification_ref_id.as_deref()),
        (
            "event_id",
            record.projection.notification_event_id.as_deref(),
        ),
        ("call_id", record.projection.notification_call_id.as_deref()),
        ("channel", record.projection.notification_channel.as_deref()),
    ] {
        if let Some(value) = value {
            batch.put(
                TREE_INDEX,
                &notification_key(&record.agent_id, field, value, record.id),
                &id,
            );
        }
    }
    if let Some(tool_call_id) = &record.projection.tool_call_id {
        batch.put(
            TREE_INDEX,
            &trace_key(&record.agent_id, tool_call_id, record.id),
            &id,
        );
    }
}

fn increment_facet(
    increments: &mut BTreeMap<Vec<u8>, ToolFacet>,
    key: Vec<u8>,
    tool_name: &str,
    action: Option<&str>,
) {
    let entry = increments.entry(key).or_insert_with(|| ToolFacet {
        tool_name: tool_name.to_owned(),
        action: action.map(ToOwned::to_owned),
        count: 0,
    });
    entry.count = entry.count.saturating_add(1);
}

fn sort_tool_facets(facets: &mut [ToolFacet]) {
    facets.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.tool_name.cmp(&right.tool_name))
            .then_with(|| left.action.cmp(&right.action))
    });
}

fn is_lingtai_session_event_type(event_type: &str) -> bool {
    LINGTAI_SESSION_EVENT_TYPES.contains(&event_type)
}

fn session_row_from_event(record: EventRecord) -> SessionEventRow {
    SessionEventRow {
        id: record.id,
        ts: record.ts,
        event_type: record.event_type,
        fields_json: record.fields_json,
        source_file: record.source_file,
        source_offset: record.source_offset,
    }
}

fn is_notification_event_type(event_type: &str) -> bool {
    event_type.contains("notification")
}

fn is_tui_clear_completion_event(record: &EventRecord) -> bool {
    matches!(record.event_type.as_str(), "psyche_molt" | "clear_received")
        && record
            .fields_json
            .get("source")
            .and_then(|value| value.as_str())
            == Some("tui")
}

fn notification_filters(request: &NotificationLifecycleRequest) -> Vec<(&'static str, &str)> {
    let mut filters = Vec::new();
    if let Some(value) = request.ref_id.as_deref() {
        filters.push(("ref_id", value));
    }
    if let Some(value) = request.event_id.as_deref() {
        filters.push(("event_id", value));
    }
    if let Some(value) = request.call_id.as_deref() {
        filters.push(("call_id", value));
    }
    if let Some(value) = request.channel.as_deref() {
        filters.push(("channel", value));
    }
    filters
}

fn notification_record_matches(
    record: &EventRecord,
    request: &NotificationLifecycleRequest,
) -> bool {
    option_matches(
        request.ref_id.as_deref(),
        record.projection.notification_ref_id.as_deref(),
    ) && option_matches(
        request.event_id.as_deref(),
        record.projection.notification_event_id.as_deref(),
    ) && option_matches(
        request.call_id.as_deref(),
        record.projection.notification_call_id.as_deref(),
    ) && option_matches(
        request.channel.as_deref(),
        record.projection.notification_channel.as_deref(),
    )
}

fn option_matches(expected: Option<&str>, actual: Option<&str>) -> bool {
    expected.is_none_or(|expected| actual == Some(expected))
}

fn to_store_error(err: holt::Error) -> AgentEventError {
    AgentEventError::Store(err.to_string())
}

fn event_id_for_source(source_file: &str, source_offset: u64) -> u64 {
    const OFFSET_BITS: u64 = 48;
    const OFFSET_MASK: u64 = (1_u64 << OFFSET_BITS) - 1;
    let hash = source_file_hash(source_file);
    let source_prefix = u64::from_str_radix(&hash[..4], 16).unwrap_or(0);
    (source_prefix << OFFSET_BITS) | (source_offset & OFFSET_MASK)
}
