use crate::codec::{decode_json, decode_u64, encode_json, encode_u64};
use crate::key::{
    coverage_key, decode_cursor, encode_cursor, event_key, event_prefix, source_key, type_id_key,
    type_id_prefix, type_ts_key, type_ts_prefix,
};
use crate::{
    AgentBatch, AgentId, AgentIndexError, AgentIndexResult, AgentStore, EventBatch, EventId,
    EventOrder, EventPage, EventQuery, EventRecord, IndexCoverage, IngestReport, ScanDirection,
    SourceRef,
};

#[derive(Clone)]
pub struct AgentEventIndex<S> {
    store: S,
}

impl<S> AgentEventIndex<S>
where
    S: AgentStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn ingest_events(&self, batch: EventBatch) -> AgentIndexResult<IngestReport> {
        validate_batch(&batch)?;
        let mut mutation_batch = AgentBatch::new();
        let mut inserted = 0_usize;
        let mut skipped = 0_usize;

        for event in &batch.events {
            let source_key = source_key(&event.agent_id, &event.source_file, event.source_offset);
            if let Some(existing) = self.store.get(&source_key)? {
                let existing_id = decode_u64(&existing)?;
                if existing_id != event.id.0 {
                    return Err(AgentIndexError::Conflict(format!(
                        "source offset {} already maps to event {}",
                        event.source_offset, existing_id
                    )));
                }
                skipped += 1;
                continue;
            }
            mutation_batch.put(event_key(&event.agent_id, event.id), encode_json(event)?);
            mutation_batch.put(source_key, encode_u64(event.id.0).to_vec());
            mutation_batch.put(
                type_id_key(&event.agent_id, &event.event_type, event.id),
                encode_u64(event.id.0).to_vec(),
            );
            mutation_batch.put(
                type_ts_key(
                    &event.agent_id,
                    &event.event_type,
                    event.ts_micros,
                    event.id,
                ),
                encode_u64(event.id.0).to_vec(),
            );
            inserted += 1;
        }

        let coverage = self.next_coverage(&batch, inserted as u64)?;
        mutation_batch.put(
            coverage_key(&batch.source.agent_id, &batch.source.path),
            encode_json(&coverage)?,
        );
        self.store.apply(mutation_batch)?;

        Ok(IngestReport {
            inserted,
            skipped,
            coverage,
        })
    }

    pub fn query_events(&self, request: EventQuery) -> AgentIndexResult<EventPage> {
        if request.limit == 0 {
            return Err(AgentIndexError::InvalidArgument(
                "event query limit must be greater than zero".to_owned(),
            ));
        }
        if request.event_types.len() > 1 && request.cursor.is_some() {
            return Err(AgentIndexError::InvalidArgument(
                "cursor pagination over multiple event types is not supported yet".to_owned(),
            ));
        }

        let cursor = match request.cursor.as_deref() {
            Some(raw) => Some(decode_cursor(raw).ok_or_else(|| {
                AgentIndexError::InvalidArgument("invalid event cursor".to_owned())
            })?),
            None => None,
        };

        if request.event_types.len() > 1 {
            return self.query_multiple_types(&request);
        }

        if request.event_types.is_empty() {
            return self.query_all_events(&request, cursor.as_deref());
        }

        let (prefix, direction) = query_scan_prefix(&request.agent_id, &request)?;
        let page = self
            .store
            .scan(&prefix, cursor.as_deref(), request.limit, direction)?;
        let mut events = Vec::new();
        for item in &page.items {
            let event_id = decode_u64(&item.value)?;
            let Some(event) = self.read_event(&request.agent_id, EventId(event_id))? else {
                continue;
            };
            if event.agent_id != request.agent_id {
                continue;
            }
            if source_offset_matches(&event, request.source_offset_min) {
                events.push(event);
            }
        }
        Ok(EventPage {
            events,
            next_cursor: page.next_cursor.as_deref().map(encode_cursor),
            truncated: page.truncated,
        })
    }

    pub fn read_event(
        &self,
        agent_id: &AgentId,
        id: EventId,
    ) -> AgentIndexResult<Option<EventRecord>> {
        self.store
            .get(&event_key(agent_id, id))?
            .map(|bytes| decode_json(&bytes))
            .transpose()
    }

    pub fn coverage(&self, source: &SourceRef) -> AgentIndexResult<Option<IndexCoverage>> {
        self.store
            .get(&coverage_key(&source.agent_id, &source.path))?
            .map(|bytes| decode_json(&bytes))
            .transpose()
    }

    fn next_coverage(&self, batch: &EventBatch, inserted: u64) -> AgentIndexResult<IndexCoverage> {
        let current = self.coverage(&batch.source)?;
        let mut coverage = current.unwrap_or_else(|| IndexCoverage {
            source: batch.source.clone(),
            file_size: 0,
            min_offset: None,
            max_offset: None,
            row_count: 0,
        });
        coverage.file_size = batch.file_size;
        for event in &batch.events {
            coverage.min_offset = Some(
                coverage
                    .min_offset
                    .map(|offset| offset.min(event.source_offset))
                    .unwrap_or(event.source_offset),
            );
            coverage.max_offset = Some(
                coverage
                    .max_offset
                    .map(|offset| offset.max(event.source_offset))
                    .unwrap_or(event.source_offset),
            );
        }
        coverage.row_count = coverage.row_count.saturating_add(inserted);
        Ok(coverage)
    }

    fn query_all_events(
        &self,
        request: &EventQuery,
        cursor: Option<&[u8]>,
    ) -> AgentIndexResult<EventPage> {
        let direction = match request.order {
            EventOrder::IdAsc => ScanDirection::Asc,
            EventOrder::IdDesc => ScanDirection::Desc,
            EventOrder::TsDesc => {
                return Err(AgentIndexError::InvalidArgument(
                    "global timestamp-desc event queries require an event type".to_owned(),
                ))
            }
        };
        let page = self.store.scan(
            &event_prefix(&request.agent_id),
            cursor,
            request.limit,
            direction,
        )?;
        let mut events = Vec::new();
        for item in &page.items {
            let event: EventRecord = decode_json(&item.value)?;
            if source_offset_matches(&event, request.source_offset_min) {
                events.push(event);
            }
        }
        Ok(EventPage {
            events,
            next_cursor: page.next_cursor.as_deref().map(encode_cursor),
            truncated: page.truncated,
        })
    }

    fn query_multiple_types(&self, request: &EventQuery) -> AgentIndexResult<EventPage> {
        let mut events = Vec::new();
        for event_type in &request.event_types {
            let subquery = EventQuery {
                agent_id: request.agent_id.clone(),
                event_types: vec![event_type.clone()],
                source_offset_min: request.source_offset_min,
                order: request.order,
                limit: request.limit,
                cursor: None,
            };
            events.extend(self.query_events(subquery)?.events);
        }
        sort_events(&mut events, request.order);
        events.truncate(request.limit);
        Ok(EventPage {
            events,
            next_cursor: None,
            truncated: false,
        })
    }
}

fn validate_batch(batch: &EventBatch) -> AgentIndexResult<()> {
    for event in &batch.events {
        if event.agent_id != batch.source.agent_id {
            return Err(AgentIndexError::InvalidArgument(format!(
                "event {} belongs to agent {}, not {}",
                event.id.0, event.agent_id.0, batch.source.agent_id.0
            )));
        }
        if event.source_file != batch.source.path {
            return Err(AgentIndexError::InvalidArgument(format!(
                "event {} source {} does not match batch source {}",
                event.id.0, event.source_file, batch.source.path
            )));
        }
    }
    Ok(())
}

fn query_scan_prefix(
    agent_id: &AgentId,
    request: &EventQuery,
) -> AgentIndexResult<(Vec<u8>, ScanDirection)> {
    let event_type = request.event_types.first().ok_or_else(|| {
        AgentIndexError::InvalidArgument("event type is required for typed index scan".to_owned())
    })?;
    let scan = match request.order {
        EventOrder::TsDesc => (type_ts_prefix(agent_id, event_type), ScanDirection::Asc),
        EventOrder::IdAsc => (type_id_prefix(agent_id, event_type), ScanDirection::Asc),
        EventOrder::IdDesc => (type_id_prefix(agent_id, event_type), ScanDirection::Desc),
    };
    Ok(scan)
}

#[cfg(test)]
fn query_ids(index: &AgentEventIndex<crate::HoltAgentStore>, query: EventQuery) -> Vec<u64> {
    index
        .query_events(query)
        .unwrap()
        .events
        .iter()
        .map(|event| event.id.0)
        .collect()
}

fn source_offset_matches(event: &EventRecord, source_offset_min: Option<u64>) -> bool {
    source_offset_min.is_none_or(|min_offset| event.source_offset >= min_offset)
}

fn sort_events(events: &mut [EventRecord], order: EventOrder) {
    match order {
        EventOrder::IdAsc => events.sort_by_key(|event| event.id),
        EventOrder::IdDesc => events.sort_by_key(|event| std::cmp::Reverse(event.id)),
        EventOrder::TsDesc => events.sort_by_key(|event| {
            (
                std::cmp::Reverse(event.ts_micros),
                std::cmp::Reverse(event.id),
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::HoltAgentStore;

    fn record(id: u64, event_type: &str, ts_micros: i64, source_offset: u64) -> EventRecord {
        EventRecord {
            agent_id: AgentId::new("agent-a"),
            id: EventId(id),
            ts_micros,
            event_type: event_type.to_owned(),
            source_file: "logs/events.jsonl".to_owned(),
            source_offset,
            fields: json!({"type": event_type}),
            raw: json!({"type": event_type}),
        }
    }

    fn batch(events: Vec<EventRecord>) -> EventBatch {
        EventBatch {
            source: SourceRef {
                agent_id: AgentId::new("agent-a"),
                path: "logs/events.jsonl".to_owned(),
            },
            file_size: 4096,
            events,
        }
    }

    fn index() -> AgentEventIndex<HoltAgentStore> {
        AgentEventIndex::new(HoltAgentStore::open_memory().unwrap())
    }

    #[test]
    fn ingest_is_idempotent_by_source_offset() {
        let index = index();
        let first = index
            .ingest_events(batch(vec![record(1, "thinking", 10, 0)]))
            .unwrap();
        let second = index
            .ingest_events(batch(vec![record(1, "thinking", 10, 0)]))
            .unwrap();

        assert_eq!(first.inserted, 1);
        assert_eq!(second.inserted, 0);
        assert_eq!(second.skipped, 1);
        assert_eq!(second.coverage.row_count, 1);
    }

    #[test]
    fn queries_single_type_by_newest_timestamp() {
        let index = index();
        index
            .ingest_events(batch(vec![
                record(1, "thinking", 10, 0),
                record(2, "tool_call", 20, 100),
                record(3, "thinking", 30, 200),
            ]))
            .unwrap();

        let mut query = EventQuery::new(AgentId::new("agent-a"));
        query.event_types = vec!["thinking".to_owned()];
        query.order = EventOrder::TsDesc;
        query.limit = 10;
        assert_eq!(query_ids(&index, query), vec![3, 1]);
    }

    #[test]
    fn queries_all_events_by_event_id() {
        let index = index();
        index
            .ingest_events(batch(vec![
                record(1, "thinking", 10, 0),
                record(2, "tool_call", 20, 100),
                record(3, "thinking", 30, 200),
            ]))
            .unwrap();

        let mut query = EventQuery::new(AgentId::new("agent-a"));
        query.order = EventOrder::IdDesc;
        query.limit = 10;

        assert_eq!(query_ids(&index, query), vec![3, 2, 1]);
    }

    #[test]
    fn coverage_tracks_source_offsets() {
        let index = index();
        let report = index
            .ingest_events(batch(vec![
                record(1, "thinking", 10, 64),
                record(2, "thinking", 20, 128),
            ]))
            .unwrap();

        assert_eq!(report.coverage.min_offset, Some(64));
        assert_eq!(report.coverage.max_offset, Some(128));
        assert_eq!(report.coverage.row_count, 2);
        assert_eq!(report.coverage.lag_bytes(), 3968);
    }
}
