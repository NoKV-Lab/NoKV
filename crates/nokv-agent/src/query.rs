use crate::EventRecord;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventOrder {
    IdAsc,
    IdDesc,
    TsDesc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventQuery {
    pub agent_id: crate::AgentId,
    pub event_types: Vec<String>,
    pub source_offset_min: Option<u64>,
    pub order: EventOrder,
    pub limit: usize,
    pub cursor: Option<String>,
}

impl EventQuery {
    pub fn new(agent_id: crate::AgentId) -> Self {
        Self {
            agent_id,
            event_types: Vec::new(),
            source_offset_min: None,
            order: EventOrder::IdAsc,
            limit: 100,
            cursor: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct EventPage {
    pub events: Vec<EventRecord>,
    pub next_cursor: Option<String>,
    pub truncated: bool,
}
