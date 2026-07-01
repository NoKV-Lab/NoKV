use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AgentId(pub String);

impl AgentId {
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRef {
    pub agent_id: AgentId,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventRecord {
    pub agent_id: AgentId,
    pub id: EventId,
    pub ts_micros: i64,
    pub event_type: String,
    pub source_file: String,
    pub source_offset: u64,
    pub fields: Value,
    pub raw: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventBatch {
    pub source: SourceRef,
    pub file_size: u64,
    pub events: Vec<EventRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCoverage {
    pub source: SourceRef,
    pub file_size: u64,
    pub min_offset: Option<u64>,
    pub max_offset: Option<u64>,
    pub row_count: u64,
}

impl IndexCoverage {
    pub fn lag_bytes(&self) -> u64 {
        match self.max_offset {
            Some(max_offset) => self.file_size.saturating_sub(max_offset),
            None => self.file_size,
        }
    }

    pub fn has_rows(&self) -> bool {
        self.row_count > 0 && self.min_offset.is_some() && self.max_offset.is_some()
    }

    pub fn tail_near_eof(&self, slack_bytes: u64) -> bool {
        self.has_rows() && self.lag_bytes() <= slack_bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestReport {
    pub inserted: usize,
    pub skipped: usize,
    pub coverage: IndexCoverage,
}
