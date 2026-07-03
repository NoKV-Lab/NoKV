use super::types::{
    AgentEventError, AgentEventResult, EventRecord, IndexCoverage, SessionEventRow, ToolFacet,
};

const EVENT_RECORD_V1: u8 = 1;
const COVERAGE_V1: u8 = 1;
const TOOL_FACET_V1: u8 = 1;
const SESSION_EVENT_ROW_V1: u8 = 1;

pub fn encode_event(record: &EventRecord) -> AgentEventResult<Vec<u8>> {
    encode_versioned(EVENT_RECORD_V1, record)
}

pub fn decode_event(bytes: &[u8]) -> AgentEventResult<EventRecord> {
    decode_versioned(EVENT_RECORD_V1, bytes, "event record")
}

pub fn encode_coverage(coverage: &IndexCoverage) -> AgentEventResult<Vec<u8>> {
    encode_versioned(COVERAGE_V1, coverage)
}

pub fn decode_coverage(bytes: &[u8]) -> AgentEventResult<IndexCoverage> {
    decode_versioned(COVERAGE_V1, bytes, "coverage")
}

pub fn encode_tool_facet(facet: &ToolFacet) -> AgentEventResult<Vec<u8>> {
    encode_versioned(TOOL_FACET_V1, facet)
}

pub fn decode_tool_facet(bytes: &[u8]) -> AgentEventResult<ToolFacet> {
    decode_versioned(TOOL_FACET_V1, bytes, "tool facet")
}

pub fn encode_session_event_row(row: &SessionEventRow) -> AgentEventResult<Vec<u8>> {
    encode_versioned(SESSION_EVENT_ROW_V1, row)
}

pub fn decode_session_event_row(bytes: &[u8]) -> AgentEventResult<SessionEventRow> {
    decode_versioned(SESSION_EVENT_ROW_V1, bytes, "session event row")
}

fn encode_versioned<T: serde::Serialize>(version: u8, value: &T) -> AgentEventResult<Vec<u8>> {
    let mut out = vec![version];
    let mut json = serde_json::to_vec(value)?;
    out.append(&mut json);
    Ok(out)
}

fn decode_versioned<T: serde::de::DeserializeOwned>(
    expected: u8,
    bytes: &[u8],
    label: &str,
) -> AgentEventResult<T> {
    let Some((&version, payload)) = bytes.split_first() else {
        return Err(AgentEventError::Store(format!("{label} value is empty")));
    };
    if version != expected {
        return Err(AgentEventError::Store(format!(
            "{label} value version {version} is not supported"
        )));
    }
    Ok(serde_json::from_slice(payload)?)
}
