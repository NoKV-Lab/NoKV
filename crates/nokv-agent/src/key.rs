use crate::{AgentId, EventId};

pub(crate) fn node_key(agent_id: &AgentId, path: &str) -> Vec<u8> {
    format!("fs/node/{}/{}", segment(&agent_id.0), segment(path)).into_bytes()
}

pub(crate) fn node_prefix(agent_id: &AgentId) -> Vec<u8> {
    format!("fs/node/{}/", segment(&agent_id.0)).into_bytes()
}

pub(crate) fn body_key(agent_id: &AgentId, path: &str) -> Vec<u8> {
    format!("fs/body/{}/{}", segment(&agent_id.0), segment(path)).into_bytes()
}

pub(crate) fn child_key(agent_id: &AgentId, parent: &str, name: &str) -> Vec<u8> {
    format!(
        "fs/child/{}/{}/{}",
        segment(&agent_id.0),
        segment(parent),
        segment(name)
    )
    .into_bytes()
}

pub(crate) fn child_prefix(agent_id: &AgentId, parent: &str) -> Vec<u8> {
    format!("fs/child/{}/{}/", segment(&agent_id.0), segment(parent)).into_bytes()
}

pub(crate) fn catalog_key(agent_id: &AgentId, root: &str) -> Vec<u8> {
    format!("fs/catalog/{}/{}", segment(&agent_id.0), segment(root)).into_bytes()
}

pub(crate) fn row_key(agent_id: &AgentId, root: &str, path: &str) -> Vec<u8> {
    format!(
        "fs/row/{}/{}/{}",
        segment(&agent_id.0),
        segment(root),
        segment(path)
    )
    .into_bytes()
}

pub(crate) fn row_prefix(agent_id: &AgentId, root: &str) -> Vec<u8> {
    format!("fs/row/{}/{}/", segment(&agent_id.0), segment(root)).into_bytes()
}

pub(crate) fn event_key(agent_id: &AgentId, id: EventId) -> Vec<u8> {
    format!("event/{}/{:020}", segment(&agent_id.0), id.0).into_bytes()
}

pub(crate) fn event_prefix(agent_id: &AgentId) -> Vec<u8> {
    format!("event/{}/", segment(&agent_id.0)).into_bytes()
}

pub(crate) fn source_key(agent_id: &AgentId, source_file: &str, source_offset: u64) -> Vec<u8> {
    format!(
        "source/{}/{}/{:020}",
        segment(&agent_id.0),
        segment(source_file),
        source_offset
    )
    .into_bytes()
}

pub(crate) fn coverage_key(agent_id: &AgentId, source_file: &str) -> Vec<u8> {
    format!("coverage/{}/{}", segment(&agent_id.0), segment(source_file)).into_bytes()
}

pub(crate) fn type_id_key(agent_id: &AgentId, event_type: &str, id: EventId) -> Vec<u8> {
    format!(
        "type_id/{}/{}/{:020}",
        segment(&agent_id.0),
        segment(event_type),
        id.0
    )
    .into_bytes()
}

pub(crate) fn type_id_prefix(agent_id: &AgentId, event_type: &str) -> Vec<u8> {
    format!("type_id/{}/{}/", segment(&agent_id.0), segment(event_type)).into_bytes()
}

pub(crate) fn type_ts_key(
    agent_id: &AgentId,
    event_type: &str,
    ts_micros: i64,
    id: EventId,
) -> Vec<u8> {
    let ts_order = (i64::MAX - ts_micros) as u64;
    let id_order = u64::MAX - id.0;
    format!(
        "type_ts/{}/{}/{:020}/{:020}",
        segment(&agent_id.0),
        segment(event_type),
        ts_order,
        id_order
    )
    .into_bytes()
}

pub(crate) fn type_ts_prefix(agent_id: &AgentId, event_type: &str) -> Vec<u8> {
    format!("type_ts/{}/{}/", segment(&agent_id.0), segment(event_type)).into_bytes()
}

pub(crate) fn encode_cursor(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len() * 2);
    for byte in raw {
        out.push(hex_digit(byte >> 4));
        out.push(hex_digit(byte & 0x0f));
    }
    out
}

pub(crate) fn decode_cursor(raw: &str) -> Option<Vec<u8>> {
    if !raw.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(raw.len() / 2);
    for pair in raw.as_bytes().chunks_exact(2) {
        let high = from_hex(pair[0])?;
        let low = from_hex(pair[1])?;
        out.push((high << 4) | low);
    }
    Some(out)
}

pub(crate) fn segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b'-' => out.push(byte as char),
            _ => {
                out.push('%');
                out.push(hex_digit(byte >> 4));
                out.push(hex_digit(byte & 0x0f));
            }
        }
    }
    out
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!("hex digit out of range"),
    }
}

fn from_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(10 + value - b'a'),
        b'A'..=b'F' => Some(10 + value - b'A'),
        _ => None,
    }
}
