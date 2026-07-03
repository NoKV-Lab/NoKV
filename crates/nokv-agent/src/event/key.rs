use sha2::{Digest, Sha256};

pub const TREE_EVENTS: &str = "events";
pub const TREE_INDEX: &str = "index";
pub const TREE_COVERAGE: &str = "coverage";

pub fn source_file_hash(source_file: &str) -> String {
    let digest = Sha256::digest(source_file.as_bytes());
    hex_lower(&digest[..16])
}

pub fn coverage_key(agent_id: &str, source_file: &str) -> Vec<u8> {
    format!(
        "coverage/{}/{}/",
        escape(agent_id),
        source_file_hash(source_file)
    )
    .into_bytes()
}

pub fn source_key(agent_id: &str, source_file: &str, offset: u64) -> Vec<u8> {
    format!(
        "source/{}/{}/{offset:020}",
        escape(agent_id),
        source_file_hash(source_file)
    )
    .into_bytes()
}

pub fn event_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!("event/{}/{event_id:020}", escape(agent_id)).into_bytes()
}

pub fn event_prefix(agent_id: &str) -> Vec<u8> {
    format!("event/{}/", escape(agent_id)).into_bytes()
}

pub fn type_id_key(agent_id: &str, event_type: &str, event_id: u64) -> Vec<u8> {
    format!(
        "type_id/{}/{}/{:020}",
        escape(agent_id),
        escape(event_type),
        u64::MAX - event_id
    )
    .into_bytes()
}

pub fn type_id_prefix(agent_id: &str, event_type: &str) -> Vec<u8> {
    format!("type_id/{}/{}/", escape(agent_id), escape(event_type)).into_bytes()
}

pub fn type_ts_key(agent_id: &str, event_type: &str, ts: f64, event_id: u64) -> Vec<u8> {
    format!(
        "type_ts/{}/{}/{:020}/{event_id:020}",
        escape(agent_id),
        escape(event_type),
        u64::MAX - timestamp_micros(ts)
    )
    .into_bytes()
}

pub fn type_ts_prefix(agent_id: &str, event_type: &str) -> Vec<u8> {
    format!("type_ts/{}/{}/", escape(agent_id), escape(event_type)).into_bytes()
}

pub fn tool_key(agent_id: &str, tool_name: &str, action: Option<&str>, event_id: u64) -> Vec<u8> {
    format!(
        "tool/{}/{}/{}/{:020}",
        escape(agent_id),
        escape(tool_name),
        escape(action.unwrap_or("")),
        u64::MAX - event_id
    )
    .into_bytes()
}

pub fn tool_prefix(agent_id: &str) -> Vec<u8> {
    format!("tool/{}/", escape(agent_id)).into_bytes()
}

pub fn tool_name_facet_key(agent_id: &str, tool_name: &str) -> Vec<u8> {
    format!(
        "facet/{}/tool_name/{}/",
        escape(agent_id),
        escape(tool_name)
    )
    .into_bytes()
}

pub fn tool_name_facet_prefix(agent_id: &str) -> Vec<u8> {
    format!("facet/{}/tool_name/", escape(agent_id)).into_bytes()
}

pub fn tool_action_facet_key(agent_id: &str, tool_name: &str, action: Option<&str>) -> Vec<u8> {
    format!(
        "facet/{}/tool_action/{}/{}/",
        escape(agent_id),
        escape(tool_name),
        escape(action.unwrap_or(""))
    )
    .into_bytes()
}

pub fn tool_action_facet_prefix(agent_id: &str) -> Vec<u8> {
    format!("facet/{}/tool_action/", escape(agent_id)).into_bytes()
}

pub fn session_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!("session/{}/{event_id:020}", escape(agent_id)).into_bytes()
}

pub fn session_prefix(agent_id: &str) -> Vec<u8> {
    format!("session/{}/", escape(agent_id)).into_bytes()
}

pub fn notification_key(agent_id: &str, field: &str, value: &str, event_id: u64) -> Vec<u8> {
    format!(
        "notification/{}/{}/{}/{event_id:020}",
        escape(agent_id),
        escape(field),
        escape(value)
    )
    .into_bytes()
}

pub fn notification_prefix(agent_id: &str, field: &str, value: &str) -> Vec<u8> {
    format!(
        "notification/{}/{}/{}/",
        escape(agent_id),
        escape(field),
        escape(value)
    )
    .into_bytes()
}

pub fn notification_id_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!("notification_id/{}/{event_id:020}", escape(agent_id)).into_bytes()
}

pub fn notification_id_prefix(agent_id: &str) -> Vec<u8> {
    format!("notification_id/{}/", escape(agent_id)).into_bytes()
}

pub fn notification_rev_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!(
        "notification_rev/{}/{:020}",
        escape(agent_id),
        u64::MAX - event_id
    )
    .into_bytes()
}

pub fn notification_rev_prefix(agent_id: &str) -> Vec<u8> {
    format!("notification_rev/{}/", escape(agent_id)).into_bytes()
}

pub fn notification_prev_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!("notification_prev/{}/{event_id:020}", escape(agent_id)).into_bytes()
}

pub fn notification_next_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!("notification_next/{}/{event_id:020}", escape(agent_id)).into_bytes()
}

pub fn notification_tail_key(agent_id: &str) -> Vec<u8> {
    format!("notification_tail/{}/", escape(agent_id)).into_bytes()
}

pub fn tui_clear_rev_key(agent_id: &str, event_id: u64) -> Vec<u8> {
    format!(
        "tui_clear_rev/{}/{:020}",
        escape(agent_id),
        u64::MAX - event_id
    )
    .into_bytes()
}

pub fn tui_clear_rev_prefix(agent_id: &str) -> Vec<u8> {
    format!("tui_clear_rev/{}/", escape(agent_id)).into_bytes()
}

pub fn trace_key(agent_id: &str, tool_call_id: &str, event_id: u64) -> Vec<u8> {
    format!(
        "trace/{}/{}/{event_id:020}",
        escape(agent_id),
        escape(tool_call_id)
    )
    .into_bytes()
}

pub fn trace_prefix(agent_id: &str, tool_call_id: &str) -> Vec<u8> {
    format!("trace/{}/{}/", escape(agent_id), escape(tool_call_id)).into_bytes()
}

pub fn id_from_index_value(value: &[u8]) -> Option<u64> {
    let raw: [u8; 8] = value.try_into().ok()?;
    Some(u64::from_be_bytes(raw))
}

pub fn id_value(event_id: u64) -> [u8; 8] {
    event_id.to_be_bytes()
}

fn timestamp_micros(ts: f64) -> u64 {
    if !ts.is_finite() || ts <= 0.0 {
        return 0;
    }
    let micros = ts * 1_000_000.0;
    if micros >= u64::MAX as f64 {
        u64::MAX
    } else {
        micros as u64
    }
}

fn escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                out.push(nibble(byte >> 4));
                out.push(nibble(byte & 0x0f));
            }
        }
    }
    out
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(nibble(byte >> 4));
        out.push(nibble(byte & 0x0f));
    }
    out
}

fn nibble(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!("nibble is four bits"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_id_key_sorts_newest_first() {
        let newer = type_id_key("agent", "tool_call", 20);
        let older = type_id_key("agent", "tool_call", 10);
        assert!(newer < older);
    }

    #[test]
    fn type_ts_key_sorts_newest_timestamp_first() {
        let newer = type_ts_key("agent", "refresh_complete", 20.0, 1);
        let older = type_ts_key("agent", "refresh_complete", 10.0, 2);
        assert!(newer < older);
    }

    #[test]
    fn source_file_hash_is_stable_and_short() {
        assert_eq!(
            source_file_hash("logs/events.jsonl"),
            source_file_hash("logs/events.jsonl")
        );
        assert_eq!(source_file_hash("logs/events.jsonl").len(), 32);
    }
}
