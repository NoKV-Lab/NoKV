use std::io::BufRead;

use serde_json::{Map, Value};

const MAX_INLINE_FIELDS_JSON_BYTES: usize = 32 * 1024;
const INGEST_RECORD_BATCH_LIMIT: usize = 512;

use super::store::AgentEventStore;
use super::types::{
    AgentEventResult, EventProjection, IndexCoverage, IngestReport, NewEventRecord,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonlIngestOptions {
    pub agent_id: String,
    pub source_file: String,
    pub file_size: u64,
}

pub fn ingest_jsonl_reader<S, R>(
    store: &S,
    options: JsonlIngestOptions,
    mut reader: R,
) -> AgentEventResult<IngestReport>
where
    S: AgentEventStore + ?Sized,
    R: BufRead,
{
    let mut offset = 0_u64;
    let mut line_no = 0_u64;
    let mut report = IngestReport {
        accepted: 0,
        duplicates: 0,
        parse_errors: 0,
        partial_lines: 0,
        coverage: empty_coverage(&options.agent_id, &options.source_file, options.file_size),
    };
    let mut records = Vec::new();
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        let line_offset = offset;
        offset = offset.saturating_add(read as u64);
        if line.iter().all(|byte| byte.is_ascii_whitespace()) {
            continue;
        }
        if !line.ends_with(b"\n") {
            report.partial_lines = report.partial_lines.saturating_add(1);
            break;
        }
        line_no = line_no.saturating_add(1);
        match parse_event_line(&options, line_offset, line_no, &line) {
            Ok(record) => {
                records.push(record);
                if records.len() >= INGEST_RECORD_BATCH_LIMIT {
                    let batch =
                        store.ingest_batch(std::mem::take(&mut records), options.file_size)?;
                    merge_report(&mut report, batch);
                }
            }
            Err(_) => report.parse_errors = report.parse_errors.saturating_add(1),
        }
    }
    if !records.is_empty() {
        let batch = store.ingest_batch(records, options.file_size)?;
        merge_report(&mut report, batch);
    }
    Ok(report)
}

fn merge_report(total: &mut IngestReport, batch: IngestReport) {
    total.accepted = total.accepted.saturating_add(batch.accepted);
    total.duplicates = total.duplicates.saturating_add(batch.duplicates);
    total.parse_errors = total.parse_errors.saturating_add(batch.parse_errors);
    total.partial_lines = total.partial_lines.saturating_add(batch.partial_lines);
    if batch.coverage.has_rows() {
        total.coverage = batch.coverage;
    }
}

fn parse_event_line(
    options: &JsonlIngestOptions,
    source_offset: u64,
    source_line: u64,
    line: &[u8],
) -> AgentEventResult<NewEventRecord> {
    let value: Value = serde_json::from_slice(line)?;
    let Some(object) = value.as_object() else {
        return Err(super::types::AgentEventError::Json(
            "event line is not a JSON object".to_owned(),
        ));
    };
    let Some(event_type) = object.get("type").and_then(Value::as_str) else {
        return Err(super::types::AgentEventError::Json(
            "event line is missing string type".to_owned(),
        ));
    };
    let ts = object.get("ts").and_then(Value::as_f64).unwrap_or(0.0);
    Ok(NewEventRecord {
        agent_id: options.agent_id.clone(),
        source_file: options.source_file.clone(),
        source_offset,
        source_line,
        ts,
        event_type: event_type.to_owned(),
        fields_json: fields_json(object),
        projection: project_event(object),
    })
}

fn fields_json(object: &Map<String, Value>) -> Value {
    let mut fields = object.clone();
    for key in ["type", "ts", "address", "agent_name"] {
        fields.remove(key);
    }
    compact_fields_json(Value::Object(fields))
}

fn compact_fields_json(value: Value) -> Value {
    let encoded_len = serde_json::to_vec(&value)
        .map(|bytes| bytes.len())
        .unwrap_or(usize::MAX);
    if encoded_len <= MAX_INLINE_FIELDS_JSON_BYTES {
        return value;
    }
    let keys = value
        .as_object()
        .map(|object| {
            object
                .keys()
                .cloned()
                .map(Value::String)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    serde_json::json!({
        "_nokv_compacted": true,
        "original_json_bytes": encoded_len,
        "keys": keys,
    })
}

fn project_event(object: &Map<String, Value>) -> EventProjection {
    let parsed_tool_args = object.get("tool_args").and_then(parse_tool_args);
    let tool_args = parsed_tool_args.as_ref().and_then(Value::as_object);
    EventProjection {
        tool_name: object
            .get("tool_name")
            .or_else(|| object.get("tool"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        tool_action: tool_args
            .and_then(|args| args.get("action"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        tool_call_id: object
            .get("tool_call_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        tool_trace_id: object
            .get("tool_trace_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        api_call_id: object
            .get("api_call_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        command_head: tool_args
            .and_then(|args| args.get("command"))
            .and_then(Value::as_str)
            .and_then(command_head),
        file_extension: tool_args
            .and_then(|args| args.get("file_path"))
            .and_then(Value::as_str)
            .and_then(file_extension),
        notification_channel: object
            .get("channel")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        notification_ref_id: object
            .get("ref_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        notification_event_id: object
            .get("event_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        notification_call_id: object
            .get("call_id")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        status: object
            .get("status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        error: object
            .get("error")
            .or_else(|| object.get("exception_message"))
            .or_else(|| object.get("exception"))
            .or_else(|| object.get("reason"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    }
}

fn parse_tool_args(value: &Value) -> Option<Value> {
    match value {
        Value::Object(_) => Some(value.clone()),
        Value::String(text) => serde_json::from_str::<Value>(text).ok(),
        _ => None,
    }
}

fn command_head(command: &str) -> Option<String> {
    command.split_whitespace().next().map(|head| {
        if head.contains('/') {
            std::path::Path::new(head)
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("unknown")
                .to_owned()
        } else {
            head.to_owned()
        }
    })
}

fn file_extension(path: &str) -> Option<String> {
    std::path::Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase())
}

pub(crate) fn empty_coverage(agent_id: &str, source_file: &str, file_size: u64) -> IndexCoverage {
    IndexCoverage {
        agent_id: agent_id.to_owned(),
        source_file: source_file.to_owned(),
        file_size,
        min_offset: None,
        max_offset: None,
        row_count: 0,
    }
}
