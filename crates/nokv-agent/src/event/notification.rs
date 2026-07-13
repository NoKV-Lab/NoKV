use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::types::EventRecord;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NotificationBlockMeta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub injection_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_system_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_history_tokens: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_usage: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NotificationSummaryEntry {
    pub id: u64,
    pub ts: f64,
    pub source: String,
    pub call_id: Option<String>,
    pub summary: Option<String>,
    pub sources: Vec<String>,
    pub meta: Option<NotificationBlockMeta>,
    pub raw_event: EventRecord,
}

impl NotificationSummaryEntry {
    pub fn from_event(record: EventRecord) -> Self {
        let fields = record.fields_json.as_object();
        let meta = fields
            .and_then(|fields| fields.get("meta"))
            .and_then(Value::as_object)
            .map(parse_block_meta);
        Self {
            id: record.id,
            ts: record.ts,
            source: source_base(&record.source_file),
            call_id: string_field(fields, "call_id"),
            summary: string_field(fields, "summary"),
            sources: string_array_field(fields, "sources"),
            meta,
            raw_event: record,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NotificationBlockSnapshot {
    pub id: u64,
    pub ts: f64,
    pub source: String,
    pub mode: Option<String>,
    pub call_id: Option<String>,
    pub sources: Vec<String>,
    pub meta: Option<NotificationBlockMeta>,
    pub raw_meta: Option<Value>,
    pub tool_meta: Option<Value>,
    pub agent_meta: Option<Value>,
    pub guidance: Option<Value>,
    pub notification_guidance: Option<String>,
    pub notifications: BTreeMap<String, String>,
    pub raw_event: EventRecord,
}

impl NotificationBlockSnapshot {
    pub fn from_event(record: EventRecord) -> Self {
        let fields_json = record.fields_json.clone();
        let fields = fields_json.as_object();
        let mut parsed = Self {
            id: record.id,
            ts: record.ts,
            source: source_base(&record.source_file),
            mode: string_field(fields, "mode"),
            call_id: string_field(fields, "call_id"),
            sources: string_array_field(fields, "sources"),
            meta: None,
            raw_meta: None,
            tool_meta: None,
            agent_meta: None,
            guidance: None,
            notification_guidance: None,
            notifications: BTreeMap::new(),
            raw_event: record,
        };

        if let Some(meta_env) = fields
            .and_then(|fields| fields.get("_meta"))
            .and_then(Value::as_object)
        {
            parse_meta_envelope(meta_env, &mut parsed);
        } else if let Some(payload) = fields
            .and_then(|fields| fields.get("payload"))
            .and_then(Value::as_object)
        {
            let legacy_meta = fields
                .and_then(|fields| fields.get("meta"))
                .and_then(Value::as_object);
            parse_legacy_payload(payload, legacy_meta, &mut parsed);
        }

        if parsed.raw_meta.is_none() {
            if let Some(meta) = fields
                .and_then(|fields| fields.get("meta"))
                .and_then(Value::as_object)
            {
                parsed.raw_meta = Some(Value::Object(meta.clone()));
            }
        }
        if let Some(raw_meta) = parsed.raw_meta.as_ref().and_then(Value::as_object) {
            if parsed.agent_meta.is_none() {
                parsed.agent_meta = Some(Value::Object(raw_meta.clone()));
            }
            parsed.meta = Some(parse_block_meta(raw_meta));
        }
        parsed
    }
}

fn parse_meta_envelope(env: &Map<String, Value>, snapshot: &mut NotificationBlockSnapshot) {
    if let Some(value) = object_value(env, "tool_meta") {
        snapshot.tool_meta = Some(value);
    }
    if let Some(agent) = object_value(env, "agent_meta") {
        snapshot.raw_meta = Some(agent.clone());
        snapshot.agent_meta = Some(agent);
    }
    if let Some(value) = object_value(env, "guidance") {
        snapshot.guidance = Some(value);
    }
    if let Some(value) = env.get("notification_guidance").and_then(Value::as_str) {
        snapshot.notification_guidance = Some(value.to_owned());
    }
    if let Some(notifications) = env.get("notifications").and_then(Value::as_object) {
        snapshot.notifications = encode_notification_channels(notifications);
    }
}

fn parse_legacy_payload(
    payload: &Map<String, Value>,
    meta: Option<&Map<String, Value>>,
    snapshot: &mut NotificationBlockSnapshot,
) {
    if let Some(value) = object_value(payload, "_tool") {
        snapshot.tool_meta = Some(value);
    }
    if let Some(value) = object_value(payload, "_runtime.state") {
        snapshot.agent_meta = Some(value);
    }
    if let Some(value) = object_value(payload, "_runtime.guidance") {
        snapshot.guidance = Some(value);
    }
    if let Some(runtime) = payload.get("_runtime").and_then(Value::as_object) {
        if let Some(value) = object_value(runtime, "state") {
            snapshot.agent_meta = Some(value);
        }
        if let Some(value) = object_value(runtime, "guidance") {
            snapshot.guidance = Some(value);
        }
    }
    snapshot.notification_guidance = payload
        .get("notification_guidance")
        .or_else(|| payload.get("_notification_guidance"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    if let Some(notifications) = payload.get("notifications").and_then(Value::as_object) {
        snapshot.notifications = encode_notification_channels(notifications);
    }
    if let Some(meta) = meta {
        snapshot.raw_meta = Some(Value::Object(meta.clone()));
    }
}

fn parse_block_meta(meta: &Map<String, Value>) -> NotificationBlockMeta {
    let context = meta.get("context").and_then(Value::as_object);
    NotificationBlockMeta {
        current_time: meta
            .get("current_time")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        injection_seq: meta.get("injection_seq").and_then(number_to_i64),
        context_system_tokens: context
            .and_then(|context| context.get("system_tokens"))
            .and_then(number_to_i64),
        context_history_tokens: context
            .and_then(|context| context.get("history_tokens"))
            .and_then(number_to_i64),
        context_usage: context
            .and_then(|context| context.get("usage"))
            .and_then(Value::as_f64),
    }
}

fn encode_notification_channels(notifications: &Map<String, Value>) -> BTreeMap<String, String> {
    notifications
        .iter()
        .map(|(channel, value)| {
            let encoded = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
            (channel.clone(), encoded)
        })
        .collect()
}

fn object_value(object: &Map<String, Value>, field: &str) -> Option<Value> {
    object
        .get(field)
        .and_then(Value::as_object)
        .map(|value| Value::Object(value.clone()))
}

fn string_field(fields: Option<&Map<String, Value>>, field: &str) -> Option<String> {
    fields
        .and_then(|fields| fields.get(field))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn string_array_field(fields: Option<&Map<String, Value>>, field: &str) -> Vec<String> {
    fields
        .and_then(|fields| fields.get(field))
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn number_to_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_f64().map(|value| value as i64))
}

fn source_base(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(path)
        .to_owned()
}
