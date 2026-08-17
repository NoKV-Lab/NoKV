/*
 * Copyright 2024-2026 The NoKV Authors.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Transport-free Workbench and generic Agent facades.
//!
//! This crate owns the frozen tool schemas, argument validation, stable error
//! envelope, and dispatch into an SDK-backed handler. It does not own metadata
//! state machines, object I/O, routing, or server behavior.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::OnceLock;

use serde::Deserialize;
use serde_json::{json, Value};

mod facade;
mod generic;
mod projection;

pub use facade::*;
pub use generic::*;
pub use projection::*;

pub const WORKBENCH_CONTRACT_SCHEMA: &str = "nokv.workbench.mcp_input_schemas.v1";
pub const WORKBENCH_TOOL_COUNT: usize = 18;
pub const GENERIC_AGENT_CONTRACT_SCHEMA: &str = "nokv.agent.generic.mcp_input_schemas.v1";
pub const GENERIC_AGENT_TOOL_COUNT: usize = 7;

const TOOL_DESCRIPTIONS: [(&str, &str); WORKBENCH_TOOL_COUNT] = [
    (
        "workbench_create",
        "Create one Agent workbench with the five standard virtual sections: input, scripts, outputs, logs, and metadata. Exact retries converge without creating another workspace.",
    ),
    (
        "workbench_put_file",
        "Publish one artifact at a section-relative jailed path. replace=false is create-only; replace=true is replace-only. This tool is never an upsert.",
    ),
    (
        "workbench_append",
        "Append bytes with generation fencing, creating the artifact when absent and retrying bounded write conflicts. The returned digest identifies the appended delta, not the full resulting body.",
    ),
    (
        "workbench_edit",
        "Replace exact UTF-8 text. The match must be unique unless replace_all=true; concurrent changes are re-read and revalidated, and a byte-identical result publishes nothing.",
    ),
    (
        "workbench_list",
        "List direct children with stable cursor pagination at live state or a leased snapshot id/name. This is non-recursive and an expired or reaped snapshot fails loudly.",
    ),
    (
        "workbench_stat",
        "Read a compact workbench, section, directory, or artifact card without reading the body, at live state or a leased snapshot id/name.",
    ),
    (
        "workbench_read",
        "Read structured JSON/YAML/text content or an explicit byte range returned as base64. if_none_match uses generation, and snapshot reads stay frozen at the selected leased view.",
    ),
    (
        "workbench_grep",
        "Find case-insensitive literal substrings in artifact bodies with at most 16 OR alternatives and an optional basename glob. This is not regular-expression grep.",
    ),
    (
        "workbench_search",
        "Query indexed artifact metadata with catalog predicates, stable sort, projection, and facets. Omit id for root-wide metadata search; use grep for body content.",
    ),
    (
        "workbench_aggregate",
        "Compute bounded count, sum, average, minimum, maximum, grouping, filtering, and sorting over indexed artifact metadata. Omit id for root-wide aggregation.",
    ),
    (
        "workbench_catalog",
        "Discover stable indexed field ids and their supported predicate, sort, facet, and aggregate capabilities for search and aggregate calls.",
    ),
    (
        "workbench_find",
        "Find workbenches by committed state and a case-insensitive literal match over the canonical run-manifest projection, with compact summaries by default.",
    ),
    (
        "workbench_commit",
        "Publish the canonical v1 run manifest and deterministic commit identity. Exact identity replay is idempotent; a different head conflicts unless replace=true is explicit.",
    ),
    (
        "workbench_snapshot",
        "Mint a leased point-in-time snapshot of a committed workbench, optionally naming and annotating it. The lease defaults to seven days, is capped at 90 days, and is liveness rather than archival retention.",
    ),
    (
        "workbench_snapshot_renew",
        "Resolve a snapshot by id or name and extend its live lease without ever shortening it. A retired or reaped snapshot cannot be renewed.",
    ),
    (
        "workbench_snapshot_retire",
        "Explicitly retire a snapshot by id or name and release its history hold. The first transition reports retired=true; an already retired or reaped retry reports retired=false and preserves the durable optional reason.",
    ),
    (
        "workbench_snapshot_list",
        "List snapshot ids, aliases, lease deadlines, annotations, and the alive, expired, retired, or reaped lifecycle state for one workbench.",
    ),
    (
        "workbench_restore",
        "Restore a live snapshot into an absent destination workbench while preserving the source. Staging stays hidden until atomic publication, immutable revisions are reused, and exact retries converge.",
    ),
];

const GENERIC_AGENT_TOOL_DESCRIPTIONS: [(&str, &str); GENERIC_AGENT_TOOL_COUNT] = [
    (
        "ls",
        "List direct children to discover paths. Not recursive; use stat for one path details.",
    ),
    (
        "stat",
        "Inspect a single path compact card: counts, body, schema, sample, catalog, and indexed values. Use read for file body content.",
    ),
    (
        "catalog",
        "Discover field ids for find predicates, projections, sort, facets, and aggregate group or measure fields.",
    ),
    (
        "read",
        "Read file body content. Structured mode returns JSON, YAML, or text records; bytes mode returns byte ranges.",
    ),
    (
        "aggregate",
        "Compute summary rows using catalog field ids: count, sum, avg, min, max, group, filter, and sort.",
    ),
    (
        "find",
        "Search paths with catalog field predicates and project fields. Use read for body content and stat for schema or sample.",
    ),
    (
        "grep",
        "Search file bodies for case-insensitive literal substrings and return matching lines with line numbers. patterns adds OR alternatives to pattern (at most 16); glob filters file names. Scope path to one directory or file when known.",
    ),
];

#[derive(Clone, Debug, PartialEq)]
pub struct AgentToolDefinition {
    pub name: String,
    pub description: &'static str,
    pub parameters: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub details: Value,
}

impl AgentError {
    pub fn invalid_arguments(message: impl Into<String>) -> Self {
        Self {
            code: "InvalidArguments".to_owned(),
            message: message.into(),
            retryable: false,
            details: json!({}),
        }
    }

    pub fn unknown_tool(name: &str) -> Self {
        Self {
            code: "UnknownTool".to_owned(),
            message: format!("unknown Workbench tool {name}"),
            retryable: false,
            details: json!({"tool": name}),
        }
    }

    pub fn unknown_generic_agent_tool(name: &str) -> Self {
        Self {
            code: "UnknownTool".to_owned(),
            message: format!("unknown Agent tool {name}"),
            retryable: false,
            details: json!({"tool": name}),
        }
    }

    pub fn backend(
        code: impl Into<String>,
        message: impl Into<String>,
        retryable: bool,
        details: Value,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable,
            details,
        }
    }

    pub fn as_value(&self) -> Value {
        json!({
            "status": "error",
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "details": self.details,
        })
    }
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for AgentError {}

/// SDK-backed execution boundary. Implementations receive only calls that pass
/// the exact checked-in JSON Schema.
pub trait WorkbenchToolHandler: Send + Sync {
    fn execute(&self, name: &str, arguments: &Value) -> Result<Value, AgentError>;
}

/// SDK-backed execution boundary for the explicit seven-tool generic Agent
/// profile. Its arguments use the frozen path-native contract, independently
/// of the 18-tool Workbench profile.
pub trait GenericAgentToolHandler: Send + Sync {
    fn execute(&self, name: &str, arguments: &Value) -> Result<Value, AgentError>;
}

impl<F> WorkbenchToolHandler for F
where
    F: Fn(&str, &Value) -> Result<Value, AgentError> + Send + Sync,
{
    fn execute(&self, name: &str, arguments: &Value) -> Result<Value, AgentError> {
        self(name, arguments)
    }
}

impl<F> GenericAgentToolHandler for F
where
    F: Fn(&str, &Value) -> Result<Value, AgentError> + Send + Sync,
{
    fn execute(&self, name: &str, arguments: &Value) -> Result<Value, AgentError> {
        self(name, arguments)
    }
}

pub fn tool_definitions() -> Vec<AgentToolDefinition> {
    let contract = contract();
    TOOL_DESCRIPTIONS
        .iter()
        .map(|(name, description)| AgentToolDefinition {
            name: (*name).to_owned(),
            description,
            parameters: contract
                .input_schemas
                .get(*name)
                .expect("validated contract contains every tool")
                .clone(),
        })
        .collect()
}

pub fn generic_agent_tool_definitions() -> Vec<AgentToolDefinition> {
    let contract = generic_agent_contract();
    GENERIC_AGENT_TOOL_DESCRIPTIONS
        .iter()
        .map(|(name, description)| AgentToolDefinition {
            name: (*name).to_owned(),
            description,
            parameters: contract
                .input_schemas
                .get(*name)
                .expect("validated generic Agent contract contains every tool")
                .clone(),
        })
        .collect()
}

pub fn validate_tool_arguments(name: &str, arguments: &Value) -> Result<(), AgentError> {
    let schema = contract()
        .input_schemas
        .get(name)
        .ok_or_else(|| AgentError::unknown_tool(name))?;
    validate_schema(schema, arguments, "$")
        .map_err(|message| AgentError::invalid_arguments(format!("{name}: {message}")))
}

pub fn execute_tool(
    handler: &impl WorkbenchToolHandler,
    name: &str,
    arguments: &Value,
) -> Result<Value, AgentError> {
    validate_tool_arguments(name, arguments)?;
    handler.execute(name, arguments)
}

pub fn validate_generic_agent_tool_arguments(
    name: &str,
    arguments: &Value,
) -> Result<(), AgentError> {
    let schema = generic_agent_contract()
        .input_schemas
        .get(name)
        .ok_or_else(|| AgentError::unknown_generic_agent_tool(name))?;
    validate_schema(schema, arguments, "$")
        .map_err(|message| AgentError::invalid_arguments(format!("{name}: {message}")))
}

pub fn execute_generic_agent_tool(
    handler: &impl GenericAgentToolHandler,
    name: &str,
    arguments: &Value,
) -> Result<Value, AgentError> {
    validate_generic_agent_tool_arguments(name, arguments)?;
    handler.execute(name, arguments)
}

#[derive(Deserialize)]
struct ContractAsset {
    #[serde(rename = "inputSchemas")]
    input_schemas: BTreeMap<String, Value>,
    schema: String,
}

fn contract() -> &'static ContractAsset {
    static CONTRACT: OnceLock<ContractAsset> = OnceLock::new();
    CONTRACT.get_or_init(|| {
        let contract: ContractAsset =
            serde_json::from_str(include_str!("../workbench_contract_schema.json"))
                .expect("checked-in Workbench contract must be valid JSON");
        assert_eq!(
            contract.schema, WORKBENCH_CONTRACT_SCHEMA,
            "checked-in Workbench contract schema marker differs"
        );
        let expected = TOOL_DESCRIPTIONS
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        let actual = contract
            .input_schemas
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual, expected,
            "checked-in Workbench contract must contain exactly 18 tools"
        );
        contract
    })
}

fn generic_agent_contract() -> &'static ContractAsset {
    static CONTRACT: OnceLock<ContractAsset> = OnceLock::new();
    CONTRACT.get_or_init(|| {
        let contract: ContractAsset =
            serde_json::from_str(include_str!("../generic_agent_contract_schema.json"))
                .expect("checked-in generic Agent contract must be valid JSON");
        assert_eq!(
            contract.schema, GENERIC_AGENT_CONTRACT_SCHEMA,
            "checked-in generic Agent contract schema marker differs"
        );
        let expected = GENERIC_AGENT_TOOL_DESCRIPTIONS
            .iter()
            .map(|(name, _)| (*name).to_owned())
            .collect::<BTreeSet<_>>();
        let actual = contract
            .input_schemas
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            actual, expected,
            "checked-in generic Agent contract tool set differs"
        );
        contract
    })
}

fn validate_schema(schema: &Value, value: &Value, path: &str) -> Result<(), String> {
    let schema = schema
        .as_object()
        .ok_or_else(|| format!("{path}: contract schema is not an object"))?;

    if let Some(branches) = schema.get("allOf") {
        for branch in schema_array(branches, path, "allOf")? {
            validate_schema(branch, value, path)?;
        }
    }
    if let Some(branches) = schema.get("anyOf") {
        let matched = schema_array(branches, path, "anyOf")?
            .iter()
            .any(|branch| validate_schema(branch, value, path).is_ok());
        if !matched {
            return Err(format!("{path}: does not match any accepted shape"));
        }
    }
    if let Some(branches) = schema.get("oneOf") {
        let matched = schema_array(branches, path, "oneOf")?
            .iter()
            .filter(|branch| validate_schema(branch, value, path).is_ok())
            .count();
        if matched != 1 {
            return Err(format!(
                "{path}: must match exactly one accepted shape, matched {matched}"
            ));
        }
    }

    if let Some(expected) = schema.get("type") {
        let accepted = match expected {
            Value::String(expected) => matches_type(value, expected),
            Value::Array(expected) => expected
                .iter()
                .filter_map(Value::as_str)
                .any(|expected| matches_type(value, expected)),
            _ => false,
        };
        if !accepted {
            return Err(format!("{path}: has the wrong JSON type"));
        }
    }

    if let Some(allowed) = schema.get("enum").and_then(Value::as_array) {
        if !allowed.contains(value) {
            return Err(format!("{path}: value is outside the accepted enum"));
        }
    }

    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        let object = value
            .as_object()
            .ok_or_else(|| format!("{path}: required fields need an object"))?;
        for field in required {
            let field = field
                .as_str()
                .ok_or_else(|| format!("{path}: contract required field is not a string"))?;
            if !object.contains_key(field) {
                return Err(format!("{path}: missing required field {field}"));
            }
        }
    }

    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        if let Some(object) = value.as_object() {
            for (field, field_value) in object {
                if let Some(field_schema) = properties.get(field) {
                    validate_schema(field_schema, field_value, &format!("{path}.{field}"))?;
                } else if schema.get("additionalProperties") == Some(&Value::Bool(false)) {
                    return Err(format!("{path}: unknown field {field}"));
                }
            }
        }
    }

    if let (Some(items), Some(values)) = (schema.get("items"), value.as_array()) {
        for (index, item) in values.iter().enumerate() {
            validate_schema(items, item, &format!("{path}[{index}]"))?;
        }
    }

    validate_bounds(schema, value, path)?;
    Ok(())
}

fn validate_bounds(
    schema: &serde_json::Map<String, Value>,
    value: &Value,
    path: &str,
) -> Result<(), String> {
    if let Some(string) = value.as_str() {
        let characters = string.chars().count() as u64;
        if schema
            .get("minLength")
            .and_then(Value::as_u64)
            .is_some_and(|minimum| characters < minimum)
        {
            return Err(format!("{path}: string is shorter than minLength"));
        }
        if schema
            .get("maxLength")
            .and_then(Value::as_u64)
            .is_some_and(|maximum| characters > maximum)
        {
            return Err(format!("{path}: string is longer than maxLength"));
        }
        if let Some(pattern) = schema.get("pattern").and_then(Value::as_str) {
            if !matches_pattern(pattern, string)? {
                return Err(format!("{path}: string does not match required pattern"));
            }
        }
    }

    if let Some(number) = value.as_f64() {
        if schema
            .get("minimum")
            .and_then(Value::as_f64)
            .is_some_and(|minimum| number < minimum)
        {
            return Err(format!("{path}: number is below minimum"));
        }
        if schema
            .get("maximum")
            .and_then(Value::as_f64)
            .is_some_and(|maximum| number > maximum)
        {
            return Err(format!("{path}: number is above maximum"));
        }
    }

    if let Some(array) = value.as_array() {
        if schema
            .get("maxItems")
            .and_then(Value::as_u64)
            .is_some_and(|maximum| array.len() as u64 > maximum)
        {
            return Err(format!("{path}: array exceeds maxItems"));
        }
    }

    if let Some(object) = value.as_object() {
        if schema
            .get("maxProperties")
            .and_then(Value::as_u64)
            .is_some_and(|maximum| object.len() as u64 > maximum)
        {
            return Err(format!("{path}: object exceeds maxProperties"));
        }
    }
    Ok(())
}

fn matches_type(value: &Value, expected: &str) -> bool {
    match expected {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value
            .as_number()
            .is_some_and(|number| number.is_i64() || number.is_u64()),
        _ => false,
    }
}

fn matches_pattern(pattern: &str, value: &str) -> Result<bool, String> {
    match pattern {
        "^sha256:[0-9a-f]{64}$" => Ok(value.len() == 71
            && value.starts_with("sha256:")
            && value[7..]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))),
        _ => Err(format!("contract contains unsupported pattern {pattern:?}")),
    }
}

fn schema_array<'a>(value: &'a Value, path: &str, keyword: &str) -> Result<&'a [Value], String> {
    value
        .as_array()
        .map(Vec::as_slice)
        .ok_or_else(|| format!("{path}: contract {keyword} is not an array"))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn contract_contains_exactly_the_eighteen_workbench_tools() {
        let definitions = tool_definitions();
        assert_eq!(definitions.len(), WORKBENCH_TOOL_COUNT);
        assert_eq!(
            definitions
                .iter()
                .map(|definition| definition.name.as_str())
                .collect::<BTreeSet<_>>(),
            TOOL_DESCRIPTIONS
                .iter()
                .map(|(name, _)| *name)
                .collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn validator_rejects_missing_unknown_and_wrong_enum_fields() {
        assert!(validate_tool_arguments("workbench_create", &json!({})).is_err());
        assert!(validate_tool_arguments(
            "workbench_create",
            &json!({"id": "run-42", "extra": true})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "workbench_put_file",
            &json!({"id": "run-42", "section": "other", "path": "a.txt"})
        )
        .is_err());
    }

    #[test]
    fn validator_enforces_digest_pattern_and_snapshot_selector_exclusivity() {
        assert!(validate_tool_arguments(
            "workbench_commit",
            &json!({
                "id": "run-42",
                "manifest": {},
                "content_digest_uri": "sha256:ABC"
            })
        )
        .is_err());
        assert!(validate_tool_arguments(
            "workbench_snapshot_retire",
            &json!({"id": "run-42", "snapshot_id": 1, "name": "checkpoint"})
        )
        .is_err());
        assert!(validate_tool_arguments(
            "workbench_snapshot_retire",
            &json!({"id": "run-42", "snapshot_id": 1})
        )
        .is_ok());
    }

    #[test]
    fn dispatch_never_calls_backend_for_invalid_arguments() {
        let called = AtomicBool::new(false);
        let handler = |_: &str, _: &Value| {
            called.store(true, Ordering::Relaxed);
            Ok(json!({"status": "success"}))
        };
        assert!(execute_tool(&handler, "workbench_create", &json!({})).is_err());
        assert!(!called.load(Ordering::Relaxed));

        assert!(execute_tool(&handler, "workbench_create", &json!({"id": "run-42"})).is_ok());
        assert!(called.load(Ordering::Relaxed));
    }

    #[test]
    fn errors_have_the_stable_agent_envelope() {
        let error = AgentError::backend(
            "Conflict",
            "generation changed",
            true,
            json!({"generation": 9}),
        );
        assert_eq!(
            error.as_value(),
            json!({
                "status": "error",
                "code": "Conflict",
                "message": "generation changed",
                "retryable": true,
                "details": {"generation": 9},
            })
        );
    }
}
