use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use crate::fs::{normalize_path, path_name};
use crate::{
    AgentFs, AgentIndexError, AgentIndexField, AgentIndexResult, AgentIndexRow, AgentNode,
    AgentNodeKind, AgentPredicateOp, AgentPredicateValue, AgentStore,
};

const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 100;
const DEFAULT_FIND_LIMIT: usize = 10;
const MAX_FIND_LIMIT: usize = 10;
const DEFAULT_AGGREGATE_LIMIT: usize = 20;
const MAX_AGGREGATE_LIMIT: usize = 100;
const DEFAULT_GREP_LIMIT: usize = 100;
const MAX_GREP_LIMIT: usize = 100;

#[derive(Clone, Debug, PartialEq)]
pub struct AgentToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
}

pub fn agent_tool_definitions() -> Vec<AgentToolDefinition> {
    vec![
        AgentToolDefinition {
            name: "ls",
            description: "List direct children in the agent-native filesystem-shaped store.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "stat",
            description: "Inspect one agent-native path and its query catalog/indexed values.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {"path": {"type": "string"}},
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "catalog",
            description: "Discover field ids for find predicates, projections, sort, facets, and aggregate fields.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "field_prefix": {"type": ["string", "null"]},
                    "include_facets": {"type": "boolean"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "read",
            description: "Read file body content from the agent-native store.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "format": {"type": "string", "enum": ["structured", "bytes"]},
                    "cursor": {"type": ["string", "null"]},
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "aggregate",
            description: "Compute summary rows using catalog field ids: count, sum, avg, min, max, group, filter, and sort.",
            parameters: json!({
                "type": "object",
                "required": ["path", "measures"],
                "properties": {
                    "path": {"type": "string"},
                    "predicates": {"type": "array", "items": {"type": "object"}},
                    "group_by": {"type": "array", "items": {"type": "string"}},
                    "measures": {"type": "array", "items": {"type": "object"}},
                    "sort": {"type": "array", "items": {"type": "object"}},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGGREGATE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "find",
            description: "Search agent-native indexed paths with predicates, projection fields, sort, and facets.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "predicates": {"type": "array", "items": {"type": "object"}},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "facets": {"type": "array", "items": {"type": "string"}},
                    "sort": {"type": "array", "items": {"type": "object"}},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_FIND_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "grep",
            description: "Search text files for a case-insensitive literal substring and return matching lines.",
            parameters: json!({
                "type": "object",
                "required": ["path", "pattern", "recursive"],
                "properties": {
                    "path": {"type": "string"},
                    "pattern": {"type": "string"},
                    "recursive": {"type": "boolean"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_GREP_LIMIT}
                },
                "additionalProperties": false
            }),
        },
    ]
}

pub fn execute_agent_tool<S>(fs: &AgentFs<S>, name: &str, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    match name {
        "ls" => execute_ls(fs, args),
        "stat" => execute_stat(fs, args),
        "catalog" => execute_catalog(fs, args),
        "read" => execute_read(fs, args),
        "find" => execute_find(fs, args),
        "aggregate" => execute_aggregate(fs, args),
        "grep" => execute_grep(fs, args),
        other => Err(AgentIndexError::InvalidArgument(format!(
            "unknown agent tool {other}"
        ))),
    }
}

fn execute_ls<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let limit = optional_usize_arg(args, "limit", MAX_PAGE_LIMIT)?.unwrap_or(DEFAULT_PAGE_LIMIT);
    let cursor = optional_string_arg(args, "cursor")?.and_then(hex_cursor);
    let (entries, next_cursor, truncated) = fs.list(path, cursor.as_deref(), limit)?;
    Ok(json!({
        "path": normalize_path(path)?,
        "entry_count": entries.len(),
        "entries": entries.iter().map(list_entry_json).collect::<Vec<_>>(),
        "next_cursor": next_cursor.as_deref().map(hex_encode),
        "truncated": truncated,
    }))
}

fn execute_stat<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let node = fs
        .node(path)?
        .ok_or_else(|| AgentIndexError::NotFound(path.to_owned()))?;
    let rows = fs.index_rows(path).unwrap_or_default();
    Ok(json!({
        "card": card_json(&node, &fs.catalog(path).unwrap_or_default(), rows.first()),
    }))
}

fn execute_catalog<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let field_prefix = optional_string_arg(args, "field_prefix")?;
    let include_facets = optional_bool_arg(args, "include_facets")?.unwrap_or(false);
    let fields = fs.catalog(path)?;
    let rows = fs.index_rows(path)?;
    let catalog = catalog_json(&fields, &rows, field_prefix.as_deref(), include_facets);
    let catalog_empty = fields.is_empty();
    Ok(json!({
        "path": normalize_path(path)?,
        "catalog_empty": catalog_empty,
        "catalog": catalog,
        "child_catalogs": [],
    }))
}

fn execute_find<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let fields = fields_arg(args)?;
    let predicates = predicates_arg(args)?;
    let sort = sort_arg(args)?;
    let facets = facets_arg(args)?;
    let offset = cursor_offset(args)?;
    let limit = optional_usize_arg(args, "limit", MAX_FIND_LIMIT)?.unwrap_or(DEFAULT_FIND_LIMIT);
    let mut rows = fs.index_rows(path)?;
    rows.retain(|row| {
        predicates
            .iter()
            .all(|predicate| row_matches(row, predicate))
    });
    sort_rows(&mut rows, &sort);
    let match_count = rows.len();
    let facet_json = facets_json(&rows, &facets);
    let matches = rows
        .iter()
        .skip(offset)
        .take(limit)
        .map(|row| find_match_json(row, fields.as_deref()))
        .collect::<Vec<_>>();
    let next_offset = offset.saturating_add(matches.len());
    Ok(json!({
        "path": normalize_path(path)?,
        "match_count": match_count,
        "matches": matches,
        "facets": facet_json,
        "next_cursor": (next_offset < match_count).then(|| next_offset.to_string()),
        "truncated": next_offset < match_count,
    }))
}

fn execute_aggregate<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let predicates = predicates_arg(args)?;
    let group_by = string_array_arg(args, "group_by")?.unwrap_or_default();
    let measures = measures_arg(args)?;
    let sort = aggregate_sort_arg(args)?;
    let limit =
        optional_usize_arg(args, "limit", MAX_AGGREGATE_LIMIT)?.unwrap_or(DEFAULT_AGGREGATE_LIMIT);
    let mut rows = fs.index_rows(path)?;
    rows.retain(|row| {
        predicates
            .iter()
            .all(|predicate| row_matches(row, predicate))
    });
    let input_match_count = rows.len();
    let mut groups = aggregate_groups(&rows, &group_by, &measures)?;
    sort_groups(&mut groups, &sort);
    let group_count = groups.len();
    let truncated = groups.len() > limit;
    groups.truncate(limit);
    Ok(json!({
        "path": normalize_path(path)?,
        "input_match_count": input_match_count,
        "row_count": groups.len(),
        "group_count": group_count,
        "groups": groups,
        "truncated": truncated,
    }))
}

fn execute_grep<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let pattern = required_string_arg(args, "pattern")?;
    let recursive = optional_bool_arg(args, "recursive")?.ok_or_else(|| {
        AgentIndexError::InvalidArgument("grep requires a boolean recursive argument".to_owned())
    })?;
    let limit = optional_usize_arg(args, "limit", MAX_GREP_LIMIT)?.unwrap_or(DEFAULT_GREP_LIMIT);
    let offset = cursor_offset(args)?;
    let needle = pattern.to_lowercase();
    let mut all_matches = Vec::new();
    let mut bytes_read = 0_usize;
    let files = fs.files_under(path, recursive)?;
    for file in &files {
        let bytes = fs.read_file(&file.path)?;
        bytes_read = bytes_read.saturating_add(bytes.len());
        let text = String::from_utf8_lossy(&bytes);
        for (line_index, line) in text.lines().enumerate() {
            if line.to_lowercase().contains(&needle) {
                all_matches.push(json!({
                    "path": file.path,
                    "line_number": line_index + 1,
                    "snippet": line,
                }));
            }
        }
    }
    let match_count = all_matches.len();
    let matches = all_matches
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let next_offset = offset.saturating_add(matches.len());
    Ok(json!({
        "path": normalize_path(path)?,
        "pattern": pattern,
        "recursive": recursive,
        "matches": matches,
        "files_scanned": files.len(),
        "bytes_read": bytes_read,
        "next_cursor": (next_offset < match_count).then(|| next_offset.to_string()),
        "truncated": next_offset < match_count,
    }))
}

fn execute_read<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let format = object_args(args)?
        .get("format")
        .and_then(Value::as_str)
        .unwrap_or("structured");
    let offset = optional_u64_arg(args, "offset")?.unwrap_or(0) as usize;
    let limit = optional_usize_arg(args, "limit", MAX_PAGE_LIMIT)?.unwrap_or(DEFAULT_PAGE_LIMIT);
    let cursor = cursor_offset(args)?;
    let bytes = fs.read_file(path)?;
    if format == "bytes" {
        let end = offset.saturating_add(limit).min(bytes.len());
        let range = bytes.get(offset..end).unwrap_or_default().to_vec();
        return Ok(json!({
            "path": normalize_path(path)?,
            "total_size_bytes": bytes.len(),
            "format": "bytes",
            "record_type": null,
            "record_count": null,
            "cursor": offset.to_string(),
            "next_cursor": (end < bytes.len()).then(|| end.to_string()),
            "truncated": end < bytes.len(),
            "items": [],
            "bytes": range,
            "bytes_read": end.saturating_sub(offset),
        }));
    }
    if format != "structured" {
        return Err(AgentIndexError::InvalidArgument(format!(
            "unsupported read format {format}; expected structured or bytes"
        )));
    }
    let records = structured_records(path, &bytes);
    let record_count = records.len();
    let items = records
        .into_iter()
        .enumerate()
        .skip(cursor)
        .take(limit)
        .map(|(index, value)| json!({"index": index, "value": value}))
        .collect::<Vec<_>>();
    let next_offset = cursor.saturating_add(items.len());
    Ok(json!({
        "path": normalize_path(path)?,
        "total_size_bytes": bytes.len(),
        "format": "structured",
        "record_type": "json",
        "record_count": record_count,
        "cursor": cursor.to_string(),
        "next_cursor": (next_offset < record_count).then(|| next_offset.to_string()),
        "truncated": next_offset < record_count,
        "items": items,
        "bytes": null,
        "bytes_read": bytes.len(),
    }))
}

#[derive(Clone)]
struct Predicate {
    field: String,
    op: AgentPredicateOp,
    value: Option<AgentPredicateValue>,
}

#[derive(Clone)]
struct Sort {
    field: String,
    desc: bool,
}

#[derive(Clone)]
struct Measure {
    name: String,
    op: String,
    field: Option<String>,
}

fn card_json(node: &AgentNode, catalog: &[AgentIndexField], row: Option<&AgentIndexRow>) -> Value {
    json!({
        "path": node.path,
        "name": node.name,
        "kind": node_kind_name(&node.kind),
        "size_bytes": node.size_bytes,
        "entry_count": null,
        "record_count": null,
        "schema": null,
        "sample": [],
        "body": (node.kind == AgentNodeKind::File).then(|| json!({
            "producer": "nokv-agent",
            "size": node.size_bytes.unwrap_or(0),
            "content_type": node.content_type.clone().unwrap_or_else(|| "application/octet-stream".to_owned()),
        })),
        "catalog": catalog_json(catalog, &[], None, false),
        "indexed_values": row
            .map(|row| row.values.iter().map(index_value_json).collect::<Vec<_>>())
            .unwrap_or_default(),
    })
}

fn list_entry_json(node: &AgentNode) -> Value {
    json!({
        "path": node.path,
        "name": node.name,
        "kind": node_kind_name(&node.kind),
        "size_bytes": node.size_bytes,
        "entry_count": null,
    })
}

fn find_match_json(row: &AgentIndexRow, fields: Option<&[String]>) -> Value {
    let Some(fields) = fields else {
        return json!({"path": row.path});
    };
    let values = fields
        .iter()
        .filter_map(|field| {
            let values = values_for_field(row, field)
                .into_iter()
                .map(|value| value.to_json())
                .collect::<Vec<_>>();
            match values.as_slice() {
                [] => None,
                [value] => Some((field.clone(), value.clone())),
                _ => Some((field.clone(), Value::Array(values))),
            }
        })
        .collect::<Map<_, _>>();
    json!({"path": row.path, "values": values})
}

fn catalog_json(
    fields: &[AgentIndexField],
    rows: &[AgentIndexRow],
    field_prefix: Option<&str>,
    include_facets: bool,
) -> Value {
    let fields = fields
        .iter()
        .filter(|field| field_prefix.is_none_or(|prefix| field.field.id.starts_with(prefix)))
        .collect::<Vec<_>>();
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for field in &fields {
        let operators = field
            .operators
            .iter()
            .map(predicate_op_name)
            .collect::<Vec<_>>()
            .join(",");
        grouped
            .entry(operators)
            .or_default()
            .push(field.field.id.clone());
    }
    json!({
        "filterable": grouped
            .into_iter()
            .map(|(operators, fields)| json!({
                "operators": operators.split(',').filter(|value| !value.is_empty()).collect::<Vec<_>>(),
                "fields": fields,
            }))
            .collect::<Vec<_>>(),
        "sortable": fields
            .iter()
            .filter(|field| field.sortable)
            .map(|field| field.field.id.clone())
            .collect::<Vec<_>>(),
        "facetable": fields
            .iter()
            .filter(|field| field.facetable)
            .map(|field| field.field.id.clone())
            .collect::<Vec<_>>(),
        "facets": if include_facets {
            let facet_fields = fields
                .iter()
                .filter(|field| field.facetable)
                .map(|field| field.field.id.clone())
                .collect::<Vec<_>>();
            facets_json(rows, &facet_fields)
        } else {
            Vec::new()
        },
    })
}

fn facets_json(rows: &[AgentIndexRow], facets: &[String]) -> Vec<Value> {
    facets
        .iter()
        .map(|field| {
            let mut counts = BTreeMap::<String, (Value, usize)>::new();
            for row in rows {
                for value in values_for_field(row, field) {
                    let json = value.to_json();
                    let key = json.to_string();
                    let entry = counts.entry(key).or_insert((json, 0));
                    entry.1 += 1;
                }
            }
            json!({
                "field": field,
                "values": counts
                    .into_values()
                    .map(|(value, count)| json!({"value": value, "count": count}))
                    .collect::<Vec<_>>(),
                "distinct_count": counts_len(rows, field),
                "truncated": false,
            })
        })
        .collect()
}

fn counts_len(rows: &[AgentIndexRow], field: &str) -> usize {
    rows.iter()
        .flat_map(|row| values_for_field(row, field))
        .map(|value| value.to_json().to_string())
        .collect::<BTreeSet<_>>()
        .len()
}

fn index_value_json(value: &crate::AgentIndexValue) -> Value {
    json!({"field": value.field.id, "value": value.value.to_json()})
}

fn row_matches(row: &AgentIndexRow, predicate: &Predicate) -> bool {
    let values = values_for_field(row, &predicate.field);
    match predicate.op {
        AgentPredicateOp::Exists => !values.is_empty(),
        AgentPredicateOp::NotExists => values.is_empty(),
        _ => {
            let Some(expected) = predicate.value.as_ref() else {
                return false;
            };
            values
                .iter()
                .any(|actual| predicate_value_matches(actual, &predicate.op, expected))
        }
    }
}

fn predicate_value_matches(
    actual: &AgentPredicateValue,
    op: &AgentPredicateOp,
    expected: &AgentPredicateValue,
) -> bool {
    match op {
        AgentPredicateOp::Eq => value_eq(actual, expected),
        AgentPredicateOp::NotEqual => !value_eq(actual, expected),
        AgentPredicateOp::In => match expected {
            AgentPredicateValue::List(values) => values.iter().any(|value| value_eq(actual, value)),
            _ => false,
        },
        AgentPredicateOp::Prefix => string_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual.starts_with(expected)),
        AgentPredicateOp::Suffix => string_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual.ends_with(expected)),
        AgentPredicateOp::Contains => string_pair(actual, expected)
            .is_some_and(|(actual, expected)| actual.contains(expected)),
        AgentPredicateOp::GreaterThan => numeric_pair(actual, expected).is_some_and(|(a, b)| a > b),
        AgentPredicateOp::GreaterThanOrEqual => {
            numeric_pair(actual, expected).is_some_and(|(a, b)| a >= b)
        }
        AgentPredicateOp::LessThan => numeric_pair(actual, expected).is_some_and(|(a, b)| a < b),
        AgentPredicateOp::LessThanOrEqual => {
            numeric_pair(actual, expected).is_some_and(|(a, b)| a <= b)
        }
        AgentPredicateOp::Exists | AgentPredicateOp::NotExists => false,
    }
}

fn value_eq(left: &AgentPredicateValue, right: &AgentPredicateValue) -> bool {
    if let Some((left, right)) = numeric_pair(left, right) {
        return (left - right).abs() < f64::EPSILON;
    }
    left == right
}

fn numeric_pair(left: &AgentPredicateValue, right: &AgentPredicateValue) -> Option<(f64, f64)> {
    Some((left.as_f64()?, right.as_f64()?))
}

fn string_pair<'a>(
    left: &'a AgentPredicateValue,
    right: &'a AgentPredicateValue,
) -> Option<(&'a str, &'a str)> {
    match (left, right) {
        (AgentPredicateValue::String(left), AgentPredicateValue::String(right)) => {
            Some((left, right))
        }
        _ => None,
    }
}

fn values_for_field(row: &AgentIndexRow, field: &str) -> Vec<AgentPredicateValue> {
    match field {
        "path" => return vec![AgentPredicateValue::String(row.path.clone())],
        "name" => {
            return path_name(&row.path)
                .map(AgentPredicateValue::String)
                .into_iter()
                .collect()
        }
        "kind" => return vec![AgentPredicateValue::String("file".to_owned())],
        _ => {}
    }
    row.values
        .iter()
        .filter(|value| value.field.id == field)
        .map(|value| value.value.clone())
        .collect()
}

fn sort_rows(rows: &mut [AgentIndexRow], sort: &[Sort]) {
    rows.sort_by(|left, right| {
        for sort in sort {
            let ordering = compare_field(left, right, &sort.field);
            if ordering != Ordering::Equal {
                return if sort.desc {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        left.path.cmp(&right.path)
    });
}

fn compare_field(left: &AgentIndexRow, right: &AgentIndexRow, field: &str) -> Ordering {
    let left = values_for_field(left, field).into_iter().next();
    let right = values_for_field(right, field).into_iter().next();
    match (left, right) {
        (Some(left), Some(right)) => compare_value(&left, &right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn compare_value(left: &AgentPredicateValue, right: &AgentPredicateValue) -> Ordering {
    if let Some((left, right)) = numeric_pair(left, right) {
        return left.partial_cmp(&right).unwrap_or(Ordering::Equal);
    }
    match (left.as_sort_string(), right.as_sort_string()) {
        (Some(left), Some(right)) => left.cmp(right),
        _ => left.to_json().to_string().cmp(&right.to_json().to_string()),
    }
}

fn aggregate_groups(
    rows: &[AgentIndexRow],
    group_by: &[String],
    measures: &[Measure],
) -> AgentIndexResult<Vec<Value>> {
    let mut groups =
        BTreeMap::<String, (Vec<(String, AgentPredicateValue)>, Vec<&AgentIndexRow>)>::new();
    for row in rows {
        let key_values = group_by
            .iter()
            .map(|field| {
                let value = values_for_field(row, field)
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| AgentPredicateValue::String(String::new()));
                (field.clone(), value)
            })
            .collect::<Vec<_>>();
        let key = key_values
            .iter()
            .map(|(field, value)| format!("{field}={}", value.to_json()))
            .collect::<Vec<_>>()
            .join("\u{1f}");
        groups
            .entry(key)
            .or_insert((key_values, Vec::new()))
            .1
            .push(row);
    }
    if group_by.is_empty() && groups.is_empty() {
        groups.insert(String::new(), (Vec::new(), Vec::new()));
    }
    groups
        .into_values()
        .map(|(key_values, rows)| {
            let key = key_values
                .into_iter()
                .map(|(field, value)| (field, value.to_json()))
                .collect::<Map<_, _>>();
            let values = measures
                .iter()
                .map(|measure| Ok((measure.name.clone(), aggregate_measure(&rows, measure)?)))
                .collect::<AgentIndexResult<Map<_, _>>>()?;
            Ok(json!({"key": key, "values": values}))
        })
        .collect()
}

fn aggregate_measure(rows: &[&AgentIndexRow], measure: &Measure) -> AgentIndexResult<Value> {
    if measure.op == "count" {
        return Ok(json!(rows.len()));
    }
    let field = measure.field.as_deref().ok_or_else(|| {
        AgentIndexError::InvalidArgument(format!("measure {} requires field", measure.name))
    })?;
    let values = rows
        .iter()
        .flat_map(|row| values_for_field(row, field))
        .filter_map(|value| value.as_f64())
        .collect::<Vec<_>>();
    if values.is_empty() {
        return Ok(Value::Null);
    }
    let out = match measure.op.as_str() {
        "sum" => values.iter().sum::<f64>(),
        "avg" => values.iter().sum::<f64>() / values.len() as f64,
        "min" => values.iter().copied().reduce(f64::min).unwrap_or(f64::NAN),
        "max" => values.iter().copied().reduce(f64::max).unwrap_or(f64::NAN),
        other => {
            return Err(AgentIndexError::InvalidArgument(format!(
                "unsupported aggregate op {other}"
            )))
        }
    };
    Ok(serde_json::Number::from_f64(out)
        .map(Value::Number)
        .unwrap_or(Value::Null))
}

fn sort_groups(groups: &mut [Value], sort: &[Sort]) {
    groups.sort_by(|left, right| {
        for sort in sort {
            let left_value = aggregate_sort_value(left, &sort.field);
            let right_value = aggregate_sort_value(right, &sort.field);
            let ordering = json_compare(left_value, right_value);
            if ordering != Ordering::Equal {
                return if sort.desc {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }
        Ordering::Equal
    });
}

fn aggregate_sort_value<'a>(group: &'a Value, field: &str) -> Option<&'a Value> {
    group
        .get("values")
        .and_then(|values| values.get(field))
        .or_else(|| group.get("key").and_then(|key| key.get(field)))
}

fn json_compare(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (Some(Value::Number(left)), Some(Value::Number(right))) => {
            match (left.as_f64(), right.as_f64()) {
                (Some(left), Some(right)) => left.partial_cmp(&right).unwrap_or(Ordering::Equal),
                _ => Ordering::Equal,
            }
        }
        (Some(Value::String(left)), Some(Value::String(right))) => left.cmp(right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (Some(left), Some(right)) => left.to_string().cmp(&right.to_string()),
        (None, None) => Ordering::Equal,
    }
}

fn structured_records(path: &str, bytes: &[u8]) -> Vec<Value> {
    let text = String::from_utf8_lossy(bytes);
    if path.ends_with(".jsonl") {
        return text
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect();
    }
    if path.ends_with(".json") {
        return match serde_json::from_slice::<Value>(bytes) {
            Ok(Value::Array(values)) => values,
            Ok(value) => vec![value],
            Err(_) => vec![Value::String(text.to_string())],
        };
    }
    vec![Value::String(text.to_string())]
}

fn predicates_arg(args: &Value) -> AgentIndexResult<Vec<Predicate>> {
    let Some(value) = object_args(args)?.get("predicates") else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .ok_or_else(|| AgentIndexError::InvalidArgument("predicates must be an array".to_owned()))?
        .iter()
        .map(predicate_arg)
        .collect()
}

fn predicate_arg(value: &Value) -> AgentIndexResult<Predicate> {
    let object = value.as_object().ok_or_else(|| {
        AgentIndexError::InvalidArgument("predicate must be an object".to_owned())
    })?;
    let field = string_property(object, "field")?.to_owned();
    let op = predicate_op_arg(string_property(object, "op")?)?;
    let raw_value = object.get("value").filter(|value| !value.is_null());
    let value = match op {
        AgentPredicateOp::Exists | AgentPredicateOp::NotExists => None,
        AgentPredicateOp::In => {
            let value = raw_value.ok_or_else(|| {
                AgentIndexError::InvalidArgument("predicate op in requires array value".to_owned())
            })?;
            Some(AgentPredicateValue::from_json(value).ok_or_else(|| {
                AgentIndexError::InvalidArgument("unsupported predicate value".to_owned())
            })?)
        }
        _ => {
            let value = raw_value.ok_or_else(|| {
                AgentIndexError::InvalidArgument(format!(
                    "predicate op {} requires value",
                    predicate_op_name(&op)
                ))
            })?;
            Some(AgentPredicateValue::from_json(value).ok_or_else(|| {
                AgentIndexError::InvalidArgument("unsupported predicate value".to_owned())
            })?)
        }
    };
    Ok(Predicate { field, op, value })
}

fn predicate_op_arg(op: &str) -> AgentIndexResult<AgentPredicateOp> {
    match op {
        "eq" => Ok(AgentPredicateOp::Eq),
        "ne" => Ok(AgentPredicateOp::NotEqual),
        "in" => Ok(AgentPredicateOp::In),
        "prefix" => Ok(AgentPredicateOp::Prefix),
        "suffix" => Ok(AgentPredicateOp::Suffix),
        "contains" => Ok(AgentPredicateOp::Contains),
        "gt" => Ok(AgentPredicateOp::GreaterThan),
        "gte" => Ok(AgentPredicateOp::GreaterThanOrEqual),
        "lt" => Ok(AgentPredicateOp::LessThan),
        "lte" => Ok(AgentPredicateOp::LessThanOrEqual),
        "exists" => Ok(AgentPredicateOp::Exists),
        "not_exists" => Ok(AgentPredicateOp::NotExists),
        other => Err(AgentIndexError::InvalidArgument(format!(
            "unsupported predicate op {other}"
        ))),
    }
}

fn sort_arg(args: &Value) -> AgentIndexResult<Vec<Sort>> {
    let Some(value) = object_args(args)?.get("sort") else {
        return Ok(Vec::new());
    };
    value
        .as_array()
        .ok_or_else(|| AgentIndexError::InvalidArgument("sort must be an array".to_owned()))?
        .iter()
        .map(sort_item_arg)
        .collect()
}

fn sort_item_arg(value: &Value) -> AgentIndexResult<Sort> {
    let object = value.as_object().ok_or_else(|| {
        AgentIndexError::InvalidArgument("sort item must be an object".to_owned())
    })?;
    Ok(Sort {
        field: string_property(object, "field")?.to_owned(),
        desc: object
            .get("direction")
            .and_then(Value::as_str)
            .is_some_and(|direction| direction == "desc"),
    })
}

fn aggregate_sort_arg(args: &Value) -> AgentIndexResult<Vec<Sort>> {
    sort_arg(args)
}

fn fields_arg(args: &Value) -> AgentIndexResult<Option<Vec<String>>> {
    string_array_arg(args, "fields")
}

fn facets_arg(args: &Value) -> AgentIndexResult<Vec<String>> {
    Ok(string_array_arg(args, "facets")?.unwrap_or_default())
}

fn measures_arg(args: &Value) -> AgentIndexResult<Vec<Measure>> {
    let value = object_args(args)?.get("measures").ok_or_else(|| {
        AgentIndexError::InvalidArgument("missing array argument measures".to_owned())
    })?;
    let measures = value
        .as_array()
        .ok_or_else(|| AgentIndexError::InvalidArgument("measures must be an array".to_owned()))?;
    if measures.is_empty() {
        return Err(AgentIndexError::InvalidArgument(
            "measures must contain at least one measure".to_owned(),
        ));
    }
    measures
        .iter()
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                AgentIndexError::InvalidArgument("measure must be an object".to_owned())
            })?;
            Ok(Measure {
                name: string_property(object, "name")?.to_owned(),
                op: string_property(object, "op")?.to_owned(),
                field: object
                    .get("field")
                    .and_then(|value| (!value.is_null()).then_some(value))
                    .map(|value| {
                        value.as_str().map(str::to_owned).ok_or_else(|| {
                            AgentIndexError::InvalidArgument(
                                "measure field must be a string or null".to_owned(),
                            )
                        })
                    })
                    .transpose()?,
            })
        })
        .collect()
}

fn string_array_arg(args: &Value, name: &'static str) -> AgentIndexResult<Option<Vec<String>>> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_array()
        .ok_or_else(|| AgentIndexError::InvalidArgument(format!("{name} must be an array")))?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                AgentIndexError::InvalidArgument(format!("{name} entries must be strings"))
            })
        })
        .collect::<AgentIndexResult<Vec<_>>>()
        .map(Some)
}

fn object_args(args: &Value) -> AgentIndexResult<&Map<String, Value>> {
    args.as_object().ok_or_else(|| {
        AgentIndexError::InvalidArgument("agent tool arguments must be a JSON object".to_owned())
    })
}

fn required_string_arg<'a>(args: &'a Value, name: &'static str) -> AgentIndexResult<&'a str> {
    object_args(args)?
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| AgentIndexError::InvalidArgument(format!("missing string argument {name}")))
}

fn optional_string_arg(args: &Value, name: &'static str) -> AgentIndexResult<Option<String>> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_owned()))
        .ok_or_else(|| AgentIndexError::InvalidArgument(format!("{name} must be a string or null")))
}

fn optional_bool_arg(args: &Value, name: &'static str) -> AgentIndexResult<Option<bool>> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| AgentIndexError::InvalidArgument(format!("{name} must be boolean or null")))
}

fn optional_u64_arg(args: &Value, name: &'static str) -> AgentIndexResult<Option<u64>> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_u64().map(Some).ok_or_else(|| {
        AgentIndexError::InvalidArgument(format!("{name} must be a non-negative integer"))
    })
}

fn optional_usize_arg(
    args: &Value,
    name: &'static str,
    max: usize,
) -> AgentIndexResult<Option<usize>> {
    optional_u64_arg(args, name)?
        .map(|value| {
            let value = usize::try_from(value)
                .map_err(|_| AgentIndexError::InvalidArgument(format!("{name} is too large")))?;
            if value == 0 || value > max {
                return Err(AgentIndexError::InvalidArgument(format!(
                    "{name} must be between 1 and {max}"
                )));
            }
            Ok(value)
        })
        .transpose()
}

fn cursor_offset(args: &Value) -> AgentIndexResult<usize> {
    optional_string_arg(args, "cursor")?
        .map(|cursor| {
            cursor
                .parse::<usize>()
                .map_err(|_| AgentIndexError::InvalidArgument(format!("invalid cursor {cursor}")))
        })
        .transpose()
        .map(|value| value.unwrap_or(0))
}

fn string_property<'a>(
    object: &'a Map<String, Value>,
    name: &'static str,
) -> AgentIndexResult<&'a str> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| AgentIndexError::InvalidArgument(format!("missing string property {name}")))
}

fn node_kind_name(kind: &AgentNodeKind) -> &'static str {
    match kind {
        AgentNodeKind::Directory => "directory",
        AgentNodeKind::File => "file",
    }
}

fn predicate_op_name(op: &AgentPredicateOp) -> &'static str {
    match op {
        AgentPredicateOp::Eq => "eq",
        AgentPredicateOp::NotEqual => "ne",
        AgentPredicateOp::In => "in",
        AgentPredicateOp::Prefix => "prefix",
        AgentPredicateOp::Suffix => "suffix",
        AgentPredicateOp::Contains => "contains",
        AgentPredicateOp::GreaterThan => "gt",
        AgentPredicateOp::GreaterThanOrEqual => "gte",
        AgentPredicateOp::LessThan => "lt",
        AgentPredicateOp::LessThanOrEqual => "lte",
        AgentPredicateOp::Exists => "exists",
        AgentPredicateOp::NotExists => "not_exists",
    }
}

fn hex_encode(raw: &[u8]) -> String {
    let mut out = String::with_capacity(raw.len() * 2);
    for byte in raw {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn hex_cursor(raw: String) -> Option<Vec<u8>> {
    if !raw.len().is_multiple_of(2) {
        return None;
    }
    raw.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AgentFindField, AgentId, AgentIndexRegistration, AgentIndexValue, HoltAgentStore};

    fn sample_agent_fs() -> AgentFs<HoltAgentStore> {
        let fs = AgentFs::new(
            AgentId::new("unit-agent"),
            HoltAgentStore::open_memory().unwrap(),
        );
        fs.bootstrap().unwrap();
        fs.put_file(
            "/runs/run-1/stdout.txt",
            b"first\nneedle hit\n".to_vec(),
            Some("text/plain".to_owned()),
        )
        .unwrap();
        fs.put_file(
            "/runs/run-2/stdout.txt",
            b"other\n".to_vec(),
            Some("text/plain".to_owned()),
        )
        .unwrap();
        fs.register_index(AgentIndexRegistration {
            path: "/runs".to_owned(),
            fields: vec![
                AgentIndexField {
                    field: AgentFindField::new("run.status"),
                    operators: vec![
                        AgentPredicateOp::Eq,
                        AgentPredicateOp::In,
                        AgentPredicateOp::Exists,
                    ],
                    sortable: true,
                    facetable: true,
                },
                AgentIndexField {
                    field: AgentFindField::new("metric.loss"),
                    operators: vec![
                        AgentPredicateOp::GreaterThan,
                        AgentPredicateOp::LessThan,
                        AgentPredicateOp::Exists,
                    ],
                    sortable: true,
                    facetable: false,
                },
            ],
            rows: vec![
                AgentIndexRow {
                    path: "/runs/run-1".to_owned(),
                    values: vec![
                        index_value("run.status", AgentPredicateValue::String("done".to_owned())),
                        index_value("metric.loss", AgentPredicateValue::F64(0.2)),
                    ],
                },
                AgentIndexRow {
                    path: "/runs/run-2".to_owned(),
                    values: vec![
                        index_value(
                            "run.status",
                            AgentPredicateValue::String("failed".to_owned()),
                        ),
                        index_value("metric.loss", AgentPredicateValue::F64(0.7)),
                    ],
                },
            ],
        })
        .unwrap();
        fs
    }

    fn index_value(field: &str, value: AgentPredicateValue) -> AgentIndexValue {
        AgentIndexValue {
            field: AgentFindField::new(field),
            value,
        }
    }

    #[test]
    fn find_filters_sorts_and_projects_index_rows() {
        let fs = sample_agent_fs();

        let result = execute_agent_tool(
            &fs,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "metric.loss", "op": "lt", "value": 0.5}],
                "fields": ["run.status", "metric.loss"],
                "sort": [{"field": "metric.loss", "direction": "asc"}],
                "limit": 10
            }),
        )
        .unwrap();

        assert_eq!(result["match_count"], 1);
        assert_eq!(result["matches"][0]["path"], "/runs/run-1");
        assert_eq!(result["matches"][0]["values"]["run.status"], "done");
        assert_eq!(result["matches"][0]["values"]["metric.loss"], 0.2);
    }

    #[test]
    fn aggregate_groups_by_index_field() {
        let fs = sample_agent_fs();

        let result = execute_agent_tool(
            &fs,
            "aggregate",
            &json!({
                "path": "/runs",
                "group_by": ["run.status"],
                "measures": [{"name": "rows", "op": "count"}],
                "sort": [{"field": "run.status", "direction": "asc"}],
                "limit": 10
            }),
        )
        .unwrap();

        assert_eq!(result["input_match_count"], 2);
        assert_eq!(result["group_count"], 2);
        assert_eq!(result["groups"][0]["key"]["run.status"], "done");
        assert_eq!(result["groups"][0]["values"]["rows"], 1);
    }

    #[test]
    fn grep_reads_agent_native_file_bodies() {
        let fs = sample_agent_fs();

        let result = execute_agent_tool(
            &fs,
            "grep",
            &json!({
                "path": "/runs",
                "pattern": "needle",
                "recursive": true,
                "limit": 10
            }),
        )
        .unwrap();

        assert_eq!(result["matches"][0]["path"], "/runs/run-1/stdout.txt");
        assert_eq!(result["matches"][0]["line_number"], 2);
        assert_eq!(result["matches"][0]["snippet"], "needle hit");
    }
}
