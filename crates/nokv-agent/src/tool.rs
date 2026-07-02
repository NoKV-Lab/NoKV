use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

use crate::fs::{normalize_path, parent_path, path_name};
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

pub use crate::AgentToolDefinition;

pub fn agent_tool_definitions() -> Vec<AgentToolDefinition> {
    // Shared with the crate-root DFS dispatcher so both backends present
    // byte-identical tool names, descriptions, and parameter schemas.
    crate::agent_tool_definitions()
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
    let cursor = optional_string_arg(args, "cursor")?
        .map(|raw| {
            hex_cursor(&raw)
                .ok_or_else(|| AgentIndexError::InvalidArgument(format!("invalid cursor: {raw}")))
        })
        .transpose()?;
    let entry_count = fs.list(path, None, usize::MAX)?.0.len();
    let (entries, next_cursor, truncated) = fs.list(path, cursor.as_deref(), limit)?;
    let entries = entries
        .iter()
        .map(|node| list_entry_json(fs, node))
        .collect::<AgentIndexResult<Vec<_>>>()?;
    Ok(json!({
        "path": normalize_path(path)?,
        "entry_count": entry_count,
        "entries": entries,
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
    let row = indexed_row_for_path(fs, path)?;
    Ok(json!({
        "card": card_json(&node, &fs.catalog(path).unwrap_or_default(), row.as_ref()),
    }))
}

/// Find the index row registered for exactly `path`, searching every
/// registration root along the ancestor chain (rows live under the root
/// they were registered with, not under their own path).
fn indexed_row_for_path<S>(fs: &AgentFs<S>, path: &str) -> AgentIndexResult<Option<AgentIndexRow>>
where
    S: AgentStore,
{
    let normalized = normalize_path(path)?;
    let mut ancestor = normalized.clone();
    loop {
        if let Some(row) = fs
            .index_rows(&ancestor)?
            .into_iter()
            .find(|row| row.path == normalized)
        {
            return Ok(Some(row));
        }
        if ancestor == "/" {
            return Ok(None);
        }
        ancestor = parent_path(&ancestor)?;
    }
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
    let child_catalogs = if catalog_empty
        && fs
            .node(path)?
            .is_some_and(|node| node.kind == AgentNodeKind::Directory)
    {
        child_catalogs_json(fs, path, include_facets)?
    } else {
        Vec::new()
    };
    Ok(json!({
        "path": normalize_path(path)?,
        "catalog_empty": catalog_empty,
        "catalog": catalog,
        "child_catalogs": child_catalogs,
    }))
}

fn child_catalogs_json<S>(
    fs: &AgentFs<S>,
    path: &str,
    include_facets: bool,
) -> AgentIndexResult<Vec<Value>>
where
    S: AgentStore,
{
    let (entries, _, _) = fs.list(path, None, 20)?;
    let mut children = Vec::new();
    for entry in entries {
        if entry.kind != AgentNodeKind::Directory {
            continue;
        }
        let fields = fs.catalog(&entry.path)?;
        if fields.is_empty() {
            continue;
        }
        let rows = if include_facets {
            fs.index_rows(&entry.path)?
        } else {
            Vec::new()
        };
        children.push(json!({
            "path": entry.path,
            "catalog": catalog_json(&fields, &rows, None, include_facets),
        }));
        if children.len() == 5 {
            break;
        }
    }
    Ok(children)
}

fn execute_find<S>(fs: &AgentFs<S>, args: &Value) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let path = required_string_arg(args, "path")?;
    let fields = fields_arg(args)?;
    if object_args(args)?.contains_key("include") {
        return Err(AgentIndexError::InvalidArgument(
            "unsupported argument include; use stat for schema or sample and read for body content"
                .to_owned(),
        ));
    }
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
    let files = fs.files_under(path, recursive)?;
    for file in &files {
        let bytes = fs.read_file(&file.path)?;
        if bytes.contains(&0) {
            continue;
        }
        let text = String::from_utf8_lossy(&bytes);
        for (line_index, line) in text.lines().enumerate() {
            if line.to_lowercase().contains(&needle) {
                all_matches.push(json!({
                    "path": file.path,
                    "line_number": line_index + 1,
                    "snippet": line.chars().take(240).collect::<String>(),
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
    let cursor = optional_string_arg(args, "cursor")?;
    let normalized = normalize_path(path)?;
    let node = fs
        .node(&normalized)?
        .ok_or_else(|| AgentIndexError::NotFound(normalized.clone()))?;
    let bytes = fs.read_file(&normalized)?;
    if format == "bytes" {
        // The cursor, when provided, wins over offset; mirrors the DFS
        // parse_byte_cursor semantics so returned next_cursor values page.
        let start = match cursor.as_deref() {
            Some(raw) => raw.parse::<usize>().map_err(|err| {
                AgentIndexError::InvalidArgument(format!("invalid cursor: {err}"))
            })?,
            None => offset,
        };
        let end = start.saturating_add(limit).min(bytes.len());
        let range = bytes.get(start..end).unwrap_or_default().to_vec();
        return Ok(json!({
            "path": normalized,
            "total_size_bytes": bytes.len(),
            "format": "bytes",
            "record_type": null,
            "record_count": null,
            "cursor": cursor,
            "next_cursor": (end < bytes.len()).then(|| end.to_string()),
            "truncated": end < bytes.len(),
            "items": [],
            "bytes": range,
        }));
    }
    if format != "structured" {
        return Err(AgentIndexError::InvalidArgument(format!(
            "unsupported read format {format}; expected structured or bytes"
        )));
    }
    let start = match cursor.as_deref() {
        Some(raw) => raw
            .parse::<usize>()
            .map_err(|err| AgentIndexError::InvalidArgument(format!("invalid cursor: {err}")))?,
        None => 0,
    };
    let content_type = node
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream");
    let (record_type, records) = structured_records(&normalized, content_type, &bytes)?;
    let record_count = records.len();
    if record_count > MAX_PAGE_LIMIT {
        return Err(AgentIndexError::InvalidArgument(format!(
            "structured pagination for {path} has {record_count} records; use stat record_count or find with catalog predicates and limit=1, then read match_count"
        )));
    }
    let items = records
        .into_iter()
        .enumerate()
        .skip(start)
        .take(limit)
        .map(|(index, value)| json!({"index": index, "value": value}))
        .collect::<Vec<_>>();
    let next_offset = start.saturating_add(items.len());
    Ok(json!({
        "path": normalized,
        "total_size_bytes": bytes.len(),
        "format": "structured",
        "record_type": record_type,
        "record_count": record_count,
        "cursor": cursor,
        "next_cursor": (next_offset < record_count).then(|| next_offset.to_string()),
        "truncated": next_offset < record_count,
        "items": items,
        "bytes": null,
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

fn list_entry_json<S>(fs: &AgentFs<S>, node: &AgentNode) -> AgentIndexResult<Value>
where
    S: AgentStore,
{
    let entry_count = match node.kind {
        AgentNodeKind::Directory => Some(fs.list(&node.path, None, usize::MAX)?.0.len()),
        AgentNodeKind::File => None,
    };
    Ok(json!({
        "path": node.path,
        "name": node.name,
        "kind": node_kind_name(&node.kind),
        "size_bytes": node.size_bytes,
        "entry_count": entry_count,
    }))
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
    // Integers compare exactly; the f64 round-trip loses precision above
    // 2^53 and would report distinct u64 values as equal.
    if let (AgentPredicateValue::U64(left), AgentPredicateValue::U64(right)) = (left, right) {
        return left == right;
    }
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

fn structured_records(
    path: &str,
    content_type: &str,
    bytes: &[u8],
) -> AgentIndexResult<(&'static str, Vec<Value>)> {
    if is_json_path(path, content_type) {
        return json_records(bytes);
    }
    if is_yaml_path(path, content_type) {
        return yaml_records(bytes);
    }
    if is_text_path(path, content_type) {
        return text_records(bytes);
    }
    Err(AgentIndexError::InvalidArgument(format!(
        "structured read does not support content type {content_type} for {path}"
    )))
}

fn is_json_path(path: &str, content_type: &str) -> bool {
    content_type == "application/json" || path.ends_with(".json")
}

fn is_yaml_path(path: &str, content_type: &str) -> bool {
    matches!(
        content_type,
        "application/yaml" | "application/x-yaml" | "text/yaml"
    ) || path.ends_with(".yaml")
        || path.ends_with(".yml")
}

fn is_text_path(path: &str, content_type: &str) -> bool {
    content_type.starts_with("text/") || path.ends_with(".txt") || path.ends_with(".log")
}

fn json_records(bytes: &[u8]) -> AgentIndexResult<(&'static str, Vec<Value>)> {
    let value = serde_json::from_slice::<Value>(bytes).map_err(|err| {
        AgentIndexError::InvalidArgument(format!("structured JSON parse failed: {err}"))
    })?;
    match value {
        Value::Array(items) => Ok(("json_array", items)),
        Value::Object(map) => {
            let mut entries = map.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let items = entries
                .into_iter()
                .map(|(key, value)| json!({"key": key, "value": value}))
                .collect();
            Ok(("json_object", items))
        }
        _ => Err(AgentIndexError::InvalidArgument(
            "structured JSON read supports arrays and objects".to_owned(),
        )),
    }
}

fn yaml_records(bytes: &[u8]) -> AgentIndexResult<(&'static str, Vec<Value>)> {
    let value = serde_yaml::from_slice::<serde_yaml::Value>(bytes).map_err(|err| {
        AgentIndexError::InvalidArgument(format!("structured YAML parse failed: {err}"))
    })?;
    let serde_yaml::Value::Mapping(map) = value else {
        return Err(AgentIndexError::InvalidArgument(
            "structured YAML read supports mappings".to_owned(),
        ));
    };
    let mut entries = Vec::new();
    for (key, value) in map {
        let Some(key) = key.as_str() else {
            continue;
        };
        entries.push((
            key.to_owned(),
            serde_json::to_value(value).unwrap_or(Value::Null),
        ));
    }
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    let items = entries
        .into_iter()
        .map(|(key, value)| json!({"key": key, "value": value}))
        .collect();
    Ok(("yaml_mapping", items))
}

fn text_records(bytes: &[u8]) -> AgentIndexResult<(&'static str, Vec<Value>)> {
    let text = std::str::from_utf8(bytes).map_err(|err| {
        AgentIndexError::InvalidArgument(format!("structured text parse failed: {err}"))
    })?;
    let items = text
        .lines()
        .enumerate()
        .map(|(index, line)| json!({"line": index + 1, "text": line}))
        .collect();
    Ok(("text_lines", items))
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
            if !value.is_array() {
                return Err(AgentIndexError::InvalidArgument(
                    "predicate op in requires array value".to_owned(),
                ));
            }
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
        "ne" | "not_equal" => Ok(AgentPredicateOp::NotEqual),
        "in" => Ok(AgentPredicateOp::In),
        "prefix" => Ok(AgentPredicateOp::Prefix),
        "suffix" => Ok(AgentPredicateOp::Suffix),
        "contains" => Ok(AgentPredicateOp::Contains),
        "gt" | "greater_than" => Ok(AgentPredicateOp::GreaterThan),
        "gte" | "greater_than_or_equal" => Ok(AgentPredicateOp::GreaterThanOrEqual),
        "lt" | "less_than" => Ok(AgentPredicateOp::LessThan),
        "lte" | "less_than_or_equal" => Ok(AgentPredicateOp::LessThanOrEqual),
        "exists" => Ok(AgentPredicateOp::Exists),
        "not_exists" => Ok(AgentPredicateOp::NotExists),
        other => Err(AgentIndexError::InvalidArgument(format!(
            "unsupported predicate operator {other}"
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
    let field = string_property(object, "field")?.to_owned();
    let direction = object
        .get("direction")
        .and_then(Value::as_str)
        .unwrap_or("asc");
    let desc = match direction {
        "asc" => false,
        "desc" => true,
        other => {
            return Err(AgentIndexError::InvalidArgument(format!(
                "unsupported sort direction {other}"
            )))
        }
    };
    Ok(Sort { field, desc })
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
            let name = string_property(object, "name")?.to_owned();
            if name.is_empty() {
                return Err(AgentIndexError::InvalidArgument(
                    "measure name must not be empty".to_owned(),
                ));
            }
            let op = string_property(object, "op")?.to_owned();
            if !matches!(op.as_str(), "count" | "sum" | "avg" | "min" | "max") {
                return Err(AgentIndexError::InvalidArgument(format!(
                    "unsupported aggregate op {op}"
                )));
            }
            let field = object
                .get("field")
                .and_then(|value| (!value.is_null()).then_some(value))
                .map(|value| {
                    value.as_str().map(str::to_owned).ok_or_else(|| {
                        AgentIndexError::InvalidArgument(
                            "measure field must be a string or null".to_owned(),
                        )
                    })
                })
                .transpose()?;
            if op != "count" && field.is_none() {
                return Err(AgentIndexError::InvalidArgument(format!(
                    "measure {name} with op {op} requires field"
                )));
            }
            Ok(Measure { name, op, field })
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
                .map_err(|err| AgentIndexError::InvalidArgument(format!("invalid cursor: {err}")))
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

fn hex_cursor(raw: &str) -> Option<Vec<u8>> {
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

    #[test]
    fn tool_definitions_match_root_dispatcher() {
        let root = crate::agent_tool_definitions();
        let native = agent_tool_definitions();
        assert_eq!(native, root);
    }

    #[test]
    fn read_bytes_pages_advance_with_cursor() {
        let fs = sample_agent_fs();
        fs.put_file("/runs/run-1/blob.bin", b"0123456789".to_vec(), None)
            .unwrap();

        let first = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/blob.bin", "format": "bytes", "limit": 4}),
        )
        .unwrap();
        assert_eq!(first["bytes"], json!(b"0123".to_vec()));
        assert_eq!(first["cursor"], Value::Null);
        assert_eq!(first["next_cursor"], "4");
        assert_eq!(first["truncated"], true);

        let second = execute_agent_tool(
            &fs,
            "read",
            &json!({
                "path": "/runs/run-1/blob.bin",
                "format": "bytes",
                "limit": 4,
                "offset": 0,
                "cursor": first["next_cursor"],
            }),
        )
        .unwrap();
        assert_eq!(second["bytes"], json!(b"4567".to_vec()));
        assert_eq!(second["cursor"], "4");
        assert_eq!(second["next_cursor"], "8");

        let via_offset = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/blob.bin", "format": "bytes", "limit": 4, "offset": 8}),
        )
        .unwrap();
        assert_eq!(via_offset["bytes"], json!(b"89".to_vec()));
        assert_eq!(via_offset["next_cursor"], Value::Null);
        assert_eq!(via_offset["truncated"], false);
    }

    #[test]
    fn read_rejects_malformed_cursor() {
        let fs = sample_agent_fs();

        let err = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/stdout.txt", "format": "bytes", "cursor": "zz"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("invalid cursor:"),
            "unexpected error: {err}"
        );

        let err = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/stdout.txt", "format": "structured", "cursor": "zz"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("invalid cursor:"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn structured_read_returns_sorted_json_object_records() {
        let fs = sample_agent_fs();
        fs.put_file(
            "/runs/run-1/metadata.json",
            b"{\"b\":2,\"a\":1}".to_vec(),
            Some("application/json".to_owned()),
        )
        .unwrap();

        let result = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/metadata.json", "format": "structured"}),
        )
        .unwrap();

        assert_eq!(result["record_type"], "json_object");
        assert_eq!(result["record_count"], 2);
        assert_eq!(result["cursor"], Value::Null);
        assert_eq!(result["items"][0]["value"], json!({"key": "a", "value": 1}));
        assert_eq!(result["items"][1]["value"], json!({"key": "b", "value": 2}));
        assert!(result.get("bytes_read").is_none());
    }

    #[test]
    fn structured_read_returns_json_array_records() {
        let fs = sample_agent_fs();
        fs.put_file(
            "/runs/run-1/rows.json",
            b"[{\"loss\":0.2},{\"loss\":0.7}]".to_vec(),
            Some("application/json".to_owned()),
        )
        .unwrap();

        let result = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/rows.json", "format": "structured"}),
        )
        .unwrap();

        assert_eq!(result["record_type"], "json_array");
        assert_eq!(result["record_count"], 2);
        assert_eq!(result["items"][0]["value"], json!({"loss": 0.2}));
    }

    #[test]
    fn structured_read_rejects_json_scalars_and_parse_failures() {
        let fs = sample_agent_fs();
        fs.put_file(
            "/runs/run-1/scalar.json",
            b"42".to_vec(),
            Some("application/json".to_owned()),
        )
        .unwrap();
        fs.put_file(
            "/runs/run-1/broken.json",
            b"{not json".to_vec(),
            Some("application/json".to_owned()),
        )
        .unwrap();

        let err = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/scalar.json", "format": "structured"}),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("structured JSON read supports arrays and objects"),
            "unexpected error: {err}"
        );

        let err = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/broken.json", "format": "structured"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("structured JSON parse failed"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn structured_read_returns_text_lines_for_text_files() {
        let fs = sample_agent_fs();
        fs.put_file("/runs/run-1/tail.log", b"alpha\nbeta\n".to_vec(), None)
            .unwrap();
        // .jsonl is not a structured suffix; it reads as text only when the
        // producer declares a text content type, matching the DFS dispatcher.
        fs.put_file(
            "/runs/run-1/events.jsonl",
            b"{\"a\":1}\n{\"b\":2}\n".to_vec(),
            Some("text/plain".to_owned()),
        )
        .unwrap();

        let log = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/tail.log", "format": "structured"}),
        )
        .unwrap();
        assert_eq!(log["record_type"], "text_lines");
        assert_eq!(log["record_count"], 2);
        assert_eq!(
            log["items"][0]["value"],
            json!({"line": 1, "text": "alpha"})
        );
        assert_eq!(log["items"][1]["value"], json!({"line": 2, "text": "beta"}));

        let jsonl = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/events.jsonl", "format": "structured"}),
        )
        .unwrap();
        assert_eq!(jsonl["record_type"], "text_lines");
        assert_eq!(
            jsonl["items"][0]["value"],
            json!({"line": 1, "text": "{\"a\":1}"})
        );
    }

    #[test]
    fn structured_read_returns_yaml_mapping_records() {
        let fs = sample_agent_fs();
        fs.put_file(
            "/runs/run-1/config.yaml",
            b"beta: 2\nalpha: 1\n".to_vec(),
            None,
        )
        .unwrap();

        let result = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/config.yaml", "format": "structured"}),
        )
        .unwrap();

        assert_eq!(result["record_type"], "yaml_mapping");
        assert_eq!(
            result["items"][0]["value"],
            json!({"key": "alpha", "value": 1})
        );
        assert_eq!(
            result["items"][1]["value"],
            json!({"key": "beta", "value": 2})
        );
    }

    #[test]
    fn structured_read_rejects_unsupported_content_types() {
        let fs = sample_agent_fs();
        fs.put_file("/runs/run-1/blob.bin", vec![1, 2, 3], None)
            .unwrap();

        let err = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/blob.bin", "format": "structured"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "structured read does not support content type application/octet-stream for /runs/run-1/blob.bin"
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn structured_read_guards_large_record_sets() {
        let fs = sample_agent_fs();
        let body = (0..150).map(|i| format!("line-{i}\n")).collect::<String>();
        fs.put_file(
            "/runs/run-1/big.log",
            body.into_bytes(),
            Some("text/plain".to_owned()),
        )
        .unwrap();

        let err = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/big.log", "format": "structured"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "structured pagination for /runs/run-1/big.log has 150 records; use stat record_count or find with catalog predicates and limit=1, then read match_count"
            ),
            "unexpected error: {err}"
        );

        let bytes = execute_agent_tool(
            &fs,
            "read",
            &json!({"path": "/runs/run-1/big.log", "format": "bytes", "limit": 8}),
        )
        .unwrap();
        assert_eq!(bytes["truncated"], true);
    }

    #[test]
    fn ls_reports_full_entry_counts_and_pages() {
        let fs = sample_agent_fs();
        fs.put_file("/runs/run-3/stdout.txt", b"x\n".to_vec(), None)
            .unwrap();

        let first = execute_agent_tool(&fs, "ls", &json!({"path": "/runs", "limit": 2})).unwrap();
        assert_eq!(first["entry_count"], 3);
        assert_eq!(first["entries"].as_array().unwrap().len(), 2);
        assert_eq!(first["truncated"], true);
        assert_eq!(first["entries"][0]["kind"], "directory");
        assert_eq!(first["entries"][0]["entry_count"], 1);

        let cursor = first["next_cursor"].as_str().unwrap().to_owned();
        let second = execute_agent_tool(
            &fs,
            "ls",
            &json!({"path": "/runs", "cursor": cursor, "limit": 2}),
        )
        .unwrap();
        assert_eq!(second["entry_count"], 3);
        assert_eq!(second["entries"].as_array().unwrap().len(), 1);
        assert_eq!(second["truncated"], false);

        let leaf = execute_agent_tool(&fs, "ls", &json!({"path": "/runs/run-1"})).unwrap();
        assert_eq!(leaf["entries"][0]["kind"], "file");
        assert_eq!(leaf["entries"][0]["entry_count"], Value::Null);
    }

    #[test]
    fn ls_rejects_malformed_cursor() {
        let fs = sample_agent_fs();

        let err =
            execute_agent_tool(&fs, "ls", &json!({"path": "/runs", "cursor": "zz"})).unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("invalid cursor: zz".to_owned())
        );
    }

    #[test]
    fn stat_resolves_indexed_values_along_ancestor_chain() {
        let fs = sample_agent_fs();

        let root = execute_agent_tool(&fs, "stat", &json!({"path": "/runs"})).unwrap();
        assert_eq!(root["card"]["indexed_values"], json!([]));

        let child = execute_agent_tool(&fs, "stat", &json!({"path": "/runs/run-1"})).unwrap();
        let values = child["card"]["indexed_values"].as_array().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["field"], "run.status");
        assert_eq!(values[0]["value"], "done");
        assert_eq!(values[1]["field"], "metric.loss");
    }

    #[test]
    fn grep_skips_binary_files_and_truncates_snippets() {
        let fs = sample_agent_fs();
        let mut binary = b"needle".to_vec();
        binary.push(0);
        fs.put_file("/runs/run-1/binary.dat", binary, None).unwrap();
        let long_line = format!("needle {}", "x".repeat(500));
        fs.put_file("/runs/run-2/long.txt", long_line.into_bytes(), None)
            .unwrap();

        let result = execute_agent_tool(
            &fs,
            "grep",
            &json!({"path": "/runs", "pattern": "needle", "recursive": true}),
        )
        .unwrap();

        let matches = result["matches"].as_array().unwrap();
        assert!(
            matches
                .iter()
                .all(|match_| match_["path"] != "/runs/run-1/binary.dat"),
            "binary file must be skipped: {matches:?}"
        );
        let long_match = matches
            .iter()
            .find(|match_| match_["path"] == "/runs/run-2/long.txt")
            .unwrap();
        assert_eq!(long_match["snippet"].as_str().unwrap().chars().count(), 240);
        assert_eq!(result["files_scanned"], 4);
        assert!(result.get("bytes_read").is_none());
    }

    #[test]
    fn value_eq_compares_large_u64_exactly() {
        let fs = sample_agent_fs();
        fs.register_index(AgentIndexRegistration {
            path: "/counters".to_owned(),
            fields: vec![AgentIndexField {
                field: AgentFindField::new("seq"),
                operators: vec![AgentPredicateOp::Eq, AgentPredicateOp::In],
                sortable: false,
                facetable: false,
            }],
            rows: vec![
                AgentIndexRow {
                    path: "/counters/a".to_owned(),
                    values: vec![index_value(
                        "seq",
                        AgentPredicateValue::U64(9007199254740992),
                    )],
                },
                AgentIndexRow {
                    path: "/counters/b".to_owned(),
                    values: vec![index_value(
                        "seq",
                        AgentPredicateValue::U64(9007199254740993),
                    )],
                },
            ],
        })
        .unwrap();

        let result = execute_agent_tool(
            &fs,
            "find",
            &json!({
                "path": "/counters",
                "predicates": [{"field": "seq", "op": "eq", "value": 9007199254740993_u64}],
            }),
        )
        .unwrap();

        assert_eq!(result["match_count"], 1);
        assert_eq!(result["matches"][0]["path"], "/counters/b");
    }

    #[test]
    fn in_predicate_rejects_scalar_values() {
        let fs = sample_agent_fs();

        let err = execute_agent_tool(
            &fs,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "in", "value": "done"}],
            }),
        )
        .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("predicate op in requires array value".to_owned())
        );
    }

    #[test]
    fn predicate_op_aliases_match_root_dispatcher() {
        let fs = sample_agent_fs();

        let result = execute_agent_tool(
            &fs,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "metric.loss", "op": "greater_than", "value": 0.5}],
            }),
        )
        .unwrap();
        assert_eq!(result["match_count"], 1);

        let err = execute_agent_tool(
            &fs,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "metric.loss", "op": "between", "value": 0.5}],
            }),
        )
        .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("unsupported predicate operator between".to_owned())
        );
    }

    #[test]
    fn sort_rejects_unsupported_direction() {
        let fs = sample_agent_fs();

        let err = execute_agent_tool(
            &fs,
            "find",
            &json!({
                "path": "/runs",
                "sort": [{"field": "metric.loss", "direction": "sideways"}],
            }),
        )
        .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("unsupported sort direction sideways".to_owned())
        );
    }

    #[test]
    fn find_rejects_include_argument() {
        let fs = sample_agent_fs();

        let err = execute_agent_tool(&fs, "find", &json!({"path": "/runs", "include": ["body"]}))
            .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument(
                "unsupported argument include; use stat for schema or sample and read for body content"
                    .to_owned()
            )
        );
    }

    #[test]
    fn aggregate_validates_measures_at_parse_time() {
        let fs = sample_agent_fs();

        let err = execute_agent_tool(
            &fs,
            "aggregate",
            &json!({
                "path": "/runs",
                "measures": [{"name": "m", "op": "median", "field": "metric.loss"}],
            }),
        )
        .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("unsupported aggregate op median".to_owned())
        );

        let err = execute_agent_tool(
            &fs,
            "aggregate",
            &json!({
                "path": "/runs",
                "measures": [{"name": "", "op": "count"}],
            }),
        )
        .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("measure name must not be empty".to_owned())
        );

        let err = execute_agent_tool(
            &fs,
            "aggregate",
            &json!({
                "path": "/runs",
                "measures": [{"name": "total", "op": "sum"}],
            }),
        )
        .unwrap_err();
        assert_eq!(
            err,
            AgentIndexError::InvalidArgument("measure total with op sum requires field".to_owned())
        );
    }

    #[test]
    fn catalog_surfaces_child_catalogs_for_uncatalogued_directories() {
        let fs = sample_agent_fs();

        let result = execute_agent_tool(&fs, "catalog", &json!({"path": "/"})).unwrap();

        assert_eq!(result["catalog_empty"], true);
        let children = result["child_catalogs"].as_array().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["path"], "/runs");
        assert_eq!(
            children[0]["catalog"]["sortable"],
            json!(["run.status", "metric.loss"])
        );

        let indexed = execute_agent_tool(&fs, "catalog", &json!({"path": "/runs"})).unwrap();
        assert_eq!(indexed["catalog_empty"], false);
        assert_eq!(indexed["child_catalogs"], json!([]));
    }

    #[test]
    fn invalid_argument_errors_use_neutral_prefix() {
        let fs = sample_agent_fs();

        let err = execute_agent_tool(&fs, "warp", &json!({})).unwrap_err();
        assert_eq!(
            err.to_string(),
            "invalid agent argument: unknown agent tool warp"
        );
    }
}
