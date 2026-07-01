//! NoKV agent tool surface.
//!
//! Transport-free agent tooling: the JSON tool definitions exposed to a model,
//! the dispatcher that maps a tool call onto a namespace verb, argument
//! validation, and result shaping. Depends only on `nokv-meta`, `nokv-object`
//! and `nokv-types` — never on `nokv-client`, `nokv-protocol` or `nokv-control`.

use nokv_meta::{
    MetadataStore, NamespaceAggregateGroup, NamespaceAggregateMeasure, NamespaceAggregateOp,
    NamespaceAggregateRequest, NamespaceAggregateResult, NamespaceAggregateSort,
    NamespaceAggregateValue, NamespaceBodyDescriptor, NamespaceCard, NamespaceCardKind,
    NamespaceFacetSummary, NamespaceFacetValue, NamespaceFilterCapability, NamespaceFindField,
    NamespaceFindRequest, NamespaceFindResult, NamespaceGrepRequest, NamespaceGrepResult,
    NamespaceIndexValue, NamespaceListOptions, NamespaceListPage, NamespacePredicate,
    NamespacePredicateOp, NamespacePredicateValue, NamespaceQueryCatalog, NamespaceReadFormat,
    NamespaceReadItem, NamespaceReadOptions, NamespaceReadPage, NamespaceRecordCount,
    NamespaceRecordType, NamespaceSchema, NamespaceSort, NamespaceSortDirection,
    NamespaceSortField, NoKvFs,
};
use nokv_object::ObjectStore;
use serde_json::{json, Map, Value};

/// Error surface for the agent tool layer.
///
/// The copied dispatcher only ever constructs `Metadata`, `NotFound`, and
/// `InvalidArgument`. `Other` is unused here; it exists so a later step can
/// add `From<ClientError>` in `nokv-client`.
#[derive(Debug)]
pub enum AgentError {
    Metadata(nokv_meta::MetadError),
    NotFound(String),
    InvalidArgument(String),
    Other(String),
}

impl std::fmt::Display for AgentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Display strings MUST stay byte-identical to the old ClientError rendering
            // (crates/nokv-client/src/lib.rs:93-119) so model/telemetry output is unchanged:
            Self::Metadata(err) => write!(f, "metadata service error: {err}"),
            Self::NotFound(path) => write!(f, "path component not found: {path}"),
            Self::InvalidArgument(err) => write!(f, "metadata protocol error: {err}"),
            Self::Other(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for AgentError {}

impl From<nokv_meta::MetadError> for AgentError {
    fn from(err: nokv_meta::MetadError) -> Self {
        Self::Metadata(err)
    }
}

const DEFAULT_AGENT_PAGE_LIMIT: usize = 100;
const MAX_AGENT_PAGE_LIMIT: usize = 100;
const DEFAULT_AGENT_FIND_LIMIT: usize = 10;
const MAX_AGENT_FIND_LIMIT: usize = 10;
const DEFAULT_AGENT_AGGREGATE_LIMIT: usize = 20;
const MAX_AGENT_AGGREGATE_LIMIT: usize = 100;
const DEFAULT_AGENT_GREP_LIMIT: usize = 100;
const MAX_AGENT_GREP_LIMIT: usize = 100;

#[derive(Clone, Debug, PartialEq)]
pub struct AgentToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub parameters: Value,
}

pub trait AgentNamespace {
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, AgentError>;

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, AgentError>;

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, AgentError>;

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, AgentError>;

    fn agent_grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, AgentError>;

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, AgentError>;
}

impl<M, O> AgentNamespace for NoKvFs<M, O>
where
    M: MetadataStore,
    O: ObjectStore,
{
    fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, AgentError> {
        self.stat_card(path).map_err(AgentError::Metadata)
    }

    fn agent_list_page(
        &self,
        path: &str,
        options: NamespaceListOptions,
    ) -> Result<NamespaceListPage, AgentError> {
        self.list_page(path, options).map_err(AgentError::Metadata)
    }

    fn agent_find_paths(
        &self,
        request: NamespaceFindRequest,
    ) -> Result<NamespaceFindResult, AgentError> {
        self.find_paths(request).map_err(AgentError::Metadata)
    }

    fn agent_aggregate_paths(
        &self,
        request: NamespaceAggregateRequest,
    ) -> Result<NamespaceAggregateResult, AgentError> {
        self.aggregate_paths(request).map_err(AgentError::Metadata)
    }

    fn agent_grep_paths(
        &self,
        request: NamespaceGrepRequest,
    ) -> Result<NamespaceGrepResult, AgentError> {
        self.grep_paths(request).map_err(AgentError::Metadata)
    }

    fn agent_read_page(
        &self,
        path: &str,
        options: NamespaceReadOptions,
    ) -> Result<NamespaceReadPage, AgentError> {
        self.read_page(path, options).map_err(AgentError::Metadata)
    }
}

pub fn agent_tool_definitions() -> Vec<AgentToolDefinition> {
    vec![
        AgentToolDefinition {
            name: "ls",
            description: "List direct children to discover paths. Not recursive; use stat for one path details.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_PAGE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "stat",
            description: "Inspect a single path compact card: counts, body, schema, sample, catalog, and indexed values. Use read for file body content.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "catalog",
            description: "Discover field ids for find predicates, projections, sort, facets, and aggregate group or measure fields.",
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
            description: "Read file body content. Structured mode returns JSON, YAML, or text records; bytes mode returns byte ranges.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "format": {"type": "string", "enum": ["structured", "bytes"]},
                    "cursor": {"type": ["string", "null"]},
                    "offset": {"type": "integer", "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_PAGE_LIMIT},
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
                    "predicates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field", "op"],
                            "properties": {
                                "field": {"type": "string"},
                                "op": {
                                    "type": "string",
                                    "enum": ["eq", "ne", "in", "prefix", "suffix", "contains", "gt", "gte", "lt", "lte", "exists", "not_exists"]
                                },
                                "value": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "integer", "minimum": 0},
                                        {"type": "number"},
                                        {"type": "boolean"},
                                        {
                                            "type": "array",
                                            "items": {"type": ["string", "integer", "number", "boolean"]}
                                        },
                                        {"type": "null"}
                                    ]
                                }
                            },
                            "additionalProperties": false
                        }
                    },
                    "group_by": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "measures": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["name", "op"],
                            "properties": {
                                "name": {"type": "string"},
                                "op": {"type": "string", "enum": ["count", "sum", "avg", "min", "max"]},
                                "field": {"type": ["string", "null"]}
                            },
                            "additionalProperties": false
                        }
                    },
                    "sort": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field"],
                            "properties": {
                                "field": {"type": "string"},
                                "direction": {"type": "string", "enum": ["asc", "desc"]}
                            },
                            "additionalProperties": false
                        }
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_AGGREGATE_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "find",
            description:
                "Search paths with catalog field predicates and project fields. Use read for body content and stat for schema or sample.",
            parameters: json!({
                "type": "object",
                "required": ["path"],
                "properties": {
                    "path": {"type": "string"},
                    "predicates": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field", "op"],
                            "properties": {
                                "field": {"type": "string"},
                                "op": {
                                    "type": "string",
                                    "enum": ["eq", "ne", "in", "prefix", "suffix", "contains", "gt", "gte", "lt", "lte", "exists", "not_exists"]
                                },
                                "value": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "integer", "minimum": 0},
                                        {"type": "number"},
                                        {"type": "boolean"},
                                        {
                                            "type": "array",
                                            "items": {"type": ["string", "integer", "number", "boolean"]}
                                        },
                                        {"type": "null"}
                                    ]
                                }
                            },
                            "additionalProperties": false
                        }
                    },
                    "sort": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["field"],
                            "properties": {
                                "field": {"type": "string"},
                                "direction": {"type": "string", "enum": ["asc", "desc"]}
                            },
                            "additionalProperties": false
                        }
                    },
                    "fields": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "facets": {
                        "type": "array",
                        "items": {"type": "string"}
                    },
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_FIND_LIMIT}
                },
                "additionalProperties": false
            }),
        },
        AgentToolDefinition {
            name: "grep",
            description: "Search file bodies for a case-insensitive literal substring and return matching lines with line numbers. Scope path to one directory or file when known.",
            parameters: json!({
                "type": "object",
                "required": ["path", "pattern", "recursive"],
                "properties": {
                    "path": {"type": "string"},
                    "pattern": {"type": "string"},
                    "recursive": {"type": "boolean"},
                    "cursor": {"type": ["string", "null"]},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_AGENT_GREP_LIMIT}
                },
                "additionalProperties": false
            }),
        },
    ]
}

pub fn execute_agent_tool<T>(namespace: &T, name: &str, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    match name {
        "ls" => execute_ls(namespace, args),
        "stat" => execute_stat(namespace, args),
        "catalog" => execute_catalog(namespace, args),
        "grep" => execute_grep(namespace, args),
        "read" => execute_read(namespace, args),
        "find" => execute_find(namespace, args),
        "aggregate" => execute_aggregate(namespace, args),
        other => Err(AgentError::InvalidArgument(format!(
            "unknown agent tool {other}"
        ))),
    }
}

fn execute_ls<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let page = namespace.agent_list_page(
        path,
        NamespaceListOptions {
            cursor: optional_string_arg(args, "cursor")?,
            limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_PAGE_LIMIT),
        },
    )?;
    Ok(list_page_json(&page))
}

fn execute_stat<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let card = namespace
        .agent_stat_card(path)?
        .ok_or_else(|| AgentError::NotFound(path.to_owned()))?;
    Ok(json!({
        "card": card_json(&card),
    }))
}

fn execute_catalog<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let field_prefix = optional_string_arg(args, "field_prefix")?;
    let include_facets = optional_bool_arg(args, "include_facets")?.unwrap_or(false);
    let card = namespace
        .agent_stat_card(path)?
        .ok_or_else(|| AgentError::NotFound(path.to_owned()))?;
    let catalog = filtered_catalog(&card.catalog, field_prefix.as_deref(), include_facets);
    let catalog_empty = catalog_is_empty(&catalog);
    let child_catalogs = if catalog_empty && matches!(card.kind, NamespaceCardKind::Directory) {
        child_catalogs(namespace, &card.path, include_facets)?
    } else {
        Vec::new()
    };
    Ok(json!({
        "path": card.path,
        "catalog_empty": catalog_empty,
        "catalog": catalog_json(&catalog),
        "child_catalogs": child_catalogs,
    }))
}

fn execute_grep<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let pattern = required_string_arg(args, "pattern")?;
    let recursive = optional_bool_arg(args, "recursive")?.ok_or_else(|| {
        AgentError::InvalidArgument("grep requires a boolean recursive argument".to_owned())
    })?;
    let result = namespace.agent_grep_paths(NamespaceGrepRequest {
        path: path.to_owned(),
        pattern: pattern.to_owned(),
        recursive,
        cursor: optional_string_arg(args, "cursor")?,
        limit: optional_bounded_usize_arg(args, "limit", MAX_AGENT_GREP_LIMIT)?
            .unwrap_or(DEFAULT_AGENT_GREP_LIMIT),
        max_files: None,
        max_bytes: None,
    })?;
    Ok(grep_result_json(&result))
}

fn grep_result_json(result: &NamespaceGrepResult) -> Value {
    json!({
        "path": result.path,
        "pattern": result.pattern,
        "recursive": result.recursive,
        "matches": result
            .matches
            .iter()
            .map(|match_| json!({
                "path": match_.path,
                "line_number": match_.line_number,
                "snippet": match_.snippet,
            }))
            .collect::<Vec<_>>(),
        "files_scanned": result.files_scanned,
        "next_cursor": result.next_cursor,
        "truncated": result.truncated,
    })
}

fn execute_read<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let options = NamespaceReadOptions {
        format: read_format_arg(args)?,
        cursor: optional_string_arg(args, "cursor")?,
        offset: optional_u64_arg(args, "offset")?.unwrap_or(0),
        limit: optional_usize_arg(args, "limit")?.unwrap_or(DEFAULT_AGENT_PAGE_LIMIT),
        expected_generation: None,
    };
    guard_large_structured_pagination(namespace, path, &options)?;
    let page = namespace.agent_read_page(path, options)?;
    Ok(read_page_json(&page))
}

fn guard_large_structured_pagination<T>(
    namespace: &T,
    path: &str,
    options: &NamespaceReadOptions,
) -> Result<(), AgentError>
where
    T: AgentNamespace + ?Sized,
{
    if !matches!(options.format, NamespaceReadFormat::Structured) {
        return Ok(());
    }
    let Some(card) = namespace.agent_stat_card(path)? else {
        return Ok(());
    };
    let Some(record_count) = card.record_count else {
        return Ok(());
    };
    if record_count.count <= MAX_AGENT_PAGE_LIMIT {
        return Ok(());
    }
    Err(AgentError::InvalidArgument(format!(
        "structured pagination for {path} has {} records; use stat record_count or find with catalog predicates and limit=1, then read match_count",
        record_count.count
    )))
}

fn execute_find<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let fields = fields_arg(args)?;
    reject_unsupported_find_arguments(args)?;
    let result = namespace.agent_find_paths(NamespaceFindRequest {
        path: path.to_owned(),
        predicates: predicates_arg(args)?,
        sort: sort_arg(args)?,
        include: Vec::new(),
        facets: facets_arg(args)?,
        cursor: optional_string_arg(args, "cursor")?,
        limit: optional_bounded_usize_arg(args, "limit", MAX_AGENT_FIND_LIMIT)?
            .unwrap_or(DEFAULT_AGENT_FIND_LIMIT),
    })?;
    Ok(find_result_json(&result, fields.as_deref()))
}

fn execute_aggregate<T>(namespace: &T, args: &Value) -> Result<Value, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let path = required_string_arg(args, "path")?;
    let predicates = predicates_arg(args)?;
    let group_by = group_by_arg(args)?;
    let measures = aggregate_measures_arg(args)?;
    let sort = aggregate_sort_arg(args)?;
    let limit = optional_bounded_usize_arg(args, "limit", MAX_AGENT_AGGREGATE_LIMIT)?
        .unwrap_or(DEFAULT_AGENT_AGGREGATE_LIMIT);
    let result = namespace.agent_aggregate_paths(NamespaceAggregateRequest {
        path: path.to_owned(),
        predicates,
        group_by,
        measures,
        sort,
        limit,
    })?;
    Ok(aggregate_result_json(&result))
}

fn list_page_json(page: &NamespaceListPage) -> Value {
    json!({
        "path": page.path,
        "entry_count": page.entry_count,
        "entries": page.entries.iter().map(list_entry_json).collect::<Vec<_>>(),
        "next_cursor": page.next_cursor,
        "truncated": page.truncated,
    })
}

fn find_result_json(result: &NamespaceFindResult, fields: Option<&[String]>) -> Value {
    json!({
        "path": result.path,
        "match_count": result.match_count,
        "matches": result.matches.iter().map(|card| find_match_json(card, fields)).collect::<Vec<_>>(),
        "facets": result.facets.iter().map(facet_summary_json).collect::<Vec<_>>(),
        "next_cursor": result.next_cursor,
        "truncated": result.truncated,
    })
}

fn read_page_json(page: &NamespaceReadPage) -> Value {
    json!({
        "path": page.path,
        "total_size_bytes": page.total_size_bytes,
        "format": read_format_name(&page.format),
        "record_type": page.record_type.as_ref().map(record_type_name),
        "record_count": page.record_count,
        "cursor": page.cursor,
        "next_cursor": page.next_cursor,
        "truncated": page.truncated,
        "items": page.items.iter().map(read_item_json).collect::<Vec<_>>(),
        "bytes": page.bytes,
    })
}

fn card_json(card: &NamespaceCard) -> Value {
    json!({
        "path": card.path,
        "name": card.name,
        "kind": card_kind_name(&card.kind),
        "size_bytes": card.size_bytes,
        "entry_count": card.entry_count,
        "record_count": card.record_count.as_ref().map(record_count_json),
        "schema": card.schema.as_ref().map(schema_json),
        "sample": card.sample,
        "body": card.body.as_ref().map(body_json),
        "catalog": catalog_json(&card.catalog),
        "indexed_values": card.indexed_values.iter().map(index_value_json).collect::<Vec<_>>(),
    })
}

fn list_entry_json(card: &NamespaceCard) -> Value {
    json!({
        "path": card.path,
        "name": card.name,
        "kind": card_kind_name(&card.kind),
        "size_bytes": card.size_bytes,
        "entry_count": card.entry_count,
    })
}

fn find_match_json(card: &NamespaceCard, fields: Option<&[String]>) -> Value {
    if fields.is_some() {
        let mut object = Map::new();
        object.insert("path".to_owned(), json!(card.path));
        object.insert(
            "values".to_owned(),
            projected_values_json(card, fields.unwrap_or(&[])),
        );
        return Value::Object(object);
    }
    let mut object = Map::new();
    object.insert("path".to_owned(), json!(card.path));
    Value::Object(object)
}

fn projected_values_json(card: &NamespaceCard, fields: &[String]) -> Value {
    let mut output = Map::new();
    for field in fields {
        let values = projected_predicate_values(card, field)
            .into_iter()
            .map(|value| predicate_value_json(&value))
            .collect::<Vec<_>>();
        match values.as_slice() {
            [] => {}
            [value] => {
                output.insert(field.clone(), value.clone());
            }
            _ => {
                output.insert(field.clone(), Value::Array(values));
            }
        }
    }
    Value::Object(output)
}

fn projected_predicate_values(card: &NamespaceCard, field: &str) -> Vec<NamespacePredicateValue> {
    match field {
        "path" => vec![NamespacePredicateValue::String(card.path.clone())],
        "name" => vec![NamespacePredicateValue::String(card.name.clone())],
        "kind" => vec![NamespacePredicateValue::String(
            card_kind_name(&card.kind).to_owned(),
        )],
        "size_bytes" => card
            .size_bytes
            .map(NamespacePredicateValue::U64)
            .into_iter()
            .collect(),
        "body.content_type" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.content_type.clone()))
            .into_iter()
            .collect(),
        "body.producer" => card
            .body
            .as_ref()
            .map(|body| NamespacePredicateValue::String(body.producer.clone()))
            .into_iter()
            .collect(),
        _ => card
            .indexed_values
            .iter()
            .filter(|value| value.field.id == field)
            .map(|value| value.value.clone())
            .collect(),
    }
}

fn record_count_json(count: &NamespaceRecordCount) -> Value {
    json!(count.count)
}

fn schema_json(schema: &NamespaceSchema) -> Value {
    json!({
        "record_type": record_type_name(&schema.record_type),
        "fields": schema.fields,
    })
}

fn body_json(body: &NamespaceBodyDescriptor) -> Value {
    json!({
        "producer": body.producer,
        "size": body.size,
        "content_type": body.content_type,
    })
}

fn catalog_json(catalog: &NamespaceQueryCatalog) -> Value {
    json!({
        "filterable": grouped_filterable_json(&catalog.filterable),
        "sortable": catalog.sortable.iter().map(|field| field.id.clone()).collect::<Vec<_>>(),
        "facetable": catalog.facetable.iter().map(|field| field.id.clone()).collect::<Vec<_>>(),
        "facets": catalog.facets.iter().map(facet_summary_json).collect::<Vec<_>>(),
    })
}

fn grouped_filterable_json(capabilities: &[NamespaceFilterCapability]) -> Vec<Value> {
    let mut groups: Vec<(Vec<&'static str>, Vec<String>)> = Vec::new();
    for capability in capabilities {
        let operators = capability
            .operators
            .iter()
            .map(predicate_op_name)
            .collect::<Vec<_>>();
        match groups.iter_mut().find(|(group, _)| *group == operators) {
            Some((_, fields)) => fields.push(capability.field.id.clone()),
            None => groups.push((operators, vec![capability.field.id.clone()])),
        }
    }
    groups
        .into_iter()
        .map(|(operators, fields)| json!({"operators": operators, "fields": fields}))
        .collect()
}

fn facet_summary_json(facet: &NamespaceFacetSummary) -> Value {
    json!({
        "field": facet.field.id,
        "values": facet.values.iter().map(facet_value_json).collect::<Vec<_>>(),
        "distinct_count": facet.distinct_count,
        "truncated": facet.truncated,
    })
}

fn facet_value_json(value: &NamespaceFacetValue) -> Value {
    json!({
        "value": predicate_value_json(&value.value),
        "count": value.count,
    })
}

fn index_value_json(value: &NamespaceIndexValue) -> Value {
    json!({
        "field": value.field.id,
        "value": predicate_value_json(&value.value),
    })
}

fn predicate_value_json(value: &NamespacePredicateValue) -> Value {
    match value {
        NamespacePredicateValue::String(value) => json!(value),
        NamespacePredicateValue::U64(value) => json!(value),
        NamespacePredicateValue::F64(value) if value.is_finite() => json!(value),
        NamespacePredicateValue::F64(_) => Value::Null,
        NamespacePredicateValue::List(values) => {
            Value::Array(values.iter().map(predicate_value_json).collect())
        }
    }
}

fn aggregate_result_json(result: &NamespaceAggregateResult) -> Value {
    json!({
        "path": result.path,
        "input_match_count": result.input_match_count,
        "row_count": result.row_count,
        "group_count": result.group_count,
        "groups": result.groups.iter().map(aggregate_group_json).collect::<Vec<_>>(),
        "truncated": result.truncated,
    })
}

fn aggregate_group_json(group: &NamespaceAggregateGroup) -> Value {
    let key = group
        .key
        .iter()
        .map(|value| (value.field.id.clone(), predicate_value_json(&value.value)))
        .collect::<Map<_, _>>();
    let values = group
        .measures
        .iter()
        .map(|measure| (measure.name.clone(), aggregate_value_json(&measure.value)))
        .collect::<Map<_, _>>();
    json!({
        "key": key,
        "values": values,
    })
}

fn aggregate_value_json(value: &NamespaceAggregateValue) -> Value {
    match value {
        NamespaceAggregateValue::U64(value) => json!(value),
        NamespaceAggregateValue::F64(value) if value.is_finite() => json!(value),
        NamespaceAggregateValue::F64(_) | NamespaceAggregateValue::Null => Value::Null,
    }
}

fn read_item_json(item: &NamespaceReadItem) -> Value {
    let value = serde_json::from_str::<Value>(&item.value_json)
        .unwrap_or_else(|_| Value::String(item.value_json.clone()));
    json!({
        "index": item.index,
        "value": value,
    })
}

fn object_args(args: &Value) -> Result<&Map<String, Value>, AgentError> {
    args.as_object().ok_or_else(|| {
        AgentError::InvalidArgument("agent tool arguments must be a JSON object".to_owned())
    })
}

fn required_string_arg<'a>(args: &'a Value, name: &'static str) -> Result<&'a str, AgentError> {
    object_args(args)?
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| AgentError::InvalidArgument(format!("missing string argument {name}")))
}

fn optional_string_arg(args: &Value, name: &'static str) -> Result<Option<String>, AgentError> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_str()
        .map(|value| Some(value.to_owned()))
        .ok_or_else(|| AgentError::InvalidArgument(format!("{name} must be a string or null")))
}

fn optional_bool_arg(args: &Value, name: &'static str) -> Result<Option<bool>, AgentError> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value
        .as_bool()
        .map(Some)
        .ok_or_else(|| AgentError::InvalidArgument(format!("{name} must be a boolean or null")))
}

fn optional_u64_arg(args: &Value, name: &'static str) -> Result<Option<u64>, AgentError> {
    let Some(value) = object_args(args)?.get(name) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    value.as_u64().map(Some).ok_or_else(|| {
        AgentError::InvalidArgument(format!("{name} must be a non-negative integer"))
    })
}

fn optional_usize_arg(args: &Value, name: &'static str) -> Result<Option<usize>, AgentError> {
    optional_bounded_usize_arg(args, name, MAX_AGENT_PAGE_LIMIT)
}

fn optional_bounded_usize_arg(
    args: &Value,
    name: &'static str,
    max: usize,
) -> Result<Option<usize>, AgentError> {
    optional_u64_arg(args, name)?
        .map(|value| {
            let value = usize::try_from(value).map_err(|_| {
                AgentError::InvalidArgument(format!("{name} exceeds platform limit"))
            })?;
            if value == 0 || value > max {
                return Err(AgentError::InvalidArgument(format!(
                    "{name} must be between 1 and {max}"
                )));
            }
            Ok(value)
        })
        .transpose()
}

fn read_format_arg(args: &Value) -> Result<NamespaceReadFormat, AgentError> {
    match object_args(args)?.get("format").and_then(Value::as_str) {
        None | Some("structured") => Ok(NamespaceReadFormat::Structured),
        Some("bytes") => Ok(NamespaceReadFormat::Bytes),
        Some(other) => Err(AgentError::InvalidArgument(format!(
            "unsupported read format {other}; expected structured or bytes"
        ))),
    }
}

fn predicates_arg(args: &Value) -> Result<Vec<NamespacePredicate>, AgentError> {
    let Some(value) = object_args(args)?.get("predicates") else {
        return Ok(Vec::new());
    };
    let predicates = value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("predicates must be an array".to_owned()))?;
    predicates.iter().map(predicate_arg).collect()
}

fn predicate_arg(value: &Value) -> Result<NamespacePredicate, AgentError> {
    let object = value
        .as_object()
        .ok_or_else(|| AgentError::InvalidArgument("predicate must be an object".to_owned()))?;
    let field = string_property(object, "field")?;
    let op = string_property(object, "op")?;
    let op = predicate_op_arg(op)?;
    let raw_value = object.get("value").filter(|value| !value.is_null());
    let value = match op {
        // Tolerate a stray value on existence checks; agents commonly pass
        // {"op": "exists", "value": true} and the value carries no meaning.
        NamespacePredicateOp::Exists | NamespacePredicateOp::NotExists => None,
        NamespacePredicateOp::In => {
            let value = raw_value.ok_or_else(|| {
                AgentError::InvalidArgument("predicate op in requires array value".to_owned())
            })?;
            if !value.is_array() {
                return Err(AgentError::InvalidArgument(
                    "predicate op in requires array value".to_owned(),
                ));
            }
            Some(predicate_value_arg(value)?)
        }
        _ => {
            let value = raw_value.ok_or_else(|| {
                AgentError::InvalidArgument(format!(
                    "predicate op {} requires value",
                    predicate_op_name(&op)
                ))
            })?;
            Some(predicate_value_arg(value)?)
        }
    };
    Ok(NamespacePredicate {
        field: NamespaceFindField::new(field),
        op,
        value,
    })
}

fn sort_arg(args: &Value) -> Result<Vec<NamespaceSort>, AgentError> {
    let Some(value) = object_args(args)?.get("sort") else {
        return Ok(Vec::new());
    };
    let sort = value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("sort must be an array".to_owned()))?;
    sort.iter().map(sort_item_arg).collect()
}

fn sort_item_arg(value: &Value) -> Result<NamespaceSort, AgentError> {
    let object = value
        .as_object()
        .ok_or_else(|| AgentError::InvalidArgument("sort item must be an object".to_owned()))?;
    let field = string_property(object, "field")?;
    let direction = object
        .get("direction")
        .and_then(Value::as_str)
        .unwrap_or("asc");
    Ok(NamespaceSort {
        field: NamespaceSortField::new(field),
        direction: match direction {
            "asc" => NamespaceSortDirection::Asc,
            "desc" => NamespaceSortDirection::Desc,
            other => {
                return Err(AgentError::InvalidArgument(format!(
                    "unsupported sort direction {other}"
                )))
            }
        },
    })
}

fn reject_unsupported_find_arguments(args: &Value) -> Result<(), AgentError> {
    if object_args(args)?.contains_key("include") {
        return Err(AgentError::InvalidArgument(
            "unsupported argument include; use stat for schema or sample and read for body content"
                .to_owned(),
        ));
    }
    Ok(())
}

fn fields_arg(args: &Value) -> Result<Option<Vec<String>>, AgentError> {
    let Some(value) = object_args(args)?.get("fields") else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let fields = value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("fields must be an array".to_owned()))?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                AgentError::InvalidArgument("fields entries must be strings".to_owned())
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(fields))
}

fn facets_arg(args: &Value) -> Result<Vec<NamespaceFindField>, AgentError> {
    let Some(value) = object_args(args)?.get("facets") else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("facets must be an array".to_owned()))?
        .iter()
        .map(|value| {
            value.as_str().map(NamespaceFindField::new).ok_or_else(|| {
                AgentError::InvalidArgument("facets entries must be strings".to_owned())
            })
        })
        .collect()
}

fn group_by_arg(args: &Value) -> Result<Vec<NamespaceFindField>, AgentError> {
    let Some(value) = object_args(args)?.get("group_by") else {
        return Ok(Vec::new());
    };
    if value.is_null() {
        return Ok(Vec::new());
    }
    value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("group_by must be an array".to_owned()))?
        .iter()
        .map(|value| {
            value.as_str().map(NamespaceFindField::new).ok_or_else(|| {
                AgentError::InvalidArgument("group_by entries must be strings".to_owned())
            })
        })
        .collect()
}

fn aggregate_measures_arg(args: &Value) -> Result<Vec<NamespaceAggregateMeasure>, AgentError> {
    let value = object_args(args)?
        .get("measures")
        .ok_or_else(|| AgentError::InvalidArgument("missing array argument measures".to_owned()))?;
    let measures = value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("measures must be an array".to_owned()))?;
    if measures.is_empty() {
        return Err(AgentError::InvalidArgument(
            "measures must contain at least one measure".to_owned(),
        ));
    }
    measures
        .iter()
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                AgentError::InvalidArgument("measure must be an object".to_owned())
            })?;
            let name = string_property(object, "name")?.to_owned();
            if name.is_empty() {
                return Err(AgentError::InvalidArgument(
                    "measure name must not be empty".to_owned(),
                ));
            }
            let op = aggregate_op_arg(string_property(object, "op")?)?;
            let field = object
                .get("field")
                .and_then(|value| (!value.is_null()).then_some(value))
                .map(|value| {
                    value.as_str().map(str::to_owned).ok_or_else(|| {
                        AgentError::InvalidArgument(
                            "measure field must be a string or null".to_owned(),
                        )
                    })
                })
                .transpose()?;
            if !matches!(op, NamespaceAggregateOp::Count) && field.is_none() {
                return Err(AgentError::InvalidArgument(format!(
                    "measure {name} with op {} requires field",
                    aggregate_op_name(&op)
                )));
            }
            Ok(NamespaceAggregateMeasure {
                name,
                op,
                field: field.map(NamespaceFindField::new),
            })
        })
        .collect()
}

fn aggregate_op_arg(op: &str) -> Result<NamespaceAggregateOp, AgentError> {
    match op {
        "count" => Ok(NamespaceAggregateOp::Count),
        "sum" => Ok(NamespaceAggregateOp::Sum),
        "avg" => Ok(NamespaceAggregateOp::Avg),
        "min" => Ok(NamespaceAggregateOp::Min),
        "max" => Ok(NamespaceAggregateOp::Max),
        other => Err(AgentError::InvalidArgument(format!(
            "unsupported aggregate op {other}"
        ))),
    }
}

fn aggregate_sort_arg(args: &Value) -> Result<Vec<NamespaceAggregateSort>, AgentError> {
    let Some(value) = object_args(args)?.get("sort") else {
        return Ok(Vec::new());
    };
    let sort = value
        .as_array()
        .ok_or_else(|| AgentError::InvalidArgument("sort must be an array".to_owned()))?;
    sort.iter()
        .map(|value| {
            let object = value.as_object().ok_or_else(|| {
                AgentError::InvalidArgument("sort item must be an object".to_owned())
            })?;
            let field = string_property(object, "field")?.to_owned();
            let direction = object
                .get("direction")
                .and_then(Value::as_str)
                .unwrap_or("asc");
            Ok(NamespaceAggregateSort {
                field,
                direction: match direction {
                    "asc" => NamespaceSortDirection::Asc,
                    "desc" => NamespaceSortDirection::Desc,
                    other => {
                        return Err(AgentError::InvalidArgument(format!(
                            "unsupported sort direction {other}"
                        )))
                    }
                },
            })
        })
        .collect()
}

fn filtered_catalog(
    catalog: &NamespaceQueryCatalog,
    field_prefix: Option<&str>,
    include_facets: bool,
) -> NamespaceQueryCatalog {
    NamespaceQueryCatalog {
        filterable: catalog
            .filterable
            .iter()
            .filter(|capability| field_matches_prefix(capability.field.id.as_str(), field_prefix))
            .cloned()
            .collect(),
        sortable: catalog
            .sortable
            .iter()
            .filter(|field| field_matches_prefix(field.id.as_str(), field_prefix))
            .cloned()
            .collect(),
        facetable: catalog
            .facetable
            .iter()
            .filter(|field| field_matches_prefix(field.id.as_str(), field_prefix))
            .cloned()
            .collect(),
        facets: if include_facets {
            catalog
                .facets
                .iter()
                .filter(|facet| field_matches_prefix(facet.field.id.as_str(), field_prefix))
                .cloned()
                .collect()
        } else {
            Vec::new()
        },
        projections: catalog.projections.clone(),
    }
}

fn catalog_is_empty(catalog: &NamespaceQueryCatalog) -> bool {
    catalog.filterable.is_empty() && catalog.sortable.is_empty() && catalog.facetable.is_empty()
}

fn child_catalogs<T>(
    namespace: &T,
    path: &str,
    include_facets: bool,
) -> Result<Vec<Value>, AgentError>
where
    T: AgentNamespace + ?Sized,
{
    let page = namespace.agent_list_page(
        path,
        NamespaceListOptions {
            cursor: None,
            limit: 20,
        },
    )?;
    let mut children = Vec::new();
    for entry in page.entries {
        if !matches!(entry.kind, NamespaceCardKind::Directory) {
            continue;
        }
        let Some(card) = namespace.agent_stat_card(&entry.path)? else {
            continue;
        };
        let catalog = filtered_catalog(&card.catalog, None, include_facets);
        if catalog_is_empty(&catalog) {
            continue;
        }
        children.push(json!({
            "path": card.path,
            "catalog": catalog_json(&catalog),
        }));
        if children.len() == 5 {
            break;
        }
    }
    Ok(children)
}

fn field_matches_prefix(field: &str, prefix: Option<&str>) -> bool {
    prefix
        .map(|prefix| field.starts_with(prefix) || field.contains(prefix))
        .unwrap_or(true)
}

fn aggregate_op_name(op: &NamespaceAggregateOp) -> &'static str {
    match op {
        NamespaceAggregateOp::Count => "count",
        NamespaceAggregateOp::Sum => "sum",
        NamespaceAggregateOp::Avg => "avg",
        NamespaceAggregateOp::Min => "min",
        NamespaceAggregateOp::Max => "max",
    }
}

fn string_property<'a>(
    object: &'a Map<String, Value>,
    name: &'static str,
) -> Result<&'a str, AgentError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| AgentError::InvalidArgument(format!("missing string property {name}")))
}

fn predicate_op_arg(op: &str) -> Result<NamespacePredicateOp, AgentError> {
    match op {
        "eq" => Ok(NamespacePredicateOp::Eq),
        "ne" | "not_equal" => Ok(NamespacePredicateOp::NotEqual),
        "in" => Ok(NamespacePredicateOp::In),
        "prefix" => Ok(NamespacePredicateOp::Prefix),
        "suffix" => Ok(NamespacePredicateOp::Suffix),
        "contains" => Ok(NamespacePredicateOp::Contains),
        "gt" | "greater_than" => Ok(NamespacePredicateOp::GreaterThan),
        "gte" | "greater_than_or_equal" => Ok(NamespacePredicateOp::GreaterThanOrEqual),
        "lt" | "less_than" => Ok(NamespacePredicateOp::LessThan),
        "lte" | "less_than_or_equal" => Ok(NamespacePredicateOp::LessThanOrEqual),
        "exists" => Ok(NamespacePredicateOp::Exists),
        "not_exists" => Ok(NamespacePredicateOp::NotExists),
        other => Err(AgentError::InvalidArgument(format!(
            "unsupported predicate operator {other}"
        ))),
    }
}

fn predicate_value_arg(value: &Value) -> Result<NamespacePredicateValue, AgentError> {
    if let Some(values) = value.as_array() {
        return values
            .iter()
            .map(predicate_scalar_value_arg)
            .collect::<Result<Vec<_>, _>>()
            .map(NamespacePredicateValue::List);
    }
    predicate_scalar_value_arg(value)
}

fn predicate_scalar_value_arg(value: &Value) -> Result<NamespacePredicateValue, AgentError> {
    if let Some(value) = value.as_str() {
        return Ok(NamespacePredicateValue::String(value.to_owned()));
    }
    if let Some(value) = value.as_bool() {
        return Ok(NamespacePredicateValue::U64(u64::from(value)));
    }
    if let Some(value) = value.as_u64() {
        return Ok(NamespacePredicateValue::U64(value));
    }
    if let Some(value) = value.as_f64() {
        if value.is_finite() {
            return Ok(NamespacePredicateValue::F64(value));
        }
    }
    Err(AgentError::InvalidArgument(
        "predicate value must be a string, boolean, finite number, or array of scalar values"
            .to_owned(),
    ))
}

fn card_kind_name(kind: &NamespaceCardKind) -> &'static str {
    match kind {
        NamespaceCardKind::File => "file",
        NamespaceCardKind::Directory => "directory",
        NamespaceCardKind::Symlink => "symlink",
        NamespaceCardKind::Special => "special",
    }
}

fn record_type_name(record_type: &NamespaceRecordType) -> &'static str {
    match record_type {
        NamespaceRecordType::DirectoryEntries => "directory_entries",
        NamespaceRecordType::JsonArray => "json_array",
        NamespaceRecordType::JsonObject => "json_object",
        NamespaceRecordType::YamlMapping => "yaml_mapping",
        NamespaceRecordType::TextLines => "text_lines",
    }
}

fn read_format_name(format: &NamespaceReadFormat) -> &'static str {
    match format {
        NamespaceReadFormat::Structured => "structured",
        NamespaceReadFormat::Bytes => "bytes",
    }
}

fn predicate_op_name(op: &NamespacePredicateOp) -> &'static str {
    match op {
        NamespacePredicateOp::Eq => "eq",
        NamespacePredicateOp::NotEqual => "ne",
        NamespacePredicateOp::In => "in",
        NamespacePredicateOp::Prefix => "prefix",
        NamespacePredicateOp::Suffix => "suffix",
        NamespacePredicateOp::Contains => "contains",
        NamespacePredicateOp::GreaterThan => "gt",
        NamespacePredicateOp::GreaterThanOrEqual => "gte",
        NamespacePredicateOp::LessThan => "lt",
        NamespacePredicateOp::LessThanOrEqual => "lte",
        NamespacePredicateOp::Exists => "exists",
        NamespacePredicateOp::NotExists => "not_exists",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nokv_meta::{
        NamespaceAggregateOutputMeasure, NamespaceFieldSource, NamespaceFieldSourceKind,
        NamespaceFieldValue, NamespaceGrepMatch, NamespaceInclude, NamespaceRecordCount,
        RecordCountProvenance,
    };
    use nokv_types::InodeId;
    use std::cell::RefCell;
    use std::collections::BTreeMap;

    struct FakeNamespace {
        last_find: RefCell<Option<NamespaceFindRequest>>,
        last_aggregate: RefCell<Option<NamespaceAggregateRequest>>,
        read_calls: RefCell<usize>,
        record_count: usize,
        find_matches: Vec<NamespaceCard>,
        aggregate_result: NamespaceAggregateResult,
        stat_cards: BTreeMap<String, NamespaceCard>,
        list_entries: Vec<NamespaceCard>,
    }

    impl FakeNamespace {
        fn new() -> Self {
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
                find_matches: vec![sample_card("/runs/run-1", 1)],
                aggregate_result: sample_aggregate_result(),
                stat_cards: BTreeMap::new(),
                list_entries: vec![sample_card("/runs/run-1", 1)],
            }
        }

        fn with_record_count(record_count: usize) -> Self {
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count,
                find_matches: vec![sample_card("/runs/run-1", record_count)],
                aggregate_result: sample_aggregate_result(),
                stat_cards: BTreeMap::new(),
                list_entries: vec![sample_card("/runs/run-1", record_count)],
            }
        }

        fn with_aggregate_result(aggregate_result: NamespaceAggregateResult) -> Self {
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
                find_matches: vec![sample_card("/runs/run-1", 1)],
                aggregate_result,
                stat_cards: BTreeMap::new(),
                list_entries: vec![sample_card("/runs/run-1", 1)],
            }
        }

        fn with_root_child_catalog() -> Self {
            let mut root = sample_card("/yanex", 1);
            root.kind = NamespaceCardKind::Directory;
            root.catalog = empty_catalog();
            let mut runs = sample_card("/yanex/runs", 1);
            runs.kind = NamespaceCardKind::Directory;
            runs.catalog.filterable.push(NamespaceFilterCapability {
                field: NamespaceFindField::new("run.script"),
                operators: vec![NamespacePredicateOp::Eq],
            });
            runs.catalog
                .sortable
                .push(NamespaceSortField::new("run.script"));
            let mut stat_cards = BTreeMap::new();
            stat_cards.insert(root.path.clone(), root);
            stat_cards.insert(runs.path.clone(), runs.clone());
            Self {
                last_find: RefCell::new(None),
                last_aggregate: RefCell::new(None),
                read_calls: RefCell::new(0),
                record_count: 1,
                find_matches: vec![sample_card("/yanex/runs/run-1", 1)],
                aggregate_result: sample_aggregate_result(),
                stat_cards,
                list_entries: vec![runs],
            }
        }
    }

    impl AgentNamespace for FakeNamespace {
        fn agent_stat_card(&self, path: &str) -> Result<Option<NamespaceCard>, AgentError> {
            Ok(Some(
                self.stat_cards
                    .get(path)
                    .cloned()
                    .unwrap_or_else(|| sample_card(path, self.record_count)),
            ))
        }

        fn agent_list_page(
            &self,
            path: &str,
            _options: NamespaceListOptions,
        ) -> Result<NamespaceListPage, AgentError> {
            Ok(NamespaceListPage {
                path: path.to_owned(),
                evidence: "nokv-native:///runs".to_owned(),
                snapshot_id: Some(9),
                entry_count: self.list_entries.len(),
                entries: self.list_entries.clone(),
                next_cursor: None,
                truncated: false,
            })
        }

        fn agent_find_paths(
            &self,
            request: NamespaceFindRequest,
        ) -> Result<NamespaceFindResult, AgentError> {
            self.last_find.replace(Some(request));
            let matches = self.find_matches.clone();
            Ok(NamespaceFindResult {
                path: "/runs".to_owned(),
                evidence: "nokv-native:///runs".to_owned(),
                snapshot_id: Some(9),
                match_count: matches.len(),
                matches,
                facets: vec![NamespaceFacetSummary {
                    field: NamespaceFindField::new("run.script"),
                    values: vec![NamespaceFacetValue {
                        value: NamespacePredicateValue::String("train.py".to_owned()),
                        count: 1,
                    }],
                    distinct_count: 1,
                    truncated: false,
                }],
                next_cursor: None,
                truncated: false,
                scanned_entries: 1,
            })
        }

        fn agent_aggregate_paths(
            &self,
            request: NamespaceAggregateRequest,
        ) -> Result<NamespaceAggregateResult, AgentError> {
            let mut result = self.aggregate_result.clone();
            result.path = request.path.clone();
            result.predicates = request.predicates.clone();
            self.last_aggregate.replace(Some(request));
            Ok(result)
        }

        fn agent_grep_paths(
            &self,
            request: NamespaceGrepRequest,
        ) -> Result<NamespaceGrepResult, AgentError> {
            let matched = "Checkpoint: best_model_1.0_1.pt";
            let matches = if matched
                .to_lowercase()
                .contains(&request.pattern.to_lowercase())
            {
                vec![NamespaceGrepMatch {
                    path: format!("{}/artifacts/stdout.txt", request.path),
                    line_number: 3,
                    snippet: matched.to_owned(),
                    evidence: format!(
                        "nokv-native://{}/artifacts/stdout.txt@generation:1#L3",
                        request.path
                    ),
                    generation: 1,
                }]
            } else {
                Vec::new()
            };
            Ok(NamespaceGrepResult {
                path: request.path.clone(),
                pattern: request.pattern.clone(),
                recursive: request.recursive,
                evidence: format!("nokv-native://{}", request.path),
                snapshot_id: Some(1),
                matches,
                files_scanned: 1,
                bytes_read: matched.len(),
                next_cursor: None,
                truncated: false,
            })
        }

        fn agent_read_page(
            &self,
            path: &str,
            _options: NamespaceReadOptions,
        ) -> Result<NamespaceReadPage, AgentError> {
            *self.read_calls.borrow_mut() += 1;
            Ok(NamespaceReadPage {
                path: path.to_owned(),
                evidence: "nokv-native:///runs/run-1/metadata.json@generation:7".to_owned(),
                snapshot_id: Some(9),
                generation: 7,
                total_size_bytes: 13,
                format: NamespaceReadFormat::Structured,
                record_type: Some(NamespaceRecordType::JsonObject),
                record_count: Some(1),
                cursor: None,
                next_cursor: None,
                truncated: false,
                items: vec![NamespaceReadItem {
                    index: 0,
                    value_json: r#"{"status":"completed"}"#.to_owned(),
                    evidence: "nokv-native:///runs/run-1/metadata.json@generation:7#item:0"
                        .to_owned(),
                }],
                bytes: None,
            })
        }
    }

    fn tool_definition<'a>(
        tools: &'a [AgentToolDefinition],
        name: &str,
    ) -> &'a AgentToolDefinition {
        tools
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("{name} tool must be registered"))
    }

    fn assert_tool_description_contains(
        tools: &[AgentToolDefinition],
        name: &str,
        expected: &[&str],
    ) {
        let description = tool_definition(tools, name).description;
        let normalized_description = description.to_ascii_lowercase();
        for snippet in expected {
            let normalized_snippet = snippet.to_ascii_lowercase();
            assert!(
                normalized_description.contains(&normalized_snippet),
                "{name} description must contain {snippet:?}; got {description:?}"
            );
        }
    }

    fn assert_json_lacks_agent_noise(value: &Value) {
        const FORBIDDEN_KEYS: &[&str] = &[
            "tool",
            "bytes_read",
            "evidence",
            "snapshot_id",
            "generation",
            "field_values",
            "source_path",
            "source_kind",
            "key_fields",
            "measures",
            "sample_matches",
            "scope_note",
            "predicates",
            "scanned_entries",
            "digest_uri",
            "manifest_id",
            "chunk_size",
            "block_size",
            "inode",
            "provenance",
        ];
        match value {
            Value::Object(object) => {
                for forbidden in FORBIDDEN_KEYS {
                    assert!(
                        !object.contains_key(*forbidden),
                        "agent output must not contain noisy key {forbidden:?}: {value}"
                    );
                }
                for child in object.values() {
                    assert_json_lacks_agent_noise(child);
                }
            }
            Value::Array(values) => {
                for child in values {
                    assert_json_lacks_agent_noise(child);
                }
            }
            _ => {}
        }
    }

    fn assert_schema_lacks_nested_descriptions(value: &Value) {
        match value {
            Value::Object(object) => {
                assert!(
                    !object.contains_key("description"),
                    "tool parameter schema must not contain nested descriptions: {value}"
                );
                for child in object.values() {
                    assert_schema_lacks_nested_descriptions(child);
                }
            }
            Value::Array(values) => {
                for child in values {
                    assert_schema_lacks_nested_descriptions(child);
                }
            }
            _ => {}
        }
    }

    fn empty_catalog() -> NamespaceQueryCatalog {
        NamespaceQueryCatalog {
            filterable: Vec::new(),
            sortable: Vec::new(),
            facetable: Vec::new(),
            facets: Vec::new(),
            projections: Vec::new(),
        }
    }

    fn sample_field_value(field: &str, value: NamespacePredicateValue) -> NamespaceFieldValue {
        NamespaceFieldValue {
            field: NamespaceFindField::new(field),
            value,
            source: NamespaceFieldSource {
                evidence: "nokv-native:///runs/run-1@generation:7".to_owned(),
                source_path: "/runs/run-1".to_owned(),
                source_kind: NamespaceFieldSourceKind::MaterializedIndex,
            },
        }
    }

    fn sample_aggregate_result() -> NamespaceAggregateResult {
        NamespaceAggregateResult {
            path: "/runs".to_owned(),
            evidence: "nokv-native:///runs".to_owned(),
            snapshot_id: Some(9),
            predicates: Vec::new(),
            input_match_count: 5,
            row_count: 3,
            group_count: 2,
            groups: vec![NamespaceAggregateGroup {
                key: vec![sample_field_value(
                    "param.lr",
                    NamespacePredicateValue::String("0.001".to_owned()),
                )],
                measures: vec![
                    NamespaceAggregateOutputMeasure {
                        name: "run_count".to_owned(),
                        op: NamespaceAggregateOp::Count,
                        field: Some(NamespaceFindField::new("metric.val_loss.min")),
                        value: NamespaceAggregateValue::U64(2),
                    },
                    NamespaceAggregateOutputMeasure {
                        name: "avg_min_val_loss".to_owned(),
                        op: NamespaceAggregateOp::Avg,
                        field: Some(NamespaceFindField::new("metric.val_loss.min")),
                        value: NamespaceAggregateValue::F64(0.3),
                    },
                    NamespaceAggregateOutputMeasure {
                        name: "stdout_available".to_owned(),
                        op: NamespaceAggregateOp::Sum,
                        field: Some(NamespaceFindField::new("artifact.stdout_available")),
                        value: NamespaceAggregateValue::U64(1),
                    },
                ],
                evidence: "nokv-native:///runs".to_owned(),
                sample_matches: vec![nokv_meta::NamespaceAggregateSample {
                    path: "/runs/run-1".to_owned(),
                    evidence: "nokv-native:///runs/run-1@generation:7".to_owned(),
                    generation: 7,
                }],
            }],
            truncated: false,
            scanned_entries: 5,
        }
    }

    fn sample_card(path: &str, record_count: usize) -> NamespaceCard {
        NamespaceCard {
            path: path.to_owned(),
            name: path.rsplit('/').next().unwrap_or("").to_owned(),
            kind: NamespaceCardKind::File,
            evidence: format!("nokv-native://{path}@generation:7"),
            snapshot_id: Some(9),
            inode: InodeId::new(42).unwrap(),
            generation: 7,
            size_bytes: Some(13),
            entry_count: None,
            record_count: Some(NamespaceRecordCount {
                count: record_count,
                provenance: RecordCountProvenance::MaterializedIndex,
            }),
            schema: Some(NamespaceSchema {
                record_type: NamespaceRecordType::JsonObject,
                fields: vec!["status".to_owned()],
            }),
            sample: vec![r#"{"status":"completed"}"#.to_owned()],
            body: Some(NamespaceBodyDescriptor {
                producer: "unit-test".to_owned(),
                digest_uri: "sha256:test".to_owned(),
                size: 13,
                content_type: "application/json".to_owned(),
                manifest_id: "runs/run-1/metadata.json".to_owned(),
                generation: 7,
                chunk_size: 4096,
                block_size: 4096,
            }),
            catalog: NamespaceQueryCatalog {
                filterable: vec![NamespaceFilterCapability {
                    field: NamespaceFindField::new("run.status"),
                    operators: vec![NamespacePredicateOp::Eq],
                }],
                sortable: vec![NamespaceSortField::new("run.status")],
                facetable: vec![NamespaceFindField::new("run.status")],
                facets: vec![NamespaceFacetSummary {
                    field: NamespaceFindField::new("run.status"),
                    values: vec![NamespaceFacetValue {
                        value: NamespacePredicateValue::String("completed".to_owned()),
                        count: 1,
                    }],
                    distinct_count: 1,
                    truncated: false,
                }],
                projections: vec![
                    NamespaceInclude::Body,
                    NamespaceInclude::Schema,
                    NamespaceInclude::Sample,
                    NamespaceInclude::Catalog,
                ],
            },
            indexed_values: vec![NamespaceIndexValue {
                field: NamespaceFindField::new("run.status"),
                value: NamespacePredicateValue::String("completed".to_owned()),
            }],
        }
    }

    #[test]
    fn agent_tool_registry_uses_posix_like_names() {
        let names = agent_tool_definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec!["ls", "stat", "catalog", "read", "aggregate", "find", "grep"]
        );
    }

    #[test]
    fn find_tool_schema_excludes_include_and_caps_limit_at_ten() {
        let tools = agent_tool_definitions();
        let find = tools
            .iter()
            .find(|tool| tool.name == "find")
            .expect("find tool must be registered");

        assert!(find.parameters["properties"].get("include").is_none());
        assert_eq!(find.parameters["properties"]["limit"]["maximum"], 10);
    }

    #[test]
    fn agent_tool_registry_documents_stable_api_semantics() {
        let tools = agent_tool_definitions();

        assert_tool_description_contains(&tools, "ls", &["direct children", "discover paths"]);
        assert_tool_description_contains(
            &tools,
            "stat",
            &[
                "single path",
                "compact card",
                "body",
                "schema",
                "sample",
                "catalog",
            ],
        );
        assert_tool_description_contains(&tools, "catalog", &["field ids", "find", "aggregate"]);
        assert_tool_description_contains(&tools, "read", &["file body", "json", "yaml", "text"]);
        assert_tool_description_contains(
            &tools,
            "aggregate",
            &["catalog field ids", "count", "group", "summary"],
        );
        assert_tool_description_contains(
            &tools,
            "find",
            &["search paths", "project fields", "read"],
        );
    }

    #[test]
    fn agent_tool_parameter_schemas_are_token_compact() {
        let tools = agent_tool_definitions();
        let find = tool_definition(&tools, "find");
        let read = tool_definition(&tools, "read");

        for tool in &tools {
            assert_schema_lacks_nested_descriptions(&tool.parameters);
        }
        assert!(find.parameters["properties"].get("include").is_none());
        assert!(read.parameters["properties"]
            .get("expected_generation")
            .is_none());
    }

    #[test]
    fn stat_tool_returns_compact_card_without_agent_noise() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "stat",
            &json!({
                "path": "/runs",
            }),
        )
        .unwrap();

        assert_json_lacks_agent_noise(&output);
        assert_eq!(output["card"]["path"], "/runs");
        assert_eq!(
            output["card"]["body"],
            json!({
                "producer": "unit-test",
                "size": 13,
                "content_type": "application/json"
            })
        );
        assert_eq!(
            output["card"]["catalog"]["filterable"][0]["fields"][0],
            "run.status"
        );
        assert!(output["card"]["catalog"].get("projections").is_none());
        assert_eq!(
            output["card"]["catalog"]["facets"][0],
            json!({
                "field": "run.status",
                "values": [{"value": "completed", "count": 1}],
                "distinct_count": 1,
                "truncated": false
            })
        );
        assert_eq!(output["card"]["record_count"], 1);
    }

    #[test]
    fn catalog_tool_returns_compact_filtered_catalog() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/runs",
                "field_prefix": "run.",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_json_lacks_agent_noise(&output);
        assert_eq!(output["path"], "/runs");
        assert_eq!(
            output["catalog"]["filterable"][0]["fields"],
            json!(["run.status"])
        );
        assert_eq!(output["catalog"]["sortable"], json!(["run.status"]));
        assert_eq!(output["catalog"]["facetable"], json!(["run.status"]));
        assert_eq!(output["catalog"]["facets"], json!([]));
        assert!(output.get("card").is_none());
        assert!(output.get("body").is_none());
    }

    #[test]
    fn catalog_tool_matches_field_prefix_inside_field_ids() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/runs",
                "field_prefix": "status",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_eq!(
            output["catalog"]["filterable"][0]["fields"][0],
            "run.status"
        );
        assert_eq!(output["catalog"]["sortable"], json!(["run.status"]));
    }

    #[test]
    fn catalog_tool_suggests_child_catalogs_when_current_scope_is_empty() {
        let namespace = FakeNamespace::with_root_child_catalog();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/yanex",
                "field_prefix": "status",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_eq!(output["catalog_empty"], true);
        assert_eq!(output["child_catalogs"][0]["path"], "/yanex/runs");
        let child_filterable = &output["child_catalogs"][0]["catalog"]["filterable"];
        let child_fields = child_filterable
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|group| group["fields"].as_array().unwrap().clone())
            .collect::<Vec<_>>();
        assert!(child_fields.contains(&json!("run.status")));
        assert!(child_fields.contains(&json!("run.script")));
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn catalog_tool_does_not_discover_child_catalogs_for_files() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "catalog",
            &json!({
                "path": "/runs/run-1",
                "field_prefix": "missing.",
                "include_facets": false
            }),
        )
        .unwrap();

        assert_eq!(output["catalog_empty"], true);
        assert_eq!(output["child_catalogs"], json!([]));
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn find_tool_translates_catalog_field_ids() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "eq", "value": "completed"}],
                "sort": [{"field": "run.status", "direction": "desc"}],
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.predicates[0].field.id, "run.status");
        assert_eq!(
            request.predicates[0].value,
            Some(NamespacePredicateValue::String("completed".to_owned()))
        );
        assert_eq!(request.sort[0].field.id, "run.status");
        assert_eq!(request.include, Vec::<NamespaceInclude>::new());
        assert_eq!(output["match_count"], 1);
        assert_eq!(output["matches"][0], json!({"path": "/runs/run-1"}));
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
        assert!(output["matches"][0].get("catalog").is_none());
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn find_tool_requests_and_returns_filtered_facets() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "eq", "value": "completed"}],
                "facets": ["run.script"],
                "limit": 1
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.facets, vec![NamespaceFindField::new("run.script")]);
        assert_eq!(
            output["facets"],
            json!([{
                "field": "run.script",
                "values": [{"value": "train.py", "count": 1}],
                "distinct_count": 1,
                "truncated": false
            }])
        );
    }

    #[test]
    fn find_tool_schema_rejects_body_schema_sample_includes() {
        let namespace = FakeNamespace::new();

        let err = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "include": ["body", "schema", "sample"],
                "limit": 1
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, AgentError::InvalidArgument(ref message) if message.contains("unsupported argument include")),
            "unexpected error: {err:?}"
        );
        assert!(namespace.last_find.borrow().is_none());
    }

    #[test]
    fn find_tool_rejects_catalog_include() {
        let namespace = FakeNamespace::new();

        let err = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "include": ["catalog"],
                "limit": 1
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, AgentError::InvalidArgument(ref message) if message.contains("unsupported argument include")),
            "unexpected error: {err:?}"
        );
        assert!(namespace.last_find.borrow().is_none());
    }

    #[test]
    fn find_tool_defaults_to_limit_ten() {
        let namespace = FakeNamespace::new();

        execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(request.limit, 10);
    }

    #[test]
    fn find_tool_rejects_limit_above_ten() {
        let namespace = FakeNamespace::new();

        let err = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "limit": 11
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, AgentError::InvalidArgument(ref message) if message.contains("limit must be between 1 and 10")),
            "unexpected error: {err:?}"
        );
        assert!(namespace.last_find.borrow().is_none());
    }

    #[test]
    fn find_tool_accepts_boolean_predicates_as_u64_facets() {
        let namespace = FakeNamespace::new();

        execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "git.has_uncommitted_changes", "op": "eq", "value": true}],
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_find.borrow().clone().unwrap();
        assert_eq!(
            request.predicates[0].field.id,
            "git.has_uncommitted_changes"
        );
        assert_eq!(
            request.predicates[0].value,
            Some(NamespacePredicateValue::U64(1))
        );
    }

    #[test]
    fn find_tool_projects_indexed_values_when_fields_are_requested() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "fields": ["run.id"],
                "limit": 5
            }),
        )
        .unwrap();

        assert_eq!(output["matches"][0]["path"], "/runs/run-1");
        assert_eq!(output["matches"][0]["values"], json!({}));
        assert!(output["matches"][0].get("indexed_values").is_none());
        assert!(output["matches"][0].get("name").is_none());
        assert!(output.get("catalog").is_none() || output["catalog"].is_null());
        assert!(namespace.last_find.borrow().is_some());
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn find_tool_projects_requested_fields_without_sources() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "fields": ["run.status"],
                "limit": 5
            }),
        )
        .unwrap();

        let match_ = &output["matches"][0];
        assert_eq!(match_["path"], "/runs/run-1");
        assert_eq!(match_["values"], json!({"run.status": "completed"}));
        assert!(match_.get("indexed_values").is_none());
        assert!(match_.get("name").is_none());
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn find_tool_does_not_project_storage_internal_body_fields() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "find",
            &json!({
                "path": "/runs",
                "fields": ["body.manifest_id"],
                "limit": 5
            }),
        )
        .unwrap();

        let match_ = &output["matches"][0];
        assert_eq!(match_["path"], "/runs/run-1");
        assert_eq!(match_["values"], json!({}));
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn grep_tool_returns_compact_matches_without_agent_noise() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "grep",
            &json!({
                "path": "/runs/run-1",
                "pattern": "best_model",
                "recursive": true,
                "limit": 5
            }),
        )
        .unwrap();

        assert_eq!(output["path"], "/runs/run-1");
        assert_eq!(output["matches"][0]["line_number"], 3);
        assert_eq!(
            output["matches"][0]["snippet"],
            "Checkpoint: best_model_1.0_1.pt"
        );
        assert!(output.get("tool").is_none());
        assert!(output.get("bytes_read").is_none());
        assert!(output["matches"][0].get("evidence").is_none());
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn aggregate_tool_groups_and_sorts_indexed_values() {
        let namespace = FakeNamespace::with_aggregate_result(sample_aggregate_result());

        let output = execute_agent_tool(
            &namespace,
            "aggregate",
            &json!({
                "path": "/runs",
                "predicates": [{"field": "run.status", "op": "eq", "value": "completed"}],
                "group_by": ["param.lr"],
                "measures": [
                    {"name": "run_count", "op": "count", "field": "metric.val_loss.min"},
                    {"name": "avg_min_val_loss", "op": "avg", "field": "metric.val_loss.min"},
                    {"name": "stdout_available", "op": "sum", "field": "artifact.stdout_available"}
                ],
                "sort": [{"field": "avg_min_val_loss", "direction": "asc"}],
                "limit": 5
            }),
        )
        .unwrap();

        let request = namespace.last_aggregate.borrow().clone().unwrap();
        assert_eq!(request.predicates[0].field.id, "run.status");
        assert!(namespace.last_find.borrow().is_none());
        assert_eq!(output["input_match_count"], 5);
        assert_eq!(output["row_count"], 3);
        assert_eq!(output["group_count"], 2);
        assert_eq!(output["groups"][0]["key"], json!({"param.lr": "0.001"}));
        assert_eq!(output["groups"][0]["values"]["run_count"], 2);
        assert_eq!(output["groups"][0]["values"]["avg_min_val_loss"], 0.3);
        assert_eq!(output["groups"][0]["values"]["stdout_available"], 1);
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn ls_tool_keeps_entries_lightweight() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "ls",
            &json!({
                "path": "/runs",
                "limit": 1
            }),
        )
        .unwrap();

        assert_eq!(output["entries"][0]["path"], "/runs/run-1");
        assert!(output["entries"][0].get("record_count").is_none());
        assert!(output["entries"][0].get("sample").is_none());
        assert!(output["entries"][0].get("catalog").is_none());
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn read_tool_returns_body_content_without_evidence_noise() {
        let namespace = FakeNamespace::new();

        let output = execute_agent_tool(
            &namespace,
            "read",
            &json!({
                "path": "/runs/run-1/metadata.json",
                "format": "structured",
                "limit": 10
            }),
        )
        .unwrap();

        assert_eq!(output["path"], "/runs/run-1/metadata.json");
        assert_eq!(output["items"][0]["index"], 0);
        assert_eq!(output["items"][0]["value"]["status"], "completed");
        assert_json_lacks_agent_noise(&output);
    }

    #[test]
    fn read_tool_rejects_large_structured_pagination() {
        let namespace = FakeNamespace::with_record_count(MAX_AGENT_PAGE_LIMIT + 1);

        let err = execute_agent_tool(
            &namespace,
            "read",
            &json!({
                "path": "/index/large.json",
                "format": "structured",
                "cursor": "100",
                "limit": 100
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, AgentError::InvalidArgument(ref message) if message.contains("find with catalog predicates")),
            "unexpected error: {err:?}"
        );
        assert_eq!(*namespace.read_calls.borrow(), 0);
    }

    #[test]
    fn read_tool_rejects_large_structured_initial_page() {
        let namespace = FakeNamespace::with_record_count(MAX_AGENT_PAGE_LIMIT + 1);

        let err = execute_agent_tool(
            &namespace,
            "read",
            &json!({
                "path": "/index/large.json",
                "format": "structured",
                "limit": 100
            }),
        )
        .unwrap_err();

        assert!(
            matches!(err, AgentError::InvalidArgument(ref message) if message.contains("find with catalog predicates")),
            "unexpected error: {err:?}"
        );
        assert_eq!(*namespace.read_calls.borrow(), 0);
    }
}
